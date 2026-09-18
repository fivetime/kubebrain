package server

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"io"
	"net/http"
	"sync/atomic"
	"testing"
	"time"

	"github.com/kubewharf/kubebrain/pkg/backend/election"
	"github.com/stretchr/testify/require"
)

func TestPeerRetirementSenderToAuthenticatedHandler(t *testing.T) {
	auth, pool, certs, payload, condition := retirementHandlerFixture(t)
	for _, h2 := range []bool{false, true} {
		t.Run(fmt.Sprintf("h2=%v", h2), func(t *testing.T) {
			var calls atomic.Int32
			h, err := newPeerRetirementHandler(auth, func(ctx context.Context, received election.OwnershipCondition) error {
				wire, err := received.MarshalBinary()
				if err != nil {
					return err
				}
				if !bytes.Equal(payload, wire) {
					return errPeerRetirementRequest
				}
				calls.Add(1)
				return nil
			}, time.Second, time.Second, 2, 100)
			require.NoError(t, err)
			var protocol atomic.Int32
			srv := retirementHandlerServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				protocol.Store(int32(r.ProtoMajor))
				if r.URL.Path != peerRetirementPath {
					w.WriteHeader(404)
					return
				}
				h.ServeHTTP(w, r)
			}), pool, certs, h2)
			credentials := &tls.Config{RootCAs: pool, Certificates: []tls.Certificate{certs[0]}}
			sender, err := newPeerRetirementSender("instance", "old", []string{srv.URL}, credentials, time.Second)
			require.NoError(t, err)
			require.NoError(t, sender.send(context.Background(), condition))
			require.Equal(t, int32(1), calls.Load())
			if h2 {
				require.Equal(t, int32(2), protocol.Load())
			} else {
				require.Equal(t, int32(1), protocol.Load())
			}
			// Same CA and CN is not the configured holder's key.
			credentials.Certificates = []tls.Certificate{certs[1]}
			other, err := newPeerRetirementSender("instance", "old", []string{srv.URL}, credentials, time.Second)
			require.NoError(t, err)
			require.ErrorIs(t, other.send(context.Background(), condition), errPeerRetirementUnconfirmed)
			require.Equal(t, int32(1), calls.Load())
			wrongHolder, err := newPeerRetirementSender("instance", "other", []string{srv.URL}, credentials, time.Second)
			require.NoError(t, err)
			require.ErrorIs(t, wrongHolder.send(context.Background(), condition), errPeerRetirementUnconfirmed)
			require.Equal(t, int32(1), calls.Load())
		})
	}
}

func TestPeerRetirementSenderNoRedirectAndBoundedFallback(t *testing.T) {
	_, pool, certs, _, condition := retirementHandlerFixture(t)
	credentials := &tls.Config{RootCAs: pool, Certificates: []tls.Certificate{certs[0]}}
	var redirected, fallback atomic.Int32
	target := retirementHandlerServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { redirected.Add(1); w.WriteHeader(204) }), pool, certs, false)
	first := retirementHandlerServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, target.URL, http.StatusTemporaryRedirect)
	}), pool, certs, false)
	second := retirementHandlerServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { fallback.Add(1); w.WriteHeader(204) }), pool, certs, false)
	sender, err := newPeerRetirementSender("instance", "old", []string{first.URL, second.URL}, credentials, time.Second)
	require.NoError(t, err)
	require.NoError(t, sender.send(context.Background(), condition))
	require.Zero(t, redirected.Load())
	require.Equal(t, int32(1), fallback.Load())
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	require.ErrorIs(t, sender.send(ctx, condition), errPeerRetirementUnconfirmed)
	require.Equal(t, int32(1), fallback.Load())

	// Hold the first response until client cancellation. An attempt-local timer
	// must not reset the global budget and start a second request afterward.
	slow := retirementHandlerServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		<-r.Context().Done()
	}), pool, certs, false)
	bounded, err := newPeerRetirementSender("instance", "old", []string{slow.URL, second.URL}, credentials, 50*time.Millisecond)
	require.NoError(t, err)
	start := time.Now()
	require.ErrorIs(t, bounded.send(context.Background(), condition), errPeerRetirementUnconfirmed)
	require.Less(t, time.Since(start), time.Second)
	require.Equal(t, int32(1), fallback.Load(), "shared deadline exhausted: no next peer request")
}

func TestPeerRetirementSenderRejectsUnsafeConfiguration(t *testing.T) {
	_, pool, certs, _, _ := retirementHandlerFixture(t)
	credentials := &tls.Config{RootCAs: pool, Certificates: []tls.Certificate{certs[0]}}
	for _, endpoint := range []string{"http://peer", "https://user:secret@peer", "https://peer/path", "https://peer?", "https://peer?x=y", "https://peer/#x", "https:///", "https://peer/%2f"} {
		_, err := newPeerRetirementSender("instance", "old", []string{endpoint}, credentials, time.Second)
		require.Error(t, err, "endpoint %q", endpoint)
	}
	for _, identity := range []string{"", " old", "old\r\nInjected: value", "old\x00"} {
		_, err := newPeerRetirementSender("instance", identity, []string{"https://peer"}, credentials, time.Second)
		require.Error(t, err)
	}
	for _, endpoints := range [][]string{nil, {"https://peer", "https://peer/"}, make([]string, 17)} {
		_, err := newPeerRetirementSender("instance", "old", endpoints, credentials, time.Second)
		require.Error(t, err)
	}
	for _, name := range []string{"insecure", "missing roots", "missing cert", "dynamic cert", "custom verifier", "servername override", "old maximum"} {
		t.Run(name, func(t *testing.T) {
			config := credentials.Clone()
			switch name {
			case "insecure":
				config.InsecureSkipVerify = true
			case "missing roots":
				config.RootCAs = nil
			case "missing cert":
				config.Certificates = nil
			case "dynamic cert":
				config.GetClientCertificate = func(*tls.CertificateRequestInfo) (*tls.Certificate, error) { return &certs[0], nil }
			case "custom verifier":
				config.VerifyConnection = func(tls.ConnectionState) error { return nil }
			case "servername override":
				config.ServerName = "other"
			case "old maximum":
				config.MaxVersion = tls.VersionTLS11
			}
			_, err := newPeerRetirementSender("instance", "old", []string{"https://peer"}, config, time.Second)
			require.Error(t, err)
		})
	}
	credentials.MinVersion = tls.VersionTLS13
	sender, err := newPeerRetirementSender("instance", "old", []string{"https://peer"}, credentials, time.Second)
	require.NoError(t, err)
	transport := sender.client.Transport.(*http.Transport)
	require.Equal(t, uint16(tls.VersionTLS13), transport.TLSClientConfig.MinVersion)
	require.Nil(t, transport.Proxy)
}

func TestPeerRetirementSenderRejectsUntrustedServer(t *testing.T) {
	_, pool, certs, _, condition := retirementHandlerFixture(t)
	var calls atomic.Int32
	srv := retirementHandlerServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { calls.Add(1); w.WriteHeader(204) }), pool, certs, false)
	sender, err := newPeerRetirementSender("instance", "old", []string{srv.URL}, &tls.Config{RootCAs: x509.NewCertPool(), Certificates: []tls.Certificate{certs[0]}}, time.Second)
	require.NoError(t, err)
	require.ErrorIs(t, sender.send(context.Background(), condition), errPeerRetirementUnconfirmed)
	require.Zero(t, calls.Load())
}

func TestPeerRetirementSenderLostAckIsNotSuccessOrAutomaticRetry(t *testing.T) {
	_, pool, certs, _, condition := retirementHandlerFixture(t)
	var calls atomic.Int32
	srv := retirementHandlerServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		calls.Add(1) // Represents a callback that may already have committed.
		conn, _, err := w.(http.Hijacker).Hijack()
		if err == nil {
			_ = conn.Close()
		}
	}), pool, certs, false)
	sender, err := newPeerRetirementSender("instance", "old", []string{srv.URL}, &tls.Config{RootCAs: pool, Certificates: []tls.Certificate{certs[0]}}, time.Second)
	require.NoError(t, err)
	require.ErrorIs(t, sender.send(context.Background(), condition), errPeerRetirementUnconfirmed)
	require.Equal(t, int32(1), calls.Load(), "no implicit transport replay of a POST with lost acknowledgement")
}

func TestPeerRetirementSenderRequiresExactAcknowledgement(t *testing.T) {
	_, pool, certs, _, condition := retirementHandlerFixture(t)
	for _, status := range []int{200, 202, 404, 409, 503} {
		t.Run(fmt.Sprint(status), func(t *testing.T) {
			srv := retirementHandlerServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(status) }), pool, certs, false)
			sender, err := newPeerRetirementSender("instance", "old", []string{srv.URL}, &tls.Config{RootCAs: pool, Certificates: []tls.Certificate{certs[0]}}, time.Second)
			require.NoError(t, err)
			require.ErrorIs(t, sender.send(context.Background(), condition), errPeerRetirementUnconfirmed)
		})
	}
}
