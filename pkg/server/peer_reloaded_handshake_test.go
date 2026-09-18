package server

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/kubewharf/kubebrain/pkg/transportidentity"
	"github.com/stretchr/testify/require"
)

type handshakeMaterialSource struct {
	material transportidentity.ClientCredentialMaterial
	err      error
	loads    int
}

func (s *handshakeMaterialSource) LoadClientCredentialMaterial(context.Context) (transportidentity.ClientCredentialMaterial, error) {
	s.loads++
	return s.material, s.err
}

type handshakeTrackedConn struct {
	net.Conn
	closed atomic.Bool
}

func (c *handshakeTrackedConn) Close() error {
	c.closed.Store(true)
	return c.Conn.Close()
}

func TestReloadedHandshakeFreshMaterialAndNoFallback(t *testing.T) {
	pool, certificates := retirementTestCertificates(t)
	auth, err := newPeerRetirementAuthorizer("scope", map[string][]string{
		"local":  {retirementTestPin(certificates[0]), retirementTestPin(certificates[2])},
		"remote": {retirementTestPin(certificates[1])},
	})
	require.NoError(t, err)
	observedClients := make(chan string, 8)
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	server.TLS = &tls.Config{Certificates: []tls.Certificate{certificates[1]}, ClientAuth: tls.RequireAndVerifyClientCert, ClientCAs: pool,
		VerifyConnection: func(state tls.ConnectionState) error {
			observedClients <- state.PeerCertificates[0].SerialNumber.String()
			return nil
		}}
	server.StartTLS()
	defer server.Close()
	source := &handshakeMaterialSource{}
	for _, mode := range []string{"valid", "rotated local", "removed CA", "load error", "wrong local pin", "wrong remote pin", "wrong hostname", "recovered"} {
		t.Run(mode, func(t *testing.T) {
			source.material = transportidentity.ClientCredentialMaterial{Roots: pool, Certificate: certificates[0]}
			source.err = nil
			remote, hostname := "remote", "127.0.0.1"
			switch mode {
			case "rotated local":
				source.material.Certificate = certificates[2]
			case "removed CA":
				source.material.Roots = x509.NewCertPool()
			case "load error":
				source.err = errors.New("private source error must not escape")
			case "wrong local pin":
				source.material.Certificate = certificates[3]
			case "wrong remote pin":
				remote = "local"
			case "wrong hostname":
				hostname = "wrong.invalid"
			}
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			raw, err := (&net.Dialer{}).DialContext(ctx, "tcp", strings.TrimPrefix(server.URL, "https://"))
			require.NoError(t, err)
			tracked := &handshakeTrackedConn{Conn: raw}
			defer tracked.Close()
			before := source.loads
			conn, err := handshakeReloadedPeer(ctx, tracked, source, auth, "local", remote, hostname, []string{"http/1.1"})
			require.Equal(t, before+1, source.loads)
			if mode == "valid" || mode == "rotated local" || mode == "recovered" {
				require.NoError(t, err)
				require.False(t, tracked.closed.Load())
				state := conn.ConnectionState()
				require.True(t, state.HandshakeComplete)
				require.NotEmpty(t, state.VerifiedChains, "HTTP response authorization needs transport-owned verified chains")
				require.NoError(t, auth.authorize(&state, "scope", remote, time.Now()))
				select {
				case serial := <-observedClients:
					require.Equal(t, source.material.Certificate.Leaf.SerialNumber.String(), serial)
				case <-ctx.Done():
					t.Fatal("server did not verify the fresh client certificate")
				}
				require.NoError(t, conn.Close())
			} else {
				require.ErrorIs(t, err, errPeerCredentialVerification)
				require.Nil(t, conn)
				require.True(t, tracked.closed.Load(), "failed handshakes must release their socket")
			}
		})
	}
}

func TestReloadedHandshakeStalledPeerHonorsDeadline(t *testing.T) {
	pool, certs := retirementTestCertificates(t)
	auth, err := newPeerRetirementAuthorizer("scope", map[string][]string{"local": {retirementTestPin(certs[0])}})
	require.NoError(t, err)
	raw, other := net.Pipe()
	defer other.Close()
	tracked := &handshakeTrackedConn{Conn: raw}
	source := &handshakeMaterialSource{material: transportidentity.ClientCredentialMaterial{Roots: pool, Certificate: certs[0]}}
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	start := time.Now()
	conn, err := handshakeReloadedPeer(ctx, tracked, source, auth, "local", "remote", "127.0.0.1", nil)
	require.ErrorIs(t, err, errPeerCredentialVerification)
	require.Nil(t, conn)
	require.True(t, tracked.closed.Load())
	require.Less(t, time.Since(start), time.Second)
}

func TestReloadedHandshakeRequiresLiveBoundedContext(t *testing.T) {
	pool, certs := retirementTestCertificates(t)
	auth, err := newPeerRetirementAuthorizer("scope", map[string][]string{"local": {retirementTestPin(certs[0])}})
	require.NoError(t, err)
	for _, mode := range []string{"unbounded", "canceled", "nil"} {
		t.Run(mode, func(t *testing.T) {
			ctx := context.Background()
			if mode == "canceled" {
				var cancel context.CancelFunc
				ctx, cancel = context.WithTimeout(ctx, time.Second)
				cancel()
			} else if mode == "nil" {
				ctx = nil
			}
			raw, other := net.Pipe()
			defer other.Close()
			tracked := &handshakeTrackedConn{Conn: raw}
			source := &handshakeMaterialSource{material: transportidentity.ClientCredentialMaterial{Roots: pool, Certificate: certs[0]}}
			conn, err := handshakeReloadedPeer(ctx, tracked, source, auth, "local", "remote", "127.0.0.1", nil)
			require.ErrorIs(t, err, errPeerCredentialVerification)
			require.Nil(t, conn)
			require.True(t, tracked.closed.Load())
			require.Zero(t, source.loads)
		})
	}
}
