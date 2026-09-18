package main

import (
	"context"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"encoding/pem"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
)

func probeFixture(t *testing.T, handler http.Handler) config {
	t.Helper()
	s := httptest.NewUnstartedServer(handler)
	// This fixture verifies that the probe sends a client certificate; product
	// CA/SPKI authorization remains covered by the real Endpoint tests.
	s.TLS = &tls.Config{ClientAuth: tls.RequireAnyClientCert, MinVersion: tls.VersionTLS12}
	s.StartTLS()
	t.Cleanup(s.Close)
	dir := t.TempDir()
	cert := s.TLS.Certificates[0]
	key, err := x509.MarshalPKCS8PrivateKey(cert.PrivateKey)
	require.NoError(t, err)
	crtPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: cert.Certificate[0]})
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: key})
	crtPath, keyPath := filepath.Join(dir, "tls.crt"), filepath.Join(dir, "tls.key")
	require.NoError(t, os.WriteFile(crtPath, crtPEM, 0600))
	require.NoError(t, os.WriteFile(keyPath, keyPEM, 0600))
	pin := sha256.Sum256(s.Certificate().RawSubjectPublicKeyInfo)
	return config{endpoint: s.URL, serverName: "example.com", serverPin: hex.EncodeToString(pin[:]), ca: crtPath, cert: crtPath, key: keyPath, scope: "retirement-v1:test", sender: "sender:3380", receiver: "receiver:3380"}
}

func contractHandler(t *testing.T, mutate func(http.Header), calls *atomic.Int32) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		body, err := io.ReadAll(r.Body)
		if err != nil || len(body) != 0 || r.ContentLength != 0 || len(r.TransferEncoding) != 0 || r.Header.Get("Content-Type") != "" || r.Method != http.MethodPost {
			t.Error("probe must never submit retirement payload or alternate method")
			w.WriteHeader(500)
			return
		}
		if r.TLS == nil || len(r.TLS.PeerCertificates) == 0 {
			t.Error("missing TLS client identity")
		}
		w.Header().Set("Cache-Control", "no-store")
		if r.Header.Get("X-Kubebrain-Retirement-Instance") != "retirement-v1:test" || r.Header.Get("X-Kubebrain-Retirement-Holder") != "sender:3380" {
			w.WriteHeader(http.StatusForbidden)
			return
		}
		if r.URL.Path == "/internal/successor/v1" {
			w.Header().Set("X-Kubebrain-Successor-Scope", "retirement-v1:test")
			w.Header().Set("X-Kubebrain-Successor-Holder", "receiver:3380")
			if mutate != nil {
				mutate(w.Header())
			}
			w.WriteHeader(http.StatusNoContent)
		} else if r.URL.Path == "/internal/retirement/v1" {
			w.WriteHeader(http.StatusBadRequest)
		} else {
			t.Error("unexpected route")
			w.WriteHeader(404)
		}
	})
}

func TestProbeUsesSixEmptyAuthenticatedRequestsWithoutRetirement(t *testing.T) {
	var calls atomic.Int32
	c := probeFixture(t, contractHandler(t, nil, &calls))
	client, err := newClient(c)
	require.NoError(t, err)
	defer client.CloseIdleConnections()
	require.NoError(t, probe(context.Background(), client, c))
	require.EqualValues(t, 6, calls.Load())
}

func TestProbeRejectsWrongOrDuplicateSuccessorIdentity(t *testing.T) {
	for _, name := range []string{"X-Kubebrain-Successor-Scope", "X-Kubebrain-Successor-Holder"} {
		for _, duplicate := range []bool{false, true} {
			t.Run(name+map[bool]string{false: "-wrong", true: "-duplicate"}[duplicate], func(t *testing.T) {
				var calls atomic.Int32
				c := probeFixture(t, contractHandler(t, func(h http.Header) {
					if duplicate {
						h.Add(name, h.Get(name))
					} else {
						h.Set(name, "wrong")
					}
				}, &calls))
				client, err := newClient(c)
				require.NoError(t, err)
				defer client.CloseIdleConnections()
				require.Error(t, probe(context.Background(), client, c))
				require.EqualValues(t, 3, calls.Load(), "stop before retirement route after bad successor proof")
			})
		}
	}
}

func TestProbeRequiresStandardTLSAndPinBeforeHTTP(t *testing.T) {
	for _, change := range []string{"pin", "dns", "roots"} {
		t.Run(change, func(t *testing.T) {
			var calls atomic.Int32
			c := probeFixture(t, contractHandler(t, nil, &calls))
			switch change {
			case "pin":
				c.serverPin = hex.EncodeToString(make([]byte, 32))
			case "dns":
				c.serverName = "wrong.invalid"
			}
			client, err := newClient(c)
			require.NoError(t, err)
			defer client.CloseIdleConnections()
			if change == "roots" {
				client.Transport.(*http.Transport).TLSClientConfig.RootCAs = x509.NewCertPool()
			}
			require.Error(t, probe(context.Background(), client, c))
			require.Zero(t, calls.Load())
		})
	}
}

func TestProbeRejectsRedirectUnexpectedStatusBodyAndCancellation(t *testing.T) {
	for _, mode := range []string{"redirect", "accepted-invalid", "body", "cancel"} {
		t.Run(mode, func(t *testing.T) {
			var calls atomic.Int32
			c := probeFixture(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				w.Header().Set("Cache-Control", "no-store")
				switch mode {
				case "redirect":
					w.Header().Set("Location", "/other")
					w.WriteHeader(307)
				case "accepted-invalid":
					w.WriteHeader(204)
				default:
					w.WriteHeader(403)
					_, _ = w.Write([]byte("unexpected"))
				}
			}))
			client, err := newClient(c)
			require.NoError(t, err)
			defer client.CloseIdleConnections()
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			if mode == "cancel" {
				cancel()
			}
			require.Error(t, probe(ctx, client, c))
			if mode == "cancel" {
				require.Zero(t, calls.Load())
			} else {
				require.EqualValues(t, 1, calls.Load(), "must not redirect or retry")
			}
		})
	}
}

func TestProbeRejectsAmbiguousInputsBeforeNetwork(t *testing.T) {
	c := config{endpoint: "https://127.0.0.1:3380", serverName: "peer.test", serverPin: hex.EncodeToString(make([]byte, 32)), scope: "scope", sender: "sender", receiver: "receiver"}
	for _, endpoint := range []string{"http://peer", "https://peer/", "https://peer?", "https://peer?x=1", "https://user@peer", "https://peer#fragment"} {
		copy := c
		copy.endpoint = endpoint
		require.Error(t, copy.validate())
	}
	c.receiver = c.sender
	require.Error(t, c.validate())
}
