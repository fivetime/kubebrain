package endpoint

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/kubewharf/kubebrain/pkg/transportidentity"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
)

func TestPeerHTTPPreservesOuterVerifiedTLSIdentity(t *testing.T) {
	for _, h2 := range []bool{false, true} {
		t.Run(fmt.Sprintf("h2=%v", h2), func(t *testing.T) {
			ca := newRotationCA(t)
			dir := t.TempDir()
			certFile, keyFile := writeRotationCertificate(t, dir, "peer", ca, 2, "peer.test")
			caFile := filepath.Join(dir, "ca.pem")
			require.NoError(t, os.WriteFile(caFile, ca.pem, 0600))
			cert, err := tls.LoadX509KeyPair(certFile, keyFile)
			require.NoError(t, err)
			pool := x509.NewCertPool()
			require.True(t, pool.AppendCertsFromPEM(ca.pem))
			e := &Endpoint{tlsIdentities: &transportidentity.Registry{}}
			seen := make(chan *tls.ConnectionState, 1)
			internal := e.newPeerHTTPTransport(grpc.NewServer(), http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.TLS == nil || !r.TLS.HandshakeComplete || len(r.TLS.VerifiedChains) == 0 {
					w.WriteHeader(http.StatusForbidden)
					return
				}
				if err := http.NewResponseController(w).SetReadDeadline(time.Now().Add(time.Second)); err != nil {
					w.WriteHeader(http.StatusServiceUnavailable)
					return
				}
				seen <- r.TLS
				w.WriteHeader(http.StatusNoContent)
			}))
			internal.goAwayPropagationDelay = 0
			secure := newSecureServer(&SecurityConfig{CertFile: certFile, KeyFile: keyFile, CA: caFile, ClientAuth: true}, e.tlsIdentities, internal)
			listener, err := net.Listen("tcp", "127.0.0.1:0")
			require.NoError(t, err)
			done := make(chan error, 1)
			go func() { done <- secure.serve(listener) }()
			t.Cleanup(func() {
				require.NoError(t, secure.close())
				select {
				case err := <-done:
					require.NoError(t, err)
				case <-time.After(3 * time.Second):
					t.Error("peer transport did not stop")
				}
			})
			transport := &http.Transport{TLSClientConfig: &tls.Config{MinVersion: tls.VersionTLS12, RootCAs: pool, ServerName: "peer.test", Certificates: []tls.Certificate{cert}}, ForceAttemptHTTP2: h2}
			// The production listener prefers HTTP/1 when both ALPN choices are
			// offered. Require exactly the protocol this subtest is exercising.
			protocols := new(http.Protocols)
			protocols.SetHTTP1(!h2)
			protocols.SetHTTP2(h2)
			transport.Protocols = protocols
			if !h2 {
				transport.TLSNextProto = map[string]func(string, *tls.Conn) http.RoundTripper{}
			}
			client := &http.Client{Transport: transport, Timeout: 3 * time.Second}
			t.Cleanup(client.CloseIdleConnections)
			response, err := client.Post("https://"+listener.Addr().String()+"/internal/test", "application/json", nil)
			require.NoError(t, err)
			defer response.Body.Close()
			require.Equal(t, http.StatusNoContent, response.StatusCode)
			if h2 {
				require.Equal(t, 2, response.ProtoMajor)
			} else {
				require.Equal(t, 1, response.ProtoMajor)
			}
			select {
			case state := <-seen:
				require.Equal(t, cert.Certificate[0], state.PeerCertificates[0].Raw)
			case <-time.After(time.Second):
				t.Fatal("missing verified identity")
			}
		})
	}
}

func TestPeerHTTPIdentityDoesNotTrustHeadersOrOverrideNativeTLS(t *testing.T) {
	for _, mode := range []string{"headers only", "native TLS", "unverified registry"} {
		t.Run(mode, func(t *testing.T) {
			r := httptest.NewRequest("POST", "http://peer.invalid/internal/test", nil)
			r.Header.Set("X-Forwarded-Proto", "https")
			r.Header.Set("X-Forwarded-Client-Cert", "fabricated")
			r.Header.Set("X-Kubebrain-Retirement-Holder", "old")
			native := &tls.ConnectionState{Version: tls.VersionTLS13}
			switch mode {
			case "native TLS":
				r.TLS = native
				r = r.WithContext(transportidentity.WithTLSState(r.Context(), tls.ConnectionState{Version: tls.VersionTLS12}))
			case "unverified registry":
				r = r.WithContext(transportidentity.WithTLSState(r.Context(), tls.ConnectionState{HandshakeComplete: true, Version: tls.VersionTLS13}))
			}
			original := r.TLS
			handler := peerHTTPTransportIdentity(http.HandlerFunc(func(w http.ResponseWriter, got *http.Request) {
				switch mode {
				case "headers only":
					require.Nil(t, got.TLS)
				case "native TLS":
					require.Same(t, native, got.TLS)
				case "unverified registry":
					require.NotNil(t, got.TLS)
					require.Empty(t, got.TLS.VerifiedChains)
				}
			}))
			handler.ServeHTTP(httptest.NewRecorder(), r)
			require.Same(t, original, r.TLS, "middleware must not mutate the caller request")
		})
	}
}
