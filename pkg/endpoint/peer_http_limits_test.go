package endpoint

import (
	"bufio"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"net"
	"net/http"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/kubewharf/kubebrain/pkg/transportidentity"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
)

func TestExperimentalPeerHTTPHeaderPolicy(t *testing.T) {
	for _, enabled := range []bool{false, true} {
		e := &Endpoint{config: &Config{}}
		if enabled {
			e.config.ExperimentalPeerRetirement = &PeerRetirementOptions{}
		}
		s := e.newPeerHTTPTransport(grpc.NewServer(), http.NotFoundHandler()).httpServer.svr
		if enabled {
			require.Equal(t, 5*time.Second, s.ReadHeaderTimeout)
			require.Equal(t, 16<<10, s.MaxHeaderBytes)
		} else {
			require.Equal(t, httpReadHeaderTimeout, s.ReadHeaderTimeout)
			require.Equal(t, httpMaxHeaderBytes, s.MaxHeaderBytes)
		}
		require.Zero(t, s.ReadTimeout, "do not impose a whole-request deadline on gRPC streams")
		require.Zero(t, s.WriteTimeout)
	}
}

func TestExperimentalPeerHTTPRejectsSlowAndOversizedHeaders(t *testing.T) {
	ca := newRotationCA(t)
	certFile, keyFile := writeRotationCertificate(t, t.TempDir(), "peer", ca, 2, "peer.test")
	caFile := filepath.Join(t.TempDir(), "ca.pem")
	writeRotationCA(t, caFile, ca)
	cert, err := tls.LoadX509KeyPair(certFile, keyFile)
	require.NoError(t, err)
	pool := x509.NewCertPool()
	pool.AddCert(ca.cert)
	e := &Endpoint{config: &Config{ExperimentalPeerRetirement: &PeerRetirementOptions{}}, tlsIdentities: &transportidentity.Registry{}}
	var calls atomic.Int32
	internal := e.newPeerHTTPTransport(grpc.NewServer(), http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.WriteHeader(http.StatusNoContent)
	}))
	internal.goAwayPropagationDelay = 0
	secure := newSecureServer(&SecurityConfig{CertFile: certFile, KeyFile: keyFile, CA: caFile, ClientAuth: true}, e.tlsIdentities, internal)
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	done := make(chan error, 1)
	go func() { done <- secure.serve(listener) }()
	defer func() { require.NoError(t, secure.close()); require.NoError(t, <-done) }()
	dial := func(t *testing.T) *tls.Conn {
		t.Helper()
		c, err := tls.DialWithDialer(&net.Dialer{Timeout: time.Second}, "tcp", listener.Addr().String(), &tls.Config{
			RootCAs: pool, Certificates: []tls.Certificate{cert}, ServerName: "peer.test", NextProtos: []string{"http/1.1"}, MinVersion: tls.VersionTLS12,
		})
		require.NoError(t, err)
		t.Cleanup(func() { _ = c.Close() })
		require.NoError(t, c.SetDeadline(time.Now().Add(7*time.Second)))
		return c
	}
	t.Run("oversized", func(t *testing.T) {
		c := dial(t)
		_, err := fmt.Fprintf(c, "POST /internal/successor/v1 HTTP/1.1\r\nHost: peer.test\r\nX-Large: %s\r\n\r\n", strings.Repeat("x", 24<<10))
		require.NoError(t, err)
		response, err := http.ReadResponse(bufio.NewReader(c), nil)
		require.NoError(t, err)
		defer response.Body.Close()
		require.Equal(t, http.StatusRequestHeaderFieldsTooLarge, response.StatusCode)
	})
	t.Run("slow", func(t *testing.T) {
		c := dial(t)
		_, err := fmt.Fprint(c, "POST /internal/successor/v1 HTTP/1.1\r\nHost: peer.test\r\nX-Slow: ")
		require.NoError(t, err)
		_, err = http.ReadResponse(bufio.NewReader(c), nil)
		require.Error(t, err)
		if timeout, ok := err.(net.Error); ok {
			require.False(t, timeout.Timeout(), "server must close before the client's seven-second safety deadline")
		}
	})
	t.Run("silent after TLS", func(t *testing.T) {
		c := dial(t)
		_, err := c.Read(make([]byte, 1))
		require.Error(t, err)
		if timeout, ok := err.(net.Error); ok {
			require.False(t, timeout.Timeout(), "classification must close before the client deadline")
		}
	})
	require.Zero(t, calls.Load(), "header rejection must precede handler admission")
}
