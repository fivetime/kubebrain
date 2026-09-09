package endpoint

import (
	"context"
	"crypto/tls"
	"net"
	"net/http"
	"testing"
	"time"

	"github.com/kubewharf/kubebrain/pkg/transportidentity"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
)

func TestSecureQuiescedServerFinalCloseReturns(t *testing.T) {
	identities := &transportidentity.Registry{}
	internal := newGRPCMuxedHTTPServer(grpc.NewServer(), http.NotFoundHandler(), identities)
	internal.goAwayPropagationDelay = 0
	secure := newSecureServer(&SecurityConfig{
		CertFile: getAuthPath("server.crt"), KeyFile: getAuthPath("server.key"),
	}, identities, internal)
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { _ = listener.Close() })
	done := make(chan error, 1)
	go func() { done <- secure.serve(listener) }()
	client := &http.Client{Timeout: time.Second, Transport: &http.Transport{
		// The checked-in test certificate is used only on this loopback listener.
		TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
	}}
	t.Cleanup(client.CloseIdleConnections)
	require.Eventually(t, func() bool {
		response, requestErr := client.Get("https://" + listener.Addr().String())
		if requestErr != nil {
			return false
		}
		_ = response.Body.Close()
		return true
	}, 5*time.Second, 10*time.Millisecond)
	internal.quiesce()
	select {
	case err := <-done:
		t.Fatalf("quiesce must not terminate the TLS endpoint before final close: %v", err)
	case <-time.After(100 * time.Millisecond):
	}
	require.NoError(t, secure.close())
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("final TLS close did not release quiesced inner runners")
	}
}

func TestSecureCloseBeforeServeDoesNotStartInnerRunners(t *testing.T) {
	// A cancelled outer runner can close the wrapper before its Serve goroutine
	// is scheduled. No TLS configuration or listener may be used afterwards.
	secure := &secureServer{}
	require.NoError(t, secure.close())
	require.NoError(t, secure.serve(nil))
	require.NoError(t, secure.close())
}

func TestSecureNativeGRPCFinalCloseStopsAdmittedStream(t *testing.T) {
	grpcServer := grpc.NewServer()
	healthpb.RegisterHealthServer(grpcServer, health.NewServer())
	internal := newNativeGRPCServer(grpcServer)
	internal.goAwayPropagationDelay = 0
	internal.shutdownTimeout = 50 * time.Millisecond
	secure := newSecureServer(&SecurityConfig{
		CertFile: getAuthPath("server.crt"), KeyFile: getAuthPath("server.key"),
	}, &transportidentity.Registry{}, internal)
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { _ = listener.Close() })
	t.Cleanup(func() { _ = secure.close() })
	done := make(chan error, 1)
	go func() { done <- secure.serve(listener) }()
	connection, err := grpc.NewClient("passthrough:///"+listener.Addr().String(),
		// Loopback-only fixture certificate, as in the HTTPS regression above.
		grpc.WithTransportCredentials(credentials.NewTLS(&tls.Config{InsecureSkipVerify: true})))
	require.NoError(t, err)
	t.Cleanup(func() { _ = connection.Close() })
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	watch, err := healthpb.NewHealthClient(connection).Watch(ctx, &healthpb.HealthCheckRequest{})
	require.NoError(t, err)
	_, err = watch.Recv()
	require.NoError(t, err)
	internal.quiesce()
	select {
	case <-internal.gracefulDone:
		t.Fatal("quiesce must preserve the admitted stream until final close")
	default:
	}
	require.NoError(t, secure.close())
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("final TLS close did not release native gRPC runner")
	}
	_, err = watch.Recv()
	require.Error(t, err)
}

func TestSecureServeAndCloseRace(t *testing.T) {
	for range 20 {
		secure := newSecureServer(&SecurityConfig{
			CertFile: getAuthPath("server.crt"), KeyFile: getAuthPath("server.key"),
		}, &transportidentity.Registry{}, newHTTPServer(http.NotFoundHandler()))
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		require.NoError(t, err)
		done := make(chan error, 1)
		go func() { done <- secure.serve(listener) }()
		require.NoError(t, secure.close())
		select {
		case err := <-done:
			require.NoError(t, err)
		case <-time.After(time.Second):
			_ = listener.Close()
			t.Fatal("concurrent Serve/Close did not terminate")
		}
		_ = listener.Close()
	}
}
