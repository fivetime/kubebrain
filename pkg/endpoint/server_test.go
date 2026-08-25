// Copyright 2026 The KubeBrain Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package endpoint

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/soheilhy/cmux"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
)

type closeErrorServer struct {
	err error
}

func (s closeErrorServer) name() string { return "close-error" }
func (s closeErrorServer) matchWriters() []cmux.MatchWriter {
	return matchersToMatchWriters(cmux.Any())
}
func (s closeErrorServer) serve(net.Listener) error { return nil }
func (s closeErrorServer) close() error             { return s.err }

func TestNormalizeServeError(t *testing.T) {
	for _, err := range []error{
		nil,
		http.ErrServerClosed,
		grpc.ErrServerStopped,
		net.ErrClosed,
		cmux.ErrListenerClosed,
		cmux.ErrServerClosed,
		fmt.Errorf("wrapped: %w", cmux.ErrListenerClosed),
	} {
		require.NoError(t, normalizeServeError(err))
	}

	unexpected := errors.New("accept failed")
	require.ErrorIs(t, normalizeServeError(unexpected), unexpected)
}

func TestHTTPServerCloseDrainsInFlightRequest(t *testing.T) {
	handlerStarted := make(chan struct{})
	releaseHandler := make(chan struct{})
	server := newHTTPServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		close(handlerStarted)
		<-releaseHandler
		w.WriteHeader(http.StatusNoContent)
	}))
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	serveDone := make(chan error, 1)
	go func() { serveDone <- server.serve(listener) }()

	client := &http.Client{Timeout: 5 * time.Second}
	t.Cleanup(client.CloseIdleConnections)
	type requestResult struct {
		status int
		err    error
	}
	requestDone := make(chan requestResult, 1)
	go func() {
		response, requestErr := client.Get("http://" + listener.Addr().String())
		if requestErr != nil {
			requestDone <- requestResult{err: requestErr}
			return
		}
		defer response.Body.Close()
		requestDone <- requestResult{status: response.StatusCode}
	}()
	select {
	case <-handlerStarted:
	case <-time.After(5 * time.Second):
		t.Fatal("request did not reach handler")
	}

	closeDone := make(chan error, 1)
	go func() { closeDone <- server.close() }()
	select {
	case closeErr := <-closeDone:
		t.Fatalf("close returned before the in-flight request drained: %v", closeErr)
	case <-time.After(100 * time.Millisecond):
	}
	close(releaseHandler)

	result := <-requestDone
	require.NoError(t, result.err)
	require.Equal(t, http.StatusNoContent, result.status)
	require.NoError(t, <-closeDone)
	require.NoError(t, normalizeServeError(<-serveDone))
}

func TestHTTPServerCloseForcesRequestPastDrainBudget(t *testing.T) {
	handlerStarted := make(chan struct{})
	handlerCanceled := make(chan struct{})
	server := newHTTPServer(http.HandlerFunc(func(_ http.ResponseWriter, request *http.Request) {
		close(handlerStarted)
		<-request.Context().Done()
		close(handlerCanceled)
	}))
	server.shutdownTimeout = 50 * time.Millisecond
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	serveDone := make(chan error, 1)
	go func() { serveDone <- server.serve(listener) }()

	requestDone := make(chan error, 1)
	go func() {
		response, requestErr := http.Get("http://" + listener.Addr().String())
		if response != nil {
			requestErr = errors.Join(requestErr, response.Body.Close())
		}
		requestDone <- requestErr
	}()
	select {
	case <-handlerStarted:
	case <-time.After(5 * time.Second):
		t.Fatal("request did not reach handler")
	}

	started := time.Now()
	require.NoError(t, server.close())
	require.Less(t, time.Since(started), time.Second,
		"an in-flight request must not outlive the bounded shutdown window")
	require.Error(t, <-requestDone)
	select {
	case <-handlerCanceled:
	case <-time.After(time.Second):
		t.Fatal("forced HTTP close did not cancel the in-flight request")
	}
	require.NoError(t, normalizeServeError(<-serveDone))
}

func TestMuxedHTTP2CloseBoundsQuiescingRequest(t *testing.T) {
	handlerStarted := make(chan struct{})
	handlerCanceled := make(chan struct{})
	server := newGRPCMuxedHTTPServer(grpc.NewServer(), http.HandlerFunc(func(_ http.ResponseWriter, request *http.Request) {
		close(handlerStarted)
		<-request.Context().Done()
		close(handlerCanceled)
	}))
	server.httpServer.shutdownTimeout = 50 * time.Millisecond
	server.quiesceDelay = 50 * time.Millisecond
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	serveDone := make(chan error, 1)
	go func() { serveDone <- server.serve(listener) }()

	requestDone := make(chan error, 1)
	go func() {
		response, requestErr := http.Get("http://" + listener.Addr().String())
		if response != nil {
			requestErr = errors.Join(requestErr, response.Body.Close())
		}
		requestDone <- requestErr
	}()
	select {
	case <-handlerStarted:
	case <-time.After(5 * time.Second):
		t.Fatal("request did not reach handler")
	}

	server.quiesce()
	started := time.Now()
	require.NoError(t, server.close())
	require.Less(t, time.Since(started), time.Second,
		"final close must bound the background quiesce wait")
	require.Error(t, <-requestDone)
	select {
	case <-handlerCanceled:
	case <-time.After(time.Second):
		t.Fatal("final close did not cancel the quiescing request")
	}
	require.NoError(t, normalizeServeError(<-serveDone))
}

func TestMuxedHTTP2QuiesceBoundsLongLivedRequest(t *testing.T) {
	handlerStarted := make(chan struct{})
	handlerCanceled := make(chan struct{})
	server := newGRPCMuxedHTTPServer(grpc.NewServer(), http.HandlerFunc(func(_ http.ResponseWriter, request *http.Request) {
		close(handlerStarted)
		<-request.Context().Done()
		close(handlerCanceled)
	}))
	server.httpServer.shutdownTimeout = 50 * time.Millisecond
	server.quiesceDelay = 50 * time.Millisecond
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	serveDone := make(chan error, 1)
	go func() { serveDone <- server.serve(listener) }()
	t.Cleanup(func() { require.NoError(t, server.close()) })

	requestDone := make(chan error, 1)
	go func() {
		response, requestErr := http.Get("http://" + listener.Addr().String())
		if response != nil {
			requestErr = errors.Join(requestErr, response.Body.Close())
		}
		requestDone <- requestErr
	}()
	select {
	case <-handlerStarted:
	case <-time.After(5 * time.Second):
		t.Fatal("request did not reach handler")
	}

	server.quiesce()
	select {
	case <-handlerCanceled:
	case <-time.After(time.Second):
		t.Fatal("quiesce left a long-lived request attached past its drain budget")
	}
	require.Error(t, <-requestDone)
	require.NoError(t, normalizeServeError(<-serveDone))
}

func TestMuxedHTTP2QuiesceBoundsLongLivedGRPCStreamAfterPropagation(t *testing.T) {
	grpcServer := grpc.NewServer()
	healthServer := health.NewServer()
	healthServer.SetServingStatus("", healthpb.HealthCheckResponse_SERVING)
	healthpb.RegisterHealthServer(grpcServer, healthServer)
	server := newGRPCMuxedHTTPServer(grpcServer, http.NotFoundHandler())
	server.quiesceDelay = 30 * time.Millisecond
	server.httpServer.shutdownTimeout = 20 * time.Millisecond
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	serveDone := make(chan error, 1)
	go func() { serveDone <- server.serve(listener) }()
	t.Cleanup(func() { require.NoError(t, server.close()) })

	connection, err := grpc.NewClient(
		"passthrough:///"+listener.Addr().String(),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, connection.Close()) })
	watch, err := healthpb.NewHealthClient(connection).Watch(t.Context(), &healthpb.HealthCheckRequest{})
	require.NoError(t, err)
	initial, err := watch.Recv()
	require.NoError(t, err)
	require.Equal(t, healthpb.HealthCheckResponse_SERVING, initial.Status)

	started := time.Now()
	server.quiesce()
	streamDone := make(chan error, 1)
	go func() {
		_, recvErr := watch.Recv()
		streamDone <- recvErr
	}()
	select {
	case recvErr := <-streamDone:
		require.Error(t, recvErr)
		require.GreaterOrEqual(t, time.Since(started), server.quiesceDelay,
			"the connection must remain available through the endpoint propagation window")
	case <-time.After(time.Second):
		t.Fatal("quiesce did not force-close the long-lived gRPC stream within its bounded budget")
	}
	require.NoError(t, normalizeServeError(<-serveDone))
}

func TestMuxedHTTP2QuiesceKeepsRunnerAliveUntilShutdown(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	server := newGRPCMuxedHTTPServer(grpc.NewServer(), http.NotFoundHandler())
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	done := make(chan error, 1)
	go func() { done <- runSubServer(ctx, listener, server)() }()
	client := &http.Client{Timeout: time.Second}
	t.Cleanup(client.CloseIdleConnections)

	require.Eventually(t, func() bool {
		response, requestErr := client.Get("http://" + listener.Addr().String())
		if requestErr != nil {
			return false
		}
		_ = response.Body.Close()
		return true
	}, 5*time.Second, 10*time.Millisecond)

	server.quiesce()
	require.Eventually(t, func() bool {
		_, requestErr := client.Get("http://" + listener.Addr().String())
		return requestErr != nil
	}, 5*time.Second, 10*time.Millisecond)
	select {
	case err := <-done:
		t.Fatalf("quiesced transport stopped the endpoint before process shutdown: %v", err)
	case <-time.After(100 * time.Millisecond):
	}

	cancel()
	require.NoError(t, <-done)
}

func TestSecureServerCloseNormalizesAndReturnsErrors(t *testing.T) {
	unexpected := errors.New("close failed")
	server := &secureServer{internalServers: []exposedServer{
		closeErrorServer{err: net.ErrClosed},
		closeErrorServer{err: cmux.ErrServerClosed},
		closeErrorServer{err: unexpected},
	}}

	require.ErrorIs(t, server.close(), unexpected)
}

func TestNewHttpServerBoundsHeaderAdmission(t *testing.T) {
	exposed := newHttpServer(http.NotFoundHandler())
	server, ok := exposed.(*httpServer)
	require.True(t, ok)

	require.Equal(t, httpReadHeaderTimeout, server.svr.ReadHeaderTimeout)
	require.Equal(t, httpIdleTimeout, server.svr.IdleTimeout)
	require.Equal(t, httpShutdownTimeout, server.shutdownTimeout)
	require.Equal(t, httpMaxHeaderBytes, server.svr.MaxHeaderBytes)
	require.True(t, server.svr.Protocols.HTTP1())
	require.True(t, server.svr.Protocols.HTTP2())
	require.True(t, server.svr.Protocols.UnencryptedHTTP2())
	require.Zero(t, server.svr.ReadTimeout)
	require.Zero(t, server.svr.WriteTimeout)
}

func TestMuxedHTTP2QuiesceIncludesEndpointPropagationWindow(t *testing.T) {
	server := newGRPCMuxedHTTPServer(grpc.NewServer(), http.NotFoundHandler())
	require.Equal(t, transportQuiescePropagationDelay, server.quiesceDelay)
	require.Positive(t, server.quiesceDelay)
	require.Less(t, server.quiesceDelay+server.httpServer.shutdownTimeout, 5*time.Second,
		"quiesce propagation and forced-close budgets must fit inside the rollout SLA")
}

func TestRootServerBoundsProtocolClassificationForSecureEndpoints(t *testing.T) {
	plain := newHTTPServer(http.NotFoundHandler())
	require.Zero(t, newRootServer(2379, plain).initialReadTimeout,
		"plaintext HTTP retains its independent five-minute header deadline")

	secure := &secureServer{}
	require.Equal(t, tlsIdentityHandshakeTimeout,
		newRootServer(2379, secure).initialReadTimeout)
	require.Equal(t, tlsIdentityHandshakeTimeout,
		newRootServer(2379, secure, plain).initialReadTimeout,
		"a zero-byte connection on a dual-mode socket cannot yet be classified as TLS or plaintext")
}

func TestHTTPServerEnforcesEtcdCompatibleHeaderLimit(t *testing.T) {
	server := newHTTPServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	done := make(chan error, 1)
	go func() { done <- server.serve(listener) }()
	t.Cleanup(func() {
		require.NoError(t, server.close())
		require.ErrorIs(t, <-done, http.ErrServerClosed)
	})

	for _, tt := range []struct {
		name       string
		transport  *http.Transport
		protoMajor int
	}{
		{name: "HTTP/1", transport: &http.Transport{}, protoMajor: 1},
		{name: "h2c", transport: h2cTransport(), protoMajor: 2},
	} {
		t.Run(tt.name, func(t *testing.T) {
			client := &http.Client{Transport: tt.transport, Timeout: 5 * time.Second}
			t.Cleanup(tt.transport.CloseIdleConnections)
			request, err := http.NewRequest(http.MethodGet, "http://"+listener.Addr().String(), nil)
			require.NoError(t, err)
			// This is deliberately above KubeBrain's former 32 KiB limit but below
			// upstream etcd's effective net/http default.
			request.Header.Set("X-Etcd-Compatible-Metadata", strings.Repeat("a", 64<<10))
			response, err := client.Do(request)
			require.NoError(t, err)
			require.Equal(t, tt.protoMajor, response.ProtoMajor)
			require.Equal(t, http.StatusNoContent, response.StatusCode)
			require.NoError(t, response.Body.Close())

			request, err = http.NewRequest(http.MethodGet, "http://"+listener.Addr().String(), nil)
			require.NoError(t, err)
			request.Header.Set("X-Oversized-Metadata", strings.Repeat("a", 2<<20))
			response, err = client.Do(request)
			if tt.protoMajor == 2 {
				// HTTP/2 rejects an oversized header list by resetting the stream;
				// HTTP/1 can return a regular 431 response instead.
				require.ErrorContains(t, err, "request header list larger than peer's advertised limit")
				return
			}
			require.NoError(t, err)
			require.Equal(t, http.StatusRequestHeaderFieldsTooLarge, response.StatusCode)
			require.NoError(t, response.Body.Close())
		})
	}
}

func h2cTransport() *http.Transport {
	protocols := new(http.Protocols)
	protocols.SetUnencryptedHTTP2(true)
	return &http.Transport{Protocols: protocols}
}

func TestMetricsHTTPServerGatesPprofOnInfoPort(t *testing.T) {
	for _, tt := range []struct {
		name       string
		enable     bool
		wantStatus int
		wantBody   string
	}{
		{
			name:       "disabled by default",
			wantStatus: http.StatusNotFound,
			wantBody:   "404 page not found\n",
		},
		{
			name:       "enabled explicitly",
			enable:     true,
			wantStatus: http.StatusOK,
			wantBody:   "Types of profiles available",
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			endpoint := &Endpoint{
				metrics: &interceptorOrderMetrics{},
				server:  rejectingInterceptorServer{},
				config:  &Config{EnablePprof: tt.enable},
			}
			exposed := endpoint.buildMetricsHttpServer()
			server, ok := exposed.(*httpServer)
			require.True(t, ok)

			response := httptest.NewRecorder()
			server.svr.Handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/debug/pprof/", nil))

			require.Equal(t, tt.wantStatus, response.Code)
			require.Contains(t, response.Body.String(), tt.wantBody)
		})
	}
}

func TestPprofHandlersIncludeEtcdDebugSubpaths(t *testing.T) {
	handlers := getPProfHandlers()
	for _, path := range []string{
		"/debug/pprof/",
		"/debug/pprof/profile",
		"/debug/pprof/symbol",
		"/debug/pprof/cmdline",
		"/debug/pprof/trace",
		"/debug/pprof/heap",
		"/debug/pprof/goroutine",
		"/debug/pprof/threadcreate",
		"/debug/pprof/block",
		"/debug/pprof/mutex",
	} {
		require.NotNilf(t, handlers[path], "missing pprof handler for %s", path)
	}
}

func TestMetricsHTTPServerRoutesPprofSubpathsOnlyWhenEnabled(t *testing.T) {
	for _, tt := range []struct {
		name       string
		enable     bool
		wantStatus int
	}{
		{
			name:       "disabled by default",
			wantStatus: http.StatusNotFound,
		},
		{
			name:       "enabled explicitly",
			enable:     true,
			wantStatus: http.StatusOK,
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			endpoint := &Endpoint{
				metrics: &interceptorOrderMetrics{},
				server:  rejectingInterceptorServer{},
				config:  &Config{EnablePprof: tt.enable},
			}
			exposed := endpoint.buildMetricsHttpServer()
			server, ok := exposed.(*httpServer)
			require.True(t, ok)

			for _, path := range []string{
				"/debug/pprof/cmdline",
				"/debug/pprof/goroutine?debug=1",
			} {
				t.Run(path, func(t *testing.T) {
					response := httptest.NewRecorder()
					server.svr.Handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, path, nil))

					require.Equal(t, tt.wantStatus, response.Code)
				})
			}
		})
	}
}

func TestClientHTTPHandlerNeverExposesPprof(t *testing.T) {
	endpoint := &Endpoint{
		server: rejectingInterceptorServer{},
		config: &Config{Port: 1, EnablePprof: true},
	}
	handler, conn, err := endpoint.buildClientHTTPHandler(t.Context())
	require.NoError(t, err)
	require.Nil(t, conn)

	for _, path := range []string{
		"/debug/pprof/",
		"/debug/pprof/cmdline",
		"/debug/pprof/goroutine?debug=1",
	} {
		t.Run(path, func(t *testing.T) {
			response := httptest.NewRecorder()
			handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, path, nil))

			require.Equal(t, http.StatusNotFound, response.Code)
			require.Equal(t, "404 page not found\n", response.Body.String())
		})
	}
}
