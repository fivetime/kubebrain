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
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/soheilhy/cmux"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
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
	require.Equal(t, httpMaxHeaderBytes, server.svr.MaxHeaderBytes)
	require.True(t, server.svr.Protocols.HTTP1())
	require.True(t, server.svr.Protocols.HTTP2())
	require.True(t, server.svr.Protocols.UnencryptedHTTP2())
	require.Zero(t, server.svr.ReadTimeout)
	require.Zero(t, server.svr.WriteTimeout)
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
