// Copyright 2026 ByteDance and/or its affiliates
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package endpoint

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/kubewharf/kubebrain/pkg/transportidentity"
)

func TestHTTPAccessControllerMatchesEtcdClientCertificateGatewayBoundary(t *testing.T) {
	called := 0
	authEnabled := true
	handler := newClientHTTPAccessControlledHandler(nil, nil, true,
		func(context.Context) (bool, error) { return authEnabled, nil },
		map[string]http.Handler{"/v3/": http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			called++
			w.WriteHeader(http.StatusNoContent)
		})},
	)

	request := httptest.NewRequest(http.MethodPost, "https://etcd.example/v3/maintenance/status", nil)
	request.TLS = &tls.ConnectionState{VerifiedChains: [][]*x509.Certificate{{{
		Subject: pkix.Name{CommonName: "root"},
	}}}}
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	require.Equal(t, http.StatusBadRequest, response.Code)
	require.Equal(t, gatewayClientCertificateRejection+"\n", response.Body.String())
	require.Equal(t, 0, called)

	request = httptest.NewRequest(http.MethodPost, "https://etcd.example/v3beta/maintenance/status", nil)
	request.TLS = &tls.ConnectionState{VerifiedChains: [][]*x509.Certificate{{{
		Subject: pkix.Name{CommonName: "root"},
	}}}}
	response = httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	require.Equal(t, http.StatusBadRequest, response.Code)
	require.Equal(t, 0, called)

	authEnabled = false
	response = httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	require.Equal(t, http.StatusNoContent, response.Code)
	require.Equal(t, 1, called)
}

func TestHTTPAccessControllerUsesVerifiedTLSStateFromTransportContext(t *testing.T) {
	handler := newClientHTTPAccessControlledHandler(nil, nil, true,
		func(context.Context) (bool, error) { return true, nil },
		map[string]http.Handler{"/v3/": http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusNoContent)
		})},
	)
	request := httptest.NewRequest(http.MethodPost, "https://etcd.example/v3/maintenance/status", nil)
	request.TLS = nil
	request = request.WithContext(transportidentity.WithTLSState(request.Context(), tls.ConnectionState{
		VerifiedChains: [][]*x509.Certificate{{{Subject: pkix.Name{CommonName: "root"}}}},
	}))
	response := httptest.NewRecorder()

	handler.ServeHTTP(response, request)

	require.Equal(t, http.StatusBadRequest, response.Code)
	require.Equal(t, gatewayClientCertificateRejection+"\n", response.Body.String())
}

func TestHTTPAccessControllerClientCertificateGatewayBoundaryIsNarrow(t *testing.T) {
	called := 0
	stateErr := errors.New("auth state unavailable")
	controller := newHTTPAccessController(nil, nil, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		called++
		w.WriteHeader(http.StatusNoContent)
	}))
	controller.gatewayClientCertAuth = true
	controller.authEnabled = func(context.Context) (bool, error) { return true, nil }

	for _, request := range []*http.Request{
		httptest.NewRequest(http.MethodPost, "http://etcd.example/v3/maintenance/status", nil),
		func() *http.Request {
			r := httptest.NewRequest(http.MethodGet, "https://etcd.example/health", nil)
			r.TLS = &tls.ConnectionState{VerifiedChains: [][]*x509.Certificate{{{Subject: pkix.Name{CommonName: "root"}}}}}
			return r
		}(),
		func() *http.Request {
			r := httptest.NewRequest(http.MethodPost, "https://etcd.example/v3/maintenance/status", nil)
			r.TLS = &tls.ConnectionState{VerifiedChains: [][]*x509.Certificate{{{}}}}
			return r
		}(),
	} {
		response := httptest.NewRecorder()
		controller.ServeHTTP(response, request)
		require.Equal(t, http.StatusNoContent, response.Code)
	}
	require.Equal(t, 3, called)

	controller.authEnabled = func(context.Context) (bool, error) { return false, stateErr }
	request := httptest.NewRequest(http.MethodPost, "https://etcd.example/v3/maintenance/status", nil)
	request.TLS = &tls.ConnectionState{VerifiedChains: [][]*x509.Certificate{{{Subject: pkix.Name{CommonName: "root"}}}}}
	response := httptest.NewRecorder()
	controller.ServeHTTP(response, request)
	require.Equal(t, http.StatusServiceUnavailable, response.Code)
	require.Equal(t, "failed to determine authentication state\n", response.Body.String())
	require.Equal(t, 3, called)
}

func TestHTTPAccessControllerMatchesEtcdDefaultsAndPreflight(t *testing.T) {
	called := 0
	handler := newHTTPAccessController([]string{"*"}, []string{"*"}, http.HandlerFunc(
		func(w http.ResponseWriter, _ *http.Request) {
			called++
			w.WriteHeader(http.StatusNoContent)
		},
	))

	response := httptest.NewRecorder()
	handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "http://etcd.example/health", nil))
	require.Equal(t, http.StatusNoContent, response.Code)
	require.Equal(t, 1, called)
	require.Equal(t, "*", response.Header().Get("Access-Control-Allow-Origin"))
	require.Equal(t, "POST, GET, OPTIONS, PUT, DELETE", response.Header().Get("Access-Control-Allow-Methods"))
	require.Equal(t, "accept, content-type, authorization", response.Header().Get("Access-Control-Allow-Headers"))

	response = httptest.NewRecorder()
	handler.ServeHTTP(response, httptest.NewRequest(http.MethodOptions, "http://etcd.example/health", nil))
	require.Equal(t, http.StatusOK, response.Code)
	require.Equal(t, 1, called, "preflight must terminate before the route handler")
	require.Empty(t, response.Body.String())
	require.Equal(t, "*", response.Header().Get("Access-Control-Allow-Origin"))
}

func TestHTTPAccessControllerRejectsNilRequestLikeEtcd(t *testing.T) {
	called := 0
	handler := newHTTPAccessController([]string{"*"}, []string{"*"}, http.HandlerFunc(
		func(w http.ResponseWriter, _ *http.Request) {
			called++
			w.WriteHeader(http.StatusNoContent)
		},
	))

	response := httptest.NewRecorder()
	require.NotPanics(t, func() {
		handler.ServeHTTP(response, nil)
	})
	require.Equal(t, http.StatusBadRequest, response.Code)
	require.Equal(t, 0, called)
	require.Contains(t, response.Body.String(), "Request is nil")
	require.Equal(t, "", requestHostname(nil))
}

func TestHTTPAccessControllerRewritesV3BetaGatewayPrefix(t *testing.T) {
	var seenPath, seenQuery string
	handler := newHTTPAccessController(nil, nil, http.HandlerFunc(
		func(w http.ResponseWriter, req *http.Request) {
			seenPath = req.URL.Path
			seenQuery = req.URL.RawQuery
			w.WriteHeader(http.StatusNoContent)
		},
	))

	response := httptest.NewRecorder()
	handler.ServeHTTP(response, httptest.NewRequest(http.MethodPost,
		"http://etcd.example/v3beta/kv/range?serializable=true", nil))

	require.Equal(t, http.StatusNoContent, response.Code)
	require.Equal(t, "/v3/kv/range", seenPath)
	require.Equal(t, "serializable=true", seenQuery)
}

func TestHTTPAccessControllerEnforcesConfiguredOriginAndPlaintextHost(t *testing.T) {
	handler := newHTTPAccessController(
		[]string{"https://console.example"},
		[]string{"etcd.internal"},
		http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusNoContent) }),
	)

	request := httptest.NewRequest(http.MethodGet, "http://etcd.internal:2379/health", nil)
	request.Header.Set("Origin", "https://console.example")
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	require.Equal(t, http.StatusNoContent, response.Code)
	require.Equal(t, "https://console.example", response.Header().Get("Access-Control-Allow-Origin"))

	request = httptest.NewRequest(http.MethodGet, "http://etcd.internal:2379/health", nil)
	request.Header.Set("Origin", "https://untrusted.example")
	response = httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	require.Equal(t, http.StatusNoContent, response.Code)
	require.Empty(t, response.Header().Get("Access-Control-Allow-Origin"))

	response = httptest.NewRecorder()
	handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "http://untrusted.example/health", nil))
	require.Equal(t, http.StatusMisdirectedRequest, response.Code)
	require.Contains(t, response.Body.String(), "DNS Rebinding")
	require.Empty(t, response.Header().Get("Access-Control-Allow-Origin"),
		"rejected hosts must not receive access-control headers")

	request = httptest.NewRequest(http.MethodGet, "https://untrusted.example/health", nil)
	request.TLS = &tls.ConnectionState{}
	response = httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	require.Equal(t, http.StatusNoContent, response.Code, "TLS requests bypass the plaintext Host defense")
}
