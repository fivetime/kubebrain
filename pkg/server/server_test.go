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

package server

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"

	"github.com/kubewharf/kubebrain/pkg/backend"
	"github.com/kubewharf/kubebrain/pkg/metrics/mock"
	"github.com/kubewharf/kubebrain/pkg/server/service/leader"
	"github.com/kubewharf/kubebrain/pkg/storage"
	imemkv "github.com/kubewharf/kubebrain/pkg/storage/memkv"
)

type healthStorage struct {
	storage.KvStorage
	fail bool
}

func (s *healthStorage) Get(ctx context.Context, key []byte) ([]byte, error) {
	if s.fail {
		return nil, errors.New("storage unavailable")
	}
	return s.KvStorage.Get(ctx, key)
}

func (s *healthStorage) Iter(ctx context.Context, start, end []byte, timestamp, limit uint64) (storage.Iter, error) {
	if s.fail {
		return nil, errors.New("storage unavailable")
	}
	return s.KvStorage.Iter(ctx, start, end, timestamp, limit)
}

func healthStatus(t *testing.T, s *server) healthpb.HealthCheckResponse_ServingStatus {
	t.Helper()
	resp, err := s.healthServer.Check(context.Background(), &healthpb.HealthCheckRequest{})
	require.NoError(t, err)
	return resp.Status
}

// TestLeadershipHealthTransitions pins #61: losing leadership must flip the gRPC
// health status to NOT_SERVING (it previously wrongly set SERVING), while
// acquiring leadership sets SERVING.
func TestLeadershipHealthTransitions(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	m := mock.NewMinimalMetrics(ctrl)
	kv := imemkv.NewKvStorage()
	b := backend.NewBackend(kv, backend.Config{Prefix: "/registry", Identity: "test", EnableEtcdCompatibility: true}, m)
	defer func() { require.NoError(t, b.(interface{ Close() error }).Close()) }()

	s := &server{
		healthServer: health.NewServer(),
		metricCli:    m,
		backend:      b,
	}
	// Initial state mirrors register(): NOT_SERVING.
	s.healthServer.SetServingStatus("", healthpb.HealthCheckResponse_NOT_SERVING)
	require.Equal(t, healthpb.HealthCheckResponse_NOT_SERVING, healthStatus(t, s))

	// Acquire leadership -> SERVING. etcdServer is nil here, so ReloadLeases is
	// skipped; RebuildCountIndex runs against the (empty) backend.
	s.onStartedLeading(context.Background())
	require.Equal(t, healthpb.HealthCheckResponse_SERVING, healthStatus(t, s),
		"acquiring leadership must report SERVING")

	// Lose leadership -> NOT_SERVING (the #61 fix).
	s.onStoppedLeading()
	require.Equal(t, healthpb.HealthCheckResponse_NOT_SERVING, healthStatus(t, s),
		"losing leadership must report NOT_SERVING so clients stop routing here as leader")
}

func TestLeaderReadinessWaitsForDurableStartup(t *testing.T) {
	ctrl := gomock.NewController(t)
	m := mock.NewMinimalMetrics(ctrl)
	kv := imemkv.NewKvStorage()
	b := backend.NewBackend(kv, backend.Config{Prefix: "/registry", Identity: "test"}, m)
	defer func() { require.NoError(t, b.(interface{ Close() error }).Close()) }()
	s := &server{
		healthServer:   health.NewServer(),
		leaderElection: &leader.Stub{ElectionInfo: leader.ElectionInfo{IsLeader: true}},
		backend:        b,
	}
	s.healthServer.SetServingStatus("", healthpb.HealthCheckResponse_NOT_SERVING)
	require.False(t, s.leaderServing(), "election ownership alone must not publish readiness")
	recorder := httptest.NewRecorder()
	s.httpReadyHandler(recorder, httptest.NewRequest(http.MethodGet, "/ready", nil))
	require.Equal(t, http.StatusServiceUnavailable, recorder.Code)

	s.healthServer.SetServingStatus("", healthpb.HealthCheckResponse_SERVING)
	require.True(t, s.leaderServing())
	recorder = httptest.NewRecorder()
	s.httpReadyHandler(recorder, httptest.NewRequest(http.MethodGet, "/ready", nil))
	require.Equal(t, http.StatusOK, recorder.Code)

	s.healthServer.SetServingStatus("", healthpb.HealthCheckResponse_NOT_SERVING)
	require.False(t, s.leaderServing(), "leadership loss must withdraw readiness")
}

func TestHTTPHealthChecksLeaderAndBackend(t *testing.T) {
	ctrl := gomock.NewController(t)
	m := mock.NewMinimalMetrics(ctrl)
	kv := &healthStorage{KvStorage: imemkv.NewKvStorage()}
	b := backend.NewBackend(kv, backend.Config{Prefix: "/registry", Identity: "test"}, m)
	defer func() { require.NoError(t, b.(interface{ Close() error }).Close()) }()
	s := &server{
		healthServer:   health.NewServer(),
		leaderElection: &leader.Stub{},
		backend:        b,
	}
	s.healthServer.SetServingStatus("", healthpb.HealthCheckResponse_NOT_SERVING)

	recorder := httptest.NewRecorder()
	s.httpHealthHandler(recorder, httptest.NewRequest(http.MethodGet, "/health", nil))
	require.Equal(t, http.StatusServiceUnavailable, recorder.Code)
	require.JSONEq(t, `{"health":"false","reason":"RAFT NO LEADER"}`, recorder.Body.String())

	recorder = httptest.NewRecorder()
	s.httpHealthHandler(recorder, httptest.NewRequest(http.MethodGet, "/health?serializable=true", nil))
	require.Equal(t, http.StatusOK, recorder.Code)
	require.JSONEq(t, HealthResponse, recorder.Body.String())

	kv.fail = true
	recorder = httptest.NewRecorder()
	s.httpHealthHandler(recorder, httptest.NewRequest(http.MethodGet, "/health?serializable=true", nil))
	require.Equal(t, http.StatusServiceUnavailable, recorder.Code)
	require.Contains(t, recorder.Body.String(), `"health":"false"`)
	require.Contains(t, recorder.Body.String(), `"reason":"RANGE ERROR:`)

	recorder = httptest.NewRecorder()
	s.httpPingHandler(recorder, httptest.NewRequest(http.MethodGet, "/ping", nil))
	require.Equal(t, http.StatusOK, recorder.Code)
	require.JSONEq(t, HealthResponse, recorder.Body.String())
}

func TestEtcdLivezAndReadyzChecks(t *testing.T) {
	ctrl := gomock.NewController(t)
	m := mock.NewMinimalMetrics(ctrl)
	kv := &healthStorage{KvStorage: imemkv.NewKvStorage()}
	b := backend.NewBackend(kv, backend.Config{Prefix: "/registry", Identity: "test"}, m)
	defer func() { require.NoError(t, b.(interface{ Close() error }).Close()) }()
	s := &server{
		healthServer:   health.NewServer(),
		leaderElection: &leader.Stub{},
		backend:        b,
	}
	s.healthServer.SetServingStatus("", healthpb.HealthCheckResponse_NOT_SERVING)
	handlers := s.GetClientHttpHandlers()

	recorder := httptest.NewRecorder()
	handlers["/livez"].ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/livez", nil))
	require.Equal(t, http.StatusOK, recorder.Code)
	require.Equal(t, "text/plain; charset=utf-8", recorder.Header().Get("Content-Type"))
	require.Equal(t, "nosniff", recorder.Header().Get("X-Content-Type-Options"))
	require.Equal(t, "ok\n", recorder.Body.String())

	recorder = httptest.NewRecorder()
	handlers["/livez"].ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/livez?verbose=false", nil))
	require.Equal(t, http.StatusOK, recorder.Code)
	require.Equal(t, "[+]serializable_read ok\nok\n", recorder.Body.String())

	recorder = httptest.NewRecorder()
	handlers["/readyz"].ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/readyz", nil))
	require.Equal(t, http.StatusServiceUnavailable, recorder.Code)
	require.Contains(t, recorder.Body.String(), "[+]data_corruption ok\n")
	require.Contains(t, recorder.Body.String(), "[+]serializable_read ok\n")
	require.Contains(t, recorder.Body.String(), "[-]linearizable_read failed: RAFT NO LEADER\n")
	require.Contains(t, recorder.Body.String(), "[+]non_learner ok\n")

	recorder = httptest.NewRecorder()
	handlers["/readyz"].ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/readyz?exclude=linearizable_read&exclude=unknown", nil))
	require.Equal(t, http.StatusOK, recorder.Code)
	require.Equal(t, "ok\n", recorder.Body.String())

	recorder = httptest.NewRecorder()
	handlers["/readyz/serializable_read"].ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/readyz/serializable_read?verbose", nil))
	require.Equal(t, http.StatusOK, recorder.Code)
	require.Equal(t, "[+]serializable_read ok\nok\n", recorder.Body.String())

	recorder = httptest.NewRecorder()
	handlers["/livez"].ServeHTTP(recorder, httptest.NewRequest(http.MethodPost, "/livez", nil))
	require.Equal(t, http.StatusMethodNotAllowed, recorder.Code)
	require.Equal(t, http.MethodGet, recorder.Header().Get("Allow"))

	kv.fail = true
	recorder = httptest.NewRecorder()
	handlers["/livez"].ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/livez", nil))
	require.Equal(t, http.StatusServiceUnavailable, recorder.Code)
	require.Contains(t, recorder.Body.String(), "[-]serializable_read failed: storage unavailable\n")

	recorder = httptest.NewRecorder()
	handlers["/ping"].ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/ping", nil))
	require.Equal(t, http.StatusOK, recorder.Code)
	require.JSONEq(t, HealthResponse, recorder.Body.String())
}

func TestEtcdHealthCheckHandlersAvailableOnInfoPort(t *testing.T) {
	s := &server{}
	handlers := s.GetInfoHttpHandlers()
	for _, path := range []string{
		"/ping",
		"/livez",
		"/livez/serializable_read",
		"/readyz",
		"/readyz/data_corruption",
		"/readyz/serializable_read",
		"/readyz/linearizable_read",
		"/readyz/non_learner",
	} {
		require.Contains(t, handlers, path)
	}
}
