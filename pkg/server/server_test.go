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
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"

	"github.com/kubewharf/kubebrain/pkg/backend"
	"github.com/kubewharf/kubebrain/pkg/metrics"
	"github.com/kubewharf/kubebrain/pkg/metrics/mock"
	"github.com/kubewharf/kubebrain/pkg/server/service/leader"
	"github.com/kubewharf/kubebrain/pkg/server/service/revision"
	"github.com/kubewharf/kubebrain/pkg/storage"
	imemkv "github.com/kubewharf/kubebrain/pkg/storage/memkv"
)

type alwaysLeaderElection struct{}

func (alwaysLeaderElection) Campaign(context.Context)                       {}
func (alwaysLeaderElection) GetLeaderInfo() string                          { return "self" }
func (alwaysLeaderElection) RefreshLeaderInfo(context.Context) error        { return nil }
func (alwaysLeaderElection) LeadershipTerm(context.Context) (uint64, error) { return 1, nil }
func (alwaysLeaderElection) CurrentLeadershipTerm() uint64                  { return 1 }
func (alwaysLeaderElection) IsLeader() bool                                 { return true }
func (alwaysLeaderElection) EpochAndLeadingFresh() (uint64, bool)           { return 1, true }
func (alwaysLeaderElection) GetElectionInfo() (leader.ElectionInfo, error) {
	return leader.ElectionInfo{LeaderAddress: "self", IsLeader: true}, nil
}

type healthStorage struct {
	storage.KvStorage
	fail bool
}

type blockingLeadershipBackend struct {
	backend.Backend
	entered chan struct{}
	exited  chan struct{}
	once    sync.Once
}

type quotaRefreshBackend struct {
	backend.Backend
	calls     atomic.Int32
	completed atomic.Int32
	block     atomic.Bool
	err       error
}

type coldRevisionBackend struct {
	backend.Backend
	current atomic.Uint64
}

func (b *coldRevisionBackend) GetCurrentRevision() uint64    { return b.current.Load() }
func (b *coldRevisionBackend) SetCurrentRevision(rev uint64) { b.current.Store(rev) }

func (b *quotaRefreshBackend) QuotaStatus(ctx context.Context) (int64, int64, bool, error) {
	b.calls.Add(1)
	if b.block.Load() {
		<-ctx.Done()
		b.completed.Add(1)
		return 0, 0, false, ctx.Err()
	}
	b.completed.Add(1)
	return 0, 0, false, b.err
}

func (b *blockingLeadershipBackend) ResumePhysicalCompaction(ctx context.Context) error {
	b.once.Do(func() { close(b.entered) })
	<-ctx.Done()
	close(b.exited)
	return ctx.Err()
}

type healthMetricEvent struct {
	kind  string
	name  string
	value interface{}
	tags  []metrics.T
}

type healthMetricRecorder struct {
	mu     sync.Mutex
	events []healthMetricEvent
}

func (r *healthMetricRecorder) GetGrpcServerOption() []grpc.ServerOption { return nil }
func (r *healthMetricRecorder) GetHttpHandlers() map[string]http.Handler { return nil }
func (r *healthMetricRecorder) EmitHistogram(string, interface{}, ...metrics.T) error {
	return nil
}
func (r *healthMetricRecorder) EmitCounter(name string, value interface{}, tags ...metrics.T) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.events = append(r.events, healthMetricEvent{kind: "counter", name: name, value: value, tags: tags})
	return nil
}
func (r *healthMetricRecorder) EmitGauge(name string, value interface{}, tags ...metrics.T) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.events = append(r.events, healthMetricEvent{kind: "gauge", name: name, value: value, tags: tags})
	return nil
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

func TestRevisionHandlerRestoresColdLeaderFromDurableRevision(t *testing.T) {
	metrics := &healthMetricRecorder{}
	kv := imemkv.NewKvStorage()
	t.Cleanup(func() { require.NoError(t, kv.Close()) })
	b := backend.NewBackend(kv, backend.Config{
		Prefix: "/registry", Identity: "cold-revision", EnableEtcdCompatibility: true,
	}, metrics)
	_, writtenRevision, err := b.TxnApply(context.Background(), []backend.TxnWriteOp{{
		Key: []byte("cold/revision"), Value: []byte("value"),
	}}, nil)
	require.NoError(t, err)
	require.Positive(t, writtenRevision)
	cold := &coldRevisionBackend{Backend: b}

	s := &server{backend: cold, metricCli: metrics, leaderElection: alwaysLeaderElection{}}
	recorder := httptest.NewRecorder()
	s.revisionHandler(recorder, httptest.NewRequest(http.MethodGet, "/revision", nil))
	require.Equal(t, http.StatusOK, recorder.Code)
	var got revision.LeaderRevision
	require.NoError(t, json.Unmarshal(recorder.Body.Bytes(), &got))
	require.Equal(t, writtenRevision, got.Revision)
	require.Equal(t, writtenRevision, cold.GetCurrentRevision())
}

func TestCloseWaitsForLeadershipCallbackBeforeReturning(t *testing.T) {
	ctrl := gomock.NewController(t)
	m := mock.NewMinimalMetrics(ctrl)
	kv := imemkv.NewKvStorage()
	base := backend.NewBackend(kv, backend.Config{
		Prefix: "/registry", Identity: "close-waits", EnableEtcdCompatibility: true,
	}, m)
	blocking := &blockingLeadershipBackend{
		Backend: base,
		entered: make(chan struct{}),
		exited:  make(chan struct{}),
	}
	s := NewServer(context.Background(), blocking, m, Config{})

	select {
	case <-blocking.entered:
	case <-time.After(3 * time.Second):
		t.Fatal("leader callback did not reach startup work")
	}

	require.NoError(t, s.Close())
	select {
	case <-blocking.exited:
	default:
		t.Fatal("Close returned before the leadership callback exited")
	}
	require.NoError(t, s.Close(), "Close must remain idempotent")
	require.NoError(t, base.(interface{ Close() error }).Close())
	ctrl.Finish()
}

func TestQuotaMetricsRefreshRunsImmediatelyPeriodicallyAndStops(t *testing.T) {
	ctrl := gomock.NewController(t)
	m := mock.NewMinimalMetrics(ctrl)
	kv := imemkv.NewKvStorage()
	t.Cleanup(func() { require.NoError(t, kv.Close()) })
	base := backend.NewBackend(kv, backend.Config{
		Prefix: "/registry", Identity: "quota-refresh", EnableEtcdCompatibility: true,
	}, m)
	wrapped := &quotaRefreshBackend{Backend: base}
	s := &server{backend: wrapped, metricCli: m}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		s.runQuotaMetricsRefresh(ctx, time.Millisecond, time.Second)
	}()
	require.Eventually(t, func() bool { return wrapped.calls.Load() >= 2 }, time.Second, time.Millisecond)
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("quota metrics refresh did not stop after context cancellation")
	}
	stoppedAt := wrapped.calls.Load()
	time.Sleep(5 * time.Millisecond)
	require.Equal(t, stoppedAt, wrapped.calls.Load())
}

func TestQuotaMetricsRefreshBoundsBlockedStorageRead(t *testing.T) {
	ctrl := gomock.NewController(t)
	m := mock.NewMinimalMetrics(ctrl)
	kv := imemkv.NewKvStorage()
	t.Cleanup(func() { require.NoError(t, kv.Close()) })
	base := backend.NewBackend(kv, backend.Config{
		Prefix: "/registry", Identity: "quota-refresh-timeout", EnableEtcdCompatibility: true,
	}, m)
	wrapped := &quotaRefreshBackend{Backend: base}
	wrapped.block.Store(true)
	s := &server{backend: wrapped, metricCli: m}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		s.runQuotaMetricsRefresh(ctx, time.Hour, 5*time.Millisecond)
	}()
	require.Eventually(t, func() bool { return wrapped.completed.Load() == 1 }, time.Second, time.Millisecond)
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("quota metrics refresh did not stop after a bounded storage timeout")
	}
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
	watchCtx, cancelWatch := context.WithCancel(context.Background())
	defer cancelWatch()
	watchCh, err := b.Watch(watchCtx, "/registry/watch/term", 0)
	require.NoError(t, err)

	// Lose leadership -> NOT_SERVING (the #61 fix).
	s.onStoppedLeading()
	require.Equal(t, healthpb.HealthCheckResponse_NOT_SERVING, healthStatus(t, s),
		"losing leadership must report NOT_SERVING so clients stop routing here as leader")
	select {
	case _, ok := <-watchCh:
		require.False(t, ok, "leadership loss must retire local watch subscriptions")
	case <-time.After(time.Second):
		t.Fatal("local watch remained open after leadership loss")
	}
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
	require.Equal(t, "text/plain; charset=utf-8", recorder.Header().Get("Content-Type"))
	require.Empty(t, recorder.Header().Get("X-Content-Type-Options"))

	kv.fail = true
	recorder = httptest.NewRecorder()
	s.httpHealthHandler(recorder, httptest.NewRequest(http.MethodGet, "/health?serializable=true", nil))
	require.Equal(t, http.StatusServiceUnavailable, recorder.Code)
	require.Contains(t, recorder.Body.String(), `"health":"false"`)
	require.Contains(t, recorder.Body.String(), `"reason":"ALARM ERROR:`)

	recorder = httptest.NewRecorder()
	s.httpPingHandler(recorder, httptest.NewRequest(http.MethodGet, "/ping", nil))
	require.Equal(t, http.StatusOK, recorder.Code)
	require.JSONEq(t, HealthResponse, recorder.Body.String())
}

func TestHTTPHealthHandlerRejectsNonGet(t *testing.T) {
	for _, method := range []string{
		http.MethodConnect,
		http.MethodTrace,
		http.MethodPut,
		http.MethodPost,
		http.MethodHead,
	} {
		t.Run(method, func(t *testing.T) {
			recorder := httptest.NewRecorder()
			(&server{}).httpHealthHandler(
				recorder,
				httptest.NewRequest(method, "/health", nil),
			)
			require.Equal(t, http.StatusMethodNotAllowed, recorder.Code)
			require.Equal(t, http.MethodGet, recorder.Header().Get("Allow"))
			require.Equal(t, "Method Not Allowed\n", recorder.Body.String())
			require.Equal(t, "text/plain; charset=utf-8", recorder.Header().Get("Content-Type"))
			require.Equal(t, "nosniff", recorder.Header().Get("X-Content-Type-Options"))
		})
	}
}

func TestHTTPHealthSerializableQueryUsesFirstExactValue(t *testing.T) {
	ctrl := gomock.NewController(t)
	m := mock.NewMinimalMetrics(ctrl)
	kv := &healthStorage{KvStorage: imemkv.NewKvStorage()}
	b := backend.NewBackend(kv, backend.Config{Prefix: "/registry", Identity: "health-query-test"}, m)
	defer func() { require.NoError(t, b.(interface{ Close() error }).Close()) }()
	s := &server{
		healthServer:   health.NewServer(),
		leaderElection: &leader.Stub{},
		backend:        b,
	}

	for _, test := range []struct {
		name   string
		target string
		status int
	}{
		{
			name:   "first true enables serializable",
			target: "/health?serializable=true&serializable=false",
			status: http.StatusOK,
		},
		{
			name:   "later true is ignored",
			target: "/health?serializable=false&serializable=true",
			status: http.StatusServiceUnavailable,
		},
		{
			name:   "uppercase true is not accepted",
			target: "/health?serializable=TRUE",
			status: http.StatusServiceUnavailable,
		},
		{
			name:   "whitespace is not trimmed",
			target: "/health?serializable=%20true%20",
			status: http.StatusServiceUnavailable,
		},
		{
			name:   "empty first value is authoritative",
			target: "/health?serializable=&serializable=true",
			status: http.StatusServiceUnavailable,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			recorder := httptest.NewRecorder()
			s.httpHealthHandler(recorder, httptest.NewRequest(http.MethodGet, test.target, nil))
			require.Equal(t, test.status, recorder.Code)
			if test.status == http.StatusOK {
				require.JSONEq(t, HealthResponse, recorder.Body.String())
			} else {
				require.JSONEq(t, `{"health":"false","reason":"RAFT NO LEADER"}`, recorder.Body.String())
			}
		})
	}
}

func TestClientHTTPHandlersExposeEtcdCORSOptions(t *testing.T) {
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
	s.healthServer.SetServingStatus("", healthpb.HealthCheckResponse_SERVING)

	healthHandler := s.GetClientHttpHandlers()["/health"]
	require.NotNil(t, healthHandler)
	options := httptest.NewRecorder()
	healthHandler.ServeHTTP(options, httptest.NewRequest(http.MethodOptions, "/health", nil))
	require.Equal(t, http.StatusOK, options.Code)
	require.Empty(t, options.Body.String())
	require.Equal(t, "POST, GET, OPTIONS, PUT, DELETE", options.Header().Get("Access-Control-Allow-Methods"))
	require.Equal(t, "*", options.Header().Get("Access-Control-Allow-Origin"))
	require.Equal(t, "accept, content-type, authorization", options.Header().Get("Access-Control-Allow-Headers"))

	get := httptest.NewRecorder()
	healthHandler.ServeHTTP(get, httptest.NewRequest(http.MethodGet, "/health?serializable=true", nil))
	require.Equal(t, http.StatusOK, get.Code)
	require.JSONEq(t, HealthResponse, get.Body.String())
	require.Equal(t, "*", get.Header().Get("Access-Control-Allow-Origin"))
}

func TestInfoHTTPVersionHandlerExposeEtcdCORSOptions(t *testing.T) {
	versionHandler := (&server{}).GetInfoHttpHandlers()["/version"]
	require.NotNil(t, versionHandler)

	options := httptest.NewRecorder()
	versionHandler.ServeHTTP(options, httptest.NewRequest(http.MethodOptions, "/version", nil))
	require.Equal(t, http.StatusOK, options.Code)
	require.Empty(t, options.Body.String())
	require.Equal(t, "POST, GET, OPTIONS, PUT, DELETE", options.Header().Get("Access-Control-Allow-Methods"))
	require.Equal(t, "*", options.Header().Get("Access-Control-Allow-Origin"))
	require.Equal(t, "accept, content-type, authorization", options.Header().Get("Access-Control-Allow-Headers"))

	get := httptest.NewRecorder()
	versionHandler.ServeHTTP(get, httptest.NewRequest(http.MethodGet, "/version", nil))
	require.Equal(t, http.StatusOK, get.Code)
	require.Contains(t, get.Body.String(), `"etcdserver"`)
	require.Equal(t, "*", get.Header().Get("Access-Control-Allow-Origin"))
}

func TestHTTPHealthMatchesEtcdNoSpaceAlarmSemantics(t *testing.T) {
	ctrl := gomock.NewController(t)
	m := mock.NewMinimalMetrics(ctrl)
	kv := imemkv.NewKvStorage()
	b := backend.NewBackend(kv, backend.Config{
		Prefix:            "/registry",
		Identity:          "health-quota-test",
		QuotaBackendBytes: 1,
	}, m)
	require.NoError(t, b.EnsureQuotaInitialized(context.Background()))
	defer func() { require.NoError(t, b.(interface{ Close() error }).Close()) }()
	s := &server{
		healthServer:   health.NewServer(),
		leaderElection: &leader.Stub{ElectionInfo: leader.ElectionInfo{IsLeader: true}},
		backend:        b,
	}
	s.healthServer.SetServingStatus("", healthpb.HealthCheckResponse_SERVING)
	_, err := b.ArmNoSpace(context.Background(), 42)
	require.NoError(t, err)

	for _, target := range []string{"/health", "/health?serializable=true", "/health?exclude=CORRUPT"} {
		recorder := httptest.NewRecorder()
		s.httpHealthHandler(recorder, httptest.NewRequest(http.MethodGet, target, nil))
		require.Equal(t, http.StatusServiceUnavailable, recorder.Code, target)
		require.JSONEq(t, `{"health":"false","reason":"ALARM NOSPACE"}`, recorder.Body.String(), target)
		require.Equal(t, "text/plain; charset=utf-8", recorder.Header().Get("Content-Type"), target)
		require.Equal(t, "nosniff", recorder.Header().Get("X-Content-Type-Options"), target)
	}

	recorder := httptest.NewRecorder()
	s.httpHealthHandler(recorder, httptest.NewRequest(http.MethodGet, "/health?exclude=NOSPACE", nil))
	require.Equal(t, http.StatusOK, recorder.Code)
	require.JSONEq(t, HealthResponse, recorder.Body.String())
	require.Equal(t, "text/plain; charset=utf-8", recorder.Header().Get("Content-Type"))
	require.Empty(t, recorder.Header().Get("X-Content-Type-Options"))

	recorder = httptest.NewRecorder()
	s.httpReadyHandler(recorder, httptest.NewRequest(http.MethodGet, "/ready", nil))
	require.Equal(t, http.StatusOK, recorder.Code, "NOSPACE must not withdraw serving readiness")
	require.JSONEq(t, HealthResponse, recorder.Body.String())

	s.leaderElection = &leader.Stub{}
	recorder = httptest.NewRecorder()
	s.httpHealthHandler(recorder, httptest.NewRequest(http.MethodGet, "/health", nil))
	require.Equal(t, http.StatusServiceUnavailable, recorder.Code)
	require.JSONEq(t, `{"health":"false","reason":"ALARM NOSPACE"}`, recorder.Body.String(),
		"etcd checks alarms before leader availability")
}

func TestHTTPHealthAndReadyzExposeCorruptAlarm(t *testing.T) {
	ctrl := gomock.NewController(t)
	m := mock.NewMinimalMetrics(ctrl)
	kv := imemkv.NewKvStorage()
	b := backend.NewBackend(kv, backend.Config{Prefix: "/registry", Identity: "health-corrupt-test"}, m)
	defer func() { require.NoError(t, b.(interface{ Close() error }).Close()) }()
	s := &server{
		healthServer:   health.NewServer(),
		leaderElection: &leader.Stub{ElectionInfo: leader.ElectionInfo{IsLeader: true}},
		backend:        b,
	}
	s.healthServer.SetServingStatus("", healthpb.HealthCheckResponse_SERVING)
	require.NoError(t, b.ArmCorrupt(context.Background(), 42))

	for _, target := range []string{"/health", "/health?serializable=true", "/health?exclude=NOSPACE"} {
		recorder := httptest.NewRecorder()
		s.httpHealthHandler(recorder, httptest.NewRequest(http.MethodGet, target, nil))
		require.Equal(t, http.StatusServiceUnavailable, recorder.Code, target)
		require.JSONEq(t, `{"health":"false","reason":"ALARM CORRUPT"}`, recorder.Body.String(), target)
	}
	recorder := httptest.NewRecorder()
	s.httpHealthHandler(recorder, httptest.NewRequest(http.MethodGet, "/health?exclude=CORRUPT", nil))
	require.Equal(t, http.StatusOK, recorder.Code)

	handlers := s.GetClientHttpHandlers()
	recorder = httptest.NewRecorder()
	handlers["/readyz/data_corruption"].ServeHTTP(recorder,
		httptest.NewRequest(http.MethodGet, "/readyz/data_corruption?verbose", nil))
	require.Equal(t, http.StatusServiceUnavailable, recorder.Code)
	require.Equal(t, "[-]data_corruption failed: alarm activated: CORRUPT\n\n", recorder.Body.String())
	require.Equal(t, "text/plain; charset=utf-8", recorder.Header().Get("Content-Type"))
	require.Equal(t, "nosniff", recorder.Header().Get("X-Content-Type-Options"))
	recorder = httptest.NewRecorder()
	handlers["/readyz/data_corruption"].ServeHTTP(recorder,
		httptest.NewRequest(http.MethodGet, "/readyz/data_corruption?exclude=data_corruption", nil))
	require.Equal(t, http.StatusServiceUnavailable, recorder.Code)
	require.Equal(t, "[-]data_corruption failed: alarm activated: CORRUPT\n\n", recorder.Body.String())
	recorder = httptest.NewRecorder()
	handlers["/readyz"].ServeHTTP(recorder,
		httptest.NewRequest(http.MethodGet, "/readyz?exclude=data_corruption", nil))
	require.Equal(t, http.StatusOK, recorder.Code)
}

func TestHTTPHealthExposesAndExcludesUnknownAlarm(t *testing.T) {
	ctrl := gomock.NewController(t)
	m := mock.NewMinimalMetrics(ctrl)
	kv := imemkv.NewKvStorage()
	b := backend.NewBackend(kv, backend.Config{Prefix: "/registry", Identity: "health-unknown-test"}, m)
	defer func() { require.NoError(t, b.(interface{ Close() error }).Close()) }()
	s := &server{
		healthServer:   health.NewServer(),
		leaderElection: &leader.Stub{ElectionInfo: leader.ElectionInfo{IsLeader: true}},
		backend:        b,
		genericAlarms: func(context.Context) ([]*etcdserverpb.AlarmMember, error) {
			return []*etcdserverpb.AlarmMember{{MemberID: 7, Alarm: etcdserverpb.AlarmType(127)}}, nil
		},
	}
	for _, target := range []string{"/health", "/health?serializable=true", "/health?exclude=UNKNOWN"} {
		recorder := httptest.NewRecorder()
		s.httpHealthHandler(recorder, httptest.NewRequest(http.MethodGet, target, nil))
		require.Equal(t, http.StatusServiceUnavailable, recorder.Code, target)
		require.JSONEq(t, `{"health":"false","reason":"ALARM UNKNOWN"}`, recorder.Body.String(), target)
	}
	recorder := httptest.NewRecorder()
	s.httpHealthHandler(recorder, httptest.NewRequest(http.MethodGet, "/health?exclude=127", nil))
	require.Equal(t, http.StatusOK, recorder.Code)
	require.JSONEq(t, HealthResponse, recorder.Body.String())
}

func TestHTTPHealthPropagatesGenericAlarmReadFailure(t *testing.T) {
	ctrl := gomock.NewController(t)
	m := mock.NewMinimalMetrics(ctrl)
	kv := imemkv.NewKvStorage()
	b := backend.NewBackend(kv, backend.Config{Prefix: "/registry", Identity: "health-unknown-error-test"}, m)
	defer func() { require.NoError(t, b.(interface{ Close() error }).Close()) }()
	wantErr := errors.New("generic alarm metadata unavailable")
	s := &server{
		healthServer:   health.NewServer(),
		leaderElection: &leader.Stub{ElectionInfo: leader.ElectionInfo{IsLeader: true}},
		backend:        b,
		genericAlarms: func(context.Context) ([]*etcdserverpb.AlarmMember, error) {
			return nil, wantErr
		},
	}
	recorder := httptest.NewRecorder()
	s.httpHealthHandler(recorder, httptest.NewRequest(http.MethodGet, "/health", nil))
	require.Equal(t, http.StatusServiceUnavailable, recorder.Code)
	require.JSONEq(t,
		`{"health":"false","reason":"ALARM ERROR:generic alarm metadata unavailable"}`,
		recorder.Body.String(),
	)
}

func TestHTTPHealthExcludeCollectsExactNonEmptyAlarmSet(t *testing.T) {
	ctrl := gomock.NewController(t)
	m := mock.NewMinimalMetrics(ctrl)
	kv := imemkv.NewKvStorage()
	b := backend.NewBackend(kv, backend.Config{
		Prefix:            "/registry",
		Identity:          "health-exclude-test",
		QuotaBackendBytes: 1,
	}, m)
	require.NoError(t, b.EnsureQuotaInitialized(context.Background()))
	defer func() { require.NoError(t, b.(interface{ Close() error }).Close()) }()
	require.NoError(t, b.ArmCorrupt(context.Background(), 41))
	_, err := b.ArmNoSpace(context.Background(), 42)
	require.NoError(t, err)
	s := &server{
		healthServer:   health.NewServer(),
		leaderElection: &leader.Stub{ElectionInfo: leader.ElectionInfo{IsLeader: true}},
		backend:        b,
	}

	for _, test := range []struct {
		name   string
		target string
		status int
		reason string
	}{
		{
			name:   "excluding nospace still reports corrupt",
			target: "/health?exclude=NOSPACE",
			status: http.StatusServiceUnavailable,
			reason: healthCorruptReason,
		},
		{
			name:   "excluding corrupt still reports nospace",
			target: "/health?exclude=CORRUPT",
			status: http.StatusServiceUnavailable,
			reason: healthNoSpaceReason,
		},
		{
			name:   "all alarms excluded",
			target: "/health?exclude=NOSPACE&exclude=CORRUPT",
			status: http.StatusOK,
		},
		{
			name:   "repeated alarm names form a set",
			target: "/health?exclude=CORRUPT&exclude=NOSPACE&exclude=CORRUPT&exclude=NOSPACE",
			status: http.StatusOK,
		},
		{
			name:   "empty and unknown values do not affect exact exclusions",
			target: "/health?exclude=&exclude=UNKNOWN&exclude=NOSPACE&exclude=CORRUPT",
			status: http.StatusOK,
		},
		{
			name:   "alarm names are case sensitive",
			target: "/health?exclude=nospace&exclude=corrupt",
			status: http.StatusServiceUnavailable,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			recorder := httptest.NewRecorder()
			s.httpHealthHandler(recorder, httptest.NewRequest(http.MethodGet, test.target, nil))
			require.Equal(t, test.status, recorder.Code)
			if test.status == http.StatusOK {
				require.JSONEq(t, HealthResponse, recorder.Body.String())
			} else if test.reason != "" {
				require.JSONEq(t,
					`{"health":"false","reason":"`+test.reason+`"}`,
					recorder.Body.String(),
				)
			} else {
				require.Contains(t, recorder.Body.String(), `"health":"false"`)
			}
		})
	}
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
	require.Equal(t, "[-]serializable_read failed: storage unavailable\n\n", recorder.Body.String())
	require.Equal(t, "text/plain; charset=utf-8", recorder.Header().Get("Content-Type"))
	require.Equal(t, "nosniff", recorder.Header().Get("X-Content-Type-Options"))

	recorder = httptest.NewRecorder()
	handlers["/livez"].ServeHTTP(recorder,
		httptest.NewRequest(http.MethodGet, "/livez?exclude=serializable_read", nil))
	require.Equal(t, http.StatusOK, recorder.Code)
	require.Equal(t, "ok\n", recorder.Body.String())

	recorder = httptest.NewRecorder()
	handlers["/livez/serializable_read"].ServeHTTP(recorder,
		httptest.NewRequest(http.MethodGet, "/livez/serializable_read?exclude=serializable_read", nil))
	require.Equal(t, http.StatusServiceUnavailable, recorder.Code)
	require.Equal(t, "[-]serializable_read failed: storage unavailable\n\n", recorder.Body.String())

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

func TestEtcdHealthCheckMetrics(t *testing.T) {
	ctrl := gomock.NewController(t)
	m := mock.NewMinimalMetrics(ctrl)
	kv := &healthStorage{KvStorage: imemkv.NewKvStorage()}
	b := backend.NewBackend(kv, backend.Config{Prefix: "/registry", Identity: "test"}, m)
	defer func() { require.NoError(t, b.(interface{ Close() error }).Close()) }()
	recorder := &healthMetricRecorder{}
	s := &server{
		healthServer:   health.NewServer(),
		leaderElection: &leader.Stub{},
		backend:        b,
		metricCli:      recorder,
	}
	s.healthServer.SetServingStatus("", healthpb.HealthCheckResponse_NOT_SERVING)
	handlers := s.GetClientHttpHandlers()

	response := httptest.NewRecorder()
	handlers["/readyz"].ServeHTTP(response, httptest.NewRequest(
		http.MethodGet,
		"/readyz?exclude=data_corruption&exclude=non_learner",
		nil,
	))
	require.Equal(t, http.StatusServiceUnavailable, response.Code)
	require.Equal(t, []healthMetricEvent{
		{
			kind: "gauge", name: "etcd.server.healthcheck", value: 1,
			tags: []metrics.T{metrics.Tag("type", "readyz"), metrics.Tag("name", "serializable_read")},
		},
		{
			kind: "counter", name: "etcd.server.healthchecks_total", value: 1,
			tags: []metrics.T{
				metrics.Tag("type", "readyz"),
				metrics.Tag("name", "serializable_read"),
				metrics.Tag("status", "success"),
			},
		},
		{
			kind: "gauge", name: "etcd.server.healthcheck", value: 0,
			tags: []metrics.T{metrics.Tag("type", "readyz"), metrics.Tag("name", "linearizable_read")},
		},
		{
			kind: "counter", name: "etcd.server.healthchecks_total", value: 1,
			tags: []metrics.T{
				metrics.Tag("type", "readyz"),
				metrics.Tag("name", "linearizable_read"),
				metrics.Tag("status", "error"),
			},
		},
	}, recorder.events)

	recorder.events = nil
	response = httptest.NewRecorder()
	s.httpHealthHandler(response, httptest.NewRequest(http.MethodGet, "/health", nil))
	require.Equal(t, http.StatusServiceUnavailable, response.Code)
	require.Equal(t, []healthMetricEvent{{
		kind: "counter", name: "etcd.server.health_failures", value: 1,
	}}, recorder.events)

	recorder.events = nil
	response = httptest.NewRecorder()
	s.httpHealthHandler(response, httptest.NewRequest(http.MethodGet, "/health?serializable=true", nil))
	require.Equal(t, http.StatusOK, response.Code)
	require.Equal(t, []healthMetricEvent{{
		kind: "counter", name: "etcd.server.health_success", value: 1,
	}}, recorder.events)
}
