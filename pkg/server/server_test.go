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
	imemkv "github.com/kubewharf/kubebrain/pkg/storage/memkv"
)

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
	defer func() { require.NoError(t, kv.Close()) }()
	b := backend.NewBackend(kv, backend.Config{Prefix: "/registry", Identity: "test", EnableEtcdCompatibility: true}, m)

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
	s := &server{
		healthServer:   health.NewServer(),
		leaderElection: &leader.Stub{ElectionInfo: leader.ElectionInfo{IsLeader: true}},
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
