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

package etcd

import (
	"context"
	"errors"
	"math"
	"net/http"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/authpb"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/mvccpb"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/stats"
	"google.golang.org/grpc/status"

	"github.com/kubewharf/kubebrain/pkg/metrics"
)

type recordedHistogram struct {
	name  string
	value interface{}
	tags  []metrics.T
}

type recordingMetrics struct {
	mu         sync.Mutex
	histograms []recordedHistogram
	counters   []recordedCounter
	gauges     []recordedGauge
}

func (r *recordingMetrics) snapshotCounters() []recordedCounter {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]recordedCounter(nil), r.counters...)
}

type histogramRegistrationRecorder struct {
	recordingMetrics
	registered []string
}

func (r *histogramRegistrationRecorder) RegisterHistogram(name string, _ ...metrics.T) error {
	r.registered = append(r.registered, name)
	return nil
}

type recordedCounter struct {
	name  string
	value interface{}
	tags  []metrics.T
}

type recordedGauge struct {
	name  string
	value interface{}
	tags  []metrics.T
}

func recordedWatchGenerationRecoveryValues(rec *recordingMetrics, outcome string) []interface{} {
	rec.mu.Lock()
	defer rec.mu.Unlock()
	var values []interface{}
	for _, counter := range rec.counters {
		if counter.name == "watch.generation.recovery" && len(counter.tags) == 1 &&
			counter.tags[0] == metrics.Tag("outcome", outcome) {
			values = append(values, counter.value)
		}
	}
	return values
}

func recordedWatcherSlowConsumerOutcomeValues(rec *recordingMetrics, outcome string) []interface{} {
	rec.mu.Lock()
	defer rec.mu.Unlock()
	var values []interface{}
	for _, counter := range rec.counters {
		if counter.name == "watcher_hub.slow_consumer.outcome" && len(counter.tags) == 1 &&
			counter.tags[0] == metrics.Tag("outcome", outcome) {
			values = append(values, counter.value)
		}
	}
	return values
}

func recordedWatchBackendIntegrityValues(rec *recordingMetrics, kind string) []interface{} {
	rec.mu.Lock()
	defer rec.mu.Unlock()
	var values []interface{}
	for _, counter := range rec.counters {
		if counter.name == "watch.backend.integrity_failure" && len(counter.tags) == 1 &&
			counter.tags[0] == metrics.Tag("kind", kind) {
			values = append(values, counter.value)
		}
	}
	return values
}

func recordedSnapshotFailureValues(rec *recordingMetrics, stage string) []interface{} {
	rec.mu.Lock()
	defer rec.mu.Unlock()
	var values []interface{}
	for _, counter := range rec.counters {
		if counter.name == "maintenance.snapshot.failure" && len(counter.tags) == 1 &&
			counter.tags[0] == metrics.Tag("stage", stage) {
			values = append(values, counter.value)
		}
	}
	return values
}

func recordedCounterValues(rec *recordingMetrics, name string) []interface{} {
	rec.mu.Lock()
	defer rec.mu.Unlock()
	var values []interface{}
	for _, counter := range rec.counters {
		if counter.name == name && len(counter.tags) == 0 {
			values = append(values, counter.value)
		}
	}
	return values
}

func recordedGaugeValues(rec *recordingMetrics, name string) []interface{} {
	rec.mu.Lock()
	defer rec.mu.Unlock()
	var values []interface{}
	for _, gauge := range rec.gauges {
		if gauge.name == name && len(gauge.tags) == 0 {
			values = append(values, gauge.value)
		}
	}
	return values
}

func recordedMaintenanceProxyIntegrityValues(rec *recordingMetrics, rpc string) []interface{} {
	rec.mu.Lock()
	defer rec.mu.Unlock()
	var values []interface{}
	for _, counter := range rec.counters {
		if counter.name == "maintenance.proxy.integrity_failure" && len(counter.tags) == 1 &&
			counter.tags[0] == metrics.Tag("rpc", rpc) {
			values = append(values, counter.value)
		}
	}
	return values
}

func recordedKVProxyIntegrityValues(rec *recordingMetrics, rpc string) []interface{} {
	rec.mu.Lock()
	defer rec.mu.Unlock()
	var values []interface{}
	for _, counter := range rec.counters {
		if counter.name == "kv.proxy.integrity_failure" && len(counter.tags) == 1 &&
			counter.tags[0] == metrics.Tag("rpc", rpc) {
			values = append(values, counter.value)
		}
	}
	return values
}

func recordedAuthProxyIntegrityValues(rec *recordingMetrics, action string) []interface{} {
	rec.mu.Lock()
	defer rec.mu.Unlock()
	var values []interface{}
	for _, counter := range rec.counters {
		if counter.name == "auth.proxy.integrity_failure" && len(counter.tags) == 1 &&
			counter.tags[0] == metrics.Tag("action", action) {
			values = append(values, counter.value)
		}
	}
	return values
}

func recordedLeaseProxyIntegrityValues(rec *recordingMetrics, rpc string) []interface{} {
	rec.mu.Lock()
	defer rec.mu.Unlock()
	var values []interface{}
	for _, counter := range rec.counters {
		if counter.name == "lease.proxy.integrity_failure" && len(counter.tags) == 1 &&
			counter.tags[0] == metrics.Tag("rpc", rpc) {
			values = append(values, counter.value)
		}
	}
	return values
}

func recordedClusterProxyIntegrityValues(rec *recordingMetrics) []interface{} {
	rec.mu.Lock()
	defer rec.mu.Unlock()
	var values []interface{}
	for _, counter := range rec.counters {
		if counter.name == "cluster.proxy.integrity_failure" && len(counter.tags) == 1 &&
			counter.tags[0] == metrics.Tag("rpc", clusterProxyRPCMemberList) {
			values = append(values, counter.value)
		}
	}
	return values
}

func TestClusterProxyIntegrityMetricsAndValidation(t *testing.T) {
	rec := &recordingMetrics{}
	initClusterProxyIntegrityMetrics(rec)
	response, err := validateClusterProxyResult[etcdserverpb.MemberListResponse](rec, clusterProxyRPCMemberList, nil, nil)
	require.Nil(t, response)
	require.Equal(t, codes.DataLoss, status.Code(err))
	response, err = validateClusterProxyResult(rec, clusterProxyRPCMemberList,
		&etcdserverpb.MemberListResponse{}, errors.New("mixed"))
	require.Nil(t, response)
	require.Equal(t, codes.DataLoss, status.Code(err))

	response, err = validateClusterProxyResult(rec, clusterProxyRPCMemberList,
		&etcdserverpb.MemberListResponse{}, nil)
	require.Nil(t, response)
	require.Equal(t, codes.DataLoss, status.Code(err))
	require.ErrorContains(t, err, "without a header")
	response, err = validateClusterProxyResult(rec, clusterProxyRPCMemberList,
		&etcdserverpb.MemberListResponse{Header: txnHeader(-1)}, nil)
	require.Nil(t, response)
	require.Equal(t, codes.DataLoss, status.Code(err))
	require.ErrorContains(t, err, "negative header revision")
	require.Equal(t, []interface{}{int64(0), 1, 1, 1, 1}, recordedClusterProxyIntegrityValues(rec))

	want := &etcdserverpb.MemberListResponse{Header: &etcdserverpb.ResponseHeader{}}
	response, err = validateClusterProxyResult(rec, clusterProxyRPCMemberList, want, nil)
	require.Same(t, want, response)
	require.NoError(t, err)
	wantErr := errors.New("transport failed")
	response, err = validateClusterProxyResult[etcdserverpb.MemberListResponse](rec, clusterProxyRPCMemberList, nil, wantErr)
	require.Nil(t, response)
	require.ErrorIs(t, err, wantErr)
	require.Equal(t, []interface{}{int64(0), 1, 1, 1, 1}, recordedClusterProxyIntegrityValues(rec))
}

func TestMemberListProxyPayloadValidation(t *testing.T) {
	for _, tt := range []struct {
		name    string
		members []*etcdserverpb.Member
		valid   bool
	}{
		{name: "empty", valid: true},
		{name: "strict ID order", members: []*etcdserverpb.Member{{ID: 0}, {ID: 2, Name: "started"}}, valid: true},
		{name: "nil member", members: []*etcdserverpb.Member{nil}},
		{name: "duplicate ID", members: []*etcdserverpb.Member{{ID: 1}, {ID: 1}}},
		{name: "descending ID", members: []*etcdserverpb.Member{{ID: 2}, {ID: 1}}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			rec := &recordingMetrics{}
			want := &etcdserverpb.MemberListResponse{Header: txnHeader(1), Members: tt.members}
			response, err := validateMemberListProxyPayload(rec, want, nil)
			if tt.valid {
				require.Same(t, want, response)
				require.NoError(t, err)
				require.Empty(t, recordedClusterProxyIntegrityValues(rec))
				return
			}
			require.Nil(t, response)
			require.Equal(t, codes.DataLoss, status.Code(err))
			require.Equal(t, []interface{}{1}, recordedClusterProxyIntegrityValues(rec))
		})
	}

	wantErr := errors.New("transport failed")
	want := &etcdserverpb.MemberListResponse{}
	response, err := validateMemberListProxyPayload(nil, want, wantErr)
	require.Same(t, want, response)
	require.ErrorIs(t, err, wantErr)
}

func TestLeaseProxyIntegrityMetricsAndValidation(t *testing.T) {
	rec := &recordingMetrics{}
	initLeaseProxyIntegrityMetrics(rec)
	for _, rpc := range leaseProxyRPCs {
		response, err := validateLeaseProxyResult[etcdserverpb.LeaseGrantResponse](rec, rpc, nil, nil)
		require.Nil(t, response)
		require.Equal(t, codes.DataLoss, status.Code(err))

		response, err = validateLeaseProxyResult(rec, rpc, &etcdserverpb.LeaseGrantResponse{}, errors.New("mixed"))
		require.Nil(t, response)
		require.Equal(t, codes.DataLoss, status.Code(err))

		response, err = validateLeaseProxyResult(rec, rpc, &etcdserverpb.LeaseGrantResponse{}, nil)
		require.Nil(t, response)
		require.Equal(t, codes.DataLoss, status.Code(err))
		require.ErrorContains(t, err, "without a header")
		response, err = validateLeaseProxyResult(rec, rpc, &etcdserverpb.LeaseGrantResponse{Header: txnHeader(-1)}, nil)
		require.Nil(t, response)
		require.Equal(t, codes.DataLoss, status.Code(err))
		require.ErrorContains(t, err, "negative header revision")
		require.Equal(t, []interface{}{int64(0), 1, 1, 1, 1}, recordedLeaseProxyIntegrityValues(rec, rpc))
	}

	want := &etcdserverpb.LeaseGrantResponse{Header: &etcdserverpb.ResponseHeader{}}
	response, err := validateLeaseProxyResult(rec, leaseProxyRPCGrant, want, nil)
	require.Same(t, want, response)
	require.NoError(t, err)
	wantErr := errors.New("transport failed")
	response, err = validateLeaseProxyResult[etcdserverpb.LeaseGrantResponse](rec, leaseProxyRPCGrant, nil, wantErr)
	require.Nil(t, response)
	require.ErrorIs(t, err, wantErr)
	require.Equal(t, []interface{}{int64(0), 1, 1, 1, 1}, recordedLeaseProxyIntegrityValues(rec, leaseProxyRPCGrant))
}

func TestLeaseProxyResponseIDValidation(t *testing.T) {
	rec := &recordingMetrics{}
	initLeaseProxyIntegrityMetrics(rec)

	grant := &etcdserverpb.LeaseGrantResponse{Header: txnHeader(1), ID: math.MinInt64}
	response, err := validateLeaseProxyResponseID(rec, leaseProxyRPCGrant, int64(math.MinInt64), grant, nil)
	require.Same(t, grant, response)
	require.NoError(t, err)

	response, err = validateLeaseProxyResponseID(rec, leaseProxyRPCGrant, -1, grant, nil)
	require.Nil(t, response)
	require.Equal(t, codes.DataLoss, status.Code(err))
	require.ErrorContains(t, err, "lease ID -9223372036854775808 for request ID -1")
	require.Equal(t, []interface{}{int64(0), 1}, recordedLeaseProxyIntegrityValues(rec, leaseProxyRPCGrant))

	wantErr := errors.New("transport failed")
	response, err = validateLeaseProxyResponseID(rec, leaseProxyRPCGrant, -1, (*etcdserverpb.LeaseGrantResponse)(nil), wantErr)
	require.Nil(t, response)
	require.ErrorIs(t, err, wantErr)
	require.Equal(t, []interface{}{int64(0), 1}, recordedLeaseProxyIntegrityValues(rec, leaseProxyRPCGrant))
}

func TestLeaseProxyPayloadValidation(t *testing.T) {
	t.Run("grant", func(t *testing.T) {
		for _, test := range []struct {
			name     string
			request  *etcdserverpb.LeaseGrantRequest
			response *etcdserverpb.LeaseGrantResponse
			valid    bool
		}{
			{name: "exact TTL", request: &etcdserverpb.LeaseGrantRequest{TTL: 5}, response: &etcdserverpb.LeaseGrantResponse{TTL: 5}, valid: true},
			{name: "server-chosen longer TTL", request: &etcdserverpb.LeaseGrantRequest{TTL: 1}, response: &etcdserverpb.LeaseGrantResponse{TTL: 5}, valid: true},
			{name: "negative request raised to minimum", request: &etcdserverpb.LeaseGrantRequest{TTL: -1}, response: &etcdserverpb.LeaseGrantResponse{TTL: 5}, valid: true},
			{name: "non-positive TTL", request: &etcdserverpb.LeaseGrantRequest{TTL: 1}, response: &etcdserverpb.LeaseGrantResponse{}},
			{name: "TTL below request", request: &etcdserverpb.LeaseGrantRequest{TTL: 5}, response: &etcdserverpb.LeaseGrantResponse{TTL: 4}},
			{name: "legacy error", request: &etcdserverpb.LeaseGrantRequest{TTL: 5}, response: &etcdserverpb.LeaseGrantResponse{TTL: 5, Error: "failed"}},
		} {
			t.Run(test.name, func(t *testing.T) {
				rec := &recordingMetrics{}
				response, err := validateLeaseGrantProxyPayload(rec, test.request, test.response, nil)
				if test.valid {
					require.Same(t, test.response, response)
					require.NoError(t, err)
					require.Empty(t, recordedLeaseProxyIntegrityValues(rec, leaseProxyRPCGrant))
					return
				}
				require.Nil(t, response)
				require.Equal(t, codes.DataLoss, status.Code(err))
				require.Equal(t, []interface{}{1}, recordedLeaseProxyIntegrityValues(rec, leaseProxyRPCGrant))
			})
		}

		wantErr := errors.New("transport failed")
		want := &etcdserverpb.LeaseGrantResponse{}
		response, err := validateLeaseGrantProxyPayload(nil, &etcdserverpb.LeaseGrantRequest{}, want, wantErr)
		require.Same(t, want, response)
		require.ErrorIs(t, err, wantErr)
	})

	t.Run("keep alive", func(t *testing.T) {
		rec := &recordingMetrics{}
		initLeaseProxyIntegrityMetrics(rec)
		valid := &etcdserverpb.LeaseKeepAliveResponse{TTL: 0}
		response, err := validateLeaseKeepAliveProxyPayload(rec, valid, nil)
		require.Same(t, valid, response)
		require.NoError(t, err)
		response, err = validateLeaseKeepAliveProxyPayload(rec, &etcdserverpb.LeaseKeepAliveResponse{TTL: -1}, nil)
		require.Nil(t, response)
		require.Equal(t, codes.DataLoss, status.Code(err))
		require.Equal(t, []interface{}{int64(0), 1}, recordedLeaseProxyIntegrityValues(rec, leaseProxyRPCKeepAlive))
	})

	t.Run("time to live", func(t *testing.T) {
		for _, test := range []struct {
			name          string
			keysRequested bool
			response      *etcdserverpb.LeaseTimeToLiveResponse
			valid         bool
		}{
			{name: "found", keysRequested: true, response: &etcdserverpb.LeaseTimeToLiveResponse{TTL: 0, GrantedTTL: 1, Keys: [][]byte{[]byte("key")}}, valid: true},
			{name: "expired found", keysRequested: true, response: &etcdserverpb.LeaseTimeToLiveResponse{TTL: -2, GrantedTTL: 1, Keys: [][]byte{[]byte("key")}}, valid: true},
			{name: "minus one found", response: &etcdserverpb.LeaseTimeToLiveResponse{TTL: -1, GrantedTTL: 1}, valid: true},
			{name: "not found", response: &etcdserverpb.LeaseTimeToLiveResponse{TTL: -1}, valid: true},
			{name: "keys not requested", response: &etcdserverpb.LeaseTimeToLiveResponse{TTL: 1, GrantedTTL: 1, Keys: [][]byte{[]byte("key")}}},
			{name: "negative without granted ttl", keysRequested: true, response: &etcdserverpb.LeaseTimeToLiveResponse{TTL: -2}},
			{name: "found without granted ttl", keysRequested: true, response: &etcdserverpb.LeaseTimeToLiveResponse{TTL: 0}},
			{name: "malformed not found", keysRequested: true, response: &etcdserverpb.LeaseTimeToLiveResponse{TTL: -1, Keys: [][]byte{[]byte("stale")}}},
			{name: "empty key", keysRequested: true, response: &etcdserverpb.LeaseTimeToLiveResponse{TTL: 1, GrantedTTL: 1, Keys: [][]byte{{}}}},
			{name: "duplicate key", keysRequested: true, response: &etcdserverpb.LeaseTimeToLiveResponse{TTL: 1, GrantedTTL: 1, Keys: [][]byte{[]byte("key"), []byte("key")}}},
		} {
			t.Run(test.name, func(t *testing.T) {
				rec := &recordingMetrics{}
				initLeaseProxyIntegrityMetrics(rec)
				response, err := validateLeaseTimeToLiveProxyPayload(rec, test.keysRequested, test.response, nil)
				if test.valid {
					require.Same(t, test.response, response)
					require.NoError(t, err)
					require.Equal(t, []interface{}{int64(0)}, recordedLeaseProxyIntegrityValues(rec, leaseProxyRPCTimeToLive))
				} else {
					require.Nil(t, response)
					require.Equal(t, codes.DataLoss, status.Code(err))
					require.Equal(t, []interface{}{int64(0), 1}, recordedLeaseProxyIntegrityValues(rec, leaseProxyRPCTimeToLive))
				}
			})
		}
	})

	t.Run("leases", func(t *testing.T) {
		for _, test := range []struct {
			name     string
			response *etcdserverpb.LeaseLeasesResponse
			valid    bool
		}{
			{name: "empty", response: &etcdserverpb.LeaseLeasesResponse{}, valid: true},
			{name: "signed ids", response: &etcdserverpb.LeaseLeasesResponse{Leases: []*etcdserverpb.LeaseStatus{{ID: math.MinInt64}, {ID: math.MaxInt64}}}, valid: true},
			{name: "nil status", response: &etcdserverpb.LeaseLeasesResponse{Leases: []*etcdserverpb.LeaseStatus{nil}}},
			{name: "zero id", response: &etcdserverpb.LeaseLeasesResponse{Leases: []*etcdserverpb.LeaseStatus{{ID: 0}}}},
			{name: "duplicate id", response: &etcdserverpb.LeaseLeasesResponse{Leases: []*etcdserverpb.LeaseStatus{{ID: -1}, {ID: -1}}}},
		} {
			t.Run(test.name, func(t *testing.T) {
				rec := &recordingMetrics{}
				initLeaseProxyIntegrityMetrics(rec)
				response, err := validateLeaseLeasesProxyPayload(rec, test.response, nil)
				if test.valid {
					require.Same(t, test.response, response)
					require.NoError(t, err)
					require.Equal(t, []interface{}{int64(0)}, recordedLeaseProxyIntegrityValues(rec, leaseProxyRPCLeases))
				} else {
					require.Nil(t, response)
					require.Equal(t, codes.DataLoss, status.Code(err))
					require.Equal(t, []interface{}{int64(0), 1}, recordedLeaseProxyIntegrityValues(rec, leaseProxyRPCLeases))
				}
			})
		}
	})
}

func TestAuthProxyIntegrityMetricsAndValidation(t *testing.T) {
	rec := &recordingMetrics{}
	initAuthProxyIntegrityMetrics(rec)
	for _, action := range authProxyActions {
		response, err := validateAuthProxyResult[etcdserverpb.AuthStatusResponse](rec, action, nil, nil)
		require.Nil(t, response)
		require.Equal(t, codes.DataLoss, status.Code(err))

		response, err = validateAuthProxyResult(rec, action, &etcdserverpb.AuthStatusResponse{}, errors.New("mixed"))
		require.Nil(t, response)
		require.Equal(t, codes.DataLoss, status.Code(err))

		response, err = validateAuthProxyResult(rec, action, &etcdserverpb.AuthStatusResponse{}, nil)
		require.Nil(t, response)
		require.Equal(t, codes.DataLoss, status.Code(err))
		require.ErrorContains(t, err, "without a header")
		response, err = validateAuthProxyResult(rec, action, &etcdserverpb.AuthStatusResponse{Header: txnHeader(-1)}, nil)
		require.Nil(t, response)
		require.Equal(t, codes.DataLoss, status.Code(err))
		require.ErrorContains(t, err, "negative header revision")
		require.Equal(t, []interface{}{int64(0), 1, 1, 1, 1}, recordedAuthProxyIntegrityValues(rec, action))
	}

	want := &etcdserverpb.AuthStatusResponse{Header: &etcdserverpb.ResponseHeader{}}
	response, err := validateAuthProxyResult(rec, authProxyActionStatus, want, nil)
	require.Same(t, want, response)
	require.NoError(t, err)
	wantErr := errors.New("transport failed")
	response, err = validateAuthProxyResult[etcdserverpb.AuthStatusResponse](rec, authProxyActionStatus, nil, wantErr)
	require.Nil(t, response)
	require.ErrorIs(t, err, wantErr)
	require.Equal(t, []interface{}{int64(0), 1, 1, 1, 1}, recordedAuthProxyIntegrityValues(rec, authProxyActionStatus))
}

func TestAuthNameListProxyPayloadValidation(t *testing.T) {
	for _, tt := range []struct {
		name   string
		action string
		names  []string
		valid  bool
	}{
		{name: "empty list", action: authProxyActionUserList, valid: true},
		{name: "sorted user roles", action: authProxyActionUserGet, names: []string{"reader", "writer"}, valid: true},
		{name: "sorted users", action: authProxyActionUserList, names: []string{"alice", "root"}, valid: true},
		{name: "sorted roles", action: authProxyActionRoleList, names: []string{"reader", "root", "writer"}, valid: true},
		{name: "empty user", action: authProxyActionUserList, names: []string{""}},
		{name: "empty user role", action: authProxyActionUserGet, names: []string{""}},
		{name: "duplicate user role", action: authProxyActionUserGet, names: []string{"reader", "reader"}},
		{name: "unsorted user roles", action: authProxyActionUserGet, names: []string{"writer", "reader"}},
		{name: "duplicate role", action: authProxyActionRoleList, names: []string{"reader", "reader"}},
		{name: "unsorted user", action: authProxyActionUserList, names: []string{"root", "alice"}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			rec := &recordingMetrics{}
			want := &etcdserverpb.AuthUserListResponse{Header: txnHeader(1)}
			response, err := validateAuthNameListProxyPayload(rec, tt.action, tt.names, want, nil)
			if tt.valid {
				require.Same(t, want, response)
				require.NoError(t, err)
				require.Empty(t, recordedAuthProxyIntegrityValues(rec, tt.action))
				return
			}
			require.Nil(t, response)
			require.Equal(t, codes.DataLoss, status.Code(err))
			require.Equal(t, []interface{}{1}, recordedAuthProxyIntegrityValues(rec, tt.action))
		})
	}

	wantErr := errors.New("transport failed")
	want := &etcdserverpb.AuthUserListResponse{}
	response, err := validateAuthNameListProxyPayload(nil, authProxyActionUserList, []string{""}, want, wantErr)
	require.Same(t, want, response)
	require.ErrorIs(t, err, wantErr)
}

func TestAuthRoleGetProxyPayloadValidation(t *testing.T) {
	permission := func(permissionType authpb.Permission_Type, key, end string) *authpb.Permission {
		return &authpb.Permission{PermType: permissionType, Key: []byte(key), RangeEnd: []byte(end)}
	}
	tests := []struct {
		name     string
		request  *etcdserverpb.AuthRoleGetRequest
		response *etcdserverpb.AuthRoleGetResponse
		valid    bool
	}{
		{name: "empty ordinary role", request: &etcdserverpb.AuthRoleGetRequest{Role: "reader"}, response: &etcdserverpb.AuthRoleGetResponse{Header: txnHeader(1)}, valid: true},
		{name: "sorted ordinary permissions", request: &etcdserverpb.AuthRoleGetRequest{Role: "reader"}, response: &etcdserverpb.AuthRoleGetResponse{Header: txnHeader(1), Perm: []*authpb.Permission{permission(authpb.READ, "a", "b"), permission(authpb.WRITE, "b", ""), permission(authpb.READWRITE, "z", "\x00")}}, valid: true},
		{name: "same key ranges", request: &etcdserverpb.AuthRoleGetRequest{Role: "reader"}, response: &etcdserverpb.AuthRoleGetResponse{Header: txnHeader(1), Perm: []*authpb.Permission{permission(authpb.READ, "a", "b"), permission(authpb.WRITE, "a", "c")}}, valid: true},
		// Upstream sorts permissions by key alone. With multiple ranges for one
		// key, granting a non-first range again can append an exact duplicate.
		{name: "upstream duplicate exact permission", request: &etcdserverpb.AuthRoleGetRequest{Role: "reader"}, response: &etcdserverpb.AuthRoleGetResponse{Header: txnHeader(1), Perm: []*authpb.Permission{permission(authpb.READ, "a", "b"), permission(authpb.WRITE, "a", "c"), permission(authpb.WRITE, "a", "c")}}, valid: true},
		{name: "unknown permission type", request: &etcdserverpb.AuthRoleGetRequest{Role: "reader"}, response: &etcdserverpb.AuthRoleGetResponse{Header: txnHeader(1), Perm: []*authpb.Permission{permission(authpb.Permission_Type(99), "a", "")}}, valid: true},
		{name: "canonical root", request: &etcdserverpb.AuthRoleGetRequest{Role: "root"}, response: &etcdserverpb.AuthRoleGetResponse{Header: txnHeader(1), Perm: []*authpb.Permission{{PermType: authpb.READWRITE, RangeEnd: []byte{0}}}}, valid: true},
		{name: "nil permission", request: &etcdserverpb.AuthRoleGetRequest{Role: "reader"}, response: &etcdserverpb.AuthRoleGetResponse{Header: txnHeader(1), Perm: []*authpb.Permission{nil}}},
		{name: "empty key", request: &etcdserverpb.AuthRoleGetRequest{Role: "reader"}, response: &etcdserverpb.AuthRoleGetResponse{Header: txnHeader(1), Perm: []*authpb.Permission{{PermType: authpb.READ}}}},
		{name: "reversed range", request: &etcdserverpb.AuthRoleGetRequest{Role: "reader"}, response: &etcdserverpb.AuthRoleGetResponse{Header: txnHeader(1), Perm: []*authpb.Permission{permission(authpb.READ, "z", "a")}}},
		{name: "unsorted permissions", request: &etcdserverpb.AuthRoleGetRequest{Role: "reader"}, response: &etcdserverpb.AuthRoleGetResponse{Header: txnHeader(1), Perm: []*authpb.Permission{permission(authpb.READ, "b", ""), permission(authpb.READ, "a", "")}}},
		{name: "empty root permissions", request: &etcdserverpb.AuthRoleGetRequest{Role: "root"}, response: &etcdserverpb.AuthRoleGetResponse{Header: txnHeader(1)}},
		{name: "non-canonical root type", request: &etcdserverpb.AuthRoleGetRequest{Role: "root"}, response: &etcdserverpb.AuthRoleGetResponse{Header: txnHeader(1), Perm: []*authpb.Permission{{PermType: authpb.READ, RangeEnd: []byte{0}}}}},
		{name: "non-canonical root key", request: &etcdserverpb.AuthRoleGetRequest{Role: "root"}, response: &etcdserverpb.AuthRoleGetResponse{Header: txnHeader(1), Perm: []*authpb.Permission{{PermType: authpb.READWRITE, Key: []byte{0}, RangeEnd: []byte{0}}}}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rec := &recordingMetrics{}
			response, err := validateAuthRoleGetProxyPayload(rec, tt.request, tt.response, nil)
			if tt.valid {
				require.Same(t, tt.response, response)
				require.NoError(t, err)
				require.Empty(t, recordedAuthProxyIntegrityValues(rec, authProxyActionRoleGet))
				return
			}
			require.Nil(t, response)
			require.Equal(t, codes.DataLoss, status.Code(err))
			require.Equal(t, []interface{}{1}, recordedAuthProxyIntegrityValues(rec, authProxyActionRoleGet))
		})
	}

	wantErr := errors.New("transport failed")
	want := &etcdserverpb.AuthRoleGetResponse{}
	response, err := validateAuthRoleGetProxyPayload(nil, &etcdserverpb.AuthRoleGetRequest{}, want, wantErr)
	require.Same(t, want, response)
	require.ErrorIs(t, err, wantErr)
}

func TestAuthStatusProxyPayloadValidation(t *testing.T) {
	for _, tt := range []struct {
		name     string
		response *etcdserverpb.AuthStatusResponse
		valid    bool
	}{
		{name: "disabled initial revision", response: &etcdserverpb.AuthStatusResponse{Header: txnHeader(1), AuthRevision: 1}, valid: true},
		{name: "enabled revision", response: &etcdserverpb.AuthStatusResponse{Header: txnHeader(1), Enabled: true, AuthRevision: 10}, valid: true},
		{name: "zero revision", response: &etcdserverpb.AuthStatusResponse{Header: txnHeader(1)}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			rec := &recordingMetrics{}
			response, err := validateAuthStatusProxyPayload(rec, tt.response, nil)
			if tt.valid {
				require.Same(t, tt.response, response)
				require.NoError(t, err)
				require.Empty(t, recordedAuthProxyIntegrityValues(rec, authProxyActionStatus))
				return
			}
			require.Nil(t, response)
			require.Equal(t, codes.DataLoss, status.Code(err))
			require.Equal(t, []interface{}{1}, recordedAuthProxyIntegrityValues(rec, authProxyActionStatus))
		})
	}

	wantErr := errors.New("transport failed")
	want := &etcdserverpb.AuthStatusResponse{}
	response, err := validateAuthStatusProxyPayload(nil, want, wantErr)
	require.Same(t, want, response)
	require.ErrorIs(t, err, wantErr)
}

func TestAuthenticateProxyPayloadValidation(t *testing.T) {
	for _, tt := range []struct {
		name     string
		response *etcdserverpb.AuthenticateResponse
		valid    bool
	}{
		{name: "opaque token", response: &etcdserverpb.AuthenticateResponse{Header: txnHeader(1), Token: "opaque"}, valid: true},
		{name: "empty-prefix simple token", response: &etcdserverpb.AuthenticateResponse{Header: txnHeader(1), Token: ".42"}, valid: true},
		{name: "empty token", response: &etcdserverpb.AuthenticateResponse{Header: txnHeader(1)}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			rec := &recordingMetrics{}
			response, err := validateAuthenticateProxyPayload(rec, tt.response, nil)
			if tt.valid {
				require.Same(t, tt.response, response)
				require.NoError(t, err)
				require.Empty(t, recordedAuthProxyIntegrityValues(rec, authProxyActionAuthenticate))
				return
			}
			require.Nil(t, response)
			require.Equal(t, codes.DataLoss, status.Code(err))
			require.Equal(t, []interface{}{1}, recordedAuthProxyIntegrityValues(rec, authProxyActionAuthenticate))
		})
	}

	wantErr := errors.New("transport failed")
	want := &etcdserverpb.AuthenticateResponse{}
	response, err := validateAuthenticateProxyPayload(nil, want, wantErr)
	require.Same(t, want, response)
	require.ErrorIs(t, err, wantErr)
}

func TestKVProxyIntegrityMetricsAndValidation(t *testing.T) {
	rec := &recordingMetrics{}
	initKVProxyIntegrityMetrics(rec)
	for _, rpc := range kvProxyRPCs {
		response, err := validateKVProxyResult[etcdserverpb.RangeResponse](rec, rpc, nil, nil)
		require.Nil(t, response)
		require.Equal(t, codes.DataLoss, status.Code(err))

		response, err = validateKVProxyResult(rec, rpc, &etcdserverpb.RangeResponse{}, errors.New("mixed"))
		require.Nil(t, response)
		require.Equal(t, codes.DataLoss, status.Code(err))

		response, err = validateKVProxyResult(rec, rpc, &etcdserverpb.RangeResponse{}, nil)
		require.Nil(t, response)
		require.Equal(t, codes.DataLoss, status.Code(err))
		require.ErrorContains(t, err, "without a header")
		response, err = validateKVProxyResult(rec, rpc, &etcdserverpb.RangeResponse{Header: txnHeader(-1)}, nil)
		require.Nil(t, response)
		require.Equal(t, codes.DataLoss, status.Code(err))
		require.ErrorContains(t, err, "negative header revision")
		require.Equal(t, []interface{}{int64(0), 1, 1, 1, 1}, recordedKVProxyIntegrityValues(rec, rpc))
	}

	want := &etcdserverpb.RangeResponse{Header: &etcdserverpb.ResponseHeader{}}
	response, err := validateKVProxyResult(rec, kvProxyRPCRange, want, nil)
	require.Same(t, want, response)
	require.NoError(t, err)
	wantErr := errors.New("transport failed")
	response, err = validateKVProxyResult[etcdserverpb.RangeResponse](rec, kvProxyRPCRange, nil, wantErr)
	require.Nil(t, response)
	require.ErrorIs(t, err, wantErr)
	require.Equal(t, []interface{}{int64(0), 1, 1, 1, 1}, recordedKVProxyIntegrityValues(rec, kvProxyRPCRange))
}

func TestRangeProxyPayloadValidation(t *testing.T) {
	tests := []struct {
		name    string
		request *etcdserverpb.RangeRequest
		result  *etcdserverpb.RangeResponse
		valid   bool
	}{
		{name: "exact key with signed lease", request: &etcdserverpb.RangeRequest{Key: []byte("b")}, result: &etcdserverpb.RangeResponse{Header: txnHeader(3), Count: 1, Kvs: []*mvccpb.KeyValue{{Key: []byte("b"), CreateRevision: 2, ModRevision: 3, Version: 2, Lease: math.MinInt64}}}, valid: true},
		{name: "from key", request: &etcdserverpb.RangeRequest{Key: []byte("b"), RangeEnd: []byte{0}}, result: &etcdserverpb.RangeResponse{Header: txnHeader(2), Count: 2, Kvs: []*mvccpb.KeyValue{{Key: []byte("b"), CreateRevision: 1, ModRevision: 1, Version: 1}, {Key: []byte("z"), CreateRevision: 2, ModRevision: 2, Version: 1}}}, valid: true},
		{name: "negative count", request: &etcdserverpb.RangeRequest{Key: []byte("b")}, result: &etcdserverpb.RangeResponse{Header: txnHeader(1), Count: -1}},
		{name: "count below values", request: &etcdserverpb.RangeRequest{Key: []byte("b")}, result: &etcdserverpb.RangeResponse{Header: txnHeader(1), Kvs: []*mvccpb.KeyValue{{Key: []byte("b")}}}},
		{name: "count only values", request: &etcdserverpb.RangeRequest{Key: []byte("b"), CountOnly: true}, result: &etcdserverpb.RangeResponse{Header: txnHeader(1), Count: 1, Kvs: []*mvccpb.KeyValue{{Key: []byte("b")}}}},
		{name: "count only more", request: &etcdserverpb.RangeRequest{Key: []byte("b"), CountOnly: true}, result: &etcdserverpb.RangeResponse{Header: txnHeader(1), More: true}},
		{name: "nil value", request: &etcdserverpb.RangeRequest{Key: []byte("b")}, result: &etcdserverpb.RangeResponse{Header: txnHeader(1), Count: 1, Kvs: []*mvccpb.KeyValue{nil}}},
		{name: "empty key", request: &etcdserverpb.RangeRequest{Key: []byte("b"), RangeEnd: []byte("d")}, result: &etcdserverpb.RangeResponse{Header: txnHeader(1), Count: 1, Kvs: []*mvccpb.KeyValue{{}}}},
		{name: "before range", request: &etcdserverpb.RangeRequest{Key: []byte("b"), RangeEnd: []byte("d")}, result: &etcdserverpb.RangeResponse{Header: txnHeader(1), Count: 1, Kvs: []*mvccpb.KeyValue{{Key: []byte("a")}}}},
		{name: "at range end", request: &etcdserverpb.RangeRequest{Key: []byte("b"), RangeEnd: []byte("d")}, result: &etcdserverpb.RangeResponse{Header: txnHeader(1), Count: 1, Kvs: []*mvccpb.KeyValue{{Key: []byte("d")}}}},
		{name: "duplicate key", request: &etcdserverpb.RangeRequest{Key: []byte("b"), RangeEnd: []byte("d")}, result: &etcdserverpb.RangeResponse{Header: txnHeader(1), Count: 2, Kvs: []*mvccpb.KeyValue{{Key: []byte("b")}, {Key: []byte("b")}}}},
		{name: "keys only value", request: &etcdserverpb.RangeRequest{Key: []byte("b"), KeysOnly: true}, result: &etcdserverpb.RangeResponse{Header: txnHeader(1), Count: 1, Kvs: []*mvccpb.KeyValue{{Key: []byte("b"), Value: []byte("secret")}}}},
		{name: "header below requested revision", request: &etcdserverpb.RangeRequest{Key: []byte("b"), Revision: 3}, result: &etcdserverpb.RangeResponse{Header: txnHeader(2)}},
		{name: "missing metadata", request: &etcdserverpb.RangeRequest{Key: []byte("b")}, result: &etcdserverpb.RangeResponse{Header: txnHeader(2), Count: 1, Kvs: []*mvccpb.KeyValue{{Key: []byte("b")}}}},
		{name: "future create revision", request: &etcdserverpb.RangeRequest{Key: []byte("b")}, result: &etcdserverpb.RangeResponse{Header: txnHeader(3), Count: 1, Kvs: []*mvccpb.KeyValue{{Key: []byte("b"), CreateRevision: 3, ModRevision: 2, Version: 1}}}},
		{name: "version one after create", request: &etcdserverpb.RangeRequest{Key: []byte("b")}, result: &etcdserverpb.RangeResponse{Header: txnHeader(3), Count: 1, Kvs: []*mvccpb.KeyValue{{Key: []byte("b"), CreateRevision: 1, ModRevision: 2, Version: 1}}}},
		{name: "impossible version", request: &etcdserverpb.RangeRequest{Key: []byte("b")}, result: &etcdserverpb.RangeResponse{Header: txnHeader(3), Count: 1, Kvs: []*mvccpb.KeyValue{{Key: []byte("b"), CreateRevision: 2, ModRevision: 3, Version: 3}}}},
		{name: "newer than historical snapshot", request: &etcdserverpb.RangeRequest{Key: []byte("b"), Revision: 2}, result: &etcdserverpb.RangeResponse{Header: txnHeader(3), Count: 1, Kvs: []*mvccpb.KeyValue{{Key: []byte("b"), CreateRevision: 3, ModRevision: 3, Version: 1}}}},
		{name: "below mod filter", request: &etcdserverpb.RangeRequest{Key: []byte("b"), MinModRevision: 3}, result: &etcdserverpb.RangeResponse{Header: txnHeader(3), Count: 1, Kvs: []*mvccpb.KeyValue{{Key: []byte("b"), CreateRevision: 2, ModRevision: 2, Version: 1}}}},
		{name: "above create filter", request: &etcdserverpb.RangeRequest{Key: []byte("b"), MaxCreateRevision: 1}, result: &etcdserverpb.RangeResponse{Header: txnHeader(3), Count: 1, Kvs: []*mvccpb.KeyValue{{Key: []byte("b"), CreateRevision: 2, ModRevision: 2, Version: 1}}}},
		{name: "limited page", request: &etcdserverpb.RangeRequest{Key: []byte("a"), RangeEnd: []byte("z"), Limit: 2}, result: &etcdserverpb.RangeResponse{Header: txnHeader(2), Count: 3, More: true, Kvs: []*mvccpb.KeyValue{{Key: []byte("a"), CreateRevision: 1, ModRevision: 1, Version: 1}, {Key: []byte("b"), CreateRevision: 2, ModRevision: 2, Version: 1}}}, valid: true},
		{name: "descending key", request: &etcdserverpb.RangeRequest{Key: []byte("a"), RangeEnd: []byte("z"), SortTarget: etcdserverpb.RangeRequest_KEY, SortOrder: etcdserverpb.RangeRequest_DESCEND}, result: &etcdserverpb.RangeResponse{Header: txnHeader(2), Count: 2, Kvs: []*mvccpb.KeyValue{{Key: []byte("b"), CreateRevision: 2, ModRevision: 2, Version: 1}, {Key: []byte("a"), CreateRevision: 1, ModRevision: 1, Version: 1}}}, valid: true},
		{name: "none value becomes ascending", request: &etcdserverpb.RangeRequest{Key: []byte("a"), RangeEnd: []byte("z"), SortTarget: etcdserverpb.RangeRequest_VALUE}, result: &etcdserverpb.RangeResponse{Header: txnHeader(2), Count: 2, Kvs: []*mvccpb.KeyValue{{Key: []byte("b"), Value: []byte("a"), CreateRevision: 2, ModRevision: 2, Version: 1}, {Key: []byte("a"), Value: []byte("z"), CreateRevision: 1, ModRevision: 1, Version: 1}}}, valid: true},
		{name: "keys only value order is projected", request: &etcdserverpb.RangeRequest{Key: []byte("a"), RangeEnd: []byte("z"), KeysOnly: true, SortTarget: etcdserverpb.RangeRequest_VALUE, SortOrder: etcdserverpb.RangeRequest_DESCEND}, result: &etcdserverpb.RangeResponse{Header: txnHeader(2), Count: 2, Kvs: []*mvccpb.KeyValue{{Key: []byte("b"), CreateRevision: 2, ModRevision: 2, Version: 1}, {Key: []byte("a"), CreateRevision: 1, ModRevision: 1, Version: 1}}}, valid: true},
		{name: "unlimited more", request: &etcdserverpb.RangeRequest{Key: []byte("a"), RangeEnd: []byte("z")}, result: &etcdserverpb.RangeResponse{Header: txnHeader(2), Count: 2, More: true}},
		{name: "over limit", request: &etcdserverpb.RangeRequest{Key: []byte("a"), RangeEnd: []byte("z"), Limit: 1}, result: &etcdserverpb.RangeResponse{Header: txnHeader(2), Count: 2, More: true, Kvs: []*mvccpb.KeyValue{{Key: []byte("a"), CreateRevision: 1, ModRevision: 1, Version: 1}, {Key: []byte("b"), CreateRevision: 2, ModRevision: 2, Version: 1}}}},
		{name: "non-full page with more", request: &etcdserverpb.RangeRequest{Key: []byte("a"), RangeEnd: []byte("z"), Limit: 2}, result: &etcdserverpb.RangeResponse{Header: txnHeader(2), Count: 2, More: true, Kvs: []*mvccpb.KeyValue{{Key: []byte("a"), CreateRevision: 1, ModRevision: 1, Version: 1}}}},
		{name: "count requires more", request: &etcdserverpb.RangeRequest{Key: []byte("a"), RangeEnd: []byte("z"), Limit: 1}, result: &etcdserverpb.RangeResponse{Header: txnHeader(2), Count: 2, Kvs: []*mvccpb.KeyValue{{Key: []byte("a"), CreateRevision: 1, ModRevision: 1, Version: 1}}}},
		{name: "key sort", request: &etcdserverpb.RangeRequest{Key: []byte("a"), RangeEnd: []byte("z")}, result: &etcdserverpb.RangeResponse{Header: txnHeader(2), Count: 2, Kvs: []*mvccpb.KeyValue{{Key: []byte("b"), CreateRevision: 2, ModRevision: 2, Version: 1}, {Key: []byte("a"), CreateRevision: 1, ModRevision: 1, Version: 1}}}},
		{name: "value sort", request: &etcdserverpb.RangeRequest{Key: []byte("a"), RangeEnd: []byte("z"), SortTarget: etcdserverpb.RangeRequest_VALUE, SortOrder: etcdserverpb.RangeRequest_ASCEND}, result: &etcdserverpb.RangeResponse{Header: txnHeader(2), Count: 2, Kvs: []*mvccpb.KeyValue{{Key: []byte("a"), Value: []byte("z"), CreateRevision: 1, ModRevision: 1, Version: 1}, {Key: []byte("b"), Value: []byte("a"), CreateRevision: 2, ModRevision: 2, Version: 1}}}},
		{name: "version sort", request: &etcdserverpb.RangeRequest{Key: []byte("a"), RangeEnd: []byte("z"), SortTarget: etcdserverpb.RangeRequest_VERSION, SortOrder: etcdserverpb.RangeRequest_DESCEND}, result: &etcdserverpb.RangeResponse{Header: txnHeader(2), Count: 2, Kvs: []*mvccpb.KeyValue{{Key: []byte("a"), CreateRevision: 1, ModRevision: 1, Version: 1}, {Key: []byte("b"), CreateRevision: 1, ModRevision: 2, Version: 2}}}},
		{name: "create sort", request: &etcdserverpb.RangeRequest{Key: []byte("a"), RangeEnd: []byte("z"), SortTarget: etcdserverpb.RangeRequest_CREATE, SortOrder: etcdserverpb.RangeRequest_ASCEND}, result: &etcdserverpb.RangeResponse{Header: txnHeader(2), Count: 2, Kvs: []*mvccpb.KeyValue{{Key: []byte("b"), CreateRevision: 2, ModRevision: 2, Version: 1}, {Key: []byte("a"), CreateRevision: 1, ModRevision: 1, Version: 1}}}},
		{name: "mod sort", request: &etcdserverpb.RangeRequest{Key: []byte("a"), RangeEnd: []byte("z"), SortTarget: etcdserverpb.RangeRequest_MOD, SortOrder: etcdserverpb.RangeRequest_DESCEND}, result: &etcdserverpb.RangeResponse{Header: txnHeader(2), Count: 2, Kvs: []*mvccpb.KeyValue{{Key: []byte("a"), CreateRevision: 1, ModRevision: 1, Version: 1}, {Key: []byte("b"), CreateRevision: 2, ModRevision: 2, Version: 1}}}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rec := &recordingMetrics{}
			response, err := validateRangeProxyPayload(rec, tt.request, tt.result, nil)
			if tt.valid {
				require.Same(t, tt.result, response)
				require.NoError(t, err)
				require.Empty(t, recordedKVProxyIntegrityValues(rec, kvProxyRPCRange))
				return
			}
			require.Nil(t, response)
			require.Equal(t, codes.DataLoss, status.Code(err))
			require.Equal(t, []interface{}{1}, recordedKVProxyIntegrityValues(rec, kvProxyRPCRange))
		})
	}

	wantErr := errors.New("transport failed")
	want := &etcdserverpb.RangeResponse{}
	response, err := validateRangeProxyPayload(nil, &etcdserverpb.RangeRequest{}, want, wantErr)
	require.Same(t, want, response)
	require.ErrorIs(t, err, wantErr)
}

func TestPutProxyPayloadValidation(t *testing.T) {
	key := []byte("key")
	validPrevious := &mvccpb.KeyValue{
		Key: key, Value: []byte("old"), CreateRevision: 2, ModRevision: 3, Version: 2, Lease: math.MinInt64,
	}
	tests := []struct {
		name     string
		request  *etcdserverpb.PutRequest
		response *etcdserverpb.PutResponse
		valid    bool
	}{
		{name: "create with requested previous", request: &etcdserverpb.PutRequest{Key: key, PrevKv: true}, response: &etcdserverpb.PutResponse{Header: txnHeader(2)}, valid: true},
		{name: "update with signed lease previous", request: &etcdserverpb.PutRequest{Key: key, PrevKv: true}, response: &etcdserverpb.PutResponse{Header: txnHeader(4), PrevKv: validPrevious}, valid: true},
		{name: "no previous requested", request: &etcdserverpb.PutRequest{Key: key}, response: &etcdserverpb.PutResponse{Header: txnHeader(2)}, valid: true},
		{name: "zero write revision", request: &etcdserverpb.PutRequest{Key: key}, response: &etcdserverpb.PutResponse{Header: txnHeader(0)}},
		{name: "unrequested previous", request: &etcdserverpb.PutRequest{Key: key}, response: &etcdserverpb.PutResponse{Header: txnHeader(4), PrevKv: validPrevious}},
		{name: "missing ignore previous", request: &etcdserverpb.PutRequest{Key: key, PrevKv: true, IgnoreValue: true}, response: &etcdserverpb.PutResponse{Header: txnHeader(4)}},
		{name: "wrong previous key", request: &etcdserverpb.PutRequest{Key: key, PrevKv: true}, response: &etcdserverpb.PutResponse{Header: txnHeader(4), PrevKv: &mvccpb.KeyValue{Key: []byte("other"), CreateRevision: 2, ModRevision: 3, Version: 2}}},
		{name: "incomplete previous metadata", request: &etcdserverpb.PutRequest{Key: key, PrevKv: true}, response: &etcdserverpb.PutResponse{Header: txnHeader(4), PrevKv: &mvccpb.KeyValue{Key: key}}},
		{name: "impossible previous metadata", request: &etcdserverpb.PutRequest{Key: key, PrevKv: true}, response: &etcdserverpb.PutResponse{Header: txnHeader(4), PrevKv: &mvccpb.KeyValue{Key: key, CreateRevision: 2, ModRevision: 3, Version: 3}}},
		{name: "previous at write revision", request: &etcdserverpb.PutRequest{Key: key, PrevKv: true}, response: &etcdserverpb.PutResponse{Header: txnHeader(3), PrevKv: validPrevious}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rec := &recordingMetrics{}
			response, err := validatePutProxyPayload(rec, tt.request, tt.response, nil)
			if tt.valid {
				require.Same(t, tt.response, response)
				require.NoError(t, err)
				require.Empty(t, recordedKVProxyIntegrityValues(rec, kvProxyRPCPut))
				return
			}
			require.Nil(t, response)
			require.Equal(t, codes.DataLoss, status.Code(err))
			require.Equal(t, []interface{}{1}, recordedKVProxyIntegrityValues(rec, kvProxyRPCPut))
		})
	}

	wantErr := errors.New("transport failed")
	want := &etcdserverpb.PutResponse{}
	response, err := validatePutProxyPayload(nil, &etcdserverpb.PutRequest{}, want, wantErr)
	require.Same(t, want, response)
	require.ErrorIs(t, err, wantErr)
}

func TestDeleteRangeProxyPayloadValidation(t *testing.T) {
	previousA := &mvccpb.KeyValue{Key: []byte("a"), CreateRevision: 1, ModRevision: 2, Version: 2, Lease: math.MinInt64}
	previousB := &mvccpb.KeyValue{Key: []byte("b"), CreateRevision: 3, ModRevision: 3, Version: 1}
	tests := []struct {
		name     string
		request  *etcdserverpb.DeleteRangeRequest
		response *etcdserverpb.DeleteRangeResponse
		valid    bool
	}{
		{name: "empty with previous requested", request: &etcdserverpb.DeleteRangeRequest{Key: []byte("a"), PrevKv: true}, response: &etcdserverpb.DeleteRangeResponse{Header: txnHeader(3)}, valid: true},
		{name: "range with signed lease previous", request: &etcdserverpb.DeleteRangeRequest{Key: []byte("a"), RangeEnd: []byte("c"), PrevKv: true}, response: &etcdserverpb.DeleteRangeResponse{Header: txnHeader(4), Deleted: 2, PrevKvs: []*mvccpb.KeyValue{previousA, previousB}}, valid: true},
		{name: "no previous requested", request: &etcdserverpb.DeleteRangeRequest{Key: []byte("a"), RangeEnd: []byte("c")}, response: &etcdserverpb.DeleteRangeResponse{Header: txnHeader(4), Deleted: 2}, valid: true},
		{name: "zero revision", request: &etcdserverpb.DeleteRangeRequest{Key: []byte("a")}, response: &etcdserverpb.DeleteRangeResponse{Header: txnHeader(0)}},
		{name: "negative deleted", request: &etcdserverpb.DeleteRangeRequest{Key: []byte("a")}, response: &etcdserverpb.DeleteRangeResponse{Header: txnHeader(3), Deleted: -1}},
		{name: "unrequested previous", request: &etcdserverpb.DeleteRangeRequest{Key: []byte("a")}, response: &etcdserverpb.DeleteRangeResponse{Header: txnHeader(3), Deleted: 1, PrevKvs: []*mvccpb.KeyValue{previousA}}},
		{name: "previous count mismatch", request: &etcdserverpb.DeleteRangeRequest{Key: []byte("a"), PrevKv: true}, response: &etcdserverpb.DeleteRangeResponse{Header: txnHeader(3), Deleted: 1}},
		{name: "nil previous", request: &etcdserverpb.DeleteRangeRequest{Key: []byte("a"), PrevKv: true}, response: &etcdserverpb.DeleteRangeResponse{Header: txnHeader(3), Deleted: 1, PrevKvs: []*mvccpb.KeyValue{nil}}},
		{name: "outside range", request: &etcdserverpb.DeleteRangeRequest{Key: []byte("a"), RangeEnd: []byte("b"), PrevKv: true}, response: &etcdserverpb.DeleteRangeResponse{Header: txnHeader(4), Deleted: 1, PrevKvs: []*mvccpb.KeyValue{previousB}}},
		{name: "invalid metadata", request: &etcdserverpb.DeleteRangeRequest{Key: []byte("a"), PrevKv: true}, response: &etcdserverpb.DeleteRangeResponse{Header: txnHeader(4), Deleted: 1, PrevKvs: []*mvccpb.KeyValue{{Key: []byte("a"), CreateRevision: 1, ModRevision: 2, Version: 3}}}},
		{name: "previous at delete revision", request: &etcdserverpb.DeleteRangeRequest{Key: []byte("b"), PrevKv: true}, response: &etcdserverpb.DeleteRangeResponse{Header: txnHeader(3), Deleted: 1, PrevKvs: []*mvccpb.KeyValue{previousB}}},
		{name: "unsorted previous", request: &etcdserverpb.DeleteRangeRequest{Key: []byte("a"), RangeEnd: []byte("c"), PrevKv: true}, response: &etcdserverpb.DeleteRangeResponse{Header: txnHeader(4), Deleted: 2, PrevKvs: []*mvccpb.KeyValue{previousB, previousA}}},
		{name: "duplicate previous", request: &etcdserverpb.DeleteRangeRequest{Key: []byte("a"), RangeEnd: []byte("c"), PrevKv: true}, response: &etcdserverpb.DeleteRangeResponse{Header: txnHeader(4), Deleted: 2, PrevKvs: []*mvccpb.KeyValue{previousA, previousA}}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rec := &recordingMetrics{}
			response, err := validateDeleteRangeProxyPayload(rec, tt.request, tt.response, nil)
			if tt.valid {
				require.Same(t, tt.response, response)
				require.NoError(t, err)
				require.Empty(t, recordedKVProxyIntegrityValues(rec, kvProxyRPCDeleteRange))
				return
			}
			require.Nil(t, response)
			require.Equal(t, codes.DataLoss, status.Code(err))
			require.Equal(t, []interface{}{1}, recordedKVProxyIntegrityValues(rec, kvProxyRPCDeleteRange))
		})
	}

	wantErr := errors.New("transport failed")
	want := &etcdserverpb.DeleteRangeResponse{}
	response, err := validateDeleteRangeProxyPayload(nil, &etcdserverpb.DeleteRangeRequest{}, want, wantErr)
	require.Same(t, want, response)
	require.ErrorIs(t, err, wantErr)
}

func TestCompactProxyPayloadValidation(t *testing.T) {
	tests := []struct {
		name     string
		request  *etcdserverpb.CompactionRequest
		response *etcdserverpb.CompactionResponse
		valid    bool
	}{
		{name: "current revision after requested", request: &etcdserverpb.CompactionRequest{Revision: 123}, response: &etcdserverpb.CompactionResponse{Header: txnHeader(456)}, valid: true},
		{name: "initial zero compaction", request: &etcdserverpb.CompactionRequest{}, response: &etcdserverpb.CompactionResponse{Header: txnHeader(1)}, valid: true},
		{name: "zero current revision", request: &etcdserverpb.CompactionRequest{}, response: &etcdserverpb.CompactionResponse{Header: txnHeader(0)}},
		{name: "current revision below requested", request: &etcdserverpb.CompactionRequest{Revision: 123}, response: &etcdserverpb.CompactionResponse{Header: txnHeader(122)}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rec := &recordingMetrics{}
			response, err := validateCompactProxyPayload(rec, tt.request, tt.response, nil)
			if tt.valid {
				require.Same(t, tt.response, response)
				require.NoError(t, err)
				require.Empty(t, recordedKVProxyIntegrityValues(rec, kvProxyRPCCompact))
				return
			}
			require.Nil(t, response)
			require.Equal(t, codes.DataLoss, status.Code(err))
			require.Equal(t, []interface{}{1}, recordedKVProxyIntegrityValues(rec, kvProxyRPCCompact))
		})
	}

	wantErr := errors.New("transport failed")
	want := &etcdserverpb.CompactionResponse{}
	response, err := validateCompactProxyPayload(nil, &etcdserverpb.CompactionRequest{}, want, wantErr)
	require.Same(t, want, response)
	require.ErrorIs(t, err, wantErr)
}

func TestTxnProxyPayloadValidation(t *testing.T) {
	rangeRequest := func(key string) *etcdserverpb.RequestOp {
		return &etcdserverpb.RequestOp{Request: &etcdserverpb.RequestOp_RequestRange{RequestRange: &etcdserverpb.RangeRequest{Key: []byte(key)}}}
	}
	putRequest := func(key string) *etcdserverpb.RequestOp {
		return &etcdserverpb.RequestOp{Request: &etcdserverpb.RequestOp_RequestPut{RequestPut: &etcdserverpb.PutRequest{Key: []byte(key)}}}
	}
	deleteRequest := func(key string) *etcdserverpb.RequestOp {
		return &etcdserverpb.RequestOp{Request: &etcdserverpb.RequestOp_RequestDeleteRange{RequestDeleteRange: &etcdserverpb.DeleteRangeRequest{Key: []byte(key)}}}
	}
	rangeResponse := func() *etcdserverpb.ResponseOp {
		return &etcdserverpb.ResponseOp{Response: &etcdserverpb.ResponseOp_ResponseRange{ResponseRange: &etcdserverpb.RangeResponse{Header: txnHeader(5)}}}
	}
	putResponse := func() *etcdserverpb.ResponseOp {
		return &etcdserverpb.ResponseOp{Response: &etcdserverpb.ResponseOp_ResponsePut{ResponsePut: &etcdserverpb.PutResponse{Header: txnHeader(5)}}}
	}
	deleteResponse := func() *etcdserverpb.ResponseOp {
		return &etcdserverpb.ResponseOp{Response: &etcdserverpb.ResponseOp_ResponseDeleteRange{ResponseDeleteRange: &etcdserverpb.DeleteRangeResponse{Header: txnHeader(5)}}}
	}
	nestedRequest := &etcdserverpb.TxnRequest{Failure: []*etcdserverpb.RequestOp{deleteRequest("nested")}}
	nestedResponse := &etcdserverpb.TxnResponse{Header: txnHeader(0), Responses: []*etcdserverpb.ResponseOp{deleteResponse()}}
	request := &etcdserverpb.TxnRequest{
		Success: []*etcdserverpb.RequestOp{
			rangeRequest("range"), putRequest("put"), deleteRequest("delete"),
			{Request: &etcdserverpb.RequestOp_RequestTxn{RequestTxn: nestedRequest}},
		},
		Failure: []*etcdserverpb.RequestOp{putRequest("failure")},
	}
	validSuccess := &etcdserverpb.TxnResponse{
		Header: txnHeader(5), Succeeded: true,
		Responses: []*etcdserverpb.ResponseOp{
			rangeResponse(), putResponse(), deleteResponse(),
			{Response: &etcdserverpb.ResponseOp_ResponseTxn{ResponseTxn: nestedResponse}},
		},
	}
	tests := []struct {
		name     string
		request  *etcdserverpb.TxnRequest
		response *etcdserverpb.TxnResponse
		valid    bool
	}{
		{name: "selected success tree", request: request, response: validSuccess, valid: true},
		{name: "selected failure branch", request: request, response: &etcdserverpb.TxnResponse{Header: txnHeader(5), Responses: []*etcdserverpb.ResponseOp{putResponse()}}, valid: true},
		{name: "pre-write operation revision", request: &etcdserverpb.TxnRequest{Success: []*etcdserverpb.RequestOp{rangeRequest("key")}}, response: &etcdserverpb.TxnResponse{Header: txnHeader(5), Succeeded: true, Responses: []*etcdserverpb.ResponseOp{{Response: &etcdserverpb.ResponseOp_ResponseRange{ResponseRange: &etcdserverpb.RangeResponse{Header: txnHeader(4)}}}}}, valid: true},
		{name: "historical range payload with signed lease", request: &etcdserverpb.TxnRequest{Success: []*etcdserverpb.RequestOp{{Request: &etcdserverpb.RequestOp_RequestRange{RequestRange: &etcdserverpb.RangeRequest{Key: []byte("a"), Revision: 4}}}}}, response: &etcdserverpb.TxnResponse{Header: txnHeader(5), Succeeded: true, Responses: []*etcdserverpb.ResponseOp{{Response: &etcdserverpb.ResponseOp_ResponseRange{ResponseRange: &etcdserverpb.RangeResponse{Header: txnHeader(5), Count: 1, Kvs: []*mvccpb.KeyValue{{Key: []byte("a"), CreateRevision: 3, ModRevision: 4, Version: 2, Lease: math.MinInt64}}}}}}}, valid: true},
		{name: "put previous from same revision", request: &etcdserverpb.TxnRequest{Success: []*etcdserverpb.RequestOp{{Request: &etcdserverpb.RequestOp_RequestPut{RequestPut: &etcdserverpb.PutRequest{Key: []byte("p"), PrevKv: true}}}}}, response: &etcdserverpb.TxnResponse{Header: txnHeader(5), Succeeded: true, Responses: []*etcdserverpb.ResponseOp{{Response: &etcdserverpb.ResponseOp_ResponsePut{ResponsePut: &etcdserverpb.PutResponse{Header: txnHeader(5), PrevKv: &mvccpb.KeyValue{Key: []byte("p"), CreateRevision: 5, ModRevision: 5, Version: 1, Lease: math.MinInt64}}}}}}, valid: true},
		{name: "delete previous from same revision", request: &etcdserverpb.TxnRequest{Success: []*etcdserverpb.RequestOp{{Request: &etcdserverpb.RequestOp_RequestDeleteRange{RequestDeleteRange: &etcdserverpb.DeleteRangeRequest{Key: []byte("a"), RangeEnd: []byte("c"), PrevKv: true}}}}}, response: &etcdserverpb.TxnResponse{Header: txnHeader(5), Succeeded: true, Responses: []*etcdserverpb.ResponseOp{{Response: &etcdserverpb.ResponseOp_ResponseDeleteRange{ResponseDeleteRange: &etcdserverpb.DeleteRangeResponse{Header: txnHeader(5), Deleted: 2, PrevKvs: []*mvccpb.KeyValue{{Key: []byte("a"), CreateRevision: 5, ModRevision: 5, Version: 1}, {Key: []byte("b"), CreateRevision: 4, ModRevision: 4, Version: 1}}}}}}}, valid: true},
		{name: "zero revision", request: &etcdserverpb.TxnRequest{}, response: &etcdserverpb.TxnResponse{Header: txnHeader(0), Succeeded: true}},
		{name: "wrong response count", request: request, response: &etcdserverpb.TxnResponse{Header: txnHeader(5), Succeeded: true}},
		{name: "nil response operation", request: &etcdserverpb.TxnRequest{Success: []*etcdserverpb.RequestOp{rangeRequest("key")}}, response: &etcdserverpb.TxnResponse{Header: txnHeader(5), Succeeded: true, Responses: []*etcdserverpb.ResponseOp{nil}}},
		{name: "wrong response type", request: &etcdserverpb.TxnRequest{Success: []*etcdserverpb.RequestOp{rangeRequest("key")}}, response: &etcdserverpb.TxnResponse{Header: txnHeader(5), Succeeded: true, Responses: []*etcdserverpb.ResponseOp{putResponse()}}},
		{name: "nil typed payload", request: &etcdserverpb.TxnRequest{Success: []*etcdserverpb.RequestOp{rangeRequest("key")}}, response: &etcdserverpb.TxnResponse{Header: txnHeader(5), Succeeded: true, Responses: []*etcdserverpb.ResponseOp{{Response: &etcdserverpb.ResponseOp_ResponseRange{}}}}},
		{name: "nested wrong branch count", request: request, response: &etcdserverpb.TxnResponse{Header: txnHeader(5), Succeeded: true, Responses: []*etcdserverpb.ResponseOp{rangeResponse(), putResponse(), deleteResponse(), {Response: &etcdserverpb.ResponseOp_ResponseTxn{ResponseTxn: &etcdserverpb.TxnResponse{Succeeded: false}}}}}},
		{name: "nil nested payload", request: request, response: &etcdserverpb.TxnResponse{Header: txnHeader(5), Succeeded: true, Responses: []*etcdserverpb.ResponseOp{rangeResponse(), putResponse(), deleteResponse(), {Response: &etcdserverpb.ResponseOp_ResponseTxn{}}}}},
		{name: "missing operation header", request: &etcdserverpb.TxnRequest{Success: []*etcdserverpb.RequestOp{rangeRequest("key")}}, response: &etcdserverpb.TxnResponse{Header: txnHeader(5), Succeeded: true, Responses: []*etcdserverpb.ResponseOp{{Response: &etcdserverpb.ResponseOp_ResponseRange{ResponseRange: &etcdserverpb.RangeResponse{}}}}}},
		{name: "old operation revision", request: &etcdserverpb.TxnRequest{Success: []*etcdserverpb.RequestOp{rangeRequest("key")}}, response: &etcdserverpb.TxnResponse{Header: txnHeader(5), Succeeded: true, Responses: []*etcdserverpb.ResponseOp{{Response: &etcdserverpb.ResponseOp_ResponseRange{ResponseRange: &etcdserverpb.RangeResponse{Header: txnHeader(3)}}}}}},
		{name: "future operation revision", request: &etcdserverpb.TxnRequest{Success: []*etcdserverpb.RequestOp{rangeRequest("key")}}, response: &etcdserverpb.TxnResponse{Header: txnHeader(5), Succeeded: true, Responses: []*etcdserverpb.ResponseOp{{Response: &etcdserverpb.ResponseOp_ResponseRange{ResponseRange: &etcdserverpb.RangeResponse{Header: txnHeader(6)}}}}}},
		{name: "nonzero nested revision", request: request, response: &etcdserverpb.TxnResponse{Header: txnHeader(5), Succeeded: true, Responses: []*etcdserverpb.ResponseOp{rangeResponse(), putResponse(), deleteResponse(), {Response: &etcdserverpb.ResponseOp_ResponseTxn{ResponseTxn: &etcdserverpb.TxnResponse{Header: txnHeader(1), Responses: []*etcdserverpb.ResponseOp{deleteResponse()}}}}}}},
		{name: "range count mismatch", request: &etcdserverpb.TxnRequest{Success: []*etcdserverpb.RequestOp{rangeRequest("key")}}, response: &etcdserverpb.TxnResponse{Header: txnHeader(5), Succeeded: true, Responses: []*etcdserverpb.ResponseOp{{Response: &etcdserverpb.ResponseOp_ResponseRange{ResponseRange: &etcdserverpb.RangeResponse{Header: txnHeader(5), Kvs: []*mvccpb.KeyValue{{Key: []byte("key"), CreateRevision: 4, ModRevision: 4, Version: 1}}}}}}}},
		{name: "nested range outside request", request: &etcdserverpb.TxnRequest{Success: []*etcdserverpb.RequestOp{{Request: &etcdserverpb.RequestOp_RequestTxn{RequestTxn: &etcdserverpb.TxnRequest{Success: []*etcdserverpb.RequestOp{rangeRequest("a")}}}}}}, response: &etcdserverpb.TxnResponse{Header: txnHeader(5), Succeeded: true, Responses: []*etcdserverpb.ResponseOp{{Response: &etcdserverpb.ResponseOp_ResponseTxn{ResponseTxn: &etcdserverpb.TxnResponse{Header: txnHeader(0), Succeeded: true, Responses: []*etcdserverpb.ResponseOp{{Response: &etcdserverpb.ResponseOp_ResponseRange{ResponseRange: &etcdserverpb.RangeResponse{Header: txnHeader(5), Count: 1, Kvs: []*mvccpb.KeyValue{{Key: []byte("b"), CreateRevision: 4, ModRevision: 4, Version: 1}}}}}}}}}}}},
		{name: "put revision before outer", request: &etcdserverpb.TxnRequest{Success: []*etcdserverpb.RequestOp{putRequest("p")}}, response: &etcdserverpb.TxnResponse{Header: txnHeader(5), Succeeded: true, Responses: []*etcdserverpb.ResponseOp{{Response: &etcdserverpb.ResponseOp_ResponsePut{ResponsePut: &etcdserverpb.PutResponse{Header: txnHeader(4)}}}}}},
		{name: "put unrequested previous", request: &etcdserverpb.TxnRequest{Success: []*etcdserverpb.RequestOp{putRequest("p")}}, response: &etcdserverpb.TxnResponse{Header: txnHeader(5), Succeeded: true, Responses: []*etcdserverpb.ResponseOp{{Response: &etcdserverpb.ResponseOp_ResponsePut{ResponsePut: &etcdserverpb.PutResponse{Header: txnHeader(5), PrevKv: &mvccpb.KeyValue{Key: []byte("p"), CreateRevision: 4, ModRevision: 4, Version: 1}}}}}}},
		{name: "put future previous", request: &etcdserverpb.TxnRequest{Success: []*etcdserverpb.RequestOp{{Request: &etcdserverpb.RequestOp_RequestPut{RequestPut: &etcdserverpb.PutRequest{Key: []byte("p"), PrevKv: true}}}}}, response: &etcdserverpb.TxnResponse{Header: txnHeader(5), Succeeded: true, Responses: []*etcdserverpb.ResponseOp{{Response: &etcdserverpb.ResponseOp_ResponsePut{ResponsePut: &etcdserverpb.PutResponse{Header: txnHeader(5), PrevKv: &mvccpb.KeyValue{Key: []byte("p"), CreateRevision: 6, ModRevision: 6, Version: 1}}}}}}},
		{name: "delete count mismatch", request: &etcdserverpb.TxnRequest{Success: []*etcdserverpb.RequestOp{{Request: &etcdserverpb.RequestOp_RequestDeleteRange{RequestDeleteRange: &etcdserverpb.DeleteRangeRequest{Key: []byte("a"), PrevKv: true}}}}}, response: &etcdserverpb.TxnResponse{Header: txnHeader(5), Succeeded: true, Responses: []*etcdserverpb.ResponseOp{{Response: &etcdserverpb.ResponseOp_ResponseDeleteRange{ResponseDeleteRange: &etcdserverpb.DeleteRangeResponse{Header: txnHeader(5), Deleted: 1}}}}}},
		{name: "effective delete before outer", request: &etcdserverpb.TxnRequest{Success: []*etcdserverpb.RequestOp{deleteRequest("a")}}, response: &etcdserverpb.TxnResponse{Header: txnHeader(5), Succeeded: true, Responses: []*etcdserverpb.ResponseOp{{Response: &etcdserverpb.ResponseOp_ResponseDeleteRange{ResponseDeleteRange: &etcdserverpb.DeleteRangeResponse{Header: txnHeader(4), Deleted: 1}}}}}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rec := &recordingMetrics{}
			response, err := validateTxnProxyPayload(rec, tt.request, tt.response, nil)
			if tt.valid {
				require.Same(t, tt.response, response)
				require.NoError(t, err)
				require.Empty(t, recordedKVProxyIntegrityValues(rec, kvProxyRPCTxn))
				return
			}
			require.Nil(t, response)
			require.Equal(t, codes.DataLoss, status.Code(err))
			require.Equal(t, []interface{}{1}, recordedKVProxyIntegrityValues(rec, kvProxyRPCTxn))
		})
	}

	wantErr := errors.New("transport failed")
	want := &etcdserverpb.TxnResponse{}
	response, err := validateTxnProxyPayload(nil, &etcdserverpb.TxnRequest{}, want, wantErr)
	require.Same(t, want, response)
	require.ErrorIs(t, err, wantErr)
}

func TestAlarmProxyPayloadValidation(t *testing.T) {
	alarm := func(memberID uint64, alarmType etcdserverpb.AlarmType) *etcdserverpb.AlarmMember {
		return &etcdserverpb.AlarmMember{MemberID: memberID, Alarm: alarmType}
	}
	unknownAlarm := etcdserverpb.AlarmType(99)
	tests := []struct {
		name     string
		request  *etcdserverpb.AlarmRequest
		response *etcdserverpb.AlarmResponse
		valid    bool
	}{
		{name: "get empty", request: &etcdserverpb.AlarmRequest{}, response: &etcdserverpb.AlarmResponse{Header: txnHeader(1)}, valid: true},
		{name: "get all unordered including unknown", request: &etcdserverpb.AlarmRequest{}, response: &etcdserverpb.AlarmResponse{Header: txnHeader(1), Alarms: []*etcdserverpb.AlarmMember{alarm(2, unknownAlarm), alarm(0, etcdserverpb.AlarmType_CORRUPT), alarm(1, etcdserverpb.AlarmType_NOSPACE)}}, valid: true},
		{name: "get filtered", request: &etcdserverpb.AlarmRequest{Alarm: etcdserverpb.AlarmType_NOSPACE}, response: &etcdserverpb.AlarmResponse{Header: txnHeader(1), Alarms: []*etcdserverpb.AlarmMember{alarm(2, etcdserverpb.AlarmType_NOSPACE), alarm(1, etcdserverpb.AlarmType_NOSPACE)}}, valid: true},
		{name: "activate", request: &etcdserverpb.AlarmRequest{Action: etcdserverpb.AlarmRequest_ACTIVATE, MemberID: 7, Alarm: etcdserverpb.AlarmType_CORRUPT}, response: &etcdserverpb.AlarmResponse{Header: txnHeader(1), Alarms: []*etcdserverpb.AlarmMember{alarm(7, etcdserverpb.AlarmType_CORRUPT)}}, valid: true},
		{name: "activate default owner", request: &etcdserverpb.AlarmRequest{Action: etcdserverpb.AlarmRequest_ACTIVATE, Alarm: etcdserverpb.AlarmType_NOSPACE}, response: &etcdserverpb.AlarmResponse{Header: txnHeader(1), Alarms: []*etcdserverpb.AlarmMember{alarm(7, etcdserverpb.AlarmType_NOSPACE)}}, valid: true},
		{name: "activate none", request: &etcdserverpb.AlarmRequest{Action: etcdserverpb.AlarmRequest_ACTIVATE}, response: &etcdserverpb.AlarmResponse{Header: txnHeader(1)}, valid: true},
		{name: "deactivate absent", request: &etcdserverpb.AlarmRequest{Action: etcdserverpb.AlarmRequest_DEACTIVATE, MemberID: 7, Alarm: etcdserverpb.AlarmType_CORRUPT}, response: &etcdserverpb.AlarmResponse{Header: txnHeader(1)}, valid: true},
		{name: "deactivate present", request: &etcdserverpb.AlarmRequest{Action: etcdserverpb.AlarmRequest_DEACTIVATE, MemberID: 7, Alarm: unknownAlarm}, response: &etcdserverpb.AlarmResponse{Header: txnHeader(1), Alarms: []*etcdserverpb.AlarmMember{alarm(7, unknownAlarm)}}, valid: true},
		{name: "nil get alarm", request: &etcdserverpb.AlarmRequest{}, response: &etcdserverpb.AlarmResponse{Header: txnHeader(1), Alarms: []*etcdserverpb.AlarmMember{nil}}},
		{name: "none get alarm", request: &etcdserverpb.AlarmRequest{}, response: &etcdserverpb.AlarmResponse{Header: txnHeader(1), Alarms: []*etcdserverpb.AlarmMember{alarm(1, etcdserverpb.AlarmType_NONE)}}},
		{name: "get filter mismatch", request: &etcdserverpb.AlarmRequest{Alarm: etcdserverpb.AlarmType_NOSPACE}, response: &etcdserverpb.AlarmResponse{Header: txnHeader(1), Alarms: []*etcdserverpb.AlarmMember{alarm(1, etcdserverpb.AlarmType_CORRUPT)}}},
		{name: "duplicate get alarm", request: &etcdserverpb.AlarmRequest{}, response: &etcdserverpb.AlarmResponse{Header: txnHeader(1), Alarms: []*etcdserverpb.AlarmMember{alarm(1, etcdserverpb.AlarmType_NOSPACE), alarm(1, etcdserverpb.AlarmType_NOSPACE)}}},
		{name: "activate missing", request: &etcdserverpb.AlarmRequest{Action: etcdserverpb.AlarmRequest_ACTIVATE, MemberID: 7, Alarm: etcdserverpb.AlarmType_CORRUPT}, response: &etcdserverpb.AlarmResponse{Header: txnHeader(1)}},
		{name: "activate mismatch", request: &etcdserverpb.AlarmRequest{Action: etcdserverpb.AlarmRequest_ACTIVATE, MemberID: 7, Alarm: etcdserverpb.AlarmType_CORRUPT}, response: &etcdserverpb.AlarmResponse{Header: txnHeader(1), Alarms: []*etcdserverpb.AlarmMember{alarm(8, etcdserverpb.AlarmType_CORRUPT)}}},
		{name: "deactivate too many", request: &etcdserverpb.AlarmRequest{Action: etcdserverpb.AlarmRequest_DEACTIVATE, MemberID: 7, Alarm: etcdserverpb.AlarmType_CORRUPT}, response: &etcdserverpb.AlarmResponse{Header: txnHeader(1), Alarms: []*etcdserverpb.AlarmMember{alarm(7, etcdserverpb.AlarmType_CORRUPT), alarm(7, etcdserverpb.AlarmType_CORRUPT)}}},
		{name: "deactivate wrong zero owner", request: &etcdserverpb.AlarmRequest{Action: etcdserverpb.AlarmRequest_DEACTIVATE, Alarm: etcdserverpb.AlarmType_CORRUPT}, response: &etcdserverpb.AlarmResponse{Header: txnHeader(1), Alarms: []*etcdserverpb.AlarmMember{alarm(7, etcdserverpb.AlarmType_CORRUPT)}}},
		{name: "none mutation result", request: &etcdserverpb.AlarmRequest{Action: etcdserverpb.AlarmRequest_DEACTIVATE}, response: &etcdserverpb.AlarmResponse{Header: txnHeader(1), Alarms: []*etcdserverpb.AlarmMember{alarm(0, etcdserverpb.AlarmType_NONE)}}},
		{name: "unknown action success", request: &etcdserverpb.AlarmRequest{Action: etcdserverpb.AlarmRequest_AlarmAction(99)}, response: &etcdserverpb.AlarmResponse{Header: txnHeader(1)}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rec := &recordingMetrics{}
			response, err := validateAlarmProxyPayload(rec, tt.request, tt.response, nil)
			if tt.valid {
				require.Same(t, tt.response, response)
				require.NoError(t, err)
				require.Empty(t, recordedMaintenanceProxyIntegrityValues(rec, maintenanceProxyRPCAlarm))
				return
			}
			require.Nil(t, response)
			require.Equal(t, codes.DataLoss, status.Code(err))
			require.Equal(t, []interface{}{1}, recordedMaintenanceProxyIntegrityValues(rec, maintenanceProxyRPCAlarm))
		})
	}

	wantErr := errors.New("transport failed")
	want := &etcdserverpb.AlarmResponse{}
	response, err := validateAlarmProxyPayload(nil, &etcdserverpb.AlarmRequest{}, want, wantErr)
	require.Same(t, want, response)
	require.ErrorIs(t, err, wantErr)
}

func TestStatusProxyPayloadValidation(t *testing.T) {
	valid := func() *etcdserverpb.StatusResponse {
		return &etcdserverpb.StatusResponse{
			Header: txnHeader(5), Version: "3.6.0", DbSize: 10, DbSizeInUse: 8, DbSizeQuota: 100,
			Leader: 1, RaftIndex: 7, RaftAppliedIndex: 6, DowngradeInfo: &etcdserverpb.DowngradeInfo{},
		}
	}
	pre34Defaults := func(response *etcdserverpb.StatusResponse) {
		response.Version = "3.3.0"
		response.RaftAppliedIndex = 0
		response.Errors = nil
		response.DbSizeInUse = 0
		response.IsLearner = false
		response.StorageVersion = ""
		response.DbSizeQuota = 0
		response.DowngradeInfo = nil
	}
	tests := []struct {
		name   string
		mutate func(*etcdserverpb.StatusResponse)
		valid  bool
	}{
		{name: "healthy", valid: true},
		{name: "no leader", mutate: func(response *etcdserverpb.StatusResponse) {
			response.Leader = 0
			response.Errors = []string{rpctypes.ErrNoLeader.Error()}
		}, valid: true},
		{name: "zero-member alarm", mutate: func(response *etcdserverpb.StatusResponse) {
			response.Errors = []string{"alarm:NOSPACE"}
		}, valid: true},
		{name: "member alarm", mutate: func(response *etcdserverpb.StatusResponse) {
			response.Errors = []string{"memberID:7  alarm:CORRUPT"}
		}, valid: true},
		{name: "unknown alarm", mutate: func(response *etcdserverpb.StatusResponse) {
			response.Errors = []string{"memberID:7  alarm:127"}
		}, valid: true},
		{name: "no leader before alarm", mutate: func(response *etcdserverpb.StatusResponse) {
			response.Leader = 0
			response.Errors = []string{rpctypes.ErrNoLeader.Error(), "memberID:7  alarm:NOSPACE"}
		}, valid: true},
		{name: "empty storage version allowed", mutate: func(response *etcdserverpb.StatusResponse) { response.StorageVersion = "" }, valid: true},
		{name: "canonical storage version", mutate: func(response *etcdserverpb.StatusResponse) { response.StorageVersion = "3.6.0" }, valid: true},
		{name: "pre-3.6 defaults", mutate: func(response *etcdserverpb.StatusResponse) {
			response.Version = "3.5.0"
			response.DbSizeQuota = 0
			response.DowngradeInfo = nil
		}, valid: true},
		{name: "pre-3.6 canonical alarm", mutate: func(response *etcdserverpb.StatusResponse) {
			response.Version = "3.5.0"
			response.DbSizeQuota = 0
			response.DowngradeInfo = nil
			response.Errors = []string{"memberID:7  alarm:NOSPACE"}
		}, valid: true},
		{name: "pre-3.6 arbitrary status error", mutate: func(response *etcdserverpb.StatusResponse) {
			response.Version = "3.5.0"
			response.DbSizeQuota = 0
			response.DowngradeInfo = nil
			response.Errors = []string{"disk unavailable"}
		}},
		{name: "pre-3.6 versioned fields", mutate: func(response *etcdserverpb.StatusResponse) {
			response.Version = "3.5.0"
			response.StorageVersion = "3.5.0"
		}},
		{name: "pre-3.4 defaults", mutate: pre34Defaults, valid: true},
		{name: "pre-3.4 zero leader without unavailable errors field", mutate: func(response *etcdserverpb.StatusResponse) {
			pre34Defaults(response)
			response.Leader = 0
		}, valid: true},
		{name: "pre-3.4 applied index", mutate: func(response *etcdserverpb.StatusResponse) {
			pre34Defaults(response)
			response.RaftAppliedIndex = 1
		}},
		{name: "pre-3.4 errors", mutate: func(response *etcdserverpb.StatusResponse) {
			pre34Defaults(response)
			response.Errors = []string{"alarm:NOSPACE"}
		}},
		{name: "pre-3.4 database size in use", mutate: func(response *etcdserverpb.StatusResponse) {
			pre34Defaults(response)
			response.DbSizeInUse = 1
		}},
		{name: "pre-3.4 learner", mutate: func(response *etcdserverpb.StatusResponse) {
			pre34Defaults(response)
			response.IsLearner = true
		}},
		{name: "semantic prerelease server version", mutate: func(response *etcdserverpb.StatusResponse) {
			response.Version = "3.7.0-rc.1+build.2"
		}, valid: true},
		{name: "zero raft fields allowed", mutate: func(response *etcdserverpb.StatusResponse) {
			response.RaftIndex = 0
			response.RaftAppliedIndex = 0
			response.RaftTerm = 0
		}, valid: true},
		{name: "empty version", mutate: func(response *etcdserverpb.StatusResponse) { response.Version = "" }},
		{name: "invalid server version", mutate: func(response *etcdserverpb.StatusResponse) { response.Version = "not-semver" }},
		{name: "invalid storage version", mutate: func(response *etcdserverpb.StatusResponse) { response.StorageVersion = "not-semver" }},
		{name: "storage version with nonzero patch", mutate: func(response *etcdserverpb.StatusResponse) { response.StorageVersion = "3.6.1" }},
		{name: "storage version with prerelease", mutate: func(response *etcdserverpb.StatusResponse) { response.StorageVersion = "3.6.0-rc.1" }},
		{name: "storage version with metadata", mutate: func(response *etcdserverpb.StatusResponse) { response.StorageVersion = "3.6.0+build.2" }},
		{name: "negative size", mutate: func(response *etcdserverpb.StatusResponse) { response.DbSize = -1 }},
		{name: "negative in use", mutate: func(response *etcdserverpb.StatusResponse) { response.DbSizeInUse = -1 }},
		{name: "independently sampled in use above size", mutate: func(response *etcdserverpb.StatusResponse) { response.DbSizeInUse = 11 }, valid: true},
		{name: "zero quota", mutate: func(response *etcdserverpb.StatusResponse) { response.DbSizeQuota = 0 }},
		{name: "independently sampled applied above committed", mutate: func(response *etcdserverpb.StatusResponse) { response.RaftAppliedIndex = 8 }, valid: true},
		{name: "negative disabled quota", mutate: func(response *etcdserverpb.StatusResponse) { response.DbSizeQuota = -1 }, valid: true},
		{name: "missing downgrade info", mutate: func(response *etcdserverpb.StatusResponse) { response.DowngradeInfo = nil }},
		{name: "disabled downgrade with target", mutate: func(response *etcdserverpb.StatusResponse) {
			response.DowngradeInfo.TargetVersion = "3.5.0"
		}},
		{name: "enabled downgrade without target", mutate: func(response *etcdserverpb.StatusResponse) {
			response.DowngradeInfo.Enabled = true
		}},
		{name: "enabled downgrade with invalid target", mutate: func(response *etcdserverpb.StatusResponse) {
			response.DowngradeInfo.Enabled = true
			response.DowngradeInfo.TargetVersion = "not-semver"
		}},
		{name: "enabled downgrade with prerelease target", mutate: func(response *etcdserverpb.StatusResponse) {
			response.DowngradeInfo.Enabled = true
			response.DowngradeInfo.TargetVersion = "3.5.0-rc.1"
		}},
		{name: "enabled downgrade with skipped minor target", mutate: func(response *etcdserverpb.StatusResponse) {
			response.DowngradeInfo.Enabled = true
			response.DowngradeInfo.TargetVersion = "3.4.0"
		}},
		{name: "enabled downgrade with cross major target", mutate: func(response *etcdserverpb.StatusResponse) {
			response.DowngradeInfo.Enabled = true
			response.DowngradeInfo.TargetVersion = "4.0.0"
		}},
		{name: "enabled downgrade with nonzero patch target", mutate: func(response *etcdserverpb.StatusResponse) {
			response.DowngradeInfo.Enabled = true
			response.DowngradeInfo.TargetVersion = "3.5.1"
		}},
		{name: "enabled downgrade with release target", mutate: func(response *etcdserverpb.StatusResponse) {
			response.DowngradeInfo.Enabled = true
			response.DowngradeInfo.TargetVersion = "3.5.0"
		}, valid: true},
		{name: "enabled downgrade already on target release", mutate: func(response *etcdserverpb.StatusResponse) {
			response.DowngradeInfo.Enabled = true
			response.DowngradeInfo.TargetVersion = "3.6.0"
		}, valid: true},
		{name: "empty status error", mutate: func(response *etcdserverpb.StatusResponse) { response.Errors = []string{""} }},
		{name: "arbitrary status error", mutate: func(response *etcdserverpb.StatusResponse) { response.Errors = []string{"disk unavailable"} }},
		{name: "alarm without type", mutate: func(response *etcdserverpb.StatusResponse) { response.Errors = []string{"memberID:7"} }},
		{name: "NONE alarm", mutate: func(response *etcdserverpb.StatusResponse) { response.Errors = []string{"memberID:7 alarm:NONE"} }},
		{name: "explicit zero member", mutate: func(response *etcdserverpb.StatusResponse) { response.Errors = []string{"memberID:0 alarm:NOSPACE"} }},
		{name: "noncanonical member", mutate: func(response *etcdserverpb.StatusResponse) { response.Errors = []string{"memberID:007 alarm:NOSPACE"} }},
		{name: "noncanonical unknown alarm", mutate: func(response *etcdserverpb.StatusResponse) { response.Errors = []string{"memberID:7 alarm:0127"} }},
		{name: "duplicate alarm", mutate: func(response *etcdserverpb.StatusResponse) {
			response.Errors = []string{"memberID:7  alarm:NOSPACE", "memberID:7  alarm:NOSPACE"}
		}},
		{name: "duplicate no leader", mutate: func(response *etcdserverpb.StatusResponse) {
			response.Leader = 0
			response.Errors = []string{rpctypes.ErrNoLeader.Error(), rpctypes.ErrNoLeader.Error()}
		}},
		{name: "no leader after alarm", mutate: func(response *etcdserverpb.StatusResponse) {
			response.Leader = 0
			response.Errors = []string{"memberID:7  alarm:NOSPACE", rpctypes.ErrNoLeader.Error()}
		}},
		{name: "zero leader without error", mutate: func(response *etcdserverpb.StatusResponse) { response.Leader = 0 }},
		{name: "leader with no leader error", mutate: func(response *etcdserverpb.StatusResponse) { response.Errors = []string{rpctypes.ErrNoLeader.Error()} }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			response := valid()
			if tt.mutate != nil {
				tt.mutate(response)
			}
			rec := &recordingMetrics{}
			got, err := validateStatusProxyPayload(rec, response, nil)
			if tt.valid {
				require.Same(t, response, got)
				require.NoError(t, err)
				require.Empty(t, recordedMaintenanceProxyIntegrityValues(rec, maintenanceProxyRPCStatus))
				return
			}
			require.Nil(t, got)
			require.Equal(t, codes.DataLoss, status.Code(err))
			require.Equal(t, []interface{}{1}, recordedMaintenanceProxyIntegrityValues(rec, maintenanceProxyRPCStatus))
		})
	}

	wantErr := errors.New("transport failed")
	want := &etcdserverpb.StatusResponse{}
	response, err := validateStatusProxyPayload(nil, want, wantErr)
	require.Same(t, want, response)
	require.ErrorIs(t, err, wantErr)
}

func TestHashProxyPayloadValidation(t *testing.T) {
	for _, tt := range []struct {
		name     string
		response *etcdserverpb.HashResponse
		valid    bool
	}{
		{name: "positive current revision", response: &etcdserverpb.HashResponse{Header: txnHeader(1)}, valid: true},
		{name: "zero current revision", response: &etcdserverpb.HashResponse{Header: txnHeader(0)}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			rec := &recordingMetrics{}
			response, err := validateHashProxyPayload(rec, tt.response, nil)
			if tt.valid {
				require.Same(t, tt.response, response)
				require.NoError(t, err)
				require.Empty(t, recordedMaintenanceProxyIntegrityValues(rec, maintenanceProxyRPCHash))
				return
			}
			require.Nil(t, response)
			require.Equal(t, codes.DataLoss, status.Code(err))
			require.Equal(t, []interface{}{1}, recordedMaintenanceProxyIntegrityValues(rec, maintenanceProxyRPCHash))
		})
	}

	wantErr := errors.New("transport failed")
	want := &etcdserverpb.HashResponse{}
	response, err := validateHashProxyPayload(nil, want, wantErr)
	require.Same(t, want, response)
	require.ErrorIs(t, err, wantErr)
}

func TestHashKVProxyPayloadValidation(t *testing.T) {
	tests := []struct {
		name     string
		request  *etcdserverpb.HashKVRequest
		response *etcdserverpb.HashKVResponse
		valid    bool
	}{
		{name: "latest", request: &etcdserverpb.HashKVRequest{}, response: &etcdserverpb.HashKVResponse{Header: txnHeader(7), HashRevision: 7, CompactRevision: -1}, valid: true},
		{name: "historical", request: &etcdserverpb.HashKVRequest{Revision: 5}, response: &etcdserverpb.HashKVResponse{Header: txnHeader(7), HashRevision: 5, CompactRevision: 3}, valid: true},
		{name: "negative empty hash", request: &etcdserverpb.HashKVRequest{Revision: -1}, response: &etcdserverpb.HashKVResponse{Header: txnHeader(7), HashRevision: -1, CompactRevision: 3}, valid: true},
		{name: "zero current revision", request: &etcdserverpb.HashKVRequest{}, response: &etcdserverpb.HashKVResponse{Header: txnHeader(0), HashRevision: 0, CompactRevision: -1}},
		{name: "latest mismatch", request: &etcdserverpb.HashKVRequest{}, response: &etcdserverpb.HashKVResponse{Header: txnHeader(7), HashRevision: 6, CompactRevision: -1}},
		{name: "historical mismatch", request: &etcdserverpb.HashKVRequest{Revision: 5}, response: &etcdserverpb.HashKVResponse{Header: txnHeader(7), HashRevision: 4, CompactRevision: 3}},
		{name: "compact below sentinel", request: &etcdserverpb.HashKVRequest{Revision: -1}, response: &etcdserverpb.HashKVResponse{Header: txnHeader(7), HashRevision: -1, CompactRevision: -2}},
		{name: "hash ahead of current", request: &etcdserverpb.HashKVRequest{Revision: 8}, response: &etcdserverpb.HashKVResponse{Header: txnHeader(7), HashRevision: 8, CompactRevision: 3}},
		{name: "compact ahead of hash", request: &etcdserverpb.HashKVRequest{Revision: 5}, response: &etcdserverpb.HashKVResponse{Header: txnHeader(7), HashRevision: 5, CompactRevision: 6}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rec := &recordingMetrics{}
			response, err := validateHashKVProxyPayload(rec, tt.request, tt.response, nil)
			if tt.valid {
				require.Same(t, tt.response, response)
				require.NoError(t, err)
				require.Empty(t, recordedMaintenanceProxyIntegrityValues(rec, maintenanceProxyRPCHashKV))
				return
			}
			require.Nil(t, response)
			require.Equal(t, codes.DataLoss, status.Code(err))
			require.Equal(t, []interface{}{1}, recordedMaintenanceProxyIntegrityValues(rec, maintenanceProxyRPCHashKV))
		})
	}

	wantErr := errors.New("transport failed")
	want := &etcdserverpb.HashKVResponse{}
	response, err := validateHashKVProxyPayload(nil, &etcdserverpb.HashKVRequest{}, want, wantErr)
	require.Same(t, want, response)
	require.ErrorIs(t, err, wantErr)
}

func TestDowngradeProxyPayloadValidation(t *testing.T) {
	for _, tt := range []struct {
		name    string
		action  etcdserverpb.DowngradeRequest_DowngradeAction
		version string
		valid   bool
	}{
		{name: "validate", action: etcdserverpb.DowngradeRequest_VALIDATE, version: ClusterVersion, valid: true},
		{name: "enable", action: etcdserverpb.DowngradeRequest_ENABLE, version: ClusterVersion, valid: true},
		{name: "cancel", action: etcdserverpb.DowngradeRequest_CANCEL, version: ClusterVersion, valid: true},
		{name: "empty version", action: etcdserverpb.DowngradeRequest_VALIDATE},
		{name: "target instead of current version", action: etcdserverpb.DowngradeRequest_ENABLE, version: "3.6"},
		{name: "unknown action", action: etcdserverpb.DowngradeRequest_DowngradeAction(127), version: ClusterVersion},
	} {
		t.Run(tt.name, func(t *testing.T) {
			rec := &recordingMetrics{}
			want := &etcdserverpb.DowngradeResponse{Header: txnHeader(1), Version: tt.version}
			response, err := validateDowngradeProxyPayload(rec, &etcdserverpb.DowngradeRequest{Action: tt.action}, want, nil)
			if tt.valid {
				require.Same(t, want, response)
				require.NoError(t, err)
				require.Empty(t, recordedMaintenanceProxyIntegrityValues(rec, maintenanceProxyRPCDowngrade))
				return
			}
			require.Nil(t, response)
			require.Equal(t, codes.DataLoss, status.Code(err))
			require.Equal(t, []interface{}{1}, recordedMaintenanceProxyIntegrityValues(rec, maintenanceProxyRPCDowngrade))
		})
	}

	wantErr := errors.New("transport failed")
	want := &etcdserverpb.DowngradeResponse{}
	response, err := validateDowngradeProxyPayload(nil, &etcdserverpb.DowngradeRequest{}, want, wantErr)
	require.Same(t, want, response)
	require.ErrorIs(t, err, wantErr)
}

func TestMaintenanceProxyIntegrityMetricsAndValidation(t *testing.T) {
	rec := &recordingMetrics{}
	initMaintenanceProxyIntegrityMetrics(rec)
	for _, rpc := range maintenanceProxyRPCs {
		response, err := validateMaintenanceProxyResult[etcdserverpb.StatusResponse](rec, rpc, nil, nil)
		require.Nil(t, response)
		require.Equal(t, codes.DataLoss, status.Code(err))
		require.ErrorContains(t, err, "neither response nor error")

		response, err = validateMaintenanceProxyResult(rec, rpc, &etcdserverpb.StatusResponse{}, errors.New("mixed"))
		require.Nil(t, response)
		require.Equal(t, codes.DataLoss, status.Code(err))
		require.ErrorContains(t, err, "both response and error")
		if rpc != maintenanceProxyRPCDefragment {
			response, err = validateMaintenanceProxyResult(rec, rpc, &etcdserverpb.StatusResponse{}, nil)
			require.Nil(t, response)
			require.Equal(t, codes.DataLoss, status.Code(err))
			require.ErrorContains(t, err, "without a header")
			response, err = validateMaintenanceProxyResult(rec, rpc, &etcdserverpb.StatusResponse{Header: txnHeader(-1)}, nil)
			require.Nil(t, response)
			require.Equal(t, codes.DataLoss, status.Code(err))
			require.ErrorContains(t, err, "negative header revision")
			require.Equal(t, []interface{}{int64(0), 1, 1, 1, 1}, recordedMaintenanceProxyIntegrityValues(rec, rpc))
		} else {
			require.Equal(t, []interface{}{int64(0), 1, 1}, recordedMaintenanceProxyIntegrityValues(rec, rpc))
		}
	}

	want := &etcdserverpb.StatusResponse{Header: &etcdserverpb.ResponseHeader{}}
	response, err := validateMaintenanceProxyResult(rec, maintenanceProxyRPCStatus, want, nil)
	require.Same(t, want, response)
	require.NoError(t, err)
	wantErr := errors.New("transport failed")
	response, err = validateMaintenanceProxyResult[etcdserverpb.StatusResponse](rec, maintenanceProxyRPCStatus, nil, wantErr)
	require.Nil(t, response)
	require.ErrorIs(t, err, wantErr)
	require.Equal(t, []interface{}{int64(0), 1, 1, 1, 1}, recordedMaintenanceProxyIntegrityValues(rec, maintenanceProxyRPCStatus))

	wantDefragment := &etcdserverpb.DefragmentResponse{}
	responseDefragment, err := validateMaintenanceProxyResult(rec, maintenanceProxyRPCDefragment, wantDefragment, nil)
	require.Same(t, wantDefragment, responseDefragment)
	require.NoError(t, err)
	require.Equal(t, []interface{}{int64(0), 1, 1}, recordedMaintenanceProxyIntegrityValues(rec, maintenanceProxyRPCDefragment))
}

func TestSnapshotFailureMetricsInitializeFixedStages(t *testing.T) {
	rec := &recordingMetrics{}
	initSnapshotFailureMetrics(rec)
	for _, stage := range snapshotFailureStages {
		emitSnapshotFailure(rec, stage)
	}
	for _, stage := range snapshotFailureStages {
		require.Equal(t, []interface{}{int64(0), 1}, recordedSnapshotFailureValues(rec, stage))
	}
	emitSnapshotAdmissionRejected(rec)
	emitSnapshotActive(rec, true)
	emitSnapshotActive(rec, false)
	require.Equal(t, []interface{}{int64(0), 1}, recordedCounterValues(rec, snapshotAdmissionRejectedMetric))
	require.Equal(t, []interface{}{int64(0), int64(1), int64(0)}, recordedGaugeValues(rec, snapshotActiveMetric))
}

func TestWatchGenerationRecoveryMetricsInitializeFixedOutcomes(t *testing.T) {
	rec := &recordingMetrics{}
	initWatchGenerationRecoveryMetrics(rec)
	for _, outcome := range watchGenerationRecoveryOutcomes {
		emitWatchGenerationRecovery(rec, outcome)
	}
	for _, outcome := range watchGenerationRecoveryOutcomes {
		require.Equal(t, []interface{}{int64(0), 1}, recordedWatchGenerationRecoveryValues(rec, outcome))
	}
}

func (r *recordingMetrics) GetGrpcServerOption() []grpc.ServerOption { return nil }

func (r *recordingMetrics) GetHttpHandlers() map[string]http.Handler { return nil }

func (r *recordingMetrics) EmitCounter(name string, value interface{}, tags ...metrics.T) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.counters = append(r.counters, recordedCounter{
		name:  name,
		value: value,
		tags:  append([]metrics.T(nil), tags...),
	})
	return nil
}

func (r *recordingMetrics) EmitGauge(name string, value interface{}, tags ...metrics.T) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.gauges = append(r.gauges, recordedGauge{
		name:  name,
		value: value,
		tags:  append([]metrics.T(nil), tags...),
	})
	return nil
}

func (r *recordingMetrics) EmitHistogram(name string, value interface{}, tags ...metrics.T) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.histograms = append(r.histograms, recordedHistogram{
		name:  name,
		value: value,
		tags:  append([]metrics.T(nil), tags...),
	})
	return nil
}

func TestEmitEtcdRequestDurationUsesUpstreamMetricNameAndLabels(t *testing.T) {
	rec := &recordingMetrics{}

	emitEtcdRequestDuration(rec, "Range", 1500*time.Millisecond, nil)
	emitEtcdRequestDuration(rec, "Put", 2*time.Second, errors.New("boom"))

	require.Len(t, rec.histograms, 2)
	require.Equal(t, "etcd.server.request.duration.seconds", rec.histograms[0].name)
	require.Equal(t, 1.5, rec.histograms[0].value)
	require.Equal(t, []metrics.T{
		metrics.Tag("type", "Range"),
		metrics.Tag("success", "true"),
	}, rec.histograms[0].tags)

	require.Equal(t, "etcd.server.request.duration.seconds", rec.histograms[1].name)
	require.Equal(t, 2.0, rec.histograms[1].value)
	require.Equal(t, []metrics.T{
		metrics.Tag("type", "Put"),
		metrics.Tag("success", "false"),
	}, rec.histograms[1].tags)
}

func TestUnaryRequestDurationCoversUpstreamRequestTypesAndFailures(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()
	rec := &recordingMetrics{}
	server.metricCli = rec

	tests := []struct {
		name       string
		method     string
		request    any
		wantType   string
		handlerErr error
	}{
		{name: "range", method: etcdserverpb.KV_Range_FullMethodName, request: &etcdserverpb.RangeRequest{}, wantType: "Range"},
		{name: "put", method: etcdserverpb.KV_Put_FullMethodName, request: &etcdserverpb.PutRequest{}, wantType: "Put"},
		{name: "delete range", method: etcdserverpb.KV_DeleteRange_FullMethodName, request: &etcdserverpb.DeleteRangeRequest{}, wantType: "DeleteRange"},
		{name: "readonly txn", method: etcdserverpb.KV_Txn_FullMethodName, request: &etcdserverpb.TxnRequest{}, wantType: "ReadonlyTxn"},
		{name: "write txn", method: etcdserverpb.KV_Txn_FullMethodName, request: &etcdserverpb.TxnRequest{Success: []*etcdserverpb.RequestOp{{Request: &etcdserverpb.RequestOp_RequestPut{RequestPut: &etcdserverpb.PutRequest{Key: []byte("key")}}}}}, wantType: "Txn"},
		{name: "compact", method: etcdserverpb.KV_Compact_FullMethodName, request: &etcdserverpb.CompactionRequest{}, wantType: "Compaction"},
		{name: "lease grant", method: etcdserverpb.Lease_LeaseGrant_FullMethodName, request: &etcdserverpb.LeaseGrantRequest{}, wantType: "LeaseGrant"},
		{name: "lease revoke", method: etcdserverpb.Lease_LeaseRevoke_FullMethodName, request: &etcdserverpb.LeaseRevokeRequest{}, wantType: "LeaseRevoke"},
		{name: "alarm failure", method: etcdserverpb.Maintenance_Alarm_FullMethodName, request: &etcdserverpb.AlarmRequest{}, wantType: "Alarm", handlerErr: errors.New("alarm failed")},
		{name: "authenticate", method: etcdserverpb.Auth_Authenticate_FullMethodName, request: &etcdserverpb.AuthenticateRequest{}, wantType: "Authenticate"},
		{name: "auth enable", method: etcdserverpb.Auth_AuthEnable_FullMethodName, request: &etcdserverpb.AuthEnableRequest{}, wantType: "AuthEnable"},
		{name: "auth disable", method: etcdserverpb.Auth_AuthDisable_FullMethodName, request: &etcdserverpb.AuthDisableRequest{}, wantType: "AuthDisable"},
		{name: "auth status", method: etcdserverpb.Auth_AuthStatus_FullMethodName, request: &etcdserverpb.AuthStatusRequest{}, wantType: "AuthStatus"},
		{name: "auth user add", method: etcdserverpb.Auth_UserAdd_FullMethodName, request: &etcdserverpb.AuthUserAddRequest{}, wantType: "AuthUserAdd"},
		{name: "auth user get", method: etcdserverpb.Auth_UserGet_FullMethodName, request: &etcdserverpb.AuthUserGetRequest{}, wantType: "AuthUserGet"},
		{name: "auth user list", method: etcdserverpb.Auth_UserList_FullMethodName, request: &etcdserverpb.AuthUserListRequest{}, wantType: "AuthUserList"},
		{name: "auth user delete", method: etcdserverpb.Auth_UserDelete_FullMethodName, request: &etcdserverpb.AuthUserDeleteRequest{}, wantType: "AuthUserDelete"},
		{name: "auth user change password", method: etcdserverpb.Auth_UserChangePassword_FullMethodName, request: &etcdserverpb.AuthUserChangePasswordRequest{}, wantType: "AuthUserChangePassword"},
		{name: "auth user grant role", method: etcdserverpb.Auth_UserGrantRole_FullMethodName, request: &etcdserverpb.AuthUserGrantRoleRequest{}, wantType: "AuthUserGrantRole"},
		{name: "auth user revoke role", method: etcdserverpb.Auth_UserRevokeRole_FullMethodName, request: &etcdserverpb.AuthUserRevokeRoleRequest{}, wantType: "AuthUserRevokeRole"},
		{name: "auth role add", method: etcdserverpb.Auth_RoleAdd_FullMethodName, request: &etcdserverpb.AuthRoleAddRequest{}, wantType: "AuthRoleAdd"},
		{name: "auth role get", method: etcdserverpb.Auth_RoleGet_FullMethodName, request: &etcdserverpb.AuthRoleGetRequest{}, wantType: "AuthRoleGet"},
		{name: "auth role list", method: etcdserverpb.Auth_RoleList_FullMethodName, request: &etcdserverpb.AuthRoleListRequest{}, wantType: "AuthRoleList"},
		{name: "auth role delete", method: etcdserverpb.Auth_RoleDelete_FullMethodName, request: &etcdserverpb.AuthRoleDeleteRequest{}, wantType: "AuthRoleDelete"},
		{name: "auth role grant permission", method: etcdserverpb.Auth_RoleGrantPermission_FullMethodName, request: &etcdserverpb.AuthRoleGrantPermissionRequest{}, wantType: "AuthRoleGrantPermission"},
		{name: "auth role revoke permission", method: etcdserverpb.Auth_RoleRevokePermission_FullMethodName, request: &etcdserverpb.AuthRoleRevokePermissionRequest{}, wantType: "AuthRoleRevokePermission"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			rec.histograms = nil
			_, err := server.stampUnary(context.Background(), test.request, &grpc.UnaryServerInfo{FullMethod: test.method},
				func(context.Context, any) (any, error) {
					return &etcdserverpb.RangeResponse{Header: &etcdserverpb.ResponseHeader{}}, test.handlerErr
				})
			if test.handlerErr == nil {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
			}
			require.Len(t, rec.histograms, 1)
			require.Equal(t, "etcd.server.request.duration.seconds", rec.histograms[0].name)
			require.Equal(t, test.wantType, rec.histograms[0].tags[0].Value)
			require.Equal(t, strconv.FormatBool(test.handlerErr == nil), rec.histograms[0].tags[1].Value)
		})
	}
}

func TestLeaseCheckpointRequestDurationCoversPersistAndRenewClear(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()
	rec := &recordingMetrics{}
	server.metricCli = rec
	const leaseID int64 = 416_900

	_, err := server.LeaseGrant(context.Background(), &etcdserverpb.LeaseGrantRequest{TTL: 600, ID: leaseID})
	require.NoError(t, err)
	server.leaseMu.Lock()
	state := server.leases[leaseID]
	require.NotNil(t, state)
	state.timer.Stop()
	state.checkpointTimer.Stop()
	state.deadline = time.Now().Add(240 * time.Second)
	server.leaseMu.Unlock()

	server.checkpointLease(leaseID)
	stream := &fakeLeaseKeepAliveServer{requests: []*etcdserverpb.LeaseKeepAliveRequest{{ID: leaseID}}}
	require.NoError(t, server.LeaseKeepAlive(stream))

	var got []string
	for _, histogram := range rec.histograms {
		if histogram.name == "etcd.server.request.duration.seconds" {
			got = append(got, histogram.tags[0].Value+"/"+histogram.tags[1].Value)
		}
	}
	require.Equal(t, []string{"LeaseCheckpoint/true", "LeaseCheckpoint/true"}, got)
}

func TestLeaseCheckpointDurationsShareFailureClassification(t *testing.T) {
	rec := &recordingMetrics{}
	emitEtcdLeaseCheckpointDurations(rec, 1500*time.Millisecond, errors.New("checkpoint failed"))

	require.Equal(t, []recordedHistogram{
		{name: "etcd.server.apply_duration_seconds", value: 1.5, tags: []metrics.T{
			metrics.Tag("version", "v3"), metrics.Tag("op", "LeaseCheckpoint"), metrics.Tag("success", "false"),
		}},
		{name: "etcd.server.request.duration.seconds", value: 1.5, tags: []metrics.T{
			metrics.Tag("type", "LeaseCheckpoint"), metrics.Tag("success", "false"),
		}},
	}, rec.histograms)
}

func TestEmitEtcdRangeDurationUsesUpstreamMetricNameAndLabels(t *testing.T) {
	rec := &recordingMetrics{}

	emitEtcdRangeDuration(rec, 1500*time.Millisecond, nil)
	emitEtcdRangeDuration(rec, 2*time.Second, errors.New("boom"))

	require.Equal(t, []recordedHistogram{
		{name: "etcd.server.range_duration_seconds", value: 1.5, tags: []metrics.T{metrics.Tag("success", "true")}},
		{name: "etcd.server.range_duration_seconds", value: 2.0, tags: []metrics.T{metrics.Tag("success", "false")}},
	}, rec.histograms)
}

func TestRangeStreamFailureMetricsInitializeFixedStages(t *testing.T) {
	rec := &recordingMetrics{}
	initRangeStreamFailureMetrics(rec)
	emitRangeStreamFailure(rec, rangeStreamFailureProtocol)

	require.Equal(t, []recordedCounter{
		{name: "read.range_stream.failure", value: int64(0), tags: []metrics.T{metrics.Tag("stage", "backend")}},
		{name: "read.range_stream.failure", value: int64(0), tags: []metrics.T{metrics.Tag("stage", "send")}},
		{name: "read.range_stream.failure", value: int64(0), tags: []metrics.T{metrics.Tag("stage", "protocol")}},
		{name: "read.range_stream.proxy_retry", value: int64(0)},
		{name: "read.range_stream.failure", value: 1, tags: []metrics.T{metrics.Tag("stage", "protocol")}},
	}, rec.counters)
}

func TestDeleteRangeAdmissionMetricsInitializeAuthoritativeZero(t *testing.T) {
	rec := &recordingMetrics{}
	initDeleteRangeAdmissionMetrics(rec)
	emitDeleteRangeAdmissionRejected(rec)

	require.Equal(t, []recordedCounter{
		{name: "delete_range.admission.rejected", value: int64(0)},
		{name: "delete_range.admission.rejected", value: 1},
	}, rec.counters)
}

func TestClientAdmissionMetricsInitializeFixedGuardsAndActiveGauges(t *testing.T) {
	rec := &recordingMetrics{}
	initClientAdmissionMetrics(rec)
	for _, guard := range clientAdmissionGuards {
		emitClientAdmissionRejection(rec, guard)
	}

	require.Equal(t, []recordedGauge{
		{name: "grpc.server.admission.inflight", value: int64(0)},
		{name: "watch.admission.active", value: int64(0)},
	}, rec.gauges)
	require.Equal(t, []recordedCounter{
		{name: "client.admission.rejection", value: int64(0), tags: []metrics.T{metrics.Tag("guard", "concurrency")}},
		{name: "client.admission.rejection", value: int64(0), tags: []metrics.T{metrics.Tag("guard", "rate")}},
		{name: "client.admission.rejection", value: int64(0), tags: []metrics.T{metrics.Tag("guard", "watch")}},
		{name: "client.admission.rejection", value: 1, tags: []metrics.T{metrics.Tag("guard", "concurrency")}},
		{name: "client.admission.rejection", value: 1, tags: []metrics.T{metrics.Tag("guard", "rate")}},
		{name: "client.admission.rejection", value: 1, tags: []metrics.T{metrics.Tag("guard", "watch")}},
	}, rec.counters)
}

func TestCountProxyMetricsInitializeFixedOutcomes(t *testing.T) {
	rec := &recordingMetrics{}
	initCountProxyMetrics(rec)
	for _, outcome := range countProxyOutcomes {
		emitCountProxyOutcome(rec, outcome)
	}

	require.Equal(t, []recordedCounter{
		{name: "count.proxy.outcome", value: int64(0), tags: []metrics.T{metrics.Tag("outcome", "hit")}},
		{name: "count.proxy.outcome", value: int64(0), tags: []metrics.T{metrics.Tag("outcome", "failure")}},
		{name: "count.proxy.outcome", value: int64(0), tags: []metrics.T{metrics.Tag("outcome", "quiet_skip")}},
		{name: "count.proxy.outcome", value: 1, tags: []metrics.T{metrics.Tag("outcome", "hit")}},
		{name: "count.proxy.outcome", value: 1, tags: []metrics.T{metrics.Tag("outcome", "failure")}},
		{name: "count.proxy.outcome", value: 1, tags: []metrics.T{metrics.Tag("outcome", "quiet_skip")}},
	}, rec.counters)
}

func TestEmitEtcdApplyDurationUsesCompleteUpstreamV3LabelMatrix(t *testing.T) {
	rec := &recordingMetrics{}
	ops := []string{
		"Range", "Put", "DeleteRange", "Txn", "Compaction",
		"LeaseGrant", "LeaseRevoke", "LeaseCheckpoint", "Alarm", "Authenticate",
		"AuthEnable", "AuthDisable", "unknown",
		"AuthUserAdd", "AuthUserDelete", "AuthUserChangePassword", "AuthUserGrantRole",
		"AuthUserGet", "AuthUserRevokeRole", "AuthUserList",
		"AuthRoleAdd", "AuthRoleGrantPermission", "AuthRoleGet",
		"AuthRoleRevokePermission", "AuthRoleDelete", "AuthRoleList",
	}

	for i, op := range ops {
		var err error
		if i%2 != 0 {
			err = errors.New("apply failed")
		}
		emitEtcdApplyDuration(rec, op, time.Duration(i+1)*time.Millisecond, err)
	}

	require.Len(t, rec.histograms, len(ops))
	for i, op := range ops {
		require.Equal(t, "etcd.server.apply_duration_seconds", rec.histograms[i].name)
		require.Equal(t, float64(i+1)/1000, rec.histograms[i].value)
		require.Equal(t, []metrics.T{
			metrics.Tag("version", "v3"),
			metrics.Tag("op", op),
			metrics.Tag("success", strconv.FormatBool(i%2 == 0)),
		}, rec.histograms[i].tags)
	}
}

func TestApplyDurationBracketsPostAdmissionOperationsOnly(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()
	rec := &recordingMetrics{}
	server.metricCli = rec
	ctx := context.Background()

	_, err := server.Put(ctx, &etcdserverpb.PutRequest{})
	require.Error(t, err)
	require.Empty(t, rec.histograms, "request validation happens before upstream apply")

	key := []byte("/apply-metrics/key")
	_, err = server.Put(ctx, &etcdserverpb.PutRequest{Key: key, Value: []byte("value")})
	require.NoError(t, err)
	_, err = server.Range(ctx, &etcdserverpb.RangeRequest{Key: key, Serializable: true})
	require.NoError(t, err)
	_, err = server.Txn(ctx, &etcdserverpb.TxnRequest{Success: []*etcdserverpb.RequestOp{{
		Request: &etcdserverpb.RequestOp_RequestRange{RequestRange: &etcdserverpb.RangeRequest{
			Key: key, Serializable: true,
		}},
	}}})
	require.NoError(t, err)
	_, err = server.Txn(ctx, &etcdserverpb.TxnRequest{Success: []*etcdserverpb.RequestOp{{
		Request: &etcdserverpb.RequestOp_RequestPut{RequestPut: &etcdserverpb.PutRequest{
			Key: key, Value: []byte("updated"),
		}},
	}}})
	require.NoError(t, err)
	lease, err := server.LeaseGrant(ctx, &etcdserverpb.LeaseGrantRequest{TTL: 5})
	require.NoError(t, err)
	require.NotZero(t, lease.ID)
	_, err = server.Alarm(ctx, &etcdserverpb.AlarmRequest{Action: etcdserverpb.AlarmRequest_GET})
	require.NoError(t, err)
	_, err = server.RoleList(ctx, &etcdserverpb.AuthRoleListRequest{})
	require.NoError(t, err)
	_, err = server.AuthStatus(ctx, &etcdserverpb.AuthStatusRequest{})
	require.NoError(t, err)

	var got []string
	for _, histogram := range rec.histograms {
		if histogram.name != etcdApplyDurationMetric {
			continue
		}
		got = append(got, histogram.tags[1].Value+"/"+histogram.tags[2].Value)
	}
	require.Equal(t, []string{
		"Put/true", "Txn/true", "LeaseGrant/true",
		"Alarm/true", "AuthRoleList/true", "unknown/true",
	}, got)

	rec.histograms = nil
	stream := &fakeRangeStreamServer{ctx: ctx}
	require.NoError(t, server.RangeStream(&etcdserverpb.RangeRequest{Key: key, Serializable: true}, stream))
	for _, histogram := range rec.histograms {
		require.NotEqual(t, etcdApplyDurationMetric, histogram.name,
			"upstream RangeStream bypasses uberApplier and must not emit apply duration")
	}

	rec.histograms = nil
	server.peers = testPeerService{noLeader: true}
	_, err = server.Put(ctx, &etcdserverpb.PutRequest{Key: []byte("follower"), Value: []byte("value")})
	require.Error(t, err)
	require.Empty(t, rec.histograms, "follower rejection is not a local apply")
}

func TestApplyDurationTreatsCompactedAsSuccessfulApply(t *testing.T) {
	rec := &recordingMetrics{}
	emitEtcdApplyDuration(rec, "Range", time.Millisecond, rpctypes.ErrGRPCCompacted)
	require.Equal(t, "true", rec.histograms[0].tags[2].Value)
}

func TestApplyDurationIncludesApplyAuthorizationButExcludesInvalidAuthInfo(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()
	aliceCtx := setupAuthKVUser(t, server)
	rec := &recordingMetrics{}
	server.metricCli = rec

	_, err := server.Put(aliceCtx, &etcdserverpb.PutRequest{Key: []byte("/denied/key"), Value: []byte("value")})
	require.ErrorIs(t, err, rpctypes.ErrPermissionDenied)
	_, err = server.Put(context.Background(), &etcdserverpb.PutRequest{Key: []byte("/anonymous/key"), Value: []byte("value")})
	require.ErrorIs(t, err, rpctypes.ErrUserEmpty)
	invalidCtx := metadata.NewIncomingContext(context.Background(), metadata.Pairs(
		rpctypes.TokenFieldNameGRPC, "invalid-token",
	))
	_, err = server.Put(invalidCtx, &etcdserverpb.PutRequest{Key: []byte("/invalid/key"), Value: []byte("value")})
	require.ErrorIs(t, err, rpctypes.ErrInvalidAuthToken)
	_, err = server.UserGet(aliceCtx, &etcdserverpb.AuthUserGetRequest{Name: "root"})
	require.ErrorIs(t, err, rpctypes.ErrPermissionDenied)
	_, err = server.RoleGet(aliceCtx, &etcdserverpb.AuthRoleGetRequest{Role: "root"})
	require.ErrorIs(t, err, rpctypes.ErrPermissionDenied)

	var got []string
	for _, histogram := range rec.histograms {
		if histogram.name == etcdApplyDurationMetric {
			got = append(got, histogram.tags[1].Value+"/"+histogram.tags[2].Value)
		}
	}
	require.Equal(t, []string{
		"Put/false", "Put/false", "AuthUserGet/false", "AuthRoleGet/false",
	}, got, "invalid authentication metadata must fail before the apply observer")
}

func TestBackendShimObservesPointRangeAndCountMVCCReads(t *testing.T) {
	rec := &recordingMetrics{}
	shim := NewBackendShim(&rangeRevisionProbeBackend{}, rec)

	_, err := shim.Get(context.Background(), &etcdserverpb.RangeRequest{Key: []byte("point")})
	require.NoError(t, err)
	_, err = shim.List(context.Background(), &etcdserverpb.RangeRequest{Key: []byte("a"), RangeEnd: []byte("z")})
	require.NoError(t, err)
	_, err = shim.Count(context.Background(), &etcdserverpb.RangeRequest{
		Key: []byte("a"), RangeEnd: []byte("z"), CountOnly: true,
	})
	require.NoError(t, err)

	require.Len(t, rec.histograms, 3)
	for _, observed := range rec.histograms {
		require.Equal(t, etcdRangeDurationMetric, observed.name)
		require.Equal(t, []metrics.T{metrics.Tag("success", "true")}, observed.tags)
		require.GreaterOrEqual(t, observed.value.(float64), 0.0)
	}
}

type countMetricFallbackBackend struct{ rangeRevisionProbeBackend }

func (b *countMetricFallbackBackend) CountAtRevision(context.Context, []byte, []byte, uint64) (int64, bool) {
	return 0, false
}

func TestBackendShimObservesCountFallbackExactlyOnce(t *testing.T) {
	for _, revision := range []int64{0, 7} {
		t.Run(strconv.FormatInt(revision, 10), func(t *testing.T) {
			rec := &recordingMetrics{}
			shim := NewBackendShim(&countMetricFallbackBackend{}, rec)

			_, err := shim.Count(context.Background(), &etcdserverpb.RangeRequest{
				Key: []byte("a"), RangeEnd: []byte("z"), Revision: revision, CountOnly: true,
			})
			require.NoError(t, err)
			require.Len(t, rec.histograms, 1)
			require.Equal(t, etcdRangeDurationMetric, rec.histograms[0].name)
			require.Equal(t, []metrics.T{metrics.Tag("success", "true")}, rec.histograms[0].tags)
		})
	}
}

func TestEmitWatchSendLoopDurationsUseUpstreamMetricNames(t *testing.T) {
	rec := &recordingMetrics{}

	emitWatchSendLoopWatchStreamDuration(rec, 2*time.Second, 4)
	emitWatchSendLoopControlStreamDuration(rec, 1500*time.Millisecond)
	emitWatchSendLoopProgressDuration(rec, 250*time.Millisecond)

	require.Len(t, rec.histograms, 4)
	require.Equal(t, "etcd_debugging.server.watch_send_loop.watch_stream.duration.seconds", rec.histograms[0].name)
	require.Equal(t, 2.0, rec.histograms[0].value)
	require.Empty(t, rec.histograms[0].tags)
	require.Equal(t, "etcd_debugging.server.watch_send_loop.watch_stream.duration_per_event.seconds", rec.histograms[1].name)
	require.Equal(t, 0.5, rec.histograms[1].value)
	require.Empty(t, rec.histograms[1].tags)
	require.Equal(t, "etcd_debugging.server.watch_send_loop.control_stream.duration.seconds", rec.histograms[2].name)
	require.Equal(t, 1.5, rec.histograms[2].value)
	require.Empty(t, rec.histograms[2].tags)
	require.Equal(t, "etcd_debugging.server.watch_send_loop.progress.duration.seconds", rec.histograms[3].name)
	require.Equal(t, 0.25, rec.histograms[3].value)
	require.Empty(t, rec.histograms[3].tags)
}

func TestEtcdMVCCHashDurationHistogramsUseUpstreamMetricNames(t *testing.T) {
	rec := &recordingMetrics{}

	emitEtcdMVCCHashDuration(rec, 1500*time.Millisecond)
	emitEtcdMVCCHashRevDuration(rec, 2*time.Second)

	require.Equal(t, []recordedHistogram{
		{name: "etcd.mvcc.hash_duration_seconds", value: 1.5},
		{name: "etcd.mvcc.hash_rev_duration_seconds", value: 2.0},
	}, rec.histograms)
}

func TestDefragmentNoOpKeepsUpstreamPhysicalMetricsAtZero(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()
	rec := &recordingMetrics{}
	server.metricCli = rec
	initEtcdBackendDefragMetrics(rec)

	response, err := server.Defragment(context.Background(), &etcdserverpb.DefragmentRequest{})
	require.NoError(t, err)
	require.NotNil(t, response)
	require.Equal(t, []recordedGauge{{name: etcdDefragInflightMetric, value: 0}}, rec.gauges)
	for _, histogram := range rec.histograms {
		require.NotEqual(t, etcdBackendDefragDurationMetric, histogram.name,
			"a platform no-op must not be reported as physical bbolt defragmentation")
	}
}

func TestBackendBboltCommitPhasesAreRegisteredWithoutSyntheticSamples(t *testing.T) {
	rec := &histogramRegistrationRecorder{}

	initEtcdBackendBboltCommitPhaseMetrics(rec)

	require.Equal(t, etcdBackendBboltCommitPhaseMetrics, rec.registered)
	require.Empty(t, rec.histograms,
		"TiKV commits must not be reported as embedded bbolt commit phases")
}

func TestEtcdWALMetricsAreRegisteredAtPlatformZero(t *testing.T) {
	rec := &histogramRegistrationRecorder{}

	initEtcdWALMetrics(rec)

	require.Equal(t, []string{
		"etcd.disk.wal_fsync_duration_seconds",
		"etcd.disk.wal_write_duration_seconds",
	}, rec.registered)
	require.Equal(t, []recordedGauge{{name: "etcd.disk.wal_write_bytes_total", value: 0}}, rec.gauges)
	require.Empty(t, rec.histograms)
}

func TestEtcdRaftSnapshotMetricsAreRegisteredAtPlatformZero(t *testing.T) {
	rec := &histogramRegistrationRecorder{}

	initEtcdRaftSnapshotMetrics(rec)

	require.Equal(t, []string{
		"etcd_debugging.snap.save_marshalling_duration_seconds",
		"etcd_debugging.snap.save_total_duration_seconds",
		"etcd.snap.fsync_duration_seconds",
		"etcd.snap_db.save_total_duration_seconds",
		"etcd.snap_db.fsync_duration_seconds",
	}, rec.registered)
	require.Empty(t, rec.histograms)
}

func TestMemberPromoteFailuresUseUpstreamMetricNameAndReasonLabel(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()
	rec := &recordingMetrics{}
	server.metricCli = rec
	server.SetStaticMembers([]*etcdserverpb.Member{
		{ID: 11, Name: "voter"},
		{ID: 12, Name: "learner", IsLearner: true},
	})

	for _, id := range []uint64{99, 11, 12} {
		_, err := server.MemberPromote(context.Background(), &etcdserverpb.MemberPromoteRequest{ID: id})
		require.Error(t, err)
	}

	var failures []recordedCounter
	for _, counter := range rec.counters {
		if counter.name == "etcd.server.learner_promote_failures" {
			failures = append(failures, counter)
		}
	}
	require.Len(t, failures, 3)
	require.Equal(t, metrics.Tag("Reason", rpctypes.ErrGRPCMemberNotFound.Error()), failures[0].tags[0])
	require.Equal(t, metrics.Tag("Reason", rpctypes.ErrGRPCMemberNotLearner.Error()), failures[1].tags[0])
	require.Equal(t, "Reason", failures[2].tags[0].Name)
	require.Contains(t, failures[2].tags[0].Value, "DBaaS control plane")
	for _, failure := range failures {
		require.Equal(t, 1, failure.value)
		require.Len(t, failure.tags, 1)
	}
}

func TestEtcdMVCCWatchEventCounterUsesUpstreamMetricName(t *testing.T) {
	rec := &recordingMetrics{}

	initEtcdMVCCWatchEventCounter(rec)
	emitEtcdMVCCWatchEventCounter(rec, 3)

	require.Equal(t, []recordedCounter{
		{name: "etcd_debugging.mvcc.events_total", value: 0},
		{name: "etcd_debugging.mvcc.events_total", value: 3},
	}, rec.counters)
}

func TestEtcdMVCCWatchPendingEventGaugeUsesUpstreamMetricName(t *testing.T) {
	rec := &recordingMetrics{}
	shim := &backendShim{metricCli: rec}

	initEtcdMVCCWatchPendingEventGauge(rec)
	shim.addEtcdMVCCPendingWatchEvents(4)
	shim.addEtcdMVCCPendingWatchEvents(-2)
	shim.addEtcdMVCCPendingWatchEvents(-2)

	require.Equal(t, []recordedGauge{
		{name: "etcd_debugging.mvcc.pending_events_total", value: int64(0)},
		{name: "etcd_debugging.mvcc.pending_events_total", value: int64(4)},
		{name: "etcd_debugging.mvcc.pending_events_total", value: int64(2)},
		{name: "etcd_debugging.mvcc.pending_events_total", value: int64(0)},
	}, rec.gauges)
}

func TestWatchBackendIntegrityMetricsInitializeAndPreserveLegacyCounters(t *testing.T) {
	rec := &recordingMetrics{}

	initWatchBackendIntegrityMetrics(rec)
	emitWatchBackendIntegrityFailure(rec, "invalid_result")
	emitWatchBackendIntegrityFailure(rec, "invalid_revision")

	require.Equal(t, []recordedCounter{
		{name: "watch.backend.integrity_failure", value: 0, tags: []metrics.T{metrics.Tag("kind", "invalid_result")}},
		{name: "watch.backend.integrity_failure", value: 0, tags: []metrics.T{metrics.Tag("kind", "invalid_revision")}},
		{name: "watch.backend.invalid_result", value: 1},
		{name: "watch.backend.integrity_failure", value: 1, tags: []metrics.T{metrics.Tag("kind", "invalid_result")}},
		{name: "watch.backend.invalid_revision", value: 1},
		{name: "watch.backend.integrity_failure", value: 1, tags: []metrics.T{metrics.Tag("kind", "invalid_revision")}},
	}, rec.counters)
}

func TestEtcdMVCCKeysGaugeUsesUpstreamMetricName(t *testing.T) {
	rec := &recordingMetrics{}

	initEtcdMVCCKeysGauge(rec)
	emitEtcdMVCCKeysGauge(rec, 7)

	require.Equal(t, []recordedGauge{
		{name: "etcd_debugging.mvcc.keys_total", value: int64(0)},
		{name: "mvcc.keys_total.refresh.last_success_timestamp_seconds", value: int64(0)},
		{name: "etcd_debugging.mvcc.keys_total", value: int64(7)},
	}, rec.gauges)
	require.Equal(t, []recordedCounter{
		{name: "mvcc.keys_total.refresh.miss", value: int64(0)},
	}, rec.counters)
}

func TestEtcdMVCCPutSizeGaugeUsesUpstreamMetricName(t *testing.T) {
	rec := &recordingMetrics{}
	server := &RPCServer{metricCli: rec}

	initEtcdMVCCPutSizeGauge(rec)
	server.recordEtcdMVCCPutSize([]byte("key"), []byte("value"))
	server.recordEtcdMVCCPutSize([]byte("k"), []byte("v"))

	require.Equal(t, []recordedGauge{
		{name: "etcd_debugging.mvcc.total_put_size_in_bytes", value: int64(0)},
		{name: "etcd_debugging.mvcc.total_put_size_in_bytes", value: int64(8)},
		{name: "etcd_debugging.mvcc.total_put_size_in_bytes", value: int64(10)},
	}, rec.gauges)
}

type countIndexBackendShim struct {
	BackendShim
	count  int64
	served bool
	key    []byte
	end    []byte
	rev    uint64
}

func (b *countIndexBackendShim) CountAtRevision(_ context.Context, key, end []byte, rev uint64) (int64, bool) {
	b.key = append([]byte(nil), key...)
	b.end = append([]byte(nil), end...)
	b.rev = rev
	return b.count, b.served
}

func TestRefreshMVCCKeysMetricUsesCountIndexOnly(t *testing.T) {
	rec := &recordingMetrics{}
	backend := &countIndexBackendShim{count: 3, served: true}
	server := &RPCServer{backend: backend, metricCli: rec}

	require.True(t, server.RefreshMVCCKeysMetric(context.Background()))

	require.Equal(t, []byte{0}, backend.key)
	require.Equal(t, []byte{0}, backend.end)
	require.Zero(t, backend.rev)
	require.Len(t, rec.gauges, 2)
	require.Equal(t, recordedGauge{name: "etcd_debugging.mvcc.keys_total", value: int64(3)}, rec.gauges[0])
	require.Equal(t, "mvcc.keys_total.refresh.last_success_timestamp_seconds", rec.gauges[1].name)
	require.IsType(t, int64(0), rec.gauges[1].value)
	require.Positive(t, rec.gauges[1].value)
	require.Empty(t, rec.counters)
}

func TestRefreshMVCCKeysMetricSkipsScanWhenCountIndexUnavailable(t *testing.T) {
	rec := &recordingMetrics{}
	server := &RPCServer{
		backend:   &countIndexBackendShim{served: false},
		metricCli: rec,
	}

	require.False(t, server.RefreshMVCCKeysMetric(context.Background()))

	require.Empty(t, rec.gauges)
	require.Equal(t, []recordedCounter{
		{name: "mvcc.keys_total.refresh.miss", value: 1},
	}, rec.counters)
}

func TestEtcdLeaseExpiredCounterUsesUpstreamMetricName(t *testing.T) {
	rec := &recordingMetrics{}

	initEtcdLeaseExpiredCounter(rec)
	emitEtcdLeaseExpiredCounter(rec, 1)

	require.Equal(t, []recordedCounter{
		{name: "etcd_debugging.server.lease_expired_total", value: 0},
		{name: "etcd_debugging.server.lease_expired_total", value: 1},
	}, rec.counters)
}

func TestLeaseUncertainReconcileMetricsInitializeAuthoritativeZero(t *testing.T) {
	rec := &recordingMetrics{}

	initLeaseUncertainReconcileMetrics(rec)

	require.Equal(t, []recordedCounter{
		{name: "lease.uncertain_reconcile.retry", value: int64(0)},
		{name: "lease.uncertain_reconcile.success", value: int64(0)},
		{name: "lease.uncertain_reconcile.err", value: int64(0)},
	}, rec.counters)
}

func TestLeaseBackgroundFailureMetricsUseFixedOperations(t *testing.T) {
	rec := &recordingMetrics{}

	initLeaseBackgroundFailureMetrics(rec)
	emitLeaseBackgroundFailure(rec, "checkpoint")
	emitLeaseBackgroundFailure(rec, "expire_delete")
	emitLeaseBackgroundFailure(rec, "expire_corrupt_deferred")

	require.Equal(t, []recordedCounter{
		{name: "lease.background.failure", value: int64(0), tags: []metrics.T{metrics.Tag("operation", "checkpoint")}},
		{name: "lease.background.failure", value: int64(0), tags: []metrics.T{metrics.Tag("operation", "expire_delete")}},
		{name: "lease.background.failure", value: int64(0), tags: []metrics.T{metrics.Tag("operation", "expire_corrupt_deferred")}},
		{name: "lease.background.failure", value: 1, tags: []metrics.T{metrics.Tag("operation", "checkpoint")}},
		{name: "lease.background.failure", value: 1, tags: []metrics.T{metrics.Tag("operation", "expire_delete")}},
		{name: "lease.background.failure", value: 1, tags: []metrics.T{metrics.Tag("operation", "expire_corrupt_deferred")}},
	}, rec.counters)
}

func TestLeaseStartupRestoreFailureMetricInitializesAuthoritativeZero(t *testing.T) {
	rec := &recordingMetrics{}

	initLeaseStartupRestoreFailureMetric(rec)
	emitLeaseStartupRestoreFailure(rec)

	require.Equal(t, []recordedCounter{
		{name: "lease.startup_restore.failure", value: int64(0)},
		{name: "lease.startup_restore.failure", value: 1},
	}, rec.counters)
}

func TestLeaseOrphanSweepFailureMetricsUseFixedStages(t *testing.T) {
	rec := &recordingMetrics{}

	initLeaseOrphanSweepFailureMetrics(rec)
	for _, stage := range leaseOrphanSweepFailureStages {
		emitLeaseOrphanSweepFailure(rec, stage)
	}

	require.Len(t, rec.counters, 2*len(leaseOrphanSweepFailureStages))
	for i, stage := range leaseOrphanSweepFailureStages {
		require.Equal(t, recordedCounter{
			name: "lease.orphan_sweep.failure", value: int64(0), tags: []metrics.T{metrics.Tag("stage", stage)},
		}, rec.counters[i])
		require.Equal(t, recordedCounter{
			name: "lease.orphan_sweep.failure", value: 1, tags: []metrics.T{metrics.Tag("stage", stage)},
		}, rec.counters[len(leaseOrphanSweepFailureStages)+i])
	}
}

func TestLeaseOrphanSweepFailureMetricsIgnoreCanceledWork(t *testing.T) {
	rec := &recordingMetrics{}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	emitLeaseOrphanSweepFailureForContext(ctx, rec, "load")

	require.Empty(t, rec.counters)
}

func TestLeaseGrantCleanupMetricsUseFixedOutcomes(t *testing.T) {
	rec := &recordingMetrics{}

	initLeaseGrantCleanupMetrics(rec)
	for _, outcome := range leaseGrantCleanupOutcomes {
		emitLeaseGrantCleanup(rec, outcome)
	}

	require.Len(t, rec.counters, 2*len(leaseGrantCleanupOutcomes))
	for i, outcome := range leaseGrantCleanupOutcomes {
		require.Equal(t, recordedCounter{
			name: "lease.grant_cleanup", value: int64(0), tags: []metrics.T{metrics.Tag("outcome", outcome)},
		}, rec.counters[i])
		require.Equal(t, recordedCounter{
			name: "lease.grant_cleanup", value: 1, tags: []metrics.T{metrics.Tag("outcome", outcome)},
		}, rec.counters[len(leaseGrantCleanupOutcomes)+i])
	}
}

func TestLeaseRevokeReconcileMetricsUseFixedOutcomes(t *testing.T) {
	rec := &recordingMetrics{}

	initLeaseRevokeReconcileMetrics(rec)
	for _, outcome := range leaseRevokeReconcileOutcomes {
		emitLeaseRevokeReconcile(rec, outcome)
	}

	require.Len(t, rec.counters, 2*len(leaseRevokeReconcileOutcomes))
	for i, outcome := range leaseRevokeReconcileOutcomes {
		require.Equal(t, recordedCounter{
			name: "lease.revoke_reconcile", value: int64(0), tags: []metrics.T{metrics.Tag("outcome", outcome)},
		}, rec.counters[i])
		require.Equal(t, recordedCounter{
			name: "lease.revoke_reconcile", value: 1, tags: []metrics.T{metrics.Tag("outcome", outcome)},
		}, rec.counters[len(leaseRevokeReconcileOutcomes)+i])
	}
}

func TestEtcdLeaseLifecycleMetricsUseUpstreamMetricNames(t *testing.T) {
	rec := &recordingMetrics{}

	initEtcdLeaseLifecycleMetrics(rec)
	emitEtcdLeaseGrantedCounter(rec, 1)
	emitEtcdLeaseRevokedCounter(rec, 2)
	emitEtcdLeaseRenewedCounter(rec, 3)
	emitEtcdLeaseTTLHistogram(rec, 30)

	require.Equal(t, []recordedCounter{
		{name: "etcd_debugging.lease.granted_total", value: 0},
		{name: "etcd_debugging.lease.revoked_total", value: 0},
		{name: "etcd_debugging.lease.renewed_total", value: 0},
		{name: "etcd_debugging.lease.granted_total", value: 1},
		{name: "etcd_debugging.lease.revoked_total", value: 2},
		{name: "etcd_debugging.lease.renewed_total", value: 3},
	}, rec.counters)
	require.Equal(t, []recordedHistogram{
		{name: "etcd_debugging.lease.ttl_total", value: int64(30)},
	}, rec.histograms)
}

func TestEtcdClientRequestCounterUsesUpstreamMetricNameAndLabels(t *testing.T) {
	rec := &recordingMetrics{}

	initEtcdClientRequestCounters(rec)
	emitEtcdClientRequestCounter(rec, "unary", "3.7", 1)
	emitEtcdClientRequestCounter(rec, "stream", "unknown", 2)

	require.Equal(t, []recordedCounter{
		{
			name:  "etcd.server.client_requests_total",
			value: 0,
			tags: []metrics.T{
				metrics.Tag("type", "unary"),
				metrics.Tag("client_api_version", "unknown"),
			},
		},
		{
			name:  "etcd.server.client_requests_total",
			value: 0,
			tags: []metrics.T{
				metrics.Tag("type", "stream"),
				metrics.Tag("client_api_version", "unknown"),
			},
		},
		{
			name:  "etcd.server.client_requests_total",
			value: 1,
			tags: []metrics.T{
				metrics.Tag("type", "unary"),
				metrics.Tag("client_api_version", "3.7"),
			},
		},
		{
			name:  "etcd.server.client_requests_total",
			value: 2,
			tags: []metrics.T{
				metrics.Tag("type", "stream"),
				metrics.Tag("client_api_version", "unknown"),
			},
		},
	}, rec.counters)
}

func TestEtcdClientGRPCBytesCountersUseUpstreamMetricNames(t *testing.T) {
	rec := &recordingMetrics{}

	initEtcdClientGRPCBytesCounters(rec)
	handler := newEtcdClientGRPCBytesStatsHandler(rec)
	handler.HandleRPC(context.Background(), &stats.InPayload{Length: 17})
	handler.HandleRPC(context.Background(), &stats.OutPayload{Length: 29})

	require.Equal(t, []recordedCounter{
		{name: "etcd.network.client_grpc_received_bytes_total", value: 0},
		{name: "etcd.network.client_grpc_sent_bytes_total", value: 0},
		{name: "etcd.network.client_grpc_received_bytes_total", value: 17},
		{name: "etcd.network.client_grpc_sent_bytes_total", value: 29},
	}, rec.counters)
}

func TestEtcdServerStreamFailureCounterUsesUpstreamMetricNameAndLabels(t *testing.T) {
	rec := &recordingMetrics{}

	initEtcdServerStreamFailureCounters(rec)
	emitEtcdServerStreamFailureCounter(rec, "send", "watch", 2)

	require.Equal(t, []recordedCounter{
		{
			name:  "etcd.network.server_stream_failures_total",
			value: 0,
			tags: []metrics.T{
				metrics.Tag("Type", "receive"),
				metrics.Tag("API", "watch"),
			},
		},
		{
			name:  "etcd.network.server_stream_failures_total",
			value: 0,
			tags: []metrics.T{
				metrics.Tag("Type", "send"),
				metrics.Tag("API", "watch"),
			},
		},
		{
			name:  "etcd.network.server_stream_failures_total",
			value: 0,
			tags: []metrics.T{
				metrics.Tag("Type", "receive"),
				metrics.Tag("API", "lease-keepalive"),
			},
		},
		{
			name:  "etcd.network.server_stream_failures_total",
			value: 0,
			tags: []metrics.T{
				metrics.Tag("Type", "send"),
				metrics.Tag("API", "lease-keepalive"),
			},
		},
		{
			name:  "etcd.network.server_stream_failures_total",
			value: 2,
			tags: []metrics.T{
				metrics.Tag("Type", "send"),
				metrics.Tag("API", "watch"),
			},
		},
	}, rec.counters)
}
