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
	"net/http"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/stats"

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

func TestEmitEtcdRangeDurationUsesUpstreamMetricNameAndLabels(t *testing.T) {
	rec := &recordingMetrics{}

	emitEtcdRangeDuration(rec, 1500*time.Millisecond, nil)
	emitEtcdRangeDuration(rec, 2*time.Second, errors.New("boom"))

	require.Equal(t, []recordedHistogram{
		{name: "etcd.server.range_duration_seconds", value: 1.5, tags: []metrics.T{metrics.Tag("success", "true")}},
		{name: "etcd.server.range_duration_seconds", value: 2.0, tags: []metrics.T{metrics.Tag("success", "false")}},
	}, rec.histograms)
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

func TestBackendShimObservesPointAndRangeMVCCReads(t *testing.T) {
	rec := &recordingMetrics{}
	shim := NewBackendShim(&rangeRevisionProbeBackend{}, rec)

	_, err := shim.Get(context.Background(), &etcdserverpb.RangeRequest{Key: []byte("point")})
	require.NoError(t, err)
	_, err = shim.List(context.Background(), &etcdserverpb.RangeRequest{Key: []byte("a"), RangeEnd: []byte("z")})
	require.NoError(t, err)

	require.Len(t, rec.histograms, 2)
	for _, observed := range rec.histograms {
		require.Equal(t, etcdRangeDurationMetric, observed.name)
		require.Equal(t, []metrics.T{metrics.Tag("success", "true")}, observed.tags)
		require.GreaterOrEqual(t, observed.value.(float64), 0.0)
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

func TestEtcdMVCCKeysGaugeUsesUpstreamMetricName(t *testing.T) {
	rec := &recordingMetrics{}

	initEtcdMVCCKeysGauge(rec)
	emitEtcdMVCCKeysGauge(rec, 7)

	require.Equal(t, []recordedGauge{
		{name: "etcd_debugging.mvcc.keys_total", value: int64(0)},
		{name: "etcd_debugging.mvcc.keys_total", value: int64(7)},
	}, rec.gauges)
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

	require.Equal(t, []byte(nil), backend.key)
	require.Equal(t, []byte{0}, backend.end)
	require.Zero(t, backend.rev)
	require.Equal(t, []recordedGauge{
		{name: "etcd_debugging.mvcc.keys_total", value: int64(3)},
	}, rec.gauges)
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
