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
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	"github.com/kubewharf/kubebrain/pkg/backend"
	"github.com/kubewharf/kubebrain/pkg/metrics"
	"github.com/kubewharf/kubebrain/pkg/metrics/mock"
)

type writeAfterHashBackendShim struct {
	BackendShim
	t      *testing.T
	result backend.HashKVResult
}

type compactAfterHashBackendShim struct {
	BackendShim
	t      *testing.T
	result backend.HashKVResult
}

type compactBeforeHashBackendShim struct {
	BackendShim
	t      *testing.T
	target uint64
}

type alarmReadErrorBackendShim struct {
	BackendShim
	err error
}

type quotaStatusErrorBackendShim struct {
	BackendShim
	err error
}

type requireSyncBeforeHashBackendShim struct {
	BackendShim
	t      *testing.T
	synced func() bool
}

func (b *alarmReadErrorBackendShim) NoSpaceAlarms(context.Context) ([]uint64, error) {
	return nil, b.err
}

func (b *quotaStatusErrorBackendShim) QuotaStatus(context.Context) (int64, int64, bool, error) {
	return 0, 0, false, b.err
}

func (b *requireSyncBeforeHashBackendShim) HashKV(ctx context.Context, revision int64) (backend.HashKVResult, error) {
	require.True(b.t, b.synced(), "peer HashKV should refresh the revision cache before hashing")
	return b.BackendShim.HashKV(ctx, revision)
}

func (b *compactBeforeHashBackendShim) HashKV(ctx context.Context, revision int64) (backend.HashKVResult, error) {
	_, err := b.BackendShim.CompactAsync(ctx, b.target)
	require.NoError(b.t, err)
	return b.BackendShim.HashKV(ctx, revision)
}

func (b *compactAfterHashBackendShim) HashKV(ctx context.Context, revision int64) (backend.HashKVResult, error) {
	result, err := b.BackendShim.HashKV(ctx, revision)
	if err != nil {
		return backend.HashKVResult{}, err
	}
	b.result = result
	_, err = b.BackendShim.CompactAsync(ctx, uint64(result.HashRevision))
	require.NoError(b.t, err)
	return result, nil
}

func (b *writeAfterHashBackendShim) HashKV(ctx context.Context, revision int64) (backend.HashKVResult, error) {
	result, err := b.BackendShim.HashKV(ctx, revision)
	if err != nil {
		return backend.HashKVResult{}, err
	}
	b.result = result
	_, err = b.BackendShim.Put(ctx, &etcdserverpb.PutRequest{
		Key:   []byte("/registry/maintenance/write-after-hash"),
		Value: []byte(fmt.Sprintf("after-%d", result.HashRevision)),
	})
	require.NoError(b.t, err)
	return result, nil
}

// TestStatusVersionEnablesRequestWatchProgress guards the exact gate the
// kube-apiserver applies: Maintenance.Status.Version must be semver-parseable
// and satisfy >= 3.5.13 (or [3.4.31, 3.5.0)) or RequestWatchProgress —
// consistent-list-from-cache / WatchList — stays disabled.
// See k8s.io/apiserver/pkg/storage/feature/feature_support_checker.go.
func TestStatusVersionEnablesRequestWatchProgress(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	resp, err := server.Status(context.Background(), &etcdserverpb.StatusRequest{})
	require.NoError(t, err)

	var maj, min, patch int
	n, err := fmt.Sscanf(resp.Version, "%d.%d.%d", &maj, &min, &patch)
	require.NoError(t, err, "Status.Version %q must be semver-parseable", resp.Version)
	require.Equal(t, 3, n, "Status.Version %q must be major.minor.patch", resp.Version)

	v := maj*1_000_000 + min*1_000 + patch
	ge3_4_31 := v >= 3*1_000_000+4*1_000+31
	lt3_5_0 := v < 3*1_000_000+5*1_000+0
	ge3_5_13 := v >= 3*1_000_000+5*1_000+13
	supported := ge3_5_13 || (ge3_4_31 && lt3_5_0)
	require.True(t, supported,
		"Status.Version %q does not satisfy the apiserver RequestWatchProgress gate (>=3.5.13 or [3.4.31,3.5.0))", resp.Version)
}

func TestVersionMetricsMatchAdvertisedProtocolVersions(t *testing.T) {
	ctrl := gomock.NewController(t)
	metricCli := mock.NewMockMetrics(ctrl)
	metricCli.EXPECT().EmitGauge(
		"etcd.server.version", 1, metrics.Tag("server_version", Version),
	).Return(nil)
	metricCli.EXPECT().EmitGauge(
		"etcd.cluster.version", 1, metrics.Tag("cluster_version", ClusterVersion),
	).Return(nil)

	emitVersionMetrics(metricCli)
	require.Equal(t, Version[:len(ClusterVersion)], ClusterVersion)
	require.Equal(t, byte('.'), Version[len(ClusterVersion)])
}

func TestMaintenanceBasicDiagnostics(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	ctx := context.Background()
	put1, err := server.Put(ctx, &etcdserverpb.PutRequest{
		Key:   []byte("/registry/maintenance/key"),
		Value: []byte("v1"),
	})
	require.NoError(t, err)

	statusResp, err := server.Status(ctx, &etcdserverpb.StatusRequest{})
	require.NoError(t, err)
	require.Equal(t, Version, statusResp.Version)
	require.Equal(t, Version, statusResp.StorageVersion)
	require.NotNil(t, statusResp.Header)
	require.NotNil(t, statusResp.DowngradeInfo)
	require.False(t, statusResp.DowngradeInfo.Enabled)
	require.EqualValues(t, 1, statusResp.DbSize)
	require.EqualValues(t, 1, statusResp.DbSizeInUse)
	require.Equal(t, defaultEtcdBackendQuota, statusResp.DbSizeQuota)
	require.Equal(t, uint64(1), statusResp.RaftTerm)

	hashResp, err := server.HashKV(ctx, &etcdserverpb.HashKVRequest{})
	require.NoError(t, err)
	require.NotZero(t, hashResp.Hash)
	require.Equal(t, put1.Header.Revision, hashResp.HashRevision)
	require.Equal(t, int64(-1), hashResp.CompactRevision)

	negativeHash, err := server.HashKV(ctx, &etcdserverpb.HashKVRequest{Revision: -1})
	require.NoError(t, err)
	require.Equal(t, int64(-1), negativeHash.HashRevision)
	require.Equal(t, hashResp.Header.Revision, negativeHash.Header.Revision)
	require.Equal(t, int64(-1), negativeHash.CompactRevision)
	require.Equal(t, uint32(0x40a4756d), negativeHash.Hash)
	require.NotEqual(t, hashResp.Hash, negativeHash.Hash)

	_, err = server.Put(ctx, &etcdserverpb.PutRequest{
		Key:   []byte("/registry/maintenance/key"),
		Value: []byte("v2"),
	})
	require.NoError(t, err)
	hashAfterUpdate, err := server.HashKV(ctx, &etcdserverpb.HashKVRequest{})
	require.NoError(t, err)
	require.NotEqual(t, hashResp.Hash, hashAfterUpdate.Hash)

	historical, err := server.HashKV(ctx, &etcdserverpb.HashKVRequest{
		Revision: put1.Header.Revision,
	})
	require.NoError(t, err)
	require.Equal(t, hashResp.Hash, historical.Hash)
	require.Equal(t, put1.Header.Revision, historical.HashRevision)

	alarmResp, err := server.Alarm(ctx, &etcdserverpb.AlarmRequest{})
	require.NoError(t, err)
	require.Empty(t, alarmResp.Alarms)
	require.Equal(t, int64(server.backend.GetCurrentRevision()), alarmResp.Header.Revision)

	defragResp, err := server.Defragment(ctx, &etcdserverpb.DefragmentRequest{})
	require.NoError(t, err)
	require.Nil(t, defragResp.Header)
}

func TestMaintenanceHashKVFutureRevisionMatchesEtcd(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	ctx := context.Background()
	put, err := server.Put(ctx, &etcdserverpb.PutRequest{
		Key: []byte("/registry/maintenance/hashkv-future"), Value: []byte("value"),
	})
	require.NoError(t, err)

	tests := []struct {
		name     string
		revision int64
	}{
		{name: "future", revision: put.Header.Revision + 1},
		{name: "max-int", revision: math.MaxInt64},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := server.HashKV(ctx, &etcdserverpb.HashKVRequest{Revision: tt.revision})
			requireMaintenanceDirectError(t, err, rpctypes.ErrGRPCFutureRev, codes.OutOfRange, "etcdserver: mvcc: required revision is a future revision")
		})
	}
}

func TestMaintenanceHashStableUntilWriteAndChangesAfterWrite(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	ctx := context.Background()
	put, err := server.Put(ctx, &etcdserverpb.PutRequest{
		Key: []byte("/registry/maintenance/hash-stability"), Value: []byte("before"),
	})
	require.NoError(t, err)

	first, err := server.Hash(ctx, &etcdserverpb.HashRequest{})
	require.NoError(t, err)
	require.NotZero(t, first.Hash)
	require.GreaterOrEqual(t, first.Header.Revision, put.Header.Revision)

	second, err := server.Hash(ctx, &etcdserverpb.HashRequest{})
	require.NoError(t, err)
	require.Equal(t, first.Header.Revision, second.Header.Revision)
	require.Equal(t, first.Hash, second.Hash)

	update, err := server.Put(ctx, &etcdserverpb.PutRequest{
		Key: []byte("/registry/maintenance/hash-stability"), Value: []byte("after"),
	})
	require.NoError(t, err)
	third, err := server.Hash(ctx, &etcdserverpb.HashRequest{})
	require.NoError(t, err)
	require.GreaterOrEqual(t, third.Header.Revision, update.Header.Revision)
	require.Greater(t, third.Header.Revision, second.Header.Revision)
	require.NotEqual(t, second.Hash, third.Hash)
}

func TestAlarmGetFiltersUnknownAlarmAndMaxMemberLikeEtcd(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	ctx := context.Background()
	put, err := server.Put(ctx, &etcdserverpb.PutRequest{
		Key: []byte("/registry/maintenance/alarm-get-filter"), Value: []byte("value"),
	})
	require.NoError(t, err)

	for _, tc := range []struct {
		name     string
		memberID uint64
		alarm    etcdserverpb.AlarmType
	}{
		{name: "all"},
		{name: "nospace", alarm: etcdserverpb.AlarmType_NOSPACE},
		{name: "corrupt", alarm: etcdserverpb.AlarmType_CORRUPT},
		{name: "unknown-alarm", alarm: etcdserverpb.AlarmType(127)},
		{name: "max-member", memberID: math.MaxUint64},
	} {
		t.Run(tc.name, func(t *testing.T) {
			resp, err := server.Alarm(ctx, &etcdserverpb.AlarmRequest{
				Action: etcdserverpb.AlarmRequest_GET, MemberID: tc.memberID, Alarm: tc.alarm,
			})
			require.NoError(t, err)
			require.Empty(t, resp.Alarms)
			require.NotNil(t, resp.Header)
			require.GreaterOrEqual(t, resp.Header.Revision, put.Header.Revision)
		})
	}
}

func TestAlarmGetIsolatesFiltersFromUnrelatedQuotaFailure(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	ctx := context.Background()
	const memberID = uint64(42)
	_, err := server.Alarm(ctx, &etcdserverpb.AlarmRequest{
		Action:   etcdserverpb.AlarmRequest_ACTIVATE,
		Alarm:    etcdserverpb.AlarmType_CORRUPT,
		MemberID: memberID,
	})
	require.NoError(t, err)

	wantErr := errors.New("quota metadata unavailable")
	server.backend = &quotaStatusErrorBackendShim{BackendShim: server.backend, err: wantErr}

	corrupt, err := server.Alarm(ctx, &etcdserverpb.AlarmRequest{
		Action: etcdserverpb.AlarmRequest_GET,
		Alarm:  etcdserverpb.AlarmType_CORRUPT,
	})
	require.NoError(t, err)
	require.Equal(t, []*etcdserverpb.AlarmMember{{
		MemberID: memberID,
		Alarm:    etcdserverpb.AlarmType_CORRUPT,
	}}, corrupt.Alarms)

	unknown, err := server.Alarm(ctx, &etcdserverpb.AlarmRequest{
		Action: etcdserverpb.AlarmRequest_GET,
		Alarm:  etcdserverpb.AlarmType(127),
	})
	require.NoError(t, err)
	require.Empty(t, unknown.Alarms)

	for _, filter := range []etcdserverpb.AlarmType{
		etcdserverpb.AlarmType_NONE,
		etcdserverpb.AlarmType_NOSPACE,
	} {
		_, err := server.Alarm(ctx, &etcdserverpb.AlarmRequest{
			Action: etcdserverpb.AlarmRequest_GET,
			Alarm:  filter,
		})
		require.ErrorIs(t, err, wantErr)
	}
}

func TestPeerHashKVHandler(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	ctx := context.Background()
	put, err := server.Put(ctx, &etcdserverpb.PutRequest{
		Key:   []byte("/registry/maintenance/peer-hash"),
		Value: []byte("value"),
	})
	require.NoError(t, err)

	body, err := json.Marshal(&etcdserverpb.HashKVRequest{Revision: put.Header.Revision})
	require.NoError(t, err)
	req := httptest.NewRequest(http.MethodGet, PeerHashKVPath, bytes.NewReader(body))
	req.Header.Set(etcdClusterIDHeader, strconv.FormatUint(server.backend.ClusterID(), 16))
	rec := httptest.NewRecorder()

	server.GetPeerHttpHandlers()[PeerHashKVPath].ServeHTTP(rec, req)

	require.Equal(t, http.StatusOK, rec.Code)
	require.Equal(t, strconv.FormatUint(server.backend.ClusterID(), 16), rec.Header().Get(etcdClusterIDHeader))
	require.Equal(t, "application/json", rec.Header().Get("Content-Type"))
	var resp etcdserverpb.HashKVResponse
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	require.Equal(t, put.Header.Revision, resp.HashRevision)
	require.Equal(t, put.Header.Revision, resp.Header.Revision)
	require.NotZero(t, resp.Hash)
}

func TestPeerHashKVHandlerRefreshesRevisionBeforeHash(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	ctx := context.Background()
	put, err := server.Put(ctx, &etcdserverpb.PutRequest{
		Key: []byte("/registry/maintenance/peer-hash-refresh"), Value: []byte("value"),
	})
	require.NoError(t, err)
	synced := false
	server.peers = testPeerService{isLeader: true, syncReadFn: func(context.Context) error {
		synced = true
		return errors.New("leader unavailable")
	}}
	server.backend = &requireSyncBeforeHashBackendShim{
		BackendShim: server.backend,
		t:           t,
		synced:      func() bool { return synced },
	}

	body, err := json.Marshal(&etcdserverpb.HashKVRequest{Revision: put.Header.Revision})
	require.NoError(t, err)
	req := httptest.NewRequest(http.MethodGet, PeerHashKVPath, bytes.NewReader(body))
	rec := httptest.NewRecorder()

	server.peerHashKVHandler(rec, req)

	require.Equal(t, http.StatusOK, rec.Code)
}

func TestPeerHashKVHandlerRejectsBadPeerRequests(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	tests := []struct {
		name       string
		method     string
		target     string
		body       io.Reader
		clusterID  string
		wantStatus int
		wantBody   string
	}{
		{
			name:       "wrong method",
			method:     http.MethodPost,
			target:     PeerHashKVPath,
			body:       bytes.NewReader([]byte(`{}`)),
			wantStatus: http.StatusMethodNotAllowed,
			wantBody:   "Method Not Allowed",
		},
		{
			name:       "wrong path",
			method:     http.MethodGet,
			target:     "/members/hashkv/extra",
			body:       bytes.NewReader([]byte(`{}`)),
			wantStatus: http.StatusBadRequest,
			wantBody:   "bad path",
		},
		{
			name:       "cluster mismatch",
			method:     http.MethodGet,
			target:     PeerHashKVPath,
			body:       bytes.NewReader([]byte(`{}`)),
			clusterID:  "deadbeef",
			wantStatus: http.StatusPreconditionFailed,
			wantBody:   "cluster ID mismatch",
		},
		{
			name:       "bad json",
			method:     http.MethodGet,
			target:     PeerHashKVPath,
			body:       bytes.NewReader([]byte(`{`)),
			wantStatus: http.StatusBadRequest,
			wantBody:   "error unmarshalling request",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest(tt.method, tt.target, tt.body)
			if tt.clusterID != "" {
				req.Header.Set(etcdClusterIDHeader, tt.clusterID)
			}
			rec := httptest.NewRecorder()

			server.peerHashKVHandler(rec, req)

			require.Equal(t, tt.wantStatus, rec.Code)
			require.Contains(t, rec.Body.String(), tt.wantBody)
		})
	}
}

func TestPeerHashKVHandlerRejectsByEtcdPriority(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	clusterID := strconv.FormatUint(server.backend.ClusterID(), 16)
	tests := []struct {
		name       string
		method     string
		target     string
		clusterID  string
		wantStatus int
		wantBody   string
		wantAllow  string
	}{
		{
			name:       "method precedes path cluster and body",
			method:     http.MethodPost,
			target:     "/members/hashkv/extra",
			clusterID:  "deadbeef",
			wantStatus: http.StatusMethodNotAllowed,
			wantBody:   "Method Not Allowed\n",
			wantAllow:  http.MethodGet,
		},
		{
			name:       "path precedes cluster and body",
			method:     http.MethodGet,
			target:     "/members/hashkv/extra",
			clusterID:  "deadbeef",
			wantStatus: http.StatusBadRequest,
			wantBody:   "bad path\n",
		},
		{
			name:       "cluster precedes body",
			method:     http.MethodGet,
			target:     PeerHashKVPath,
			clusterID:  "deadbeef",
			wantStatus: http.StatusPreconditionFailed,
			wantBody:   "cluster ID mismatch\n",
		},
		{
			name:       "body follows valid admission",
			method:     http.MethodGet,
			target:     PeerHashKVPath,
			clusterID:  clusterID,
			wantStatus: http.StatusBadRequest,
			wantBody:   "error unmarshalling request\n",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest(tt.method, tt.target, bytes.NewReader([]byte(`{`)))
			req.Header.Set(etcdClusterIDHeader, tt.clusterID)
			rec := httptest.NewRecorder()

			server.peerHashKVHandler(rec, req)

			require.Equal(t, tt.wantStatus, rec.Code)
			require.Equal(t, tt.wantBody, rec.Body.String())
			require.Equal(t, tt.wantAllow, rec.Header().Get("Allow"))
			require.Equal(t, "text/plain; charset=utf-8", rec.Header().Get("Content-Type"))
			require.Equal(t, "nosniff", rec.Header().Get("X-Content-Type-Options"))
			require.Empty(t, rec.Header().Get(etcdClusterIDHeader))
		})
	}
}

func TestPeerHashKVHandlerMapsRevisionErrors(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	ctx := context.Background()
	first, err := server.Put(ctx, &etcdserverpb.PutRequest{
		Key: []byte("/registry/maintenance/peer-hash-errors"), Value: []byte("v1"),
	})
	require.NoError(t, err)
	second, err := server.Put(ctx, &etcdserverpb.PutRequest{
		Key: []byte("/registry/maintenance/peer-hash-errors"), Value: []byte("v2"),
	})
	require.NoError(t, err)
	_, err = server.backend.CompactAsync(ctx, uint64(second.Header.Revision))
	require.NoError(t, err)

	tests := []struct {
		name     string
		revision int64
		wantBody string
	}{
		{
			name:     "compacted",
			revision: first.Header.Revision,
			wantBody: "mvcc: required revision has been compacted",
		},
		{
			name:     "future",
			revision: second.Header.Revision + 100,
			wantBody: "mvcc: required revision is a future revision",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			body, err := json.Marshal(&etcdserverpb.HashKVRequest{Revision: tt.revision})
			require.NoError(t, err)
			req := httptest.NewRequest(http.MethodGet, PeerHashKVPath, bytes.NewReader(body))
			rec := httptest.NewRecorder()

			server.peerHashKVHandler(rec, req)

			require.Equal(t, http.StatusBadRequest, rec.Code)
			require.Contains(t, rec.Body.String(), tt.wantBody)
		})
	}
}

func TestCorruptAlarmBlocksEtcdApplierSurfaceOverGRPC(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	grpcServer := grpc.NewServer(server.ClientServerOptions()...)
	etcdserverpb.RegisterKVServer(grpcServer, server)
	etcdserverpb.RegisterLeaseServer(grpcServer, server)
	etcdserverpb.RegisterMaintenanceServer(grpcServer, server)
	listener := bufconn.Listen(1 << 20)
	go func() { _ = grpcServer.Serve(listener) }()
	t.Cleanup(grpcServer.Stop)
	conn, err := grpc.NewClient("passthrough:///corrupt-alarm",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return listener.Dial() }))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, conn.Close()) })

	ctx := context.Background()
	kv := etcdserverpb.NewKVClient(conn)
	lease := etcdserverpb.NewLeaseClient(conn)
	maintenance := etcdserverpb.NewMaintenanceClient(conn)
	const memberID = uint64(42)
	activated, err := maintenance.Alarm(ctx, &etcdserverpb.AlarmRequest{
		Action: etcdserverpb.AlarmRequest_ACTIVATE, Alarm: etcdserverpb.AlarmType_CORRUPT, MemberID: memberID,
	})
	require.NoError(t, err)
	require.Len(t, activated.Alarms, 1)
	require.Equal(t, memberID, activated.Alarms[0].MemberID)
	require.Equal(t, etcdserverpb.AlarmType_CORRUPT, activated.Alarms[0].Alarm)
	_, err = maintenance.Alarm(ctx, &etcdserverpb.AlarmRequest{
		Action: etcdserverpb.AlarmRequest_ACTIVATE, Alarm: etcdserverpb.AlarmType_CORRUPT, MemberID: 7,
	})
	require.NoError(t, err)

	get, err := maintenance.Alarm(ctx, &etcdserverpb.AlarmRequest{Action: etcdserverpb.AlarmRequest_GET})
	require.NoError(t, err)
	require.Len(t, get.Alarms, 2)
	require.Equal(t, uint64(7), get.Alarms[0].MemberID)
	require.Equal(t, memberID, get.Alarms[1].MemberID)
	for _, alarm := range get.Alarms {
		require.Equal(t, etcdserverpb.AlarmType_CORRUPT, alarm.Alarm)
	}
	nospace, err := maintenance.Alarm(ctx, &etcdserverpb.AlarmRequest{
		Action: etcdserverpb.AlarmRequest_GET, Alarm: etcdserverpb.AlarmType_NOSPACE,
	})
	require.NoError(t, err)
	require.Empty(t, nospace.Alarms)
	statusResp, err := maintenance.Status(ctx, &etcdserverpb.StatusRequest{})
	require.NoError(t, err)
	require.Contains(t, statusResp.Errors, activated.Alarms[0].String())
	require.Contains(t, statusResp.Errors, get.Alarms[0].String())
	_, err = kv.Put(ctx, &etcdserverpb.PutRequest{})
	requireMaintenanceDirectError(t, err, rpctypes.ErrGRPCEmptyKey, codes.InvalidArgument, "etcdserver: key is not provided")
	_, err = kv.DeleteRange(ctx, &etcdserverpb.DeleteRangeRequest{})
	requireMaintenanceDirectError(t, err, rpctypes.ErrGRPCEmptyKey, codes.InvalidArgument, "etcdserver: key is not provided")
	_, err = kv.Txn(ctx, &etcdserverpb.TxnRequest{Success: []*etcdserverpb.RequestOp{{
		Request: &etcdserverpb.RequestOp_RequestPut{RequestPut: &etcdserverpb.PutRequest{}},
	}}})
	requireMaintenanceDirectError(t, err, rpctypes.ErrGRPCEmptyKey, codes.InvalidArgument, "etcdserver: key is not provided")

	calls := []struct {
		name string
		call func() error
	}{
		{name: "put", call: func() error { _, err := kv.Put(ctx, &etcdserverpb.PutRequest{Key: []byte("key")}); return err }},
		{name: "delete", call: func() error {
			_, err := kv.DeleteRange(ctx, &etcdserverpb.DeleteRangeRequest{Key: []byte("key")})
			return err
		}},
		{name: "txn", call: func() error {
			_, err := kv.Txn(ctx, &etcdserverpb.TxnRequest{Success: []*etcdserverpb.RequestOp{{
				Request: &etcdserverpb.RequestOp_RequestPut{RequestPut: &etcdserverpb.PutRequest{Key: []byte("key")}},
			}}})
			return err
		}},
		{name: "write in unchosen txn branch", call: func() error {
			_, err := kv.Txn(ctx, &etcdserverpb.TxnRequest{
				Compare: []*etcdserverpb.Compare{{
					Key: []byte("key"), Result: etcdserverpb.Compare_EQUAL,
					Target:      etcdserverpb.Compare_CREATE,
					TargetUnion: &etcdserverpb.Compare_CreateRevision{CreateRevision: 0},
				}},
				Success: []*etcdserverpb.RequestOp{{
					Request: &etcdserverpb.RequestOp_RequestRange{RequestRange: &etcdserverpb.RangeRequest{Key: []byte("key")}},
				}},
				Failure: []*etcdserverpb.RequestOp{{
					Request: &etcdserverpb.RequestOp_RequestPut{RequestPut: &etcdserverpb.PutRequest{Key: []byte("key")}},
				}},
			})
			return err
		}},
		{name: "compact", call: func() error { _, err := kv.Compact(ctx, &etcdserverpb.CompactionRequest{}); return err }},
		{name: "lease grant", call: func() error { _, err := lease.LeaseGrant(ctx, &etcdserverpb.LeaseGrantRequest{TTL: 10}); return err }},
		{name: "lease revoke", call: func() error { _, err := lease.LeaseRevoke(ctx, &etcdserverpb.LeaseRevokeRequest{ID: 1}); return err }},
	}
	for _, tc := range calls {
		t.Run(tc.name, func(t *testing.T) {
			requireMaintenanceDirectError(t, tc.call(), rpctypes.ErrGRPCCorrupt, codes.DataLoss, "etcdserver: corrupt cluster")
		})
	}
	_, err = kv.Range(ctx, &etcdserverpb.RangeRequest{Key: []byte("key")})
	require.NoError(t, err)
	for _, serializable := range []bool{false, true} {
		response, txnErr := kv.Txn(ctx, &etcdserverpb.TxnRequest{Success: []*etcdserverpb.RequestOp{{
			Request: &etcdserverpb.RequestOp_RequestRange{RequestRange: &etcdserverpb.RangeRequest{
				Key: []byte("key"), Serializable: serializable,
			}},
		}}})
		require.NoError(t, txnErr)
		require.Len(t, response.Responses, 1)
	}
	stream, err := kv.RangeStream(ctx, &etcdserverpb.RangeRequest{Key: []byte("key")})
	require.NoError(t, err)
	_, err = stream.Recv()
	require.NoError(t, err)

	wrong, err := maintenance.Alarm(ctx, &etcdserverpb.AlarmRequest{
		Action: etcdserverpb.AlarmRequest_DEACTIVATE, Alarm: etcdserverpb.AlarmType_CORRUPT, MemberID: memberID + 1,
	})
	require.NoError(t, err)
	require.Empty(t, wrong.Alarms)
	_, err = kv.Put(ctx, &etcdserverpb.PutRequest{Key: []byte("key"), Value: []byte("blocked")})
	requireMaintenanceDirectError(t, err, rpctypes.ErrGRPCCorrupt, codes.DataLoss, "etcdserver: corrupt cluster")

	deactivated, err := maintenance.Alarm(ctx, &etcdserverpb.AlarmRequest{
		Action: etcdserverpb.AlarmRequest_DEACTIVATE, Alarm: etcdserverpb.AlarmType_CORRUPT, MemberID: memberID,
	})
	require.NoError(t, err)
	require.Len(t, deactivated.Alarms, 1)
	require.Equal(t, memberID, deactivated.Alarms[0].MemberID)
	require.Equal(t, etcdserverpb.AlarmType_CORRUPT, deactivated.Alarms[0].Alarm)
	_, err = kv.Put(ctx, &etcdserverpb.PutRequest{Key: []byte("key"), Value: []byte("value")})
	requireMaintenanceDirectError(t, err, rpctypes.ErrGRPCCorrupt, codes.DataLoss, "etcdserver: corrupt cluster")
	last, err := maintenance.Alarm(ctx, &etcdserverpb.AlarmRequest{
		Action: etcdserverpb.AlarmRequest_DEACTIVATE, Alarm: etcdserverpb.AlarmType_CORRUPT, MemberID: 7,
	})
	require.NoError(t, err)
	require.Len(t, last.Alarms, 1)
	_, err = kv.Put(ctx, &etcdserverpb.PutRequest{Key: []byte("key"), Value: []byte("value")})
	require.NoError(t, err)
}

func TestCombinedAlarmsPreferCorruptThenRecoverToNoSpace(t *testing.T) {
	server := newQuotaRPCServer(t, 0)
	ctx := context.Background()
	const (
		noSpaceOwner = uint64(700001)
		corruptOwner = uint64(700002)
	)
	key := []byte("combined-alarm-key")
	type alarmSummary struct {
		memberID uint64
		alarm    etcdserverpb.AlarmType
	}
	summarize := func(alarms []*etcdserverpb.AlarmMember) []alarmSummary {
		result := make([]alarmSummary, 0, len(alarms))
		for _, alarm := range alarms {
			result = append(result, alarmSummary{memberID: alarm.MemberID, alarm: alarm.Alarm})
		}
		return result
	}

	_, err := server.Put(ctx, &etcdserverpb.PutRequest{Key: key, Value: []byte("before")})
	require.NoError(t, err)
	noSpace, err := server.Alarm(ctx, &etcdserverpb.AlarmRequest{
		Action: etcdserverpb.AlarmRequest_ACTIVATE, Alarm: etcdserverpb.AlarmType_NOSPACE, MemberID: noSpaceOwner,
	})
	require.NoError(t, err)
	require.Equal(t, []alarmSummary{{memberID: noSpaceOwner, alarm: etcdserverpb.AlarmType_NOSPACE}}, summarize(noSpace.Alarms))
	corrupt, err := server.Alarm(ctx, &etcdserverpb.AlarmRequest{
		Action: etcdserverpb.AlarmRequest_ACTIVATE, Alarm: etcdserverpb.AlarmType_CORRUPT, MemberID: corruptOwner,
	})
	require.NoError(t, err)
	require.Equal(t, []alarmSummary{{memberID: corruptOwner, alarm: etcdserverpb.AlarmType_CORRUPT}}, summarize(corrupt.Alarms))

	list, err := server.Alarm(ctx, &etcdserverpb.AlarmRequest{Action: etcdserverpb.AlarmRequest_GET})
	require.NoError(t, err)
	require.ElementsMatch(t, []alarmSummary{
		{memberID: noSpaceOwner, alarm: etcdserverpb.AlarmType_NOSPACE},
		{memberID: corruptOwner, alarm: etcdserverpb.AlarmType_CORRUPT},
	}, summarize(list.Alarms))
	statusResp, err := server.Status(ctx, &etcdserverpb.StatusRequest{})
	require.NoError(t, err)
	require.ElementsMatch(t, []string{noSpace.Alarms[0].String(), corrupt.Alarms[0].String()}, statusResp.Errors)

	_, err = server.Range(ctx, &etcdserverpb.RangeRequest{Key: key})
	require.NoError(t, err)
	_, err = server.Txn(ctx, &etcdserverpb.TxnRequest{Success: []*etcdserverpb.RequestOp{{
		Request: &etcdserverpb.RequestOp_RequestRange{
			RequestRange: &etcdserverpb.RangeRequest{Key: key},
		},
	}}})
	require.NoError(t, err)
	_, err = server.Put(ctx, &etcdserverpb.PutRequest{Key: key, Value: []byte("blocked-by-corrupt")})
	requireMaintenanceDirectError(t, err, rpctypes.ErrGRPCCorrupt, codes.DataLoss, "etcdserver: corrupt cluster")
	_, err = server.Txn(ctx, &etcdserverpb.TxnRequest{Success: []*etcdserverpb.RequestOp{{
		Request: &etcdserverpb.RequestOp_RequestDeleteRange{
			RequestDeleteRange: &etcdserverpb.DeleteRangeRequest{Key: key},
		},
	}}})
	requireMaintenanceDirectError(t, err, rpctypes.ErrGRPCCorrupt, codes.DataLoss, "etcdserver: corrupt cluster")
	_, err = server.LeaseGrant(ctx, &etcdserverpb.LeaseGrantRequest{TTL: 30})
	requireMaintenanceDirectError(t, err, rpctypes.ErrGRPCCorrupt, codes.DataLoss, "etcdserver: corrupt cluster")

	deactivatedCorrupt, err := server.Alarm(ctx, &etcdserverpb.AlarmRequest{
		Action: etcdserverpb.AlarmRequest_DEACTIVATE, Alarm: etcdserverpb.AlarmType_CORRUPT, MemberID: corruptOwner,
	})
	require.NoError(t, err)
	require.Equal(t, summarize(corrupt.Alarms), summarize(deactivatedCorrupt.Alarms))
	_, err = server.Put(ctx, &etcdserverpb.PutRequest{Key: key, Value: []byte("blocked-by-nospace")})
	requireMaintenanceDirectError(t, err, rpctypes.ErrGRPCNoSpace, codes.ResourceExhausted, "etcdserver: mvcc: database space exceeded")
	_, err = server.Txn(ctx, &etcdserverpb.TxnRequest{Success: []*etcdserverpb.RequestOp{{
		Request: &etcdserverpb.RequestOp_RequestDeleteRange{
			RequestDeleteRange: &etcdserverpb.DeleteRangeRequest{Key: key},
		},
	}}})
	require.NoError(t, err)
	_, err = server.LeaseGrant(ctx, &etcdserverpb.LeaseGrantRequest{TTL: 30})
	requireMaintenanceDirectError(t, err, rpctypes.ErrGRPCNoSpace, codes.ResourceExhausted, "etcdserver: mvcc: database space exceeded")

	deactivatedNoSpace, err := server.Alarm(ctx, &etcdserverpb.AlarmRequest{
		Action: etcdserverpb.AlarmRequest_DEACTIVATE, Alarm: etcdserverpb.AlarmType_NOSPACE, MemberID: noSpaceOwner,
	})
	require.NoError(t, err)
	require.Equal(t, summarize(noSpace.Alarms), summarize(deactivatedNoSpace.Alarms))
	empty, err := server.Alarm(ctx, &etcdserverpb.AlarmRequest{Action: etcdserverpb.AlarmRequest_GET})
	require.NoError(t, err)
	require.Empty(t, empty.Alarms)
	_, err = server.Put(ctx, &etcdserverpb.PutRequest{Key: key, Value: []byte("restored")})
	require.NoError(t, err)
}

func TestStatusManualNoSpaceAlarmDifferentialScenarioMatchesEtcd(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()
	ctx := context.Background()

	const memberID uint64 = 424242
	activated, err := server.Alarm(ctx, &etcdserverpb.AlarmRequest{
		Action:   etcdserverpb.AlarmRequest_ACTIVATE,
		MemberID: memberID,
		Alarm:    etcdserverpb.AlarmType_NOSPACE,
	})
	require.NoError(t, err)
	require.Len(t, activated.Alarms, 1)
	require.Equal(t, memberID, activated.Alarms[0].MemberID)
	require.Equal(t, etcdserverpb.AlarmType_NOSPACE, activated.Alarms[0].Alarm)

	active, err := server.Status(ctx, &etcdserverpb.StatusRequest{})
	require.NoError(t, err)
	require.Equal(t, []string{activated.Alarms[0].String()}, active.Errors)

	deactivated, err := server.Alarm(ctx, &etcdserverpb.AlarmRequest{
		Action:   etcdserverpb.AlarmRequest_DEACTIVATE,
		MemberID: memberID,
		Alarm:    etcdserverpb.AlarmType_NOSPACE,
	})
	require.NoError(t, err)
	require.Len(t, deactivated.Alarms, 1)
	require.Equal(t, memberID, deactivated.Alarms[0].MemberID)
	require.Equal(t, etcdserverpb.AlarmType_NOSPACE, deactivated.Alarms[0].Alarm)

	disarmed, err := server.Status(ctx, &etcdserverpb.StatusRequest{})
	require.NoError(t, err)
	require.Empty(t, disarmed.Errors)
}

func TestCorruptAlarmMemberSetConcurrentCAS(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()
	ctx := context.Background()
	const count = 32

	run := func(call func(uint64) error) {
		t.Helper()
		var group sync.WaitGroup
		errors := make(chan error, count)
		for id := uint64(1); id <= count; id++ {
			group.Add(1)
			go func() {
				defer group.Done()
				errors <- call(id)
			}()
		}
		group.Wait()
		close(errors)
		for err := range errors {
			require.NoError(t, err)
		}
	}
	run(func(id uint64) error { return server.backend.ArmCorrupt(ctx, id) })
	members, err := server.backend.CorruptAlarms(ctx)
	require.NoError(t, err)
	require.Len(t, members, count)
	for index, memberID := range members {
		require.Equal(t, uint64(index+1), memberID)
	}
	run(func(id uint64) error {
		removed, err := server.backend.DisarmCorrupt(ctx, id)
		if err != nil {
			return err
		}
		if !removed {
			return fmt.Errorf("alarm %d was not removed", id)
		}
		return nil
	})
	members, err = server.backend.CorruptAlarms(ctx)
	require.NoError(t, err)
	require.Empty(t, members)
}

func TestCorruptAlarmMetadataRejectsInvalidJSON(t *testing.T) {
	tests := []struct {
		name string
		raw  string
		err  string
	}{
		{name: "null", raw: "null", err: "corrupt alarm metadata must be a JSON array"},
		{name: "object", raw: `{"members":[1]}`, err: "decode corrupt alarm metadata"},
		{name: "trailing", raw: `[1]{"members":[2]}`, err: "corrupt alarm metadata contains trailing JSON"},
		{name: "duplicate", raw: `[1,1]`, err: "corrupt alarm metadata is not strictly ordered"},
		{name: "descending", raw: `[2,1]`, err: "corrupt alarm metadata is not strictly ordered"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			server, closeFn := newTestRPCServer(t)
			defer closeFn()
			ctx := context.Background()
			require.NoError(t, server.backend.InternalCAS(ctx, []backend.InternalCASOp{{
				Key: []byte("alarms/corrupt"), Value: []byte(test.raw),
			}}))
			_, err := server.backend.CorruptAlarms(ctx)
			require.ErrorContains(t, err, test.err)
		})
	}
}

func TestMaintenanceHashHeadersStayPinnedToHashedRevision(t *testing.T) {
	tests := []struct {
		name string
		call func(context.Context, *RPCServer) (*etcdserverpb.ResponseHeader, error)
	}{
		{
			name: "Hash",
			call: func(ctx context.Context, server *RPCServer) (*etcdserverpb.ResponseHeader, error) {
				resp, err := server.Hash(ctx, &etcdserverpb.HashRequest{})
				if err != nil {
					return nil, err
				}
				return resp.Header, nil
			},
		},
		{
			name: "HashKV",
			call: func(ctx context.Context, server *RPCServer) (*etcdserverpb.ResponseHeader, error) {
				resp, err := server.HashKV(ctx, &etcdserverpb.HashKVRequest{})
				if err != nil {
					return nil, err
				}
				require.Equal(t, resp.HashRevision, resp.Header.Revision)
				return resp.Header, nil
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			server, closeFn := newTestRPCServer(t)
			defer closeFn()
			ctx := context.Background()
			_, err := server.Put(ctx, &etcdserverpb.PutRequest{
				Key: []byte("/registry/maintenance/before-hash"), Value: []byte("before"),
			})
			require.NoError(t, err)

			backend := &writeAfterHashBackendShim{BackendShim: server.backend, t: t}
			server.backend = backend
			header, err := tt.call(ctx, server)
			require.NoError(t, err)
			require.Equal(t, backend.result.CurrentRevision, header.Revision)
			require.Greater(t, int64(server.backend.GetCurrentRevision()), header.Revision,
				"the injected post-hash write must advance current revision")
		})
	}
}

func TestMaintenanceHashKVCompactRevisionComesFromHashedSnapshot(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()
	ctx := context.Background()
	_, err := server.Put(ctx, &etcdserverpb.PutRequest{
		Key: []byte("/registry/maintenance/before-compact"), Value: []byte("before"),
	})
	require.NoError(t, err)

	backend := &compactAfterHashBackendShim{BackendShim: server.backend, t: t}
	server.backend = backend
	resp, err := server.HashKV(ctx, &etcdserverpb.HashKVRequest{})
	require.NoError(t, err)
	require.Equal(t, backend.result.CompactRevision, resp.CompactRevision)
	require.Equal(t, int64(-1), resp.CompactRevision)
	hasCompactRevision, err := server.backend.HasCompactRevision(ctx)
	require.NoError(t, err)
	require.True(t, hasCompactRevision,
		"the injected post-hash compaction must advance storage after the snapshot")
}

func TestMaintenanceHashKVRechecksCompactionInsideHashedSnapshot(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()
	ctx := context.Background()
	first, err := server.Put(ctx, &etcdserverpb.PutRequest{
		Key: []byte("/registry/maintenance/hash-before-compact"), Value: []byte("v1"),
	})
	require.NoError(t, err)
	second, err := server.Put(ctx, &etcdserverpb.PutRequest{
		Key: []byte("/registry/maintenance/hash-before-compact"), Value: []byte("v2"),
	})
	require.NoError(t, err)

	server.backend = &compactBeforeHashBackendShim{
		BackendShim: server.backend,
		t:           t,
		target:      uint64(second.Header.Revision),
	}
	_, err = server.HashKV(ctx, &etcdserverpb.HashKVRequest{Revision: first.Header.Revision})
	requireMaintenanceDirectError(t, err, rpctypes.ErrGRPCCompacted, codes.OutOfRange, "etcdserver: mvcc: required revision has been compacted")
}

func TestStatusReportsNoLeaderInsteadOfClaimingSelf(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()
	server.peers = testPeerService{noLeader: true}

	resp, err := server.Status(context.Background(), &etcdserverpb.StatusRequest{})
	require.NoError(t, err)
	require.Zero(t, resp.Leader)
	require.Contains(t, resp.Errors, "etcdserver: no leader")
}

func TestStatusReportsNoLeaderBeforeActiveAlarms(t *testing.T) {
	server := newQuotaRPCServer(t, 1)
	server.peers = testPeerService{noLeader: true, currentTermFn: func() uint64 { return 1 }}
	ctx := context.Background()

	alarm, err := server.Alarm(ctx, &etcdserverpb.AlarmRequest{
		Action: etcdserverpb.AlarmRequest_ACTIVATE,
		Alarm:  etcdserverpb.AlarmType_NOSPACE,
	})
	require.NoError(t, err)
	require.Len(t, alarm.Alarms, 1)

	resp, err := server.Status(ctx, &etcdserverpb.StatusRequest{})
	require.NoError(t, err)
	require.Equal(t, []string{
		rpctypes.ErrNoLeader.Error(),
		alarm.Alarms[0].String(),
	}, resp.Errors)
}

func TestStatusDoesNotHideActiveAlarmReadFailure(t *testing.T) {
	server := newQuotaRPCServer(t, 1)
	ctx := context.Background()
	_, err := server.Alarm(ctx, &etcdserverpb.AlarmRequest{
		Action: etcdserverpb.AlarmRequest_ACTIVATE,
		Alarm:  etcdserverpb.AlarmType_NOSPACE,
	})
	require.NoError(t, err)

	wantErr := errors.New("alarm metadata unavailable")
	server.backend = &alarmReadErrorBackendShim{BackendShim: server.backend, err: wantErr}
	_, err = server.Status(ctx, &etcdserverpb.StatusRequest{})
	require.ErrorIs(t, err, wantErr)
}

func TestLocalMaintenanceDiagnosticsDoNotRequireLeaderBarrier(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()
	ctx := context.Background()
	put, err := server.Put(ctx, &etcdserverpb.PutRequest{
		Key: []byte("/registry/maintenance/no-leader"), Value: []byte("value"),
	})
	require.NoError(t, err)

	barrierErr := errors.New("leader unavailable")
	barrierCalls := 0
	server.peers = testPeerService{
		noLeader: true,
		syncReadFn: func(context.Context) error {
			barrierCalls++
			return barrierErr
		},
		currentTermFn: func() uint64 { return 7 },
	}

	statusResp, err := server.Status(ctx, &etcdserverpb.StatusRequest{})
	require.NoError(t, err)
	require.Zero(t, statusResp.Leader)
	require.Contains(t, statusResp.Errors, rpctypes.ErrNoLeader.Error())
	require.Equal(t, uint64(7), statusResp.RaftTerm)

	hashResp, err := server.Hash(ctx, &etcdserverpb.HashRequest{})
	require.NoError(t, err)
	require.GreaterOrEqual(t, hashResp.Header.Revision, put.Header.Revision)

	hashKVResp, err := server.HashKV(ctx, &etcdserverpb.HashKVRequest{
		Revision: put.Header.Revision,
	})
	require.NoError(t, err)
	require.Equal(t, put.Header.Revision, hashKVResp.HashRevision)
	require.Equal(t, 2, barrierCalls,
		"Hash and HashKV should attempt a revision refresh without requiring it")

	_, err = server.Alarm(ctx, &etcdserverpb.AlarmRequest{Action: etcdserverpb.AlarmRequest_GET})
	require.Equal(t, codes.Unavailable, status.Code(err),
		"Alarm GET remains a cluster-wide alarm-store query and must keep its read barrier")
	require.Equal(t, barrierErr.Error(), status.Convert(err).Message())
	require.Equal(t, 3, barrierCalls, "Alarm GET must retain its required read barrier")
}

func TestStatusUsesCachedLeadershipTerm(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()
	server.peers = testPeerService{
		isLeader:      true,
		currentTermFn: func() uint64 { return 9 },
		leadershipTermFn: func(context.Context) (uint64, error) {
			t.Fatal("Status must not reread a populated leadership term")
			return 0, nil
		},
	}

	resp, err := server.Status(context.Background(), &etcdserverpb.StatusRequest{})
	require.NoError(t, err)
	require.Equal(t, uint64(9), resp.RaftTerm)
}

func requireMaintenanceDirectError(t *testing.T, err error, want error, code codes.Code, message string) {
	t.Helper()
	require.ErrorIs(t, err, want)
	require.Equal(t, code, status.Code(err))
	require.Equal(t, message, status.Convert(err).Message())
}
