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
	"runtime"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/proto"

	"github.com/kubewharf/kubebrain/pkg/backend"
	"github.com/kubewharf/kubebrain/pkg/etcdsnapshot"
	"github.com/kubewharf/kubebrain/pkg/metrics"
	"github.com/kubewharf/kubebrain/pkg/metrics/mock"
	"github.com/kubewharf/kubebrain/pkg/storage"
)

type writeAfterHashBackendShim struct {
	BackendShim
	t              *testing.T
	resultRevision int64
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

type maintenanceHashTrapBackendShim struct {
	BackendShim
	err error
}

type requireSyncBeforeHashBackendShim struct {
	BackendShim
	t      *testing.T
	synced func() bool
}

type checkpointHashKVBackendShim struct {
	BackendShim
	t          *testing.T
	checkpoint backend.SerializableCheckpoint
}

type checkpointHashBackendShim struct {
	BackendShim
	t           *testing.T
	checkpoint  backend.SerializableCheckpoint
	liveErr     error
	liveCalls   int
	pinnedCalls int
}

type checkpointDefragmentBackendShim struct {
	BackendShim
	checkpoint backend.SerializableCheckpoint
	liveErr    error
	liveCalls  int
}

func (b *checkpointDefragmentBackendShim) GetSerializableCheckpoint() (backend.SerializableCheckpoint, error) {
	return b.checkpoint, nil
}

func (b *checkpointDefragmentBackendShim) InternalGet(ctx context.Context, key []byte) ([]byte, error) {
	if bytes.Equal(key, authConfigKey) {
		if _, pinned := backend.SerializableCheckpointFromContext(ctx); pinned {
			return nil, storage.ErrKeyNotFound
		}
		b.liveCalls++
		return nil, b.liveErr
	}
	return b.BackendShim.InternalGet(ctx, key)
}

func (b *checkpointHashBackendShim) GetSerializableCheckpoint() (backend.SerializableCheckpoint, error) {
	return b.checkpoint, nil
}

func (b *checkpointHashBackendShim) Hash(ctx context.Context) (backend.BackendHashResult, error) {
	checkpoint, pinned := backend.SerializableCheckpointFromContext(ctx)
	if !pinned {
		b.liveCalls++
		return backend.BackendHashResult{}, b.liveErr
	}
	require.Equal(b.t, b.checkpoint, checkpoint)
	b.pinnedCalls++
	return backend.BackendHashResult{Hash: 73, CurrentRevision: int64(checkpoint.Revision)}, nil
}

type coldStatusRevisionBackendShim struct {
	BackendShim
	current    uint64
	durable    uint64
	durableErr error
}

type checkpointStatusBackendShim struct {
	BackendShim
	t           *testing.T
	checkpoint  backend.SerializableCheckpoint
	liveErr     error
	liveCalls   int
	pinnedCalls int
}

func (b *checkpointStatusBackendShim) GetSerializableCheckpoint() (backend.SerializableCheckpoint, error) {
	return b.checkpoint, nil
}

func (b *checkpointStatusBackendShim) GetDurableRevision(ctx context.Context) (uint64, error) {
	_, pinned := backend.SerializableCheckpointFromContext(ctx)
	if !pinned {
		b.liveCalls++
		return 0, b.liveErr
	}
	require.FailNow(b.t, "protected Status must use the revision already carried by its checkpoint")
	return 0, nil
}

func (b *checkpointStatusBackendShim) requirePinned(ctx context.Context) {
	checkpoint, pinned := backend.SerializableCheckpointFromContext(ctx)
	require.True(b.t, pinned)
	require.Equal(b.t, b.checkpoint, checkpoint)
	b.pinnedCalls++
}

func (b *checkpointStatusBackendShim) QuotaStatus(ctx context.Context) (int64, int64, bool, error) {
	b.requirePinned(ctx)
	return 11, 22, false, nil
}

func (b *checkpointStatusBackendShim) NoSpaceAlarms(ctx context.Context) ([]uint64, error) {
	b.requirePinned(ctx)
	return nil, nil
}

func (b *checkpointStatusBackendShim) ValidateCorruptAlarmMetadata(ctx context.Context) error {
	b.requirePinned(ctx)
	return nil
}

func (b *checkpointStatusBackendShim) CorruptAlarms(ctx context.Context) ([]uint64, error) {
	b.requirePinned(ctx)
	return nil, nil
}

func (b *checkpointStatusBackendShim) InternalGet(ctx context.Context, key []byte) ([]byte, error) {
	if _, pinned := backend.SerializableCheckpointFromContext(ctx); !pinned {
		return b.BackendShim.InternalGet(ctx, key)
	}
	b.requirePinned(ctx)
	return nil, storage.ErrKeyNotFound
}

func (b *coldStatusRevisionBackendShim) GetCurrentRevision() uint64 { return b.current }
func (b *coldStatusRevisionBackendShim) SetCurrentRevision(revision uint64) {
	if revision > b.current {
		b.current = revision
	}
}
func (b *coldStatusRevisionBackendShim) GetDurableRevision(context.Context) (uint64, error) {
	return b.durable, b.durableErr
}
func (b *coldStatusRevisionBackendShim) GetCompactRevision(context.Context) (uint64, error) {
	return 0, nil
}
func (b *coldStatusRevisionBackendShim) QuotaStatus(ctx context.Context) (int64, int64, bool, error) {
	// Model backend.QuotaStatus's safeCurrentRevision call recovering the cold
	// process after Status has already sampled GetCurrentRevision.
	b.SetCurrentRevision(b.durable)
	return b.BackendShim.QuotaStatus(ctx)
}

func (b *alarmReadErrorBackendShim) NoSpaceAlarms(context.Context) ([]uint64, error) {
	return nil, b.err
}

func (b *quotaStatusErrorBackendShim) QuotaStatus(context.Context) (int64, int64, bool, error) {
	return 0, 0, false, b.err
}

func (b *maintenanceHashTrapBackendShim) Hash(context.Context) (backend.BackendHashResult, error) {
	return backend.BackendHashResult{}, b.err
}

func (b *maintenanceHashTrapBackendShim) HashKV(context.Context, int64) (backend.HashKVResult, error) {
	return backend.HashKVResult{}, b.err
}

func (b *requireSyncBeforeHashBackendShim) HashKV(ctx context.Context, revision int64) (backend.HashKVResult, error) {
	require.True(b.t, b.synced(), "peer HashKV should refresh the revision cache before hashing")
	return b.BackendShim.HashKV(ctx, revision)
}

func (b *checkpointHashKVBackendShim) GetSerializableCheckpoint() (backend.SerializableCheckpoint, error) {
	return b.checkpoint, nil
}

func (b *checkpointHashKVBackendShim) GetCompactRevisionFresh(context.Context) (uint64, error) {
	return b.checkpoint.CompactRevision, nil
}

func (b *checkpointHashKVBackendShim) HashKV(ctx context.Context, revision int64) (backend.HashKVResult, error) {
	checkpoint, ok := backend.SerializableCheckpointFromContext(ctx)
	require.True(b.t, ok, "degraded fixed-revision HashKV must use the protected checkpoint")
	require.Equal(b.t, b.checkpoint, checkpoint)
	return backend.HashKVResult{
		Hash: 71, HashRevision: revision, CurrentRevision: int64(checkpoint.Revision),
		CompactRevision: int64(checkpoint.CompactRevision),
	}, nil
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
	b.resultRevision = result.CurrentRevision
	_, err = b.BackendShim.Put(ctx, &etcdserverpb.PutRequest{
		Key:   []byte("/registry/maintenance/write-after-hash"),
		Value: []byte(fmt.Sprintf("after-%d", result.HashRevision)),
	})
	require.NoError(b.t, err)
	return result, nil
}

func (b *writeAfterHashBackendShim) Hash(ctx context.Context) (backend.BackendHashResult, error) {
	result, err := b.BackendShim.Hash(ctx)
	if err != nil {
		return backend.BackendHashResult{}, err
	}
	b.resultRevision = result.CurrentRevision
	_, err = b.BackendShim.Put(ctx, &etcdserverpb.PutRequest{
		Key:   []byte("/registry/maintenance/write-after-hash"),
		Value: []byte(fmt.Sprintf("after-%d", result.CurrentRevision)),
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

func TestStatusFallsBackToProtectedCheckpointWhenBackendIsUnavailable(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()
	checkpoint := backend.SerializableCheckpoint{
		Revision: 37, Timestamp: 101, CompactRevision: 7, ValidUntil: time.Now().Add(time.Minute),
	}
	shim := &checkpointStatusBackendShim{
		BackendShim: server.backend, t: t, checkpoint: checkpoint, liveErr: storage.ErrUnavailable,
	}
	server.backend = shim
	server.auth.repo.backend = shim
	server.tokens.repo.backend = shim
	server.tokens.snapshots = newAuthSnapshotCache(shim)
	server.peers = testPeerService{isLeader: true, currentTermFn: func() uint64 { return 9 }}

	response, err := server.Status(context.Background(), &etcdserverpb.StatusRequest{})
	require.NoError(t, err)
	require.Equal(t, int64(checkpoint.Revision), response.Header.Revision)
	require.Equal(t, checkpoint.Revision, response.RaftIndex)
	require.Equal(t, checkpoint.Revision, response.RaftAppliedIndex)
	require.Equal(t, uint64(9), response.RaftTerm)
	require.Equal(t, 1, shim.liveCalls)
	require.GreaterOrEqual(t, shim.pinnedCalls, 3)
}

func TestStatusDoesNotMaskDeterministicBackendFailureWithCheckpoint(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()
	checkpoint := backend.SerializableCheckpoint{
		Revision: 37, Timestamp: 101, CompactRevision: 7, ValidUntil: time.Now().Add(time.Minute),
	}
	wantErr := status.Error(codes.DataLoss, "corrupt durable revision")
	shim := &checkpointStatusBackendShim{
		BackendShim: server.backend, t: t, checkpoint: checkpoint, liveErr: wantErr,
	}
	server.backend = shim
	server.auth.repo.backend = shim
	server.tokens.repo.backend = shim
	server.tokens.snapshots = newAuthSnapshotCache(shim)
	server.peers = testPeerService{isLeader: true, currentTermFn: func() uint64 { return 9 }}

	response, err := server.Status(context.Background(), &etcdserverpb.StatusRequest{})
	require.Nil(t, response)
	require.ErrorIs(t, err, wantErr)
	require.Equal(t, 1, shim.liveCalls)
	require.Zero(t, shim.pinnedCalls)
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
	metricCli.EXPECT().EmitGauge(
		"etcd.server.go_version", 1, metrics.Tag("server_go_version", runtime.Version()),
	).Return(nil)

	emitVersionMetrics(metricCli)
	require.Equal(t, Version[:len(ClusterVersion)], ClusterVersion)
	require.Equal(t, byte('.'), Version[len(ClusterVersion)])
}

func TestServerIDMetricUsesUpstreamHexLabel(t *testing.T) {
	ctrl := gomock.NewController(t)
	metricCli := mock.NewMockMetrics(ctrl)
	metricCli.EXPECT().EmitGauge(
		"etcd.server.id", 1, metrics.Tag("server_id", "abc123"),
	).Return(nil)

	emitServerIDMetric(metricCli, 0xabc123)
}

func TestServerIDMetricSkipsUnknownMember(t *testing.T) {
	ctrl := gomock.NewController(t)
	metricCli := mock.NewMockMetrics(ctrl)

	emitServerIDMetric(metricCli, 0)
}

func TestKnownPeersMetricUsesUpstreamHexLabels(t *testing.T) {
	ctrl := gomock.NewController(t)
	metricCli := mock.NewMockMetrics(ctrl)
	metricCli.EXPECT().EmitGauge(
		"etcd.network.known_peers", 1,
		metrics.Tag("Local", "abc123"),
		metrics.Tag("Remote", "abc123"),
	).Return(nil)
	metricCli.EXPECT().EmitGauge(
		"etcd.network.known_peers", 1,
		metrics.Tag("Local", "abc123"),
		metrics.Tag("Remote", "def456"),
	).Return(nil)

	emitKnownPeersMetric(metricCli, 0xabc123, []*etcdserverpb.Member{
		{ID: 0xabc123},
		{ID: 0xdef456},
		{ID: 0},
	})
}

func TestKnownPeersMetricSkipsUnknownLocalMember(t *testing.T) {
	ctrl := gomock.NewController(t)
	metricCli := mock.NewMockMetrics(ctrl)

	emitKnownPeersMetric(metricCli, 0, []*etcdserverpb.Member{{ID: 0xabc123}})
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
	require.Equal(t, etcdsnapshot.StorageVersion, statusResp.StorageVersion)
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

func TestStatusReportsLocalLearnerStateMatchesEtcd(t *testing.T) {
	for _, tc := range []struct {
		name         string
		localLearner bool
		want         bool
	}{
		{name: "local learner", localLearner: true, want: true},
		{name: "foreign learner does not mark local voter", localLearner: false, want: false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			server, closeFn := newTestRPCServer(t)
			defer closeFn()
			localID := server.memberIDForPeerIdentity(server.backend.GetResourceLock().Identity())
			server.SetStaticMembers([]*etcdserverpb.Member{
				{ID: localID, Name: "test-peer", PeerURLs: []string{"http://test-peer"}, IsLearner: tc.localLearner},
				{ID: localID + 1, Name: "other", PeerURLs: []string{"http://other"}, IsLearner: true},
			})

			response, err := server.Status(context.Background(), &etcdserverpb.StatusRequest{})
			require.NoError(t, err)
			require.Equal(t, tc.want, response.GetIsLearner())
		})
	}
}

func TestFollowerStatusHedgesIsolatedStorageToLeaderAndPreservesLocalLearner(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()
	localID := server.memberIDForPeerIdentity(server.backend.GetResourceLock().Identity())
	server.SetStaticMembers([]*etcdserverpb.Member{
		{ID: localID, Name: "local", IsLearner: true},
		{ID: 8, Name: "leader"},
	})

	want := &etcdserverpb.StatusResponse{
		Header:           proxiedResponseHeader(server, 91),
		Version:          Version,
		DbSize:           1,
		DbSizeInUse:      2,
		DbSizeQuota:      -1,
		Leader:           8,
		RaftIndex:        90,
		RaftAppliedIndex: 91,
		IsLearner:        false,
		DowngradeInfo:    &etcdserverpb.DowngradeInfo{},
	}
	request := &etcdserverpb.StatusRequest{}
	called := 0
	server.peers = testPeerService{
		isLeader: false, proxyEnabled: true,
		epochFn: func() (uint64, bool) { return 7, false },
		statusFn: func(_ context.Context, got *etcdserverpb.StatusRequest) (*etcdserverpb.StatusResponse, error) {
			called++
			require.Same(t, request, got)
			return want, nil
		},
	}
	server.backend = &quotaStatusErrorBackendShim{BackendShim: server.backend, err: errors.New("storage unavailable")}

	response, err := server.Status(context.Background(), request)
	require.NoError(t, err)
	require.Equal(t, 1, called)
	require.Same(t, want, response)
	require.True(t, response.GetIsLearner(), "the ingress member's learner state must override the leader payload")
	require.Equal(t, int64(91), response.GetHeader().GetRevision())
}

func TestFollowerStatusHedgeRejectsInvalidProxyResult(t *testing.T) {
	for _, mixed := range []bool{false, true} {
		name := "nil"
		if mixed {
			name = "mixed"
		}
		t.Run(name, func(t *testing.T) {
			server, closeFn := newTestRPCServer(t)
			defer closeFn()
			rec := &recordingMetrics{}
			server.metricCli = rec
			initMaintenanceProxyIntegrityMetrics(rec)
			server.backend = &quotaStatusErrorBackendShim{BackendShim: server.backend, err: errors.New("storage unavailable")}
			server.peers = testPeerService{
				isLeader: false, proxyEnabled: true,
				epochFn: func() (uint64, bool) { return 7, false },
				statusFn: func(context.Context, *etcdserverpb.StatusRequest) (*etcdserverpb.StatusResponse, error) {
					if mixed {
						return &etcdserverpb.StatusResponse{}, errors.New("mixed")
					}
					return nil, nil
				},
			}

			response, err := server.Status(context.Background(), &etcdserverpb.StatusRequest{})
			require.Nil(t, response)
			require.Equal(t, codes.DataLoss, status.Code(err))
			require.Equal(t, []interface{}{int64(0), 1},
				recordedMaintenanceProxyIntegrityValues(rec, maintenanceProxyRPCStatus))
		})
	}
}

func TestFollowerStatusHedgeRejectsInvalidProxyPayload(t *testing.T) {
	for _, tt := range []struct {
		name   string
		mutate func(*etcdserverpb.StatusResponse)
	}{
		{name: "empty version", mutate: func(response *etcdserverpb.StatusResponse) { response.Version = "" }},
		{name: "invalid server version", mutate: func(response *etcdserverpb.StatusResponse) { response.Version = "not-semver" }},
		{name: "invalid storage version", mutate: func(response *etcdserverpb.StatusResponse) { response.StorageVersion = "not-semver" }},
		{name: "pre-3.6 versioned fields", mutate: func(response *etcdserverpb.StatusResponse) {
			response.Version = "3.5.0"
			response.StorageVersion = "3.5.0"
		}},
		{name: "pre-3.4 versioned fields", mutate: func(response *etcdserverpb.StatusResponse) {
			response.Version = "3.3.0"
			response.StorageVersion = ""
			response.DbSizeQuota = 0
			response.DowngradeInfo = nil
		}},
		{name: "storage version with nonzero patch", mutate: func(response *etcdserverpb.StatusResponse) { response.StorageVersion = "3.7.1" }},
		{name: "storage version with prerelease", mutate: func(response *etcdserverpb.StatusResponse) { response.StorageVersion = "3.7.0-rc.1" }},
		{name: "storage version with metadata", mutate: func(response *etcdserverpb.StatusResponse) { response.StorageVersion = "3.7.0+build.2" }},
		{name: "negative size", mutate: func(response *etcdserverpb.StatusResponse) { response.DbSize = -1 }},
		{name: "negative in use", mutate: func(response *etcdserverpb.StatusResponse) { response.DbSizeInUse = -1 }},
		{name: "zero quota", mutate: func(response *etcdserverpb.StatusResponse) { response.DbSizeQuota = 0 }},
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
			response.DowngradeInfo.TargetVersion = "3.5.0"
		}},
		{name: "enabled downgrade with cross major target", mutate: func(response *etcdserverpb.StatusResponse) {
			response.DowngradeInfo.Enabled = true
			response.DowngradeInfo.TargetVersion = "4.0.0"
		}},
		{name: "enabled downgrade with nonzero patch target", mutate: func(response *etcdserverpb.StatusResponse) {
			response.DowngradeInfo.Enabled = true
			response.DowngradeInfo.TargetVersion = "3.6.1"
		}},
		{name: "inconsistent leader health", mutate: func(response *etcdserverpb.StatusResponse) { response.Leader = 0 }},
		{name: "unknown leader", mutate: func(response *etcdserverpb.StatusResponse) {
			response.Leader = response.GetHeader().GetMemberId() + 1
		}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			server, closeFn := newTestRPCServer(t)
			defer closeFn()
			rec := &recordingMetrics{}
			server.metricCli = rec
			initMaintenanceProxyIntegrityMetrics(rec)
			server.backend = &quotaStatusErrorBackendShim{BackendShim: server.backend, err: errors.New("storage unavailable")}
			header := proxiedResponseHeader(server, 5)
			server.SetStaticMembers([]*etcdserverpb.Member{{ID: header.GetMemberId(), Name: "known"}})
			response := &etcdserverpb.StatusResponse{
				Header: header, Version: Version, DbSize: 10, DbSizeInUse: 8, DbSizeQuota: 100,
				Leader: header.GetMemberId(), RaftIndex: 7, RaftAppliedIndex: 6,
				DowngradeInfo: &etcdserverpb.DowngradeInfo{},
			}
			tt.mutate(response)
			server.peers = testPeerService{
				isLeader: false, proxyEnabled: true,
				epochFn: func() (uint64, bool) { return 7, false },
				statusFn: func(context.Context, *etcdserverpb.StatusRequest) (*etcdserverpb.StatusResponse, error) {
					return response, nil
				},
			}

			got, err := server.Status(context.Background(), &etcdserverpb.StatusRequest{})
			require.Nil(t, got)
			require.Equal(t, codes.DataLoss, status.Code(err))
			require.Equal(t, []interface{}{int64(0), 1}, recordedMaintenanceProxyIntegrityValues(rec, maintenanceProxyRPCStatus))
		})
	}
}

func TestFollowerStatusHedgesIsolatedLeaderToLocalStatus(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()
	started, canceled := make(chan struct{}), make(chan struct{})
	server.peers = testPeerService{
		isLeader: false, proxyEnabled: true,
		epochFn: func() (uint64, bool) { return 7, false },
		statusFn: func(ctx context.Context, _ *etcdserverpb.StatusRequest) (*etcdserverpb.StatusResponse, error) {
			close(started)
			<-ctx.Done()
			close(canceled)
			return nil, ctx.Err()
		},
	}

	response, err := server.Status(context.Background(), &etcdserverpb.StatusRequest{})
	require.NoError(t, err)
	require.NotNil(t, response)
	require.Equal(t, int64(server.backend.GetCurrentRevision()), response.GetHeader().GetRevision())
	requireChannelsClosedEventually(t, started, canceled)
}

func TestFollowerStatusHedgeFailsClosedOnLocalAuthentication(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()
	_ = setupAuthKVUser(t, server)
	canceled := make(chan struct{})
	server.peers = testPeerService{
		isLeader: false, proxyEnabled: true,
		epochFn: func() (uint64, bool) { return 7, false },
		statusFn: func(ctx context.Context, _ *etcdserverpb.StatusRequest) (*etcdserverpb.StatusResponse, error) {
			<-ctx.Done()
			close(canceled)
			return nil, ctx.Err()
		},
	}

	response, err := server.Status(context.Background(), &etcdserverpb.StatusRequest{})
	require.Nil(t, response)
	require.ErrorIs(t, err, rpctypes.ErrUserEmpty)
	require.Eventually(t, func() bool {
		select {
		case <-canceled:
			return true
		default:
			return false
		}
	}, time.Second, time.Millisecond)
}

func TestFollowerHashesHedgeIsolatedStorageAndPreserveRevision(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()
	wantErr := errors.New("local hash storage must not be reached")
	server.backend = &maintenanceHashTrapBackendShim{BackendShim: server.backend, err: wantErr}

	hashRequest := &etcdserverpb.HashRequest{}
	hashKVRequest := &etcdserverpb.HashKVRequest{Revision: 42}
	hashResponse := &etcdserverpb.HashResponse{Header: proxiedResponseHeader(server, 77), Hash: 101}
	hashKVResponse := &etcdserverpb.HashKVResponse{
		Header: proxiedResponseHeader(server, 77), Hash: 202, HashRevision: 42, CompactRevision: 10,
	}
	hashCalls, hashKVCalls := 0, 0
	syncStarted := make(chan struct{}, 2)
	server.peers = testPeerService{
		isLeader: false, proxyEnabled: true,
		epochFn: func() (uint64, bool) { return 7, false },
		syncReadFn: func(context.Context) error {
			syncStarted <- struct{}{}
			return errors.New("latest revision sync unavailable")
		},
		hashFn: func(_ context.Context, got *etcdserverpb.HashRequest) (*etcdserverpb.HashResponse, error) {
			hashCalls++
			require.Same(t, hashRequest, got)
			return hashResponse, nil
		},
		hashKVFn: func(_ context.Context, got *etcdserverpb.HashKVRequest) (*etcdserverpb.HashKVResponse, error) {
			hashKVCalls++
			if got.GetRevision() <= 0 {
				<-syncStarted
			} else {
				require.Same(t, hashKVRequest, got)
			}
			response := proto.Clone(hashKVResponse).(*etcdserverpb.HashKVResponse)
			if got.GetRevision() == 0 {
				response.HashRevision = response.GetHeader().GetRevision()
			} else {
				response.HashRevision = got.GetRevision()
			}
			return response, nil
		},
	}

	gotHash, err := server.Hash(context.Background(), hashRequest)
	require.NoError(t, err)
	require.Same(t, hashResponse, gotHash)
	require.Equal(t, 1, hashCalls)

	gotHashKV, err := server.HashKV(context.Background(), hashKVRequest)
	require.NoError(t, err)
	require.Equal(t, hashKVResponse, gotHashKV)
	require.Equal(t, 1, hashKVCalls)
	require.Equal(t, int64(42), gotHashKV.GetHashRevision())
	require.Equal(t, int64(10), gotHashKV.GetCompactRevision())
	require.Equal(t, uint64(77), server.backend.GetCurrentRevision())

	got, err := server.HashKV(context.Background(), &etcdserverpb.HashKVRequest{})
	require.NoError(t, err)
	require.Equal(t, int64(77), got.GetHashRevision())
	got, err = server.HashKV(context.Background(), &etcdserverpb.HashKVRequest{Revision: -1})
	require.NoError(t, err)
	require.Equal(t, int64(-1), got.GetHashRevision())
}

func TestFollowerHashesHedgeRejectsInvalidProxyPayload(t *testing.T) {
	t.Run("hash zero current revision", func(t *testing.T) {
		server, closeFn := newTestRPCServer(t)
		defer closeFn()
		rec := &recordingMetrics{}
		server.metricCli = rec
		initMaintenanceProxyIntegrityMetrics(rec)
		server.backend = &maintenanceHashTrapBackendShim{BackendShim: server.backend, err: errors.New("local storage unavailable")}
		server.peers = testPeerService{
			isLeader: false, proxyEnabled: true,
			epochFn: func() (uint64, bool) { return 7, false },
			hashFn: func(context.Context, *etcdserverpb.HashRequest) (*etcdserverpb.HashResponse, error) {
				return &etcdserverpb.HashResponse{Header: proxiedResponseHeader(server, 0)}, nil
			},
		}

		response, err := server.Hash(context.Background(), &etcdserverpb.HashRequest{})
		require.Nil(t, response)
		require.Equal(t, codes.DataLoss, status.Code(err))
		require.Equal(t, []interface{}{int64(0), 1}, recordedMaintenanceProxyIntegrityValues(rec, maintenanceProxyRPCHash))
	})

	t.Run("hash kv revision mismatch", func(t *testing.T) {
		server, closeFn := newTestRPCServer(t)
		defer closeFn()
		rec := &recordingMetrics{}
		server.metricCli = rec
		initMaintenanceProxyIntegrityMetrics(rec)
		server.backend = &maintenanceHashTrapBackendShim{BackendShim: server.backend, err: errors.New("local storage unavailable")}
		server.peers = testPeerService{
			isLeader: false, proxyEnabled: true,
			epochFn: func() (uint64, bool) { return 7, false },
			hashKVFn: func(context.Context, *etcdserverpb.HashKVRequest) (*etcdserverpb.HashKVResponse, error) {
				return &etcdserverpb.HashKVResponse{Header: proxiedResponseHeader(server, 7), HashRevision: 4, CompactRevision: 3}, nil
			},
		}

		response, err := server.HashKV(context.Background(), &etcdserverpb.HashKVRequest{Revision: 5})
		require.Nil(t, response)
		require.Equal(t, codes.DataLoss, status.Code(err))
		require.Equal(t, []interface{}{int64(0), 1}, recordedMaintenanceProxyIntegrityValues(rec, maintenanceProxyRPCHashKV))
	})
}

func TestFollowerHashesHedgeIsolatedLeaderWithLocalStorage(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()
	put, err := server.Put(context.Background(), &etcdserverpb.PutRequest{
		Key: []byte("/registry/maintenance/public-hash-local-hedge"), Value: []byte("value"),
	})
	require.NoError(t, err)
	wantHash, err := server.backend.Hash(context.Background())
	require.NoError(t, err)
	wantHashKV, err := server.backend.HashKV(context.Background(), put.GetHeader().GetRevision())
	require.NoError(t, err)
	hashStarted, hashCanceled := make(chan struct{}), make(chan struct{})
	hashKVStarted, hashKVCanceled := make(chan struct{}), make(chan struct{})
	server.peers = testPeerService{
		isLeader: false, proxyEnabled: true,
		epochFn: func() (uint64, bool) { return 7, false },
		hashFn: func(ctx context.Context, _ *etcdserverpb.HashRequest) (*etcdserverpb.HashResponse, error) {
			close(hashStarted)
			<-ctx.Done()
			close(hashCanceled)
			return nil, ctx.Err()
		},
		hashKVFn: func(ctx context.Context, _ *etcdserverpb.HashKVRequest) (*etcdserverpb.HashKVResponse, error) {
			close(hashKVStarted)
			<-ctx.Done()
			close(hashKVCanceled)
			return nil, ctx.Err()
		},
	}

	hash, err := server.Hash(context.Background(), &etcdserverpb.HashRequest{})
	require.NoError(t, err)
	require.Equal(t, wantHash.Hash, hash.GetHash())
	require.Equal(t, wantHash.CurrentRevision, hash.GetHeader().GetRevision())
	requireChannelsClosedEventually(t, hashStarted, hashCanceled)

	hashKV, err := server.HashKV(context.Background(), &etcdserverpb.HashKVRequest{
		Revision: put.GetHeader().GetRevision(),
	})
	require.NoError(t, err)
	require.Equal(t, wantHashKV.Hash, hashKV.GetHash())
	require.Equal(t, wantHashKV.HashRevision, hashKV.GetHashRevision())
	require.Equal(t, wantHashKV.CompactRevision, hashKV.GetCompactRevision())
	requireChannelsClosedEventually(t, hashKVStarted, hashKVCanceled)
}

func requireChannelsClosedEventually(t *testing.T, started, canceled <-chan struct{}) {
	t.Helper()
	require.Eventually(t, func() bool {
		select {
		case <-started:
			select {
			case <-canceled:
				return true
			default:
			}
		default:
		}
		return false
	}, time.Second, time.Millisecond)
}

func TestFollowerHashHedgeFailsClosedOnLocalAuthorization(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()
	aliceCtx := setupAuthKVUser(t, server)
	peerCanceled := make(chan struct{})
	server.peers = testPeerService{
		isLeader: false, proxyEnabled: true,
		epochFn: func() (uint64, bool) { return 7, false },
		hashFn: func(ctx context.Context, _ *etcdserverpb.HashRequest) (*etcdserverpb.HashResponse, error) {
			<-ctx.Done()
			close(peerCanceled)
			return nil, ctx.Err()
		},
	}

	response, err := server.Hash(aliceCtx, &etcdserverpb.HashRequest{})
	require.Nil(t, response)
	require.ErrorIs(t, err, rpctypes.ErrPermissionDenied)
	require.Eventually(t, func() bool {
		select {
		case <-peerCanceled:
			return true
		default:
			return false
		}
	}, time.Second, time.Millisecond)
}

func TestStatusRestoresColdRaftEnvelopeFromDurableRevision(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()
	ctx := context.Background()

	put, err := server.Put(ctx, &etcdserverpb.PutRequest{
		Key: []byte("/a3465/status-cold-revision"), Value: []byte("value"),
	})
	require.NoError(t, err)
	server.backend = &coldStatusRevisionBackendShim{
		BackendShim: server.backend,
		durable:     uint64(put.GetHeader().GetRevision()),
	}

	response, err := server.Status(ctx, &etcdserverpb.StatusRequest{})
	require.NoError(t, err)
	require.Equal(t, put.GetHeader().GetRevision(), response.GetHeader().GetRevision(),
		"Status header must recover the shared durable revision")
	require.Equal(t, uint64(put.GetHeader().GetRevision()), response.GetRaftIndex(),
		"the first cold Status must not capture raft index before durable recovery")
	require.Equal(t, response.GetRaftIndex(), response.GetRaftAppliedIndex())
}

func TestStatusRefreshesLaggingFollowerRevisionFromDurableWatermark(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()
	server.backend = &coldStatusRevisionBackendShim{
		BackendShim: server.backend,
		current:     100,
		durable:     200,
	}

	response, err := server.Status(context.Background(), &etcdserverpb.StatusRequest{})
	require.NoError(t, err)
	require.Equal(t, int64(200), response.GetHeader().GetRevision())
	require.Equal(t, uint64(200), response.GetRaftIndex())
	require.Equal(t, uint64(200), response.GetRaftAppliedIndex())
	require.Equal(t, uint64(200), server.backend.GetCurrentRevision())
}

func TestAlarmMutationRestoresColdHeaderFromDurableRevision(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()
	ctx := context.Background()

	put, err := server.Put(ctx, &etcdserverpb.PutRequest{
		Key: []byte("/a3466/alarm-cold-revision"), Value: []byte("value"),
	})
	require.NoError(t, err)
	server.backend = &coldStatusRevisionBackendShim{
		BackendShim: server.backend,
		durable:     uint64(put.GetHeader().GetRevision()),
	}

	const alarmType = etcdserverpb.AlarmType(127)
	activated, err := server.Alarm(ctx, &etcdserverpb.AlarmRequest{
		Action: etcdserverpb.AlarmRequest_ACTIVATE,
		Alarm:  alarmType,
	})
	require.NoError(t, err)
	require.Len(t, activated.Alarms, 1)
	require.Equal(t, put.GetHeader().GetRevision(), activated.GetHeader().GetRevision(),
		"a successful cold-replica alarm mutation must expose the durable user revision")

	deactivated, err := server.Alarm(ctx, &etcdserverpb.AlarmRequest{
		Action: etcdserverpb.AlarmRequest_DEACTIVATE,
		Alarm:  alarmType,
	})
	require.NoError(t, err)
	require.Len(t, deactivated.Alarms, 1)
}

func TestFollowerAlarmProxiesEveryActionToLeader(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	var forwarded []etcdserverpb.AlarmRequest_AlarmAction
	server.peers = testPeerService{
		isLeader:     false,
		proxyEnabled: true,
		syncReadFn: func(context.Context) error {
			t.Fatal("proxying follower must not execute Alarm GET against local storage")
			return nil
		},
		alarmFn: func(_ context.Context, req *etcdserverpb.AlarmRequest) (*etcdserverpb.AlarmResponse, error) {
			forwarded = append(forwarded, req.GetAction())
			return &etcdserverpb.AlarmResponse{Header: proxiedResponseHeader(server, int64(len(forwarded)))}, nil
		},
	}

	for _, action := range []etcdserverpb.AlarmRequest_AlarmAction{
		etcdserverpb.AlarmRequest_GET,
		etcdserverpb.AlarmRequest_ACTIVATE,
		etcdserverpb.AlarmRequest_DEACTIVATE,
	} {
		response, err := server.Alarm(context.Background(), &etcdserverpb.AlarmRequest{Action: action})
		require.NoError(t, err)
		require.Equal(t, int64(len(forwarded)), response.GetHeader().GetRevision())
	}
	require.Equal(t, []etcdserverpb.AlarmRequest_AlarmAction{
		etcdserverpb.AlarmRequest_GET,
		etcdserverpb.AlarmRequest_ACTIVATE,
		etcdserverpb.AlarmRequest_DEACTIVATE,
	}, forwarded)
}

func TestFollowerRejectsInvalidAlarmProxyPayload(t *testing.T) {
	for _, tt := range []struct {
		name     string
		request  *etcdserverpb.AlarmRequest
		response *etcdserverpb.AlarmResponse
	}{
		{name: "nil get alarm", request: &etcdserverpb.AlarmRequest{}, response: &etcdserverpb.AlarmResponse{Header: txnHeader(2), Alarms: []*etcdserverpb.AlarmMember{nil}}},
		{name: "get filter mismatch", request: &etcdserverpb.AlarmRequest{Alarm: etcdserverpb.AlarmType_NOSPACE}, response: &etcdserverpb.AlarmResponse{Header: txnHeader(2), Alarms: []*etcdserverpb.AlarmMember{{MemberID: 1, Alarm: etcdserverpb.AlarmType_CORRUPT}}}},
		{name: "activate mismatch", request: &etcdserverpb.AlarmRequest{Action: etcdserverpb.AlarmRequest_ACTIVATE, MemberID: 1, Alarm: etcdserverpb.AlarmType_NOSPACE}, response: &etcdserverpb.AlarmResponse{Header: txnHeader(2), Alarms: []*etcdserverpb.AlarmMember{{MemberID: 2, Alarm: etcdserverpb.AlarmType_NOSPACE}}}},
		{name: "deactivate too many", request: &etcdserverpb.AlarmRequest{Action: etcdserverpb.AlarmRequest_DEACTIVATE, MemberID: 1, Alarm: etcdserverpb.AlarmType_NOSPACE}, response: &etcdserverpb.AlarmResponse{Header: txnHeader(2), Alarms: []*etcdserverpb.AlarmMember{{MemberID: 1, Alarm: etcdserverpb.AlarmType_NOSPACE}, {MemberID: 1, Alarm: etcdserverpb.AlarmType_NOSPACE}}}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			server, closeFn := newTestRPCServer(t)
			defer closeFn()
			rec := &recordingMetrics{}
			server.metricCli = rec
			initMaintenanceProxyIntegrityMetrics(rec)
			tt.response.Header.ClusterId = server.backend.ClusterID()
			tt.response.Header.MemberId = server.localMemberID()
			server.peers = testPeerService{
				isLeader: false, proxyEnabled: true,
				alarmFn: func(context.Context, *etcdserverpb.AlarmRequest) (*etcdserverpb.AlarmResponse, error) {
					return tt.response, nil
				},
			}

			response, err := server.Alarm(context.Background(), tt.request)
			require.Nil(t, response)
			require.Equal(t, codes.DataLoss, status.Code(err))
			require.Equal(t, []interface{}{int64(0), 1}, recordedMaintenanceProxyIntegrityValues(rec, maintenanceProxyRPCAlarm))
		})
	}
}

func TestFollowerUnaryMaintenanceRejectsInvalidProxyResults(t *testing.T) {
	for _, tc := range []struct {
		name      string
		invoke    func(*RPCServer) (any, error)
		configure func(*testPeerService, string)
		rpc       string
	}{
		{
			name: "alarm", rpc: maintenanceProxyRPCAlarm,
			configure: func(peers *testPeerService, shape string) {
				peers.alarmFn = func(context.Context, *etcdserverpb.AlarmRequest) (*etcdserverpb.AlarmResponse, error) {
					if shape == "mixed" {
						return &etcdserverpb.AlarmResponse{}, errors.New("mixed")
					}
					if shape == "missing_header" {
						return &etcdserverpb.AlarmResponse{}, nil
					}
					return nil, nil
				}
			},
			invoke: func(server *RPCServer) (any, error) {
				return server.Alarm(context.Background(), &etcdserverpb.AlarmRequest{})
			},
		},
		{
			name: "downgrade", rpc: maintenanceProxyRPCDowngrade,
			configure: func(peers *testPeerService, shape string) {
				peers.downgradeFn = func(context.Context, *etcdserverpb.DowngradeRequest) (*etcdserverpb.DowngradeResponse, error) {
					if shape == "mixed" {
						return &etcdserverpb.DowngradeResponse{}, errors.New("mixed")
					}
					if shape == "missing_header" {
						return &etcdserverpb.DowngradeResponse{}, nil
					}
					return nil, nil
				}
			},
			invoke: func(server *RPCServer) (any, error) {
				return server.Downgrade(context.Background(), &etcdserverpb.DowngradeRequest{})
			},
		},
	} {
		for _, shape := range []string{"nil", "mixed", "missing_header"} {
			t.Run(tc.name+"/"+shape, func(t *testing.T) {
				server, closeFn := newTestRPCServer(t)
				defer closeFn()
				rec := &recordingMetrics{}
				server.metricCli = rec
				initMaintenanceProxyIntegrityMetrics(rec)
				peers := testPeerService{isLeader: false, proxyEnabled: true}
				tc.configure(&peers, shape)
				server.peers = peers

				response, err := tc.invoke(server)
				require.Nil(t, response)
				require.Equal(t, codes.DataLoss, status.Code(err))
				require.Equal(t, []interface{}{int64(0), 1}, recordedMaintenanceProxyIntegrityValues(rec, tc.rpc))
			})
		}
	}
}

func TestFollowerDefragmentHedgesIsolatedStorageToLeader(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()
	wantErr := errors.New("auth storage unavailable")
	server.tokens.snapshots.repo.backend = &authMetadataReadErrorBackend{
		BackendShim: server.backend, err: wantErr,
	}

	request := &etcdserverpb.DefragmentRequest{}
	called := false
	server.peers = testPeerService{
		isLeader:     false,
		proxyEnabled: true,
		defragmentFn: func(_ context.Context, got *etcdserverpb.DefragmentRequest) (*etcdserverpb.DefragmentResponse, error) {
			called = true
			require.Same(t, request, got)
			return &etcdserverpb.DefragmentResponse{}, nil
		},
	}

	response, err := server.Defragment(context.Background(), request)
	require.NoError(t, err)
	require.NotNil(t, response)
	require.True(t, called)
}

func TestFollowerDefragmentHedgesIsolatedLeaderToLocalNoOp(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()
	started, canceled := make(chan struct{}), make(chan struct{})
	server.peers = testPeerService{
		isLeader: false, proxyEnabled: true,
		defragmentFn: func(ctx context.Context, _ *etcdserverpb.DefragmentRequest) (*etcdserverpb.DefragmentResponse, error) {
			close(started)
			<-ctx.Done()
			close(canceled)
			return nil, ctx.Err()
		},
	}

	response, err := server.Defragment(context.Background(), &etcdserverpb.DefragmentRequest{})
	require.NoError(t, err)
	require.NotNil(t, response)
	requireChannelsClosedEventually(t, started, canceled)
}

func TestFollowerDefragmentHedgeFailsClosedOnLocalAuthorization(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()
	aliceCtx := setupAuthKVUser(t, server)
	canceled := make(chan struct{})
	server.peers = testPeerService{
		isLeader: false, proxyEnabled: true,
		defragmentFn: func(ctx context.Context, _ *etcdserverpb.DefragmentRequest) (*etcdserverpb.DefragmentResponse, error) {
			<-ctx.Done()
			close(canceled)
			return nil, ctx.Err()
		},
	}

	response, err := server.Defragment(aliceCtx, &etcdserverpb.DefragmentRequest{})
	require.Nil(t, response)
	require.ErrorIs(t, err, rpctypes.ErrPermissionDenied)
	require.Eventually(t, func() bool {
		select {
		case <-canceled:
			return true
		default:
			return false
		}
	}, time.Second, time.Millisecond)
}

func TestMaintenanceDefragmentFallsBackToProtectedAuthSnapshotWhenBackendIsUnavailable(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()
	checkpoint := backend.SerializableCheckpoint{
		Revision: 37, Timestamp: 101, AuthRevision: 1, ValidUntil: time.Now().Add(time.Minute),
	}
	shim := &checkpointDefragmentBackendShim{
		BackendShim: server.backend, checkpoint: checkpoint, liveErr: storage.ErrUnavailable,
	}
	server.backend = shim
	server.auth.repo.backend = shim
	server.tokens.repo.backend = shim
	server.tokens.snapshots = newAuthSnapshotCache(shim)
	server.peers = testPeerService{isLeader: true}

	response, err := server.Defragment(context.Background(), &etcdserverpb.DefragmentRequest{})
	require.NoError(t, err)
	require.NotNil(t, response)
	require.Nil(t, response.Header)
	require.Equal(t, 1, shim.liveCalls)
}

func TestMaintenanceDefragmentDoesNotMaskDeterministicAuthFailureWithCheckpoint(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()
	checkpoint := backend.SerializableCheckpoint{
		Revision: 37, Timestamp: 101, AuthRevision: 1, ValidUntil: time.Now().Add(time.Minute),
	}
	wantErr := status.Error(codes.DataLoss, "corrupt auth metadata")
	shim := &checkpointDefragmentBackendShim{
		BackendShim: server.backend, checkpoint: checkpoint, liveErr: wantErr,
	}
	server.backend = shim
	server.auth.repo.backend = shim
	server.tokens.repo.backend = shim
	server.tokens.snapshots = newAuthSnapshotCache(shim)
	server.peers = testPeerService{isLeader: true}

	response, err := server.Defragment(context.Background(), &etcdserverpb.DefragmentRequest{})
	require.Nil(t, response)
	require.ErrorIs(t, err, wantErr)
	require.Equal(t, 1, shim.liveCalls)
}

func TestMaintenanceDefragmentCheckpointFallbackPreservesAdminAuthorization(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()
	aliceCtx := setupAuthKVUser(t, server)
	rootToken, err := server.tokens.authenticate(context.Background(), "root", "root-secret")
	require.NoError(t, err)
	rootCtx := metadata.NewIncomingContext(
		context.Background(), metadata.Pairs(rpctypes.TokenFieldNameGRPC, rootToken),
	)
	_, err = server.Defragment(rootCtx, &etcdserverpb.DefragmentRequest{})
	require.NoError(t, err, "prime the complete member-local root auth snapshot")
	authSnapshot, err := server.tokens.snapshots.current(context.Background())
	require.NoError(t, err)
	checkpoint := backend.SerializableCheckpoint{
		Revision: 37, Timestamp: 101, AuthRevision: authSnapshot.Config.Revision,
		ValidUntil: time.Now().Add(time.Minute),
	}
	shim := &checkpointDefragmentBackendShim{
		BackendShim: server.backend, checkpoint: checkpoint, liveErr: storage.ErrUnavailable,
	}
	server.backend = shim
	server.auth.repo.backend = shim
	server.tokens.repo.backend = shim
	server.peers = testPeerService{isLeader: true}

	response, err := server.Defragment(rootCtx, &etcdserverpb.DefragmentRequest{})
	require.NoError(t, err)
	require.NotNil(t, response)
	require.Nil(t, response.Header)

	response, err = server.Defragment(aliceCtx, &etcdserverpb.DefragmentRequest{})
	require.Nil(t, response)
	require.ErrorIs(t, err, rpctypes.ErrPermissionDenied)
}

func TestFollowerDowngradeProxiesEveryActionToLeader(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	var forwarded []etcdserverpb.DowngradeRequest_DowngradeAction
	server.peers = testPeerService{
		isLeader:     false,
		proxyEnabled: true,
		downgradeFn: func(_ context.Context, req *etcdserverpb.DowngradeRequest) (*etcdserverpb.DowngradeResponse, error) {
			forwarded = append(forwarded, req.GetAction())
			return &etcdserverpb.DowngradeResponse{Header: proxiedResponseHeader(server, int64(len(forwarded))), Version: ClusterVersion}, nil
		},
	}

	actions := []etcdserverpb.DowngradeRequest_DowngradeAction{
		etcdserverpb.DowngradeRequest_VALIDATE,
		etcdserverpb.DowngradeRequest_ENABLE,
		etcdserverpb.DowngradeRequest_CANCEL,
		etcdserverpb.DowngradeRequest_DowngradeAction(127),
	}
	for _, action := range actions {
		response, err := server.Downgrade(context.Background(), &etcdserverpb.DowngradeRequest{
			Action: action, Version: "3.6",
		})
		if action == etcdserverpb.DowngradeRequest_DowngradeAction(127) {
			require.Nil(t, response)
			require.Equal(t, codes.DataLoss, status.Code(err))
			continue
		}
		require.NoError(t, err)
		require.Equal(t, int64(len(forwarded)), response.GetHeader().GetRevision())
	}
	require.Equal(t, actions, forwarded)
}

func TestFollowerDowngradeRejectsInvalidProxyPayload(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()
	rec := &recordingMetrics{}
	server.metricCli = rec
	initMaintenanceProxyIntegrityMetrics(rec)
	server.peers = testPeerService{
		isLeader: false, proxyEnabled: true,
		downgradeFn: func(context.Context, *etcdserverpb.DowngradeRequest) (*etcdserverpb.DowngradeResponse, error) {
			return &etcdserverpb.DowngradeResponse{Header: proxiedResponseHeader(server, 2), Version: "3.6"}, nil
		},
	}

	response, err := server.Downgrade(context.Background(), &etcdserverpb.DowngradeRequest{
		Action: etcdserverpb.DowngradeRequest_VALIDATE, Version: "3.6",
	})
	require.Nil(t, response)
	require.Equal(t, codes.DataLoss, status.Code(err))
	require.Equal(t, []interface{}{int64(0), 1}, recordedMaintenanceProxyIntegrityValues(rec, maintenanceProxyRPCDowngrade))
}

func TestAlarmMutationFailsBeforeWriteWhenColdRevisionUnavailable(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()
	ctx := context.Background()
	wantErr := errors.New("durable revision unavailable")
	underlying := server.backend
	server.backend = &coldStatusRevisionBackendShim{
		BackendShim: underlying,
		durableErr:  wantErr,
	}

	const alarmType = etcdserverpb.AlarmType(126)
	response, err := server.Alarm(ctx, &etcdserverpb.AlarmRequest{
		Action: etcdserverpb.AlarmRequest_ACTIVATE,
		Alarm:  alarmType,
	})
	require.Nil(t, response)
	require.ErrorIs(t, err, wantErr)

	server.backend = underlying
	alarms, err := server.genericAlarms(ctx, alarmType)
	require.NoError(t, err)
	require.Empty(t, alarms, "header recovery failure must happen before persistent alarm mutation")
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

func TestMaintenanceFixedRevisionHashKVUsesProtectedCheckpoint(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()
	_, err := server.HashKV(context.Background(), &etcdserverpb.HashKVRequest{Revision: 1})
	require.NoError(t, err, "prime the complete member-local auth snapshot")
	recorded := &recordingMetrics{}
	server.metricCli = recorded

	checkpoint := backend.SerializableCheckpoint{
		Revision: 91, Timestamp: 1234, CompactRevision: 0, AuthRevision: 1,
		ValidUntil: time.Now().Add(time.Minute),
	}
	server.backend = &checkpointHashKVBackendShim{
		BackendShim: server.backend, t: t, checkpoint: checkpoint,
	}
	synced := false
	server.peers = testPeerService{
		isLeader: true, epochFn: func() (uint64, bool) { return 7, true },
		syncReadFn: func(context.Context) error {
			synced = true
			return errors.New("fixed-revision HashKV must not cross a read-index barrier")
		},
	}

	response, err := server.HashKV(context.Background(), &etcdserverpb.HashKVRequest{Revision: 1})
	require.NoError(t, err)
	require.Equal(t, int64(91), response.Header.Revision)
	require.Equal(t, int64(1), response.HashRevision)
	require.Equal(t, int64(0), response.CompactRevision)
	require.Equal(t, uint32(71), response.Hash)
	require.False(t, synced)
	var stages []string
	for _, counter := range recorded.counters {
		if counter.name == "maintenance.hashkv.stage" {
			require.Len(t, counter.tags, 1)
			require.Equal(t, []metrics.T{metrics.Tag("stage", counter.tags[0].Value)}, counter.tags)
			stages = append(stages, counter.tags[0].Value)
		}
	}
	require.Equal(t, []string{
		"entered", "checkpoint_attached", "local", "local_entered", "auth_complete",
		"refresh_complete", "revision_validated", "backend_started", "backend_complete",
	}, stages)
}

func TestMaintenanceHashFallsBackToProtectedCheckpointWhenBackendIsUnavailable(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()
	_, err := server.Hash(context.Background(), &etcdserverpb.HashRequest{})
	require.NoError(t, err, "prime the complete member-local auth snapshot")
	checkpoint := backend.SerializableCheckpoint{
		Revision: 91, Timestamp: 1234, CompactRevision: 7, AuthRevision: 1,
		ValidUntil: time.Now().Add(time.Minute),
	}
	shim := &checkpointHashBackendShim{
		BackendShim: server.backend, t: t, checkpoint: checkpoint, liveErr: storage.ErrUnavailable,
	}
	server.backend = shim
	server.auth.repo.backend = shim
	server.tokens.repo.backend = shim
	server.tokens.snapshots = newAuthSnapshotCache(shim)
	server.peers = testPeerService{isLeader: true}

	response, err := server.Hash(context.Background(), &etcdserverpb.HashRequest{})
	require.NoError(t, err)
	require.Equal(t, uint32(73), response.Hash)
	require.Equal(t, int64(checkpoint.Revision), response.Header.Revision)
	require.Equal(t, 1, shim.liveCalls)
	require.Equal(t, 1, shim.pinnedCalls)
}

func TestMaintenanceHashDoesNotMaskDeterministicBackendFailureWithCheckpoint(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()
	checkpoint := backend.SerializableCheckpoint{
		Revision: 91, Timestamp: 1234, CompactRevision: 7, ValidUntil: time.Now().Add(time.Minute),
	}
	wantErr := status.Error(codes.DataLoss, "corrupt backend hash")
	shim := &checkpointHashBackendShim{
		BackendShim: server.backend, t: t, checkpoint: checkpoint, liveErr: wantErr,
	}
	server.backend = shim
	server.auth.repo.backend = shim
	server.tokens.repo.backend = shim
	server.tokens.snapshots = newAuthSnapshotCache(shim)
	server.peers = testPeerService{isLeader: true}

	response, err := server.Hash(context.Background(), &etcdserverpb.HashRequest{})
	require.Nil(t, response)
	require.ErrorIs(t, err, wantErr)
	require.Equal(t, 1, shim.liveCalls)
	require.Zero(t, shim.pinnedCalls)
}

func TestMaintenanceHashCheckpointFallbackPreservesAdminAuthorization(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()
	aliceCtx := setupAuthKVUser(t, server)
	rootToken, err := server.tokens.authenticate(context.Background(), "root", "root-secret")
	require.NoError(t, err)
	rootCtx := metadata.NewIncomingContext(
		context.Background(), metadata.Pairs(rpctypes.TokenFieldNameGRPC, rootToken),
	)
	_, err = server.Hash(rootCtx, &etcdserverpb.HashRequest{})
	require.NoError(t, err, "prime the complete member-local root auth snapshot")
	authSnapshot, err := server.tokens.snapshots.current(context.Background())
	require.NoError(t, err)

	checkpoint := backend.SerializableCheckpoint{
		Revision: 91, Timestamp: 1234, CompactRevision: 7,
		AuthRevision: authSnapshot.Config.Revision, ValidUntil: time.Now().Add(time.Minute),
	}
	shim := &checkpointHashBackendShim{
		BackendShim: server.backend, t: t, checkpoint: checkpoint, liveErr: storage.ErrUnavailable,
	}
	server.backend = shim
	server.auth.repo.backend = shim
	server.tokens.repo.backend = shim
	server.peers = testPeerService{isLeader: true}

	response, err := server.Hash(rootCtx, &etcdserverpb.HashRequest{})
	require.NoError(t, err)
	require.Equal(t, uint32(73), response.Hash)
	require.Equal(t, int64(checkpoint.Revision), response.Header.Revision)
	require.Equal(t, 1, shim.pinnedCalls)

	response, err = server.Hash(aliceCtx, &etcdserverpb.HashRequest{})
	require.Nil(t, response)
	require.ErrorIs(t, err, rpctypes.ErrPermissionDenied)
	require.Equal(t, 1, shim.pinnedCalls, "a non-admin caller must not reach the protected backend hash")
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

func TestAlarmStatusErrorMatchesReferenceStatusText(t *testing.T) {
	tests := []struct {
		name  string
		alarm *etcdserverpb.AlarmMember
		want  string
	}{
		{name: "zero message", alarm: &etcdserverpb.AlarmMember{}, want: ""},
		{name: "zero member", alarm: &etcdserverpb.AlarmMember{Alarm: etcdserverpb.AlarmType_NOSPACE}, want: "alarm:NOSPACE"},
		{name: "zero alarm", alarm: &etcdserverpb.AlarmMember{MemberID: 7}, want: "memberID:7"},
		{name: "known", alarm: &etcdserverpb.AlarmMember{MemberID: 7, Alarm: etcdserverpb.AlarmType_CORRUPT}, want: "memberID:7  alarm:CORRUPT"},
		{name: "unknown", alarm: &etcdserverpb.AlarmMember{MemberID: 7, Alarm: etcdserverpb.AlarmType(127)}, want: "memberID:7  alarm:127"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			require.Equal(t, test.want, alarmStatusError(test.alarm))
			_, err := proto.Marshal(test.alarm)
			require.NoError(t, err)
			require.True(t, matchesAlarmStatusError(test.alarm, test.alarm.String()))
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

func TestPeerHashKVHandlerHedgesIsolatedStorageWithTrustedMarker(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()
	wantErr := errors.New("isolated peer hash storage must not be reached")
	server.backend = &maintenanceHashTrapBackendShim{BackendShim: server.backend, err: wantErr}
	want := &etcdserverpb.HashKVResponse{
		Header: proxiedResponseHeader(server, 77), Hash: 202, HashRevision: 42, CompactRevision: 10,
	}
	type forwardedHashKV struct {
		request  *etcdserverpb.HashKVRequest
		metadata metadata.MD
	}
	forwarded := make(chan forwardedHashKV, 1)
	server.peers = testPeerService{
		isLeader: false, proxyEnabled: true,
		epochFn: func() (uint64, bool) { return 7, false },
		hashKVFn: func(ctx context.Context, request *etcdserverpb.HashKVRequest) (*etcdserverpb.HashKVResponse, error) {
			outgoing, _ := metadata.FromOutgoingContext(ctx)
			forwarded <- forwardedHashKV{request: request, metadata: outgoing.Copy()}
			return want, nil
		},
	}
	body, err := json.Marshal(&etcdserverpb.HashKVRequest{Revision: 42})
	require.NoError(t, err)
	req := httptest.NewRequest(http.MethodGet, PeerHashKVPath, bytes.NewReader(body))
	req = req.WithContext(metadata.NewOutgoingContext(req.Context(), metadata.Pairs(
		authorizedPeerHashKVProxyMetadataKey, "malformed",
		"kubebrain-test-peer-metadata", "preserved",
	)))
	rec := httptest.NewRecorder()

	server.peerHashKVHandler(rec, req)

	require.Equal(t, http.StatusOK, rec.Code)
	gotForwarded := <-forwarded
	require.Equal(t, int64(42), gotForwarded.request.GetRevision())
	require.Equal(t, []string{"1"}, gotForwarded.metadata.Get(authorizedPeerHashKVProxyMetadataKey))
	require.Equal(t, []string{"preserved"}, gotForwarded.metadata.Get("kubebrain-test-peer-metadata"))
	var response etcdserverpb.HashKVResponse
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &response))
	require.Equal(t, want.GetHash(), response.GetHash())
	require.Equal(t, want.GetHashRevision(), response.GetHashRevision())
	require.Equal(t, want.GetCompactRevision(), response.GetCompactRevision())
	require.Equal(t, uint64(77), server.backend.GetCurrentRevision())
}

func TestPeerHashKVHandlerHedgesIsolatedLeaderWithLocalStorage(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()
	put, err := server.Put(context.Background(), &etcdserverpb.PutRequest{
		Key: []byte("/registry/maintenance/peer-hash-local-hedge"), Value: []byte("value"),
	})
	require.NoError(t, err)
	want, err := server.backend.HashKV(context.Background(), put.GetHeader().GetRevision())
	require.NoError(t, err)
	peerStarted := make(chan struct{})
	peerCanceled := make(chan struct{})
	server.peers = testPeerService{
		isLeader: false, proxyEnabled: true,
		epochFn: func() (uint64, bool) { return 7, false },
		hashKVFn: func(ctx context.Context, request *etcdserverpb.HashKVRequest) (*etcdserverpb.HashKVResponse, error) {
			close(peerStarted)
			<-ctx.Done()
			close(peerCanceled)
			return nil, ctx.Err()
		},
	}
	body, err := json.Marshal(&etcdserverpb.HashKVRequest{Revision: put.GetHeader().GetRevision()})
	require.NoError(t, err)
	req := httptest.NewRequest(http.MethodGet, PeerHashKVPath, bytes.NewReader(body))
	rec := httptest.NewRecorder()

	server.peerHashKVHandler(rec, req)

	require.Equal(t, http.StatusOK, rec.Code)
	var response etcdserverpb.HashKVResponse
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &response))
	require.Equal(t, want.Hash, response.GetHash())
	require.Equal(t, want.HashRevision, response.GetHashRevision())
	require.Equal(t, want.CompactRevision, response.GetCompactRevision())
	require.Eventually(t, func() bool {
		select {
		case <-peerStarted:
			select {
			case <-peerCanceled:
				return true
			default:
			}
		default:
		}
		return false
	}, time.Second, time.Millisecond)
}

func TestAuthorizedPeerHashKVProxyMarkerRequiresCanonicalPeerSingleton(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()
	setupAuthKVUser(t, server)
	server.peers = testPeerService{isLeader: true, epochFn: func() (uint64, bool) { return 7, true }}
	tests := []struct {
		name       string
		peer       bool
		values     []string
		authorized bool
	}{
		{name: "public canonical", values: []string{"1"}},
		{name: "peer canonical", peer: true, values: []string{"1"}, authorized: true},
		{name: "peer malformed", peer: true, values: []string{"malformed"}},
		{name: "peer duplicated", peer: true, values: []string{"1", "1"}},
		{name: "peer mixed", peer: true, values: []string{"1", "malformed"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			md := metadata.MD{}
			md.Set(authorizedPeerHashKVProxyMetadataKey, tt.values...)
			ctx := metadata.NewIncomingContext(context.Background(), md)
			if tt.peer {
				ctx = context.WithValue(ctx, peerRequestContextKey{}, true)
			}
			response, err := server.HashKV(ctx, &etcdserverpb.HashKVRequest{})
			if tt.authorized {
				require.NoError(t, err)
				require.NotNil(t, response)
				return
			}
			require.Nil(t, response)
			require.ErrorIs(t, err, rpctypes.ErrUserEmpty)
		})
	}
}

func TestPeerHashKVHandlerMapsForwardedRevisionErrors(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want string
	}{
		{name: "future", err: rpctypes.ErrGRPCFutureRev, want: "mvcc: required revision is a future revision\n"},
		{name: "compacted", err: rpctypes.ErrGRPCCompacted, want: "mvcc: required revision has been compacted\n"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			server, closeFn := newTestRPCServer(t)
			defer closeFn()
			server.peers = testPeerService{
				isLeader: false, proxyEnabled: true,
				epochFn: func() (uint64, bool) { return 7, false },
				hashKVFn: func(context.Context, *etcdserverpb.HashKVRequest) (*etcdserverpb.HashKVResponse, error) {
					return nil, test.err
				},
			}
			req := httptest.NewRequest(http.MethodGet, PeerHashKVPath, bytes.NewReader([]byte(`{"revision":42}`)))
			rec := httptest.NewRecorder()

			server.peerHashKVHandler(rec, req)

			require.Equal(t, http.StatusBadRequest, rec.Code)
			require.Equal(t, test.want, rec.Body.String())
		})
	}
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
		{
			name:       "body too large",
			method:     http.MethodGet,
			target:     PeerHashKVPath,
			body:       bytes.NewReader(bytes.Repeat([]byte("x"), maxPeerHashKVRequestBytes+1)),
			wantStatus: http.StatusRequestEntityTooLarge,
			wantBody:   "request body too large",
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

func TestHedgedPeerHashKVRejectsInvalidForwardedResult(t *testing.T) {
	for _, shape := range []string{"nil", "mixed", "missing_header", "negative_revision", "payload"} {
		t.Run(shape, func(t *testing.T) {
			server, closeFn := newTestRPCServer(t)
			defer closeFn()
			rec := &recordingMetrics{}
			server.metricCli = rec
			initMaintenanceProxyIntegrityMetrics(rec)
			server.backend = &maintenanceHashTrapBackendShim{BackendShim: server.backend, err: errors.New("local storage unavailable")}
			server.peers = testPeerService{
				isLeader: false, proxyEnabled: true,
				hashKVFn: func(context.Context, *etcdserverpb.HashKVRequest) (*etcdserverpb.HashKVResponse, error) {
					if shape == "mixed" {
						return &etcdserverpb.HashKVResponse{}, errors.New("mixed")
					}
					if shape == "missing_header" {
						return &etcdserverpb.HashKVResponse{}, nil
					}
					if shape == "negative_revision" {
						return &etcdserverpb.HashKVResponse{Header: proxiedResponseHeader(server, -1)}, nil
					}
					if shape == "payload" {
						return &etcdserverpb.HashKVResponse{Header: proxiedResponseHeader(server, 7), HashRevision: 6, CompactRevision: -1}, nil
					}
					return nil, nil
				},
			}

			response, err := server.hedgedPeerHashKV(context.Background(), &etcdserverpb.HashKVRequest{})
			require.Nil(t, response)
			require.Equal(t, codes.DataLoss, status.Code(err))
			require.Equal(t, []interface{}{int64(0), 1},
				recordedMaintenanceProxyIntegrityValues(rec, maintenanceProxyRPCHashKV))
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
			wantBody: "mvcc: required revision has been compacted\n",
		},
		{
			name:     "future",
			revision: second.Header.Revision + 100,
			wantBody: "mvcc: required revision is a future revision\n",
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
			require.Equal(t, tt.wantBody, rec.Body.String())
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
	require.Contains(t, statusResp.Errors, alarmStatusError(activated.Alarms[0]))
	require.Contains(t, statusResp.Errors, alarmStatusError(get.Alarms[0]))
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
	require.ElementsMatch(t, []string{
		alarmStatusError(noSpace.Alarms[0]), alarmStatusError(corrupt.Alarms[0]),
	}, statusResp.Errors)

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
	require.Equal(t, []string{alarmStatusError(activated.Alarms[0])}, active.Errors)

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
			require.ErrorIs(t, err, backend.ErrInvalidAlarmMetadata)
			require.ErrorContains(t, err, test.err)
		})
	}
}

func TestCorruptAlarmDisarmWrongMemberRejectsMalformedGenerationDirect(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()
	ctx := context.Background()
	require.NoError(t, server.backend.InternalPut(ctx, []byte("alarms/corrupt-generation"), []byte("bad")))

	_, err := server.Alarm(ctx, &etcdserverpb.AlarmRequest{
		Action: etcdserverpb.AlarmRequest_DEACTIVATE,
		Alarm:  etcdserverpb.AlarmType_CORRUPT,
		// No member owns this alarm. The no-op shape must still validate the
		// generation that would guard any future activation/deactivation.
		MemberID: 99117,
	})
	require.ErrorIs(t, err, backend.ErrInvalidAlarmMetadata)
	require.ErrorContains(t, err, "corrupt alarm generation has length 3")
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
			require.Equal(t, backend.resultRevision, header.Revision)
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

func TestAlarmRejectsUnknownActionWithoutBackendMutation(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	response, err := server.Alarm(context.Background(), &etcdserverpb.AlarmRequest{
		Action: etcdserverpb.AlarmRequest_AlarmAction(99),
		Alarm:  etcdserverpb.AlarmType_NOSPACE,
	})
	require.Nil(t, response)
	require.Equal(t, codes.InvalidArgument, status.Code(err))
	require.Equal(t, "etcdserver: invalid alarm action", status.Convert(err).Message())

	alarms, listErr := server.Alarm(context.Background(), &etcdserverpb.AlarmRequest{
		Action: etcdserverpb.AlarmRequest_GET,
	})
	require.NoError(t, listErr)
	require.Empty(t, alarms.Alarms)
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
