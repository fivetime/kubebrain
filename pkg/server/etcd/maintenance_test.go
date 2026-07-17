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
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
)

type writeAfterHashBackendShim struct {
	BackendShim
	t               *testing.T
	currentRevision int64
}

func (b *writeAfterHashBackendShim) HashKV(ctx context.Context, revision int64) (uint32, int64, int64, error) {
	hash, hashRevision, currentRevision, err := b.BackendShim.HashKV(ctx, revision)
	if err != nil {
		return 0, 0, 0, err
	}
	b.currentRevision = currentRevision
	_, err = b.BackendShim.Put(ctx, &etcdserverpb.PutRequest{
		Key:   []byte("/registry/maintenance/write-after-hash"),
		Value: []byte(fmt.Sprintf("after-%d", hashRevision)),
	})
	require.NoError(b.t, err)
	return hash, hashRevision, currentRevision, nil
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

	defragResp, err := server.Defragment(ctx, &etcdserverpb.DefragmentRequest{})
	require.NoError(t, err)
	require.Nil(t, defragResp.Header)
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
			require.Equal(t, backend.currentRevision, header.Revision)
			require.Greater(t, int64(server.backend.GetCurrentRevision()), header.Revision,
				"the injected post-hash write must advance current revision")
		})
	}
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
