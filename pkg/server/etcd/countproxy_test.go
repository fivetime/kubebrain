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
	"path"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	proto "github.com/kubewharf/kubebrain-client/api/v2rpc"
	"github.com/kubewharf/kubebrain/pkg/backend"
	"github.com/kubewharf/kubebrain/pkg/metrics/mock"
	imemkv "github.com/kubewharf/kubebrain/pkg/storage/memkv"
)

// TestCountProxyServesFollowerCounts pins #41: when the local count index
// cannot serve (a follower — the index is leader-only), counts route to the
// wired proxy (the leader's index over the wire) instead of a full range scan;
// when the proxy declines, the local fallback still answers correctly.
func TestCountProxyServesFollowerCounts(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	m := mock.NewMinimalMetrics(ctrl)
	kv := imemkv.NewKvStorage()
	defer func() { require.NoError(t, kv.Close()) }()

	pfx := fmt.Sprintf("/kubebrain/cproxy_test/%d", time.Now().UnixNano())
	// EnableCountIndex is off: CountAtRevision never serves, mimicking a
	// follower where the index does not exist.
	be := backend.NewBackend(kv, backend.Config{
		Prefix: pfx, Identity: fmt.Sprintf("cproxy-%d", time.Now().UnixNano()),
		EnableEtcdCompatibility: true,
	}, m)
	be.SetCurrentRevision(uint64(time.Now().UnixNano()))
	ctx := context.Background()

	for i := 0; i < 3; i++ {
		_, err := be.Create(ctx, &proto.CreateRequest{
			Key: []byte(path.Join(pfx, fmt.Sprintf("k%d", i))), Value: []byte("v")})
		require.NoError(t, err)
	}

	shim := NewBackendShim(be, m).(*backendShim)

	rangeEnd := []byte(pfx + "0") // prefix end
	req := &etcdserverpb.RangeRequest{Key: []byte(pfx + "/"), RangeEnd: rangeEnd, CountOnly: true}

	// Proxy answers: its count wins over the local scan.
	proxied := 0
	shim.SetCountProxy(func(ctx context.Context, r *etcdserverpb.RangeRequest) (int64, bool) {
		proxied++
		require.True(t, r.CountOnly)
		return 42, true
	})
	resp, err := shim.Count(ctx, req)
	require.NoError(t, err)
	require.Equal(t, int64(42), resp.Count, "proxy-served count must win over the local fallback")
	require.Equal(t, 1, proxied)

	// Proxy declines: the local fallback still answers with the real count.
	shim.SetCountProxy(func(ctx context.Context, r *etcdserverpb.RangeRequest) (int64, bool) {
		return 0, false
	})
	resp, err = shim.Count(ctx, req)
	require.NoError(t, err)
	require.Equal(t, int64(3), resp.Count, "local fallback must answer when the proxy declines")

	// No proxy wired (leader / tests): identical local behavior.
	shim.SetCountProxy(nil)
	resp, err = shim.Count(ctx, req)
	require.NoError(t, err)
	require.Equal(t, int64(3), resp.Count)
}

// TestCountProxyFastRejectsWhenIndexNotReady pins the review-51 leftover: during
// a leader's count-index rebuild, a follower-proxied count the index cannot serve
// must be fast-rejected (Unavailable) so the follower falls back locally, instead
// of the leader running a full-scan for every proxied count and taking the whole
// cluster's count load. A directly-connected client (no proxy marker) still gets
// the leader's best-effort scan.
func TestCountProxyFastRejectsWhenIndexNotReady(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	m := mock.NewMinimalMetrics(ctrl)
	kv := imemkv.NewKvStorage()
	defer func() { require.NoError(t, kv.Close()) }()

	pfx := fmt.Sprintf("/kubebrain/cproxy_reject/%d", time.Now().UnixNano())
	// Count index off => CountAtRevision never serves: the leader mid-rebuild.
	be := backend.NewBackend(kv, backend.Config{
		Prefix: pfx, Identity: fmt.Sprintf("cproxy-reject-%d", time.Now().UnixNano()),
		EnableEtcdCompatibility: true,
	}, m)
	be.SetCurrentRevision(uint64(time.Now().UnixNano()))
	ctx := context.Background()
	for i := 0; i < 3; i++ {
		_, err := be.Create(ctx, &proto.CreateRequest{Key: []byte(path.Join(pfx, fmt.Sprintf("k%d", i))), Value: []byte("v")})
		require.NoError(t, err)
	}

	shim := NewBackendShim(be, m).(*backendShim)
	shim.SetCountProxy(nil) // this node is the leader (nothing to proxy to)
	req := &etcdserverpb.RangeRequest{Key: []byte(pfx + "/"), RangeEnd: []byte(pfx + "0"), CountOnly: true}

	// Direct client (no marker): best-effort scan returns the real count.
	resp, err := shim.Count(ctx, req)
	require.NoError(t, err)
	require.Equal(t, int64(3), resp.Count, "a direct client must get the leader's fallback scan")

	// Follower-proxied count (marker present) the index cannot serve: fast-reject.
	proxyCtx := metadata.NewIncomingContext(ctx, metadata.Pairs(countProxyMarkerKey, "1"))
	_, err = shim.Count(proxyCtx, req)
	requireCountProxyIndexNotReady(t, err)

	// Same for a revision-pinned proxied count.
	reqRev := &etcdserverpb.RangeRequest{Key: []byte(pfx + "/"), RangeEnd: []byte(pfx + "0"), CountOnly: true, Revision: int64(be.GetCurrentRevision())}
	_, err = shim.Count(proxyCtx, reqRev)
	requireCountProxyIndexNotReady(t, err)
}

func requireCountProxyIndexNotReady(t *testing.T, err error) {
	t.Helper()
	require.Error(t, err)
	require.Equal(t, codes.Unavailable, status.Code(err))
	require.Equal(t, "count index not ready (rebuilding); fall back locally", status.Convert(err).Message())
}
