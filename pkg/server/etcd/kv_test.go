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
	"sync"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/mvccpb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/kubewharf/kubebrain/pkg/backend"
	"github.com/kubewharf/kubebrain/pkg/metrics/mock"
	"github.com/kubewharf/kubebrain/pkg/server/service/etcdproxy"
	"github.com/kubewharf/kubebrain/pkg/server/service/leader"
	"github.com/kubewharf/kubebrain/pkg/storage/memkv"
)

type testPeerService struct {
	isLeader         bool
	noLeader         bool
	proxyEnabled     bool
	rangeFn          func(context.Context, *etcdserverpb.RangeRequest) (*etcdserverpb.RangeResponse, error)
	putFn            func(context.Context, *etcdserverpb.PutRequest) (*etcdserverpb.PutResponse, error)
	deleteRangeFn    func(context.Context, *etcdserverpb.DeleteRangeRequest) (*etcdserverpb.DeleteRangeResponse, error)
	compactFn        func(context.Context, *etcdserverpb.CompactionRequest) (*etcdserverpb.CompactionResponse, error)
	watchFn          func(context.Context, []byte, []byte, uint64) (<-chan etcdproxy.WatchResult, error)
	leaseGrantFn     func(context.Context, *etcdserverpb.LeaseGrantRequest) (*etcdserverpb.LeaseGrantResponse, error)
	leaseKeepAliveFn func(context.Context, *etcdserverpb.LeaseKeepAliveRequest) (*etcdserverpb.LeaseKeepAliveResponse, error)
}

func (testPeerService) SyncReadRevision(context.Context) error {
	return nil
}

func (testPeerService) Close() error {
	return nil
}

func (testPeerService) Campaign(context.Context) {
}

func (s testPeerService) GetLeaderInfo() string {
	if s.noLeader {
		return ""
	}
	return "test-peer"
}

func (s testPeerService) IsLeader() bool {
	return s.isLeader
}

func (s testPeerService) EpochAndLeadingFresh() (uint64, bool) {
	return 0, s.isLeader
}

func (s testPeerService) GetElectionInfo() (leader.ElectionInfo, error) {
	return leader.ElectionInfo{LeaderAddress: "test-peer", IsLeader: s.isLeader}, nil
}

func (s testPeerService) EtcdProxyEnabled() bool {
	return s.proxyEnabled
}

func (s testPeerService) Ready() error {
	return nil
}

func (testPeerService) Txn(context.Context, *etcdserverpb.TxnRequest) (*etcdserverpb.TxnResponse, error) {
	return nil, nil
}

func (s testPeerService) Range(ctx context.Context, req *etcdserverpb.RangeRequest) (*etcdserverpb.RangeResponse, error) {
	if s.rangeFn != nil {
		return s.rangeFn(ctx, req)
	}
	return nil, nil
}

func (s testPeerService) Put(ctx context.Context, req *etcdserverpb.PutRequest) (*etcdserverpb.PutResponse, error) {
	if s.putFn != nil {
		return s.putFn(ctx, req)
	}
	return nil, nil
}

func (s testPeerService) DeleteRange(ctx context.Context, req *etcdserverpb.DeleteRangeRequest) (*etcdserverpb.DeleteRangeResponse, error) {
	if s.deleteRangeFn != nil {
		return s.deleteRangeFn(ctx, req)
	}
	return nil, nil
}

func (s testPeerService) Compact(ctx context.Context, req *etcdserverpb.CompactionRequest) (*etcdserverpb.CompactionResponse, error) {
	if s.compactFn != nil {
		return s.compactFn(ctx, req)
	}
	return nil, nil
}

func (s testPeerService) Watch(ctx context.Context, key, rangeEnd []byte, revision uint64) (<-chan etcdproxy.WatchResult, error) {
	if s.watchFn != nil {
		return s.watchFn(ctx, key, rangeEnd, revision)
	}
	return nil, nil
}

func (s testPeerService) LeaseGrant(ctx context.Context, req *etcdserverpb.LeaseGrantRequest) (*etcdserverpb.LeaseGrantResponse, error) {
	if s.leaseGrantFn != nil {
		return s.leaseGrantFn(ctx, req)
	}
	return nil, nil
}

func (testPeerService) LeaseRevoke(context.Context, *etcdserverpb.LeaseRevokeRequest) (*etcdserverpb.LeaseRevokeResponse, error) {
	return nil, nil
}

func (s testPeerService) LeaseKeepAlive(ctx context.Context, req *etcdserverpb.LeaseKeepAliveRequest) (*etcdserverpb.LeaseKeepAliveResponse, error) {
	if s.leaseKeepAliveFn != nil {
		return s.leaseKeepAliveFn(ctx, req)
	}
	return nil, nil
}

func (testPeerService) LeaseTimeToLive(context.Context, *etcdserverpb.LeaseTimeToLiveRequest) (*etcdserverpb.LeaseTimeToLiveResponse, error) {
	return nil, nil
}

func (testPeerService) LeaseLeases(context.Context, *etcdserverpb.LeaseLeasesRequest) (*etcdserverpb.LeaseLeasesResponse, error) {
	return nil, nil
}

func newTestRPCServer(t *testing.T) (*RPCServer, func()) {
	ctrl := gomock.NewController(t)
	metrics := mock.NewMinimalMetrics(ctrl)
	kv := memkv.NewKvStorage()
	b := backend.NewBackend(kv, backend.Config{
		Identity:                "test-peer",
		EnableEtcdCompatibility: true,
	}, metrics)
	server := New(b, metrics, testPeerService{isLeader: true})
	return server, func() {
		server.stopLeases()
		require.NoError(t, kv.Close())
		ctrl.Finish()
	}
}

func TestPutCreatesAndOverwritesKey(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	ctx := context.Background()
	putResp, err := server.Put(ctx, &etcdserverpb.PutRequest{
		Key:   []byte("/registry/pods/a"),
		Value: []byte("v1"),
	})
	require.NoError(t, err)
	require.NotNil(t, putResp.Header)

	rangeResp, err := server.Range(ctx, &etcdserverpb.RangeRequest{Key: []byte("/registry/pods/a")})
	require.NoError(t, err)
	require.Len(t, rangeResp.Kvs, 1)
	require.Equal(t, []byte("v1"), rangeResp.Kvs[0].Value)
	firstRevision := rangeResp.Kvs[0].ModRevision

	overwriteResp, err := server.Put(ctx, &etcdserverpb.PutRequest{
		Key:    []byte("/registry/pods/a"),
		Value:  []byte("v2"),
		PrevKv: true,
	})
	require.NoError(t, err)
	require.NotNil(t, overwriteResp.PrevKv)
	require.Equal(t, []byte("v1"), overwriteResp.PrevKv.Value)

	rangeResp, err = server.Range(ctx, &etcdserverpb.RangeRequest{Key: []byte("/registry/pods/a")})
	require.NoError(t, err)
	require.Len(t, rangeResp.Kvs, 1)
	require.Equal(t, []byte("v2"), rangeResp.Kvs[0].Value)
	require.Greater(t, rangeResp.Kvs[0].ModRevision, firstRevision)
}

func TestPutIgnoreLeasePreservesExistingLease(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	ctx := context.Background()
	key := []byte("/registry/pods/ignore-lease")
	leaseResp, err := server.LeaseGrant(ctx, &etcdserverpb.LeaseGrantRequest{TTL: 30, ID: 24680})
	require.NoError(t, err)

	_, err = server.Put(ctx, &etcdserverpb.PutRequest{
		Key:   key,
		Value: []byte("leased"),
		Lease: leaseResp.ID,
	})
	require.NoError(t, err)

	_, err = server.Put(ctx, &etcdserverpb.PutRequest{
		Key:         key,
		Value:       []byte("updated"),
		IgnoreLease: true,
	})
	require.NoError(t, err)

	rangeResp, err := server.Range(ctx, &etcdserverpb.RangeRequest{Key: key})
	require.NoError(t, err)
	require.Len(t, rangeResp.Kvs, 1)
	require.Equal(t, []byte("updated"), rangeResp.Kvs[0].Value)

	ttlResp, err := server.LeaseTimeToLive(ctx, &etcdserverpb.LeaseTimeToLiveRequest{ID: leaseResp.ID, Keys: true})
	require.NoError(t, err)
	require.Equal(t, [][]byte{key}, ttlResp.Keys)
}

func TestPutIgnoreValueUpdatesLeasePreservesValue(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	ctx := context.Background()
	key := []byte("/registry/pods/ignore-value")
	leaseOne, err := server.LeaseGrant(ctx, &etcdserverpb.LeaseGrantRequest{TTL: 30, ID: 24682})
	require.NoError(t, err)
	leaseTwo, err := server.LeaseGrant(ctx, &etcdserverpb.LeaseGrantRequest{TTL: 30, ID: 24683})
	require.NoError(t, err)

	_, err = server.Put(ctx, &etcdserverpb.PutRequest{
		Key:   key,
		Value: []byte("original"),
		Lease: leaseOne.ID,
	})
	require.NoError(t, err)

	_, err = server.Put(ctx, &etcdserverpb.PutRequest{
		Key:         key,
		Lease:       leaseTwo.ID,
		IgnoreValue: true,
	})
	require.NoError(t, err)

	rangeResp, err := server.Range(ctx, &etcdserverpb.RangeRequest{Key: key})
	require.NoError(t, err)
	require.Len(t, rangeResp.Kvs, 1)
	require.Equal(t, []byte("original"), rangeResp.Kvs[0].Value)

	ttlOne, err := server.LeaseTimeToLive(ctx, &etcdserverpb.LeaseTimeToLiveRequest{ID: leaseOne.ID, Keys: true})
	require.NoError(t, err)
	require.Empty(t, ttlOne.Keys)
	ttlTwo, err := server.LeaseTimeToLive(ctx, &etcdserverpb.LeaseTimeToLiveRequest{ID: leaseTwo.ID, Keys: true})
	require.NoError(t, err)
	require.Equal(t, [][]byte{key}, ttlTwo.Keys)
}

func TestPutRejectsInvalidRequest(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	tests := []struct {
		name string
		req  *etcdserverpb.PutRequest
	}{
		{
			name: "empty key",
			req:  &etcdserverpb.PutRequest{Value: []byte("v1")},
		},
		{
			name: "ignore value with value",
			req:  &etcdserverpb.PutRequest{Key: []byte("/registry/pods/invalid-put"), Value: []byte("v1"), IgnoreValue: true},
		},
		{
			name: "ignore lease with lease",
			req:  &etcdserverpb.PutRequest{Key: []byte("/registry/pods/invalid-put"), Lease: 123, IgnoreLease: true},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := server.Put(context.Background(), tt.req)
			require.Error(t, err)
			require.Equal(t, codes.InvalidArgument, status.Code(err))
		})
	}
}

func TestRangeKeysOnlyOmitsValuesForGet(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	ctx := context.Background()
	key := []byte("/registry/pods/keys-only-get")
	_, err := server.Put(ctx, &etcdserverpb.PutRequest{
		Key:   key,
		Value: []byte("hidden"),
	})
	require.NoError(t, err)

	rangeResp, err := server.Range(ctx, &etcdserverpb.RangeRequest{
		Key:      key,
		KeysOnly: true,
	})
	require.NoError(t, err)
	require.Equal(t, int64(1), rangeResp.Count)
	require.Len(t, rangeResp.Kvs, 1)
	require.Equal(t, key, rangeResp.Kvs[0].Key)
	require.Empty(t, rangeResp.Kvs[0].Value)
	require.NotZero(t, rangeResp.Kvs[0].ModRevision)
}

func TestRangeKeysOnlyOmitsValuesForList(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	ctx := context.Background()
	prefix := "/registry/pods/keys-only-list/"
	for _, suffix := range []string{"a", "b"} {
		_, err := server.Put(ctx, &etcdserverpb.PutRequest{
			Key:   []byte(prefix + suffix),
			Value: []byte("hidden-" + suffix),
		})
		require.NoError(t, err)
	}

	var rangeResp *etcdserverpb.RangeResponse
	require.Eventually(t, func() bool {
		var err error
		rangeResp, err = server.Range(ctx, &etcdserverpb.RangeRequest{
			Key:      []byte(prefix),
			RangeEnd: []byte("/registry/pods/keys-only-list0"),
			KeysOnly: true,
		})
		return err == nil && len(rangeResp.Kvs) == 2
	}, time.Second, 10*time.Millisecond)
	require.Equal(t, int64(2), rangeResp.Count)
	require.Len(t, rangeResp.Kvs, 2)
	for _, kv := range rangeResp.Kvs {
		require.Contains(t, string(kv.Key), prefix)
		require.Empty(t, kv.Value)
		require.NotZero(t, kv.ModRevision)
	}
}

func TestRangeEmptyNonFromKeyRangeMatchesEtcd(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	ctx := context.Background()
	for _, key := range []string{
		"/registry/pods/empty-range/a",
		"/registry/pods/empty-range/b",
	} {
		_, err := server.Put(ctx, &etcdserverpb.PutRequest{
			Key:   []byte(key),
			Value: []byte("value"),
		})
		require.NoError(t, err)
	}

	for _, tc := range []struct {
		name     string
		key      []byte
		rangeEnd []byte
	}{
		{
			name:     "same start and end",
			key:      []byte("/registry/pods/empty-range/a"),
			rangeEnd: []byte("/registry/pods/empty-range/a"),
		},
		{
			name:     "start after end",
			key:      []byte("/registry/pods/empty-range/b"),
			rangeEnd: []byte("/registry/pods/empty-range/a"),
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			resp, err := server.Range(ctx, &etcdserverpb.RangeRequest{
				Key:      tc.key,
				RangeEnd: tc.rangeEnd,
			})
			require.NoError(t, err)
			require.Equal(t, int64(0), resp.Count)
			require.Empty(t, resp.Kvs)
			require.False(t, resp.More)
			require.NotNil(t, resp.Header)
		})
	}
}

func TestRangeSortsByKeyDescending(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	ctx := context.Background()
	prefix := "/registry/pods/sort-key/"
	for _, suffix := range []string{"a", "b", "c"} {
		_, err := server.Put(ctx, &etcdserverpb.PutRequest{
			Key:   []byte(prefix + suffix),
			Value: []byte(suffix),
		})
		require.NoError(t, err)
	}

	var rangeResp *etcdserverpb.RangeResponse
	require.Eventually(t, func() bool {
		var err error
		rangeResp, err = server.Range(ctx, &etcdserverpb.RangeRequest{
			Key:        []byte(prefix),
			RangeEnd:   []byte("/registry/pods/sort-key0"),
			SortTarget: etcdserverpb.RangeRequest_KEY,
			SortOrder:  etcdserverpb.RangeRequest_DESCEND,
		})
		return err == nil && len(rangeResp.Kvs) == 3
	}, time.Second, 10*time.Millisecond)
	require.Equal(t, [][]byte{
		[]byte(prefix + "c"),
		[]byte(prefix + "b"),
		[]byte(prefix + "a"),
	}, [][]byte{
		rangeResp.Kvs[0].Key,
		rangeResp.Kvs[1].Key,
		rangeResp.Kvs[2].Key,
	})
}

func TestRangeSortsByValueAscending(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	ctx := context.Background()
	valuesByKey := map[string]string{
		"/registry/pods/sort-value/a": "3",
		"/registry/pods/sort-value/b": "1",
		"/registry/pods/sort-value/c": "2",
	}
	for key, value := range valuesByKey {
		_, err := server.Put(ctx, &etcdserverpb.PutRequest{
			Key:   []byte(key),
			Value: []byte(value),
		})
		require.NoError(t, err)
	}

	var rangeResp *etcdserverpb.RangeResponse
	require.Eventually(t, func() bool {
		var err error
		rangeResp, err = server.Range(ctx, &etcdserverpb.RangeRequest{
			Key:        []byte("/registry/pods/sort-value/"),
			RangeEnd:   []byte("/registry/pods/sort-value0"),
			SortTarget: etcdserverpb.RangeRequest_VALUE,
		})
		return err == nil && len(rangeResp.Kvs) == 3
	}, time.Second, 10*time.Millisecond)
	require.Equal(t, [][]byte{[]byte("1"), []byte("2"), []byte("3")}, [][]byte{
		rangeResp.Kvs[0].Value,
		rangeResp.Kvs[1].Value,
		rangeResp.Kvs[2].Value,
	})
}

func TestRangeRejectsInvalidSortOptions(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	_, err := server.Range(context.Background(), &etcdserverpb.RangeRequest{
		Key:       []byte("/registry/pods/invalid-sort"),
		SortOrder: etcdserverpb.RangeRequest_SortOrder(99),
	})
	require.Error(t, err)
	require.Equal(t, codes.InvalidArgument, status.Code(err))

	_, err = server.Range(context.Background(), &etcdserverpb.RangeRequest{
		Key:        []byte("/registry/pods/invalid-sort"),
		SortTarget: etcdserverpb.RangeRequest_SortTarget(99),
	})
	require.Error(t, err)
	require.Equal(t, codes.InvalidArgument, status.Code(err))
}

func TestRangeFiltersByModRevision(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	ctx := context.Background()
	prefix := "/registry/pods/filter-mod/"
	putA, err := server.Put(ctx, &etcdserverpb.PutRequest{
		Key:   []byte(prefix + "a"),
		Value: []byte("old"),
	})
	require.NoError(t, err)
	putB, err := server.Put(ctx, &etcdserverpb.PutRequest{
		Key:   []byte(prefix + "b"),
		Value: []byte("new"),
	})
	require.NoError(t, err)

	var rangeResp *etcdserverpb.RangeResponse
	require.Eventually(t, func() bool {
		var err error
		rangeResp, err = server.Range(ctx, &etcdserverpb.RangeRequest{
			Key:            []byte(prefix),
			RangeEnd:       []byte("/registry/pods/filter-mod0"),
			MinModRevision: putB.Header.Revision,
		})
		return err == nil && len(rangeResp.Kvs) == 1
	}, time.Second, 10*time.Millisecond)
	require.Equal(t, int64(2), rangeResp.Count)
	require.Equal(t, []byte(prefix+"b"), rangeResp.Kvs[0].Key)

	countResp, err := server.Range(ctx, &etcdserverpb.RangeRequest{
		Key:            []byte(prefix),
		RangeEnd:       []byte("/registry/pods/filter-mod0"),
		MaxModRevision: putA.Header.Revision,
		CountOnly:      true,
	})
	require.NoError(t, err)
	require.Equal(t, int64(2), countResp.Count)
	require.Empty(t, countResp.Kvs)
}

func TestRangeAppliesLimitAfterModRevisionFilter(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	ctx := context.Background()
	prefix := "/registry/pods/filter-limit/"
	putA, err := server.Put(ctx, &etcdserverpb.PutRequest{
		Key:   []byte(prefix + "a"),
		Value: []byte("old"),
	})
	require.NoError(t, err)
	_, err = server.Put(ctx, &etcdserverpb.PutRequest{
		Key:   []byte(prefix + "b"),
		Value: []byte("newer-1"),
	})
	require.NoError(t, err)
	_, err = server.Put(ctx, &etcdserverpb.PutRequest{
		Key:   []byte(prefix + "c"),
		Value: []byte("newer-2"),
	})
	require.NoError(t, err)

	var rangeResp *etcdserverpb.RangeResponse
	require.Eventually(t, func() bool {
		var err error
		rangeResp, err = server.Range(ctx, &etcdserverpb.RangeRequest{
			Key:            []byte(prefix),
			RangeEnd:       []byte("/registry/pods/filter-limit0"),
			MinModRevision: putA.Header.Revision + 1,
			Limit:          1,
		})
		return err == nil && rangeResp.Count == 3 && len(rangeResp.Kvs) == 1
	}, time.Second, 10*time.Millisecond)
	require.True(t, rangeResp.More)
	require.Equal(t, []byte(prefix+"b"), rangeResp.Kvs[0].Key)
}

func TestRangeLimitCountReportsTotalMatches(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	ctx := context.Background()
	prefix := "/registry/pods/limit-count/"
	for _, name := range []string{"a", "b", "c", "d", "e"} {
		_, err := server.Put(ctx, &etcdserverpb.PutRequest{
			Key:   []byte(prefix + name),
			Value: []byte(name),
		})
		require.NoError(t, err)
	}

	var rangeResp *etcdserverpb.RangeResponse
	require.Eventually(t, func() bool {
		var err error
		rangeResp, err = server.Range(ctx, &etcdserverpb.RangeRequest{
			Key:      []byte(prefix),
			RangeEnd: []byte("/registry/pods/limit-count0"),
			Limit:    2,
		})
		return err == nil && rangeResp.Count == 5 && len(rangeResp.Kvs) == 2
	}, time.Second, 10*time.Millisecond)
	require.True(t, rangeResp.More)
	require.Equal(t, []byte(prefix+"a"), rangeResp.Kvs[0].Key)
	require.Equal(t, []byte(prefix+"b"), rangeResp.Kvs[1].Key)

	var continueResp *etcdserverpb.RangeResponse
	require.Eventually(t, func() bool {
		var err error
		continueResp, err = server.Range(ctx, &etcdserverpb.RangeRequest{
			Key:      append([]byte(prefix+"b"), 0),
			RangeEnd: []byte("/registry/pods/limit-count0"),
			Limit:    2,
		})
		return err == nil && continueResp.Count == 3 && len(continueResp.Kvs) == 2
	}, time.Second, 10*time.Millisecond)
	require.True(t, continueResp.More)
	require.Equal(t, []byte(prefix+"c"), continueResp.Kvs[0].Key)
	require.Equal(t, []byte(prefix+"d"), continueResp.Kvs[1].Key)
}

func TestRangeCountOnlyLimitWithModRevisionFilterDoesNotTruncateCount(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	ctx := context.Background()
	prefix := "/registry/pods/filter-count-limit/"
	first, err := server.Put(ctx, &etcdserverpb.PutRequest{
		Key:   []byte(prefix + "a"),
		Value: []byte("a"),
	})
	require.NoError(t, err)
	_, err = server.Put(ctx, &etcdserverpb.PutRequest{
		Key:   []byte(prefix + "b"),
		Value: []byte("b"),
	})
	require.NoError(t, err)
	_, err = server.Put(ctx, &etcdserverpb.PutRequest{
		Key:   []byte(prefix + "c"),
		Value: []byte("c"),
	})
	require.NoError(t, err)

	var rangeResp *etcdserverpb.RangeResponse
	require.Eventually(t, func() bool {
		var err error
		rangeResp, err = server.Range(ctx, &etcdserverpb.RangeRequest{
			Key:            []byte(prefix),
			RangeEnd:       []byte("/registry/pods/filter-count-limit0"),
			MinModRevision: first.Header.Revision,
			Limit:          1,
			CountOnly:      true,
		})
		return err == nil && rangeResp.Count == 3
	}, time.Second, 10*time.Millisecond)
	require.Empty(t, rangeResp.Kvs)
	require.False(t, rangeResp.More)
}

func TestRangeCreateRevisionFilter(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	ctx := context.Background()
	prefix := "/registry/pods/create-filter/"
	first, err := server.Put(ctx, &etcdserverpb.PutRequest{
		Key:   []byte(prefix + "a"),
		Value: []byte("a"),
	})
	require.NoError(t, err)
	second, err := server.Put(ctx, &etcdserverpb.PutRequest{
		Key:   []byte(prefix + "b"),
		Value: []byte("b"),
	})
	require.NoError(t, err)
	_, err = server.Put(ctx, &etcdserverpb.PutRequest{
		Key:   []byte(prefix + "c"),
		Value: []byte("c"),
	})
	require.NoError(t, err)

	var rangeResp *etcdserverpb.RangeResponse
	require.Eventually(t, func() bool {
		var err error
		rangeResp, err = server.Range(ctx, &etcdserverpb.RangeRequest{
			Key:               []byte(prefix),
			RangeEnd:          []byte("/registry/pods/create-filter0"),
			MinCreateRevision: second.Header.Revision,
			Limit:             1,
		})
		return err == nil && rangeResp.Count == 3 && len(rangeResp.Kvs) == 1
	}, time.Second, 10*time.Millisecond)
	require.True(t, rangeResp.More)
	require.Equal(t, []byte(prefix+"b"), rangeResp.Kvs[0].Key)

	rangeResp, err = server.Range(ctx, &etcdserverpb.RangeRequest{
		Key:               []byte(prefix),
		RangeEnd:          []byte("/registry/pods/create-filter0"),
		MinCreateRevision: first.Header.Revision,
		MaxCreateRevision: second.Header.Revision,
	})
	require.NoError(t, err)
	require.Equal(t, int64(3), rangeResp.Count)
	require.Len(t, rangeResp.Kvs, 2)
}

func TestRangeCreateAndVersionSortTargetsAreSupported(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	ctx := context.Background()
	prefix := "/registry/pods/sort-target/"
	_, err := server.Put(ctx, &etcdserverpb.PutRequest{Key: []byte(prefix + "b"), Value: []byte("b")})
	require.NoError(t, err)
	_, err = server.Put(ctx, &etcdserverpb.PutRequest{Key: []byte(prefix + "a"), Value: []byte("a")})
	require.NoError(t, err)

	var createResp *etcdserverpb.RangeResponse
	require.Eventually(t, func() bool {
		var err error
		createResp, err = server.Range(ctx, &etcdserverpb.RangeRequest{
			Key:        []byte(prefix),
			RangeEnd:   []byte("/registry/pods/sort-target0"),
			SortTarget: etcdserverpb.RangeRequest_CREATE,
			SortOrder:  etcdserverpb.RangeRequest_ASCEND,
		})
		return err == nil && len(createResp.Kvs) == 2
	}, time.Second, 10*time.Millisecond)
	require.Equal(t, []byte(prefix+"b"), createResp.Kvs[0].Key)
	require.Equal(t, []byte(prefix+"a"), createResp.Kvs[1].Key)
	require.Less(t, createResp.Kvs[0].CreateRevision, createResp.Kvs[1].CreateRevision)

	versionResp, err := server.Range(ctx, &etcdserverpb.RangeRequest{
		Key:        []byte(prefix),
		RangeEnd:   []byte("/registry/pods/sort-target0"),
		SortTarget: etcdserverpb.RangeRequest_VERSION,
		SortOrder:  etcdserverpb.RangeRequest_ASCEND,
	})
	require.NoError(t, err)
	require.Len(t, versionResp.Kvs, 2)
	require.LessOrEqual(t, versionResp.Kvs[0].Version, versionResp.Kvs[1].Version)
}

func TestRangeFutureRevisionMatchesEtcd(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	ctx := context.Background()
	putResp, err := server.Put(ctx, &etcdserverpb.PutRequest{
		Key:   []byte("/registry/pods/future-rev/a"),
		Value: []byte("a"),
	})
	require.NoError(t, err)

	tests := []struct {
		name string
		req  *etcdserverpb.RangeRequest
	}{
		{
			name: "get",
			req: &etcdserverpb.RangeRequest{
				Key:      []byte("/registry/pods/future-rev/a"),
				Revision: putResp.Header.Revision + 1,
			},
		},
		{
			name: "list",
			req: &etcdserverpb.RangeRequest{
				Key:      []byte("/registry/pods/future-rev/"),
				RangeEnd: []byte("/registry/pods/future-rev0"),
				Revision: putResp.Header.Revision + 1,
			},
		},
		{
			name: "count",
			req: &etcdserverpb.RangeRequest{
				Key:       []byte("/registry/pods/future-rev/"),
				RangeEnd:  []byte("/registry/pods/future-rev0"),
				Revision:  putResp.Header.Revision + 1,
				CountOnly: true,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := server.Range(ctx, tt.req)
			require.Error(t, err)
			require.Equal(t, codes.OutOfRange, status.Code(err))
			require.Contains(t, err.Error(), "etcdserver: mvcc: required revision is a future revision")
		})
	}
}

func TestFollowerHistoricalRangeProxiesToLeader(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	metrics := mock.NewMinimalMetrics(ctrl)
	kv := memkv.NewKvStorage()
	defer func() {
		require.NoError(t, kv.Close())
	}()
	backendStore := backend.NewBackend(kv, backend.Config{
		Identity:                "test-peer",
		EnableEtcdCompatibility: true,
	}, metrics)

	called := false
	server := New(backendStore, metrics, testPeerService{
		isLeader:     false,
		proxyEnabled: true,
		rangeFn: func(_ context.Context, req *etcdserverpb.RangeRequest) (*etcdserverpb.RangeResponse, error) {
			called = true
			require.Equal(t, []byte("/registry/pods/historical"), req.Key)
			require.Equal(t, int64(10), req.Revision)
			return &etcdserverpb.RangeResponse{
				Header: &etcdserverpb.ResponseHeader{Revision: 20},
				Kvs: []*mvccpb.KeyValue{{
					Key:         req.Key,
					Value:       []byte("leader"),
					ModRevision: 10,
				}},
				Count: 1,
			}, nil
		},
	})
	defer server.stopLeases()

	resp, err := server.Range(context.Background(), &etcdserverpb.RangeRequest{
		Key:      []byte("/registry/pods/historical"),
		Revision: 10,
	})
	require.NoError(t, err)
	require.True(t, called)
	require.Equal(t, int64(1), resp.Count)
	require.Equal(t, []byte("leader"), resp.Kvs[0].Value)
}

func TestRangeCompactedRevisionMatchesEtcd(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	ctx := context.Background()
	putResp, err := server.Put(ctx, &etcdserverpb.PutRequest{
		Key:   []byte("/registry/pods/compacted"),
		Value: []byte("v1"),
	})
	require.NoError(t, err)
	putResp, err = server.Put(ctx, &etcdserverpb.PutRequest{
		Key:   []byte("/registry/pods/compacted"),
		Value: []byte("v2"),
	})
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		return server.backend.GetCurrentRevision() >= uint64(putResp.Header.Revision)
	}, time.Second, 10*time.Millisecond)
	compactRev := putResp.Header.Revision

	_, err = server.Compact(ctx, &etcdserverpb.CompactionRequest{Revision: compactRev})
	require.NoError(t, err)

	_, err = server.Range(ctx, &etcdserverpb.RangeRequest{
		Key:      []byte("/registry/pods/compacted"),
		Revision: compactRev - 1,
	})
	require.Error(t, err)
	require.Equal(t, codes.OutOfRange, status.Code(err))
	require.Contains(t, err.Error(), "required revision has been compacted")

	resp, err := server.Range(ctx, &etcdserverpb.RangeRequest{
		Key:      []byte("/registry/pods/compacted"),
		Revision: compactRev,
	})
	require.NoError(t, err)
	require.Len(t, resp.Kvs, 1)
}

func TestCompactFutureAndRepeatedRevisionMatchEtcd(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	ctx := context.Background()
	putResp, err := server.Put(ctx, &etcdserverpb.PutRequest{
		Key:   []byte("/registry/pods/compact-errors"),
		Value: []byte("v1"),
	})
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		return server.backend.GetCurrentRevision() >= uint64(putResp.Header.Revision)
	}, time.Second, 10*time.Millisecond)
	compactRev := putResp.Header.Revision

	_, err = server.Compact(ctx, &etcdserverpb.CompactionRequest{Revision: compactRev + 1})
	require.Error(t, err)
	require.Equal(t, codes.OutOfRange, status.Code(err))
	require.Contains(t, err.Error(), "required revision is a future revision")

	_, err = server.Compact(ctx, &etcdserverpb.CompactionRequest{Revision: compactRev})
	require.NoError(t, err)

	_, err = server.Compact(ctx, &etcdserverpb.CompactionRequest{Revision: compactRev})
	require.Error(t, err)
	require.Equal(t, codes.OutOfRange, status.Code(err))
	require.Contains(t, err.Error(), "required revision has been compacted")
}

func TestCompactOlderRevisionReturnsCurrentHeader(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	ctx := context.Background()
	first, err := server.Put(ctx, &etcdserverpb.PutRequest{
		Key:   []byte("/registry/pods/compact-header"),
		Value: []byte("v1"),
	})
	require.NoError(t, err)
	second, err := server.Put(ctx, &etcdserverpb.PutRequest{
		Key:   []byte("/registry/pods/compact-header"),
		Value: []byte("v2"),
	})
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		return server.backend.GetCurrentRevision() >= uint64(second.Header.Revision)
	}, time.Second, 10*time.Millisecond)

	resp, err := server.Compact(ctx, &etcdserverpb.CompactionRequest{Revision: first.Header.Revision})
	require.NoError(t, err)
	require.Equal(t, second.Header.Revision, resp.Header.Revision)
}

type compactLagShim struct {
	BackendShim
	currentRevision uint64
	actualCompact   uint64
}

func (s compactLagShim) GetCurrentRevision() uint64 {
	return s.currentRevision
}

func (s compactLagShim) GetCompactRevision(context.Context) (uint64, error) {
	return 0, nil
}

func (s compactLagShim) Compact(context.Context, uint64) (*etcdserverpb.TxnResponse, error) {
	return &etcdserverpb.TxnResponse{
		Header: &etcdserverpb.ResponseHeader{Revision: int64(s.actualCompact)},
	}, nil
}

func TestCompactReturnsRetryableErrorWhenBackendCompactsBelowRequestedRevision(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()
	server.backend = compactLagShim{
		BackendShim:     server.backend,
		currentRevision: 20,
		actualCompact:   9,
	}

	// Physical=true routes to the synchronous Compact that compactLagShim
	// overrides to report a below-requested compacted revision.
	_, err := server.Compact(context.Background(), &etcdserverpb.CompactionRequest{Revision: 10, Physical: true})
	require.Error(t, err)
	require.Equal(t, codes.Unavailable, status.Code(err))
	require.Contains(t, err.Error(), "pending behind requested revision")
}

func TestFollowerCompactProxiesToLeader(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	metrics := mock.NewMinimalMetrics(ctrl)
	kv := memkv.NewKvStorage()
	defer func() {
		require.NoError(t, kv.Close())
	}()
	backendStore := backend.NewBackend(kv, backend.Config{
		Identity:                "test-peer",
		EnableEtcdCompatibility: true,
	}, metrics)

	called := false
	server := New(backendStore, metrics, testPeerService{
		isLeader:     false,
		proxyEnabled: true,
		compactFn: func(_ context.Context, req *etcdserverpb.CompactionRequest) (*etcdserverpb.CompactionResponse, error) {
			called = true
			require.Equal(t, int64(123), req.Revision)
			return &etcdserverpb.CompactionResponse{Header: &etcdserverpb.ResponseHeader{Revision: 456}}, nil
		},
	})

	resp, err := server.Compact(context.Background(), &etcdserverpb.CompactionRequest{Revision: 123})
	defer server.stopLeases()
	require.NoError(t, err)
	require.True(t, called)
	require.Equal(t, int64(456), resp.Header.Revision)
}

func TestRangeWithFromKeyMatchesEtcd(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	ctx := context.Background()
	for _, key := range []string{
		"/registry/from-key/a",
		"/registry/from-key/b",
		"/registry/from-key/c",
	} {
		_, err := server.Put(ctx, &etcdserverpb.PutRequest{Key: []byte(key), Value: []byte(key)})
		require.NoError(t, err)
	}

	require.Eventually(t, func() bool {
		resp, err := server.Range(ctx, &etcdserverpb.RangeRequest{
			Key:      []byte("/registry/from-key/b"),
			RangeEnd: []byte{0},
		})
		return err == nil &&
			len(resp.Kvs) >= 2 &&
			bytes.Equal(resp.Kvs[0].Key, []byte("/registry/from-key/b")) &&
			bytes.Equal(resp.Kvs[1].Key, []byte("/registry/from-key/c"))
	}, time.Second, 10*time.Millisecond)

	countResp, err := server.Range(ctx, &etcdserverpb.RangeRequest{
		Key:       []byte("/registry/from-key/b"),
		RangeEnd:  []byte{0},
		CountOnly: true,
	})
	require.NoError(t, err)
	require.GreaterOrEqual(t, countResp.Count, int64(2))
	require.Empty(t, countResp.Kvs)
}

func TestDeleteRangeWithFromKeyMatchesEtcd(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	ctx := context.Background()
	for _, key := range []string{
		"/registry/from-key-delete/a",
		"/registry/from-key-delete/b",
		"/registry/from-key-delete/c",
	} {
		_, err := server.Put(ctx, &etcdserverpb.PutRequest{Key: []byte(key), Value: []byte(key)})
		require.NoError(t, err)
	}

	require.Eventually(t, func() bool {
		resp, err := server.Range(ctx, &etcdserverpb.RangeRequest{
			Key:      []byte("/registry/from-key-delete/b"),
			RangeEnd: []byte{0},
		})
		return err == nil &&
			len(resp.Kvs) >= 2 &&
			bytes.Equal(resp.Kvs[0].Key, []byte("/registry/from-key-delete/b")) &&
			bytes.Equal(resp.Kvs[1].Key, []byte("/registry/from-key-delete/c"))
	}, time.Second, 10*time.Millisecond)

	resp, err := server.DeleteRange(ctx, &etcdserverpb.DeleteRangeRequest{
		Key:      []byte("/registry/from-key-delete/b"),
		RangeEnd: []byte{0},
		PrevKv:   true,
	})
	require.NoError(t, err)
	require.GreaterOrEqual(t, resp.Deleted, int64(2))
	require.GreaterOrEqual(t, len(resp.PrevKvs), 2)

	require.Eventually(t, func() bool {
		remaining, err := server.Range(ctx, &etcdserverpb.RangeRequest{
			Key:      []byte("/registry/from-key-delete/"),
			RangeEnd: []byte("/registry/from-key-delete0"),
		})
		return err == nil &&
			len(remaining.Kvs) == 1 &&
			bytes.Equal(remaining.Kvs[0].Key, []byte("/registry/from-key-delete/a"))
	}, time.Second, 10*time.Millisecond)
}

func TestDeleteRangeDeletesSingleKey(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	ctx := context.Background()
	_, err := server.Put(ctx, &etcdserverpb.PutRequest{
		Key:   []byte("/registry/services/a"),
		Value: []byte("svc"),
	})
	require.NoError(t, err)

	deleteResp, err := server.DeleteRange(ctx, &etcdserverpb.DeleteRangeRequest{
		Key:    []byte("/registry/services/a"),
		PrevKv: true,
	})
	require.NoError(t, err)
	require.Equal(t, int64(1), deleteResp.Deleted)
	require.Len(t, deleteResp.PrevKvs, 1)
	require.Equal(t, []byte("svc"), deleteResp.PrevKvs[0].Value)

	rangeResp, err := server.Range(ctx, &etcdserverpb.RangeRequest{Key: []byte("/registry/services/a")})
	require.NoError(t, err)
	require.Empty(t, rangeResp.Kvs)
}

func TestDeleteRangeMissingPointDoesNotConsumeRevision(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	ctx := context.Background()
	before := int64(server.backend.GetCurrentRevision())
	for i := 0; i < 3; i++ {
		resp, err := server.DeleteRange(ctx, &etcdserverpb.DeleteRangeRequest{
			Key:    []byte("/registry/delete-missing-point"),
			PrevKv: true,
		})
		require.NoError(t, err)
		require.Equal(t, int64(0), resp.Deleted)
		require.Empty(t, resp.PrevKvs)
		require.Equal(t, before, resp.Header.Revision)
		require.Equal(t, uint64(before), server.backend.GetCurrentRevision())
	}
}

func TestDeleteRangeRejectsEmptyKey(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	_, err := server.DeleteRange(context.Background(), &etcdserverpb.DeleteRangeRequest{})
	require.Error(t, err)
	require.Equal(t, codes.InvalidArgument, status.Code(err))
}

func TestDeleteRangeDeletesRange(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	ctx := context.Background()
	for _, key := range []string{"/registry/configmaps/a", "/registry/configmaps/b", "/registry/secrets/a"} {
		_, err := server.Put(ctx, &etcdserverpb.PutRequest{
			Key:   []byte(key),
			Value: []byte(key),
		})
		require.NoError(t, err)
	}
	require.Eventually(t, func() bool {
		rangeResp, err := server.Range(ctx, &etcdserverpb.RangeRequest{
			Key:      []byte("/registry/configmaps/"),
			RangeEnd: []byte("/registry/configmaps0"),
		})
		return err == nil && len(rangeResp.Kvs) == 2
	}, time.Second, 10*time.Millisecond)

	deleteResp, err := server.DeleteRange(ctx, &etcdserverpb.DeleteRangeRequest{
		Key:      []byte("/registry/configmaps/"),
		RangeEnd: []byte("/registry/configmaps0"),
		PrevKv:   true,
	})
	require.NoError(t, err)
	require.Equal(t, int64(2), deleteResp.Deleted)
	require.Len(t, deleteResp.PrevKvs, 2)
	for _, prevKV := range deleteResp.PrevKvs {
		require.Greater(t, deleteResp.Header.Revision, prevKV.ModRevision)
	}
	require.Eventually(t, func() bool {
		return server.backend.GetCurrentRevision() >= uint64(deleteResp.Header.Revision)
	}, time.Second, 10*time.Millisecond)
	watchCtx, watchCancel := context.WithCancel(ctx)
	defer watchCancel()
	watchCh, err := server.backend.Watch(watchCtx, "/registry/configmaps/", uint64(deleteResp.Header.Revision))
	require.NoError(t, err)
	deleteEvents := (<-watchCh).Events
	require.Len(t, deleteEvents, 2)
	for _, event := range deleteEvents {
		require.Equal(t, mvccpb.DELETE, event.Type)
		require.Equal(t, deleteResp.Header.Revision, event.Kv.ModRevision)
	}

	require.Eventually(t, func() bool {
		rangeResp, err := server.Range(ctx, &etcdserverpb.RangeRequest{
			Key:      []byte("/registry/configmaps/"),
			RangeEnd: []byte("/registry/configmaps0"),
		})
		return err == nil && len(rangeResp.Kvs) == 0
	}, time.Second, 10*time.Millisecond)

	secretResp, err := server.Range(ctx, &etcdserverpb.RangeRequest{Key: []byte("/registry/secrets/a")})
	require.NoError(t, err)
	require.Len(t, secretResp.Kvs, 1)
}

func TestDeleteRangeEmptyNonFromKeyRangeDoesNotDelete(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	ctx := context.Background()
	key := []byte("/registry/configmaps/empty-delete/a")
	_, err := server.Put(ctx, &etcdserverpb.PutRequest{
		Key:   key,
		Value: []byte("value"),
	})
	require.NoError(t, err)

	deleteResp, err := server.DeleteRange(ctx, &etcdserverpb.DeleteRangeRequest{
		Key:      key,
		RangeEnd: key,
		PrevKv:   true,
	})
	require.NoError(t, err)
	require.Equal(t, int64(0), deleteResp.Deleted)
	require.Empty(t, deleteResp.PrevKvs)
	require.NotNil(t, deleteResp.Header)

	getResp, err := server.Range(ctx, &etcdserverpb.RangeRequest{Key: key})
	require.NoError(t, err)
	require.Equal(t, int64(1), getResp.Count)
	require.Equal(t, []byte("value"), getResp.Kvs[0].Value)
}

func TestTxnDeleteRangeEmptyNonFromKeyRangeDoesNotDelete(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	ctx := context.Background()
	key := []byte("/registry/generic-txn/empty-delete/a")
	_, err := server.Put(ctx, &etcdserverpb.PutRequest{
		Key:   key,
		Value: []byte("value"),
	})
	require.NoError(t, err)

	txnResp, err := server.Txn(ctx, &etcdserverpb.TxnRequest{
		Success: []*etcdserverpb.RequestOp{
			{Request: &etcdserverpb.RequestOp_RequestDeleteRange{
				RequestDeleteRange: &etcdserverpb.DeleteRangeRequest{
					Key:      key,
					RangeEnd: key,
					PrevKv:   true,
				},
			}},
		},
	})
	require.NoError(t, err)
	require.True(t, txnResp.Succeeded)
	require.Len(t, txnResp.Responses, 1)
	deleteResp := txnResp.Responses[0].GetResponseDeleteRange()
	require.NotNil(t, deleteResp)
	require.Equal(t, int64(0), deleteResp.Deleted)
	require.Empty(t, deleteResp.PrevKvs)

	getResp, err := server.Range(ctx, &etcdserverpb.RangeRequest{Key: key})
	require.NoError(t, err)
	require.Equal(t, int64(1), getResp.Count)
	require.Equal(t, []byte("value"), getResp.Kvs[0].Value)
}

func TestTxnCompareDeleteRangeEmptyNonFromKeyRangeUsesGenericPath(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	ctx := context.Background()
	key := []byte("/registry/generic-txn/compare-empty-delete/a")
	putResp, err := server.Put(ctx, &etcdserverpb.PutRequest{
		Key:   key,
		Value: []byte("value"),
	})
	require.NoError(t, err)

	txnResp, err := server.Txn(ctx, &etcdserverpb.TxnRequest{
		Compare: []*etcdserverpb.Compare{
			{
				Key:    key,
				Target: etcdserverpb.Compare_MOD,
				Result: etcdserverpb.Compare_EQUAL,
				TargetUnion: &etcdserverpb.Compare_ModRevision{
					ModRevision: putResp.Header.Revision,
				},
			},
		},
		Success: []*etcdserverpb.RequestOp{
			{Request: &etcdserverpb.RequestOp_RequestDeleteRange{
				RequestDeleteRange: &etcdserverpb.DeleteRangeRequest{
					Key:      key,
					RangeEnd: key,
					PrevKv:   true,
				},
			}},
		},
	})
	require.NoError(t, err)
	require.True(t, txnResp.Succeeded)
	require.Len(t, txnResp.Responses, 1)
	deleteResp := txnResp.Responses[0].GetResponseDeleteRange()
	require.NotNil(t, deleteResp)
	require.Equal(t, int64(0), deleteResp.Deleted)
	require.Empty(t, deleteResp.PrevKvs)

	getResp, err := server.Range(ctx, &etcdserverpb.RangeRequest{Key: key})
	require.NoError(t, err)
	require.Equal(t, int64(1), getResp.Count)
	require.Equal(t, []byte("value"), getResp.Kvs[0].Value)
}

func TestFollowerPutAndDeleteRangeProxyToLeader(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	metrics := mock.NewMinimalMetrics(ctrl)
	kv := memkv.NewKvStorage()
	defer func() {
		require.NoError(t, kv.Close())
	}()
	b := backend.NewBackend(kv, backend.Config{
		Identity:                "follower",
		EnableEtcdCompatibility: true,
	}, metrics)

	var putForwarded, deleteForwarded bool
	server := New(b, metrics, testPeerService{
		isLeader:     false,
		proxyEnabled: true,
		putFn: func(_ context.Context, req *etcdserverpb.PutRequest) (*etcdserverpb.PutResponse, error) {
			putForwarded = true
			require.Equal(t, []byte("/registry/follower-put"), req.Key)
			return &etcdserverpb.PutResponse{Header: &etcdserverpb.ResponseHeader{Revision: 10}}, nil
		},
		deleteRangeFn: func(_ context.Context, req *etcdserverpb.DeleteRangeRequest) (*etcdserverpb.DeleteRangeResponse, error) {
			deleteForwarded = true
			require.Equal(t, []byte("/registry/follower-put"), req.Key)
			return &etcdserverpb.DeleteRangeResponse{Header: &etcdserverpb.ResponseHeader{Revision: 11}, Deleted: 1}, nil
		},
	})
	defer server.stopLeases()

	putResp, err := server.Put(context.Background(), &etcdserverpb.PutRequest{Key: []byte("/registry/follower-put"), Value: []byte("v")})
	require.NoError(t, err)
	require.Equal(t, int64(10), putResp.Header.Revision)
	require.True(t, putForwarded)

	deleteResp, err := server.DeleteRange(context.Background(), &etcdserverpb.DeleteRangeRequest{Key: []byte("/registry/follower-put")})
	require.NoError(t, err)
	require.Equal(t, int64(1), deleteResp.Deleted)
	require.True(t, deleteForwarded)
}

func TestTxnCreateUpdateDeletePath(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	ctx := context.Background()
	key := []byte("/registry/deployments/a")

	createResp, err := server.Txn(ctx, &etcdserverpb.TxnRequest{
		Compare: []*etcdserverpb.Compare{{
			Key:         key,
			Target:      etcdserverpb.Compare_MOD,
			Result:      etcdserverpb.Compare_EQUAL,
			TargetUnion: &etcdserverpb.Compare_ModRevision{ModRevision: 0},
		}},
		Success: []*etcdserverpb.RequestOp{{
			Request: &etcdserverpb.RequestOp_RequestPut{
				RequestPut: &etcdserverpb.PutRequest{Key: key, Value: []byte("v1")},
			},
		}},
	})
	require.NoError(t, err)
	require.True(t, createResp.Succeeded)

	getResp, err := server.Range(ctx, &etcdserverpb.RangeRequest{Key: key})
	require.NoError(t, err)
	require.Len(t, getResp.Kvs, 1)

	updateResp, err := server.Txn(ctx, &etcdserverpb.TxnRequest{
		Compare: []*etcdserverpb.Compare{{
			Key:         key,
			Target:      etcdserverpb.Compare_MOD,
			Result:      etcdserverpb.Compare_EQUAL,
			TargetUnion: &etcdserverpb.Compare_ModRevision{ModRevision: getResp.Kvs[0].ModRevision},
		}},
		Success: []*etcdserverpb.RequestOp{{
			Request: &etcdserverpb.RequestOp_RequestPut{
				RequestPut: &etcdserverpb.PutRequest{Key: key, Value: []byte("v2")},
			},
		}},
		Failure: []*etcdserverpb.RequestOp{{
			Request: &etcdserverpb.RequestOp_RequestRange{
				RequestRange: &etcdserverpb.RangeRequest{Key: key},
			},
		}},
	})
	require.NoError(t, err)
	require.True(t, updateResp.Succeeded)

	deleteResp, err := server.Txn(ctx, &etcdserverpb.TxnRequest{
		Success: []*etcdserverpb.RequestOp{
			{Request: &etcdserverpb.RequestOp_RequestRange{RequestRange: &etcdserverpb.RangeRequest{Key: key}}},
			{Request: &etcdserverpb.RequestOp_RequestDeleteRange{RequestDeleteRange: &etcdserverpb.DeleteRangeRequest{Key: key}}},
		},
	})
	require.NoError(t, err)
	require.True(t, deleteResp.Succeeded)
}

func TestTxnCreateWithFailureRange(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	ctx := context.Background()
	key := []byte("/registry/generic-txn/create")
	_, err := server.Put(ctx, &etcdserverpb.PutRequest{Key: key, Value: []byte("exists")})
	require.NoError(t, err)

	resp, err := server.Txn(ctx, &etcdserverpb.TxnRequest{
		Compare: []*etcdserverpb.Compare{{
			Key:         key,
			Target:      etcdserverpb.Compare_MOD,
			Result:      etcdserverpb.Compare_EQUAL,
			TargetUnion: &etcdserverpb.Compare_ModRevision{ModRevision: 0},
		}},
		Success: []*etcdserverpb.RequestOp{{
			Request: &etcdserverpb.RequestOp_RequestPut{
				RequestPut: &etcdserverpb.PutRequest{Key: key, Value: []byte("created")},
			},
		}},
		Failure: []*etcdserverpb.RequestOp{{
			Request: &etcdserverpb.RequestOp_RequestRange{
				RequestRange: &etcdserverpb.RangeRequest{Key: key},
			},
		}},
	})
	require.NoError(t, err)
	require.False(t, resp.Succeeded)
	require.Len(t, resp.Responses, 1)
	rangeResp := resp.Responses[0].GetResponseRange()
	require.NotNil(t, rangeResp)
	require.Len(t, rangeResp.Kvs, 1)
	require.Equal(t, []byte("exists"), rangeResp.Kvs[0].Value)
}

func TestTxnCreateWithoutFailureRangeReturnsEmptyFailureResponse(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	ctx := context.Background()
	key := []byte("/registry/generic-txn/create-empty-failure")
	_, err := server.Put(ctx, &etcdserverpb.PutRequest{Key: key, Value: []byte("exists")})
	require.NoError(t, err)

	resp, err := server.Txn(ctx, &etcdserverpb.TxnRequest{
		Compare: []*etcdserverpb.Compare{{
			Key:         key,
			Target:      etcdserverpb.Compare_MOD,
			Result:      etcdserverpb.Compare_EQUAL,
			TargetUnion: &etcdserverpb.Compare_ModRevision{ModRevision: 0},
		}},
		Success: []*etcdserverpb.RequestOp{{
			Request: &etcdserverpb.RequestOp_RequestPut{
				RequestPut: &etcdserverpb.PutRequest{Key: key, Value: []byte("created")},
			},
		}},
	})
	require.NoError(t, err)
	require.False(t, resp.Succeeded)
	require.Empty(t, resp.Responses)
}

func TestTxnCreateWithPrevKVReturnsNilPrevKV(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	ctx := context.Background()
	key := []byte("/registry/generic-txn/create-prev-kv")

	resp, err := server.Txn(ctx, &etcdserverpb.TxnRequest{
		Compare: []*etcdserverpb.Compare{{
			Key:         key,
			Target:      etcdserverpb.Compare_MOD,
			Result:      etcdserverpb.Compare_EQUAL,
			TargetUnion: &etcdserverpb.Compare_ModRevision{ModRevision: 0},
		}},
		Success: []*etcdserverpb.RequestOp{{
			Request: &etcdserverpb.RequestOp_RequestPut{
				RequestPut: &etcdserverpb.PutRequest{Key: key, Value: []byte("created"), PrevKv: true},
			},
		}},
	})
	require.NoError(t, err)
	require.True(t, resp.Succeeded)
	require.Len(t, resp.Responses, 1)
	put := resp.Responses[0].GetResponsePut()
	require.NotNil(t, put)
	require.Nil(t, put.PrevKv)
}

func TestTxnComparePutWithoutFailureRange(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	ctx := context.Background()
	key := []byte("/registry/generic-txn/update")
	putResp, err := server.Put(ctx, &etcdserverpb.PutRequest{Key: key, Value: []byte("v1")})
	require.NoError(t, err)

	updateResp, err := server.Txn(ctx, &etcdserverpb.TxnRequest{
		Compare: []*etcdserverpb.Compare{{
			Key:         key,
			Target:      etcdserverpb.Compare_MOD,
			Result:      etcdserverpb.Compare_EQUAL,
			TargetUnion: &etcdserverpb.Compare_ModRevision{ModRevision: putResp.Header.Revision},
		}},
		Success: []*etcdserverpb.RequestOp{{
			Request: &etcdserverpb.RequestOp_RequestPut{
				RequestPut: &etcdserverpb.PutRequest{Key: key, Value: []byte("v2")},
			},
		}},
	})
	require.NoError(t, err)
	require.True(t, updateResp.Succeeded)
	require.Len(t, updateResp.Responses, 1)
	require.NotNil(t, updateResp.Responses[0].GetResponsePut())

	staleResp, err := server.Txn(ctx, &etcdserverpb.TxnRequest{
		Compare: []*etcdserverpb.Compare{{
			Key:         key,
			Target:      etcdserverpb.Compare_MOD,
			Result:      etcdserverpb.Compare_EQUAL,
			TargetUnion: &etcdserverpb.Compare_ModRevision{ModRevision: putResp.Header.Revision},
		}},
		Success: []*etcdserverpb.RequestOp{{
			Request: &etcdserverpb.RequestOp_RequestPut{
				RequestPut: &etcdserverpb.PutRequest{Key: key, Value: []byte("v3")},
			},
		}},
	})
	require.NoError(t, err)
	require.False(t, staleResp.Succeeded)
	require.Empty(t, staleResp.Responses)
}

func TestTxnComparePutWithPrevKV(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	ctx := context.Background()
	key := []byte("/registry/generic-txn/update-prev-kv")
	putResp, err := server.Put(ctx, &etcdserverpb.PutRequest{Key: key, Value: []byte("v1")})
	require.NoError(t, err)

	updateResp, err := server.Txn(ctx, &etcdserverpb.TxnRequest{
		Compare: []*etcdserverpb.Compare{{
			Key:         key,
			Target:      etcdserverpb.Compare_MOD,
			Result:      etcdserverpb.Compare_EQUAL,
			TargetUnion: &etcdserverpb.Compare_ModRevision{ModRevision: putResp.Header.Revision},
		}},
		Success: []*etcdserverpb.RequestOp{{
			Request: &etcdserverpb.RequestOp_RequestPut{
				RequestPut: &etcdserverpb.PutRequest{Key: key, Value: []byte("v2"), PrevKv: true},
			},
		}},
	})
	require.NoError(t, err)
	require.True(t, updateResp.Succeeded)
	require.Len(t, updateResp.Responses, 1)
	put := updateResp.Responses[0].GetResponsePut()
	require.NotNil(t, put)
	require.NotNil(t, put.PrevKv)
	require.Equal(t, []byte("v1"), put.PrevKv.Value)
	require.Equal(t, putResp.Header.Revision, put.PrevKv.ModRevision)
}

func TestTxnComparePutIgnoreLeasePreservesExistingLease(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	ctx := context.Background()
	key := []byte("/registry/generic-txn/update-ignore-lease")
	leaseResp, err := server.LeaseGrant(ctx, &etcdserverpb.LeaseGrantRequest{TTL: 30, ID: 24681})
	require.NoError(t, err)
	putResp, err := server.Put(ctx, &etcdserverpb.PutRequest{Key: key, Value: []byte("v1"), Lease: leaseResp.ID})
	require.NoError(t, err)

	updateResp, err := server.Txn(ctx, &etcdserverpb.TxnRequest{
		Compare: []*etcdserverpb.Compare{{
			Key:         key,
			Target:      etcdserverpb.Compare_MOD,
			Result:      etcdserverpb.Compare_EQUAL,
			TargetUnion: &etcdserverpb.Compare_ModRevision{ModRevision: putResp.Header.Revision},
		}},
		Success: []*etcdserverpb.RequestOp{{
			Request: &etcdserverpb.RequestOp_RequestPut{
				RequestPut: &etcdserverpb.PutRequest{Key: key, Value: []byte("v2"), IgnoreLease: true},
			},
		}},
	})
	require.NoError(t, err)
	require.True(t, updateResp.Succeeded)

	ttlResp, err := server.LeaseTimeToLive(ctx, &etcdserverpb.LeaseTimeToLiveRequest{ID: leaseResp.ID, Keys: true})
	require.NoError(t, err)
	require.Equal(t, [][]byte{key}, ttlResp.Keys)

	rangeResp, err := server.Range(ctx, &etcdserverpb.RangeRequest{Key: key})
	require.NoError(t, err)
	require.Len(t, rangeResp.Kvs, 1)
	require.Equal(t, []byte("v2"), rangeResp.Kvs[0].Value)
}

func TestTxnComparePutIgnoreValueUpdatesLeasePreservesValue(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	ctx := context.Background()
	key := []byte("/registry/generic-txn/update-ignore-value")
	leaseOne, err := server.LeaseGrant(ctx, &etcdserverpb.LeaseGrantRequest{TTL: 30, ID: 24684})
	require.NoError(t, err)
	leaseTwo, err := server.LeaseGrant(ctx, &etcdserverpb.LeaseGrantRequest{TTL: 30, ID: 24685})
	require.NoError(t, err)
	putResp, err := server.Put(ctx, &etcdserverpb.PutRequest{Key: key, Value: []byte("v1"), Lease: leaseOne.ID})
	require.NoError(t, err)

	updateResp, err := server.Txn(ctx, &etcdserverpb.TxnRequest{
		Compare: []*etcdserverpb.Compare{{
			Key:         key,
			Target:      etcdserverpb.Compare_MOD,
			Result:      etcdserverpb.Compare_EQUAL,
			TargetUnion: &etcdserverpb.Compare_ModRevision{ModRevision: putResp.Header.Revision},
		}},
		Success: []*etcdserverpb.RequestOp{{
			Request: &etcdserverpb.RequestOp_RequestPut{
				RequestPut: &etcdserverpb.PutRequest{Key: key, Lease: leaseTwo.ID, IgnoreValue: true},
			},
		}},
	})
	require.NoError(t, err)
	require.True(t, updateResp.Succeeded)

	rangeResp, err := server.Range(ctx, &etcdserverpb.RangeRequest{Key: key})
	require.NoError(t, err)
	require.Len(t, rangeResp.Kvs, 1)
	require.Equal(t, []byte("v1"), rangeResp.Kvs[0].Value)

	ttlOne, err := server.LeaseTimeToLive(ctx, &etcdserverpb.LeaseTimeToLiveRequest{ID: leaseOne.ID, Keys: true})
	require.NoError(t, err)
	require.Empty(t, ttlOne.Keys)
	ttlTwo, err := server.LeaseTimeToLive(ctx, &etcdserverpb.LeaseTimeToLiveRequest{ID: leaseTwo.ID, Keys: true})
	require.NoError(t, err)
	require.Equal(t, [][]byte{key}, ttlTwo.Keys)
}

func TestTxnCompareDeleteWithoutFailureRange(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	ctx := context.Background()
	key := []byte("/registry/generic-txn/delete")
	putResp, err := server.Put(ctx, &etcdserverpb.PutRequest{Key: key, Value: []byte("v1")})
	require.NoError(t, err)

	deleteResp, err := server.Txn(ctx, &etcdserverpb.TxnRequest{
		Compare: []*etcdserverpb.Compare{{
			Key:         key,
			Target:      etcdserverpb.Compare_MOD,
			Result:      etcdserverpb.Compare_EQUAL,
			TargetUnion: &etcdserverpb.Compare_ModRevision{ModRevision: putResp.Header.Revision},
		}},
		Success: []*etcdserverpb.RequestOp{{
			Request: &etcdserverpb.RequestOp_RequestDeleteRange{
				RequestDeleteRange: &etcdserverpb.DeleteRangeRequest{Key: key},
			},
		}},
	})
	require.NoError(t, err)
	require.True(t, deleteResp.Succeeded)
	require.Len(t, deleteResp.Responses, 1)
	require.NotNil(t, deleteResp.Responses[0].GetResponseDeleteRange())

	staleResp, err := server.Txn(ctx, &etcdserverpb.TxnRequest{
		Compare: []*etcdserverpb.Compare{{
			Key:         key,
			Target:      etcdserverpb.Compare_MOD,
			Result:      etcdserverpb.Compare_EQUAL,
			TargetUnion: &etcdserverpb.Compare_ModRevision{ModRevision: putResp.Header.Revision},
		}},
		Success: []*etcdserverpb.RequestOp{{
			Request: &etcdserverpb.RequestOp_RequestDeleteRange{
				RequestDeleteRange: &etcdserverpb.DeleteRangeRequest{Key: key},
			},
		}},
	})
	require.NoError(t, err)
	require.False(t, staleResp.Succeeded)
	require.Empty(t, staleResp.Responses)
}

func TestTxnCompareDeleteWithPrevKV(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	ctx := context.Background()
	key := []byte("/registry/generic-txn/delete-prev-kv")
	putResp, err := server.Put(ctx, &etcdserverpb.PutRequest{Key: key, Value: []byte("v1")})
	require.NoError(t, err)

	deleteResp, err := server.Txn(ctx, &etcdserverpb.TxnRequest{
		Compare: []*etcdserverpb.Compare{{
			Key:         key,
			Target:      etcdserverpb.Compare_MOD,
			Result:      etcdserverpb.Compare_EQUAL,
			TargetUnion: &etcdserverpb.Compare_ModRevision{ModRevision: putResp.Header.Revision},
		}},
		Success: []*etcdserverpb.RequestOp{{
			Request: &etcdserverpb.RequestOp_RequestDeleteRange{
				RequestDeleteRange: &etcdserverpb.DeleteRangeRequest{Key: key, PrevKv: true},
			},
		}},
	})
	require.NoError(t, err)
	require.True(t, deleteResp.Succeeded)
	require.Len(t, deleteResp.Responses, 1)
	del := deleteResp.Responses[0].GetResponseDeleteRange()
	require.NotNil(t, del)
	require.Equal(t, int64(1), del.Deleted)
	require.Len(t, del.PrevKvs, 1)
	require.Equal(t, []byte("v1"), del.PrevKvs[0].Value)
	require.Equal(t, putResp.Header.Revision, del.PrevKvs[0].ModRevision)
}

func TestTxnCompareDeleteWithFailureRange(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	ctx := context.Background()
	key := []byte("/registry/generic-txn/delete-failure-range")
	putResp, err := server.Put(ctx, &etcdserverpb.PutRequest{Key: key, Value: []byte("v1")})
	require.NoError(t, err)
	_, err = server.Put(ctx, &etcdserverpb.PutRequest{Key: key, Value: []byte("v2")})
	require.NoError(t, err)

	resp, err := server.Txn(ctx, &etcdserverpb.TxnRequest{
		Compare: []*etcdserverpb.Compare{{
			Key:         key,
			Target:      etcdserverpb.Compare_MOD,
			Result:      etcdserverpb.Compare_EQUAL,
			TargetUnion: &etcdserverpb.Compare_ModRevision{ModRevision: putResp.Header.Revision},
		}},
		Success: []*etcdserverpb.RequestOp{{
			Request: &etcdserverpb.RequestOp_RequestDeleteRange{
				RequestDeleteRange: &etcdserverpb.DeleteRangeRequest{Key: key},
			},
		}},
		Failure: []*etcdserverpb.RequestOp{{
			Request: &etcdserverpb.RequestOp_RequestRange{
				RequestRange: &etcdserverpb.RangeRequest{Key: key},
			},
		}},
	})
	require.NoError(t, err)
	require.False(t, resp.Succeeded)
	require.Len(t, resp.Responses, 1)
	rangeResp := resp.Responses[0].GetResponseRange()
	require.NotNil(t, rangeResp)
	require.Len(t, rangeResp.Kvs, 1)
	require.Equal(t, []byte("v2"), rangeResp.Kvs[0].Value)
}

func TestTxnSimpleSuccessPutRangeDeleteResponsesInOrder(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	ctx := context.Background()
	key := []byte("/registry/generic-txn/simple")
	deleteKey := []byte("/registry/generic-txn/simple-delete")

	_, err := server.Put(ctx, &etcdserverpb.PutRequest{Key: deleteKey, Value: []byte("delete-me")})
	require.NoError(t, err)

	resp, err := server.Txn(ctx, &etcdserverpb.TxnRequest{
		Success: []*etcdserverpb.RequestOp{
			{Request: &etcdserverpb.RequestOp_RequestPut{
				RequestPut: &etcdserverpb.PutRequest{Key: key, Value: []byte("v1")},
			}},
			{Request: &etcdserverpb.RequestOp_RequestRange{
				RequestRange: &etcdserverpb.RangeRequest{Key: key},
			}},
			{Request: &etcdserverpb.RequestOp_RequestDeleteRange{
				RequestDeleteRange: &etcdserverpb.DeleteRangeRequest{Key: deleteKey, PrevKv: true},
			}},
		},
	})
	require.NoError(t, err)
	require.True(t, resp.Succeeded)
	require.Len(t, resp.Responses, 3)
	require.NotNil(t, resp.Responses[0].GetResponsePut())

	rangeResp := resp.Responses[1].GetResponseRange()
	require.NotNil(t, rangeResp)
	require.Len(t, rangeResp.Kvs, 1)
	require.Equal(t, []byte("v1"), rangeResp.Kvs[0].Value)

	deleteResp := resp.Responses[2].GetResponseDeleteRange()
	require.NotNil(t, deleteResp)
	require.Equal(t, int64(1), deleteResp.Deleted)
	require.Len(t, deleteResp.PrevKvs, 1)
	require.Equal(t, []byte("delete-me"), deleteResp.PrevKvs[0].Value)

	getResp, err := server.Range(ctx, &etcdserverpb.RangeRequest{Key: key})
	require.NoError(t, err)
	require.Len(t, getResp.Kvs, 1)
	require.Equal(t, []byte("v1"), getResp.Kvs[0].Value)

	deletedResp, err := server.Range(ctx, &etcdserverpb.RangeRequest{Key: deleteKey})
	require.NoError(t, err)
	require.Empty(t, deletedResp.Kvs)
}

func TestTxnRangeSeesPriorWritesButNotLaterWrites(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	ctx := context.Background()
	prefix := []byte("/registry/generic-txn/ordered-view/")
	keyA := append(append([]byte(nil), prefix...), 'a')
	keyB := append(append([]byte(nil), prefix...), 'b')
	resp, err := server.Txn(ctx, &etcdserverpb.TxnRequest{Success: []*etcdserverpb.RequestOp{
		{Request: &etcdserverpb.RequestOp_RequestPut{RequestPut: &etcdserverpb.PutRequest{Key: keyA, Value: []byte("a")}}},
		{Request: &etcdserverpb.RequestOp_RequestRange{RequestRange: &etcdserverpb.RangeRequest{
			Key: prefix, RangeEnd: []byte("/registry/generic-txn/ordered-view0"),
		}}},
		{Request: &etcdserverpb.RequestOp_RequestPut{RequestPut: &etcdserverpb.PutRequest{Key: keyB, Value: []byte("b")}}},
	}})
	require.NoError(t, err)
	rangeResp := resp.Responses[1].GetResponseRange()
	require.Len(t, rangeResp.Kvs, 1)
	require.Equal(t, keyA, rangeResp.Kvs[0].Key)
	require.Equal(t, resp.Header.Revision, rangeResp.Header.Revision)
	require.Equal(t, resp.Header.Revision, rangeResp.Kvs[0].ModRevision)

	final, err := server.Range(ctx, &etcdserverpb.RangeRequest{Key: prefix, RangeEnd: []byte("/registry/generic-txn/ordered-view0")})
	require.NoError(t, err)
	require.Len(t, final.Kvs, 2)
	require.Equal(t, resp.Header.Revision, final.Kvs[0].ModRevision)
	require.Equal(t, resp.Header.Revision, final.Kvs[1].ModRevision)
}

func TestTxnOverlappingDeleteRangesUseStagedViewAndOneRevision(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	ctx := context.Background()
	prefix := "/registry/generic-txn/overlap-delete/"
	for _, suffix := range []string{"a", "b", "c"} {
		_, err := server.Put(ctx, &etcdserverpb.PutRequest{Key: []byte(prefix + suffix), Value: []byte(suffix)})
		require.NoError(t, err)
	}
	before := int64(server.backend.GetCurrentRevision())
	resp, err := server.Txn(ctx, &etcdserverpb.TxnRequest{Success: []*etcdserverpb.RequestOp{
		{Request: &etcdserverpb.RequestOp_RequestDeleteRange{RequestDeleteRange: &etcdserverpb.DeleteRangeRequest{
			Key: []byte(prefix + "a"), RangeEnd: []byte(prefix + "c"), PrevKv: true,
		}}},
		{Request: &etcdserverpb.RequestOp_RequestDeleteRange{RequestDeleteRange: &etcdserverpb.DeleteRangeRequest{
			Key: []byte(prefix + "b"), RangeEnd: []byte(prefix + "d"), PrevKv: true,
		}}},
	}})
	require.NoError(t, err)
	require.Equal(t, before+1, resp.Header.Revision)
	first := resp.Responses[0].GetResponseDeleteRange()
	second := resp.Responses[1].GetResponseDeleteRange()
	require.Equal(t, int64(2), first.Deleted)
	require.Equal(t, [][]byte{[]byte("a"), []byte("b")}, [][]byte{first.PrevKvs[0].Value, first.PrevKvs[1].Value})
	require.Equal(t, int64(1), second.Deleted)
	require.Equal(t, []byte("c"), second.PrevKvs[0].Value)

	remaining, err := server.Range(ctx, &etcdserverpb.RangeRequest{Key: []byte(prefix), RangeEnd: []byte(prefix + "z")})
	require.NoError(t, err)
	require.Empty(t, remaining.Kvs)
}

func TestTxnIgnoreOptionsUseStagedAtomicValidation(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	ctx := context.Background()
	lease, err := server.LeaseGrant(ctx, &etcdserverpb.LeaseGrantRequest{TTL: 30})
	require.NoError(t, err)
	key := []byte("/registry/generic-txn/ignore/existing")
	_, err = server.Put(ctx, &etcdserverpb.PutRequest{Key: key, Value: []byte("old"), Lease: lease.ID})
	require.NoError(t, err)

	resp, err := server.Txn(ctx, &etcdserverpb.TxnRequest{Success: []*etcdserverpb.RequestOp{
		{Request: &etcdserverpb.RequestOp_RequestPut{RequestPut: &etcdserverpb.PutRequest{Key: key, IgnoreLease: true, Value: []byte("new")}}},
		{Request: &etcdserverpb.RequestOp_RequestRange{RequestRange: &etcdserverpb.RangeRequest{Key: key}}},
	}})
	require.NoError(t, err)
	staged := resp.Responses[1].GetResponseRange().Kvs[0]
	require.Equal(t, []byte("new"), staged.Value)
	require.Equal(t, lease.ID, staged.Lease)

	missing := []byte("/registry/generic-txn/ignore/missing")
	mustNotLand := []byte("/registry/generic-txn/ignore/must-not-land")
	resp, err = server.Txn(ctx, &etcdserverpb.TxnRequest{Success: []*etcdserverpb.RequestOp{
		{Request: &etcdserverpb.RequestOp_RequestPut{RequestPut: &etcdserverpb.PutRequest{Key: mustNotLand, Value: []byte("x")}}},
		{Request: &etcdserverpb.RequestOp_RequestPut{RequestPut: &etcdserverpb.PutRequest{Key: missing, IgnoreValue: true}}},
	}})
	require.Nil(t, resp)
	require.Equal(t, codes.InvalidArgument, status.Code(err))
	require.Equal(t, "etcdserver: key not found", status.Convert(err).Message())
	get, err := server.Range(ctx, &etcdserverpb.RangeRequest{Key: mustNotLand})
	require.NoError(t, err)
	require.Empty(t, get.Kvs)
}

func TestTxnHistoricalRangeAfterPutReadsOldValueWithTxnHeader(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	ctx := context.Background()
	key := []byte("/registry/generic-txn/historical-overlay")
	seed, err := server.Put(ctx, &etcdserverpb.PutRequest{Key: key, Value: []byte("old")})
	require.NoError(t, err)

	resp, err := server.Txn(ctx, &etcdserverpb.TxnRequest{Success: []*etcdserverpb.RequestOp{
		{Request: &etcdserverpb.RequestOp_RequestPut{RequestPut: &etcdserverpb.PutRequest{Key: key, Value: []byte("new")}}},
		{Request: &etcdserverpb.RequestOp_RequestRange{RequestRange: &etcdserverpb.RangeRequest{Key: key, Revision: seed.Header.Revision}}},
	}})
	require.NoError(t, err)
	historical := resp.Responses[1].GetResponseRange()
	require.Equal(t, []byte("old"), historical.Kvs[0].Value)
	require.Equal(t, seed.Header.Revision, historical.Kvs[0].ModRevision)
	require.Equal(t, resp.Header.Revision, historical.Header.Revision)

	current, err := server.Range(ctx, &etcdserverpb.RangeRequest{Key: key})
	require.NoError(t, err)
	require.Equal(t, []byte("new"), current.Kvs[0].Value)
}

func TestTxnNoOpDeleteRangesDoNotConsumeRevision(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	ctx := context.Background()
	before := int64(server.backend.GetCurrentRevision())
	resp, err := server.Txn(ctx, &etcdserverpb.TxnRequest{Success: []*etcdserverpb.RequestOp{
		{Request: &etcdserverpb.RequestOp_RequestDeleteRange{RequestDeleteRange: &etcdserverpb.DeleteRangeRequest{
			Key: []byte("/registry/generic-txn/noop/a"), RangeEnd: []byte("/registry/generic-txn/noop/m"),
		}}},
		{Request: &etcdserverpb.RequestOp_RequestDeleteRange{RequestDeleteRange: &etcdserverpb.DeleteRangeRequest{
			Key: []byte("/registry/generic-txn/noop/m"), RangeEnd: []byte("/registry/generic-txn/noop/z"),
		}}},
	}})
	require.NoError(t, err)
	require.Equal(t, before, resp.Header.Revision)
	require.Equal(t, int64(0), resp.Responses[0].GetResponseDeleteRange().Deleted)
	require.Equal(t, int64(0), resp.Responses[1].GetResponseDeleteRange().Deleted)
	require.Equal(t, uint64(before), server.backend.GetCurrentRevision())
}

func TestTxnRangeOptionsApplyToStagedView(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	ctx := context.Background()
	prefix := "/registry/generic-txn/range-options/"
	_, err := server.Put(ctx, &etcdserverpb.PutRequest{Key: []byte(prefix + "a"), Value: []byte("old")})
	require.NoError(t, err)
	before := int64(server.backend.GetCurrentRevision())

	resp, err := server.Txn(ctx, &etcdserverpb.TxnRequest{Success: []*etcdserverpb.RequestOp{
		{Request: &etcdserverpb.RequestOp_RequestPut{RequestPut: &etcdserverpb.PutRequest{
			Key: []byte(prefix + "b"), Value: []byte("new-b"),
		}}},
		{Request: &etcdserverpb.RequestOp_RequestPut{RequestPut: &etcdserverpb.PutRequest{
			Key: []byte(prefix + "c"), Value: []byte("new-c"),
		}}},
		{Request: &etcdserverpb.RequestOp_RequestRange{RequestRange: &etcdserverpb.RangeRequest{
			Key:            []byte(prefix),
			RangeEnd:       []byte(prefix + "z"),
			MinModRevision: before + 1,
			CountOnly:      true,
		}}},
		{Request: &etcdserverpb.RequestOp_RequestRange{RequestRange: &etcdserverpb.RangeRequest{
			Key:        []byte(prefix),
			RangeEnd:   []byte(prefix + "z"),
			SortOrder:  etcdserverpb.RangeRequest_DESCEND,
			SortTarget: etcdserverpb.RangeRequest_KEY,
			Limit:      2,
			KeysOnly:   true,
		}}},
	}})
	require.NoError(t, err)
	require.Equal(t, before+1, resp.Header.Revision)

	counted := resp.Responses[2].GetResponseRange()
	require.Equal(t, int64(3), counted.Count)
	require.Empty(t, counted.Kvs)

	limited := resp.Responses[3].GetResponseRange()
	require.Equal(t, int64(3), limited.Count)
	require.True(t, limited.More)
	require.Equal(t, [][]byte{[]byte(prefix + "c"), []byte(prefix + "b")}, [][]byte{limited.Kvs[0].Key, limited.Kvs[1].Key})
	require.Nil(t, limited.Kvs[0].Value)
	require.Nil(t, limited.Kvs[1].Value)
}

func TestPutIgnoreLeaseRequiresExistingKey(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	key := []byte("/registry/put-ignore-lease-missing")
	resp, err := server.Put(context.Background(), &etcdserverpb.PutRequest{
		Key: key, Value: []byte("must-not-create"), IgnoreLease: true,
	})
	require.Nil(t, resp)
	require.Equal(t, codes.InvalidArgument, status.Code(err))
	require.Equal(t, "etcdserver: key not found", status.Convert(err).Message())
	get, err := server.Range(context.Background(), &etcdserverpb.RangeRequest{Key: key})
	require.NoError(t, err)
	require.Empty(t, get.Kvs)
}

func TestTxnSimpleSuccessPutWithLeaseIsRevoked(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	ctx := context.Background()
	key := []byte("/registry/generic-txn/simple-lease")
	leaseResp, err := server.LeaseGrant(ctx, &etcdserverpb.LeaseGrantRequest{TTL: 30, ID: 12345})
	require.NoError(t, err)

	resp, err := server.Txn(ctx, &etcdserverpb.TxnRequest{
		Success: []*etcdserverpb.RequestOp{
			{Request: &etcdserverpb.RequestOp_RequestPut{
				RequestPut: &etcdserverpb.PutRequest{Key: key, Value: []byte("leased"), Lease: leaseResp.ID},
			}},
		},
	})
	require.NoError(t, err)
	require.True(t, resp.Succeeded)
	attachment, err := server.backend.InternalGet(ctx, leaseAttachKey(string(key)))
	require.NoError(t, err)
	require.Equal(t, []byte("12345"), attachment)

	ttlResp, err := server.LeaseTimeToLive(ctx, &etcdserverpb.LeaseTimeToLiveRequest{ID: leaseResp.ID, Keys: true})
	require.NoError(t, err)
	require.Equal(t, [][]byte{key}, ttlResp.Keys)

	_, err = server.LeaseRevoke(ctx, &etcdserverpb.LeaseRevokeRequest{ID: leaseResp.ID})
	require.NoError(t, err)

	getResp, err := server.Range(ctx, &etcdserverpb.RangeRequest{Key: key})
	require.NoError(t, err)
	require.Empty(t, getResp.Kvs)
}

func TestTxnEmptySuccessMatchesEtcd(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	resp, err := server.Txn(context.Background(), &etcdserverpb.TxnRequest{})
	require.NoError(t, err)
	require.True(t, resp.Succeeded)
	require.Empty(t, resp.Responses)
	require.NotNil(t, resp.Header)
}

func TestTxnNestedSuccessResponseMatchesEtcd(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	ctx := context.Background()
	key := []byte("/registry/generic-txn/nested-exec")
	resp, err := server.Txn(ctx, &etcdserverpb.TxnRequest{
		Success: []*etcdserverpb.RequestOp{{
			Request: &etcdserverpb.RequestOp_RequestTxn{
				RequestTxn: &etcdserverpb.TxnRequest{
					Success: []*etcdserverpb.RequestOp{
						{Request: &etcdserverpb.RequestOp_RequestPut{
							RequestPut: &etcdserverpb.PutRequest{Key: key, Value: []byte("nested")},
						}},
						{Request: &etcdserverpb.RequestOp_RequestRange{
							RequestRange: &etcdserverpb.RangeRequest{Key: key},
						}},
					},
				},
			},
		}},
	})
	require.NoError(t, err)
	require.True(t, resp.Succeeded)
	require.Len(t, resp.Responses, 1)
	nested := resp.Responses[0].GetResponseTxn()
	require.NotNil(t, nested)
	require.True(t, nested.Succeeded)
	require.Len(t, nested.Responses, 2)
	require.NotNil(t, nested.Responses[0].GetResponsePut())
	rangeResp := nested.Responses[1].GetResponseRange()
	require.NotNil(t, rangeResp)
	require.Len(t, rangeResp.Kvs, 1)
	require.Equal(t, []byte("nested"), rangeResp.Kvs[0].Value)
}

func TestTxnNestedWriteOnlyUsesSingleRevision(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	ctx := context.Background()
	keyA := []byte("/registry/generic-txn/nested-atomic/a")
	keyB := []byte("/registry/generic-txn/nested-atomic/b")
	resp, err := server.Txn(ctx, &etcdserverpb.TxnRequest{
		Success: []*etcdserverpb.RequestOp{
			{Request: &etcdserverpb.RequestOp_RequestPut{RequestPut: &etcdserverpb.PutRequest{Key: keyA, Value: []byte("a")}}},
			{Request: &etcdserverpb.RequestOp_RequestTxn{RequestTxn: &etcdserverpb.TxnRequest{
				Success: []*etcdserverpb.RequestOp{{Request: &etcdserverpb.RequestOp_RequestPut{
					RequestPut: &etcdserverpb.PutRequest{Key: keyB, Value: []byte("b")},
				}}},
			}}},
		},
	})
	require.NoError(t, err)
	require.True(t, resp.Succeeded)
	require.Len(t, resp.Responses, 2)
	nested := resp.Responses[1].GetResponseTxn()
	require.NotNil(t, nested)
	require.Equal(t, resp.Header.Revision, nested.Header.Revision)

	gotA, err := server.Range(ctx, &etcdserverpb.RangeRequest{Key: keyA})
	require.NoError(t, err)
	gotB, err := server.Range(ctx, &etcdserverpb.RangeRequest{Key: keyB})
	require.NoError(t, err)
	require.Equal(t, resp.Header.Revision, gotA.Kvs[0].ModRevision)
	require.Equal(t, resp.Header.Revision, gotB.Kvs[0].ModRevision)
}

func TestTxnAtomicPutReturnsPrevKV(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	ctx := context.Background()
	key := []byte("/registry/generic-txn/atomic-prev/a")
	other := []byte("/registry/generic-txn/atomic-prev/b")
	_, err := server.Put(ctx, &etcdserverpb.PutRequest{Key: key, Value: []byte("old")})
	require.NoError(t, err)

	resp, err := server.Txn(ctx, &etcdserverpb.TxnRequest{Success: []*etcdserverpb.RequestOp{
		{Request: &etcdserverpb.RequestOp_RequestPut{RequestPut: &etcdserverpb.PutRequest{Key: key, Value: []byte("new"), PrevKv: true}}},
		{Request: &etcdserverpb.RequestOp_RequestPut{RequestPut: &etcdserverpb.PutRequest{Key: other, Value: []byte("other")}}},
	}})
	require.NoError(t, err)
	putResp := resp.Responses[0].GetResponsePut()
	require.NotNil(t, putResp)
	require.NotNil(t, putResp.PrevKv)
	require.Equal(t, key, putResp.PrevKv.Key)
	require.Equal(t, []byte("old"), putResp.PrevKv.Value)
	require.Less(t, putResp.PrevKv.ModRevision, resp.Header.Revision)
}

func TestTxnNestedCompareFailureResponseMatchesEtcd(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	ctx := context.Background()
	key := []byte("/registry/generic-txn/nested-compare-failure")
	_, err := server.Put(ctx, &etcdserverpb.PutRequest{Key: key, Value: []byte("existing")})
	require.NoError(t, err)

	resp, err := server.Txn(ctx, &etcdserverpb.TxnRequest{
		Success: []*etcdserverpb.RequestOp{{
			Request: &etcdserverpb.RequestOp_RequestTxn{
				RequestTxn: &etcdserverpb.TxnRequest{
					Compare: []*etcdserverpb.Compare{{
						Key:         key,
						Target:      etcdserverpb.Compare_VALUE,
						Result:      etcdserverpb.Compare_EQUAL,
						TargetUnion: &etcdserverpb.Compare_Value{Value: []byte("missing")},
					}},
					Success: []*etcdserverpb.RequestOp{{
						Request: &etcdserverpb.RequestOp_RequestPut{
							RequestPut: &etcdserverpb.PutRequest{Key: key, Value: []byte("unexpected")},
						},
					}},
					Failure: []*etcdserverpb.RequestOp{{
						Request: &etcdserverpb.RequestOp_RequestRange{
							RequestRange: &etcdserverpb.RangeRequest{Key: key},
						},
					}},
				},
			},
		}},
	})
	require.NoError(t, err)
	require.True(t, resp.Succeeded)
	require.Len(t, resp.Responses, 1)
	nested := resp.Responses[0].GetResponseTxn()
	require.NotNil(t, nested)
	require.False(t, nested.Succeeded)
	require.Len(t, nested.Responses, 1)
	rangeResp := nested.Responses[0].GetResponseRange()
	require.NotNil(t, rangeResp)
	require.Len(t, rangeResp.Kvs, 1)
	require.Equal(t, []byte("existing"), rangeResp.Kvs[0].Value)
}

func TestTxnNestedComparePathIsComputedBeforeWrites(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	ctx := context.Background()
	compareKey := []byte("/registry/generic-txn/nested-path-before-write/compare")
	resultKey := []byte("/registry/generic-txn/nested-path-before-write/result")
	resp, err := server.Txn(ctx, &etcdserverpb.TxnRequest{
		Success: []*etcdserverpb.RequestOp{
			{Request: &etcdserverpb.RequestOp_RequestPut{
				RequestPut: &etcdserverpb.PutRequest{Key: compareKey, Value: []byte("outer")},
			}},
			{Request: &etcdserverpb.RequestOp_RequestTxn{
				RequestTxn: &etcdserverpb.TxnRequest{
					Compare: []*etcdserverpb.Compare{{
						Key:         compareKey,
						Target:      etcdserverpb.Compare_VALUE,
						Result:      etcdserverpb.Compare_EQUAL,
						TargetUnion: &etcdserverpb.Compare_Value{Value: []byte("outer")},
					}},
					Success: []*etcdserverpb.RequestOp{{
						Request: &etcdserverpb.RequestOp_RequestPut{
							RequestPut: &etcdserverpb.PutRequest{Key: resultKey, Value: []byte("nested-success")},
						},
					}},
					Failure: []*etcdserverpb.RequestOp{{
						Request: &etcdserverpb.RequestOp_RequestPut{
							RequestPut: &etcdserverpb.PutRequest{Key: resultKey, Value: []byte("nested-failure")},
						},
					}},
				},
			}},
		},
	})
	require.NoError(t, err)
	require.True(t, resp.Succeeded)
	require.Len(t, resp.Responses, 2)
	nested := resp.Responses[1].GetResponseTxn()
	require.NotNil(t, nested)
	require.False(t, nested.Succeeded)

	getResp, err := server.Range(ctx, &etcdserverpb.RangeRequest{Key: resultKey})
	require.NoError(t, err)
	require.Len(t, getResp.Kvs, 1)
	require.Equal(t, []byte("nested-failure"), getResp.Kvs[0].Value)
}

func TestTxnRangeFutureRevisionIsCheckedBeforeWrites(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	ctx := context.Background()
	key := []byte("/registry/generic-txn/range-future-before-write")
	futureRev := int64(server.backend.GetCurrentRevision()) + 1
	resp, err := server.Txn(ctx, &etcdserverpb.TxnRequest{
		Success: []*etcdserverpb.RequestOp{
			{Request: &etcdserverpb.RequestOp_RequestPut{
				RequestPut: &etcdserverpb.PutRequest{Key: key, Value: []byte("should-not-write")},
			}},
			{Request: &etcdserverpb.RequestOp_RequestRange{
				RequestRange: &etcdserverpb.RangeRequest{Key: key, Revision: futureRev},
			}},
		},
	})
	require.Nil(t, resp)
	require.Error(t, err)
	require.Equal(t, codes.OutOfRange, status.Code(err))
	require.Contains(t, err.Error(), "etcdserver: mvcc: required revision is a future revision")

	getResp, err := server.Range(ctx, &etcdserverpb.RangeRequest{Key: key})
	require.NoError(t, err)
	require.Empty(t, getResp.Kvs)
}

func TestTxnRangeCompactedRevisionIsCheckedBeforeWrites(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	ctx := context.Background()
	key := []byte("/registry/generic-txn/range-compacted-before-write")
	putResp, err := server.Put(ctx, &etcdserverpb.PutRequest{Key: key, Value: []byte("base")})
	require.NoError(t, err)
	putResp, err = server.Put(ctx, &etcdserverpb.PutRequest{Key: key, Value: []byte("base2")})
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		return server.backend.GetCurrentRevision() >= uint64(putResp.Header.Revision)
	}, time.Second, 10*time.Millisecond)
	compactRev := putResp.Header.Revision
	_, err = server.Compact(ctx, &etcdserverpb.CompactionRequest{Revision: compactRev})
	require.NoError(t, err)

	resp, err := server.Txn(ctx, &etcdserverpb.TxnRequest{
		Success: []*etcdserverpb.RequestOp{
			{Request: &etcdserverpb.RequestOp_RequestPut{
				RequestPut: &etcdserverpb.PutRequest{Key: key, Value: []byte("should-not-write")},
			}},
			{Request: &etcdserverpb.RequestOp_RequestRange{
				RequestRange: &etcdserverpb.RangeRequest{Key: key, Revision: compactRev - 1},
			}},
		},
	})
	require.Nil(t, resp)
	require.Error(t, err)
	require.Equal(t, codes.OutOfRange, status.Code(err))
	require.Contains(t, err.Error(), "etcdserver: mvcc: required revision has been compacted")

	getResp, err := server.Range(ctx, &etcdserverpb.RangeRequest{Key: key})
	require.NoError(t, err)
	require.Len(t, getResp.Kvs, 1)
	require.Equal(t, []byte("base2"), getResp.Kvs[0].Value)
}

func TestTxnNestedRangeFutureRevisionIsCheckedBeforeWrites(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	ctx := context.Background()
	key := []byte("/registry/generic-txn/nested-range-future-before-write")
	futureRev := int64(server.backend.GetCurrentRevision()) + 1
	resp, err := server.Txn(ctx, &etcdserverpb.TxnRequest{
		Success: []*etcdserverpb.RequestOp{
			{Request: &etcdserverpb.RequestOp_RequestPut{
				RequestPut: &etcdserverpb.PutRequest{Key: key, Value: []byte("should-not-write")},
			}},
			{Request: &etcdserverpb.RequestOp_RequestTxn{
				RequestTxn: &etcdserverpb.TxnRequest{
					Success: []*etcdserverpb.RequestOp{{
						Request: &etcdserverpb.RequestOp_RequestRange{
							RequestRange: &etcdserverpb.RangeRequest{Key: key, Revision: futureRev},
						},
					}},
				},
			}},
		},
	})
	require.Nil(t, resp)
	require.Error(t, err)
	require.Equal(t, codes.OutOfRange, status.Code(err))
	require.Contains(t, err.Error(), "etcdserver: mvcc: required revision is a future revision")

	getResp, err := server.Range(ctx, &etcdserverpb.RangeRequest{Key: key})
	require.NoError(t, err)
	require.Empty(t, getResp.Kvs)
}

func TestTxnCompareValueRunsSuccessBranch(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	ctx := context.Background()
	key := []byte("/registry/generic-txn/compare-value")
	_, err := server.Put(ctx, &etcdserverpb.PutRequest{Key: key, Value: []byte("v1")})
	require.NoError(t, err)

	resp, err := server.Txn(ctx, &etcdserverpb.TxnRequest{
		Compare: []*etcdserverpb.Compare{{
			Key:         key,
			Target:      etcdserverpb.Compare_VALUE,
			Result:      etcdserverpb.Compare_EQUAL,
			TargetUnion: &etcdserverpb.Compare_Value{Value: []byte("v1")},
		}},
		Success: []*etcdserverpb.RequestOp{
			{Request: &etcdserverpb.RequestOp_RequestPut{
				RequestPut: &etcdserverpb.PutRequest{Key: key, Value: []byte("v2")},
			}},
			{Request: &etcdserverpb.RequestOp_RequestRange{
				RequestRange: &etcdserverpb.RangeRequest{Key: key},
			}},
		},
		Failure: []*etcdserverpb.RequestOp{
			{Request: &etcdserverpb.RequestOp_RequestPut{
				RequestPut: &etcdserverpb.PutRequest{Key: key, Value: []byte("failure")},
			}},
		},
	})
	require.NoError(t, err)
	require.True(t, resp.Succeeded)
	require.Len(t, resp.Responses, 2)
	rangeResp := resp.Responses[1].GetResponseRange()
	require.NotNil(t, rangeResp)
	require.Len(t, rangeResp.Kvs, 1)
	require.Equal(t, []byte("v2"), rangeResp.Kvs[0].Value)
}

func TestTxnCompareValueRunsFailureBranch(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	ctx := context.Background()
	key := []byte("/registry/generic-txn/compare-value-failure")
	_, err := server.Put(ctx, &etcdserverpb.PutRequest{Key: key, Value: []byte("v1")})
	require.NoError(t, err)

	resp, err := server.Txn(ctx, &etcdserverpb.TxnRequest{
		Compare: []*etcdserverpb.Compare{{
			Key:         key,
			Target:      etcdserverpb.Compare_VALUE,
			Result:      etcdserverpb.Compare_EQUAL,
			TargetUnion: &etcdserverpb.Compare_Value{Value: []byte("missing")},
		}},
		Success: []*etcdserverpb.RequestOp{
			{Request: &etcdserverpb.RequestOp_RequestPut{
				RequestPut: &etcdserverpb.PutRequest{Key: key, Value: []byte("success")},
			}},
		},
		Failure: []*etcdserverpb.RequestOp{
			{Request: &etcdserverpb.RequestOp_RequestRange{
				RequestRange: &etcdserverpb.RangeRequest{Key: key},
			}},
			{Request: &etcdserverpb.RequestOp_RequestDeleteRange{
				RequestDeleteRange: &etcdserverpb.DeleteRangeRequest{Key: key, PrevKv: true},
			}},
		},
	})
	require.NoError(t, err)
	require.False(t, resp.Succeeded)
	require.Len(t, resp.Responses, 2)
	rangeResp := resp.Responses[0].GetResponseRange()
	require.NotNil(t, rangeResp)
	require.Len(t, rangeResp.Kvs, 1)
	require.Equal(t, []byte("v1"), rangeResp.Kvs[0].Value)
	deleteResp := resp.Responses[1].GetResponseDeleteRange()
	require.NotNil(t, deleteResp)
	require.Equal(t, int64(1), deleteResp.Deleted)
}

func TestTxnRejectsInvalidCompareRequest(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	tests := []struct {
		name string
		cmp  *etcdserverpb.Compare
	}{
		{
			name: "empty key",
			cmp: &etcdserverpb.Compare{
				Target:      etcdserverpb.Compare_MOD,
				Result:      etcdserverpb.Compare_EQUAL,
				TargetUnion: &etcdserverpb.Compare_ModRevision{ModRevision: 1},
			},
		},
		{
			name: "invalid result",
			cmp: &etcdserverpb.Compare{
				Key:         []byte("/registry/generic-txn/invalid-compare"),
				Target:      etcdserverpb.Compare_MOD,
				Result:      etcdserverpb.Compare_CompareResult(99),
				TargetUnion: &etcdserverpb.Compare_ModRevision{ModRevision: 1},
			},
		},
		{
			name: "invalid target",
			cmp: &etcdserverpb.Compare{
				Key:         []byte("/registry/generic-txn/invalid-compare"),
				Target:      etcdserverpb.Compare_CompareTarget(99),
				Result:      etcdserverpb.Compare_EQUAL,
				TargetUnion: &etcdserverpb.Compare_ModRevision{ModRevision: 1},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := server.Txn(context.Background(), &etcdserverpb.TxnRequest{
				Compare: []*etcdserverpb.Compare{tt.cmp},
				Success: []*etcdserverpb.RequestOp{
					{Request: &etcdserverpb.RequestOp_RequestRange{
						RequestRange: &etcdserverpb.RangeRequest{Key: []byte("/registry/generic-txn/invalid-compare")},
					}},
				},
			})
			require.Error(t, err)
			require.Equal(t, codes.InvalidArgument, status.Code(err))
		})
	}
}

func TestTxnRejectsInvalidRequestOps(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	tests := []struct {
		name string
		op   *etcdserverpb.RequestOp
	}{
		{
			name: "put empty key",
			op: &etcdserverpb.RequestOp{Request: &etcdserverpb.RequestOp_RequestPut{
				RequestPut: &etcdserverpb.PutRequest{Value: []byte("v1")},
			}},
		},
		{
			name: "put ignore value with value",
			op: &etcdserverpb.RequestOp{Request: &etcdserverpb.RequestOp_RequestPut{
				RequestPut: &etcdserverpb.PutRequest{Key: []byte("/registry/generic-txn/invalid-op"), Value: []byte("v1"), IgnoreValue: true},
			}},
		},
		{
			name: "range invalid sort",
			op: &etcdserverpb.RequestOp{Request: &etcdserverpb.RequestOp_RequestRange{
				RequestRange: &etcdserverpb.RangeRequest{Key: []byte("/registry/generic-txn/invalid-op"), SortOrder: etcdserverpb.RangeRequest_SortOrder(99)},
			}},
		},
		{
			name: "delete empty key",
			op: &etcdserverpb.RequestOp{Request: &etcdserverpb.RequestOp_RequestDeleteRange{
				RequestDeleteRange: &etcdserverpb.DeleteRangeRequest{},
			}},
		},
		{
			name: "empty operation",
			op:   &etcdserverpb.RequestOp{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := server.Txn(context.Background(), &etcdserverpb.TxnRequest{
				Success: []*etcdserverpb.RequestOp{tt.op},
			})
			require.Error(t, err)
			require.Equal(t, codes.InvalidArgument, status.Code(err))
		})
	}
}

func TestTxnRejectsTooManyOpsLikeEtcd(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	ops := make([]*etcdserverpb.RequestOp, defaultMaxTxnOps+1)
	for i := range ops {
		ops[i] = &etcdserverpb.RequestOp{Request: &etcdserverpb.RequestOp_RequestRange{
			RequestRange: &etcdserverpb.RangeRequest{Key: []byte("/registry/generic-txn/too-many")},
		}}
	}

	_, err := server.Txn(context.Background(), &etcdserverpb.TxnRequest{Success: ops})
	require.Error(t, err)
	require.Equal(t, codes.InvalidArgument, status.Code(err))
	require.Contains(t, err.Error(), "etcdserver: too many operations in txn request")
}

func TestTxnRejectsTooManyNestedOpsLikeEtcd(t *testing.T) {
	ops := make([]*etcdserverpb.RequestOp, 0, defaultMaxTxnOps)
	for i := 0; i < defaultMaxTxnOps-1; i++ {
		ops = append(ops, &etcdserverpb.RequestOp{Request: &etcdserverpb.RequestOp_RequestRange{
			RequestRange: &etcdserverpb.RangeRequest{Key: []byte("/registry/generic-txn/outer")},
		}})
	}
	ops = append(ops, &etcdserverpb.RequestOp{Request: &etcdserverpb.RequestOp_RequestTxn{
		RequestTxn: &etcdserverpb.TxnRequest{
			Success: []*etcdserverpb.RequestOp{
				{Request: &etcdserverpb.RequestOp_RequestRange{
					RequestRange: &etcdserverpb.RangeRequest{Key: []byte("/registry/generic-txn/inner")},
				}},
			},
		},
	}})

	err := validateTxnRequest(&etcdserverpb.TxnRequest{
		Success: ops,
	})
	require.Error(t, err)
	require.Equal(t, codes.InvalidArgument, status.Code(err))
	require.Contains(t, err.Error(), "etcdserver: too many operations in txn request")
}

func TestTxnRejectsDuplicatePutKeys(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	_, err := server.Txn(context.Background(), &etcdserverpb.TxnRequest{
		Success: []*etcdserverpb.RequestOp{
			{Request: &etcdserverpb.RequestOp_RequestPut{
				RequestPut: &etcdserverpb.PutRequest{Key: []byte("/registry/generic-txn/duplicate"), Value: []byte("v1")},
			}},
			{Request: &etcdserverpb.RequestOp_RequestPut{
				RequestPut: &etcdserverpb.PutRequest{Key: []byte("/registry/generic-txn/duplicate"), Value: []byte("v2")},
			}},
		},
	})
	require.Error(t, err)
	require.Equal(t, codes.InvalidArgument, status.Code(err))
	require.Contains(t, err.Error(), "duplicate key")
}

func TestTxnRejectsPutOverlappingDeleteRange(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	_, err := server.Txn(context.Background(), &etcdserverpb.TxnRequest{
		Success: []*etcdserverpb.RequestOp{
			{Request: &etcdserverpb.RequestOp_RequestDeleteRange{
				RequestDeleteRange: &etcdserverpb.DeleteRangeRequest{
					Key:      []byte("/registry/generic-txn/overlap/"),
					RangeEnd: []byte("/registry/generic-txn/overlap0"),
				},
			}},
			{Request: &etcdserverpb.RequestOp_RequestPut{
				RequestPut: &etcdserverpb.PutRequest{Key: []byte("/registry/generic-txn/overlap/a"), Value: []byte("v1")},
			}},
		},
	})
	require.Error(t, err)
	require.Equal(t, codes.InvalidArgument, status.Code(err))
	require.Contains(t, err.Error(), "duplicate key")
}

func TestTxnAllowsSameKeyInDifferentBranches(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	key := []byte("/registry/generic-txn/branch-put")
	resp, err := server.Txn(context.Background(), &etcdserverpb.TxnRequest{
		Compare: []*etcdserverpb.Compare{
			{
				Key:         key,
				Target:      etcdserverpb.Compare_MOD,
				Result:      etcdserverpb.Compare_EQUAL,
				TargetUnion: &etcdserverpb.Compare_ModRevision{ModRevision: 0},
			},
		},
		Success: []*etcdserverpb.RequestOp{
			{Request: &etcdserverpb.RequestOp_RequestPut{
				RequestPut: &etcdserverpb.PutRequest{Key: key, Value: []byte("success")},
			}},
		},
		Failure: []*etcdserverpb.RequestOp{
			{Request: &etcdserverpb.RequestOp_RequestPut{
				RequestPut: &etcdserverpb.PutRequest{Key: key, Value: []byte("failure")},
			}},
		},
	})
	require.NoError(t, err)
	require.True(t, resp.Succeeded)

	rangeResp, err := server.Range(context.Background(), &etcdserverpb.RangeRequest{Key: key})
	require.NoError(t, err)
	require.Len(t, rangeResp.Kvs, 1)
	require.Equal(t, []byte("success"), rangeResp.Kvs[0].Value)
}

func TestTxnIntervalValidationRecursesIntoNestedTxn(t *testing.T) {
	key := []byte("/registry/generic-txn/nested/a")

	err := validateTxnRequest(&etcdserverpb.TxnRequest{
		Success: []*etcdserverpb.RequestOp{
			{Request: &etcdserverpb.RequestOp_RequestDeleteRange{
				RequestDeleteRange: &etcdserverpb.DeleteRangeRequest{
					Key:      []byte("/registry/generic-txn/nested/"),
					RangeEnd: []byte("/registry/generic-txn/nested0"),
				},
			}},
			{Request: &etcdserverpb.RequestOp_RequestTxn{
				RequestTxn: &etcdserverpb.TxnRequest{
					Success: []*etcdserverpb.RequestOp{
						{Request: &etcdserverpb.RequestOp_RequestPut{
							RequestPut: &etcdserverpb.PutRequest{Key: key, Value: []byte("v1")},
						}},
					},
				},
			}},
		},
	})
	require.Error(t, err)
	require.Equal(t, codes.InvalidArgument, status.Code(err))
	require.Contains(t, err.Error(), "duplicate key")
}

func TestTxnIntervalValidationAllowsNestedThenElseSameKey(t *testing.T) {
	key := []byte("/registry/generic-txn/nested-branch/a")

	err := validateTxnRequest(&etcdserverpb.TxnRequest{
		Success: []*etcdserverpb.RequestOp{
			{Request: &etcdserverpb.RequestOp_RequestTxn{
				RequestTxn: &etcdserverpb.TxnRequest{
					Success: []*etcdserverpb.RequestOp{
						{Request: &etcdserverpb.RequestOp_RequestPut{
							RequestPut: &etcdserverpb.PutRequest{Key: key, Value: []byte("then")},
						}},
					},
					Failure: []*etcdserverpb.RequestOp{
						{Request: &etcdserverpb.RequestOp_RequestPut{
							RequestPut: &etcdserverpb.PutRequest{Key: key, Value: []byte("else")},
						}},
					},
				},
			}},
		},
	})
	require.NoError(t, err)
}

func TestTxnIntervalValidationRejectsDuplicateNestedSiblingPuts(t *testing.T) {
	key := []byte("/registry/generic-txn/nested-sibling/a")

	err := validateTxnRequest(&etcdserverpb.TxnRequest{
		Success: []*etcdserverpb.RequestOp{
			{Request: &etcdserverpb.RequestOp_RequestTxn{
				RequestTxn: &etcdserverpb.TxnRequest{
					Success: []*etcdserverpb.RequestOp{
						{Request: &etcdserverpb.RequestOp_RequestPut{
							RequestPut: &etcdserverpb.PutRequest{Key: key, Value: []byte("v1")},
						}},
					},
				},
			}},
			{Request: &etcdserverpb.RequestOp_RequestTxn{
				RequestTxn: &etcdserverpb.TxnRequest{
					Success: []*etcdserverpb.RequestOp{
						{Request: &etcdserverpb.RequestOp_RequestPut{
							RequestPut: &etcdserverpb.PutRequest{Key: key, Value: []byte("v2")},
						}},
					},
				},
			}},
		},
	})
	require.Error(t, err)
	require.Equal(t, codes.InvalidArgument, status.Code(err))
	require.Contains(t, err.Error(), "duplicate key")
}

func TestTxnRangeCompareModRevisionRequiresAllKeysToMatch(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	ctx := context.Background()
	prefix := "/registry/generic-txn/range-compare-mod/"
	putA, err := server.Put(ctx, &etcdserverpb.PutRequest{Key: []byte(prefix + "a"), Value: []byte("v1")})
	require.NoError(t, err)
	putB, err := server.Put(ctx, &etcdserverpb.PutRequest{Key: []byte(prefix + "b"), Value: []byte("v2")})
	require.NoError(t, err)

	var resp *etcdserverpb.TxnResponse
	require.Eventually(t, func() bool {
		var err error
		resp, err = server.Txn(ctx, &etcdserverpb.TxnRequest{
			Compare: []*etcdserverpb.Compare{{
				Key:         []byte(prefix),
				RangeEnd:    []byte("/registry/generic-txn/range-compare-mod0"),
				Target:      etcdserverpb.Compare_MOD,
				Result:      etcdserverpb.Compare_GREATER,
				TargetUnion: &etcdserverpb.Compare_ModRevision{ModRevision: putA.Header.Revision - 1},
			}},
			Success: []*etcdserverpb.RequestOp{
				{Request: &etcdserverpb.RequestOp_RequestRange{
					RequestRange: &etcdserverpb.RangeRequest{Key: []byte(prefix), RangeEnd: []byte("/registry/generic-txn/range-compare-mod0")},
				}},
			},
		})
		return err == nil && resp.Succeeded && len(resp.Responses[0].GetResponseRange().Kvs) == 2
	}, time.Second, 10*time.Millisecond)

	resp, err = server.Txn(ctx, &etcdserverpb.TxnRequest{
		Compare: []*etcdserverpb.Compare{{
			Key:         []byte(prefix),
			RangeEnd:    []byte("/registry/generic-txn/range-compare-mod0"),
			Target:      etcdserverpb.Compare_MOD,
			Result:      etcdserverpb.Compare_GREATER,
			TargetUnion: &etcdserverpb.Compare_ModRevision{ModRevision: putB.Header.Revision},
		}},
		Success: []*etcdserverpb.RequestOp{
			{Request: &etcdserverpb.RequestOp_RequestPut{
				RequestPut: &etcdserverpb.PutRequest{Key: []byte(prefix + "c"), Value: []byte("should-not-write")},
			}},
		},
		Failure: []*etcdserverpb.RequestOp{
			{Request: &etcdserverpb.RequestOp_RequestRange{
				RequestRange: &etcdserverpb.RangeRequest{Key: []byte(prefix), RangeEnd: []byte("/registry/generic-txn/range-compare-mod0")},
			}},
		},
	})
	require.NoError(t, err)
	require.False(t, resp.Succeeded)
	require.Len(t, resp.Responses[0].GetResponseRange().Kvs, 2)
}

func TestTxnRangeCompareValueFailsForEmptyRange(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	resp, err := server.Txn(context.Background(), &etcdserverpb.TxnRequest{
		Compare: []*etcdserverpb.Compare{{
			Key:         []byte("/registry/generic-txn/range-compare-empty/"),
			RangeEnd:    []byte("/registry/generic-txn/range-compare-empty0"),
			Target:      etcdserverpb.Compare_VALUE,
			Result:      etcdserverpb.Compare_EQUAL,
			TargetUnion: &etcdserverpb.Compare_Value{Value: []byte{}},
		}},
		Success: []*etcdserverpb.RequestOp{
			{Request: &etcdserverpb.RequestOp_RequestPut{
				RequestPut: &etcdserverpb.PutRequest{Key: []byte("/registry/generic-txn/range-compare-empty/result"), Value: []byte("should-not-write")},
			}},
		},
		Failure: []*etcdserverpb.RequestOp{
			{Request: &etcdserverpb.RequestOp_RequestRange{
				RequestRange: &etcdserverpb.RangeRequest{Key: []byte("/registry/generic-txn/range-compare-empty/result")},
			}},
		},
	})
	require.NoError(t, err)
	require.False(t, resp.Succeeded)
	require.Empty(t, resp.Responses[0].GetResponseRange().Kvs)
}

func TestTxnHasRangeCompareRecursesIntoNestedBranches(t *testing.T) {
	point := &etcdserverpb.TxnRequest{Compare: []*etcdserverpb.Compare{{Key: []byte("point")}}}
	require.False(t, txnHasRangeCompare(point))

	nested := &etcdserverpb.TxnRequest{
		Failure: []*etcdserverpb.RequestOp{{
			Request: &etcdserverpb.RequestOp_RequestTxn{RequestTxn: &etcdserverpb.TxnRequest{
				Success: []*etcdserverpb.RequestOp{{
					Request: &etcdserverpb.RequestOp_RequestTxn{RequestTxn: &etcdserverpb.TxnRequest{
						Compare: []*etcdserverpb.Compare{{
							Key:      []byte("prefix/"),
							RangeEnd: []byte("prefix0"),
						}},
					}},
				}},
			}},
		}},
	}
	require.True(t, txnHasRangeCompare(nested))
}

type blockingRangeCompareShim struct {
	BackendShim
	scanned chan struct{}
	release chan struct{}
	once    sync.Once
}

func (b *blockingRangeCompareShim) List(ctx context.Context, r *etcdserverpb.RangeRequest) (*etcdserverpb.RangeResponse, error) {
	resp, err := b.BackendShim.List(ctx, r)
	if err == nil && len(r.RangeEnd) != 0 {
		b.once.Do(func() {
			close(b.scanned)
			select {
			case <-b.release:
			case <-ctx.Done():
			}
		})
	}
	return resp, err
}

func TestTxnRangeCompareExcludesPhantomInsert(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	shim := &blockingRangeCompareShim{
		BackendShim: server.backend,
		scanned:     make(chan struct{}),
		release:     make(chan struct{}),
	}
	server.backend = shim

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	prefix := "/registry/generic-txn/phantom/"
	txnDone := make(chan *etcdserverpb.TxnResponse, 1)
	txnErr := make(chan error, 1)
	go func() {
		resp, err := server.Txn(ctx, &etcdserverpb.TxnRequest{
			Compare: []*etcdserverpb.Compare{{
				Key:         []byte(prefix),
				RangeEnd:    []byte("/registry/generic-txn/phantom0"),
				Target:      etcdserverpb.Compare_VERSION,
				Result:      etcdserverpb.Compare_EQUAL,
				TargetUnion: &etcdserverpb.Compare_Version{Version: 0},
			}},
			Success: []*etcdserverpb.RequestOp{{
				Request: &etcdserverpb.RequestOp_RequestPut{RequestPut: &etcdserverpb.PutRequest{
					Key: []byte("/registry/generic-txn/phantom-result"), Value: []byte("success"),
				}},
			}},
		})
		txnDone <- resp
		txnErr <- err
	}()

	select {
	case <-shim.scanned:
	case <-ctx.Done():
		require.FailNow(t, "range compare did not reach scan barrier")
	}

	putDone := make(chan error, 1)
	go func() {
		_, err := server.Put(ctx, &etcdserverpb.PutRequest{
			Key: []byte(prefix + "inserted"), Value: []byte("phantom"),
		})
		putDone <- err
	}()
	select {
	case err := <-putDone:
		require.FailNow(t, "phantom insert completed before range transaction", "err=%v", err)
	case <-time.After(50 * time.Millisecond):
	}

	close(shim.release)
	resp := <-txnDone
	require.NoError(t, <-txnErr)
	require.NotNil(t, resp)
	require.True(t, resp.Succeeded)
	require.NoError(t, <-putDone)
}

func TestTxnRangeCompareLeasedPutDoesNotDeadlock(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	lease, err := server.LeaseGrant(ctx, &etcdserverpb.LeaseGrantRequest{TTL: 30})
	require.NoError(t, err)

	prefix := []byte("/registry/generic-txn/range-lease/")
	key := append(append([]byte(nil), prefix...), []byte("key")...)
	resp, err := server.Txn(ctx, &etcdserverpb.TxnRequest{
		Compare: []*etcdserverpb.Compare{{
			Key:         prefix,
			RangeEnd:    []byte("/registry/generic-txn/range-lease0"),
			Target:      etcdserverpb.Compare_VERSION,
			Result:      etcdserverpb.Compare_EQUAL,
			TargetUnion: &etcdserverpb.Compare_Version{Version: 0},
		}},
		Success: []*etcdserverpb.RequestOp{{
			Request: &etcdserverpb.RequestOp_RequestPut{RequestPut: &etcdserverpb.PutRequest{
				Key: key, Value: []byte("leased"), Lease: lease.ID,
			}},
		}},
	})
	require.NoError(t, err)
	require.True(t, resp.Succeeded)

	ttl, err := server.LeaseTimeToLive(ctx, &etcdserverpb.LeaseTimeToLiveRequest{ID: lease.ID, Keys: true})
	require.NoError(t, err)
	require.Equal(t, [][]byte{key}, ttl.Keys)
}

func TestTxnCompareCreateRevisionZeroChecksExistence(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	ctx := context.Background()
	key := []byte("/registry/generic-txn/compare-create-zero")

	resp, err := server.Txn(ctx, &etcdserverpb.TxnRequest{
		Compare: []*etcdserverpb.Compare{{
			Key:         key,
			Target:      etcdserverpb.Compare_CREATE,
			Result:      etcdserverpb.Compare_EQUAL,
			TargetUnion: &etcdserverpb.Compare_CreateRevision{CreateRevision: 0},
		}},
		Success: []*etcdserverpb.RequestOp{
			{Request: &etcdserverpb.RequestOp_RequestPut{
				RequestPut: &etcdserverpb.PutRequest{Key: key, Value: []byte("created")},
			}},
		},
	})
	require.NoError(t, err)
	require.True(t, resp.Succeeded)

	resp, err = server.Txn(ctx, &etcdserverpb.TxnRequest{
		Compare: []*etcdserverpb.Compare{{
			Key:         key,
			Target:      etcdserverpb.Compare_CREATE,
			Result:      etcdserverpb.Compare_EQUAL,
			TargetUnion: &etcdserverpb.Compare_CreateRevision{CreateRevision: 0},
		}},
		Success: []*etcdserverpb.RequestOp{
			{Request: &etcdserverpb.RequestOp_RequestPut{
				RequestPut: &etcdserverpb.PutRequest{Key: key, Value: []byte("should-not-write")},
			}},
		},
		Failure: []*etcdserverpb.RequestOp{
			{Request: &etcdserverpb.RequestOp_RequestRange{
				RequestRange: &etcdserverpb.RangeRequest{Key: key},
			}},
		},
	})
	require.NoError(t, err)
	require.False(t, resp.Succeeded)
	require.Len(t, resp.Responses, 1)
	require.Equal(t, []byte("created"), resp.Responses[0].GetResponseRange().Kvs[0].Value)
}

func TestTxnCompareCreateRevisionNonZeroMatchesMetadata(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	ctx := context.Background()
	key := []byte("/registry/generic-txn/compare-create-nonzero")
	_, err := server.Put(ctx, &etcdserverpb.PutRequest{Key: key, Value: []byte("v1")})
	require.NoError(t, err)
	rangeResp, err := server.Range(ctx, &etcdserverpb.RangeRequest{Key: key})
	require.NoError(t, err)
	require.Len(t, rangeResp.Kvs, 1)
	createRevision := rangeResp.Kvs[0].CreateRevision

	resp, err := server.Txn(ctx, &etcdserverpb.TxnRequest{
		Compare: []*etcdserverpb.Compare{{
			Key:         key,
			Target:      etcdserverpb.Compare_CREATE,
			Result:      etcdserverpb.Compare_EQUAL,
			TargetUnion: &etcdserverpb.Compare_CreateRevision{CreateRevision: createRevision},
		}},
		Success: []*etcdserverpb.RequestOp{
			{Request: &etcdserverpb.RequestOp_RequestRange{
				RequestRange: &etcdserverpb.RangeRequest{Key: key},
			}},
		},
	})
	require.NoError(t, err)
	require.True(t, resp.Succeeded)
	require.Len(t, resp.Responses[0].GetResponseRange().Kvs, 1)

	resp, err = server.Txn(ctx, &etcdserverpb.TxnRequest{
		Compare: []*etcdserverpb.Compare{{
			Key:         key,
			Target:      etcdserverpb.Compare_CREATE,
			Result:      etcdserverpb.Compare_EQUAL,
			TargetUnion: &etcdserverpb.Compare_CreateRevision{CreateRevision: createRevision + 1},
		}},
		Success: []*etcdserverpb.RequestOp{
			{Request: &etcdserverpb.RequestOp_RequestPut{
				RequestPut: &etcdserverpb.PutRequest{Key: key, Value: []byte("should-not-write")},
			}},
		},
		Failure: []*etcdserverpb.RequestOp{
			{Request: &etcdserverpb.RequestOp_RequestRange{
				RequestRange: &etcdserverpb.RangeRequest{Key: key},
			}},
		},
	})
	require.NoError(t, err)
	require.False(t, resp.Succeeded)
	require.Equal(t, []byte("v1"), resp.Responses[0].GetResponseRange().Kvs[0].Value)
}

func TestTxnCompareVersionChecksExistence(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	ctx := context.Background()
	key := []byte("/registry/generic-txn/compare-version")
	_, err := server.Put(ctx, &etcdserverpb.PutRequest{Key: key, Value: []byte("v1")})
	require.NoError(t, err)

	rangeResp, err := server.Range(ctx, &etcdserverpb.RangeRequest{Key: key})
	require.NoError(t, err)
	require.Len(t, rangeResp.Kvs, 1)
	require.Equal(t, int64(1), rangeResp.Kvs[0].Version)

	resp, err := server.Txn(ctx, &etcdserverpb.TxnRequest{
		Compare: []*etcdserverpb.Compare{{
			Key:         key,
			Target:      etcdserverpb.Compare_VERSION,
			Result:      etcdserverpb.Compare_EQUAL,
			TargetUnion: &etcdserverpb.Compare_Version{Version: 1},
		}},
		Success: []*etcdserverpb.RequestOp{
			{Request: &etcdserverpb.RequestOp_RequestPut{
				RequestPut: &etcdserverpb.PutRequest{Key: key, Value: []byte("matched")},
			}},
		},
	})
	require.NoError(t, err)
	require.True(t, resp.Succeeded)

	rangeResp, err = server.Range(ctx, &etcdserverpb.RangeRequest{Key: key})
	require.NoError(t, err)
	require.Len(t, rangeResp.Kvs, 1)
	require.Equal(t, int64(2), rangeResp.Kvs[0].Version)

	resp, err = server.Txn(ctx, &etcdserverpb.TxnRequest{
		Compare: []*etcdserverpb.Compare{{
			Key:         key,
			Target:      etcdserverpb.Compare_VERSION,
			Result:      etcdserverpb.Compare_GREATER,
			TargetUnion: &etcdserverpb.Compare_Version{Version: 1},
		}},
		Success: []*etcdserverpb.RequestOp{
			{Request: &etcdserverpb.RequestOp_RequestPut{
				RequestPut: &etcdserverpb.PutRequest{Key: key, Value: []byte("should-not-write")},
			}},
		},
		Failure: []*etcdserverpb.RequestOp{
			{Request: &etcdserverpb.RequestOp_RequestRange{
				RequestRange: &etcdserverpb.RangeRequest{Key: key},
			}},
		},
	})
	require.NoError(t, err)
	require.True(t, resp.Succeeded)
}

func TestTxnCompareLeaseRunsSelectedBranch(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	ctx := context.Background()
	key := []byte("/registry/generic-txn/compare-lease")
	leaseResp, err := server.LeaseGrant(ctx, &etcdserverpb.LeaseGrantRequest{TTL: 30, ID: 67890})
	require.NoError(t, err)
	_, err = server.Put(ctx, &etcdserverpb.PutRequest{Key: key, Value: []byte("leased"), Lease: leaseResp.ID})
	require.NoError(t, err)

	resp, err := server.Txn(ctx, &etcdserverpb.TxnRequest{
		Compare: []*etcdserverpb.Compare{{
			Key:         key,
			Target:      etcdserverpb.Compare_LEASE,
			Result:      etcdserverpb.Compare_EQUAL,
			TargetUnion: &etcdserverpb.Compare_Lease{Lease: leaseResp.ID},
		}},
		Success: []*etcdserverpb.RequestOp{
			{Request: &etcdserverpb.RequestOp_RequestPut{
				RequestPut: &etcdserverpb.PutRequest{Key: key, Value: []byte("matched"), Lease: leaseResp.ID},
			}},
			{Request: &etcdserverpb.RequestOp_RequestRange{
				RequestRange: &etcdserverpb.RangeRequest{Key: key},
			}},
		},
	})
	require.NoError(t, err)
	require.True(t, resp.Succeeded)
	require.Len(t, resp.Responses, 2)
	require.Equal(t, []byte("matched"), resp.Responses[1].GetResponseRange().Kvs[0].Value)

	resp, err = server.Txn(ctx, &etcdserverpb.TxnRequest{
		Compare: []*etcdserverpb.Compare{{
			Key:         key,
			Target:      etcdserverpb.Compare_LEASE,
			Result:      etcdserverpb.Compare_EQUAL,
			TargetUnion: &etcdserverpb.Compare_Lease{Lease: leaseResp.ID + 1},
		}},
		Success: []*etcdserverpb.RequestOp{
			{Request: &etcdserverpb.RequestOp_RequestPut{
				RequestPut: &etcdserverpb.PutRequest{Key: key, Value: []byte("should-not-write")},
			}},
		},
		Failure: []*etcdserverpb.RequestOp{
			{Request: &etcdserverpb.RequestOp_RequestRange{
				RequestRange: &etcdserverpb.RangeRequest{Key: key},
			}},
		},
	})
	require.NoError(t, err)
	require.False(t, resp.Succeeded)
	require.Len(t, resp.Responses, 1)
	require.Equal(t, []byte("matched"), resp.Responses[0].GetResponseRange().Kvs[0].Value)
}

func TestTxnCompactRevisionCAS(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	ctx := context.Background()

	firstResp, err := server.Txn(ctx, compactTxn(0, "0"))
	require.NoError(t, err)
	require.True(t, firstResp.Succeeded)
	require.Len(t, firstResp.Responses, 1)
	require.NotNil(t, firstResp.Responses[0].GetResponsePut())

	staleResp, err := server.Txn(ctx, compactTxn(0, "10"))
	require.NoError(t, err)
	require.False(t, staleResp.Succeeded)
	require.Len(t, staleResp.Responses, 1)
	staleRange := staleResp.Responses[0].GetResponseRange()
	require.NotNil(t, staleRange)
	require.Len(t, staleRange.Kvs, 1)
	require.Equal(t, int64(1), staleRange.Kvs[0].Version)
	require.Equal(t, []byte("0"), staleRange.Kvs[0].Value)

	secondResp, err := server.Txn(ctx, compactTxn(1, "10"))
	require.NoError(t, err)
	require.True(t, secondResp.Succeeded)

	currentResp, err := server.Txn(ctx, compactTxn(1, "20"))
	require.NoError(t, err)
	require.False(t, currentResp.Succeeded)
	currentRange := currentResp.Responses[0].GetResponseRange()
	require.Len(t, currentRange.Kvs, 1)
	require.Equal(t, int64(2), currentRange.Kvs[0].Version)
	require.Equal(t, []byte("10"), currentRange.Kvs[0].Value)
}

// The leading-'/' watch-key gate (isPureWatchRequest) was removed in #78: etcd
// keys are arbitrary byte strings, and the gate silently rejected every watch
// from slash-less consumers (Cilium). Watchability of arbitrary keys is pinned
// end-to-end by TestCiliumConsumerCompatibility (pkg/endpoint).

// TestRangeAtMagicRevisionIsNotHijacked pins #53: a Range at revision 1888 (the
// former partition-magic value) must get normal etcd semantics, not be hijacked
// into returning partition metadata. Partition discovery uses the brain-protocol
// ListPartition RPC instead.
func TestRangeAtMagicRevisionIsNotHijacked(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()
	ctx := context.Background()

	// Put the current revision well above the magic value so the requested
	// revision 1888 is a valid (past) revision rather than a future one.
	server.backend.SetCurrentRevision(1_000_000)
	_, err := server.Put(ctx, &etcdserverpb.PutRequest{Key: []byte("/registry/a"), Value: []byte("v")})
	require.NoError(t, err)

	resp, err := server.Range(ctx, &etcdserverpb.RangeRequest{
		Key:      []byte("/registry/"),
		RangeEnd: []byte("/registry0"),
		Revision: 1888, // the former partition-magic value, now a plain revision
	})
	require.NoError(t, err)
	// Nothing existed as of revision 1888, so this is a normal empty range — not
	// partition boundary keys (which the old hijack would have returned).
	require.Empty(t, resp.Kvs)
	require.EqualValues(t, 0, resp.Count)
}

// TestTxnCompactRevisionConcurrentSingleWinner pins the #54/#72 fix: when
// several HA-apiserver compactors race the same version, the emulation must let
// exactly one win (an atomic version CAS), not several.
func TestTxnCompactRevisionConcurrentSingleWinner(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()
	ctx := context.Background()

	first, err := server.Txn(ctx, compactTxn(0, "0"))
	require.NoError(t, err)
	require.True(t, first.Succeeded) // create -> version 1

	const n = 16
	var wg sync.WaitGroup
	succeeded := make([]bool, n)
	errs := make([]error, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			r, e := server.Txn(ctx, compactTxn(1, "10"))
			errs[i] = e
			if e == nil {
				succeeded[i] = r.Succeeded
			}
		}(i)
	}
	wg.Wait()

	winners := 0
	for i := 0; i < n; i++ {
		require.NoError(t, errs[i])
		if succeeded[i] {
			winners++
		}
	}
	require.Equal(t, 1, winners, "exactly one concurrent compactor may win the version CAS")

	rr, err := server.Range(ctx, &etcdserverpb.RangeRequest{Key: []byte(compactRevKey)})
	require.NoError(t, err)
	require.Len(t, rr.Kvs, 1)
	require.Equal(t, int64(2), rr.Kvs[0].Version, "only one write must have applied over the seed")
}

func compactTxn(expectVersion int64, rev string) *etcdserverpb.TxnRequest {
	return &etcdserverpb.TxnRequest{
		Compare: []*etcdserverpb.Compare{{
			Key:         []byte(compactRevKey),
			Target:      etcdserverpb.Compare_VERSION,
			Result:      etcdserverpb.Compare_EQUAL,
			TargetUnion: &etcdserverpb.Compare_Version{Version: expectVersion},
		}},
		Success: []*etcdserverpb.RequestOp{{
			Request: &etcdserverpb.RequestOp_RequestPut{
				RequestPut: &etcdserverpb.PutRequest{Key: []byte(compactRevKey), Value: []byte(rev)},
			},
		}},
		Failure: []*etcdserverpb.RequestOp{{
			Request: &etcdserverpb.RequestOp_RequestRange{
				RequestRange: &etcdserverpb.RangeRequest{Key: []byte(compactRevKey)},
			},
		}},
	}
}
