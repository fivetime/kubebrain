// Copyright 2022 ByteDance and/or its affiliates
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
	"fmt"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/mvccpb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	"github.com/kubewharf/kubebrain/pkg/backend"
	"github.com/kubewharf/kubebrain/pkg/metrics/mock"
	"github.com/kubewharf/kubebrain/pkg/storage/memkv"
)

// fakeRangeStreamServer captures the chunks a RangeStream handler sends. It
// embeds the generated server-stream interface so the unexercised grpc.ServerStream
// methods are present; the handler only calls Send and Context.
type fakeRangeStreamServer struct {
	etcdserverpb.KV_RangeStreamServer
	ctx  context.Context
	sent []*etcdserverpb.RangeStreamResponse
}

type listCountingBackendShim struct {
	BackendShim
	listCalls int
}

type prematureRangeStreamBackendShim struct {
	BackendShim
}

func (b *prematureRangeStreamBackendShim) RangeStreamChan(
	context.Context, []byte, []byte, uint64,
) (<-chan rangeStreamChunk, error) {
	ch := make(chan rangeStreamChunk, 1)
	ch <- rangeStreamChunk{resp: &etcdserverpb.RangeResponse{
		Header: txnHeader(7),
		Kvs:    []*mvccpb.KeyValue{{Key: []byte("/premature/key"), Value: []byte("value")}},
	}}
	close(ch)
	return ch, nil
}

func (b *listCountingBackendShim) List(ctx context.Context, req *etcdserverpb.RangeRequest) (*etcdserverpb.RangeResponse, error) {
	b.listCalls++
	return b.BackendShim.List(ctx, req)
}

func (s *fakeRangeStreamServer) Send(resp *etcdserverpb.RangeStreamResponse) error {
	s.sent = append(s.sent, resp)
	return nil
}

func (s *fakeRangeStreamServer) Context() context.Context {
	if s.ctx != nil {
		return s.ctx
	}
	return context.Background()
}

// newRangeStreamTestServer builds a leader RPCServer over memkv.
func newRangeStreamTestServer(t *testing.T) (*RPCServer, func()) {
	t.Helper()
	ctrl := gomock.NewController(t)
	m := mock.NewMinimalMetrics(ctrl)
	kv := memkv.NewKvStorage()
	backendStore := backend.NewBackend(kv, backend.Config{
		Identity:                "test-peer",
		EnableEtcdCompatibility: true,
	}, m)
	server := New(backendStore, m, testPeerService{isLeader: true})
	cleanup := func() {
		server.stopLeases()
		require.NoError(t, kv.Close())
		ctrl.Finish()
	}
	return server, cleanup
}

func TestRangeStreamStreamsAllKeys(t *testing.T) {
	server, cleanup := newRangeStreamTestServer(t)
	defer cleanup()
	ctx := context.Background()

	const n = 25
	want := map[string]string{}
	for i := 0; i < n; i++ {
		key := fmt.Sprintf("/registry/pods/%03d", i)
		val := fmt.Sprintf("v-%03d", i)
		_, err := server.Put(ctx, &etcdserverpb.PutRequest{Key: []byte(key), Value: []byte(val)})
		require.NoError(t, err)
		want[key] = val
	}

	rs := &fakeRangeStreamServer{ctx: ctx}
	err := server.RangeStream(&etcdserverpb.RangeRequest{
		Key:      []byte("/registry/pods/"),
		RangeEnd: []byte("/registry/pods0"), // prefix end of "/registry/pods/"
	}, rs)
	require.NoError(t, err)
	require.NotEmpty(t, rs.sent, "must send at least the terminal header chunk")

	// Kvs concatenate to the full disjoint set; only the final chunk carries the
	// pinned revision and merged response metadata.
	got := map[string]string{}
	for i, chunk := range rs.sent {
		require.NotNil(t, chunk.RangeResponse)
		if i != len(rs.sent)-1 {
			require.Nil(t, chunk.RangeResponse.Header)
			require.Zero(t, chunk.RangeResponse.Count)
			require.False(t, chunk.RangeResponse.More)
		}
		for _, kv := range chunk.RangeResponse.Kvs {
			_, dup := got[string(kv.Key)]
			require.False(t, dup, "key %s appeared in more than one chunk (chunks must be disjoint)", kv.Key)
			got[string(kv.Key)] = string(kv.Value)
		}
	}
	last := rs.sent[len(rs.sent)-1]
	require.Empty(t, last.RangeResponse.Kvs, "final chunk must be header-only")
	require.NotNil(t, last.RangeResponse.Header)
	require.Greater(t, last.RangeResponse.Header.Revision, int64(0))
	require.EqualValues(t, n, last.RangeResponse.Count)
	for _, chunk := range rs.sent {
		require.False(t, chunk.RangeResponse.More, "unlimited RangeStream must not expose scanner chunking as Range.More")
	}
	require.Equal(t, want, got, "concatenated chunks must equal the full key set")
}

func TestRangeStreamEmptyRangeStillSendsHeaderRevision(t *testing.T) {
	server, cleanup := newRangeStreamTestServer(t)
	defer cleanup()
	ctx := context.Background()

	// One unrelated key so the store has a non-zero revision, then stream an
	// empty range.
	_, err := server.Put(ctx, &etcdserverpb.PutRequest{Key: []byte("/registry/other/x"), Value: []byte("v")})
	require.NoError(t, err)

	rs := &fakeRangeStreamServer{ctx: ctx}
	err = server.RangeStream(&etcdserverpb.RangeRequest{
		Key:      []byte("/registry/pods/"),
		RangeEnd: []byte("/registry/pods0"),
	}, rs)
	require.NoError(t, err)
	require.NotEmpty(t, rs.sent, "empty range must still emit a header-only chunk")
	last := rs.sent[len(rs.sent)-1]
	require.Empty(t, last.RangeResponse.Kvs)
	require.Greater(t, last.RangeResponse.Header.Revision, int64(0),
		"apiserver reads Header.Revision as the sync's initial revision")
}

func TestSerializableRangeStreamBypassesLeaderRevisionSync(t *testing.T) {
	server, cleanup := newRangeStreamTestServer(t)
	defer cleanup()
	ctx := context.Background()
	_, err := server.Put(ctx, &etcdserverpb.PutRequest{Key: []byte("/stream/key"), Value: []byte("value")})
	require.NoError(t, err)

	syncErr := errors.New("leader revision unavailable")
	server.peers = testPeerService{syncReadFn: func(context.Context) error { return syncErr }}
	stream := &fakeRangeStreamServer{ctx: ctx}
	require.NoError(t, server.RangeStream(&etcdserverpb.RangeRequest{
		Key: []byte("/stream/"), RangeEnd: []byte("/stream0"), Serializable: true,
	}, stream))
	require.NotEmpty(t, stream.sent)

	linearizable := &fakeRangeStreamServer{ctx: ctx}
	err = server.RangeStream(&etcdserverpb.RangeRequest{Key: []byte("/stream/"), RangeEnd: []byte("/stream0")}, linearizable)
	require.Error(t, err)
	require.Equal(t, codes.Unavailable, status.Code(err))
}

func TestHistoricalRangeStreamUsesDurableFollowerWatermark(t *testing.T) {
	server, cleanup := newRangeStreamTestServer(t)
	defer cleanup()
	ctx := context.Background()
	key := []byte("/stream-history/key")
	first, err := server.Put(ctx, &etcdserverpb.PutRequest{Key: key, Value: []byte("v1")})
	require.NoError(t, err)
	_, err = server.Put(ctx, &etcdserverpb.PutRequest{Key: key, Value: []byte("v2")})
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		rev, getErr := server.backend.GetDurableRevision(ctx)
		return getErr == nil && rev > uint64(first.Header.Revision)
	}, time.Second, time.Millisecond)
	server.peers = testPeerService{isLeader: false, syncReadFn: func(context.Context) error {
		t.Fatal("bounded historical RangeStream must not sync with the leader")
		return nil
	}}

	rs := &fakeRangeStreamServer{ctx: ctx}
	err = server.RangeStream(&etcdserverpb.RangeRequest{
		Key: []byte("/stream-history/"), RangeEnd: []byte("/stream-history0"),
		Revision: first.Header.Revision, Serializable: true,
	}, rs)
	require.NoError(t, err)
	require.NotEmpty(t, rs.sent)
	var values [][]byte
	for _, chunk := range rs.sent {
		for _, kv := range chunk.RangeResponse.Kvs {
			values = append(values, kv.Value)
		}
	}
	require.Equal(t, [][]byte{[]byte("v1")}, values)
}

func TestRangeStreamRejectsUnsupportedShapes(t *testing.T) {
	server, cleanup := newRangeStreamTestServer(t)
	defer cleanup()
	ctx := context.Background()

	cases := []struct {
		name string
		req  *etcdserverpb.RangeRequest
	}{
		{"modRevisionFilter", &etcdserverpb.RangeRequest{Key: []byte("/a"), RangeEnd: []byte("/b"), MinModRevision: 5}},
		{"sortOrder", &etcdserverpb.RangeRequest{Key: []byte("/a"), RangeEnd: []byte("/b"), SortOrder: etcdserverpb.RangeRequest_DESCEND, SortTarget: etcdserverpb.RangeRequest_KEY}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			rs := &fakeRangeStreamServer{ctx: ctx}
			err := server.RangeStream(c.req, rs)
			require.Error(t, err)
			require.Equal(t, codes.Unimplemented, status.Code(err))
			require.Empty(t, rs.sent, "no chunks on a rejected request")
		})
	}
}

func TestRangeStreamSupportedOptionsMatchUnaryRange(t *testing.T) {
	server, cleanup := newRangeStreamTestServer(t)
	defer cleanup()
	ctx := context.Background()
	for i := 0; i < 8; i++ {
		_, err := server.Put(ctx, &etcdserverpb.PutRequest{
			Key: []byte(fmt.Sprintf("/options/%02d", i)), Value: []byte(fmt.Sprintf("value-%02d", i)),
		})
		require.NoError(t, err)
	}

	tests := []struct {
		name string
		req  *etcdserverpb.RangeRequest
	}{
		{name: "count only ignores limit", req: &etcdserverpb.RangeRequest{Key: []byte("/options/"), RangeEnd: []byte("/options0"), CountOnly: true, Limit: 1}},
		{name: "limit", req: &etcdserverpb.RangeRequest{Key: []byte("/options/"), RangeEnd: []byte("/options0"), Limit: 3}},
		{name: "explicit ascending key", req: &etcdserverpb.RangeRequest{Key: []byte("/options/"), RangeEnd: []byte("/options0"), Limit: 3, SortOrder: etcdserverpb.RangeRequest_ASCEND, SortTarget: etcdserverpb.RangeRequest_KEY}},
		{name: "keys only", req: &etcdserverpb.RangeRequest{Key: []byte("/options/"), RangeEnd: []byte("/options0"), KeysOnly: true}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			unary, err := server.Range(ctx, proto.Clone(tt.req).(*etcdserverpb.RangeRequest))
			require.NoError(t, err)
			rs := &fakeRangeStreamServer{ctx: ctx}
			require.NoError(t, server.RangeStream(proto.Clone(tt.req).(*etcdserverpb.RangeRequest), rs))

			merged := &etcdserverpb.RangeResponse{}
			for _, chunk := range rs.sent {
				proto.Merge(merged, chunk.RangeResponse)
			}
			require.True(t, proto.Equal(unary, merged), "stream=%s unary=%s", merged, unary)
		})
	}
}

// TestRangeStreamMatchesUnaryRange asserts the streamed result is byte-identical
// to a unary Range over the same window.
func TestRangeStreamMatchesUnaryRange(t *testing.T) {
	server, cleanup := newRangeStreamTestServer(t)
	defer cleanup()
	ctx := context.Background()

	for i := 0; i < 10; i++ {
		_, err := server.Put(ctx, &etcdserverpb.PutRequest{
			Key: []byte(fmt.Sprintf("/registry/svc/%02d", i)), Value: []byte(fmt.Sprintf("s%02d", i))})
		require.NoError(t, err)
	}

	unary, err := server.Range(ctx, &etcdserverpb.RangeRequest{
		Key: []byte("/registry/svc/"), RangeEnd: []byte("/registry/svc0")})
	require.NoError(t, err)

	rs := &fakeRangeStreamServer{ctx: ctx}
	require.NoError(t, server.RangeStream(&etcdserverpb.RangeRequest{
		Key: []byte("/registry/svc/"), RangeEnd: []byte("/registry/svc0")}, rs))

	streamed := make([]*mvccpb.KeyValue, 0, len(unary.Kvs))
	for _, chunk := range rs.sent {
		streamed = append(streamed, chunk.RangeResponse.Kvs...)
	}
	require.Len(t, streamed, len(unary.Kvs))
	for i := range unary.Kvs {
		require.Equal(t, unary.Kvs[i].Key, streamed[i].Key, "kv %d key", i)
		require.Equal(t, unary.Kvs[i].Value, streamed[i].Value, "kv %d value", i)
		require.Equal(t, unary.Kvs[i].ModRevision, streamed[i].ModRevision, "kv %d modRevision", i)
	}
}

func TestRangeStreamChunksRespectConfiguredMessageTarget(t *testing.T) {
	server, cleanup := newRangeStreamTestServer(t)
	defer cleanup()
	server.SetRequestLimits(defaultMaxTxnOps, 256)
	ctx := context.Background()

	for i := 0; i < 12; i++ {
		_, err := server.Put(ctx, &etcdserverpb.PutRequest{
			Key:   []byte(fmt.Sprintf("/chunk-target/%02d", i)),
			Value: []byte(fmt.Sprintf("value-%02d-%090d", i, i)),
		})
		require.NoError(t, err)
	}

	rs := &fakeRangeStreamServer{ctx: ctx}
	require.NoError(t, server.RangeStream(&etcdserverpb.RangeRequest{
		Key: []byte("/chunk-target/"), RangeEnd: []byte("/chunk-target0"),
	}, rs))

	var keys [][]byte
	for _, chunk := range rs.sent {
		require.LessOrEqual(t, proto.Size(chunk), 256,
			"multi-KV RangeStream chunks must honor the configured wire-size target")
		for _, kv := range chunk.RangeResponse.Kvs {
			keys = append(keys, kv.Key)
		}
	}
	require.Len(t, keys, 12)
	for i, key := range keys {
		require.Equal(t, fmt.Sprintf("/chunk-target/%02d", i), string(key))
	}

	tracked := &listCountingBackendShim{BackendShim: server.backend}
	server.backend = tracked
	limited := &fakeRangeStreamServer{ctx: ctx}
	require.NoError(t, server.RangeStream(&etcdserverpb.RangeRequest{
		Key: []byte("/chunk-target/"), RangeEnd: []byte("/chunk-target0"), Limit: 5,
	}, limited))
	require.Greater(t, len(limited.sent), 1,
		"a bounded RangeStream must not collapse a large result into one message")
	var limitedKeys int
	for i, chunk := range limited.sent {
		require.LessOrEqual(t, proto.Size(chunk), 256)
		if i != len(limited.sent)-1 {
			require.Nil(t, chunk.RangeResponse.Header)
			require.Zero(t, chunk.RangeResponse.Count)
			require.False(t, chunk.RangeResponse.More)
		}
		limitedKeys += len(chunk.RangeResponse.Kvs)
	}
	require.NotNil(t, limited.sent[len(limited.sent)-1].RangeResponse.Header)
	require.EqualValues(t, 12, limited.sent[len(limited.sent)-1].RangeResponse.Count)
	require.True(t, limited.sent[len(limited.sent)-1].RangeResponse.More)
	require.Equal(t, 5, limitedKeys)
	require.Zero(t, tracked.listCalls,
		"bounded RangeStream must not materialize a unary List response")
}

func TestRangeStreamRejectsPrematureBackendClose(t *testing.T) {
	server, cleanup := newRangeStreamTestServer(t)
	defer cleanup()
	server.backend = &prematureRangeStreamBackendShim{BackendShim: server.backend}

	stream := &fakeRangeStreamServer{ctx: context.Background()}
	err := server.RangeStream(&etcdserverpb.RangeRequest{
		Key: []byte("/premature/"), RangeEnd: []byte("/premature0"),
	}, stream)
	require.Equal(t, codes.Unavailable, status.Code(err))
	require.Equal(t, "range stream ended without terminal metadata", status.Convert(err).Message())
	require.Len(t, stream.sent, 1,
		"the partial chunk may already be on the wire, but the terminal status must invalidate it")
}

// TestWatchNegativeStartRevisionCanceledInStream pins the black-magic retirement: a
// negative StartRevision used to overload Watch into a range stream (watcher.List).
// That is now the native KV.RangeStream RPC. etcd rejects the create in-band
// while leaving the multiplexed stream available for later watch requests.
func TestWatchNegativeStartRevisionCanceledInStream(t *testing.T) {
	server, cleanup := newRangeStreamTestServer(t)
	defer cleanup()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ws := &controllableWatchServer{ctx: ctx, recv: make(chan *etcdserverpb.WatchRequest, 1)}
	ws.recv <- &etcdserverpb.WatchRequest{
		RequestUnion: &etcdserverpb.WatchRequest_CreateRequest{
			CreateRequest: &etcdserverpb.WatchCreateRequest{
				Key:           []byte("/registry/pods/"),
				RangeEnd:      []byte("/registry/pods0"),
				StartRevision: -1,
			},
		},
	}
	done := make(chan error, 1)
	go func() { done <- server.Watch(ws) }()
	require.Eventually(t, func() bool { return len(ws.snapshot()) == 1 }, time.Second, 10*time.Millisecond)
	resp := ws.snapshot()[0]
	require.Equal(t, int64(-1), resp.WatchId)
	require.True(t, resp.Created)
	require.True(t, resp.Canceled)
	require.NotEmpty(t, resp.CancelReason)

	cancel()
	require.Error(t, <-done)
}
