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
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/mvccpb"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	gproto "google.golang.org/protobuf/proto"

	proto "github.com/kubewharf/kubebrain-client/api/v2rpc"
	"github.com/kubewharf/kubebrain/pkg/server/service/etcdproxy"
	"github.com/kubewharf/kubebrain/pkg/util"
)

type fakeWatchServer struct {
	etcdserverpb.Watch_WatchServer
	ctx  context.Context
	sent []*etcdserverpb.WatchResponse
}

func (s *fakeWatchServer) Send(resp *etcdserverpb.WatchResponse) error {
	s.sent = append(s.sent, resp)
	return nil
}

func (s *fakeWatchServer) Recv() (*etcdserverpb.WatchRequest, error) {
	return nil, context.Canceled
}

func (s *fakeWatchServer) SetHeader(metadata.MD) error {
	return nil
}

func (s *fakeWatchServer) SendHeader(metadata.MD) error {
	return nil
}

func (s *fakeWatchServer) SetTrailer(metadata.MD) {
}

func (s *fakeWatchServer) Context() context.Context {
	if s.ctx != nil {
		return s.ctx
	}
	return context.Background()
}

func (s *fakeWatchServer) SendMsg(interface{}) error {
	return nil
}

func (s *fakeWatchServer) RecvMsg(interface{}) error {
	return context.Canceled
}

func TestIsExpectedWatchCloseError(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{name: "nil", err: nil, want: false},
		{name: "context canceled", err: context.Canceled, want: true},
		{name: "wrapped context canceled", err: errors.Join(errors.New("recv failed"), context.Canceled), want: true},
		{name: "grpc canceled", err: status.Error(codes.Canceled, "context canceled"), want: true},
		{name: "transport closing", err: status.Error(codes.Unavailable, "transport is closing"), want: true},
		{name: "internal", err: status.Error(codes.Internal, "backend watch failed"), want: false},
		{name: "plain error", err: errors.New("backend watch failed"), want: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := isExpectedWatchCloseError(tt.err); got != tt.want {
				t.Fatalf("isExpectedWatchCloseError(%v) = %v, want %v", tt.err, got, tt.want)
			}
		})
	}
}

func TestFilterWatchEventsMatchesEtcdFilters(t *testing.T) {
	events := []*mvccpb.Event{
		{Type: mvccpb.PUT, Kv: &mvccpb.KeyValue{Key: []byte("put")}},
		{Type: mvccpb.DELETE, Kv: &mvccpb.KeyValue{Key: []byte("delete")}},
	}

	tests := []struct {
		name    string
		filters []etcdserverpb.WatchCreateRequest_FilterType
		want    []mvccpb.Event_EventType
	}{
		{
			name: "none",
			want: []mvccpb.Event_EventType{mvccpb.PUT, mvccpb.DELETE},
		},
		{
			name:    "no put",
			filters: []etcdserverpb.WatchCreateRequest_FilterType{etcdserverpb.WatchCreateRequest_NOPUT},
			want:    []mvccpb.Event_EventType{mvccpb.DELETE},
		},
		{
			name:    "no delete",
			filters: []etcdserverpb.WatchCreateRequest_FilterType{etcdserverpb.WatchCreateRequest_NODELETE},
			want:    []mvccpb.Event_EventType{mvccpb.PUT},
		},
		{
			name: "no put and no delete",
			filters: []etcdserverpb.WatchCreateRequest_FilterType{
				etcdserverpb.WatchCreateRequest_NOPUT,
				etcdserverpb.WatchCreateRequest_NODELETE,
			},
			want: nil,
		},
		{
			name:    "unknown ignored",
			filters: []etcdserverpb.WatchCreateRequest_FilterType{etcdserverpb.WatchCreateRequest_FilterType(99)},
			want:    []mvccpb.Event_EventType{mvccpb.PUT, mvccpb.DELETE},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := filterWatchEvents(append([]*mvccpb.Event(nil), events...), tt.filters)
			if len(got) != len(tt.want) {
				t.Fatalf("filtered events length = %d, want %d", len(got), len(tt.want))
			}
			for i, event := range got {
				if event.Type != tt.want[i] {
					t.Fatalf("filtered event %d type = %v, want %v", i, event.Type, tt.want[i])
				}
			}
		})
	}
}

func TestFilterWatchEventsByRange(t *testing.T) {
	events := []*mvccpb.Event{
		{Type: mvccpb.PUT, Kv: &mvccpb.KeyValue{Key: []byte("/registry/pods/a")}},
		{Type: mvccpb.PUT, Kv: &mvccpb.KeyValue{Key: []byte("/registry/pods/a/child")}},
		{Type: mvccpb.PUT, Kv: &mvccpb.KeyValue{Key: []byte("/registry/pods/b")}},
		{Type: mvccpb.PUT, Kv: &mvccpb.KeyValue{Key: []byte("/registry/services/a")}},
		nil,
	}

	exact := filterWatchEventsByRange(append([]*mvccpb.Event(nil), events...), []byte("/registry/pods/a"), nil)
	require.Len(t, exact, 1)
	require.Equal(t, []byte("/registry/pods/a"), exact[0].Kv.Key)

	prefix := filterWatchEventsByRange(append([]*mvccpb.Event(nil), events...), []byte("/registry/pods/"), []byte("/registry/pods0"))
	require.Len(t, prefix, 3)
	require.Equal(t, []byte("/registry/pods/a"), prefix[0].Kv.Key)
	require.Equal(t, []byte("/registry/pods/a/child"), prefix[1].Kv.Key)
	require.Equal(t, []byte("/registry/pods/b"), prefix[2].Kv.Key)

	openEnded := filterWatchEventsByRange(append([]*mvccpb.Event(nil), events...), []byte("/registry/pods/b"), []byte{})
	require.Len(t, openEnded, 2)
	require.Equal(t, []byte("/registry/pods/b"), openEnded[0].Kv.Key)
	require.Equal(t, []byte("/registry/services/a"), openEnded[1].Kv.Key)
}

func TestWatchBackendPrefix(t *testing.T) {
	require.Equal(t, "/registry/pods/a", watchBackendPrefix([]byte("/registry/pods/a"), nil))
	require.Equal(t, "/registry/pods/", watchBackendPrefix([]byte("/registry/pods/"), []byte("/registry/pods0")))
	require.Equal(t, "", watchBackendPrefix([]byte("/registry/pods/a"), []byte("/registry/pods/c")))
	require.Equal(t, "", watchBackendPrefix([]byte("/registry/pods/a"), []byte{}))
}

func TestWithoutWatchPrevKvsDoesNotMutateSharedEvents(t *testing.T) {
	events := []*mvccpb.Event{
		{Type: mvccpb.PUT, PrevKv: &mvccpb.KeyValue{Key: []byte("a")}},
		nil,
		{Type: mvccpb.DELETE, PrevKv: &mvccpb.KeyValue{Key: []byte("b")}},
	}
	withoutPrev := withoutWatchPrevKvs(events)
	if withoutPrev[0].PrevKv != nil || withoutPrev[2].PrevKv != nil {
		t.Fatalf("expected prev kvs to be cleared")
	}
	if events[0].PrevKv == nil || events[2].PrevKv == nil {
		t.Fatalf("expected source events to keep prev kvs")
	}
}

func TestNormalizeWatchCreateRequestMatchesEtcd(t *testing.T) {
	t.Run("empty key becomes minimum key", func(t *testing.T) {
		req := normalizeWatchCreateRequest(&etcdserverpb.WatchCreateRequest{})
		if string(req.Key) != "\x00" {
			t.Fatalf("key = %q, want minimum key", req.Key)
		}
		if req.RangeEnd != nil {
			t.Fatalf("range end = %v, want nil", req.RangeEnd)
		}
	})

	t.Run("nil range end remains nil", func(t *testing.T) {
		req := normalizeWatchCreateRequest(&etcdserverpb.WatchCreateRequest{Key: []byte("/registry")})
		if req.RangeEnd != nil {
			t.Fatalf("range end = %v, want nil", req.RangeEnd)
		}
	})

	t.Run("from key range end becomes open ended", func(t *testing.T) {
		req := normalizeWatchCreateRequest(&etcdserverpb.WatchCreateRequest{
			Key:      []byte("/registry"),
			RangeEnd: []byte{0},
		})
		if req.RangeEnd == nil {
			t.Fatalf("range end = nil, want non-nil open-ended marker")
		}
		if len(req.RangeEnd) != 0 {
			t.Fatalf("range end length = %d, want 0", len(req.RangeEnd))
		}
	})
}

func TestWatchRequestedIDDuplicateAndUnknownCancelMatchEtcd(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()
	stream := &fakeWatchServer{ctx: context.Background()}
	w := &watcher{
		backend: server.backend, watchServer: stream, grpcServer: server,
		watches: make(map[int64]*watch), metricCli: server.metricCli,
	}

	w.Start(context.Background(), &etcdserverpb.WatchCreateRequest{Key: []byte("/watch/id"), WatchId: 42})
	require.NotEmpty(t, stream.sent)
	require.True(t, stream.sent[0].Created)
	require.Equal(t, int64(42), stream.sent[0].WatchId)

	w.Start(context.Background(), &etcdserverpb.WatchCreateRequest{Key: []byte("/watch/duplicate"), WatchId: 42})
	require.Len(t, stream.sent, 2)
	require.True(t, stream.sent[1].Created)
	require.True(t, stream.sent[1].Canceled)
	require.Equal(t, int64(-1), stream.sent[1].WatchId)
	require.Equal(t, "mvcc: duplicate watch ID provided on the WatchStream", stream.sent[1].CancelReason)

	w.CancelRequest(999)
	require.Len(t, stream.sent, 2, "etcd silently ignores cancellation of an unknown watch ID")
	w.CancelRequest(42)
	w.wg.Wait()
}

func TestNegativeWatchRevisionCancelsCreateWithoutClosingStream(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()
	stream := &scriptedWatchServer{
		fakeWatchServer: &fakeWatchServer{ctx: context.Background()},
		reqs: []*etcdserverpb.WatchRequest{
			{RequestUnion: &etcdserverpb.WatchRequest_CreateRequest{CreateRequest: &etcdserverpb.WatchCreateRequest{Key: []byte("/watch/negative"), StartRevision: -1}}},
			{RequestUnion: &etcdserverpb.WatchRequest_CreateRequest{CreateRequest: &etcdserverpb.WatchCreateRequest{Key: []byte("/watch/next"), WatchId: 71}}},
		},
	}
	require.ErrorIs(t, server.Watch(stream), context.Canceled)
	require.GreaterOrEqual(t, len(stream.sent), 2)
	require.True(t, stream.sent[0].Created)
	require.True(t, stream.sent[0].Canceled)
	require.Equal(t, int64(-1), stream.sent[0].WatchId)
	require.Equal(t, rpctypes.ErrCompacted.Error(), stream.sent[0].CancelReason)
	require.True(t, stream.sent[1].Created)
	require.False(t, stream.sent[1].Canceled)
	require.Equal(t, int64(71), stream.sent[1].WatchId)
}

func TestSendWatchFragmentsMatchesEtcdFlags(t *testing.T) {
	response := &etcdserverpb.WatchResponse{
		Header:  &etcdserverpb.ResponseHeader{Revision: 12},
		WatchId: 7,
		Events: []*mvccpb.Event{
			{Kv: &mvccpb.KeyValue{Key: []byte("a"), Value: make([]byte, 80)}},
			{Kv: &mvccpb.KeyValue{Key: []byte("b"), Value: make([]byte, 80)}},
			{Kv: &mvccpb.KeyValue{Key: []byte("c"), Value: make([]byte, 80)}},
		},
	}
	var fragments []*etcdserverpb.WatchResponse
	require.NoError(t, sendWatchFragments(response, 140, func(resp *etcdserverpb.WatchResponse) error {
		fragments = append(fragments, gproto.Clone(resp).(*etcdserverpb.WatchResponse))
		return nil
	}))
	require.Greater(t, len(fragments), 1)
	var keys [][]byte
	for i, fragment := range fragments {
		require.Equal(t, int64(7), fragment.WatchId)
		require.Equal(t, int64(12), fragment.Header.Revision)
		require.Equal(t, i < len(fragments)-1, fragment.Fragment)
		for _, event := range fragment.Events {
			keys = append(keys, event.Kv.Key)
		}
	}
	require.Equal(t, [][]byte{[]byte("a"), []byte("b"), []byte("c")}, keys)
}

func TestCancelCompactedWatchResponseUsesBackendCompactRevisionAndErrorReason(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	ctx := context.Background()
	putResp, err := server.Put(ctx, &etcdserverpb.PutRequest{
		Key:   []byte("/registry/watch/compact"),
		Value: []byte("v1"),
	})
	if err != nil {
		t.Fatalf("put: %v", err)
	}
	require.Eventually(t, func() bool {
		return server.backend.GetCurrentRevision() >= uint64(putResp.Header.Revision)
	}, time.Second, time.Millisecond)
	if _, err := server.Compact(ctx, &etcdserverpb.CompactionRequest{Revision: putResp.Header.Revision}); err != nil {
		t.Fatalf("compact: %v", err)
	}

	stream := &fakeWatchServer{ctx: ctx}
	cancelCalled := false
	w := &watcher{
		backend:     server.backend,
		watchServer: stream,
		watches: map[int64]*watch{
			7: {cancel: func() { cancelCalled = true }, start: "/registry/watch/compact"},
		},
		metricCli: server.metricCli,
	}
	w.Cancel(7, compactedRevisionError(), true)

	if !cancelCalled {
		t.Fatalf("expected watcher context cancel to be called")
	}
	if len(stream.sent) != 1 {
		t.Fatalf("sent responses = %d, want 1", len(stream.sent))
	}
	resp := stream.sent[0]
	if !resp.Canceled {
		t.Fatalf("canceled = false, want true")
	}
	if resp.CompactRevision != putResp.Header.Revision {
		t.Fatalf("compact revision = %d, want %d", resp.CompactRevision, putResp.Header.Revision)
	}
	if resp.CancelReason != compactedRevisionError().Error() {
		t.Fatalf("cancel reason = %q, want %q", resp.CancelReason, compactedRevisionError().Error())
	}
}

func TestFollowerProxyWatchCloseIsNonCompactedCancel(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	proxyCh := make(chan etcdproxy.WatchResult)
	close(proxyCh)
	server.peers = testPeerService{
		isLeader:     false,
		proxyEnabled: true,
		watchFn: func(_ context.Context, key, rangeEnd []byte, revision uint64) (<-chan etcdproxy.WatchResult, error) {
			require.Equal(t, []byte("/registry/watch/proxy/"), key)
			require.Equal(t, []byte("/registry/watch/proxy0"), rangeEnd)
			require.Equal(t, uint64(10), revision)
			return proxyCh, nil
		},
	}

	stream := &fakeWatchServer{ctx: context.Background()}
	w := &watcher{
		backend:     server.backend,
		watchServer: stream,
		grpcServer:  server,
		watches: map[int64]*watch{
			7: {start: "/registry/watch/proxy/", end: "/registry/watch/proxy0"},
		},
		metricCli: server.metricCli,
	}
	w.wg.Add(1)
	w.Watch(context.Background(), 7, &etcdserverpb.WatchCreateRequest{
		Key:           []byte("/registry/watch/proxy/"),
		RangeEnd:      []byte("/registry/watch/proxy0"),
		StartRevision: 10,
	})

	require.Len(t, stream.sent, 1)
	resp := stream.sent[0]
	require.True(t, resp.Canceled)
	require.Equal(t, int64(7), resp.WatchId)
	require.Equal(t, int64(0), resp.CompactRevision)
	require.Equal(t, "watch closed", resp.CancelReason)
}

func TestFollowerProxyWatchCreateUnavailableIsNonCompactedCancel(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	watchErr := status.Error(codes.Unavailable, "leader is not ready")
	server.peers = testPeerService{
		isLeader:     false,
		proxyEnabled: true,
		watchFn: func(context.Context, []byte, []byte, uint64) (<-chan etcdproxy.WatchResult, error) {
			return nil, watchErr
		},
	}

	stream := &fakeWatchServer{ctx: context.Background()}
	w := &watcher{
		backend:     server.backend,
		watchServer: stream,
		grpcServer:  server,
		watches: map[int64]*watch{
			7: {start: "/registry/watch/proxy-unavailable"},
		},
		metricCli: server.metricCli,
	}
	w.wg.Add(1)
	w.Watch(context.Background(), 7, &etcdserverpb.WatchCreateRequest{
		Key:           []byte("/registry/watch/proxy-unavailable"),
		StartRevision: 10,
	})

	require.Len(t, stream.sent, 1)
	resp := stream.sent[0]
	require.True(t, resp.Canceled)
	require.Equal(t, int64(7), resp.WatchId)
	require.Equal(t, int64(0), resp.CompactRevision)
	require.Equal(t, watchErr.Error(), resp.CancelReason)
}

func TestFollowerProxyWatchCompactedErrorIsForwarded(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	ctx := context.Background()
	putResp, err := server.Put(ctx, &etcdserverpb.PutRequest{
		Key:   []byte("/registry/watch/proxy-compact"),
		Value: []byte("v1"),
	})
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		return server.backend.GetCurrentRevision() >= uint64(putResp.Header.Revision)
	}, time.Second, time.Millisecond)
	_, err = server.Compact(ctx, &etcdserverpb.CompactionRequest{Revision: putResp.Header.Revision})
	require.NoError(t, err)

	proxyCh := make(chan etcdproxy.WatchResult, 1)
	proxyCh <- etcdproxy.WatchResult{Err: compactedRevisionError()}
	close(proxyCh)
	server.peers = testPeerService{
		isLeader:     false,
		proxyEnabled: true,
		watchFn: func(context.Context, []byte, []byte, uint64) (<-chan etcdproxy.WatchResult, error) {
			return proxyCh, nil
		},
	}

	stream := &fakeWatchServer{ctx: ctx}
	w := &watcher{
		backend:     server.backend,
		watchServer: stream,
		grpcServer:  server,
		watches: map[int64]*watch{
			7: {start: "/registry/watch/proxy-compact"},
		},
		metricCli: server.metricCli,
	}
	w.wg.Add(1)
	w.Watch(ctx, 7, &etcdserverpb.WatchCreateRequest{
		Key:           []byte("/registry/watch/proxy-compact"),
		StartRevision: putResp.Header.Revision,
	})

	require.Len(t, stream.sent, 1)
	resp := stream.sent[0]
	require.True(t, resp.Canceled)
	require.Equal(t, int64(7), resp.WatchId)
	require.Equal(t, putResp.Header.Revision, resp.CompactRevision)
	require.Equal(t, compactedRevisionError().Error(), resp.CancelReason)
}

func TestIsWatchCompactedError(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{name: "nil", want: false},
		{name: "grpc out of range", err: status.Error(codes.OutOfRange, "etcdserver: mvcc: required revision has been compacted"), want: true},
		{name: "backend compacted string", err: errors.New("cache event oldest revision is compacted at 10 newer than requested revision 9"), want: true},
		{name: "backend cache too old string", err: errors.New("cache event oldest revision is 10 newer than requested revision 9"), want: true},
		{name: "grpc unavailable", err: status.Error(codes.Unavailable, "leader is not ready"), want: false},
		{name: "grpc internal", err: status.Error(codes.Internal, "backend watch failed"), want: false},
		{name: "plain non compacted", err: errors.New("watch closed"), want: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, isWatchCompactedError(tt.err))
		})
	}
}

func TestWatchEventToEtcdEventDistinguishesCreateAndUpdate(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	ctx := context.Background()
	key := []byte("/registry/watch/update-kind")
	createResp, err := server.Put(ctx, &etcdserverpb.PutRequest{
		Key:   key,
		Value: []byte("v1"),
	})
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		return server.backend.GetCurrentRevision() >= uint64(createResp.Header.Revision)
	}, time.Second, time.Millisecond)

	updateResp, err := server.Put(ctx, &etcdserverpb.PutRequest{
		Key:   key,
		Value: []byte("v2"),
	})
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		return server.backend.GetCurrentRevision() >= uint64(updateResp.Header.Revision)
	}, time.Second, time.Millisecond)

	shim := server.backend.(*backendShim)
	createEvent, err := shim.watchEventToEtcdEvent(ctx, &proto.Event{
		Type:     proto.Event_CREATE,
		Revision: uint64(createResp.Header.Revision),
		Kv: &proto.KeyValue{
			Key:      key,
			Value:    []byte("v1"),
			Revision: uint64(createResp.Header.Revision),
		},
	})
	require.NoError(t, err)
	require.True(t, (&clientv3.Event{Type: clientv3.EventTypePut, Kv: createEvent.Kv}).IsCreate())
	require.Nil(t, createEvent.PrevKv)

	updateEvent, err := shim.watchEventToEtcdEvent(ctx, &proto.Event{
		Type: proto.Event_PUT,
		Kv: &proto.KeyValue{
			Key:      key,
			Value:    []byte("v2"),
			Revision: uint64(updateResp.Header.Revision),
		},
	})
	require.NoError(t, err)
	require.False(t, (&clientv3.Event{Type: clientv3.EventTypePut, Kv: updateEvent.Kv, PrevKv: updateEvent.PrevKv}).IsCreate())
	require.NotNil(t, updateEvent.PrevKv)
	require.Equal(t, []byte("v1"), updateEvent.PrevKv.Value)
	require.Equal(t, createResp.Header.Revision, updateEvent.PrevKv.ModRevision)
	require.Equal(t, updateResp.Header.Revision, updateEvent.Kv.ModRevision)
}

func TestWatchEventToEtcdEventRemainsUpdateWhenPrevKvUnavailable(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	key := []byte("/registry/watch/missing-prev")
	shim := server.backend.(*backendShim)
	event, err := shim.watchEventToEtcdEvent(context.Background(), &proto.Event{
		Type:     proto.Event_PUT,
		Revision: 123,
		Kv: &proto.KeyValue{
			Key:      key,
			Value:    []byte("v1"),
			Revision: 123,
		},
	})
	require.NoError(t, err)
	require.Nil(t, event.PrevKv)
	// A PUT event is an update; even without prev-kv or inline metadata it must
	// not be misreported as a create (#52). CreateRevision is synthesized below
	// ModRevision so clientv3.Event.IsCreate reports false.
	require.False(t, (&clientv3.Event{Type: clientv3.EventTypePut, Kv: event.Kv}).IsCreate())
	require.NotEqual(t, event.Kv.ModRevision, event.Kv.CreateRevision)
	require.Equal(t, event.Kv.ModRevision-1, event.Kv.CreateRevision)
}

// scriptedWatchServer replays a fixed sequence of WatchRequests, then reports
// context.Canceled so RPCServer.Watch's receive loop exits deterministically.
type scriptedWatchServer struct {
	*fakeWatchServer
	reqs []*etcdserverpb.WatchRequest
	idx  int
}

func (s *scriptedWatchServer) Recv() (*etcdserverpb.WatchRequest, error) {
	if s.idx < len(s.reqs) {
		r := s.reqs[s.idx]
		s.idx++
		return r, nil
	}
	return nil, context.Canceled
}

func TestStoreMaxUint64OnlyAdvances(t *testing.T) {
	var v uint64
	util.StoreMaxUint64(&v, 5)
	require.Equal(t, uint64(5), v)
	util.StoreMaxUint64(&v, 3) // stale, must not move backwards
	require.Equal(t, uint64(5), v)
	util.StoreMaxUint64(&v, 9)
	require.Equal(t, uint64(9), v)
}

func TestMinSyncedRevisionReportsSlowestWatch(t *testing.T) {
	// No active watch: nothing can be behind, caller falls back to backend rev.
	empty := &watcher{watches: map[int64]*watch{}}
	if _, ok := empty.minSyncedRevision(); ok {
		t.Fatalf("expected ok=false with no active watches")
	}

	w := &watcher{watches: map[int64]*watch{
		1: {syncedRev: 7},
		2: {syncedRev: 3}, // slowest
		3: {syncedRev: 5},
	}}
	rev, ok := w.minSyncedRevision()
	require.True(t, ok)
	require.Equal(t, uint64(3), rev, "must report the slowest watch, never a faster one")
}

// TestWatchProgressRequestReportsSyncedNotGlobalRevision reproduces the progress
// notification data-loss bug: because the backend advances the global current
// revision before the corresponding events reach a watch, a progress
// notification that echoes the global revision tells the client it is synced
// through revisions whose events it has not received. A watch on an idle key
// must report only the revision it has actually delivered.
func TestWatchProgressRequestReportsSyncedNotGlobalRevision(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	ctx := context.Background()
	// Advance the global revision with writes to an unrelated key.
	var lastRev int64
	for i := 0; i < 3; i++ {
		putResp, err := server.Put(ctx, &etcdserverpb.PutRequest{
			Key:   []byte("/registry/other/x"),
			Value: []byte("v"),
		})
		require.NoError(t, err)
		lastRev = putResp.Header.Revision
	}
	require.Eventually(t, func() bool {
		return server.backend.GetCurrentRevision() >= uint64(lastRev)
	}, time.Second, time.Millisecond)
	global := server.backend.GetCurrentRevision()
	require.Greater(t, global, uint64(1))

	// Watch an idle key from the current revision; no events will ever be
	// delivered to it, so it is synced only through startRevision-1.
	startRev := int64(global)
	stream := &scriptedWatchServer{
		fakeWatchServer: &fakeWatchServer{ctx: ctx},
		reqs: []*etcdserverpb.WatchRequest{
			{RequestUnion: &etcdserverpb.WatchRequest_CreateRequest{
				CreateRequest: &etcdserverpb.WatchCreateRequest{
					Key:           []byte("/registry/watch/idle/x"),
					StartRevision: startRev,
				},
			}},
			{RequestUnion: &etcdserverpb.WatchRequest_ProgressRequest{
				ProgressRequest: &etcdserverpb.WatchProgressRequest{},
			}},
		},
	}

	if err := server.Watch(stream); err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("watch returned unexpected error: %v", err)
	}

	// Per-watch progress (#39): the response carries the WATCH's own id and its
	// truthful delivered watermark. A stream-level -1 (which clientv3 would
	// broadcast) must NOT appear while a lagging watch is active — that was the
	// original data-loss vector this test pins.
	var progress *etcdserverpb.WatchResponse
	for _, resp := range stream.fakeWatchServer.sent {
		if resp.WatchId == -1 && len(resp.Events) == 0 && !resp.Created && !resp.Canceled {
			t.Fatalf("stream-level -1 progress broadcast while a watch is lagging: rev=%d", resp.Header.Revision)
		}
		if resp.WatchId >= 0 && len(resp.Events) == 0 && !resp.Created && !resp.Canceled && resp.Header != nil {
			progress = resp
		}
	}
	require.NotNil(t, progress, "expected a per-watch progress notification response")
	require.Equal(t, startRev-1, progress.Header.Revision,
		"progress must report the delivered/synced revision, not the global current revision")
	require.Less(t, progress.Header.Revision, int64(global),
		"progress must not advertise the global revision that runs ahead of undelivered events")
}

// TestWatchProgressNeverOvertakesBufferedEvent pins the buffered-in-ch safety
// case and monotonicity together (deterministic). The watch loop is driven over
// an injected WatchResult channel (follower/proxy path). We feed an event at
// rev 7 followed by an in-band progress marker at rev 9: the event must be Sent
// before syncedRev claims 7, the marker must advance syncedRev to 9 without
// itself emitting a Send, and a later stale marker at rev 6 must never lower it.
func TestWatchProgressNeverOvertakesBufferedEvent(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	fed := make(chan etcdproxy.WatchResult)
	server.peers = testPeerService{
		isLeader:     false,
		proxyEnabled: true,
		watchFn: func(context.Context, []byte, []byte, uint64) (<-chan etcdproxy.WatchResult, error) {
			return fed, nil
		},
	}

	stream := &fakeWatchServer{ctx: context.Background()}
	wt := &watch{start: "/registry/watch/buffered"}
	w := &watcher{
		backend:     server.backend,
		watchServer: stream,
		grpcServer:  server,
		watches:     map[int64]*watch{7: wt},
		metricCli:   server.metricCli,
	}
	w.wg.Add(1)
	done := make(chan struct{})
	go func() {
		defer close(done)
		w.Watch(context.Background(), 7, &etcdserverpb.WatchCreateRequest{
			Key:           []byte("/registry/watch/buffered"),
			StartRevision: 5,
		})
	}()

	// Deliver a real event at rev 7.
	fed <- etcdproxy.WatchResult{Events: []*mvccpb.Event{
		{Type: mvccpb.PUT, Kv: &mvccpb.KeyValue{Key: []byte("/registry/watch/buffered"), ModRevision: 7}},
	}}
	// Once syncedRev reaches 7, the event was already Sent (storeMax runs after
	// Send on the same goroutine), so the event Send is recorded first.
	require.Eventually(t, func() bool {
		return atomic.LoadUint64(&wt.syncedRev) == 7
	}, time.Second, time.Millisecond)
	require.Len(t, stream.sent, 1, "only the event has been Sent so far")
	require.Equal(t, int64(7), stream.sent[0].Header.Revision)
	require.Len(t, stream.sent[0].Events, 1)

	// Deliver an in-band progress marker at rev 9: it folds into syncedRev but
	// emits no Send of its own.
	fed <- etcdproxy.WatchResult{ProgressRevision: 9}
	require.Eventually(t, func() bool {
		return atomic.LoadUint64(&wt.syncedRev) == 9
	}, time.Second, time.Millisecond)
	require.Len(t, stream.sent, 1, "a progress marker must not emit an event Send")

	// A stale marker at rev 6 must never lower the synced revision.
	fed <- etcdproxy.WatchResult{ProgressRevision: 6}
	require.Never(t, func() bool {
		return atomic.LoadUint64(&wt.syncedRev) != 9
	}, 100*time.Millisecond, 10*time.Millisecond)

	close(fed)
	<-done
}

// TestFromNowWatchSeedsFromPublishedNotCurrentRevision pins the seed hardening: a
// StartRevision==0 watch seeds its progress floor from GetPublishedRevision (the
// safe frontier), never GetCurrentRevision (advanced pre-publish), so the first
// progress report cannot over-report a still-in-flight event.
func TestFromNowWatchSeedsFromPublishedNotCurrentRevision(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	// Force a pre-publish gap: bump the current revision far ahead without ever
	// publishing any event, so published stays low.
	server.backend.SetCurrentRevision(1000)
	current := server.backend.GetCurrentRevision()
	published := server.backend.GetPublishedRevision()
	require.Greater(t, current, published, "test needs current ahead of published")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	stream := &fakeWatchServer{ctx: ctx}
	w := &watcher{
		backend:     server.backend,
		watchServer: stream,
		grpcServer:  server,
		watches:     make(map[int64]*watch),
		metricCli:   server.metricCli,
	}
	w.Start(ctx, &etcdserverpb.WatchCreateRequest{
		Key:            []byte("/registry/watch/from-now"),
		StartRevision:  0,
		ProgressNotify: true,
	})

	// The freshly-created watch's seed must equal the published revision, not the
	// (higher) current revision.
	var seed uint64
	w.Lock()
	require.Len(t, w.watches, 1)
	for _, wt := range w.watches {
		seed = atomic.LoadUint64(&wt.syncedRev)
	}
	w.Unlock()
	require.Equal(t, published, seed, "from-now watch must seed from published revision")
	require.Less(t, seed, current, "from-now watch must not seed from the pre-publish current revision")
}

// controllableWatchServer is a thread-safe Watch stream whose Recv blocks on a
// request channel (kept open so the stream stays alive) until its context is
// cancelled, and whose Send records responses under a mutex.
type controllableWatchServer struct {
	etcdserverpb.Watch_WatchServer
	ctx  context.Context
	recv chan *etcdserverpb.WatchRequest
	mu   sync.Mutex
	sent []*etcdserverpb.WatchResponse
}

func (s *controllableWatchServer) Send(resp *etcdserverpb.WatchResponse) error {
	s.mu.Lock()
	s.sent = append(s.sent, resp)
	s.mu.Unlock()
	return nil
}

func (s *controllableWatchServer) Recv() (*etcdserverpb.WatchRequest, error) {
	select {
	case r, ok := <-s.recv:
		if !ok {
			return nil, context.Canceled
		}
		return r, nil
	case <-s.ctx.Done():
		return nil, context.Canceled
	}
}

func (s *controllableWatchServer) Context() context.Context { return s.ctx }

func (s *controllableWatchServer) snapshot() []*etcdserverpb.WatchResponse {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]*etcdserverpb.WatchResponse, len(s.sent))
	copy(out, s.sent)
	return out
}

// TestQuietWatchProgressAdvancesWhileOtherKeysWritten is the headline repro of
// the confirmed frozen-progress bug, driven end-to-end through the real
// RPCServer.Watch pipeline under -race. A ProgressNotify watch on a quiet key
// must have its progress-notify header advance as OTHER keys are written, instead
// of freezing at the watch's start revision.
func TestQuietWatchProgressAdvancesWhileOtherKeysWritten(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Baseline: advance the store a little, then snapshot the published frontier
	// the quiet watch will seed from.
	_, err := server.Put(ctx, &etcdserverpb.PutRequest{Key: []byte("/registry/other/seed"), Value: []byte("v")})
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		return server.backend.GetPublishedRevision() > 0
	}, 2*time.Second, time.Millisecond)
	seedRev := server.backend.GetPublishedRevision()

	stream := &controllableWatchServer{
		ctx:  ctx,
		recv: make(chan *etcdserverpb.WatchRequest, 1),
	}
	// Quiet key, from now, with progress notifications requested.
	stream.recv <- &etcdserverpb.WatchRequest{RequestUnion: &etcdserverpb.WatchRequest_CreateRequest{
		CreateRequest: &etcdserverpb.WatchCreateRequest{
			Key:            []byte("/registry/quiet/x"),
			ProgressNotify: true,
		},
	}}

	watchDone := make(chan struct{})
	go func() {
		defer close(watchDone)
		_ = server.Watch(stream)
	}()

	// Write a burst of OTHER keys, advancing the global/published revision well
	// past the watch's seed. The quiet watch's prefix matches none of them.
	const K = 20
	var lastRev int64
	for i := 0; i < K; i++ {
		putResp, err := server.Put(ctx, &etcdserverpb.PutRequest{
			Key:   []byte("/registry/other/" + string(rune('a'+i))),
			Value: []byte("v"),
		})
		require.NoError(t, err)
		lastRev = putResp.Header.Revision
	}
	require.Eventually(t, func() bool {
		return server.backend.GetPublishedRevision() >= uint64(lastRev)
	}, 3*time.Second, time.Millisecond)
	target := server.backend.GetPublishedRevision()

	// The progress ticker fires ~1s; allow up to ~4s for a progress notify that
	// has advanced past the seed to the cluster's published revision.
	maxProgress := func() int64 {
		var maxRev int64
		for _, resp := range stream.snapshot() {
			// progress notify: this watch's id, not created/canceled, no events.
			if resp.WatchId >= 0 && !resp.Created && !resp.Canceled && len(resp.Events) == 0 {
				if resp.Header != nil && resp.Header.Revision > maxRev {
					maxRev = resp.Header.Revision
				}
			}
		}
		return maxRev
	}
	require.Eventually(t, func() bool {
		return maxProgress() >= int64(target)
	}, 5*time.Second, 20*time.Millisecond,
		"quiet watch progress must advance to the cluster's published revision, not freeze at the start revision")

	require.Greater(t, maxProgress(), int64(seedRev),
		"progress must advance past the watch's start/seed revision")

	cancel()
	<-watchDone
}
