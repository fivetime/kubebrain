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
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"net"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/authpb"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/mvccpb"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
	gproto "google.golang.org/protobuf/proto"

	proto "github.com/kubewharf/kubebrain-client/api/v2rpc"
	"github.com/kubewharf/kubebrain/pkg/backend"
	"github.com/kubewharf/kubebrain/pkg/metrics"
	"github.com/kubewharf/kubebrain/pkg/server/service/etcdproxy"
	"github.com/kubewharf/kubebrain/pkg/storage"
	"github.com/kubewharf/kubebrain/pkg/storage/memkv"
	"github.com/kubewharf/kubebrain/pkg/util"
)

type fakeWatchServer struct {
	etcdserverpb.Watch_WatchServer
	ctx     context.Context
	mu      sync.Mutex
	sent    []*etcdserverpb.WatchResponse
	recvErr error
	sendErr error
}

func (s *fakeWatchServer) Send(resp *etcdserverpb.WatchResponse) error {
	if s.sendErr != nil {
		return s.sendErr
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.sent = append(s.sent, resp)
	return nil
}

func (s *fakeWatchServer) sentResponses() []*etcdserverpb.WatchResponse {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]*etcdserverpb.WatchResponse(nil), s.sent...)
}

func (s *fakeWatchServer) Recv() (*etcdserverpb.WatchRequest, error) {
	if s.recvErr != nil {
		return nil, s.recvErr
	}
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

func TestWatchServerStreamFailureMetricsCountUnexpectedReceiveAndSendErrors(t *testing.T) {
	rec := &recordingMetrics{}
	server := &RPCServer{metricCli: rec}

	recvErr := errors.New("injected watch receive failure")
	err := server.Watch(&fakeWatchServer{recvErr: recvErr})
	require.ErrorIs(t, err, recvErr)

	w := &watcher{
		watchServer: &fakeWatchServer{sendErr: errors.New("injected watch send failure")},
		metricCli:   rec,
	}
	require.Error(t, w.Send(&etcdserverpb.WatchResponse{}))

	rec.mu.Lock()
	defer rec.mu.Unlock()
	require.Contains(t, rec.counters, recordedCounter{
		name:  "etcd.network.server_stream_failures_total",
		value: 1,
		tags: []metrics.T{
			metrics.Tag("Type", "receive"),
			metrics.Tag("API", "watch"),
		},
	})
	require.Contains(t, rec.counters, recordedCounter{
		name:  "etcd.network.server_stream_failures_total",
		value: 1,
		tags: []metrics.T{
			metrics.Tag("Type", "send"),
			metrics.Tag("API", "watch"),
		},
	})
}

type createCallbackWatchServer struct {
	*fakeWatchServer
	onCreated func()
	mu        sync.Mutex
	sent      []*etcdserverpb.WatchResponse
}

func (s *createCallbackWatchServer) Send(resp *etcdserverpb.WatchResponse) error {
	s.mu.Lock()
	s.sent = append(s.sent, resp)
	s.mu.Unlock()
	if resp.Created && s.onCreated != nil {
		s.onCreated()
	}
	return nil
}

func (s *createCallbackWatchServer) snapshot() []*etcdserverpb.WatchResponse {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]*etcdserverpb.WatchResponse(nil), s.sent...)
}

type blockingFirstSendWatchServer struct {
	*fakeWatchServer
	started chan struct{}
	release chan struct{}
	once    sync.Once
	mu      sync.Mutex
	sent    []*etcdserverpb.WatchResponse
}

type compactionRaceWatchStorage struct {
	storage.KvStorage
	match     []byte
	armed     atomic.Bool
	enterOnce sync.Once
	entered   chan struct{}
	release   chan struct{}
}

func (s *compactionRaceWatchStorage) Iter(ctx context.Context, start, end []byte, timestamp, limit uint64) (storage.Iter, error) {
	if s.armed.Load() && bytes.Contains(start, s.match) {
		s.enterOnce.Do(func() { close(s.entered) })
		select {
		case <-s.release:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	return s.KvStorage.Iter(ctx, start, end, timestamp, limit)
}

type queuedControlWatchServer struct {
	*blockingFirstSendWatchServer
	reqs       []*etcdserverpb.WatchRequest
	next       int
	secondRecv chan struct{}
}

func (s *queuedControlWatchServer) Recv() (*etcdserverpb.WatchRequest, error) {
	if s.next >= len(s.reqs) {
		return nil, context.Canceled
	}
	if s.next == 1 {
		close(s.secondRecv)
	}
	req := s.reqs[s.next]
	s.next++
	return req, nil
}

func (s *blockingFirstSendWatchServer) Send(resp *etcdserverpb.WatchResponse) error {
	s.once.Do(func() {
		close(s.started)
		<-s.release
	})
	s.mu.Lock()
	s.sent = append(s.sent, resp)
	s.mu.Unlock()
	return nil
}

func (s *blockingFirstSendWatchServer) snapshot() []*etcdserverpb.WatchResponse {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]*etcdserverpb.WatchResponse(nil), s.sent...)
}

func TestWatchControlSendDoesNotBlockBehindSlowEventSend(t *testing.T) {
	stream := &blockingFirstSendWatchServer{
		fakeWatchServer: &fakeWatchServer{ctx: context.Background()},
		started:         make(chan struct{}),
		release:         make(chan struct{}),
	}
	w := &watcher{
		watchServer: stream,
		controlCh:   make(chan watchControlResponse, watchControlBuffer),
	}
	w.controlWG.Add(1)
	go w.sendControls()

	eventSent := make(chan error, 1)
	go func() {
		eventSent <- w.Send(&etcdserverpb.WatchResponse{WatchId: 1, Events: []*mvccpb.Event{{}}})
	}()
	<-stream.started

	controlQueued := make(chan error, 1)
	go func() {
		controlQueued <- w.SendControl(&etcdserverpb.WatchResponse{WatchId: 1, Canceled: true})
	}()
	select {
	case err := <-controlQueued:
		require.NoError(t, err)
	case <-time.After(100 * time.Millisecond):
		t.Fatal("control response blocked behind a slow event send")
	}

	close(stream.release)
	require.NoError(t, <-eventSent)
	require.Eventually(t, func() bool {
		return len(stream.snapshot()) == 2
	}, time.Second, time.Millisecond)
	close(w.controlCh)
	w.controlWG.Wait()

	responses := stream.snapshot()
	require.Len(t, responses[0].Events, 1)
	require.True(t, responses[1].Canceled)
}

func TestWatchCreatedSendDoesNotBlockReceivingCancel(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()
	base := &blockingFirstSendWatchServer{
		fakeWatchServer: &fakeWatchServer{ctx: context.Background()},
		started:         make(chan struct{}),
		release:         make(chan struct{}),
	}
	stream := &queuedControlWatchServer{
		blockingFirstSendWatchServer: base,
		secondRecv:                   make(chan struct{}),
		reqs: []*etcdserverpb.WatchRequest{
			{RequestUnion: &etcdserverpb.WatchRequest_CreateRequest{CreateRequest: &etcdserverpb.WatchCreateRequest{
				Key: []byte("/watch/nonblocking-created"), WatchId: 7,
			}}},
			{RequestUnion: &etcdserverpb.WatchRequest_CancelRequest{CancelRequest: &etcdserverpb.WatchCancelRequest{
				WatchId: 7,
			}}},
		},
	}

	done := make(chan error, 1)
	go func() { done <- server.Watch(stream) }()
	<-stream.started
	select {
	case <-stream.secondRecv:
		// Upstream's buffered ctrlStream lets the receive loop consume this
		// cancellation even while the client is not reading Created.
	case <-time.After(100 * time.Millisecond):
		t.Fatal("watch receive loop blocked on sending Created")
	}
	require.Empty(t, stream.snapshot())

	close(stream.release)
	require.Equal(t, codes.Canceled, status.Code(<-done))
	responses := stream.snapshot()
	require.Len(t, responses, 2)
	require.True(t, responses[0].Created)
	require.False(t, responses[0].Canceled)
	require.Equal(t, int64(7), responses[0].WatchId)
	require.False(t, responses[1].Created)
	require.True(t, responses[1].Canceled)
	require.Equal(t, int64(7), responses[1].WatchId)
}

func TestIsExpectedWatchCloseError(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{name: "nil", err: nil, want: false},
		{name: "client close send", err: io.EOF, want: true},
		{name: "context canceled", err: context.Canceled, want: true},
		{name: "wrapped context canceled", err: errors.Join(errors.New("recv failed"), context.Canceled), want: true},
		{name: "grpc canceled", err: status.Error(codes.Canceled, "context canceled"), want: true},
		{name: "request too large", err: rpctypes.ErrGRPCRequestTooLarge, want: true},
		{name: "rate limited", err: rpctypes.ErrGRPCRequestTooManyRequests, want: true},
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

func TestValidateForwardedWatchEventRange(t *testing.T) {
	event := func(key string) *mvccpb.Event {
		return &mvccpb.Event{Kv: &mvccpb.KeyValue{Key: []byte(key)}}
	}
	require.NoError(t, validateForwardedWatchEventRange([]*mvccpb.Event{event("b"), event("c")}, []byte("b"), []byte("d")))
	require.NoError(t, validateForwardedWatchEventRange([]*mvccpb.Event{event("b")}, []byte("b"), nil))
	require.NoError(t, validateForwardedWatchEventRange([]*mvccpb.Event{event("z")}, []byte("b"), []byte{}))
	require.EqualError(t, validateForwardedWatchEventRange([]*mvccpb.Event{event("a")}, []byte("b"), []byte("d")),
		"watch leader proxy returned event key outside the requested range at index 0")
}

func TestWatchBackendPrefix(t *testing.T) {
	require.Equal(t, "/registry/pods/a", watchBackendPrefix([]byte("/registry/pods/a"), nil))
	require.Equal(t, "/registry/pods/", watchBackendPrefix([]byte("/registry/pods/"), []byte("/registry/pods0")))
	require.Equal(t, "", watchBackendPrefix([]byte("/registry/pods/a"), []byte("/registry/pods/c")))
	require.Equal(t, "", watchBackendPrefix([]byte("/registry/pods/a"), []byte{}))
}

func TestWithoutWatchPrevKvsDoesNotMutateSharedEvents(t *testing.T) {
	kvA := &mvccpb.KeyValue{Key: []byte("a"), Value: []byte("value-a")}
	kvB := &mvccpb.KeyValue{Key: []byte("b"), Value: []byte("value-b")}
	events := []*mvccpb.Event{
		{Type: mvccpb.PUT, Kv: kvA, PrevKv: &mvccpb.KeyValue{Key: []byte("prev-a")}},
		nil,
		{Type: mvccpb.DELETE, Kv: kvB, PrevKv: &mvccpb.KeyValue{Key: []byte("prev-b")}},
	}
	withoutPrev := withoutWatchPrevKvs(events)
	require.Len(t, withoutPrev, len(events))
	require.Equal(t, mvccpb.PUT, withoutPrev[0].Type)
	require.Equal(t, mvccpb.DELETE, withoutPrev[2].Type)
	require.Same(t, kvA, withoutPrev[0].Kv, "hot path must not clone the KV value")
	require.Same(t, kvB, withoutPrev[2].Kv, "hot path must not clone the KV value")
	require.NotSame(t, events[0], withoutPrev[0])
	require.NotSame(t, events[2], withoutPrev[2])
	require.Nil(t, withoutPrev[0].PrevKv)
	require.Nil(t, withoutPrev[1])
	require.Nil(t, withoutPrev[2].PrevKv)
	require.NotNil(t, events[0].PrevKv, "source event is shared with PrevKv watchers")
	require.NotNil(t, events[2].PrevKv, "source event is shared with PrevKv watchers")
}

func TestWithoutCompactedWatchPrevKvsDoesNotMutateSharedEvents(t *testing.T) {
	below := &mvccpb.Event{Type: mvccpb.PUT, Kv: &mvccpb.KeyValue{ModRevision: 9}, PrevKv: &mvccpb.KeyValue{Value: []byte("below")}}
	boundary := &mvccpb.Event{Type: mvccpb.DELETE, Kv: &mvccpb.KeyValue{ModRevision: 10}, PrevKv: &mvccpb.KeyValue{Value: []byte("boundary")}}
	above := &mvccpb.Event{Type: mvccpb.PUT, Kv: &mvccpb.KeyValue{ModRevision: 11}, PrevKv: &mvccpb.KeyValue{Value: []byte("above")}}
	events := []*mvccpb.Event{below, nil, boundary, above}

	filtered := withoutCompactedWatchPrevKvs(events, 10)
	require.Len(t, filtered, len(events))
	require.Nil(t, filtered[0].PrevKv)
	require.Nil(t, filtered[1])
	require.Nil(t, filtered[2].PrevKv)
	require.Same(t, above, filtered[3], "events above the watermark stay allocation-free")
	require.NotSame(t, below, filtered[0])
	require.NotSame(t, boundary, filtered[2])
	require.NotNil(t, events[0].PrevKv, "shared source must retain PrevKV")
	require.NotNil(t, events[2].PrevKv, "shared source must retain PrevKV")
	require.NotNil(t, events[3].PrevKv)

	unchanged := withoutCompactedWatchPrevKvs(events, 0)
	require.Same(t, events[0], unchanged[0])
	unchanged = withoutCompactedWatchPrevKvs(events, 8)
	require.Same(t, events[0], unchanged[0])
}

func TestWatchPrevKVVisibilityFailsClosedWhenCompactRevisionUnavailable(t *testing.T) {
	events := []*mvccpb.Event{
		{Type: mvccpb.PUT, Kv: &mvccpb.KeyValue{ModRevision: 11}, PrevKv: &mvccpb.KeyValue{Value: []byte("put-prev")}},
		{Type: mvccpb.DELETE, Kv: &mvccpb.KeyValue{ModRevision: 12}, PrevKv: &mvccpb.KeyValue{Value: []byte("delete-prev")}},
	}

	filtered := watchPrevKVVisibility(events, 0, errors.New("compact metadata unavailable"))
	require.Len(t, filtered, 2)
	require.Nil(t, filtered[0].PrevKv)
	require.Nil(t, filtered[1].PrevKv)
	require.NotNil(t, events[0].PrevKv, "shared source event must remain intact")
	require.NotNil(t, events[1].PrevKv, "shared source event must remain intact")
}

func TestValidateRequestedWatchPrevKVs(t *testing.T) {
	key := []byte("/registry/watch/key")
	previous := &mvccpb.KeyValue{Key: key, CreateRevision: 9, ModRevision: 9, Version: 1}
	for _, test := range []struct {
		name            string
		events          []*mvccpb.Event
		compactRevision uint64
		message         string
	}{
		{
			name: "create does not need previous value",
			events: []*mvccpb.Event{{
				Type: mvccpb.PUT, Kv: &mvccpb.KeyValue{Key: key, CreateRevision: 10, ModRevision: 10, Version: 1},
			}},
		},
		{
			name: "update carries previous value",
			events: []*mvccpb.Event{{
				Type: mvccpb.PUT, Kv: &mvccpb.KeyValue{Key: key, CreateRevision: 9, ModRevision: 10, Version: 2}, PrevKv: previous,
			}},
		},
		{
			name: "compacted update may omit previous value",
			events: []*mvccpb.Event{{
				Type: mvccpb.PUT, Kv: &mvccpb.KeyValue{Key: key, CreateRevision: 9, ModRevision: 10, Version: 2},
			}},
			compactRevision: 10,
		},
		{
			name: "update misses previous value",
			events: []*mvccpb.Event{{
				Type: mvccpb.PUT, Kv: &mvccpb.KeyValue{Key: key, CreateRevision: 9, ModRevision: 10, Version: 2},
			}},
			message: "omitted requested previous value for PUT event at revision 10 and index 0",
		},
		{
			name: "delete misses previous value",
			events: []*mvccpb.Event{{
				Type: mvccpb.DELETE, Kv: &mvccpb.KeyValue{Key: key, ModRevision: 10},
			}},
			message: "omitted requested previous value for DELETE event at revision 10 and index 0",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			err := validateRequestedWatchPrevKVs(test.events, test.compactRevision)
			if test.message == "" {
				require.NoError(t, err)
			} else {
				require.ErrorContains(t, err, test.message)
			}
		})
	}
}

func TestWatchMixedPrevKVStreamsKeepEventsIsolated(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	grpcServer := grpc.NewServer(server.ClientServerOptions()...)
	etcdserverpb.RegisterKVServer(grpcServer, server)
	etcdserverpb.RegisterWatchServer(grpcServer, server)
	listener := bufconn.Listen(1 << 20)
	go func() { _ = grpcServer.Serve(listener) }()
	t.Cleanup(grpcServer.Stop)

	conn, err := grpc.NewClient("passthrough:///watch-mixed-prevkv",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return listener.Dial()
		}))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, conn.Close()) })

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	kv := etcdserverpb.NewKVClient(conn)
	watchClient := etcdserverpb.NewWatchClient(conn)
	key := []byte("/registry/watch/mixed-prevkv")
	_, err = kv.Put(ctx, &etcdserverpb.PutRequest{Key: key, Value: []byte("v0")})
	require.NoError(t, err)

	withPrev, err := watchClient.Watch(ctx)
	require.NoError(t, err)
	withoutPrev, err := watchClient.Watch(ctx)
	require.NoError(t, err)
	t.Cleanup(func() { _ = withPrev.CloseSend() })
	t.Cleanup(func() { _ = withoutPrev.CloseSend() })
	require.NoError(t, withPrev.Send(&etcdserverpb.WatchRequest{
		RequestUnion: &etcdserverpb.WatchRequest_CreateRequest{
			CreateRequest: &etcdserverpb.WatchCreateRequest{Key: key, WatchId: 1, PrevKv: true},
		},
	}))
	require.NoError(t, withoutPrev.Send(&etcdserverpb.WatchRequest{
		RequestUnion: &etcdserverpb.WatchRequest_CreateRequest{
			CreateRequest: &etcdserverpb.WatchCreateRequest{Key: key, WatchId: 2},
		},
	}))
	requireWatchCreated(t, withPrev, 1)
	requireWatchCreated(t, withoutPrev, 2)

	previous := []byte("v0")
	for _, current := range [][]byte{[]byte("v1"), []byte("v2"), []byte("v3")} {
		_, err = kv.Put(ctx, &etcdserverpb.PutRequest{Key: key, Value: current})
		require.NoError(t, err)

		withPrevEvent := requireSingleWatchEvent(t, withPrev, 1)
		require.Equal(t, key, withPrevEvent.Kv.Key)
		require.Equal(t, current, withPrevEvent.Kv.Value)
		require.NotNil(t, withPrevEvent.PrevKv)
		require.Equal(t, key, withPrevEvent.PrevKv.Key)
		require.Equal(t, previous, withPrevEvent.PrevKv.Value)

		withoutPrevEvent := requireSingleWatchEvent(t, withoutPrev, 2)
		require.Equal(t, key, withoutPrevEvent.Kv.Key)
		require.Equal(t, current, withoutPrevEvent.Kv.Value)
		require.Nil(t, withoutPrevEvent.PrevKv)
		previous = current
	}
}

func requireWatchCreated(t *testing.T, stream etcdserverpb.Watch_WatchClient, watchID int64) {
	t.Helper()
	resp, err := stream.Recv()
	require.NoError(t, err)
	require.True(t, resp.Created)
	require.False(t, resp.Canceled)
	require.Equal(t, watchID, resp.WatchId)
}

func requireSingleWatchEvent(t *testing.T, stream etcdserverpb.Watch_WatchClient, watchID int64) *mvccpb.Event {
	t.Helper()
	resp, err := stream.Recv()
	require.NoError(t, err)
	require.False(t, resp.Created)
	require.False(t, resp.Canceled)
	require.Equal(t, watchID, resp.WatchId)
	require.Len(t, resp.Events, 1)
	return resp.Events[0]
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

func TestCanceledWatchCreateResponseAlwaysHasHeader(t *testing.T) {
	const revision = uint64(42)
	for _, reason := range []string{
		"",
		rpctypes.ErrCompacted.Error(),
		rpctypes.ErrPermissionDenied.Error(),
		"mvcc: duplicate watch ID provided on the WatchStream",
		"mvcc: watcher range is empty",
		watchQuotaCancelReason,
	} {
		response := canceledWatchCreateResponse(revision, reason)
		require.NotNil(t, response.Header)
		require.Equal(t, int64(revision), response.Header.Revision)
		require.Equal(t, int64(-1), response.WatchId)
		require.True(t, response.Created)
		require.True(t, response.Canceled)
		require.Equal(t, reason, response.CancelReason)
	}
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

	w.Start(context.Background(), &etcdserverpb.WatchCreateRequest{
		Key: []byte("/watch/invalid"), RangeEnd: []byte("/watch/invalid"), WatchId: 42,
	})
	require.Len(t, stream.sent, 2)
	require.True(t, stream.sent[1].Created)
	require.True(t, stream.sent[1].Canceled)
	require.Equal(t, int64(-1), stream.sent[1].WatchId)
	require.Equal(t, "mvcc: watcher range is empty", stream.sent[1].CancelReason)

	w.Start(context.Background(), &etcdserverpb.WatchCreateRequest{Key: []byte("/watch/duplicate"), WatchId: 42})
	require.Len(t, stream.sent, 3)
	require.True(t, stream.sent[2].Created)
	require.True(t, stream.sent[2].Canceled)
	require.Equal(t, int64(-1), stream.sent[2].WatchId)
	require.Equal(t, "mvcc: duplicate watch ID provided on the WatchStream", stream.sent[2].CancelReason)

	w.CancelRequest(999)
	require.Len(t, stream.sent, 3, "etcd silently ignores cancellation of an unknown watch ID")
	w.CancelRequest(42)
	w.wg.Wait()
	require.Len(t, stream.sent, 4, "backend close after client cancellation must not emit a second response")
	require.True(t, stream.sent[3].Canceled)
	require.Empty(t, stream.sent[3].CancelReason)
}

func TestWatchSignedIDCreateAndCancelBoundariesMatchEtcd(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()
	seed, err := server.Put(context.Background(), &etcdserverpb.PutRequest{
		Key: []byte("/watch/signed-id-seed"), Value: []byte("seed"),
	})
	require.NoError(t, err)

	ids := []int64{-1, math.MinInt64, math.MaxInt64}

	reqs := make([]*etcdserverpb.WatchRequest, 0, len(ids)*2+1)
	for _, id := range ids {
		reqs = append(reqs, &etcdserverpb.WatchRequest{
			RequestUnion: &etcdserverpb.WatchRequest_CreateRequest{
				CreateRequest: &etcdserverpb.WatchCreateRequest{
					Key: []byte("/watch/signed-id"), WatchId: id,
				},
			},
		})
	}
	reqs = append(reqs, &etcdserverpb.WatchRequest{
		RequestUnion: &etcdserverpb.WatchRequest_CancelRequest{
			CancelRequest: &etcdserverpb.WatchCancelRequest{WatchId: 999},
		},
	})
	for _, id := range ids {
		reqs = append(reqs, &etcdserverpb.WatchRequest{
			RequestUnion: &etcdserverpb.WatchRequest_CancelRequest{
				CancelRequest: &etcdserverpb.WatchCancelRequest{WatchId: id},
			},
		})
	}
	stream := &scriptedWatchServer{
		fakeWatchServer: &fakeWatchServer{ctx: context.Background()},
		reqs:            reqs,
	}

	requireWatchCanceled(t, server.Watch(stream))
	require.Len(t, stream.sent, len(ids)*2)
	for i, id := range ids {
		require.True(t, stream.sent[i].Created)
		require.False(t, stream.sent[i].Canceled)
		require.Equal(t, id, stream.sent[i].WatchId)
		require.NotNil(t, stream.sent[i].Header)
		require.Equal(t, seed.Header.Revision, stream.sent[i].Header.Revision)
	}

	for i, id := range ids {
		response := stream.sent[len(ids)+i]
		require.True(t, response.Canceled)
		require.False(t, response.Created)
		require.Equal(t, id, response.WatchId)
		require.Empty(t, response.CancelReason)
		require.NotNil(t, response.Header)
		require.Equal(t, seed.Header.Revision, response.Header.Revision)
	}
}

func TestWatchInvalidCreateAutomaticIDAndUnknownCancelKeepStreamAlive(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()
	seed, err := server.Put(context.Background(), &etcdserverpb.PutRequest{
		Key: []byte("/watch/id-range/seed"), Value: []byte("seed"),
	})
	require.NoError(t, err)

	stream := &scriptedWatchServer{
		fakeWatchServer: &fakeWatchServer{ctx: context.Background()},
		reqs: []*etcdserverpb.WatchRequest{
			{RequestUnion: &etcdserverpb.WatchRequest_CreateRequest{
				CreateRequest: &etcdserverpb.WatchCreateRequest{
					Key: []byte("/watch/id-range/equal"), RangeEnd: []byte("/watch/id-range/equal"), WatchId: 101,
				},
			}},
			{RequestUnion: &etcdserverpb.WatchRequest_CreateRequest{
				CreateRequest: &etcdserverpb.WatchCreateRequest{
					Key: []byte("/watch/id-range/after-error"), WatchId: 102,
				},
			}},
			{RequestUnion: &etcdserverpb.WatchRequest_CreateRequest{
				CreateRequest: &etcdserverpb.WatchCreateRequest{
					Key: []byte("/watch/id-range/automatic"),
				},
			}},
			{RequestUnion: &etcdserverpb.WatchRequest_CancelRequest{
				CancelRequest: &etcdserverpb.WatchCancelRequest{WatchId: 102},
			}},
			{RequestUnion: &etcdserverpb.WatchRequest_CancelRequest{
				CancelRequest: &etcdserverpb.WatchCancelRequest{WatchId: 0},
			}},
			{RequestUnion: &etcdserverpb.WatchRequest_CancelRequest{
				CancelRequest: &etcdserverpb.WatchCancelRequest{WatchId: 999},
			}},
			{RequestUnion: &etcdserverpb.WatchRequest_CreateRequest{
				CreateRequest: &etcdserverpb.WatchCreateRequest{
					Key: []byte("/watch/id-range/final"), WatchId: 103,
				},
			}},
			{RequestUnion: &etcdserverpb.WatchRequest_CancelRequest{
				CancelRequest: &etcdserverpb.WatchCancelRequest{WatchId: 103},
			}},
		},
	}

	requireWatchCanceled(t, server.Watch(stream))
	require.Len(t, stream.sent, 7)

	invalid := stream.sent[0]
	require.True(t, invalid.Created)
	require.True(t, invalid.Canceled)
	require.Equal(t, int64(-1), invalid.WatchId)
	require.Equal(t, "mvcc: watcher range is empty", invalid.CancelReason)
	require.NotNil(t, invalid.Header)

	require.True(t, stream.sent[1].Created)
	require.False(t, stream.sent[1].Canceled)
	require.Equal(t, int64(102), stream.sent[1].WatchId)

	require.True(t, stream.sent[2].Created)
	require.False(t, stream.sent[2].Canceled)
	require.Equal(t, int64(0), stream.sent[2].WatchId)

	require.True(t, stream.sent[3].Canceled)
	require.Equal(t, int64(102), stream.sent[3].WatchId)
	require.True(t, stream.sent[4].Canceled)
	require.Equal(t, int64(0), stream.sent[4].WatchId)

	require.True(t, stream.sent[5].Created)
	require.False(t, stream.sent[5].Canceled)
	require.Equal(t, int64(103), stream.sent[5].WatchId)
	require.True(t, stream.sent[6].Canceled)
	require.Equal(t, int64(103), stream.sent[6].WatchId)
	for _, response := range stream.sent {
		require.NotNil(t, response.Header)
		require.Equal(t, seed.Header.Revision, response.Header.Revision)
	}
}

func TestClientWatchCancelHeaderUsesCurrentRevisionBeforeEventsPublish(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	server.backend.SetCurrentRevision(71)
	backend := &futureProgressBackend{BackendShim: server.backend}
	backend.published.Store(70)
	stream := &fakeWatchServer{ctx: context.Background()}
	w := &watcher{
		backend: backend, watchServer: stream, grpcServer: server,
		watches: map[int64]*watch{9: {cancel: func() {}}}, metricCli: server.metricCli,
	}

	w.CancelRequest(9)
	require.Len(t, stream.sent, 1)
	require.True(t, stream.sent[0].Canceled)
	require.Empty(t, stream.sent[0].CancelReason)
	require.Equal(t, int64(71), stream.sent[0].Header.Revision)
}

func TestLogicalWatchAdmissionMultiplexCancelAndDisconnect(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()
	server.SetMaxWatches(1)
	stream := &fakeWatchServer{ctx: context.Background()}
	w := &watcher{
		backend: server.backend, watchServer: stream, grpcServer: server,
		watches: make(map[int64]*watch), metricCli: server.metricCli,
	}

	w.Start(context.Background(), &etcdserverpb.WatchCreateRequest{Key: []byte("/watch/first"), WatchId: 41})
	require.Len(t, stream.sent, 1)
	require.True(t, stream.sent[0].Created)
	require.False(t, stream.sent[0].Canceled)
	require.Equal(t, int64(1), server.activeWatches)

	// A second logical watch on the same gRPC stream must not bypass the
	// process-wide quota or terminate the already active watch.
	w.Start(context.Background(), &etcdserverpb.WatchCreateRequest{Key: []byte("/watch/rejected"), WatchId: 42})
	require.Len(t, stream.sent, 2)
	require.True(t, stream.sent[1].Created)
	require.True(t, stream.sent[1].Canceled)
	require.Equal(t, int64(-1), stream.sent[1].WatchId)
	require.Equal(t, watchQuotaCancelReason, stream.sent[1].CancelReason)
	require.Equal(t, int64(1), server.activeWatches)
	require.Contains(t, w.watches, int64(41))

	w.CancelRequest(41)
	w.wg.Wait()
	require.Zero(t, server.activeWatches)

	ctx, cancel := context.WithCancel(context.Background())
	w.Start(ctx, &etcdserverpb.WatchCreateRequest{Key: []byte("/watch/reused"), WatchId: 43})
	require.Len(t, stream.sent, 4)
	require.True(t, stream.sent[3].Created)
	require.False(t, stream.sent[3].Canceled)
	require.Equal(t, int64(1), server.activeWatches)

	cancel()
	w.Close()
	require.Zero(t, server.activeWatches, "stream disconnect must release every logical watch slot")
}

func TestWatcherCloseSynchronouslyReleasesQuota(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()
	server.SetMaxWatches(2)
	server.activeWatches = 2

	w := &watcher{
		grpcServer: server,
		watches: map[int64]*watch{
			1: {cancel: func() {}, quotaHeld: true},
			2: {cancel: func() {}, quotaHeld: true},
		},
		metricCli: server.metricCli,
	}
	w.Close()

	require.Empty(t, w.watches)
	require.Zero(t, server.activeWatches)
	require.True(t, server.acquireWatch(), "a new stream must reuse quota immediately after disconnect")
	server.releaseWatch()
}

func TestInvalidWatchCreatesDoNotConsumeQuota(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()
	server.SetMaxWatches(1)
	stream := &fakeWatchServer{ctx: context.Background()}
	w := &watcher{
		backend: server.backend, watchServer: stream, grpcServer: server,
		watches: make(map[int64]*watch), metricCli: server.metricCli,
	}

	w.Start(context.Background(), &etcdserverpb.WatchCreateRequest{
		Key: []byte("z"), RangeEnd: []byte("a"),
	})
	require.Zero(t, server.activeWatches)

	w.Start(context.Background(), &etcdserverpb.WatchCreateRequest{Key: []byte("/watch/valid"), WatchId: 7})
	require.Equal(t, int64(1), server.activeWatches)
	w.Start(context.Background(), &etcdserverpb.WatchCreateRequest{Key: []byte("/watch/duplicate"), WatchId: 7})
	require.Equal(t, int64(1), server.activeWatches)

	w.CancelRequest(7)
	w.wg.Wait()
	require.Zero(t, server.activeWatches)
}

func TestLogicalWatchAdmissionIsAtomicAcrossStreams(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()
	const (
		limit      = 7
		contenders = 64
	)
	server.SetMaxWatches(limit)

	start := make(chan struct{})
	results := make(chan bool, contenders)
	var wg sync.WaitGroup
	wg.Add(contenders)
	for i := 0; i < contenders; i++ {
		go func() {
			defer wg.Done()
			<-start
			results <- server.acquireWatch()
		}()
	}
	close(start)
	wg.Wait()
	close(results)

	admitted := 0
	for ok := range results {
		if ok {
			admitted++
		}
	}
	require.Equal(t, limit, admitted)
	require.Equal(t, int64(limit), server.activeWatches)
	for i := 0; i < admitted; i++ {
		server.releaseWatch()
	}
	require.Zero(t, server.activeWatches)
}

func TestWatchAutomaticIDsAreMonotonicAndSkipExplicitIDs(t *testing.T) {
	w := &watcher{watches: make(map[int64]*watch)}

	id, duplicate := w.allocateWatchIDLocked(0)
	require.Equal(t, int64(0), id)
	require.False(t, duplicate)
	w.watches[id] = &watch{}
	w.watches[2] = &watch{}

	delete(w.watches, 0)
	id, duplicate = w.allocateWatchIDLocked(0)
	require.Equal(t, int64(1), id, "canceled automatic IDs must not be reused")
	require.False(t, duplicate)
	w.watches[id] = &watch{}

	delete(w.watches, 1)
	id, duplicate = w.allocateWatchIDLocked(0)
	require.Equal(t, int64(3), id, "automatic IDs must skip an active explicit ID")
	require.False(t, duplicate)

	id, duplicate = w.allocateWatchIDLocked(2)
	require.Equal(t, int64(2), id)
	require.True(t, duplicate)
}

func TestNegativeWatchRevisionPrecedesRangeAndDuplicateWithoutClosingStream(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()
	stream := &scriptedWatchServer{
		fakeWatchServer: &fakeWatchServer{ctx: context.Background()},
		reqs: []*etcdserverpb.WatchRequest{
			{RequestUnion: &etcdserverpb.WatchRequest_CreateRequest{CreateRequest: &etcdserverpb.WatchCreateRequest{Key: []byte("/watch/existing"), WatchId: 70}}},
			{RequestUnion: &etcdserverpb.WatchRequest_CreateRequest{CreateRequest: &etcdserverpb.WatchCreateRequest{
				Key: []byte("/watch/negative"), RangeEnd: []byte("/watch/negative"), StartRevision: -1, WatchId: 70,
			}}},
			{RequestUnion: &etcdserverpb.WatchRequest_CreateRequest{CreateRequest: &etcdserverpb.WatchCreateRequest{Key: []byte("/watch/next"), WatchId: 71}}},
		},
	}
	requireWatchCanceled(t, server.Watch(stream))
	require.GreaterOrEqual(t, len(stream.sent), 3)
	require.True(t, stream.sent[0].Created)
	require.False(t, stream.sent[0].Canceled)
	require.Equal(t, int64(70), stream.sent[0].WatchId)
	require.True(t, stream.sent[1].Created)
	require.True(t, stream.sent[1].Canceled)
	require.Equal(t, int64(-1), stream.sent[1].WatchId)
	require.Equal(t, rpctypes.ErrCompacted.Error(), stream.sent[1].CancelReason)
	require.True(t, stream.sent[2].Created)
	require.False(t, stream.sent[2].Canceled)
	require.Equal(t, int64(71), stream.sent[2].WatchId)
}

func TestSendWatchFragmentsMatchesEtcdFlags(t *testing.T) {
	response := &etcdserverpb.WatchResponse{
		Header: &etcdserverpb.ResponseHeader{
			ClusterId: 1, MemberId: 2, Revision: 12, RaftTerm: 3,
		},
		WatchId: 7, Created: true, Canceled: true, CompactRevision: 9,
		CancelReason: "fragment-test",
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
		require.True(t, gproto.Equal(response.Header, fragment.Header))
		require.True(t, fragment.Created)
		require.True(t, fragment.Canceled)
		require.Equal(t, int64(9), fragment.CompactRevision)
		require.Equal(t, "fragment-test", fragment.CancelReason)
		require.Equal(t, i < len(fragments)-1, fragment.Fragment)
		for _, event := range fragment.Events {
			keys = append(keys, event.Kv.Key)
		}
	}
	require.Equal(t, [][]byte{[]byte("a"), []byte("b"), []byte("c")}, keys)
}

func TestSendWatchFragmentsDoesNotSplitSingleOversizedEvent(t *testing.T) {
	response := &etcdserverpb.WatchResponse{
		Header:  &etcdserverpb.ResponseHeader{ClusterId: 1, MemberId: 2, Revision: 12, RaftTerm: 3},
		WatchId: 7,
		Events: []*mvccpb.Event{{
			Type: mvccpb.PUT,
			Kv:   &mvccpb.KeyValue{Key: []byte("key"), Value: make([]byte, 1024)},
		}},
	}
	var sent []*etcdserverpb.WatchResponse
	require.NoError(t, sendWatchFragments(response, 1, func(resp *etcdserverpb.WatchResponse) error {
		sent = append(sent, resp)
		return nil
	}))
	require.Equal(t, []*etcdserverpb.WatchResponse{response}, sent)
	require.Same(t, response, sent[0])
	require.False(t, sent[0].Fragment)
}

func TestSendWatchFragmentsSplitsAtExactLimit(t *testing.T) {
	response := &etcdserverpb.WatchResponse{
		Header:  &etcdserverpb.ResponseHeader{Revision: 12},
		WatchId: 7,
		Events: []*mvccpb.Event{
			{Kv: &mvccpb.KeyValue{Key: []byte("a"), Value: make([]byte, 80)}},
			{Kv: &mvccpb.KeyValue{Key: []byte("b"), Value: make([]byte, 80)}},
			{Kv: &mvccpb.KeyValue{Key: []byte("c"), Value: make([]byte, 80)}},
		},
	}
	limit := gproto.Size(response)

	var fragments []*etcdserverpb.WatchResponse
	require.NoError(t, sendWatchFragments(response, limit, func(fragment *etcdserverpb.WatchResponse) error {
		fragments = append(fragments, fragment)
		return nil
	}))

	require.Len(t, fragments, 2)
	require.NotSame(t, response, fragments[0])
	require.NotSame(t, fragments[0], fragments[1])
	require.True(t, fragments[0].Fragment)
	require.False(t, fragments[1].Fragment)
	require.Len(t, fragments[0].Events, 2)
	require.Len(t, fragments[1].Events, 1)
	require.Equal(t, []byte("a"), fragments[0].Events[0].Kv.Key)
	require.Equal(t, []byte("b"), fragments[0].Events[1].Kv.Key)
	require.Equal(t, []byte("c"), fragments[1].Events[0].Kv.Key)
	require.False(t, response.Fragment)
	require.Len(t, response.Events, 3)
}

func TestWatchResponseHeaderIsStampedBeforeFragmentSizing(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()
	response := &etcdserverpb.WatchResponse{
		Header:  txnHeader(12),
		WatchId: 7,
		Events: []*mvccpb.Event{
			{Kv: &mvccpb.KeyValue{Key: []byte("a"), Value: make([]byte, 80)}},
			{Kv: &mvccpb.KeyValue{Key: []byte("b"), Value: make([]byte, 80)}},
			{Kv: &mvccpb.KeyValue{Key: []byte("c"), Value: make([]byte, 80)}},
		},
	}

	require.NoError(t, server.stampWatchResponseHeader(context.Background(), response))
	require.Equal(t, server.backend.ClusterID(), response.Header.ClusterId)
	require.NotZero(t, response.Header.MemberId)
	require.Equal(t, uint64(1), response.Header.RaftTerm)
	require.Equal(t, int64(12), response.Header.Revision)

	var fragments []*etcdserverpb.WatchResponse
	require.NoError(t, sendWatchFragments(response, gproto.Size(response), func(fragment *etcdserverpb.WatchResponse) error {
		fragments = append(fragments, fragment)
		return nil
	}))
	require.Len(t, fragments, 2)
	require.True(t, fragments[0].Fragment)
	require.False(t, fragments[1].Fragment)
}

func TestWatchFragmentLimitUsesConfiguredRequestBytesWithEtcdOverhead(t *testing.T) {
	server := &RPCServer{maxRequestBytes: 1024}
	require.Equal(t, 1024+512*1024, server.watchFragmentBytes())
}

func TestPeriodicProgressSuppressesOneTickAfterEvent(t *testing.T) {
	state := newPeriodicProgressState()
	require.True(t, state.tick(), "a newly created quiet watch is eligible")
	require.True(t, state.tick(), "a quiet watch remains eligible each interval")

	state.eventSent()
	require.False(t, state.tick(), "an event suppresses the next progress tick")
	require.True(t, state.tick(), "the suppressed tick rearms periodic progress")

	state.eventSent()
	state.eventSent()
	require.False(t, state.tick(), "multiple events before a tick still suppress exactly one tick")
	require.True(t, state.tick())
}

func TestWatchProgressIntervalMatchesUpstreamJitterEnvelope(t *testing.T) {
	tests := []struct {
		name     string
		interval time.Duration
		jitter   int64
		want     time.Duration
		wantN    int64
	}{
		{name: "default interval", interval: 0, jitter: 25_000_000, want: 1025 * time.Millisecond, wantN: int64(100 * time.Millisecond)},
		{name: "zero jitter", interval: 2 * time.Second, jitter: 0, want: 2 * time.Second, wantN: int64(200 * time.Millisecond)},
		{name: "upper edge exclusive", interval: time.Second, jitter: int64(100*time.Millisecond - time.Nanosecond), want: 1100*time.Millisecond - time.Nanosecond, wantN: int64(100 * time.Millisecond)},
		{name: "sub-ten-nanosecond interval", interval: 9 * time.Nanosecond, want: 9 * time.Nanosecond},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			called := false
			got := watchProgressIntervalWithJitter(tc.interval, func(n int64) int64 {
				called = true
				require.Equal(t, tc.wantN, n)
				return tc.jitter
			})
			require.Equal(t, tc.want, got)
			require.Equal(t, tc.wantN > 0, called)
		})
	}
}

func TestCancelCompactedWatchResponseUsesBackendCompactRevisionAndEmptyReason(t *testing.T) {
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
	require.Empty(t, resp.CancelReason)
	require.NotNil(t, resp.Header)
	require.Zero(t, resp.Header.Revision)
}

type roleSwitchWatchBackend struct {
	BackendShim
	local  <-chan etcdproxy.WatchResult
	called chan uint64
}

type generationWatchBackend struct {
	BackendShim
	generations             []<-chan etcdproxy.WatchResult
	called                  chan uint64
	firstGenerationCanceled chan struct{}
	progressInterval        time.Duration
}

type compactedGenerationWatchBackend struct {
	*generationWatchBackend
	compactRevision atomic.Uint64
}

func (b *compactedGenerationWatchBackend) GetCompactRevisionFresh(context.Context) (uint64, error) {
	return b.compactRevision.Load(), nil
}

type blockingWatchCompactRevisionBackend struct {
	BackendShim
	entered chan struct{}
	release chan struct{}
	once    sync.Once
	calls   atomic.Int64
}

type blockingCancelCompactRevisionBackend struct {
	BackendShim
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func (b *blockingCancelCompactRevisionBackend) GetCompactRevisionFresh(ctx context.Context) (uint64, error) {
	b.once.Do(func() { close(b.entered) })
	select {
	case <-b.release:
		return 12, nil
	case <-ctx.Done():
		return 0, ctx.Err()
	}
}

func (b *blockingWatchCompactRevisionBackend) GetCompactRevisionFresh(ctx context.Context) (uint64, error) {
	if b.calls.Add(1) != 2 {
		return b.BackendShim.GetCompactRevisionFresh(ctx)
	}
	b.once.Do(func() { close(b.entered) })
	select {
	case <-b.release:
		return 0, nil
	case <-ctx.Done():
		return 0, ctx.Err()
	}
}

func (b *generationWatchBackend) Watch(ctx context.Context, _ string, revision uint64) (<-chan etcdproxy.WatchResult, error) {
	b.called <- revision
	first := len(b.generations) > 1
	ch := b.generations[0]
	b.generations = b.generations[1:]
	if first && b.firstGenerationCanceled != nil {
		go func() {
			<-ctx.Done()
			close(b.firstGenerationCanceled)
		}()
	}
	return ch, nil
}

func (b *generationWatchBackend) WatchProgressNotifyInterval() time.Duration {
	if b.progressInterval > 0 {
		return b.progressInterval
	}
	return b.BackendShim.WatchProgressNotifyInterval()
}

func TestWatchRejectsVisibleEventBelowRequestedStartRevision(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	results := make(chan etcdproxy.WatchResult, 1)
	called := make(chan uint64, 1)
	server.backend = &roleSwitchWatchBackend{BackendShim: server.backend, local: results, called: called}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	stream := &createCallbackWatchServer{fakeWatchServer: &fakeWatchServer{ctx: ctx}}
	w := &watcher{
		backend:     server.backend,
		watchServer: stream,
		grpcServer:  server,
		watches: map[int64]*watch{
			7: {cancel: func() {}, start: "/registry/watch/", end: "/registry/watch0", syncedRev: 9},
		},
		metricCli: server.metricCli,
	}
	w.wg.Add(1)
	go w.Watch(ctx, 7, &etcdserverpb.WatchCreateRequest{
		Key: []byte("/registry/watch/"), RangeEnd: []byte("/registry/watch0"), StartRevision: 10,
	})
	require.Equal(t, uint64(10), <-called)
	results <- etcdproxy.WatchResult{Revision: 10, Events: []*mvccpb.Event{{
		Type: mvccpb.PUT,
		Kv:   &mvccpb.KeyValue{Key: []byte("/registry/watch/a"), Value: []byte("stale"), CreateRevision: 9, ModRevision: 9, Version: 1},
	}}}
	close(results)

	require.Eventually(t, func() bool {
		for _, response := range stream.snapshot() {
			if response.Canceled {
				return true
			}
		}
		return false
	}, time.Second, time.Millisecond)
	for _, response := range stream.snapshot() {
		require.Empty(t, response.Events, "an event below the requested start revision must never be published")
		if response.Canceled {
			require.Contains(t, response.CancelReason, "event revision 9 below watch start revision 10")
		}
	}
	w.wg.Wait()
}

func TestWatchRejectsBatchRevisionBelowVisibleEvent(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	results := make(chan etcdproxy.WatchResult, 1)
	called := make(chan uint64, 1)
	server.backend = &roleSwitchWatchBackend{BackendShim: server.backend, local: results, called: called}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	stream := &createCallbackWatchServer{fakeWatchServer: &fakeWatchServer{ctx: ctx}}
	w := &watcher{
		backend:     server.backend,
		watchServer: stream,
		grpcServer:  server,
		watches: map[int64]*watch{
			7: {cancel: func() {}, start: "/registry/watch/", end: "/registry/watch0", syncedRev: 9},
		},
		metricCli: server.metricCli,
	}
	w.wg.Add(1)
	go w.Watch(ctx, 7, &etcdserverpb.WatchCreateRequest{
		Key: []byte("/registry/watch/"), RangeEnd: []byte("/registry/watch0"), StartRevision: 10,
	})
	require.Equal(t, uint64(10), <-called)
	results <- etcdproxy.WatchResult{Revision: 10, Events: []*mvccpb.Event{{
		Type: mvccpb.PUT,
		Kv:   &mvccpb.KeyValue{Key: []byte("/registry/watch/a"), Value: []byte("future"), CreateRevision: 11, ModRevision: 11, Version: 1},
	}}}
	close(results)

	require.Eventually(t, func() bool {
		for _, response := range stream.snapshot() {
			if response.Canceled {
				return true
			}
		}
		return false
	}, time.Second, time.Millisecond)
	for _, response := range stream.snapshot() {
		require.Empty(t, response.Events, "a response header below its event revision must never be published")
		if response.Canceled {
			require.Contains(t, response.CancelReason, "batch revision 10 below event revision 11")
		}
	}
	w.wg.Wait()
}

func TestWatchRejectsMixedProgressAndEventResult(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	results := make(chan etcdproxy.WatchResult, 1)
	called := make(chan uint64, 1)
	server.backend = &roleSwitchWatchBackend{BackendShim: server.backend, local: results, called: called}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	stream := &createCallbackWatchServer{fakeWatchServer: &fakeWatchServer{ctx: ctx}}
	w := &watcher{
		backend:     server.backend,
		watchServer: stream,
		grpcServer:  server,
		watches: map[int64]*watch{
			7: {cancel: func() {}, start: "/registry/watch/", end: "/registry/watch0", syncedRev: 9},
		},
		metricCli: server.metricCli,
	}
	w.wg.Add(1)
	go w.Watch(ctx, 7, &etcdserverpb.WatchCreateRequest{
		Key: []byte("/registry/watch/"), RangeEnd: []byte("/registry/watch0"), StartRevision: 10,
	})
	require.Equal(t, uint64(10), <-called)
	results <- etcdproxy.WatchResult{
		ProgressRevision: 10,
		Revision:         10,
		Events: []*mvccpb.Event{{
			Type: mvccpb.PUT,
			Kv:   &mvccpb.KeyValue{Key: []byte("/registry/watch/a"), Value: []byte("must-not-disappear"), ModRevision: 10},
		}},
	}
	close(results)

	require.Eventually(t, func() bool {
		for _, response := range stream.snapshot() {
			if response.Canceled {
				return true
			}
		}
		return false
	}, time.Second, time.Millisecond)
	for _, response := range stream.snapshot() {
		require.Empty(t, response.Events)
		if response.Canceled {
			require.Contains(t, response.CancelReason, "mixed progress and events")
		}
	}
	w.wg.Wait()
}

func TestInvalidWatchResultShapeRejectsProgressWithBatchRevision(t *testing.T) {
	require.EqualError(t, invalidWatchResultShape(etcdproxy.WatchResult{}), "watch backend returned an empty watch result")
	err := invalidWatchResultShape(etcdproxy.WatchResult{ProgressRevision: 10, Revision: 9})
	require.EqualError(t, err, "watch backend returned mixed progress revision 10 and batch revision 9")
	require.NoError(t, invalidWatchResultShape(etcdproxy.WatchResult{ProgressRevision: 10}))
	require.NoError(t, invalidWatchResultShape(etcdproxy.WatchResult{Revision: 10, Events: []*mvccpb.Event{{
		Type: mvccpb.PUT, Kv: &mvccpb.KeyValue{Key: []byte("key"), ModRevision: 10},
	}}}))
	require.EqualError(t, invalidWatchResultShape(etcdproxy.WatchResult{
		Revision: 10, Events: []*mvccpb.Event{{Type: mvccpb.PUT}},
	}), "watch backend returned invalid nil event at index 0")
	require.EqualError(t, invalidWatchResultShape(etcdproxy.WatchResult{
		Revision: 10, Events: []*mvccpb.Event{{Type: mvccpb.Event_EventType(99), Kv: &mvccpb.KeyValue{Key: []byte("key"), ModRevision: 10}}},
	}), "watch backend returned unsupported event type 99 at index 0")
	require.EqualError(t, invalidWatchResultShape(etcdproxy.WatchResult{
		Revision: 10, Events: []*mvccpb.Event{{Type: mvccpb.PUT, Kv: &mvccpb.KeyValue{ModRevision: 10}}},
	}), "watch backend returned an empty event key at index 0")
	require.EqualError(t, invalidWatchResultShape(etcdproxy.WatchResult{
		Revision: 10, Events: []*mvccpb.Event{{
			Type: mvccpb.PUT,
			Kv:   &mvccpb.KeyValue{Key: []byte("key"), ModRevision: 10},
			PrevKv: &mvccpb.KeyValue{
				Key: []byte("other"), ModRevision: 9,
			},
		}},
	}), "watch backend returned a previous key that differs from the event key at index 0")
}

func TestInvalidWatchResultShapeRejectsNonTombstoneDeleteMetadata(t *testing.T) {
	for _, test := range []struct {
		name    string
		kv      *mvccpb.KeyValue
		message string
	}{
		{
			name:    "value",
			kv:      &mvccpb.KeyValue{Key: []byte("key"), Value: []byte("stale"), ModRevision: 10},
			message: "DELETE event with a non-empty value",
		},
		{
			name:    "create revision",
			kv:      &mvccpb.KeyValue{Key: []byte("key"), CreateRevision: 2, ModRevision: 10},
			message: "DELETE event with create revision 2",
		},
		{
			name:    "version",
			kv:      &mvccpb.KeyValue{Key: []byte("key"), ModRevision: 10, Version: 3},
			message: "DELETE event with version 3",
		},
		{
			name:    "lease",
			kv:      &mvccpb.KeyValue{Key: []byte("key"), ModRevision: 10, Lease: -1},
			message: "DELETE event with lease -1",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			err := invalidWatchResultShape(etcdproxy.WatchResult{
				Revision: 10,
				Events:   []*mvccpb.Event{{Type: mvccpb.DELETE, Kv: test.kv}},
			})
			require.ErrorContains(t, err, test.message)
		})
	}
}

func TestWatchRejectsNonTombstoneDeleteBeforePublication(t *testing.T) {
	for _, test := range []struct {
		name    string
		kv      *mvccpb.KeyValue
		message string
	}{
		{
			name:    "value",
			kv:      &mvccpb.KeyValue{Key: []byte("/registry/watch/key"), Value: []byte("stale"), ModRevision: 10},
			message: "DELETE event with a non-empty value",
		},
		{
			name:    "generation",
			kv:      &mvccpb.KeyValue{Key: []byte("/registry/watch/key"), CreateRevision: 2, ModRevision: 10, Version: 3},
			message: "DELETE event with create revision 2",
		},
		{
			name:    "lease",
			kv:      &mvccpb.KeyValue{Key: []byte("/registry/watch/key"), ModRevision: 10, Lease: math.MinInt64},
			message: fmt.Sprintf("DELETE event with lease %d", int64(math.MinInt64)),
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			responses := runInjectedWatchResult(t, 1, 0, etcdproxy.WatchResult{
				Revision: 10,
				Events:   []*mvccpb.Event{{Type: mvccpb.DELETE, Kv: test.kv}},
			})
			require.Len(t, responses, 1)
			require.True(t, responses[0].Canceled)
			require.Empty(t, responses[0].Events)
			require.Contains(t, responses[0].CancelReason, test.message)
		})
	}
}

func TestWatchAllowsCanonicalDeleteTombstoneWithSignedPreviousLease(t *testing.T) {
	key := []byte("/registry/watch/key")
	result := etcdproxy.WatchResult{
		Revision: 10,
		Events: []*mvccpb.Event{{
			Type: mvccpb.DELETE,
			Kv:   &mvccpb.KeyValue{Key: key, ModRevision: 10},
			PrevKv: &mvccpb.KeyValue{
				Key: key, Value: []byte("old"), CreateRevision: 9, ModRevision: 9, Version: 1, Lease: math.MinInt64,
			},
		}},
	}
	require.NoError(t, invalidWatchResultShape(result))
	_, err := validatedWatchBatchRevision(result, 0)
	require.NoError(t, err)
	require.Equal(t, int64(math.MinInt64), result.Events[0].PrevKv.Lease)
}

func TestWatchRejectsMismatchedPreviousKeyBeforePublication(t *testing.T) {
	responses := runInjectedWatchResult(t, 1, 0, etcdproxy.WatchResult{
		Revision: 10,
		Events: []*mvccpb.Event{{
			Type: mvccpb.PUT,
			Kv:   &mvccpb.KeyValue{Key: []byte("/registry/watch/key"), ModRevision: 10},
			PrevKv: &mvccpb.KeyValue{
				Key: []byte("/registry/watch/other"), ModRevision: 9,
			},
		}},
	})
	require.Len(t, responses, 1)
	require.True(t, responses[0].Canceled)
	require.Empty(t, responses[0].Events)
	require.Contains(t, responses[0].CancelReason, "previous key that differs from the event key")
}

func TestWatchRejectsMalformedEventBatchBeforePartialPublication(t *testing.T) {
	responses := runInjectedWatchResult(t, 1, 0, etcdproxy.WatchResult{
		Revision: 10,
		Events: []*mvccpb.Event{
			{Type: mvccpb.PUT, Kv: &mvccpb.KeyValue{Key: []byte("/registry/watch/valid"), CreateRevision: 10, ModRevision: 10, Version: 1}},
			nil,
		},
	})
	require.Len(t, responses, 1)
	require.True(t, responses[0].Canceled)
	require.Empty(t, responses[0].Events)
	require.Contains(t, responses[0].CancelReason, "invalid nil event at index 1")
}

func TestWatchRejectsOutOfRangeEventAboveBatchRevisionBeforePartialPublication(t *testing.T) {
	responses := runInjectedWatchResult(t, 1, 0, etcdproxy.WatchResult{
		Revision: 10,
		Events: []*mvccpb.Event{
			{Type: mvccpb.PUT, Kv: &mvccpb.KeyValue{Key: []byte("/registry/watch/valid"), CreateRevision: 10, ModRevision: 10, Version: 1}},
			{Type: mvccpb.PUT, Kv: &mvccpb.KeyValue{Key: []byte("/outside/proxy-prefix"), CreateRevision: 11, ModRevision: 11, Version: 1}},
		},
	})
	require.Len(t, responses, 1)
	require.True(t, responses[0].Canceled)
	require.Empty(t, responses[0].Events)
	require.Contains(t, responses[0].CancelReason, "batch revision 10 below event revision 11 at index 1")
}

func TestValidatedWatchBatchRevisionRejectsNonPositiveEventRevision(t *testing.T) {
	for _, revision := range []int64{0, -1} {
		_, err := validatedWatchBatchRevision(etcdproxy.WatchResult{
			Revision: 10,
			Events: []*mvccpb.Event{{
				Type: mvccpb.PUT, Kv: &mvccpb.KeyValue{ModRevision: revision},
			}},
		}, 0)
		require.EqualError(t, err, fmt.Sprintf("watch backend returned invalid event revision %d at index 0", revision))
	}
}

func TestValidatedWatchBatchRevisionRejectsInvalidKeyValueRevisions(t *testing.T) {
	for _, test := range []struct {
		name    string
		kv      *mvccpb.KeyValue
		prevKV  *mvccpb.KeyValue
		message string
	}{
		{
			name:    "negative create revision",
			kv:      &mvccpb.KeyValue{ModRevision: 10, CreateRevision: -1},
			message: "invalid event create revision -1 for mod revision 10",
		},
		{
			name:    "future create revision",
			kv:      &mvccpb.KeyValue{ModRevision: 10, CreateRevision: 11},
			message: "invalid event create revision 11 for mod revision 10",
		},
		{
			name:    "negative version",
			kv:      &mvccpb.KeyValue{ModRevision: 10, Version: -1},
			message: "invalid event version -1",
		},
		{
			name:    "missing put create revision",
			kv:      &mvccpb.KeyValue{ModRevision: 10, Version: 1},
			message: "PUT event without a create revision",
		},
		{
			name:    "missing put version",
			kv:      &mvccpb.KeyValue{CreateRevision: 2, ModRevision: 10},
			message: "PUT event without a version",
		},
		{
			name:    "put version one after create",
			kv:      &mvccpb.KeyValue{CreateRevision: 2, ModRevision: 10, Version: 1},
			message: "PUT version 1 with create revision 2 differing from mod revision 10",
		},
		{
			name:    "put version exceeds revision window",
			kv:      &mvccpb.KeyValue{CreateRevision: 9, ModRevision: 10, Version: 3},
			message: "PUT version 3 exceeding maximum 2",
		},
		{
			name: "negative previous mod revision",
			kv:   &mvccpb.KeyValue{CreateRevision: 2, ModRevision: 10, Version: 2}, prevKV: &mvccpb.KeyValue{ModRevision: -1},
			message: "invalid previous mod revision -1 for event revision 10",
		},
		{
			name: "future previous mod revision",
			kv:   &mvccpb.KeyValue{CreateRevision: 2, ModRevision: 10, Version: 2}, prevKV: &mvccpb.KeyValue{ModRevision: 10},
			message: "invalid previous mod revision 10 for event revision 10",
		},
		{
			name: "future previous create revision",
			kv:   &mvccpb.KeyValue{CreateRevision: 2, ModRevision: 10, Version: 2}, prevKV: &mvccpb.KeyValue{ModRevision: 9, CreateRevision: 10},
			message: "invalid previous create revision 10 for mod revision 9",
		},
		{
			name: "negative previous version",
			kv:   &mvccpb.KeyValue{CreateRevision: 2, ModRevision: 10, Version: 2}, prevKV: &mvccpb.KeyValue{ModRevision: 9, CreateRevision: 2, Version: -1},
			message: "invalid previous version -1",
		},
		{
			name: "empty previous metadata",
			kv:   &mvccpb.KeyValue{CreateRevision: 2, ModRevision: 10, Version: 2}, prevKV: &mvccpb.KeyValue{},
			message: "invalid previous mod revision 0",
		},
		{
			name: "previous version one after create",
			kv:   &mvccpb.KeyValue{CreateRevision: 2, ModRevision: 10, Version: 2}, prevKV: &mvccpb.KeyValue{CreateRevision: 2, ModRevision: 9, Version: 1},
			message: "previous version 1 with create revision 2 differing from mod revision 9",
		},
		{
			name: "previous version exceeds revision window",
			kv:   &mvccpb.KeyValue{CreateRevision: 2, ModRevision: 10, Version: 4}, prevKV: &mvccpb.KeyValue{CreateRevision: 2, ModRevision: 3, Version: 3},
			message: "previous version 3 exceeding maximum 2",
		},
		{
			name: "put and previous values have different generations",
			kv:   &mvccpb.KeyValue{CreateRevision: 2, ModRevision: 10, Version: 3}, prevKV: &mvccpb.KeyValue{CreateRevision: 3, ModRevision: 9, Version: 2},
			message: "PUT and previous values from different create revisions 2 and 3",
		},
		{
			name: "put skips previous version",
			kv:   &mvccpb.KeyValue{CreateRevision: 2, ModRevision: 10, Version: 4}, prevKV: &mvccpb.KeyValue{CreateRevision: 2, ModRevision: 9, Version: 2},
			message: "PUT version 4 not following previous version 2",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			_, err := validatedWatchBatchRevision(etcdproxy.WatchResult{
				Revision: 10,
				Events:   []*mvccpb.Event{{Type: mvccpb.PUT, Kv: test.kv, PrevKv: test.prevKV}},
			}, 0)
			require.ErrorContains(t, err, test.message)
		})
	}
}

func TestValidatedWatchBatchRevisionPreservesSignedLeaseIDs(t *testing.T) {
	_, err := validatedWatchBatchRevision(etcdproxy.WatchResult{
		Revision: 10,
		Events: []*mvccpb.Event{{
			Type: mvccpb.PUT,
			Kv: &mvccpb.KeyValue{
				CreateRevision: 9, ModRevision: 10, Version: 2, Lease: math.MinInt64,
			},
			PrevKv: &mvccpb.KeyValue{
				CreateRevision: 9, ModRevision: 9, Version: 1, Lease: -1,
			},
		}},
	}, 0)
	require.NoError(t, err, "etcd preserves the full signed lease ID domain; only zero means no lease")
}

func TestWatchRejectsInvalidGenerationMetadataBeforePublication(t *testing.T) {
	for _, test := range []struct {
		name    string
		kv      *mvccpb.KeyValue
		prevKV  *mvccpb.KeyValue
		message string
	}{
		{
			name:    "impossible current lifecycle",
			kv:      &mvccpb.KeyValue{Key: []byte("/registry/watch/key"), CreateRevision: 2, ModRevision: 10, Version: 1},
			message: "PUT version 1 with create revision 2 differing from mod revision 10",
		},
		{
			name:    "discontinuous previous version",
			kv:      &mvccpb.KeyValue{Key: []byte("/registry/watch/key"), CreateRevision: 2, ModRevision: 10, Version: 4},
			prevKV:  &mvccpb.KeyValue{Key: []byte("/registry/watch/key"), CreateRevision: 2, ModRevision: 9, Version: 2},
			message: "PUT version 4 not following previous version 2",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			responses := runInjectedWatchResult(t, 1, 0, etcdproxy.WatchResult{
				Revision: 10,
				Events: []*mvccpb.Event{{
					Type: mvccpb.PUT, Kv: test.kv, PrevKv: test.prevKV,
				}},
			})
			require.Len(t, responses, 1)
			require.True(t, responses[0].Canceled)
			require.Empty(t, responses[0].Events)
			require.Contains(t, responses[0].CancelReason, test.message)
		})
	}
}

func TestWatchRejectsMissingRequestedPreviousValueBeforePublication(t *testing.T) {
	for _, event := range []*mvccpb.Event{
		{
			Type: mvccpb.PUT,
			Kv:   &mvccpb.KeyValue{Key: []byte("/registry/watch/update"), CreateRevision: 9, ModRevision: 10, Version: 2},
		},
		{
			Type: mvccpb.DELETE,
			Kv:   &mvccpb.KeyValue{Key: []byte("/registry/watch/delete"), ModRevision: 10},
		},
	} {
		responses := runInjectedWatchResultWithPrevKV(t, 1, 0, true, etcdproxy.WatchResult{
			Revision: 10,
			Events:   []*mvccpb.Event{event},
		})
		require.Len(t, responses, 1)
		require.True(t, responses[0].Canceled)
		require.Empty(t, responses[0].Events)
		require.Contains(t, responses[0].CancelReason, "omitted requested previous value")
	}
}

func TestWatchRejectsInvalidPreviousRevisionBeforePublication(t *testing.T) {
	responses := runInjectedWatchResult(t, 1, 0, etcdproxy.WatchResult{
		Revision: 10,
		Events: []*mvccpb.Event{{
			Type: mvccpb.PUT,
			Kv:   &mvccpb.KeyValue{Key: []byte("/registry/watch/key"), CreateRevision: 2, ModRevision: 10, Version: 2},
			PrevKv: &mvccpb.KeyValue{
				Key: []byte("/registry/watch/key"), CreateRevision: 2, ModRevision: 11, Version: 1,
			},
		}},
	})
	require.Len(t, responses, 1)
	require.True(t, responses[0].Canceled)
	require.Empty(t, responses[0].Events)
	require.Contains(t, responses[0].CancelReason, "invalid previous mod revision 11 for event revision 10")
}

func TestWatchRejectsRegressingEventRevisionWithinBatch(t *testing.T) {
	responses := runInjectedWatchResult(t, 1, 0, etcdproxy.WatchResult{
		Revision: 10,
		Events: []*mvccpb.Event{
			{Type: mvccpb.PUT, Kv: &mvccpb.KeyValue{Key: []byte("/registry/watch/newer"), CreateRevision: 10, ModRevision: 10, Version: 1}},
			{Type: mvccpb.PUT, Kv: &mvccpb.KeyValue{Key: []byte("/registry/watch/older"), CreateRevision: 9, ModRevision: 9, Version: 1}},
		},
	})
	require.Len(t, responses, 1)
	require.True(t, responses[0].Canceled)
	require.Empty(t, responses[0].Events)
	require.Contains(t, responses[0].CancelReason, "event revision 9 at index 1 below preceding revision 10")
}

func TestWatchRejectsEventAlreadyCoveredBySourceWatermark(t *testing.T) {
	responses := runInjectedWatchResult(t, 1, 10, etcdproxy.WatchResult{
		Revision: 11,
		Events: []*mvccpb.Event{{
			Type: mvccpb.PUT,
			Kv:   &mvccpb.KeyValue{Key: []byte("/registry/watch/stale"), CreateRevision: 10, ModRevision: 10, Version: 1},
		}},
	})
	require.Len(t, responses, 1)
	require.True(t, responses[0].Canceled)
	require.Empty(t, responses[0].Events)
	require.Contains(t, responses[0].CancelReason, "event revision 10 at index 0 does not advance source revision 10")
}

func TestWatchRejectsEmptyBackendResult(t *testing.T) {
	responses := runInjectedWatchResult(t, 1, 0, etcdproxy.WatchResult{})
	require.Len(t, responses, 1)
	require.True(t, responses[0].Canceled)
	require.Contains(t, responses[0].CancelReason, "empty watch result")
}

func TestWatchRejectsRegressingProgressRevision(t *testing.T) {
	responses := runInjectedWatchResult(t, 1, 10, etcdproxy.WatchResult{ProgressRevision: 9})
	require.Len(t, responses, 1)
	require.True(t, responses[0].Canceled)
	require.Contains(t, responses[0].CancelReason, "progress revision 9 below source revision 10")
}

func TestWatchRejectsRegressingBatchRevision(t *testing.T) {
	responses := runInjectedWatchResult(t, 1, 10, etcdproxy.WatchResult{
		Revision: 9,
		Events: []*mvccpb.Event{{
			Type: mvccpb.PUT,
			Kv:   &mvccpb.KeyValue{Key: []byte("/registry/watch/a"), Value: []byte("duplicate"), ModRevision: 9},
		}},
	})
	for _, response := range responses {
		require.Empty(t, response.Events, "a batch below the delivered watermark must never be replayed")
		if response.Canceled {
			require.Contains(t, response.CancelReason, "batch revision 9 below source revision 10")
		}
	}
}

func TestWatchRejectsProgressRevisionOutsideInt64(t *testing.T) {
	responses := runInjectedWatchResult(t, 1, 0, etcdproxy.WatchResult{ProgressRevision: math.MaxUint64})
	require.Len(t, responses, 1)
	require.True(t, responses[0].Canceled)
	require.Contains(t, responses[0].CancelReason, "progress revision 18446744073709551615 exceeds MaxInt64")
}

func TestWatchRejectsBatchRevisionOutsideInt64(t *testing.T) {
	responses := runInjectedWatchResult(t, 1, 0, etcdproxy.WatchResult{
		Revision: math.MaxUint64,
		Events: []*mvccpb.Event{{
			Type: mvccpb.PUT,
			Kv:   &mvccpb.KeyValue{Key: []byte("/registry/watch/a"), Value: []byte("invalid-header"), ModRevision: 1},
		}},
	})
	for _, response := range responses {
		require.Empty(t, response.Events)
		if response.Canceled {
			require.Contains(t, response.CancelReason, "batch revision 18446744073709551615 exceeds MaxInt64")
		}
	}
}

func runInjectedWatchResult(t *testing.T, startRevision int64, syncedRevision uint64, result etcdproxy.WatchResult) []*etcdserverpb.WatchResponse {
	return runInjectedWatchResultWithPrevKV(t, startRevision, syncedRevision, false, result)
}

func runInjectedWatchResultWithPrevKV(t *testing.T, startRevision int64, syncedRevision uint64, prevKV bool, result etcdproxy.WatchResult) []*etcdserverpb.WatchResponse {
	t.Helper()
	server, closeFn := newTestRPCServer(t)
	defer closeFn()
	results := make(chan etcdproxy.WatchResult, 1)
	called := make(chan uint64, 1)
	server.backend = &roleSwitchWatchBackend{BackendShim: server.backend, local: results, called: called}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	stream := &createCallbackWatchServer{fakeWatchServer: &fakeWatchServer{ctx: ctx}}
	w := &watcher{
		backend:     server.backend,
		watchServer: stream,
		grpcServer:  server,
		watches: map[int64]*watch{
			7: {cancel: func() {}, start: "/registry/watch/", end: "/registry/watch0", syncedRev: syncedRevision, sourceRev: syncedRevision},
		},
		metricCli: server.metricCli,
	}
	w.wg.Add(1)
	go w.Watch(ctx, 7, &etcdserverpb.WatchCreateRequest{
		Key: []byte("/registry/watch/"), RangeEnd: []byte("/registry/watch0"), StartRevision: startRevision, PrevKv: prevKV,
	})
	require.Equal(t, uint64(startRevision), <-called)
	results <- result
	close(results)
	require.Eventually(t, func() bool {
		for _, response := range stream.snapshot() {
			if response.Canceled {
				return true
			}
		}
		return false
	}, time.Second, time.Millisecond)
	w.wg.Wait()
	return stream.snapshot()
}

func (b *roleSwitchWatchBackend) Watch(_ context.Context, _ string, revision uint64) (<-chan etcdproxy.WatchResult, error) {
	b.called <- revision
	return b.local, nil
}

func TestLeaderWatchResumesThroughProxyAfterLocalGenerationCloses(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	localCh := make(chan etcdproxy.WatchResult, 1)
	proxyCh := make(chan etcdproxy.WatchResult, 1)
	localCalled := make(chan uint64, 1)
	backend := &roleSwitchWatchBackend{BackendShim: server.backend, local: localCh, called: localCalled}
	server.backend = backend
	var leading atomic.Bool
	leading.Store(true)
	proxyCalled := make(chan uint64, 1)
	server.peers = testPeerService{
		isLeaderFn:   leading.Load,
		proxyEnabled: true,
		watchFn: func(_ context.Context, key, rangeEnd []byte, revision uint64) (<-chan etcdproxy.WatchResult, error) {
			require.Equal(t, []byte("/registry/watch/proxy/"), key)
			require.Equal(t, []byte("/registry/watch/proxy0"), rangeEnd)
			proxyCalled <- revision
			return proxyCh, nil
		},
	}

	ctx, cancel := context.WithCancel(context.Background())
	stream := &createCallbackWatchServer{fakeWatchServer: &fakeWatchServer{ctx: ctx}}
	w := &watcher{
		backend:     server.backend,
		watchServer: stream,
		grpcServer:  server,
		watches: map[int64]*watch{
			7: {start: "/registry/watch/proxy/", end: "/registry/watch/proxy0", syncedRev: 9},
		},
		metricCli: server.metricCli,
	}
	w.wg.Add(1)
	done := make(chan struct{})
	go func() {
		defer close(done)
		w.Watch(ctx, 7, &etcdserverpb.WatchCreateRequest{
			Key: []byte("/registry/watch/proxy/"), RangeEnd: []byte("/registry/watch/proxy0"), StartRevision: 10,
		})
	}()
	require.Equal(t, uint64(10), <-localCalled)
	localCh <- etcdproxy.WatchResult{Revision: 10, Events: []*mvccpb.Event{{
		Type: mvccpb.PUT, Kv: &mvccpb.KeyValue{Key: []byte("/registry/watch/proxy/a"), Value: []byte("local"), CreateRevision: 10, ModRevision: 10, Version: 1},
	}}}
	require.Eventually(t, func() bool { return len(stream.snapshot()) == 1 }, time.Second, time.Millisecond)

	leading.Store(false)
	close(localCh)
	require.Equal(t, uint64(11), <-proxyCalled, "resume must start after the last successfully delivered revision")
	proxyCh <- etcdproxy.WatchResult{Revision: 11, Events: []*mvccpb.Event{{
		Type: mvccpb.PUT, Kv: &mvccpb.KeyValue{Key: []byte("/registry/watch/proxy/b"), Value: []byte("proxy"), CreateRevision: 11, ModRevision: 11, Version: 1},
	}}}
	require.Eventually(t, func() bool { return len(stream.snapshot()) == 2 }, time.Second, time.Millisecond)
	for _, response := range stream.snapshot() {
		require.False(t, response.Canceled)
	}

	cancel()
	close(proxyCh)
	<-done
}

func TestFreshLeaderReopensLocalWatchWithoutPeerProxy(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	firstCh := make(chan etcdproxy.WatchResult, 1)
	secondCh := make(chan etcdproxy.WatchResult, 1)
	called := make(chan uint64, 2)
	firstCanceled := make(chan struct{})
	server.backend = &generationWatchBackend{
		BackendShim:             server.backend,
		generations:             []<-chan etcdproxy.WatchResult{firstCh, secondCh},
		called:                  called,
		firstGenerationCanceled: firstCanceled,
	}
	server.peers = testPeerService{
		isLeaderFn: func() bool { return true },
		epochFn:    func() (uint64, bool) { return 7, true },
		// Peer proxy is intentionally disabled. A fresh leader still owns an
		// authoritative local source and must not cancel on generation rollover.
		proxyEnabled: false,
	}

	ctx, cancel := context.WithCancel(context.Background())
	stream := &createCallbackWatchServer{fakeWatchServer: &fakeWatchServer{ctx: ctx}}
	w := &watcher{
		backend: server.backend, watchServer: stream, grpcServer: server,
		watches:   map[int64]*watch{7: {start: "/registry/watch/local-reopen/", syncedRev: 9, sourceRev: 9}},
		metricCli: server.metricCli,
	}
	w.wg.Add(1)
	done := make(chan struct{})
	go func() {
		defer close(done)
		w.Watch(ctx, 7, &etcdserverpb.WatchCreateRequest{
			Key: []byte("/registry/watch/local-reopen/"), StartRevision: 10,
		})
	}()
	require.Equal(t, uint64(10), <-called)
	firstCh <- etcdproxy.WatchResult{Revision: 10, Events: []*mvccpb.Event{{
		Type: mvccpb.PUT,
		Kv:   &mvccpb.KeyValue{Key: []byte("/registry/watch/local-reopen/"), Value: []byte("first"), CreateRevision: 10, ModRevision: 10, Version: 1},
	}}}
	require.Eventually(t, func() bool { return len(stream.snapshot()) == 1 }, time.Second, time.Millisecond)
	close(firstCh)

	require.Equal(t, uint64(11), <-called, "fresh local reopen must resume after the delivered revision")
	<-firstCanceled
	secondCh <- etcdproxy.WatchResult{Revision: 11, Events: []*mvccpb.Event{{
		Type: mvccpb.PUT,
		Kv:   &mvccpb.KeyValue{Key: []byte("/registry/watch/local-reopen/"), Value: []byte("second"), CreateRevision: 10, ModRevision: 11, Version: 2},
	}}}
	require.Eventually(t, func() bool { return len(stream.snapshot()) == 2 }, time.Second, time.Millisecond)
	for _, response := range stream.snapshot() {
		require.False(t, response.Canceled)
	}

	cancel()
	close(secondCh)
	<-done
}

func TestFreshLeaderCompactedBeforeReopenRecordsTerminalOutcome(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()
	rec := &recordingMetrics{}
	initWatchGenerationRecoveryMetrics(rec)
	firstCh := make(chan etcdproxy.WatchResult)
	called := make(chan uint64, 1)
	backend := &compactedGenerationWatchBackend{generationWatchBackend: &generationWatchBackend{
		BackendShim: server.backend, generations: []<-chan etcdproxy.WatchResult{firstCh}, called: called,
	}}
	server.backend = backend
	server.peers = testPeerService{
		isLeaderFn: func() bool { return true },
		epochFn:    func() (uint64, bool) { return 7, true },
	}

	stream := &fakeWatchServer{ctx: context.Background()}
	w := &watcher{
		backend: server.backend, watchServer: stream, grpcServer: server,
		watches:   map[int64]*watch{7: {start: "/registry/watch/precompact/", syncedRev: 9, sourceRev: 9}},
		metricCli: rec,
	}
	w.wg.Add(1)
	done := make(chan struct{})
	go func() {
		defer close(done)
		w.Watch(context.Background(), 7, &etcdserverpb.WatchCreateRequest{
			Key: []byte("/registry/watch/precompact/"), StartRevision: 10,
		})
	}()
	require.Equal(t, uint64(10), <-called)
	backend.compactRevision.Store(11)
	close(firstCh)
	<-done

	require.Len(t, stream.sent, 1)
	terminal := stream.sent[0]
	require.True(t, terminal.GetCanceled())
	require.Equal(t, int64(11), terminal.GetCompactRevision())
	require.Empty(t, terminal.GetCancelReason())
	require.Equal(t, []interface{}{int64(0), 1},
		recordedWatchGenerationRecoveryValues(rec, watchGenerationRecoveryCompacted))
	require.Equal(t, []interface{}{int64(0)},
		recordedWatchGenerationRecoveryValues(rec, watchGenerationRecoveryRetry))
	require.Equal(t, []interface{}{int64(0)},
		recordedWatchGenerationRecoveryValues(rec, watchGenerationRecoveryRecovered))
}

// TestSlowLocalWatchBeyondRingReopensFromDurableHistory pins the complete
// client-visible contract behind the backend hub's "dropped" outcome. The hub
// closes one overloaded backend generation after its missed tail leaves the
// bounded ring, but a fresh leader must transparently reopen that logical Watch
// at the first revision not successfully sent and replay the durable history.
// In particular, ring eviction is not itself a public cancellation or a reason
// for an O(all-keys) client re-list; only compaction of the resume revision is.
func TestSlowLocalWatchBeyondRingReopensFromDurableHistory(t *testing.T) {
	rec := &recordingMetrics{}
	kv := memkv.NewKvStorage()
	rawBackend := backend.NewBackend(kv, backend.Config{
		Identity:                "slow-watch-ring-peer",
		EnableEtcdCompatibility: true,
		WatchCacheSize:          4,
		WatchFanoutBuffer:       1,
	}, rec)
	server := New(rawBackend, rec, testPeerService{
		isLeader: true,
		epochFn:  func() (uint64, bool) { return 7, true },
	})
	defer func() {
		require.NoError(t, server.Close())
		closer, ok := rawBackend.(interface{ Close() error })
		require.True(t, ok)
		require.NoError(t, closer.Close())
	}()

	const totalEvents = 112 // > result buffer (100) + hub buffer (1) + ring (4)
	key := func(i int) []byte { return []byte(fmt.Sprintf("/registry/watch/ring/%03d", i)) }
	first, err := server.Put(context.Background(), &etcdserverpb.PutRequest{Key: key(0), Value: []byte("0")})
	require.NoError(t, err)
	firstRevision := uint64(first.GetHeader().GetRevision())
	require.Eventually(t, func() bool {
		return server.backend.GetPublishedRevision() >= firstRevision
	}, 5*time.Second, time.Millisecond, "seed event must enter the watch cache")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	stream := &blockingFirstSendWatchServer{
		fakeWatchServer: &fakeWatchServer{ctx: ctx},
		started:         make(chan struct{}),
		release:         make(chan struct{}),
	}
	const watchPrefix = "/registry/watch/ring/"
	wt := &watch{
		start:     watchPrefix,
		end:       "/registry/watch/ring0",
		syncedRev: firstRevision - 1,
		sourceRev: firstRevision - 1,
	}
	w := &watcher{
		backend: server.backend, watchServer: stream, grpcServer: server,
		watches: map[int64]*watch{7: wt}, metricCli: rec,
	}
	w.wg.Add(1)
	done := make(chan struct{})
	go func() {
		defer close(done)
		w.Watch(ctx, 7, &etcdserverpb.WatchCreateRequest{
			Key: []byte(watchPrefix), RangeEnd: []byte("/registry/watch/ring0"),
			StartRevision: int64(firstRevision),
		})
	}()
	<-stream.started // the seed event is blocked at the public Send boundary

	for i := 1; i < totalEvents; i++ {
		response, putErr := server.Put(context.Background(), &etcdserverpb.PutRequest{
			Key: key(i), Value: []byte(fmt.Sprintf("%d", i)),
		})
		require.NoError(t, putErr)
		revision := uint64(response.GetHeader().GetRevision())
		require.Equal(t, firstRevision+uint64(i), revision)
		// Do not issue the next write until this one has been broadcast. This
		// makes every mutation a distinct fan-out batch and deterministically
		// fills the downstream buffers instead of letting the collector coalesce.
		require.Eventually(t, func() bool {
			return server.backend.GetPublishedRevision() >= revision
		}, 5*time.Second, time.Millisecond, "revision %d must be fanned out", revision)
	}

	close(stream.release)
	require.Eventually(t, func() bool {
		return len(recordedCounterValues(rec, "drop.slow.watcher")) == 1
	}, 5*time.Second, time.Millisecond, "the real hub backlog must leave the four-event ring")
	require.Eventually(t, func() bool {
		count := 0
		for _, response := range stream.snapshot() {
			count += len(response.GetEvents())
		}
		return count == totalEvents
	}, 10*time.Second, time.Millisecond, "the reopened generation must replay every durable event")

	responses := stream.snapshot()
	revisions := make([]uint64, 0, totalEvents)
	for _, response := range responses {
		require.False(t, response.GetCanceled(), "ring eviction must stay internal to the logical Watch")
		for _, event := range response.GetEvents() {
			revisions = append(revisions, uint64(event.GetKv().GetModRevision()))
		}
	}
	require.Len(t, revisions, totalEvents)
	for i, revision := range revisions {
		require.Equal(t, firstRevision+uint64(i), revision,
			"durable generation reopen must be gap-free and duplicate-free")
	}
	require.Equal(t, []interface{}{int64(0), 1},
		recordedWatchGenerationRecoveryValues(rec, watchGenerationRecoveryRecovered))
	require.Equal(t, []interface{}{int64(0), 1},
		recordedWatcherSlowConsumerOutcomeValues(rec, "dropped"))

	cancel()
	<-done
}

// TestSlowLocalWatchCompactedWhileReopeningCancelsAtDurableWatermark pins the
// active-compaction race behind generation recovery. The slow generation is
// first evicted from the real bounded Hub. Its durable history scan is then
// held open while Compact advances the shared logical watermark past the first
// undelivered revision. Like upstream's unsynced watcher, the logical Watch must
// terminate with the authoritative compact revision rather than replay rows
// returned by a stale scan or retry the generation forever.
func TestSlowLocalWatchCompactedWhileReopeningCancelsAtDurableWatermark(t *testing.T) {
	rec := &recordingMetrics{}
	watchPrefix := "/registry/watch/compact-race/"
	kv := &compactionRaceWatchStorage{
		KvStorage: memkv.NewKvStorage(), match: []byte(watchPrefix),
		entered: make(chan struct{}), release: make(chan struct{}),
	}
	released := false
	defer func() {
		if !released {
			close(kv.release)
		}
	}()
	rawBackend := backend.NewBackend(kv, backend.Config{
		Identity:                "slow-watch-compaction-peer",
		EnableEtcdCompatibility: true,
		WatchCacheSize:          4,
		WatchFanoutBuffer:       1,
	}, rec)
	server := New(rawBackend, rec, testPeerService{
		isLeader: true,
		epochFn:  func() (uint64, bool) { return 7, true },
	})
	defer func() {
		require.NoError(t, server.Close())
		closer, ok := rawBackend.(interface{ Close() error })
		require.True(t, ok)
		require.NoError(t, closer.Close())
	}()

	const totalEvents = 112 // > result buffer (100) + hub buffer (1) + ring (4)
	key := func(i int) []byte { return []byte(fmt.Sprintf("%s%03d", watchPrefix, i)) }
	first, err := server.Put(context.Background(), &etcdserverpb.PutRequest{Key: key(0), Value: []byte("0")})
	require.NoError(t, err)
	firstRevision := uint64(first.GetHeader().GetRevision())
	require.Eventually(t, func() bool {
		return server.backend.GetPublishedRevision() >= firstRevision
	}, 5*time.Second, time.Millisecond)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	stream := &blockingFirstSendWatchServer{
		fakeWatchServer: &fakeWatchServer{ctx: ctx},
		started:         make(chan struct{}),
		release:         make(chan struct{}),
	}
	wt := &watch{
		start: watchPrefix, end: "/registry/watch/compact-race0",
		syncedRev: firstRevision - 1, sourceRev: firstRevision - 1,
	}
	w := &watcher{
		backend: server.backend, watchServer: stream, grpcServer: server,
		watches: map[int64]*watch{7: wt}, metricCli: rec,
	}
	w.wg.Add(1)
	done := make(chan struct{})
	go func() {
		defer close(done)
		w.Watch(ctx, 7, &etcdserverpb.WatchCreateRequest{
			Key: []byte(watchPrefix), RangeEnd: []byte("/registry/watch/compact-race0"),
			StartRevision: int64(firstRevision),
		})
	}()
	<-stream.started

	lastRevision := firstRevision
	for i := 1; i < totalEvents; i++ {
		response, putErr := server.Put(context.Background(), &etcdserverpb.PutRequest{
			Key: key(i), Value: []byte(fmt.Sprintf("%d", i)),
		})
		require.NoError(t, putErr)
		lastRevision = uint64(response.GetHeader().GetRevision())
		require.Equal(t, firstRevision+uint64(i), lastRevision)
		require.Eventually(t, func() bool {
			return server.backend.GetPublishedRevision() >= lastRevision
		}, 5*time.Second, time.Millisecond)
	}

	// Arm only the reopen scan; all setup writes and the original generation are
	// already established. Releasing the blocked public Send lets the Hub expose
	// its dropped generation and starts durable catch-up.
	kv.armed.Store(true)
	close(stream.release)
	select {
	case <-kv.entered:
	case <-time.After(10 * time.Second):
		t.Fatal("reopened watch did not enter the gated history scan")
	}
	_, err = server.Compact(context.Background(), &etcdserverpb.CompactionRequest{Revision: int64(lastRevision)})
	require.NoError(t, err)
	close(kv.release)
	released = true

	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("compacted logical watch did not terminate")
	}
	responses := stream.snapshot()
	require.NotEmpty(t, responses)
	terminal := responses[len(responses)-1]
	require.True(t, terminal.GetCanceled())
	require.Empty(t, terminal.GetEvents())
	require.Empty(t, terminal.GetCancelReason())
	require.Equal(t, int64(lastRevision), terminal.GetCompactRevision())
	require.Equal(t, []interface{}{int64(0), 1},
		recordedWatchGenerationRecoveryValues(rec, watchGenerationRecoveryCompacted))
	require.Equal(t, []interface{}{int64(0)},
		recordedWatchGenerationRecoveryValues(rec, watchGenerationRecoveryRetry))
	require.Equal(t, []interface{}{int64(0)},
		recordedWatchGenerationRecoveryValues(rec, watchGenerationRecoveryRecovered))
	require.Equal(t, []interface{}{int64(0), 1},
		recordedWatcherSlowConsumerOutcomeValues(rec, "dropped"))
}

func TestLocalWatchReopenStopsWhenFreshnessIsLostWithoutPeerProxy(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()
	rec := &recordingMetrics{}
	initWatchGenerationRecoveryMetrics(rec)
	server.peers = testPeerService{
		isLeaderFn:   func() bool { return true }, // stale client-go flag
		epochFn:      func() (uint64, bool) { return 7, false },
		proxyEnabled: false,
	}
	w := &watcher{backend: server.backend, grpcServer: server, metricCli: rec}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	started := time.Now()
	_, _, _, err := w.reopenWatchChannel(ctx, &etcdserverpb.WatchCreateRequest{
		Key: []byte("/registry/watch/no-source"),
	}, "/registry/watch/no-source", 10)
	require.EqualError(t, err, "watch has no fresh local generation and peer proxy is disabled")
	require.Less(t, time.Since(started), 500*time.Millisecond,
		"reopen must not retry forever when no authoritative source is reachable")
	require.Equal(t, []interface{}{int64(0), 1}, recordedWatchGenerationRecoveryValues(rec, watchGenerationRecoveryFailed))
	require.Equal(t, []interface{}{int64(0)}, recordedWatchGenerationRecoveryValues(rec, watchGenerationRecoveryRetry))
}

func TestWatchReopenMetricsRecordRetryAndRecovery(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()
	rec := &recordingMetrics{}
	initWatchGenerationRecoveryMetrics(rec)
	initWatchBackendIntegrityMetrics(rec)
	watchCh := make(chan etcdproxy.WatchResult)
	var attempts atomic.Int32
	server.peers = testPeerService{
		epochFn:      func() (uint64, bool) { return 0, false },
		proxyEnabled: true,
		watchFn: func(context.Context, []byte, []byte, uint64) (<-chan etcdproxy.WatchResult, error) {
			if attempts.Add(1) == 1 {
				return nil, nil
			}
			return watchCh, nil
		},
	}
	w := &watcher{backend: server.backend, grpcServer: server, metricCli: rec}

	got, local, _, err := w.reopenWatchChannel(context.Background(), &etcdserverpb.WatchCreateRequest{
		Key: []byte("/registry/watch/retry"),
	}, "/registry/watch/retry", 10)
	require.NoError(t, err)
	require.Equal(t, (<-chan etcdproxy.WatchResult)(watchCh), got)
	require.False(t, local)
	require.Equal(t, int32(2), attempts.Load())
	require.Equal(t, []interface{}{int64(0), 1}, recordedWatchGenerationRecoveryValues(rec, watchGenerationRecoveryRetry))
	require.Equal(t, []interface{}{int64(0), 1}, recordedWatchGenerationRecoveryValues(rec, watchGenerationRecoveryRecovered))
	require.Equal(t, []interface{}{int64(0)}, recordedWatchGenerationRecoveryValues(rec, watchGenerationRecoveryCompacted))
	require.Equal(t, []interface{}{0, 1}, recordedWatchBackendIntegrityValues(rec, "invalid_result"))
}

func TestOpenWatchChannelRejectsNilInitialGeneration(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()
	rec := &recordingMetrics{}
	initWatchBackendIntegrityMetrics(rec)
	server.peers = testPeerService{
		epochFn:      func() (uint64, bool) { return 0, false },
		proxyEnabled: true,
		// The zero-value test proxy returns (nil, nil), simulating a broken
		// adapter that claims success without a readable generation.
	}
	w := &watcher{backend: server.backend, grpcServer: server, metricCli: rec}

	ch, local, _, err := w.openWatchChannel(context.Background(), &etcdserverpb.WatchCreateRequest{
		Key: []byte("/registry/watch/nil-generation"),
	}, "/registry/watch/nil-generation", 10)
	require.ErrorIs(t, err, errNilWatchGeneration)
	require.Nil(t, ch)
	require.False(t, local)
	require.Equal(t, []interface{}{0, 1}, recordedWatchBackendIntegrityValues(rec, "invalid_result"))
}

func TestOpenWatchChannelRejectsNilLocalGeneration(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()
	rec := &recordingMetrics{}
	initWatchBackendIntegrityMetrics(rec)
	server.backend = &generationWatchBackend{
		BackendShim: server.backend,
		generations: []<-chan etcdproxy.WatchResult{nil},
		called:      make(chan uint64, 1),
	}
	server.peers = testPeerService{epochFn: func() (uint64, bool) { return 7, true }}
	w := &watcher{backend: server.backend, grpcServer: server, metricCli: rec}

	ch, local, epoch, err := w.openWatchChannel(context.Background(), &etcdserverpb.WatchCreateRequest{
		Key: []byte("/registry/watch/nil-local-generation"),
	}, "/registry/watch/nil-local-generation", 10)
	require.ErrorIs(t, err, errNilWatchGeneration)
	require.Nil(t, ch)
	require.True(t, local)
	require.Equal(t, uint64(7), epoch)
	require.Equal(t, []interface{}{0, 1}, recordedWatchBackendIntegrityValues(rec, "invalid_result"))
}

func TestWatchReopenMetricsRecordCompactedTerminal(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()
	rec := &recordingMetrics{}
	initWatchGenerationRecoveryMetrics(rec)
	server.peers = testPeerService{
		epochFn:      func() (uint64, bool) { return 0, false },
		proxyEnabled: true,
		watchFn: func(context.Context, []byte, []byte, uint64) (<-chan etcdproxy.WatchResult, error) {
			return nil, status.Error(codes.OutOfRange, "required revision has been compacted")
		},
	}
	w := &watcher{backend: server.backend, grpcServer: server, metricCli: rec}

	_, _, _, err := w.reopenWatchChannel(context.Background(), &etcdserverpb.WatchCreateRequest{
		Key: []byte("/registry/watch/compacted"),
	}, "/registry/watch/compacted", 10)
	require.Equal(t, codes.OutOfRange, status.Code(err))
	require.Equal(t, []interface{}{int64(0), 1}, recordedWatchGenerationRecoveryValues(rec, watchGenerationRecoveryCompacted))
	require.Equal(t, []interface{}{int64(0)}, recordedWatchGenerationRecoveryValues(rec, watchGenerationRecoveryRetry))
}

func TestWatchReopenContextCancellationIsNotRecoveryFailure(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()
	rec := &recordingMetrics{}
	initWatchGenerationRecoveryMetrics(rec)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	server.peers = testPeerService{
		epochFn:      func() (uint64, bool) { return 0, false },
		proxyEnabled: true,
		watchFn: func(context.Context, []byte, []byte, uint64) (<-chan etcdproxy.WatchResult, error) {
			cancel()
			return nil, errors.New("peer unavailable")
		},
	}
	w := &watcher{backend: server.backend, grpcServer: server, metricCli: rec}

	_, _, _, err := w.reopenWatchChannel(ctx, &etcdserverpb.WatchCreateRequest{
		Key: []byte("/registry/watch/canceled"),
	}, "/registry/watch/canceled", 10)
	require.ErrorIs(t, err, context.Canceled)
	require.Equal(t, []interface{}{int64(0), 1}, recordedWatchGenerationRecoveryValues(rec, watchGenerationRecoveryRetry))
	require.Equal(t, []interface{}{int64(0)}, recordedWatchGenerationRecoveryValues(rec, watchGenerationRecoveryFailed))
}

func TestLeaderWatchFencesEstablishedLocalGenerationAfterEpochChange(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	oldLocalCh := make(chan etcdproxy.WatchResult, 2)
	newLocalCh := make(chan etcdproxy.WatchResult, 1)
	localCalled := make(chan uint64, 2)
	oldGenerationCanceled := make(chan struct{})
	server.backend = &generationWatchBackend{
		BackendShim:             server.backend,
		generations:             []<-chan etcdproxy.WatchResult{oldLocalCh, newLocalCh},
		called:                  localCalled,
		firstGenerationCanceled: oldGenerationCanceled,
	}
	var fresh atomic.Bool
	fresh.Store(true)
	var epoch atomic.Uint64
	epoch.Store(7)
	server.peers = testPeerService{
		isLeaderFn:   func() bool { return true }, // OnStoppedLeading has not run yet.
		epochFn:      func() (uint64, bool) { return epoch.Load(), fresh.Load() },
		proxyEnabled: true,
	}

	ctx, cancel := context.WithCancel(context.Background())
	stream := &createCallbackWatchServer{fakeWatchServer: &fakeWatchServer{ctx: ctx}}
	w := &watcher{
		backend:     server.backend,
		watchServer: stream,
		grpcServer:  server,
		watches: map[int64]*watch{
			7: {start: "/registry/watch/fenced/", end: "/registry/watch/fenced0", syncedRev: 9},
		},
		metricCli: server.metricCli,
	}
	w.wg.Add(1)
	done := make(chan struct{})
	go func() {
		defer close(done)
		w.Watch(ctx, 7, &etcdserverpb.WatchCreateRequest{
			Key: []byte("/registry/watch/fenced/"), RangeEnd: []byte("/registry/watch/fenced0"), StartRevision: 10,
		})
	}()
	require.Equal(t, uint64(10), <-localCalled)
	oldLocalCh <- etcdproxy.WatchResult{Revision: 10, Events: []*mvccpb.Event{{
		Type: mvccpb.PUT, Kv: &mvccpb.KeyValue{Key: []byte("/registry/watch/fenced/a"), Value: []byte("local"), CreateRevision: 10, ModRevision: 10, Version: 1},
	}}}
	require.Eventually(t, func() bool { return len(stream.snapshot()) == 1 }, time.Second, time.Millisecond)

	// The replica has entered a new leadership epoch before the old local channel
	// closes. Its successor-window result must be replayed by the new generation.
	epoch.Store(8)
	oldLocalCh <- etcdproxy.WatchResult{Revision: 11, Events: []*mvccpb.Event{{
		Type: mvccpb.PUT, Kv: &mvccpb.KeyValue{Key: []byte("/registry/watch/fenced/b"), Value: []byte("stale-local"), CreateRevision: 11, ModRevision: 11, Version: 1},
	}}}
	require.Equal(t, uint64(11), <-localCalled, "new epoch must resume after the last delivered revision")
	<-oldGenerationCanceled
	newLocalCh <- etcdproxy.WatchResult{Revision: 11, Events: []*mvccpb.Event{{
		Type: mvccpb.PUT, Kv: &mvccpb.KeyValue{Key: []byte("/registry/watch/fenced/b"), Value: []byte("authoritative-new-epoch"), CreateRevision: 11, ModRevision: 11, Version: 1},
	}}}
	require.Eventually(t, func() bool { return len(stream.snapshot()) == 2 }, time.Second, time.Millisecond)
	responses := stream.snapshot()
	require.Equal(t, []byte("local"), responses[0].Events[0].Kv.Value)
	require.Equal(t, []byte("authoritative-new-epoch"), responses[1].Events[0].Kv.Value)
	for _, response := range responses {
		require.False(t, response.Canceled)
	}

	cancel()
	close(newLocalCh)
	<-done
}

func TestLeaderWatchFreshnessLossDiscardsLocalProgressBeforeResume(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	localCh := make(chan etcdproxy.WatchResult, 1)
	proxyCh := make(chan etcdproxy.WatchResult, 1)
	localCalled := make(chan uint64, 1)
	server.backend = &roleSwitchWatchBackend{BackendShim: server.backend, local: localCh, called: localCalled}
	var fresh atomic.Bool
	fresh.Store(true)
	proxyCalled := make(chan uint64, 1)
	server.peers = testPeerService{
		isLeaderFn:   func() bool { return true }, // Deliberately stale client-go flag.
		epochFn:      func() (uint64, bool) { return 7, fresh.Load() },
		proxyEnabled: true,
		watchFn: func(_ context.Context, _, _ []byte, revision uint64) (<-chan etcdproxy.WatchResult, error) {
			proxyCalled <- revision
			return proxyCh, nil
		},
	}

	ctx, cancel := context.WithCancel(context.Background())
	wt := &watch{start: "/registry/watch/progress/", end: "/registry/watch/progress0", syncedRev: 9, sourceRev: 9}
	w := &watcher{
		backend: server.backend, watchServer: &fakeWatchServer{ctx: ctx}, grpcServer: server,
		watches: map[int64]*watch{7: wt}, metricCli: server.metricCli,
	}
	w.wg.Add(1)
	done := make(chan struct{})
	go func() {
		defer close(done)
		w.Watch(ctx, 7, &etcdserverpb.WatchCreateRequest{
			Key: []byte("/registry/watch/progress/"), RangeEnd: []byte("/registry/watch/progress0"), StartRevision: 10,
		})
	}()
	require.Equal(t, uint64(10), <-localCalled)

	// An obsolete progress marker must not move either watermark to 20. The
	// authoritative generation therefore resumes at the still-undelivered 10.
	fresh.Store(false)
	localCh <- etcdproxy.WatchResult{ProgressRevision: 20}
	require.Equal(t, uint64(10), <-proxyCalled)
	require.Equal(t, uint64(9), atomic.LoadUint64(&wt.syncedRev))
	require.Equal(t, uint64(9), atomic.LoadUint64(&wt.sourceRev))

	cancel()
	close(proxyCh)
	<-done
}

func TestPeriodicProgressTickRehomesQuietSelfFencedGeneration(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	localCh := make(chan etcdproxy.WatchResult)
	proxyCh := make(chan etcdproxy.WatchResult, 1)
	localCalled := make(chan uint64, 1)
	server.backend = &generationWatchBackend{
		BackendShim: server.backend, generations: []<-chan etcdproxy.WatchResult{localCh},
		called: localCalled, progressInterval: 5 * time.Millisecond,
	}
	var fresh atomic.Bool
	fresh.Store(true)
	proxyCalled := make(chan uint64, 1)
	server.peers = testPeerService{
		isLeaderFn:   func() bool { return true }, // OnStoppedLeading remains delayed.
		epochFn:      func() (uint64, bool) { return 7, fresh.Load() },
		proxyEnabled: true,
		watchFn: func(_ context.Context, _, _ []byte, revision uint64) (<-chan etcdproxy.WatchResult, error) {
			proxyCalled <- revision
			return proxyCh, nil
		},
	}

	ctx, cancel := context.WithCancel(context.Background())
	stream := &controllableWatchServer{ctx: ctx}
	w := &watcher{
		backend: server.backend, watchServer: stream, grpcServer: server,
		watches: map[int64]*watch{7: {
			start: "/registry/watch/quiet-fenced", progressStartRevision: 10, syncedRev: 9, sourceRev: 9,
		}},
		metricCli: server.metricCli,
	}
	w.wg.Add(1)
	done := make(chan struct{})
	go func() {
		defer close(done)
		w.Watch(ctx, 7, &etcdserverpb.WatchCreateRequest{
			Key: []byte("/registry/watch/quiet-fenced"), StartRevision: 10, ProgressNotify: true,
		})
	}()
	require.Equal(t, uint64(10), <-localCalled)
	fresh.Store(false)

	// No local result and no OnStoppedLeading callback arrives. The periodic
	// progress tick itself must fence and rehome the quiet generation before it
	// can emit a stale-node progress response.
	require.Equal(t, uint64(10), <-proxyCalled)
	require.Empty(t, stream.snapshot())
	proxyCh <- etcdproxy.WatchResult{ProgressRevision: 10}
	require.Eventually(t, func() bool {
		responses := stream.snapshot()
		return len(responses) == 1 && responses[0].Header.GetRevision() == 10
	}, time.Second, time.Millisecond)

	cancel()
	close(proxyCh)
	<-done
}

func TestLeaderWatchRechecksFreshnessAfterPrevKVAssemblyBeforeSend(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	localCh := make(chan etcdproxy.WatchResult, 1)
	proxyCh := make(chan etcdproxy.WatchResult, 1)
	localCalled := make(chan uint64, 1)
	blockedBackend := &blockingWatchCompactRevisionBackend{
		BackendShim: server.backend,
		entered:     make(chan struct{}),
		release:     make(chan struct{}),
	}
	server.backend = &roleSwitchWatchBackend{BackendShim: blockedBackend, local: localCh, called: localCalled}
	var fresh atomic.Bool
	fresh.Store(true)
	proxyCalled := make(chan uint64, 1)
	server.peers = testPeerService{
		isLeaderFn:   func() bool { return true },
		epochFn:      func() (uint64, bool) { return 7, fresh.Load() },
		proxyEnabled: true,
		watchFn: func(_ context.Context, _, _ []byte, revision uint64) (<-chan etcdproxy.WatchResult, error) {
			proxyCalled <- revision
			return proxyCh, nil
		},
	}

	ctx, cancel := context.WithCancel(context.Background())
	stream := &createCallbackWatchServer{fakeWatchServer: &fakeWatchServer{ctx: ctx}}
	w := &watcher{
		backend: server.backend, watchServer: stream, grpcServer: server,
		watches: map[int64]*watch{7: {
			start: "/registry/watch/send-fence/", end: "/registry/watch/send-fence0", syncedRev: 9, sourceRev: 9,
		}},
		metricCli: server.metricCli,
	}
	w.wg.Add(1)
	done := make(chan struct{})
	go func() {
		defer close(done)
		w.Watch(ctx, 7, &etcdserverpb.WatchCreateRequest{
			Key: []byte("/registry/watch/send-fence/"), RangeEnd: []byte("/registry/watch/send-fence0"),
			StartRevision: 10, PrevKv: true,
		})
	}()
	require.Equal(t, uint64(10), <-localCalled)
	localCh <- etcdproxy.WatchResult{Revision: 10, Events: []*mvccpb.Event{{
		Type: mvccpb.PUT,
		Kv:   &mvccpb.KeyValue{Key: []byte("/registry/watch/send-fence/a"), Value: []byte("stale-local"), CreateRevision: 9, ModRevision: 10, Version: 2},
		PrevKv: &mvccpb.KeyValue{Key: []byte("/registry/watch/send-fence/a"), Value: []byte("old"),
			CreateRevision: 9, ModRevision: 9, Version: 1},
	}}}
	<-blockedBackend.entered

	// The result passed its receive-time fence, then leadership freshness
	// expired while PrevKV visibility was being resolved. The send-time fence
	// must discard it without advancing revision 9.
	fresh.Store(false)
	close(blockedBackend.release)
	require.Equal(t, uint64(10), <-proxyCalled)
	proxyCh <- etcdproxy.WatchResult{Revision: 10, Events: []*mvccpb.Event{{
		Type: mvccpb.PUT,
		Kv:   &mvccpb.KeyValue{Key: []byte("/registry/watch/send-fence/a"), Value: []byte("authoritative-proxy"), CreateRevision: 9, ModRevision: 10, Version: 2},
		PrevKv: &mvccpb.KeyValue{Key: []byte("/registry/watch/send-fence/a"), Value: []byte("old"),
			CreateRevision: 9, ModRevision: 9, Version: 1},
	}}}
	require.Eventually(t, func() bool { return len(stream.snapshot()) == 1 }, time.Second, time.Millisecond)
	responses := stream.snapshot()
	require.Equal(t, []byte("authoritative-proxy"), responses[0].Events[0].Kv.Value)

	cancel()
	close(proxyCh)
	<-done
}

func TestWatchCancelAndIDReusePrecedeNoOldGenerationEventSend(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	results := make(chan etcdproxy.WatchResult, 1)
	called := make(chan uint64, 1)
	server.backend = &roleSwitchWatchBackend{BackendShim: server.backend, local: results, called: called}
	headerEntered := make(chan struct{})
	headerRelease := make(chan struct{})
	server.peers = testPeerService{
		isLeaderFn:    func() bool { return true },
		epochFn:       func() (uint64, bool) { return 7, true },
		currentTermFn: func() uint64 { return 0 },
		leadershipTermFn: func(context.Context) (uint64, error) {
			close(headerEntered)
			<-headerRelease
			return 7, nil
		},
	}

	ctx, cancel := context.WithCancel(context.Background())
	stream := &createCallbackWatchServer{fakeWatchServer: &fakeWatchServer{ctx: ctx}}
	w := &watcher{
		backend: server.backend, watchServer: stream, grpcServer: server,
		watches: map[int64]*watch{7: {
			cancel: cancel, start: "/registry/watch/cancel-order", syncedRev: 9, sourceRev: 9,
		}},
		metricCli: server.metricCli,
	}
	w.wg.Add(1)
	go w.Watch(ctx, 7, &etcdserverpb.WatchCreateRequest{
		Key: []byte("/registry/watch/cancel-order"), StartRevision: 10,
	})
	require.Equal(t, uint64(10), <-called)
	results <- etcdproxy.WatchResult{Revision: 10, Events: []*mvccpb.Event{{
		Type: mvccpb.PUT,
		Kv:   &mvccpb.KeyValue{Key: []byte("/registry/watch/cancel-order"), Value: []byte("must-not-follow-cancel"), CreateRevision: 10, ModRevision: 10, Version: 1},
	}}}
	<-headerEntered

	// The event was already assembled but had not committed to the stream. A
	// client cancellation removes the WatchId and sends its terminal response.
	// Reuse the same explicit ID before releasing the old generation: mere map
	// existence must not let that old event impersonate the replacement watch.
	w.CancelRequest(7)
	replacement := &watch{start: "/registry/watch/replacement", syncedRev: 19, sourceRev: 19}
	w.Lock()
	w.watches[7] = replacement
	w.Unlock()
	require.NoError(t, w.SendControl(&etcdserverpb.WatchResponse{
		Header: txnHeader(19), WatchId: 7, Created: true,
	}))
	responses := stream.snapshot()
	require.Len(t, responses, 2)
	require.True(t, responses[0].Canceled)
	require.Empty(t, responses[0].Events)
	require.True(t, responses[1].Created)
	require.False(t, responses[1].Canceled)
	close(headerRelease)
	w.wg.Wait()
	require.Equal(t, responses, stream.snapshot(), "the old generation must not publish through a reused WatchId")
}

func TestOldWatchErrorCannotCancelReusedID(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()
	results := make(chan etcdproxy.WatchResult, 1)
	called := make(chan uint64, 1)
	server.backend = &roleSwitchWatchBackend{BackendShim: server.backend, local: results, called: called}
	server.peers = testPeerService{isLeader: true}

	ctx, cancel := context.WithCancel(context.Background())
	stream := &createCallbackWatchServer{fakeWatchServer: &fakeWatchServer{ctx: ctx}}
	oldGeneration := &watch{cancel: cancel, start: "/registry/watch/old-error", syncedRev: 9, sourceRev: 9}
	w := &watcher{
		backend: server.backend, watchServer: stream, grpcServer: server,
		watches: map[int64]*watch{7: oldGeneration}, metricCli: server.metricCli,
	}
	w.wg.Add(1)
	go w.Watch(ctx, 7, &etcdserverpb.WatchCreateRequest{
		Key: []byte("/registry/watch/old-error"), StartRevision: 10,
	})
	require.Equal(t, uint64(10), <-called)
	w.CancelRequest(7)

	replacement := &watch{start: "/registry/watch/replacement", syncedRev: 19, sourceRev: 19}
	w.Lock()
	w.watches[7] = replacement
	w.Unlock()
	require.NoError(t, w.SendControl(&etcdserverpb.WatchResponse{
		Header: txnHeader(19), WatchId: 7, Created: true,
	}))

	// The canceled producer ignores its context and reports a late failure. Its
	// generation-bound cancel must not close the replacement sharing ID 7.
	results <- etcdproxy.WatchResult{Err: errors.New("late old-generation failure")}
	w.wg.Wait()
	responses := stream.snapshot()
	require.Len(t, responses, 2)
	require.True(t, responses[0].Canceled)
	require.True(t, responses[1].Created)
	w.Lock()
	require.Same(t, replacement, w.watches[7])
	w.Unlock()
}

func TestCompactedWatchKeepsIDReservedUntilTerminalControlIsOrdered(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()
	backend := &blockingCancelCompactRevisionBackend{
		BackendShim: server.backend,
		entered:     make(chan struct{}),
		release:     make(chan struct{}),
	}
	stream := &createCallbackWatchServer{fakeWatchServer: &fakeWatchServer{ctx: context.Background()}}
	w := &watcher{
		backend: backend, watchServer: stream, grpcServer: server,
		watches:   map[int64]*watch{7: {cancel: func() {}, start: "/registry/watch/old"}},
		metricCli: server.metricCli,
	}

	cancelDone := make(chan struct{})
	go func() {
		defer close(cancelDone)
		w.Cancel(7, compactedRevisionError(), true)
	}()
	<-backend.entered

	// The old generation is already closing, but its compact revision (and thus
	// terminal frame) is not ready. Reusing ID 7 now must be rejected as a
	// duplicate rather than emitting Created(new) ahead of Canceled(old).
	w.Start(context.Background(), &etcdserverpb.WatchCreateRequest{
		Key: []byte("/registry/watch/replacement"), WatchId: 7, StartRevision: 20,
	})
	responses := stream.snapshot()
	require.Len(t, responses, 1)
	require.True(t, responses[0].Created)
	require.True(t, responses[0].Canceled)
	require.Equal(t, int64(-1), responses[0].WatchId)
	require.Contains(t, responses[0].CancelReason, "duplicate watch ID")

	close(backend.release)
	<-cancelDone
	responses = stream.snapshot()
	require.Len(t, responses, 2)
	require.True(t, responses[1].Canceled)
	require.Equal(t, int64(7), responses[1].WatchId)
	require.Equal(t, int64(12), responses[1].CompactRevision)

	// Once the terminal control is ordered, the explicit ID is reusable.
	newCtx, cancelNew := context.WithCancel(context.Background())
	w.Start(newCtx, &etcdserverpb.WatchCreateRequest{
		Key: []byte("/registry/watch/replacement"), WatchId: 7, StartRevision: 20,
	})
	responses = stream.snapshot()
	require.Len(t, responses, 3)
	require.True(t, responses[2].Created)
	require.False(t, responses[2].Canceled)
	require.Equal(t, int64(7), responses[2].WatchId)

	cancelNew()
	w.Close()
}

func TestCompactedWatchCancelDoesNotWaitIndefinitelyForTiKV(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()
	backend := &blockingCancelCompactRevisionBackend{
		BackendShim: server.backend,
		entered:     make(chan struct{}),
		release:     make(chan struct{}),
	}
	stream := &fakeWatchServer{ctx: context.Background()}
	w := &watcher{
		backend: backend, watchServer: stream, grpcServer: server,
		watches:   map[int64]*watch{7: {cancel: func() {}}},
		metricCli: server.metricCli,
	}

	start := time.Now()
	w.Cancel(7, compactedRevisionError(), true)
	require.Less(t, time.Since(start), time.Second)
	select {
	case <-backend.entered:
	default:
		t.Fatal("compacted cancellation did not probe the durable watermark")
	}
	responses := stream.sentResponses()
	require.Len(t, responses, 1)
	require.True(t, responses[0].Canceled)
	require.Equal(t, int64(7), responses[0].WatchId)
	require.Equal(t, int64(1), responses[0].CompactRevision)
	w.Lock()
	_, reserved := w.watches[7]
	w.Unlock()
	require.False(t, reserved)
}

func TestSlowWatchCancelReleasesQuotaOnceBeforeConcurrentClose(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()
	server.SetMaxWatches(1)
	server.activeWatches = 1
	backend := &blockingCancelCompactRevisionBackend{
		BackendShim: server.backend,
		entered:     make(chan struct{}),
		release:     make(chan struct{}),
	}
	w := &watcher{
		backend: backend, watchServer: &fakeWatchServer{ctx: context.Background()}, grpcServer: server,
		watches:   map[int64]*watch{7: {cancel: func() {}, quotaHeld: true}},
		metricCli: server.metricCli,
	}

	cancelDone := make(chan struct{})
	go func() {
		defer close(cancelDone)
		w.Cancel(7, compactedRevisionError(), true)
	}()
	<-backend.entered
	require.Zero(t, server.activeWatches,
		"a closing logical watch must release admission quota before terminal metadata lookup finishes")
	require.True(t, server.acquireWatch(), "slow terminal ordering must not block unrelated watch admission")
	server.releaseWatch()
	w.Close()
	require.Zero(t, server.activeWatches, "Close must not double-release quota from the retained closing entry")

	close(backend.release)
	<-cancelDone
	require.Zero(t, server.activeWatches)
	require.True(t, server.acquireWatch(), "quota must remain reusable after cancellation finishes")
	server.releaseWatch()
}

func TestSlowWatchCancelIsExcludedFromStreamProgress(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()
	backend := &blockingCancelCompactRevisionBackend{
		BackendShim: server.backend,
		entered:     make(chan struct{}),
		release:     make(chan struct{}),
	}
	wt := &watch{cancel: func() {}, syncedRev: 11}
	w := &watcher{
		backend: backend, watchServer: &fakeWatchServer{ctx: context.Background()}, grpcServer: server,
		watches:   map[int64]*watch{7: wt},
		metricCli: server.metricCli,
	}

	cancelDone := make(chan struct{})
	go func() {
		defer close(cancelDone)
		w.CancelGeneration(7, wt, compactedRevisionError(), true)
	}()
	<-backend.entered

	// The ID remains reserved until the compacted terminal response is ordered,
	// but upstream has already removed this logical watcher from progressAll.
	require.True(t, wt.closing.Load())
	snapshot, allEligible := w.progressSyncedRevSnapshot()
	require.Empty(t, snapshot)
	require.False(t, allEligible)
	_, active := w.minSyncedRevision()
	require.False(t, active)
	w.Lock()
	w.watches[8] = &watch{syncedRev: 15}
	w.Unlock()
	snapshot, allEligible = w.progressSyncedRevSnapshot()
	require.True(t, allEligible, "a closing generation must not hold back an active synchronized watch")
	require.Equal(t, map[int64]uint64{8: 15}, snapshot)
	rev, active := w.minSyncedRevision()
	require.True(t, active)
	require.Equal(t, uint64(15), rev)

	close(backend.release)
	<-cancelDone
}

func TestFollowerWatchResumesLocallyAfterProxyGenerationCloses(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	localCh := make(chan etcdproxy.WatchResult, 1)
	proxyCh := make(chan etcdproxy.WatchResult, 1)
	localCalled := make(chan uint64, 1)
	backend := &roleSwitchWatchBackend{BackendShim: server.backend, local: localCh, called: localCalled}
	server.backend = backend
	var leading atomic.Bool
	proxyCalled := make(chan uint64, 1)
	server.peers = testPeerService{
		isLeaderFn:   leading.Load,
		proxyEnabled: true,
		watchFn: func(_ context.Context, key, rangeEnd []byte, revision uint64) (<-chan etcdproxy.WatchResult, error) {
			require.Equal(t, []byte("/registry/watch/local/"), key)
			require.Equal(t, []byte("/registry/watch/local0"), rangeEnd)
			proxyCalled <- revision
			return proxyCh, nil
		},
	}

	ctx, cancel := context.WithCancel(context.Background())
	stream := &createCallbackWatchServer{fakeWatchServer: &fakeWatchServer{ctx: ctx}}
	w := &watcher{
		backend:     server.backend,
		watchServer: stream,
		grpcServer:  server,
		watches: map[int64]*watch{
			7: {start: "/registry/watch/local/", end: "/registry/watch/local0", syncedRev: 9},
		},
		metricCli: server.metricCli,
	}
	w.wg.Add(1)
	done := make(chan struct{})
	go func() {
		defer close(done)
		w.Watch(ctx, 7, &etcdserverpb.WatchCreateRequest{
			Key: []byte("/registry/watch/local/"), RangeEnd: []byte("/registry/watch/local0"), StartRevision: 10,
		})
	}()
	require.Equal(t, uint64(10), <-proxyCalled)
	proxyCh <- etcdproxy.WatchResult{Revision: 10, Events: []*mvccpb.Event{{
		Type: mvccpb.PUT, Kv: &mvccpb.KeyValue{Key: []byte("/registry/watch/local/a"), Value: []byte("proxy"), CreateRevision: 10, ModRevision: 10, Version: 1},
	}}}
	require.Eventually(t, func() bool { return len(stream.snapshot()) == 1 }, time.Second, time.Millisecond)

	leading.Store(true)
	close(proxyCh)
	require.Equal(t, uint64(11), <-localCalled, "resume must start after the last successfully delivered revision")
	localCh <- etcdproxy.WatchResult{Revision: 11, Events: []*mvccpb.Event{{
		Type: mvccpb.PUT, Kv: &mvccpb.KeyValue{Key: []byte("/registry/watch/local/b"), Value: []byte("local"), CreateRevision: 11, ModRevision: 11, Version: 1},
	}}}
	require.Eventually(t, func() bool { return len(stream.snapshot()) == 2 }, time.Second, time.Millisecond)
	for _, response := range stream.snapshot() {
		require.False(t, response.Canceled)
	}

	cancel()
	close(localCh)
	<-done
}

func TestFollowerWatchRejectsOutOfRangeProxyEvent(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()
	rec := &recordingMetrics{}
	initWatchBackendIntegrityMetrics(rec)
	proxyCh := make(chan etcdproxy.WatchResult, 1)
	server.peers = testPeerService{
		isLeader:     false,
		proxyEnabled: true,
		epochFn:      func() (uint64, bool) { return 7, false },
		watchFn: func(_ context.Context, key, rangeEnd []byte, revision uint64) (<-chan etcdproxy.WatchResult, error) {
			require.Equal(t, []byte("/registry/watch/scope/"), key)
			require.Equal(t, []byte("/registry/watch/scope0"), rangeEnd)
			require.Equal(t, uint64(10), revision)
			return proxyCh, nil
		},
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	stream := &fakeWatchServer{ctx: ctx}
	wt := &watch{start: "/registry/watch/scope/", end: "/registry/watch/scope0", syncedRev: 9}
	w := &watcher{
		backend: server.backend, watchServer: stream, grpcServer: server,
		watches: map[int64]*watch{7: wt}, metricCli: rec,
	}
	w.wg.Add(1)
	done := make(chan struct{})
	go func() {
		defer close(done)
		w.Watch(ctx, 7, &etcdserverpb.WatchCreateRequest{
			Key: []byte("/registry/watch/scope/"), RangeEnd: []byte("/registry/watch/scope0"), StartRevision: 10,
		})
	}()
	proxyCh <- etcdproxy.WatchResult{Revision: 10, Events: []*mvccpb.Event{{
		Type: mvccpb.PUT,
		Kv:   &mvccpb.KeyValue{Key: []byte("/registry/watch/other"), CreateRevision: 10, ModRevision: 10, Version: 1},
	}}}

	require.Eventually(t, func() bool { return len(stream.sentResponses()) == 1 }, time.Second, time.Millisecond)
	response := stream.sentResponses()[0]
	require.True(t, response.GetCanceled())
	require.Contains(t, response.GetCancelReason(), "outside the requested range")
	require.Empty(t, response.GetEvents())
	require.Equal(t, uint64(0), atomic.LoadUint64(&wt.sourceRev), "rejected proxy payload must not advance the source watermark")
	require.Equal(t, []interface{}{0, 1}, recordedWatchBackendIntegrityValues(rec, "invalid_result"))
	<-done
	close(proxyCh)
}

func TestWatchBackendCloseReportsCompactionWhenNextRevisionWasCompacted(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	first, err := server.Put(context.Background(), &etcdserverpb.PutRequest{
		Key: []byte("/registry/watch/slow"), Value: []byte("one"),
	})
	require.NoError(t, err)
	second, err := server.Put(context.Background(), &etcdserverpb.PutRequest{
		Key: []byte("/registry/watch/slow"), Value: []byte("two"),
	})
	require.NoError(t, err)
	compactState := &staleCompactRevisionCacheShim{BackendShim: server.backend}
	server.backend = compactState
	proxyCh := make(chan etcdproxy.WatchResult)
	watchStarted := make(chan struct{})
	server.peers = testPeerService{
		isLeader: false, proxyEnabled: true,
		watchFn: func(context.Context, []byte, []byte, uint64) (<-chan etcdproxy.WatchResult, error) {
			close(watchStarted)
			return proxyCh, nil
		},
	}
	stream := &fakeWatchServer{ctx: context.Background()}
	w := &watcher{
		backend: server.backend, watchServer: stream, grpcServer: server,
		watches: map[int64]*watch{7: {
			start: "/registry/watch/slow", syncedRev: uint64(first.GetHeader().GetRevision()) - 1,
		}},
		metricCli: server.metricCli,
	}
	w.wg.Add(1)
	watchDone := make(chan struct{})
	go func() {
		defer close(watchDone)
		w.Watch(context.Background(), 7, &etcdserverpb.WatchCreateRequest{
			Key: []byte("/registry/watch/slow"), StartRevision: first.GetHeader().GetRevision(),
		})
	}()
	<-watchStarted
	compactState.durableRevision = uint64(second.GetHeader().GetRevision())
	close(proxyCh)
	<-watchDone

	require.Len(t, stream.sent, 1)
	response := stream.sent[0]
	require.True(t, response.GetCanceled())
	require.Equal(t, second.GetHeader().GetRevision(), response.GetCompactRevision())
	require.Empty(t, response.GetCancelReason())
}

func TestNextWatchRevisionCompactedKeepsCompactBoundaryWatchable(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	const compactRevision = uint64(10)
	compactState := &staleCompactRevisionCacheShim{
		BackendShim: server.backend, durableRevision: compactRevision,
	}
	w := &watcher{backend: compactState, watches: map[int64]*watch{
		1: {syncedRev: compactRevision - 1},
		2: {syncedRev: compactRevision - 2},
	}}
	require.False(t, w.nextWatchRevisionCompacted(context.Background(), 1),
		"the event at exactly the compact revision remains watchable")
	require.True(t, w.nextWatchRevisionCompacted(context.Background(), 2),
		"a next revision below the compact watermark requires a re-list")
}

func TestNextWatchRevisionCompactionProbeIsBounded(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()
	backend := &blockingCancelCompactRevisionBackend{
		BackendShim: server.backend, entered: make(chan struct{}), release: make(chan struct{}),
	}
	w := &watcher{backend: backend, watches: map[int64]*watch{1: {syncedRev: 7}}}
	start := time.Now()
	require.False(t, w.nextWatchRevisionCompacted(context.Background(), 1))
	require.Less(t, time.Since(start), time.Second)
	select {
	case <-backend.entered:
	default:
		t.Fatal("compaction fallback did not probe durable state")
	}
}

func TestFollowerFromNowWatchUsesSynchronizedRevisionFence(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	proxyCh := make(chan etcdproxy.WatchResult, 1)
	proxyCh <- etcdproxy.WatchResult{Created: true, Revision: 50}
	var watchedRevision atomic.Uint64
	server.peers = testPeerService{
		isLeader:     false,
		proxyEnabled: true,
		syncReadFn: func(context.Context) error {
			server.backend.SetCurrentRevision(50)
			return nil
		},
		watchFn: func(watchCtx context.Context, key, rangeEnd []byte, revision uint64) (<-chan etcdproxy.WatchResult, error) {
			watchedRevision.Store(revision)
			go func() {
				<-watchCtx.Done()
				close(proxyCh)
			}()
			return proxyCh, nil
		},
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	stream := &controllableWatchServer{
		ctx: ctx, recv: make(chan *etcdserverpb.WatchRequest, 1),
	}
	done := make(chan error, 1)
	go func() { done <- server.Watch(stream) }()
	stream.recv <- &etcdserverpb.WatchRequest{RequestUnion: &etcdserverpb.WatchRequest_CreateRequest{CreateRequest: &etcdserverpb.WatchCreateRequest{
		Key: []byte("/registry/watch/fenced"), WatchId: 77,
	}}}
	require.Eventually(t, func() bool { return len(stream.snapshot()) == 1 }, time.Second, time.Millisecond)
	cancel()
	requireWatchCanceled(t, <-done)
	require.Equal(t, uint64(51), watchedRevision.Load(), "from-now follower watch must subscribe at synchronized R+1")
	responses := stream.snapshot()
	require.Equal(t, int64(50), responses[0].Header.Revision)
	require.True(t, responses[0].Created)
}

func TestFollowerWatchReadBarrierFailureIsRetryable(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	barrierErr := errors.New("leader revision transport failed")
	server.peers = testPeerService{
		isLeader:   false,
		syncReadFn: func(context.Context) error { return barrierErr },
	}
	stream := &scriptedWatchServer{
		fakeWatchServer: &fakeWatchServer{ctx: context.Background()},
		reqs: []*etcdserverpb.WatchRequest{{
			RequestUnion: &etcdserverpb.WatchRequest_CreateRequest{
				CreateRequest: &etcdserverpb.WatchCreateRequest{Key: []byte("/watch/barrier")},
			},
		}},
	}
	err := server.Watch(stream)
	requireWatchStatusError(t, err, codes.Unavailable, barrierErr.Error())
	require.Empty(t, stream.sent, "a watch without a revision fence must not be created")
}

func TestStaleFreshnessWatchCannotOpenLocalGeneration(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()
	var barrierCalls atomic.Int64
	server.peers = testPeerService{
		isLeaderFn: func() bool { return true },
		epochFn:    func() (uint64, bool) { return 7, false },
		syncReadFn: func(context.Context) error {
			barrierCalls.Add(1)
			return nil
		},
	}
	stream := &scriptedWatchServer{
		fakeWatchServer: &fakeWatchServer{ctx: context.Background()},
		reqs: []*etcdserverpb.WatchRequest{{
			RequestUnion: &etcdserverpb.WatchRequest_CreateRequest{
				CreateRequest: &etcdserverpb.WatchCreateRequest{Key: []byte("/watch/stale-freshness")},
			},
		}},
	}

	err := server.Watch(stream)
	requireWatchStatusError(t, err, codes.Unavailable, "watch error addr is test-peer leader test-peer")
	require.Equal(t, int64(1), barrierCalls.Load(), "a stale leader flag must not bypass the control revision fence")
	require.Empty(t, stream.sent, "a self-fenced replica must not publish a local watch generation")
	require.Zero(t, server.activeWatches, "failed stale creation must release its watch slot")
}

func TestStaleFreshnessWatchUsesFollowerProxyGeneration(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()
	proxyCh := make(chan etcdproxy.WatchResult, 1)
	proxyCh <- etcdproxy.WatchResult{Created: true, Revision: 50}
	var watchedRevision atomic.Uint64
	server.peers = testPeerService{
		isLeaderFn:   func() bool { return true },
		epochFn:      func() (uint64, bool) { return 7, false },
		proxyEnabled: true,
		syncReadFn: func(context.Context) error {
			server.backend.SetCurrentRevision(50)
			return nil
		},
		watchFn: func(watchCtx context.Context, _, _ []byte, revision uint64) (<-chan etcdproxy.WatchResult, error) {
			watchedRevision.Store(revision)
			go func() {
				<-watchCtx.Done()
				close(proxyCh)
			}()
			return proxyCh, nil
		},
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	stream := &controllableWatchServer{
		ctx: ctx, recv: make(chan *etcdserverpb.WatchRequest, 1),
	}
	done := make(chan error, 1)
	go func() { done <- server.Watch(stream) }()
	stream.recv <- &etcdserverpb.WatchRequest{RequestUnion: &etcdserverpb.WatchRequest_CreateRequest{
		CreateRequest: &etcdserverpb.WatchCreateRequest{Key: []byte("/watch/stale-proxy"), WatchId: 418},
	}}
	require.Eventually(t, func() bool { return len(stream.snapshot()) == 1 }, time.Second, time.Millisecond)
	cancel()
	requireWatchCanceled(t, <-done)
	require.Equal(t, uint64(51), watchedRevision.Load(),
		"a stale leader flag must use the synchronized follower R+1 proxy generation")
	require.True(t, stream.snapshot()[0].Created)
}

func TestFollowerWatchLocalRejectionsPrecedeReadBarrier(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	barrierErr := errors.New("leader revision transport failed")
	var barrierCalls atomic.Int64
	server.peers = testPeerService{
		isLeader: false,
		syncReadFn: func(context.Context) error {
			barrierCalls.Add(1)
			return barrierErr
		},
	}
	stream := &scriptedWatchServer{
		fakeWatchServer: &fakeWatchServer{ctx: context.Background()},
		reqs: []*etcdserverpb.WatchRequest{
			{RequestUnion: &etcdserverpb.WatchRequest_CreateRequest{CreateRequest: &etcdserverpb.WatchCreateRequest{
				Key: []byte("/watch/negative"), StartRevision: -1,
			}}},
			{RequestUnion: &etcdserverpb.WatchRequest_CreateRequest{CreateRequest: &etcdserverpb.WatchCreateRequest{
				Key: []byte("/watch/invalid"), RangeEnd: []byte("/watch/invalid"),
			}}},
			{RequestUnion: &etcdserverpb.WatchRequest_CreateRequest{CreateRequest: &etcdserverpb.WatchCreateRequest{
				Key: []byte("/watch/requires-fence"),
			}}},
		},
	}
	err := server.Watch(stream)
	requireWatchStatusError(t, err, codes.Unavailable, barrierErr.Error())
	require.Equal(t, int64(1), barrierCalls.Load(), "only the valid create should enter the read barrier")
	require.Len(t, stream.sent, 2)
	require.Equal(t, rpctypes.ErrCompacted.Error(), stream.sent[0].CancelReason)
	require.Equal(t, "mvcc: watcher range is empty", stream.sent[1].CancelReason)
}

func TestFollowerWatchDuplicateIDPrecedesReadBarrier(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	barrierErr := errors.New("leader revision transport failed")
	var barrierCalls atomic.Int64
	proxyResults := make(chan etcdproxy.WatchResult, 1)
	server.peers = testPeerService{
		isLeader:     false,
		proxyEnabled: true,
		syncReadFn: func(context.Context) error {
			if barrierCalls.Add(1) == 1 {
				server.backend.SetCurrentRevision(50)
				return nil
			}
			return barrierErr
		},
		watchFn: func(ctx context.Context, _ []byte, _ []byte, _ uint64) (<-chan etcdproxy.WatchResult, error) {
			proxyResults <- etcdproxy.WatchResult{Created: true, Revision: 50}
			go func() {
				<-ctx.Done()
				close(proxyResults)
			}()
			return proxyResults, nil
		},
	}
	stream := &scriptedWatchServer{
		fakeWatchServer: &fakeWatchServer{ctx: context.Background()},
		reqs: []*etcdserverpb.WatchRequest{
			{RequestUnion: &etcdserverpb.WatchRequest_CreateRequest{CreateRequest: &etcdserverpb.WatchCreateRequest{
				Key: []byte("/watch/existing"), WatchId: 414,
			}}},
			{RequestUnion: &etcdserverpb.WatchRequest_CreateRequest{CreateRequest: &etcdserverpb.WatchCreateRequest{
				Key: []byte("/watch/duplicate"), WatchId: 414,
			}}},
			{RequestUnion: &etcdserverpb.WatchRequest_CreateRequest{CreateRequest: &etcdserverpb.WatchCreateRequest{
				Key: []byte("/watch/requires-fence"), WatchId: 415,
			}}},
		},
	}
	err := server.Watch(stream)
	requireWatchStatusError(t, err, codes.Unavailable, barrierErr.Error())
	require.Equal(t, int64(2), barrierCalls.Load(), "duplicate ID must not enter the read barrier")
	require.Contains(t, []int{1, 2}, len(stream.sent), "the later barrier may close the stream before async Created is sent")
	duplicate := stream.sent[len(stream.sent)-1]
	require.True(t, duplicate.Created)
	require.True(t, duplicate.Canceled)
	require.Equal(t, int64(-1), duplicate.WatchId)
	require.Equal(t, "mvcc: duplicate watch ID provided on the WatchStream", duplicate.CancelReason)
	if len(stream.sent) == 2 {
		require.True(t, stream.sent[0].Created)
		require.False(t, stream.sent[0].Canceled)
		require.Equal(t, int64(414), stream.sent[0].WatchId)
	}
}

func TestFollowerWatchCancelPrecedesReadBarrier(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	barrierErr := errors.New("leader revision transport failed")
	var barrierCalls atomic.Int64
	proxyResults := make(chan etcdproxy.WatchResult, 1)
	proxyCanceled := make(chan struct{})
	server.peers = testPeerService{
		isLeader:     false,
		proxyEnabled: true,
		putFn: func(context.Context, *etcdserverpb.PutRequest) (*etcdserverpb.PutResponse, error) {
			return &etcdserverpb.PutResponse{Header: txnHeader(61)}, nil
		},
		syncReadFn: func(context.Context) error {
			if barrierCalls.Add(1) == 1 {
				server.backend.SetCurrentRevision(60)
				return nil
			}
			return barrierErr
		},
		watchFn: func(ctx context.Context, _ []byte, _ []byte, _ uint64) (<-chan etcdproxy.WatchResult, error) {
			proxyResults <- etcdproxy.WatchResult{Created: true, Revision: 60}
			go func() {
				<-ctx.Done()
				close(proxyCanceled)
				close(proxyResults)
			}()
			return proxyResults, nil
		},
	}
	putResponse, err := server.Put(context.Background(), &etcdserverpb.PutRequest{
		Key:   []byte("/watch/proxied-write"),
		Value: []byte("value"),
	})
	require.NoError(t, err)
	require.Equal(t, int64(61), putResponse.Header.Revision)
	require.Equal(t, uint64(61), server.backend.GetCurrentRevision())

	stream := &scriptedWatchServer{
		fakeWatchServer: &fakeWatchServer{ctx: context.Background()},
		reqs: []*etcdserverpb.WatchRequest{
			{RequestUnion: &etcdserverpb.WatchRequest_CreateRequest{CreateRequest: &etcdserverpb.WatchCreateRequest{
				Key: []byte("/watch/cancel-local"), WatchId: 415,
			}}},
			{RequestUnion: &etcdserverpb.WatchRequest_CancelRequest{CancelRequest: &etcdserverpb.WatchCancelRequest{
				WatchId: 415,
			}}},
			{RequestUnion: &etcdserverpb.WatchRequest_CreateRequest{CreateRequest: &etcdserverpb.WatchCreateRequest{
				Key: []byte("/watch/requires-fence"), WatchId: 416,
			}}},
		},
	}
	err = server.Watch(stream)
	requireWatchStatusError(t, err, codes.Unavailable, barrierErr.Error())
	require.Equal(t, int64(2), barrierCalls.Load(), "client cancel must not enter the read barrier")
	stream.mu.Lock()
	responses := append([]*etcdserverpb.WatchResponse(nil), stream.sent...)
	stream.mu.Unlock()
	// The later barrier error may terminate the whole RPC before either async
	// control is delivered. If delivery wins that race, cancellation must never
	// overtake the authoritative Created acknowledgement.
	require.Contains(t, []int{0, 1, 2}, len(responses))
	if len(responses) >= 1 {
		require.True(t, responses[0].Created)
		require.False(t, responses[0].Canceled)
		require.Equal(t, int64(415), responses[0].WatchId)
	}
	if len(responses) == 2 {
		canceled := responses[1]
		require.True(t, canceled.Canceled)
		require.Empty(t, canceled.CancelReason)
		require.Equal(t, int64(415), canceled.WatchId)
		require.Equal(t, int64(61), canceled.Header.Revision)
	}
	require.Eventually(t, func() bool {
		select {
		case <-proxyCanceled:
			return true
		default:
			return false
		}
	}, time.Second, time.Millisecond)
}

func TestFollowerWatchQuotaRejectionPrecedesReadBarrier(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()
	server.SetMaxWatches(1)
	server.activeWatches = 1

	var barrierCalls atomic.Int64
	server.peers = testPeerService{
		isLeader: false,
		syncReadFn: func(context.Context) error {
			barrierCalls.Add(1)
			return errors.New("leader revision transport failed")
		},
	}
	stream := &scriptedWatchServer{
		fakeWatchServer: &fakeWatchServer{ctx: context.Background()},
		reqs: []*etcdserverpb.WatchRequest{{
			RequestUnion: &etcdserverpb.WatchRequest_CreateRequest{CreateRequest: &etcdserverpb.WatchCreateRequest{
				Key: []byte("/watch/quota-local"), WatchId: 416,
			}},
		}},
	}
	err := server.Watch(stream)
	requireWatchCanceled(t, err)
	require.Zero(t, barrierCalls.Load(), "a locally full watch quota must not enter the read barrier")
	require.Len(t, stream.sent, 1)
	require.True(t, stream.sent[0].Created)
	require.True(t, stream.sent[0].Canceled)
	require.Equal(t, int64(-1), stream.sent[0].WatchId)
	require.Equal(t, watchQuotaCancelReason, stream.sent[0].CancelReason)
	require.Equal(t, int64(1), server.activeWatches)
}

func TestFollowerWatchQuotaReservationReleasedOnCreateFailure(t *testing.T) {
	for _, test := range []struct {
		name       string
		barrierErr error
		wantErr    string
	}{
		{name: "read barrier failure", barrierErr: errors.New("leader revision transport failed"), wantErr: "leader revision transport failed"},
		{name: "proxy disabled after barrier", wantErr: "watch error addr is test-peer leader test-peer"},
	} {
		t.Run(test.name, func(t *testing.T) {
			server, closeFn := newTestRPCServer(t)
			defer closeFn()
			server.SetMaxWatches(1)
			server.peers = testPeerService{
				isLeader: false,
				syncReadFn: func(context.Context) error {
					return test.barrierErr
				},
			}
			stream := &scriptedWatchServer{
				fakeWatchServer: &fakeWatchServer{ctx: context.Background()},
				reqs: []*etcdserverpb.WatchRequest{{
					RequestUnion: &etcdserverpb.WatchRequest_CreateRequest{CreateRequest: &etcdserverpb.WatchCreateRequest{
						Key: []byte("/watch/quota-release"), WatchId: 417,
					}},
				}},
			}
			err := server.Watch(stream)
			requireWatchStatusError(t, err, codes.Unavailable, test.wantErr)
			require.Zero(t, server.activeWatches)
			require.True(t, server.acquireWatch(), "failed create must return its reserved watch slot")
			server.releaseWatch()
		})
	}
}

func requireWatchStatusError(t *testing.T, err error, code codes.Code, message string) {
	t.Helper()
	require.EqualError(t, err, status.Error(code, message).Error())
	require.Equal(t, code, status.Code(err))
	require.Equal(t, message, status.Convert(err).Message())
}

func requireWatchCanceled(t *testing.T, err error) {
	t.Helper()
	require.EqualError(t, err, status.Error(codes.Canceled, "etcdserver: watch canceled").Error())
	require.Equal(t, codes.Canceled, status.Code(err))
	require.Equal(t, "etcdserver: watch canceled", status.Convert(err).Message())
}

func TestLeaderFromNowWatchReplaysWriteDuringCreatedResponse(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	key := []byte("/registry/watch/create-gap")
	stream := &createCallbackWatchServer{
		fakeWatchServer: &fakeWatchServer{ctx: ctx},
		onCreated: func() {
			_, err := server.Put(ctx, &etcdserverpb.PutRequest{Key: key, Value: []byte("during-created")})
			require.NoError(t, err)
		},
	}
	w := &watcher{
		backend:     server.backend,
		grpcServer:  server,
		watchServer: stream,
		watches:     make(map[int64]*watch),
		metricCli:   server.metricCli,
	}

	w.Start(ctx, &etcdserverpb.WatchCreateRequest{Key: key})
	require.Eventually(t, func() bool {
		return len(stream.snapshot()) >= 2
	}, 5*time.Second, 10*time.Millisecond)
	responses := stream.snapshot()
	require.True(t, responses[0].Created)
	require.Len(t, responses[1].Events, 1)
	require.Equal(t, key, responses[1].Events[0].Kv.Key)
	require.Equal(t, []byte("during-created"), responses[1].Events[0].Kv.Value)
	w.Close()
}

func TestClientWatchCancelUsesControlRevision(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()
	stream := &fakeWatchServer{ctx: context.Background()}
	w := &watcher{
		backend:     server.backend,
		grpcServer:  server,
		watchServer: stream,
		watches: map[int64]*watch{
			77: {cancel: func() {}, start: "/registry/watch/fenced"},
		},
		controlRev: 50,
		metricCli:  server.metricCli,
	}
	w.CancelRequest(77)
	require.Len(t, stream.sent, 1)
	require.True(t, stream.sent[0].Canceled)
	require.Equal(t, int64(50), stream.sent[0].Header.Revision)
}

func TestWatchCreateBypassesStaleCompactRevisionCache(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	shim := &staleCompactRevisionCacheShim{
		BackendShim:     server.backend,
		cachedRevision:  1,
		durableRevision: 10,
	}
	w := &watcher{backend: shim}

	compacted, err := w.isCompactedWatchRevision(context.Background(), 9)
	require.NoError(t, err)
	require.True(t, compacted)
	require.True(t, shim.freshRead)
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
	require.Empty(t, resp.CancelReason)
	require.NotNil(t, resp.Header)
	require.Zero(t, resp.Header.Revision)
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

func TestWatchIgnoresInvalidControlMessagesAndKeepsStreamAlive(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	stream := &scriptedWatchServer{
		fakeWatchServer: &fakeWatchServer{ctx: context.Background()},
		reqs: []*etcdserverpb.WatchRequest{
			{},
			{RequestUnion: &etcdserverpb.WatchRequest_CreateRequest{}},
			{RequestUnion: &etcdserverpb.WatchRequest_CancelRequest{}},
			{RequestUnion: &etcdserverpb.WatchRequest_ProgressRequest{}},
			{RequestUnion: &etcdserverpb.WatchRequest_CreateRequest{
				CreateRequest: &etcdserverpb.WatchCreateRequest{Key: []byte("/watch/after-invalid-control"), WatchId: 404},
			}},
		},
	}

	err := server.Watch(stream)
	requireWatchCanceled(t, err)
	require.Len(t, stream.sent, 1)
	require.Equal(t, int64(404), stream.sent[0].WatchId)
	require.True(t, stream.sent[0].Created)
	require.False(t, stream.sent[0].Canceled)
	require.Empty(t, stream.sent[0].CancelReason)
	require.NotNil(t, stream.sent[0].Header)
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

func TestStreamProgressRevisionRequiresImmediateFullSynchronization(t *testing.T) {
	watches := map[int64]*watch{1: {syncedRev: 9}, 2: {syncedRev: 7}}
	w := &watcher{watches: watches}
	revision, ok, retry := w.progressRequestRevision(watchProgressRequest{target: 7, generations: watches})
	require.True(t, ok)
	require.False(t, retry)
	require.Equal(t, uint64(7), revision)

	_, ok, retry = w.progressRequestRevision(watchProgressRequest{target: 8, generations: watches})
	require.False(t, ok, "a stream-wide response must not pass the slowest watch")
	require.True(t, retry)
	future := &watch{syncedRev: 9, progressStartRevision: 10}
	w.watches = map[int64]*watch{1: future}
	_, ok, retry = w.progressRequestRevision(watchProgressRequest{
		target: 7, generations: map[int64]*watch{1: future},
	})
	require.False(t, ok, "an ineligible watch must suppress the stream-wide response")
	require.False(t, retry, "a watch whose start revision is future at request time cannot become eligible for that request")
	_, ok, retry = w.progressRequestRevision(watchProgressRequest{target: 1})
	require.False(t, ok, "etcd emits no progress response without an active watch")
	require.False(t, retry)
}

func BenchmarkHighCardinalityProgressRevisionCheck(b *testing.B) {
	const watchCount = 10_000
	watches := make(map[int64]*watch, watchCount)
	for id := int64(1); id <= watchCount; id++ {
		watches[id] = &watch{syncedRev: 50}
	}
	w := &watcher{watches: watches}
	request := watchProgressRequest{target: 50, generations: watches}
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		revision, synced, retry := w.progressRequestRevision(request)
		if revision != 50 || !synced || retry {
			panic("unexpected progress revision result")
		}
	}
}

// TestWatchProgressRequestNeverReportsBelowStart reproduces the progress
// notification data-loss bug: neither the global revision nor the synthetic
// StartRevision-1 watermark proves that this watch has consumed the matching
// FIFO event stream. If the backend catch-up marker races with the scripted
// client close, a response is optional, but it must never be below the start.
func TestWatchProgressRequestNeverReportsBelowStart(t *testing.T) {
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

	// Watch an idle key from the current revision. The scripted client requests
	// progress immediately and then closes, racing the backend catch-up marker.
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

	if err := server.Watch(stream); err != nil && status.Code(err) != codes.Canceled {
		t.Fatalf("watch returned unexpected error: %v", err)
	}

	// StartRevision-1 is only a resume floor, not proof of synchronization.
	for _, resp := range stream.fakeWatchServer.sent {
		if resp.WatchId == -1 && len(resp.Events) == 0 && !resp.Created && !resp.Canceled {
			require.GreaterOrEqual(t, resp.Header.Revision, startRev,
				"progress must be backed by this watch's delivered watermark")
		}
	}
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
		{Type: mvccpb.PUT, Kv: &mvccpb.KeyValue{Key: []byte("/registry/watch/buffered"), CreateRevision: 7, ModRevision: 7, Version: 1}},
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

type futureProgressBackend struct {
	BackendShim
	published atomic.Uint64
	interval  time.Duration
}

func (b *futureProgressBackend) GetPublishedRevision() uint64 {
	return b.published.Load()
}

func (b *futureProgressBackend) WatchProgressNotifyInterval() time.Duration {
	return b.interval
}

func TestFutureRevisionWatchSuppressesProgressUntilDelivered(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	backend := &futureProgressBackend{BackendShim: server.backend, interval: 5 * time.Millisecond}
	backend.published.Store(5)
	fed := make(chan etcdproxy.WatchResult)
	server.peers = testPeerService{
		isLeader:     false,
		proxyEnabled: true,
		watchFn: func(context.Context, []byte, []byte, uint64) (<-chan etcdproxy.WatchResult, error) {
			return fed, nil
		},
	}

	stream := &controllableWatchServer{ctx: context.Background()}
	wt := &watch{
		cancel:                func() {},
		start:                 "/registry/watch/future",
		progressStartRevision: 10,
		syncedRev:             9,
	}
	w := &watcher{
		backend:     backend,
		watchServer: stream,
		grpcServer:  server,
		watches:     map[int64]*watch{7: wt},
		metricCli:   server.metricCli,
	}

	snapshot, allEligible := w.progressSyncedRevSnapshot()
	require.False(t, allEligible)
	require.Empty(t, snapshot)
	w.wg.Add(1)
	done := make(chan struct{})
	go func() {
		defer close(done)
		w.Watch(context.Background(), 7, &etcdserverpb.WatchCreateRequest{
			Key:            []byte("/registry/watch/future"),
			StartRevision:  10,
			ProgressNotify: true,
		})
	}()
	require.Never(t, func() bool {
		return len(stream.snapshot()) != 0
	}, 30*time.Millisecond, 2*time.Millisecond)

	backend.published.Store(10)
	require.Never(t, func() bool {
		return len(stream.snapshot()) != 0
	}, 30*time.Millisecond, 2*time.Millisecond,
		"published revision alone must not bypass the watch's delivered watermark")

	fed <- etcdproxy.WatchResult{ProgressRevision: 10}
	require.Eventually(t, func() bool { return len(stream.snapshot()) >= 1 }, time.Second, time.Millisecond)
	sent := stream.snapshot()
	require.Equal(t, int64(10), sent[0].Header.Revision)
	require.Equal(t, int64(7), sent[0].WatchId)

	close(fed)
	<-done
}

func TestFilteredWatchAdvancesThroughFullBatchRevision(t *testing.T) {
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
	wt := &watch{start: "/registry/watch/filtered/"}
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
			Key:           []byte("/registry/watch/filtered/"),
			RangeEnd:      []byte("/registry/watch/filtered0"),
			StartRevision: 5,
			Filters:       []etcdserverpb.WatchCreateRequest_FilterType{etcdserverpb.WatchCreateRequest_NOPUT},
		})
	}()

	// A fully filtered PUT batch emits no response but still advances the
	// watcher's delivered watermark through the batch revision.
	fed <- etcdproxy.WatchResult{
		Revision: 9,
		Events: []*mvccpb.Event{{
			Type: mvccpb.PUT,
			Kv:   &mvccpb.KeyValue{Key: []byte("/registry/watch/filtered/a"), CreateRevision: 9, ModRevision: 9, Version: 1},
		}},
	}
	require.Eventually(t, func() bool {
		return atomic.LoadUint64(&wt.syncedRev) == 9
	}, time.Second, time.Millisecond)
	require.Empty(t, stream.sent)

	// The visible DELETE is older than a filtered PUT in the same covered
	// batch. etcd reports the batch revision, not the last visible event's rev.
	fed <- etcdproxy.WatchResult{
		Revision: 12,
		Events: []*mvccpb.Event{
			{Type: mvccpb.DELETE, Kv: &mvccpb.KeyValue{Key: []byte("/registry/watch/filtered/a"), ModRevision: 10}},
			{Type: mvccpb.PUT, Kv: &mvccpb.KeyValue{Key: []byte("/registry/watch/filtered/b"), CreateRevision: 12, ModRevision: 12, Version: 1}},
		},
	}
	require.Eventually(t, func() bool {
		return atomic.LoadUint64(&wt.syncedRev) == 12
	}, time.Second, time.Millisecond)
	require.Len(t, stream.sent, 1)
	require.Equal(t, int64(12), stream.sent[0].Header.Revision)
	require.Len(t, stream.sent[0].Events, 1)
	require.Equal(t, mvccpb.DELETE, stream.sent[0].Events[0].Type)

	close(fed)
	<-done
}

func TestWatchFilterEnumUnknownAndDuplicateMatchEtcd(t *testing.T) {
	for _, tc := range []struct {
		name      string
		filters   []etcdserverpb.WatchCreateRequest_FilterType
		wantTypes []mvccpb.Event_EventType
	}{
		{
			name: "unknown filter is ignored",
			filters: []etcdserverpb.WatchCreateRequest_FilterType{
				etcdserverpb.WatchCreateRequest_FilterType(99),
			},
			wantTypes: []mvccpb.Event_EventType{mvccpb.PUT, mvccpb.DELETE},
		},
		{
			name: "duplicate noput filters put once",
			filters: []etcdserverpb.WatchCreateRequest_FilterType{
				etcdserverpb.WatchCreateRequest_NOPUT,
				etcdserverpb.WatchCreateRequest_NOPUT,
			},
			wantTypes: []mvccpb.Event_EventType{mvccpb.DELETE},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
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
			w := &watcher{
				backend:     server.backend,
				watchServer: stream,
				grpcServer:  server,
				watches:     map[int64]*watch{17: {start: "/registry/watch/filter-enum/"}},
				metricCli:   server.metricCli,
			}
			w.wg.Add(1)
			done := make(chan struct{})
			go func() {
				defer close(done)
				w.Watch(context.Background(), 17, &etcdserverpb.WatchCreateRequest{
					Key: []byte("/registry/watch/filter-enum/"), RangeEnd: []byte("/registry/watch/filter-enum0"),
					StartRevision: 1, Filters: tc.filters,
				})
			}()

			fed <- etcdproxy.WatchResult{
				Revision: 3,
				Events: []*mvccpb.Event{
					{Type: mvccpb.PUT, Kv: &mvccpb.KeyValue{Key: []byte("/registry/watch/filter-enum/a"), CreateRevision: 2, ModRevision: 2, Version: 1}},
					{Type: mvccpb.DELETE, Kv: &mvccpb.KeyValue{Key: []byte("/registry/watch/filter-enum/a"), ModRevision: 3}},
				},
			}
			require.Eventually(t, func() bool { return len(stream.sentResponses()) == 1 }, time.Second, time.Millisecond)
			responses := stream.sentResponses()
			require.Len(t, responses[0].Events, len(tc.wantTypes))
			for i, want := range tc.wantTypes {
				require.Equal(t, want, responses[0].Events[i].Type)
			}
			require.Equal(t, int64(3), responses[0].Header.Revision)

			close(fed)
			<-done
		})
	}
}

// TestRewrittenFromNowWatchPreservesPublishedProgressFloor pins both sides of
// follower registration: the backend resumes from published+1 to close the
// create gap, while progress retains the client's original revision-zero floor.
func TestRewrittenFromNowWatchPreservesPublishedProgressFloor(t *testing.T) {
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
	w.start(ctx, &etcdserverpb.WatchCreateRequest{
		Key:            []byte("/registry/watch/from-now"),
		StartRevision:  int64(published) + 1,
		ProgressNotify: true,
	}, 0, false, true, false, nil, nil)

	// The rewritten watch is caught up through published, and must remain
	// immediately progress-eligible as an original from-now request.
	var seed uint64
	var progressStart uint64
	w.Lock()
	require.Len(t, w.watches, 1)
	for _, wt := range w.watches {
		seed = atomic.LoadUint64(&wt.syncedRev)
		progressStart = wt.progressStartRevision
	}
	w.Unlock()
	require.Equal(t, published, seed, "from-now watch must seed from published revision")
	require.Less(t, seed, current, "from-now watch must not seed from the pre-publish current revision")
	require.Zero(t, progressStart, "internal R+1 resume must not create a client-visible future watch")
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

type halfClosedWatchServer struct {
	*controllableWatchServer
	once sync.Once
}

type blockedCancelSendWatchServer struct {
	*controllableWatchServer
	sendCount         atomic.Int64
	recvCount         atomic.Int64
	cancelSendStarted chan struct{}
	releaseCancelSend chan struct{}
	thirdRequestRead  chan struct{}
}

type blockedAllSendWatchServer struct {
	*fakeWatchServer
	entered chan struct{}
	release chan struct{}
}

type requestTrackingWatchServer struct {
	*controllableWatchServer
	recvCount  atomic.Int64
	target     int64
	targetRead chan struct{}
}

type contextIgnoringWatchServer struct {
	*fakeWatchServer
	entered chan struct{}
	release chan struct{}
	exited  chan struct{}
}

func (s *contextIgnoringWatchServer) Recv() (*etcdserverpb.WatchRequest, error) {
	close(s.entered)
	<-s.release
	close(s.exited)
	return nil, context.Canceled
}

func (s *halfClosedWatchServer) Recv() (*etcdserverpb.WatchRequest, error) {
	var request *etcdserverpb.WatchRequest
	s.once.Do(func() { request = <-s.recv })
	if request != nil {
		return request, nil
	}
	return nil, io.EOF
}

func (s *blockedCancelSendWatchServer) Send(resp *etcdserverpb.WatchResponse) error {
	if s.sendCount.Add(1) == 2 {
		close(s.cancelSendStarted)
		select {
		case <-s.releaseCancelSend:
		case <-s.ctx.Done():
			return s.ctx.Err()
		}
	}
	return s.controllableWatchServer.Send(resp)
}

func (s *blockedAllSendWatchServer) Send(*etcdserverpb.WatchResponse) error {
	select {
	case s.entered <- struct{}{}:
	default:
	}
	select {
	case <-s.release:
		return nil
	case <-s.ctx.Done():
		return s.ctx.Err()
	}
}

func (s *requestTrackingWatchServer) Recv() (*etcdserverpb.WatchRequest, error) {
	request, err := s.controllableWatchServer.Recv()
	if err == nil && s.recvCount.Add(1) == s.target {
		close(s.targetRead)
	}
	return request, err
}

func goroutineStackOccurrences(needle string) int {
	buffer := make([]byte, 64<<10)
	for {
		length := runtime.Stack(buffer, true)
		if length < len(buffer) {
			return strings.Count(string(buffer[:length]), needle)
		}
		buffer = make([]byte, len(buffer)*2)
	}
}

func (s *blockedCancelSendWatchServer) Recv() (*etcdserverpb.WatchRequest, error) {
	request, err := s.controllableWatchServer.Recv()
	if err == nil && s.recvCount.Add(1) == 3 {
		close(s.thirdRequestRead)
	}
	return request, err
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

func TestFollowerWatchWaitsForAuthoritativeCreateAuthorization(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	results := make(chan etcdproxy.WatchResult, 1)
	opened := make(chan struct{})
	server.peers = testPeerService{
		proxyEnabled: true,
		watchFn: func(watchCtx context.Context, _ []byte, _ []byte, _ uint64) (<-chan etcdproxy.WatchResult, error) {
			close(opened)
			go func() {
				<-watchCtx.Done()
				close(results)
			}()
			return results, nil
		},
	}
	ctx, cancel := context.WithCancel(context.Background())
	stream := &controllableWatchServer{
		ctx:  ctx,
		recv: make(chan *etcdserverpb.WatchRequest, 1),
	}
	done := make(chan error, 1)
	go func() { done <- server.Watch(stream) }()
	stream.recv <- &etcdserverpb.WatchRequest{RequestUnion: &etcdserverpb.WatchRequest_CreateRequest{
		CreateRequest: &etcdserverpb.WatchCreateRequest{Key: []byte("/denied")},
	}}

	select {
	case <-opened:
	case <-time.After(time.Second):
		t.Fatal("follower did not open authoritative leader watch")
	}
	require.Empty(t, stream.snapshot(), "follower exposed Created before leader authorization")
	results <- etcdproxy.WatchResult{Err: rpctypes.ErrPermissionDenied}
	require.Eventually(t, func() bool { return len(stream.snapshot()) == 1 }, time.Second, time.Millisecond)
	responses := stream.snapshot()
	require.Len(t, responses, 1)
	require.True(t, responses[0].Created)
	require.True(t, responses[0].Canceled)
	require.Equal(t, int64(-1), responses[0].WatchId)
	require.Equal(t, rpctypes.ErrGRPCPermissionDenied.Error(), responses[0].CancelReason)

	cancel()
	require.Error(t, <-done)
}

func TestFollowerWatchRetriesClosedGenerationBeforeAuthoritativeCreate(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	first := make(chan etcdproxy.WatchResult)
	close(first)
	second := make(chan etcdproxy.WatchResult, 2)
	var calls atomic.Int64
	server.peers = testPeerService{
		proxyEnabled: true,
		watchFn: func(watchCtx context.Context, _ []byte, _ []byte, revision uint64) (<-chan etcdproxy.WatchResult, error) {
			require.Positive(t, revision)
			if calls.Add(1) == 1 {
				return first, nil
			}
			go func() {
				<-watchCtx.Done()
				close(second)
			}()
			return second, nil
		},
	}
	ctx, cancel := context.WithCancel(context.Background())
	stream := &controllableWatchServer{
		ctx:  ctx,
		recv: make(chan *etcdserverpb.WatchRequest, 1),
	}
	done := make(chan error, 1)
	go func() { done <- server.Watch(stream) }()
	stream.recv <- &etcdserverpb.WatchRequest{RequestUnion: &etcdserverpb.WatchRequest_CreateRequest{
		CreateRequest: &etcdserverpb.WatchCreateRequest{Key: []byte("/retry-create")},
	}}

	require.Eventually(t, func() bool { return calls.Load() == 2 }, time.Second, time.Millisecond)
	require.Empty(t, stream.snapshot(), "closed predecessor generation exposed a terminal cancel")
	second <- etcdproxy.WatchResult{Created: true, Revision: 99}
	require.Eventually(t, func() bool { return len(stream.snapshot()) == 1 }, time.Second, time.Millisecond)
	created := stream.snapshot()[0]
	require.True(t, created.Created)
	require.False(t, created.Canceled)

	second <- etcdproxy.WatchResult{Revision: 99, Events: []*mvccpb.Event{{
		Type: mvccpb.PUT,
		Kv: &mvccpb.KeyValue{
			Key: []byte("/retry-create"), Value: []byte("v"), CreateRevision: 99, ModRevision: 99, Version: 1,
		},
	}}}
	require.Eventually(t, func() bool { return len(stream.snapshot()) == 2 }, time.Second, time.Millisecond)
	require.Len(t, stream.snapshot()[1].Events, 1)

	cancel()
	require.Error(t, <-done)
}

func TestFollowerPendingAuthoritativeCreateReauthorizesAfterBecomingLeader(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	proxyResults := make(chan etcdproxy.WatchResult)
	localResults := make(chan etcdproxy.WatchResult, 1)
	localCalled := make(chan uint64, 1)
	server.backend = &roleSwitchWatchBackend{
		BackendShim: server.backend,
		local:       localResults,
		called:      localCalled,
	}
	var leading atomic.Bool
	proxyOpened := make(chan struct{})
	server.peers = testPeerService{
		proxyEnabled: true,
		epochFn: func() (uint64, bool) {
			if leading.Load() {
				return 7, true
			}
			return 6, false
		},
		watchFn: func(context.Context, []byte, []byte, uint64) (<-chan etcdproxy.WatchResult, error) {
			close(proxyOpened)
			return proxyResults, nil
		},
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	stream := &controllableWatchServer{
		ctx:  ctx,
		recv: make(chan *etcdserverpb.WatchRequest, 1),
	}
	done := make(chan error, 1)
	go func() { done <- server.Watch(stream) }()
	stream.recv <- &etcdserverpb.WatchRequest{RequestUnion: &etcdserverpb.WatchRequest_CreateRequest{
		CreateRequest: &etcdserverpb.WatchCreateRequest{Key: []byte("/role-switch-before-created")},
	}}

	select {
	case <-proxyOpened:
	case <-time.After(time.Second):
		t.Fatal("follower did not open authoritative proxy generation")
	}
	require.Empty(t, stream.snapshot())
	leading.Store(true)
	close(proxyResults)
	var revision uint64
	select {
	case revision = <-localCalled:
	case <-time.After(time.Second):
		t.Fatal("pending create did not reopen on the new local leader")
	}
	localResults <- etcdproxy.WatchResult{Revision: revision, Events: []*mvccpb.Event{{
		Type: mvccpb.PUT,
		Kv: &mvccpb.KeyValue{
			Key: []byte("/role-switch-before-created"), Value: []byte("v"),
			CreateRevision: int64(revision), ModRevision: int64(revision), Version: 1,
		},
	}}}

	require.Eventually(t, func() bool { return len(stream.snapshot()) == 2 }, time.Second, time.Millisecond)
	responses := stream.snapshot()
	require.True(t, responses[0].Created)
	require.False(t, responses[0].Canceled)
	require.Len(t, responses[1].Events, 1)
	require.False(t, responses[1].Canceled)

	cancel()
	close(localResults)
	require.Error(t, <-done)
}

func TestPendingLocalWatchReauthenticationUsesOriginalAuthorizationInterval(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	manager, tokens := bootstrapAuthForToken(t, server)
	server.tokens = tokens
	ctx := context.Background()
	require.NoError(t, manager.userAdd(ctx, &etcdserverpb.AuthUserAddRequest{
		Name: "alice", Password: "secret",
	}))
	require.NoError(t, manager.roleAdd(ctx, "reader"))
	require.NoError(t, manager.roleGrantPermission(ctx, "reader", &authpb.Permission{
		PermType: authpb.READ, Key: []byte("/allowed"),
	}))
	require.NoError(t, manager.userGrantRole(ctx, "alice", "reader"))
	token, err := tokens.authenticate(ctx, "alice", "secret")
	require.NoError(t, err)
	authenticated := metadata.NewIncomingContext(ctx, metadata.Pairs(rpctypes.TokenFieldNameGRPC, token))
	w := &watcher{grpcServer: server}

	require.NoError(t, w.authorizePendingLocalWatch(authenticated, &watch{
		authoritativeAuthKey: []byte("/allowed"),
	}))
	require.ErrorIs(t, w.authorizePendingLocalWatch(authenticated, &watch{
		authoritativeAuthKey: []byte("/denied"),
	}), rpctypes.ErrPermissionDenied)
}

func TestFollowerWatchPublishesCreatedOnlyAfterAuthoritativeAcknowledgement(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	results := make(chan etcdproxy.WatchResult, 2)
	opened := make(chan struct{})
	server.peers = testPeerService{
		proxyEnabled: true,
		watchFn: func(watchCtx context.Context, _ []byte, _ []byte, _ uint64) (<-chan etcdproxy.WatchResult, error) {
			close(opened)
			go func() {
				<-watchCtx.Done()
				close(results)
			}()
			return results, nil
		},
	}
	ctx, cancel := context.WithCancel(context.Background())
	stream := &controllableWatchServer{
		ctx:  ctx,
		recv: make(chan *etcdserverpb.WatchRequest, 1),
	}
	done := make(chan error, 1)
	go func() { done <- server.Watch(stream) }()
	stream.recv <- &etcdserverpb.WatchRequest{RequestUnion: &etcdserverpb.WatchRequest_CreateRequest{
		CreateRequest: &etcdserverpb.WatchCreateRequest{Key: []byte("/allowed")},
	}}
	select {
	case <-opened:
	case <-time.After(time.Second):
		t.Fatal("follower did not open authoritative leader watch")
	}
	require.Empty(t, stream.snapshot())

	results <- etcdproxy.WatchResult{Created: true, Revision: 99}
	require.Eventually(t, func() bool { return len(stream.snapshot()) == 1 }, time.Second, time.Millisecond)
	created := stream.snapshot()[0]
	require.True(t, created.Created)
	require.False(t, created.Canceled)
	require.Equal(t, int64(0), created.WatchId)
	require.Less(t, created.Header.Revision, int64(99), "leader acknowledgement must not skip buffered history")

	results <- etcdproxy.WatchResult{Revision: 99, Events: []*mvccpb.Event{{
		Type: mvccpb.PUT,
		Kv: &mvccpb.KeyValue{
			Key: []byte("/allowed"), Value: []byte("v"), CreateRevision: 99, ModRevision: 99, Version: 1,
		},
	}}}
	require.Eventually(t, func() bool { return len(stream.snapshot()) == 2 }, time.Second, time.Millisecond)
	require.Len(t, stream.snapshot()[1].Events, 1)

	cancel()
	require.Error(t, <-done)
}

func TestFollowerPendingAuthoritativeCreateOrdersCreatedBeforeCancel(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	results := make(chan etcdproxy.WatchResult)
	opened := make(chan struct{})
	proxyCanceled := make(chan struct{})
	server.peers = testPeerService{
		proxyEnabled: true,
		watchFn: func(watchCtx context.Context, _ []byte, _ []byte, _ uint64) (<-chan etcdproxy.WatchResult, error) {
			close(opened)
			go func() {
				<-watchCtx.Done()
				close(proxyCanceled)
				close(results)
			}()
			return results, nil
		},
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	stream := &controllableWatchServer{
		ctx:  ctx,
		recv: make(chan *etcdserverpb.WatchRequest, 3),
	}
	done := make(chan error, 1)
	go func() { done <- server.Watch(stream) }()
	stream.recv <- &etcdserverpb.WatchRequest{RequestUnion: &etcdserverpb.WatchRequest_CreateRequest{
		CreateRequest: &etcdserverpb.WatchCreateRequest{Key: []byte("/pending"), WatchId: 707},
	}}
	select {
	case <-opened:
	case <-time.After(time.Second):
		t.Fatal("follower did not open authoritative leader watch")
	}
	stream.recv <- &etcdserverpb.WatchRequest{RequestUnion: &etcdserverpb.WatchRequest_CancelRequest{
		CancelRequest: &etcdserverpb.WatchCancelRequest{WatchId: 707},
	}}
	stream.recv <- &etcdserverpb.WatchRequest{RequestUnion: &etcdserverpb.WatchRequest_ProgressRequest{
		ProgressRequest: &etcdserverpb.WatchProgressRequest{},
	}}
	require.Never(t, func() bool { return len(stream.snapshot()) != 0 }, 50*time.Millisecond, time.Millisecond,
		"follower cancellation overtook the authoritative create acknowledgement")
	results <- etcdproxy.WatchResult{Created: true, Revision: 99}
	require.Eventually(t, func() bool { return len(stream.snapshot()) == 2 }, time.Second, time.Millisecond)
	responses := stream.snapshot()
	require.True(t, responses[0].Created)
	require.False(t, responses[0].Canceled)
	require.Equal(t, int64(707), responses[0].WatchId)
	require.True(t, responses[1].Canceled)
	require.False(t, responses[1].Created)
	require.Equal(t, int64(707), responses[1].WatchId)
	require.Never(t, func() bool { return len(stream.snapshot()) > 2 }, 150*time.Millisecond, time.Millisecond,
		"cancel-pending watch remained eligible for a stream progress response")
	select {
	case <-proxyCanceled:
	case <-time.After(time.Second):
		t.Fatal("ordered follower cancellation did not stop the proxy watch")
	}

	cancel()
	require.Error(t, <-done)
}

func TestFollowerPendingCreateDoesNotBlockEstablishedWatchCancelResponse(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	firstResults := make(chan etcdproxy.WatchResult, 1)
	firstResults <- etcdproxy.WatchResult{Created: true, Revision: 10}
	secondResults := make(chan etcdproxy.WatchResult)
	firstCanceled := make(chan struct{})
	secondOpened := make(chan struct{})
	var calls atomic.Int64
	server.peers = testPeerService{
		proxyEnabled: true,
		watchFn: func(watchCtx context.Context, _ []byte, _ []byte, _ uint64) (<-chan etcdproxy.WatchResult, error) {
			switch calls.Add(1) {
			case 1:
				go func() {
					<-watchCtx.Done()
					close(firstCanceled)
					close(firstResults)
				}()
				return firstResults, nil
			case 2:
				close(secondOpened)
				go func() {
					<-watchCtx.Done()
					close(secondResults)
				}()
				return secondResults, nil
			default:
				return nil, errors.New("unexpected proxy watch generation")
			}
		},
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	stream := &controllableWatchServer{
		ctx:  ctx,
		recv: make(chan *etcdserverpb.WatchRequest, 3),
	}
	done := make(chan error, 1)
	go func() { done <- server.Watch(stream) }()
	stream.recv <- &etcdserverpb.WatchRequest{RequestUnion: &etcdserverpb.WatchRequest_CreateRequest{
		CreateRequest: &etcdserverpb.WatchCreateRequest{Key: []byte("/established"), WatchId: 701},
	}}
	require.Eventually(t, func() bool { return len(stream.snapshot()) == 1 }, time.Second, time.Millisecond)
	stream.recv <- &etcdserverpb.WatchRequest{RequestUnion: &etcdserverpb.WatchRequest_CreateRequest{
		CreateRequest: &etcdserverpb.WatchCreateRequest{Key: []byte("/pending"), WatchId: 702},
	}}
	select {
	case <-secondOpened:
	case <-time.After(time.Second):
		t.Fatal("second follower watch did not enter pending authoritative create")
	}
	stream.recv <- &etcdserverpb.WatchRequest{RequestUnion: &etcdserverpb.WatchRequest_CancelRequest{
		CancelRequest: &etcdserverpb.WatchCancelRequest{WatchId: 701},
	}}
	select {
	case <-firstCanceled:
	case <-time.After(time.Second):
		t.Fatal("pending create blocked established watch cancellation")
	}
	require.Eventually(t, func() bool { return len(stream.snapshot()) == 2 }, time.Second, time.Millisecond,
		"pending create blocked established watch cancellation response")
	response := stream.snapshot()[1]
	require.True(t, response.Canceled)
	require.Equal(t, int64(701), response.WatchId)

	cancel()
	require.Error(t, <-done)
}

func TestFollowerCancelSendBackpressureDoesNotBlockSameStreamRequests(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	results := make(chan etcdproxy.WatchResult, 1)
	results <- etcdproxy.WatchResult{Created: true, Revision: 10}
	server.peers = testPeerService{
		proxyEnabled: true,
		watchFn: func(watchCtx context.Context, _ []byte, _ []byte, _ uint64) (<-chan etcdproxy.WatchResult, error) {
			go func() {
				<-watchCtx.Done()
				close(results)
			}()
			return results, nil
		},
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	releaseCancelSend := make(chan struct{})
	released := false
	defer func() {
		if !released {
			close(releaseCancelSend)
		}
	}()
	stream := &blockedCancelSendWatchServer{
		controllableWatchServer: &controllableWatchServer{
			ctx: ctx, recv: make(chan *etcdserverpb.WatchRequest, 3),
		},
		cancelSendStarted: make(chan struct{}),
		releaseCancelSend: releaseCancelSend,
		thirdRequestRead:  make(chan struct{}),
	}
	done := make(chan error, 1)
	go func() { done <- server.Watch(stream) }()
	stream.recv <- &etcdserverpb.WatchRequest{RequestUnion: &etcdserverpb.WatchRequest_CreateRequest{
		CreateRequest: &etcdserverpb.WatchCreateRequest{Key: []byte("/backpressured"), WatchId: 711},
	}}
	require.Eventually(t, func() bool { return len(stream.snapshot()) == 1 }, time.Second, time.Millisecond)
	stream.recv <- &etcdserverpb.WatchRequest{RequestUnion: &etcdserverpb.WatchRequest_CancelRequest{
		CancelRequest: &etcdserverpb.WatchCancelRequest{WatchId: 711},
	}}
	select {
	case <-stream.cancelSendStarted:
	case <-time.After(time.Second):
		t.Fatal("follower cancellation response did not enter blocked Send")
	}
	stream.recv <- &etcdserverpb.WatchRequest{}
	select {
	case <-stream.thirdRequestRead:
	case <-time.After(100 * time.Millisecond):
		t.Fatal("backpressured follower cancellation blocked the same-stream receive loop")
	}

	close(releaseCancelSend)
	released = true
	require.Eventually(t, func() bool { return len(stream.snapshot()) == 2 }, time.Second, time.Millisecond)
	responses := stream.snapshot()
	require.True(t, responses[1].Canceled)
	require.Equal(t, int64(711), responses[1].WatchId)

	cancel()
	require.Error(t, <-done)
}

func TestFollowerCancelBackpressureUsesBoundedSenderGoroutines(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	release := make(chan struct{})
	stream := &blockedAllSendWatchServer{
		fakeWatchServer: &fakeWatchServer{ctx: ctx},
		entered:         make(chan struct{}, 1),
		release:         release,
	}
	const cancellationCount = watchControlBuffer + 2
	watches := make(map[int64]*watch, cancellationCount)
	for id := int64(1); id <= cancellationCount; id++ {
		watches[id] = &watch{cancel: func() {}, authoritativeControl: &etcdserverpb.WatchResponse{}}
	}
	w := &watcher{
		backend: server.backend, watchServer: stream, grpcServer: server,
		watches: watches, metricCli: server.metricCli,
	}
	w.startDirectControls()
	defer func() {
		close(release)
		w.closeDirectControls()
		w.controlWG.Wait()
	}()

	for id := int64(1); id < cancellationCount; id++ {
		w.CancelRequest(id)
	}
	select {
	case <-stream.entered:
	case <-time.After(time.Second):
		t.Fatal("follower cancellation sender did not enter blocked Send")
	}
	require.Never(t, func() bool {
		return goroutineStackOccurrences("(*watcher).cancel.func1") > 1
	}, 100*time.Millisecond, time.Millisecond,
		"output backpressure must not create one blocked sender goroutine per canceled watch")
	require.Equal(t, 1, goroutineStackOccurrences("(*watcher).sendDirectControls"),
		"one stream-owned sender must serialize the bounded cancellation backlog")
	backpressured := make(chan struct{})
	go func() {
		w.CancelRequest(cancellationCount)
		close(backpressured)
	}()
	require.Never(t, func() bool {
		select {
		case <-backpressured:
			return true
		default:
			return false
		}
	}, 50*time.Millisecond, time.Millisecond,
		"the fixed cancellation backlog must apply receive-side backpressure when full")
	cancel()
	select {
	case <-backpressured:
	case <-time.After(time.Second):
		t.Fatal("stream cancellation did not release the bounded cancellation backlog")
	}
}

func TestFollowerCancelBacklogPrecedesLaterDuplicateControl(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()
	stream := &blockingFirstSendWatchServer{
		fakeWatchServer: &fakeWatchServer{ctx: context.Background()},
		started:         make(chan struct{}),
		release:         make(chan struct{}),
	}
	w := &watcher{
		backend: server.backend, watchServer: stream, grpcServer: server,
		watches: map[int64]*watch{
			1: {cancel: func() {}, authoritativeControl: &etcdserverpb.WatchResponse{}},
			2: {cancel: func() {}, authoritativeControl: &etcdserverpb.WatchResponse{}},
		},
		controlCh: make(chan watchControlResponse, watchControlBuffer),
		metricCli: server.metricCli,
	}
	w.controlWG.Add(1)
	go w.sendControls()
	w.startDirectControls()
	defer w.Close()

	w.CancelRequest(1)
	<-stream.started
	w.CancelRequest(2)
	controlDone := make(chan error, 2)
	for range 2 {
		go func() {
			controlDone <- w.SendControl(canceledWatchCreateResponse(
				w.responseRevision(), "mvcc: duplicate watch ID provided on the WatchStream",
			))
		}()
	}
	// Ensure the ordinary control sender is already waiting for sendMu. Holding
	// watcherMu then pauses the direct sender in finishCancel after its first
	// transport Send, exposing any cross-queue handoff that lets the ordinary
	// sender acquire sendMu before the second queued cancellation.
	time.Sleep(10 * time.Millisecond)
	w.Lock()
	func() {
		defer w.Unlock()
		close(stream.release)
		require.Never(t, func() bool { return len(stream.snapshot()) > 1 }, 50*time.Millisecond, time.Millisecond,
			"ordinary control overtook a queued follower cancellation")
	}()
	require.Eventually(t, func() bool { return len(stream.snapshot()) == 4 }, time.Second, time.Millisecond)
	require.NoError(t, <-controlDone)
	require.NoError(t, <-controlDone)
	responses := stream.snapshot()
	require.Equal(t, int64(1), responses[0].WatchId)
	require.Equal(t, int64(2), responses[1].WatchId,
		"a later duplicate rejection must not overtake an already queued cancellation")
	require.Equal(t, int64(-1), responses[2].WatchId)
	require.Equal(t, int64(-1), responses[3].WatchId)
}

func TestUnsyncedProgressRequestDoesNotBlockSameStreamCancel(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	stream := &requestTrackingWatchServer{
		controllableWatchServer: &controllableWatchServer{
			ctx: ctx, recv: make(chan *etcdserverpb.WatchRequest, 3),
		},
		target: 3, targetRead: make(chan struct{}),
	}
	done := make(chan error, 1)
	go func() { done <- server.Watch(stream) }()
	stream.recv <- &etcdserverpb.WatchRequest{RequestUnion: &etcdserverpb.WatchRequest_CreateRequest{
		CreateRequest: &etcdserverpb.WatchCreateRequest{
			Key: []byte("/future-progress"), WatchId: 801, StartRevision: 100,
		},
	}}
	require.Eventually(t, func() bool { return len(stream.snapshot()) == 1 }, time.Second, time.Millisecond)
	stream.recv <- &etcdserverpb.WatchRequest{RequestUnion: &etcdserverpb.WatchRequest_ProgressRequest{
		ProgressRequest: &etcdserverpb.WatchProgressRequest{},
	}}
	stream.recv <- &etcdserverpb.WatchRequest{RequestUnion: &etcdserverpb.WatchRequest_CancelRequest{
		CancelRequest: &etcdserverpb.WatchCancelRequest{WatchId: 801},
	}}
	select {
	case <-stream.targetRead:
	case <-time.After(50 * time.Millisecond):
		t.Fatal("an unsynchronized RequestProgress blocked the same-stream Cancel")
	}
	require.Eventually(t, func() bool {
		responses := stream.snapshot()
		return len(responses) == 2 && responses[1].Canceled && responses[1].WatchId == 801
	}, time.Second, time.Millisecond)

	cancel()
	require.Error(t, <-done)
}

func TestEmptyProgressBurstDoesNotBlockLaterWatchCreate(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	const progressRequests = watchControlBuffer + 2
	stream := &requestTrackingWatchServer{
		controllableWatchServer: &controllableWatchServer{
			ctx: ctx, recv: make(chan *etcdserverpb.WatchRequest, progressRequests+1),
		},
		target: progressRequests + 1, targetRead: make(chan struct{}),
	}
	done := make(chan error, 1)
	go func() { done <- server.Watch(stream) }()
	for range progressRequests {
		stream.recv <- &etcdserverpb.WatchRequest{RequestUnion: &etcdserverpb.WatchRequest_ProgressRequest{
			ProgressRequest: &etcdserverpb.WatchProgressRequest{},
		}}
	}
	stream.recv <- &etcdserverpb.WatchRequest{RequestUnion: &etcdserverpb.WatchRequest_CreateRequest{
		CreateRequest: &etcdserverpb.WatchCreateRequest{Key: []byte("/after-empty-progress"), WatchId: 802},
	}}
	select {
	case <-stream.targetRead:
	case <-time.After(50 * time.Millisecond):
		t.Fatal("empty RequestProgress backlog blocked a later Watch create")
	}
	require.Eventually(t, func() bool {
		responses := stream.snapshot()
		return len(responses) == 1 && responses[0].Created && responses[0].WatchId == 802
	}, time.Second, time.Millisecond)

	cancel()
	require.Error(t, <-done)
}

func TestFutureProgressBurstDoesNotBlockSameStreamCancel(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	const progressRequests = watchControlBuffer + 2
	stream := &requestTrackingWatchServer{
		controllableWatchServer: &controllableWatchServer{
			ctx: ctx, recv: make(chan *etcdserverpb.WatchRequest, progressRequests+2),
		},
		target: progressRequests + 2, targetRead: make(chan struct{}),
	}
	done := make(chan error, 1)
	go func() { done <- server.Watch(stream) }()
	stream.recv <- &etcdserverpb.WatchRequest{RequestUnion: &etcdserverpb.WatchRequest_CreateRequest{
		CreateRequest: &etcdserverpb.WatchCreateRequest{
			Key: []byte("/future-progress-burst"), WatchId: 803, StartRevision: 100,
		},
	}}
	require.Eventually(t, func() bool { return len(stream.snapshot()) == 1 }, time.Second, time.Millisecond)
	for range progressRequests {
		stream.recv <- &etcdserverpb.WatchRequest{RequestUnion: &etcdserverpb.WatchRequest_ProgressRequest{
			ProgressRequest: &etcdserverpb.WatchProgressRequest{},
		}}
	}
	stream.recv <- &etcdserverpb.WatchRequest{RequestUnion: &etcdserverpb.WatchRequest_CancelRequest{
		CancelRequest: &etcdserverpb.WatchCancelRequest{WatchId: 803},
	}}
	select {
	case <-stream.targetRead:
	case <-time.After(50 * time.Millisecond):
		t.Fatal("future RequestProgress backlog blocked a later Watch cancel")
	}
	require.Eventually(t, func() bool {
		responses := stream.snapshot()
		return len(responses) == 2 && responses[1].Canceled && responses[1].WatchId == 803
	}, time.Second, time.Millisecond)

	cancel()
	require.Error(t, <-done)
}

func TestWatchContextCancellationDoesNotWaitForBlockedRecv(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()
	ctx, cancel := context.WithCancel(context.Background())
	stream := &contextIgnoringWatchServer{
		fakeWatchServer: &fakeWatchServer{ctx: ctx},
		entered:         make(chan struct{}),
		release:         make(chan struct{}),
		exited:          make(chan struct{}),
	}

	done := make(chan error, 1)
	go func() { done <- server.Watch(stream) }()
	<-stream.entered
	cancel()
	select {
	case err := <-done:
		require.Equal(t, codes.Canceled, status.Code(err))
	case <-time.After(100 * time.Millisecond):
		t.Fatal("watch handler waited for a transport Recv that ignored context cancellation")
	}
	require.Zero(t, server.activeWatchStreams.Load())

	// A late transport result must terminate only the detached receive pump; it
	// must not access the watcher or its already-closed control channel.
	close(stream.release)
	select {
	case <-stream.exited:
	case <-time.After(time.Second):
		t.Fatal("detached receive pump did not finish after transport release")
	}
}

func TestWatchHalfCloseKeepsResponseStreamAlive(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()
	server.SetMaxWatches(1)

	ctx, cancel := context.WithCancel(context.Background())
	stream := &halfClosedWatchServer{controllableWatchServer: &controllableWatchServer{
		ctx:  ctx,
		recv: make(chan *etcdserverpb.WatchRequest, 1),
	}}
	stream.recv <- &etcdserverpb.WatchRequest{RequestUnion: &etcdserverpb.WatchRequest_CreateRequest{
		CreateRequest: &etcdserverpb.WatchCreateRequest{Key: []byte("/watch/http-half-close")},
	}}

	done := make(chan error, 1)
	go func() { done <- server.Watch(stream) }()
	require.Eventually(t, func() bool {
		responses := stream.snapshot()
		return len(responses) > 0 && responses[0].Created
	}, 2*time.Second, time.Millisecond)

	select {
	case err := <-done:
		require.Failf(t, "watch returned after CloseSend", "error: %v", err)
	case <-time.After(25 * time.Millisecond):
	}
	require.Equal(t, int64(1), server.activeWatches)

	_, err := server.Put(ctx, &etcdserverpb.PutRequest{
		Key: []byte("/watch/http-half-close"), Value: []byte("after-eof"),
	})
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		for _, response := range stream.snapshot() {
			if len(response.Events) == 1 && string(response.Events[0].Kv.Value) == "after-eof" {
				return true
			}
		}
		return false
	}, 2*time.Second, time.Millisecond)

	cancel()
	requireWatchCanceled(t, <-done)
	require.Zero(t, server.activeWatches)
}

func TestFollowerWatchDelegatesInitialAuthorizationDespiteStaleAppliedState(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()
	aliceCtx := setupAuthKVUser(t, server)
	incoming, ok := metadata.FromIncomingContext(aliceCtx)
	require.True(t, ok)
	wantToken := incoming.Get(rpctypes.TokenFieldNameGRPC)
	require.Len(t, wantToken, 1)
	storageErr := errors.New("follower auth storage unavailable")
	server.tokens.snapshots.repo.backend = &authMetadataReadErrorBackend{
		BackendShim: server.backend, err: storageErr,
	}

	proxyCalled := make(chan struct{})
	proxyResults := make(chan etcdproxy.WatchResult)
	close(proxyResults)
	server.peers = testPeerService{
		isLeader: false, proxyEnabled: true,
		watchFn: func(ctx context.Context, key, rangeEnd []byte, revision uint64) (<-chan etcdproxy.WatchResult, error) {
			md, ok := metadata.FromOutgoingContext(ctx)
			require.True(t, ok)
			require.Empty(t, md.Get(etcdproxy.AuthorizedWatchProxyMetadataKey),
				"a cached follower snapshot cannot authoritatively approve a new watch")
			tokens := md.Get(rpctypes.TokenFieldNameGRPC)
			require.Equal(t, wantToken, tokens)
			require.Equal(t, []byte("/allowed/watch"), key)
			close(proxyCalled)
			return proxyResults, nil
		},
	}

	ctx, cancel := context.WithCancel(aliceCtx)
	stream := &controllableWatchServer{
		ctx: ctx, recv: make(chan *etcdserverpb.WatchRequest, 1),
	}
	stream.recv <- &etcdserverpb.WatchRequest{RequestUnion: &etcdserverpb.WatchRequest_CreateRequest{
		CreateRequest: &etcdserverpb.WatchCreateRequest{Key: []byte("/allowed/watch")},
	}}

	done := make(chan error, 1)
	go func() { done <- server.Watch(stream) }()
	select {
	case <-proxyCalled:
	case <-time.After(2 * time.Second):
		require.FailNow(t, "follower watch was not forwarded")
	}

	cancel()
	requireWatchCanceled(t, <-done)
}

func TestFollowerEmptyKeyFromKeyWatchPreservesOpenRangeForLeaderAuthorization(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()
	aliceCtx := setupAuthKVUser(t, server)
	incoming, ok := metadata.FromIncomingContext(aliceCtx)
	require.True(t, ok)
	wantToken := incoming.Get(rpctypes.TokenFieldNameGRPC)
	require.Len(t, wantToken, 1)

	proxyCalled := make(chan struct{})
	proxyResults := make(chan etcdproxy.WatchResult)
	close(proxyResults)
	server.peers = testPeerService{
		isLeader: false, proxyEnabled: true,
		watchFn: func(ctx context.Context, key, rangeEnd []byte, _ uint64) (<-chan etcdproxy.WatchResult, error) {
			md, mdOK := metadata.FromOutgoingContext(ctx)
			require.True(t, mdOK)
			require.Equal(t, wantToken, md.Get(rpctypes.TokenFieldNameGRPC))
			require.Empty(t, md.Get(etcdproxy.AuthorizedWatchProxyMetadataKey),
				"the leader must authorize the first proxied generation")
			require.Equal(t, []byte{0}, key, "the empty wire key must be normalized to the NUL key")
			require.NotNil(t, rangeEnd, "a from-key range must remain distinguishable from a point watch")
			require.Empty(t, rangeEnd, "the internal empty slice is the open-ended range marker")
			close(proxyCalled)
			return proxyResults, nil
		},
	}

	ctx, cancel := context.WithCancel(aliceCtx)
	stream := &controllableWatchServer{ctx: ctx, recv: make(chan *etcdserverpb.WatchRequest, 1)}
	stream.recv <- &etcdserverpb.WatchRequest{RequestUnion: &etcdserverpb.WatchRequest_CreateRequest{
		CreateRequest: &etcdserverpb.WatchCreateRequest{RangeEnd: []byte{0}},
	}}
	done := make(chan error, 1)
	go func() { done <- server.Watch(stream) }()
	select {
	case <-proxyCalled:
	case <-time.After(2 * time.Second):
		require.FailNow(t, "empty-key from-key watch was not forwarded")
	}
	cancel()
	requireWatchCanceled(t, <-done)
}

func TestFollowerWatchDelegatesInitialAuthorizationWhenAppliedStateIsIncomplete(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()
	aliceCtx := setupAuthKVUser(t, server)
	incoming, ok := metadata.FromIncomingContext(aliceCtx)
	require.True(t, ok)
	wantToken := incoming.Get(rpctypes.TokenFieldNameGRPC)
	require.Len(t, wantToken, 1)
	server.tokens.snapshots.invalidate()
	storageErr := errors.New("follower auth storage unavailable")
	server.tokens.snapshots.repo.backend = &authMetadataReadErrorBackend{
		BackendShim: server.backend, err: storageErr,
	}

	proxyCalled := make(chan struct{})
	proxyResults := make(chan etcdproxy.WatchResult)
	close(proxyResults)
	server.peers = testPeerService{
		isLeader: false, proxyEnabled: true,
		watchFn: func(ctx context.Context, key, rangeEnd []byte, revision uint64) (<-chan etcdproxy.WatchResult, error) {
			md, ok := metadata.FromOutgoingContext(ctx)
			require.True(t, ok)
			require.Empty(t, md.Get(etcdproxy.AuthorizedWatchProxyMetadataKey),
				"the leader must authenticate an initial follower generation")
			require.Equal(t, wantToken, md.Get(rpctypes.TokenFieldNameGRPC))
			close(proxyCalled)
			return proxyResults, nil
		},
	}

	ctx, cancel := context.WithCancel(aliceCtx)
	stream := &controllableWatchServer{ctx: ctx, recv: make(chan *etcdserverpb.WatchRequest, 1)}
	stream.recv <- &etcdserverpb.WatchRequest{RequestUnion: &etcdserverpb.WatchRequest_CreateRequest{
		CreateRequest: &etcdserverpb.WatchCreateRequest{Key: []byte("/allowed/watch")},
	}}
	done := make(chan error, 1)
	go func() { done <- server.Watch(stream) }()
	select {
	case <-proxyCalled:
	case <-time.After(2 * time.Second):
		require.FailNow(t, "follower watch was not forwarded")
	}
	cancel()
	requireWatchCanceled(t, <-done)
}

func TestAuthorizedPeerWatchContinuationSurvivesPermissionRevisionChange(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()
	aliceCtx := setupAuthKVUser(t, server)
	md, ok := metadata.FromIncomingContext(aliceCtx)
	require.True(t, ok)

	// etcd authorizes a Watch when it is created. An existing stream remains
	// active after a permission mutation; an internal follower-to-leader resume
	// must therefore not turn that same logical Watch into a fresh authorization
	// decision at the new leader.
	require.NoError(t, server.auth.roleRevokePermission(
		context.Background(), "allowed", []byte("/allowed/"), []byte("/allowed0"),
	))

	grpcServer := grpc.NewServer(server.PeerServerOptions()...)
	etcdserverpb.RegisterWatchServer(grpcServer, server)
	listener := bufconn.Listen(1 << 20)
	go func() { _ = grpcServer.Serve(listener) }()
	t.Cleanup(grpcServer.Stop)

	conn, err := grpc.NewClient("passthrough:///authorized-peer-watch-continuation",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return listener.Dial()
		}))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, conn.Close()) })

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	proxyCtx := metadata.NewOutgoingContext(ctx, md.Copy())
	proxyCtx = metadata.AppendToOutgoingContext(proxyCtx, etcdproxy.AuthorizedWatchProxyMetadataKey, "1")
	stream, err := etcdserverpb.NewWatchClient(conn).Watch(proxyCtx)
	require.NoError(t, err)
	require.NoError(t, stream.Send(&etcdserverpb.WatchRequest{RequestUnion: &etcdserverpb.WatchRequest_CreateRequest{CreateRequest: &etcdserverpb.WatchCreateRequest{
		Key: []byte("/allowed/watch"), WatchId: 88, StartRevision: 1,
	}},
	}))
	created, err := stream.Recv()
	require.NoError(t, err)
	require.True(t, created.Created)
	require.False(t, created.Canceled)
	require.Equal(t, int64(88), created.WatchId)
	require.NoError(t, stream.CloseSend())

	// The same metadata on the public listener is untrusted and must not bypass
	// the current permission snapshot.
	publicServer := grpc.NewServer(server.ClientServerOptions()...)
	etcdserverpb.RegisterWatchServer(publicServer, server)
	publicListener := bufconn.Listen(1 << 20)
	go func() { _ = publicServer.Serve(publicListener) }()
	t.Cleanup(publicServer.Stop)
	publicConn, err := grpc.NewClient("passthrough:///spoofed-public-watch-continuation",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return publicListener.Dial()
		}))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, publicConn.Close()) })
	publicStream, err := etcdserverpb.NewWatchClient(publicConn).Watch(proxyCtx)
	require.NoError(t, err)
	require.NoError(t, publicStream.Send(&etcdserverpb.WatchRequest{RequestUnion: &etcdserverpb.WatchRequest_CreateRequest{CreateRequest: &etcdserverpb.WatchCreateRequest{
		Key: []byte("/allowed/watch"), WatchId: 89,
	}},
	}))
	rejected, err := publicStream.Recv()
	require.NoError(t, err)
	require.True(t, rejected.Created)
	require.True(t, rejected.Canceled)
	require.Equal(t, int64(-1), rejected.WatchId)
	require.Equal(t, rpctypes.ErrGRPCPermissionDenied.Error(), rejected.CancelReason)
	require.NoError(t, publicStream.CloseSend())
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
