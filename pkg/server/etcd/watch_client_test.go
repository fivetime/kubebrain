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
	"math"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
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
)

func TestClientWatchFragmentDeliversLargeBatchWithSmallRecvLimit(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	grpcServer := grpc.NewServer(server.ClientServerOptions()...)
	etcdserverpb.RegisterKVServer(grpcServer, server)
	etcdserverpb.RegisterWatchServer(grpcServer, server)
	listener := bufconn.Listen(8 << 20)
	go func() { _ = grpcServer.Serve(listener) }()
	t.Cleanup(grpcServer.Stop)

	dialOptions := []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return listener.Dial()
		}),
	}
	writer, err := clientv3.New(clientv3.Config{
		Endpoints: []string{"bufnet"}, DialTimeout: time.Second, DialOptions: dialOptions,
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, writer.Close()) })
	watcher, err := clientv3.New(clientv3.Config{
		Endpoints: []string{"bufnet"}, DialTimeout: time.Second, DialOptions: dialOptions,
		MaxCallRecvMsgSize: 1536 * 1024,
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, watcher.Close()) })

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	prefix := fmt.Sprintf("/a992/watch-fragment-client/%d/", time.Now().UnixNano())
	base, err := writer.Get(ctx, prefix, clientv3.WithPrefix())
	require.NoError(t, err)
	const eventCount = 10
	value := strings.Repeat("x", 1024*1024)
	for index := 0; index < eventCount; index++ {
		_, err = writer.Put(ctx, fmt.Sprintf("%s%d", prefix, index), value)
		require.NoError(t, err)
	}

	watchCtx, watchCancel := context.WithTimeout(ctx, 5*time.Second)
	defer watchCancel()
	responses := watcher.Watch(
		watchCtx, prefix, clientv3.WithPrefix(), clientv3.WithRev(base.Header.Revision+1),
		clientv3.WithFragment(),
	)
	events := 0
	for events < eventCount {
		select {
		case response, ok := <-responses:
			require.True(t, ok)
			require.NoError(t, response.Err())
			require.NotEmpty(t, response.Events)
			events += len(response.Events)
		case <-watchCtx.Done():
			t.Fatalf("watch fragment delivery timed out after %d events: %v", events, watchCtx.Err())
		}
	}
	require.Equal(t, eventCount, events)
}

func TestClientWatchAfterCloseIsBoundedAndCanceled(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	grpcServer := grpc.NewServer(server.ClientServerOptions()...)
	etcdserverpb.RegisterKVServer(grpcServer, server)
	etcdserverpb.RegisterWatchServer(grpcServer, server)
	listener := bufconn.Listen(1 << 20)
	go func() { _ = grpcServer.Serve(listener) }()
	t.Cleanup(grpcServer.Stop)

	client, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{"bufnet"},
		DialTimeout: time.Second,
		DialOptions: []grpc.DialOption{
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
				return listener.Dial()
			}),
		},
	})
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	active := client.Watch(ctx, "/a1128/watch/active", clientv3.WithCreatedNotify())
	created := requireWatchClientResponse(t, ctx, active)
	require.NoError(t, created.Err())
	require.True(t, created.Created)
	require.NoError(t, client.Close())
	requireWatchClientCanceledOrClosed(t, ctx, active)

	closedCtx, closedCancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer closedCancel()
	afterClose := client.Watch(closedCtx, "/a1128/watch/after-close")
	requireWatchClientCanceledOrClosed(t, closedCtx, afterClose)
}

func TestClientWatchContextCancelClosesChannel(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	grpcServer := grpc.NewServer(server.ClientServerOptions()...)
	etcdserverpb.RegisterKVServer(grpcServer, server)
	etcdserverpb.RegisterWatchServer(grpcServer, server)
	listener := bufconn.Listen(1 << 20)
	go func() { _ = grpcServer.Serve(listener) }()
	t.Cleanup(grpcServer.Stop)

	client, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{"bufnet"},
		DialTimeout: time.Second,
		DialOptions: []grpc.DialOption{
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
				return listener.Dial()
			}),
		},
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, client.Close()) })

	canceledCtx, canceledCancel := context.WithCancel(context.Background())
	canceledCancel()
	requireWatchClientClosed(t, context.Background(), client.Watch(canceledCtx, "/a1133/watch/immediate"))

	initCtx, initCancel := context.WithCancel(context.Background())
	initWatch := client.Watch(initCtx, "/a1133/watch/init")
	initCancel()
	requireWatchClientClosed(t, context.Background(), initWatch)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	runningCtx, runningCancel := context.WithCancel(ctx)
	runningWatch := client.Watch(runningCtx, "/a1133/watch/running")
	_, err = client.Put(ctx, "/a1133/watch/running", "value")
	require.NoError(t, err)
	runningCancel()
	requireWatchClientClosedAfterOptionalEvent(t, ctx, runningWatch)
}

func requireWatchClientResponse(t *testing.T, ctx context.Context, responses clientv3.WatchChan) clientv3.WatchResponse {
	t.Helper()
	select {
	case response, ok := <-responses:
		require.True(t, ok, "watch channel closed before response")
		return response
	case <-ctx.Done():
		t.Fatalf("timed out waiting for watch response: %v", ctx.Err())
	}
	return clientv3.WatchResponse{}
}

func requireWatchClientClosed(t *testing.T, parent context.Context, responses clientv3.WatchChan) {
	t.Helper()
	ctx, cancel := context.WithTimeout(parent, time.Second)
	defer cancel()
	select {
	case response, ok := <-responses:
		require.False(t, ok, "expected closed watch channel, got response=%+v err=%v", response, response.Err())
	case <-ctx.Done():
		t.Fatalf("timed out waiting for closed watch channel: %v", ctx.Err())
	}
}

func requireWatchClientClosedAfterOptionalEvent(t *testing.T, ctx context.Context, responses clientv3.WatchChan) {
	t.Helper()
	select {
	case response, ok := <-responses:
		if !ok {
			return
		}
		require.NoError(t, response.Err())
		require.False(t, response.Canceled)
		require.NotEmpty(t, response.Events)
	case <-ctx.Done():
		t.Fatalf("timed out waiting for watch event or close: %v", ctx.Err())
	}
	requireWatchClientClosed(t, ctx, responses)
}

func requireWatchClientCanceledOrClosed(t *testing.T, ctx context.Context, responses clientv3.WatchChan) {
	t.Helper()
	select {
	case response, ok := <-responses:
		if !ok {
			return
		}
		require.True(t, response.Canceled || isExpectedWatchCloseError(response.Err()),
			"watch close response canceled=%v err=%v", response.Canceled, response.Err())
	case <-ctx.Done():
		t.Fatalf("timed out waiting for watch close: %v", ctx.Err())
	}
}

func TestRawGRPCWatchFragmentPreservesPrevKV(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	ctx := context.Background()
	prefix := fmt.Sprintf("/a1028/watch-fragment/%d/", time.Now().UnixNano())
	value := []byte(strings.Repeat("x", 600*1024))
	var revision int64
	const wantEvents = 4
	for index := 0; index < wantEvents; index++ {
		key := fmt.Sprintf("%s%d", prefix, index)
		resp, err := server.Put(ctx, &etcdserverpb.PutRequest{Key: []byte(key), Value: value})
		require.NoError(t, err)
		revision = resp.Header.Revision
	}
	server.SetRequestLimits(defaultMaxTxnOps, 1024)

	grpcServer := grpc.NewServer(server.ClientServerOptions()...)
	etcdserverpb.RegisterKVServer(grpcServer, server)
	etcdserverpb.RegisterWatchServer(grpcServer, server)
	listener := bufconn.Listen(4 << 20)
	go func() { _ = grpcServer.Serve(listener) }()
	t.Cleanup(grpcServer.Stop)

	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return listener.Dial()
		}),
	)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, conn.Close()) })

	callCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	stream, err := etcdserverpb.NewWatchClient(conn).Watch(callCtx)
	require.NoError(t, err)
	t.Cleanup(func() { _ = stream.CloseSend() })
	require.NoError(t, stream.Send(&etcdserverpb.WatchRequest{
		RequestUnion: &etcdserverpb.WatchRequest_CreateRequest{
			CreateRequest: &etcdserverpb.WatchCreateRequest{
				Key: []byte(prefix), RangeEnd: []byte(clientv3.GetPrefixRangeEnd(prefix)),
				StartRevision: revision + 1, PrevKv: true, Fragment: true,
			},
		},
	}))
	created, err := stream.Recv()
	require.NoError(t, err)
	requireRawWatchHeaderWellFormed(t, created)
	require.True(t, created.Created)
	require.False(t, created.Canceled)

	_, err = etcdserverpb.NewKVClient(conn).DeleteRange(callCtx, &etcdserverpb.DeleteRangeRequest{
		Key: []byte(prefix), RangeEnd: []byte(clientv3.GetPrefixRangeEnd(prefix)), PrevKv: true,
	})
	require.NoError(t, err)

	var (
		responses      int
		observedEvents int
		prevBytes      []int
	)
	for {
		response, recvErr := stream.Recv()
		require.NoError(t, recvErr)
		requireRawWatchHeaderWellFormed(t, response)
		require.NotEmpty(t, response.Events)
		responses++
		for _, event := range response.Events {
			require.NotNil(t, event.PrevKv)
			prevBytes = append(prevBytes, len(event.PrevKv.Value))
			observedEvents++
		}
		if !response.Fragment {
			break
		}
	}
	require.Greater(t, responses, 1, "large prev-kv watch response must fragment")
	require.Equal(t, wantEvents, observedEvents)
	require.Len(t, prevBytes, wantEvents)
	for _, size := range prevBytes {
		require.Equal(t, len(value), size)
	}
}

func TestClientWatchMixedPrevKVStreamsKeepEventsIsolated(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	grpcServer := grpc.NewServer(server.ClientServerOptions()...)
	etcdserverpb.RegisterKVServer(grpcServer, server)
	etcdserverpb.RegisterWatchServer(grpcServer, server)
	listener := bufconn.Listen(1 << 20)
	go func() { _ = grpcServer.Serve(listener) }()
	t.Cleanup(grpcServer.Stop)

	client, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{"bufnet"},
		DialTimeout: time.Second,
		DialOptions: []grpc.DialOption{
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
				return listener.Dial()
			}),
		},
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, client.Close()) })

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	key := fmt.Sprintf("/a1029/watch-mixed-prevkv/%d", time.Now().UnixNano())
	_, err = client.Put(ctx, key, "v0")
	require.NoError(t, err)

	const (
		watcherCount  = 6
		withPrevCount = watcherCount / 2
		updateCount   = 8
	)
	type watchCase struct {
		withPrev bool
		cancel   context.CancelFunc
		channel  clientv3.WatchChan
	}
	watches := make([]watchCase, 0, watcherCount)
	for index := 0; index < watcherCount; index++ {
		streamCtx := metadata.NewOutgoingContext(
			ctx,
			metadata.Pairs("dbaas-watch-stream-id", fmt.Sprintf("a1088-%d", index)),
		)
		watchCtx, watchCancel := context.WithCancel(streamCtx)
		options := []clientv3.OpOption{clientv3.WithCreatedNotify()}
		withPrev := index < withPrevCount
		if withPrev {
			options = append(options, clientv3.WithPrevKV())
		}
		watches = append(watches, watchCase{
			withPrev: withPrev,
			cancel:   watchCancel,
			channel:  client.Watch(watchCtx, key, options...),
		})
	}
	t.Cleanup(func() {
		for _, watch := range watches {
			watch.cancel()
		}
	})
	for _, watch := range watches {
		requireClientWatchCreated(t, ctx, watch.channel)
	}

	previous := "v0"
	for update := 1; update <= updateCount; update++ {
		current := fmt.Sprintf("v%d", update)
		_, err = client.Put(ctx, key, current)
		require.NoError(t, err)

		for index, watch := range watches {
			event := requireSingleClientWatchEvent(t, ctx, watch.channel)
			require.Equalf(t, key, string(event.Kv.Key), "watcher %d update %d", index, update)
			require.Equalf(t, current, string(event.Kv.Value), "watcher %d update %d", index, update)
			if watch.withPrev {
				require.NotNilf(t, event.PrevKv, "watcher %d update %d", index, update)
				require.Equal(t, key, string(event.PrevKv.Key))
				require.Equalf(t, previous, string(event.PrevKv.Value), "watcher %d update %d", index, update)
			} else {
				require.Nilf(t, event.PrevKv, "watcher %d update %d", index, update)
			}
		}
		previous = current
	}
}

func TestClientFromNowWatchDoesNotLoseImmediatePostCreateWrite(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	grpcServer := grpc.NewServer(server.ClientServerOptions()...)
	etcdserverpb.RegisterKVServer(grpcServer, server)
	etcdserverpb.RegisterWatchServer(grpcServer, server)
	listener := bufconn.Listen(1 << 20)
	go func() { _ = grpcServer.Serve(listener) }()
	t.Cleanup(grpcServer.Stop)

	client, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{"bufnet"},
		DialTimeout: time.Second,
		DialOptions: []grpc.DialOption{
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
				return listener.Dial()
			}),
		},
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, client.Close()) })

	for index := 0; index < 50; index++ {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		key := fmt.Sprintf("/a1089/watch-registration/%d/%d", time.Now().UnixNano(), index)
		watch := client.Watch(ctx, key, clientv3.WithCreatedNotify())
		created := requireClientWatchCreatedResponse(t, ctx, watch)
		require.NotNil(t, created.Header)

		put, err := client.Put(ctx, key, "immediate")
		require.NoError(t, err)
		event := requireSingleClientWatchEvent(t, ctx, watch)
		require.Equal(t, key, string(event.Kv.Key))
		require.Equal(t, "immediate", string(event.Kv.Value))
		require.Equal(t, put.Header.Revision, event.Kv.ModRevision)
		cancel()
	}
}

func TestClientWatchUpdateReportsUpdateNotCreate(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	grpcServer := grpc.NewServer(server.ClientServerOptions()...)
	etcdserverpb.RegisterKVServer(grpcServer, server)
	etcdserverpb.RegisterWatchServer(grpcServer, server)
	listener := bufconn.Listen(1 << 20)
	go func() { _ = grpcServer.Serve(listener) }()
	t.Cleanup(grpcServer.Stop)

	client, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{"bufnet"},
		DialTimeout: time.Second,
		DialOptions: []grpc.DialOption{
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
				return listener.Dial()
			}),
		},
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, client.Close()) })

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	key := fmt.Sprintf("/a1030/watch-update/%d", time.Now().UnixNano())
	create, err := client.Put(ctx, key, "v1")
	require.NoError(t, err)

	watchCtx, watchCancel := context.WithCancel(ctx)
	defer watchCancel()
	watch := client.Watch(watchCtx, key, clientv3.WithRev(create.Header.Revision+1), clientv3.WithPrevKV())

	update, err := client.Put(ctx, key, "v2")
	require.NoError(t, err)
	event := requireSingleClientWatchEvent(t, ctx, watch)
	require.Equal(t, clientv3.EventTypePut, event.Type)
	require.False(t, event.IsCreate(), "watch update must not be reported as a create")
	require.Equal(t, key, string(event.Kv.Key))
	require.Equal(t, "v2", string(event.Kv.Value))
	require.Equal(t, create.Header.Revision, event.Kv.CreateRevision)
	require.Equal(t, update.Header.Revision, event.Kv.ModRevision)
	require.Less(t, event.Kv.CreateRevision, event.Kv.ModRevision)
	require.NotNil(t, event.PrevKv)
	require.Equal(t, key, string(event.PrevKv.Key))
	require.Equal(t, "v1", string(event.PrevKv.Value))
	require.Equal(t, create.Header.Revision, event.PrevKv.CreateRevision)
	require.Equal(t, create.Header.Revision, event.PrevKv.ModRevision)
}

func TestClientWatchMultiWatcherKeyAndPrefixIsolation(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	grpcServer := grpc.NewServer(server.ClientServerOptions()...)
	etcdserverpb.RegisterKVServer(grpcServer, server)
	etcdserverpb.RegisterWatchServer(grpcServer, server)
	listener := bufconn.Listen(1 << 20)
	go func() { _ = grpcServer.Serve(listener) }()
	t.Cleanup(grpcServer.Stop)

	client, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{"bufnet"},
		DialTimeout: time.Second,
		DialOptions: []grpc.DialOption{
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
				return listener.Dial()
			}),
		},
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, client.Close()) })

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	prefix := fmt.Sprintf("/a2130/watch-multi/%d/", time.Now().UnixNano())
	keys := []string{prefix + "foo", prefix + "bar", prefix + "baz"}
	exactWatches := make(map[string]clientv3.WatchChan, len(keys))
	for _, key := range keys {
		exactWatches[key] = client.Watch(ctx, key, clientv3.WithCreatedNotify())
	}
	prefixWatch := client.Watch(ctx, prefix+"b", clientv3.WithPrefix(), clientv3.WithCreatedNotify())

	for _, watch := range exactWatches {
		requireClientWatchCreated(t, ctx, watch)
	}
	requireClientWatchCreated(t, ctx, prefixWatch)

	const updateCount = 4
	type wantEvent struct {
		key      string
		value    string
		revision int64
	}
	collect := func(watch clientv3.WatchChan, count int) []wantEvent {
		t.Helper()
		got := make([]wantEvent, 0, count)
		for len(got) < count {
			response := requireClientWatchResponse(t, ctx, watch)
			require.NoError(t, response.Err())
			require.False(t, response.Created)
			require.False(t, response.Canceled)
			require.False(t, response.IsProgressNotify())
			for _, event := range response.Events {
				require.Equal(t, clientv3.EventTypePut, event.Type)
				got = append(got, wantEvent{
					key:      string(event.Kv.Key),
					value:    string(event.Kv.Value),
					revision: event.Kv.ModRevision,
				})
			}
		}
		return got
	}
	exactWants := make(map[string][]wantEvent, len(keys))
	prefixWants := make([]wantEvent, 0, updateCount*2)
	for update := 0; update < updateCount; update++ {
		for _, key := range keys {
			value := fmt.Sprintf("%s-%d", key[len(prefix):], update)
			response, err := client.Put(ctx, key, value)
			require.NoError(t, err)
			want := wantEvent{key: key, value: value, revision: response.Header.Revision}
			exactWants[key] = append(exactWants[key], want)
			if strings.HasPrefix(key, prefix+"b") {
				prefixWants = append(prefixWants, want)
			}
		}
	}

	for _, key := range keys {
		got := collect(exactWatches[key], len(exactWants[key]))
		require.Equalf(t, exactWants[key], got, "exact watcher for %q", key)
		requireNoClientWatchEventBeforeProgress(t, ctx, client, exactWatches[key])
	}
	gotPrefix := collect(prefixWatch, len(prefixWants))
	require.Equal(t, prefixWants, gotPrefix)
	requireNoClientWatchEventBeforeProgress(t, ctx, client, prefixWatch)
}

func TestClientWatchEventTypesCreateModifyDeleteAndExpire(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	grpcServer := grpc.NewServer(server.ClientServerOptions()...)
	etcdserverpb.RegisterKVServer(grpcServer, server)
	etcdserverpb.RegisterLeaseServer(grpcServer, server)
	etcdserverpb.RegisterWatchServer(grpcServer, server)
	listener := bufconn.Listen(1 << 20)
	go func() { _ = grpcServer.Serve(listener) }()
	t.Cleanup(grpcServer.Stop)

	client, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{"bufnet"},
		DialTimeout: time.Second,
		DialOptions: []grpc.DialOption{
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
				return listener.Dial()
			}),
		},
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, client.Close()) })

	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	prefix := fmt.Sprintf("/a1148/watch-event-type/%d/", time.Now().UnixNano())
	watchCtx, watchCancel := context.WithCancel(ctx)
	defer watchCancel()
	watch := client.Watch(watchCtx, prefix, clientv3.WithPrefix(), clientv3.WithCreatedNotify())
	requireClientWatchCreated(t, ctx, watch)

	key := prefix + "key"
	_, err = client.Put(ctx, key, "create")
	require.NoError(t, err)
	_, err = client.Put(ctx, key, "modify")
	require.NoError(t, err)
	_, err = client.Delete(ctx, key)
	require.NoError(t, err)
	lease, err := client.Grant(ctx, 1)
	require.NoError(t, err)
	expireKey := prefix + "expire"
	_, err = client.Put(ctx, expireKey, "lease", clientv3.WithLease(lease.ID))
	require.NoError(t, err)

	type wantEvent struct {
		key      string
		event    mvccpb.Event_EventType
		create   bool
		modified bool
	}
	want := []wantEvent{
		{key: key, event: clientv3.EventTypePut, create: true},
		{key: key, event: clientv3.EventTypePut, modified: true},
		{key: key, event: clientv3.EventTypeDelete},
		{key: expireKey, event: clientv3.EventTypePut, create: true},
		{key: expireKey, event: clientv3.EventTypeDelete},
	}
	events := make([]*clientv3.Event, 0, len(want))
	for len(events) < len(want) {
		response := requireClientWatchResponse(t, ctx, watch)
		require.False(t, response.Created)
		events = append(events, response.Events...)
	}
	require.Len(t, events, len(want))
	for index, expected := range want {
		event := events[index]
		require.Equalf(t, expected.event, event.Type, "event %d", index)
		require.Equalf(t, expected.key, string(event.Kv.Key), "event %d", index)
		require.Equalf(t, expected.create, event.IsCreate(), "event %d", index)
		require.Equalf(t, expected.modified, event.IsModify(), "event %d", index)
	}
}

func TestClientFilteredWatchProgressCoversSuppressedPut(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	grpcServer := grpc.NewServer(server.ClientServerOptions()...)
	etcdserverpb.RegisterKVServer(grpcServer, server)
	etcdserverpb.RegisterWatchServer(grpcServer, server)
	listener := bufconn.Listen(1 << 20)
	go func() { _ = grpcServer.Serve(listener) }()
	t.Cleanup(grpcServer.Stop)

	client, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{"bufnet"},
		DialTimeout: time.Second,
		DialOptions: []grpc.DialOption{
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
				return listener.Dial()
			}),
		},
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, client.Close()) })

	for index := 0; index < 25; index++ {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		key := fmt.Sprintf("/a1090/watch-filter-progress/%d/%d", time.Now().UnixNano(), index)
		watch := client.Watch(ctx, key, clientv3.WithCreatedNotify(), clientv3.WithFilterPut())
		requireClientWatchCreated(t, ctx, watch)

		put, err := client.Put(ctx, key, "filtered")
		require.NoError(t, err)
		require.NoError(t, client.RequestProgress(ctx))

		covered := false
		for !covered {
			select {
			case response, ok := <-watch:
				require.True(t, ok)
				require.NoError(t, response.Err())
				require.False(t, response.Created)
				require.Empty(t, response.Events, "NOPUT watch must suppress the PUT")
				require.NotNil(t, response.Header)
				if response.Header.Revision >= put.Header.Revision {
					covered = true
				}
			case <-ctx.Done():
				cancel()
				t.Fatalf("timed out waiting for progress covering filtered revision %d: %v", put.Header.Revision, ctx.Err())
			}
		}
		cancel()
	}
}

func TestClientWatchFiltersPutAndDeleteEvents(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	grpcServer := grpc.NewServer(server.ClientServerOptions()...)
	etcdserverpb.RegisterKVServer(grpcServer, server)
	etcdserverpb.RegisterWatchServer(grpcServer, server)
	listener := bufconn.Listen(1 << 20)
	go func() { _ = grpcServer.Serve(listener) }()
	t.Cleanup(grpcServer.Stop)

	client, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{"bufnet"},
		DialTimeout: time.Second,
		DialOptions: []grpc.DialOption{
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
				return listener.Dial()
			}),
		},
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, client.Close()) })

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	key := fmt.Sprintf("/a1141/watch-filter/%d", time.Now().UnixNano())
	noPut := client.Watch(ctx, key, clientv3.WithCreatedNotify(), clientv3.WithFilterPut())
	noDelete := client.Watch(ctx, key, clientv3.WithCreatedNotify(), clientv3.WithFilterDelete())
	requireClientWatchCreated(t, ctx, noPut)
	requireClientWatchCreated(t, ctx, noDelete)

	_, err = client.Put(ctx, key, "value")
	require.NoError(t, err)
	putEvent := requireSingleClientWatchEvent(t, ctx, noDelete)
	require.Equal(t, clientv3.EventTypePut, putEvent.Type)
	require.Equal(t, key, string(putEvent.Kv.Key))
	require.Equal(t, "value", string(putEvent.Kv.Value))

	_, err = client.Delete(ctx, key)
	require.NoError(t, err)
	deleteEvent := requireSingleClientWatchEvent(t, ctx, noPut)
	require.Equal(t, clientv3.EventTypeDelete, deleteEvent.Type)
	require.Equal(t, key, string(deleteEvent.Kv.Key))

	requireNoClientWatchEventBeforeProgress(t, ctx, client, noPut)
	requireNoClientWatchEventBeforeProgress(t, ctx, client, noDelete)
}

func TestClientWatchRangeDeliversHalfOpenIntervalOnly(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	grpcServer := grpc.NewServer(server.ClientServerOptions()...)
	etcdserverpb.RegisterKVServer(grpcServer, server)
	etcdserverpb.RegisterWatchServer(grpcServer, server)
	listener := bufconn.Listen(1 << 20)
	go func() { _ = grpcServer.Serve(listener) }()
	t.Cleanup(grpcServer.Stop)

	client, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{"bufnet"},
		DialTimeout: time.Second,
		DialOptions: []grpc.DialOption{
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
				return listener.Dial()
			}),
		},
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, client.Close()) })

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	prefix := fmt.Sprintf("/a2129/watch-range/%d/", time.Now().UnixNano())
	base, err := client.Get(ctx, prefix, clientv3.WithPrefix())
	require.NoError(t, err)

	watch := client.Watch(
		ctx,
		prefix+"a",
		clientv3.WithRange(prefix+"c"),
		clientv3.WithRev(base.Header.Revision+1),
		clientv3.WithCreatedNotify(),
	)
	requireClientWatchCreated(t, ctx, watch)

	type wantEvent struct {
		key      string
		value    string
		revision int64
	}
	wants := make([]wantEvent, 0, 3)
	for _, write := range []struct {
		suffix string
		value  string
		watch  bool
	}{
		{suffix: "a", value: "left-bound", watch: true},
		{suffix: "b", value: "middle", watch: true},
		{suffix: "bar", value: "lexicographic-middle", watch: true},
		{suffix: "c", value: "right-bound", watch: false},
		{suffix: "z", value: "outside", watch: false},
	} {
		response, err := client.Put(ctx, prefix+write.suffix, write.value)
		require.NoError(t, err)
		if write.watch {
			wants = append(wants, wantEvent{
				key:      prefix + write.suffix,
				value:    write.value,
				revision: response.Header.Revision,
			})
		}
	}

	got := make([]wantEvent, 0, len(wants))
	for len(got) < len(wants) {
		response := requireClientWatchResponse(t, ctx, watch)
		require.NoError(t, response.Err())
		require.False(t, response.Created)
		require.False(t, response.Canceled)
		require.False(t, response.IsProgressNotify())
		for _, event := range response.Events {
			require.Equal(t, clientv3.EventTypePut, event.Type)
			got = append(got, wantEvent{
				key:      string(event.Kv.Key),
				value:    string(event.Kv.Value),
				revision: event.Kv.ModRevision,
			})
		}
	}
	require.Equal(t, wants, got)

	requireNoClientWatchEventBeforeProgress(t, ctx, client, watch)
}

func TestClientWatchProgressNotifySuppressesTickAfterEvent(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	grpcServer := grpc.NewServer(server.ClientServerOptions()...)
	etcdserverpb.RegisterKVServer(grpcServer, server)
	etcdserverpb.RegisterWatchServer(grpcServer, server)
	listener := bufconn.Listen(1 << 20)
	go func() { _ = grpcServer.Serve(listener) }()
	t.Cleanup(grpcServer.Stop)

	client, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{"bufnet"},
		DialTimeout: time.Second,
		DialOptions: []grpc.DialOption{
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
				return listener.Dial()
			}),
		},
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, client.Close()) })

	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	prefix := fmt.Sprintf("/a1115/watch-progress-cadence/%d/", time.Now().UnixNano())
	watchCtx, watchCancel := context.WithCancel(ctx)
	defer watchCancel()
	watch := client.Watch(
		watchCtx, prefix,
		clientv3.WithPrefix(), clientv3.WithCreatedNotify(), clientv3.WithProgressNotify(),
	)
	requireClientWatchCreated(t, ctx, watch)

	put, err := client.Put(ctx, prefix+"event", "value")
	require.NoError(t, err)
	for {
		response := requireClientWatchResponse(t, ctx, watch)
		require.False(t, response.IsProgressNotify(), "progress must not overtake the event")
		if len(response.Events) == 0 {
			continue
		}
		require.Len(t, response.Events, 1)
		require.Equal(t, clientv3.EventTypePut, response.Events[0].Type)
		require.Equal(t, put.Header.Revision, response.Events[0].Kv.ModRevision)
		break
	}

	select {
	case response := <-watch:
		t.Fatalf("next progress tick after an event must be suppressed, got %+v", response)
	case <-time.After(1200 * time.Millisecond):
	case <-ctx.Done():
		t.Fatalf("timed out while checking suppressed progress tick: %v", ctx.Err())
	}
	response := requireClientWatchResponse(t, ctx, watch)
	require.True(t, response.IsProgressNotify(), "the following progress tick must rearm")
}

func TestClientWatchRequestProgressNotifiesAllWatchers(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	grpcServer := grpc.NewServer(server.ClientServerOptions()...)
	etcdserverpb.RegisterKVServer(grpcServer, server)
	etcdserverpb.RegisterWatchServer(grpcServer, server)
	listener := bufconn.Listen(1 << 20)
	go func() { _ = grpcServer.Serve(listener) }()
	t.Cleanup(grpcServer.Stop)

	client, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{"bufnet"},
		DialTimeout: time.Second,
		DialOptions: []grpc.DialOption{
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
				return listener.Dial()
			}),
		},
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, client.Close()) })

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	prefix := fmt.Sprintf("/a1164/watch-progress-all/%d/", time.Now().UnixNano())
	watches := []clientv3.WatchChan{
		client.Watch(ctx, prefix, clientv3.WithPrefix()),
		client.Watch(ctx, prefix, clientv3.WithPrefix()),
	}

	watched, err := client.Put(ctx, prefix+"watched", "1")
	require.NoError(t, err)
	for _, watch := range watches {
		event := requireSingleClientWatchEvent(t, ctx, watch)
		require.Equal(t, watched.Header.Revision, event.Kv.ModRevision)
		require.Equal(t, prefix+"watched", string(event.Kv.Key))
	}

	unwatched, err := client.Put(ctx, prefix[:len(prefix)-1]+"-unwatched", "1")
	require.NoError(t, err)
	require.NoError(t, client.RequestProgress(ctx))

	for _, watch := range watches {
		response := requireClientWatchResponse(t, ctx, watch)
		require.True(t, response.IsProgressNotify())
		require.Empty(t, response.Events)
		require.NotNil(t, response.Header)
		require.GreaterOrEqual(t, response.Header.Revision, unwatched.Header.Revision)
	}
}

func TestClientWatchRequestProgressWithoutWatchers(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	grpcServer := grpc.NewServer(server.ClientServerOptions()...)
	etcdserverpb.RegisterKVServer(grpcServer, server)
	etcdserverpb.RegisterWatchServer(grpcServer, server)
	listener := bufconn.Listen(1 << 20)
	go func() { _ = grpcServer.Serve(listener) }()
	t.Cleanup(grpcServer.Stop)

	client, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{"bufnet"},
		DialTimeout: time.Second,
		DialOptions: []grpc.DialOption{
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
				return listener.Dial()
			}),
		},
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, client.Close()) })

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err = client.Put(ctx, "/a1166/watch-progress-no-watchers", "advance")
	require.NoError(t, err)
	require.NoError(t, client.RequestProgress(ctx))
}

func TestClientWatchRevisionBoundariesMatchEtcd(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	grpcServer := grpc.NewServer(server.ClientServerOptions()...)
	etcdserverpb.RegisterKVServer(grpcServer, server)
	etcdserverpb.RegisterWatchServer(grpcServer, server)
	listener := bufconn.Listen(1 << 20)
	go func() { _ = grpcServer.Serve(listener) }()
	t.Cleanup(grpcServer.Stop)

	client, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{"bufnet"},
		DialTimeout: time.Second,
		DialOptions: []grpc.DialOption{
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
				return listener.Dial()
			}),
		},
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, client.Close()) })

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	prefix := fmt.Sprintf("/a1057/watch-revision-client/%d/", time.Now().UnixNano())

	latestKey := prefix + "latest"
	base, err := client.Get(ctx, latestKey)
	require.NoError(t, err)
	latestCtx, latestCancel := context.WithCancel(ctx)
	defer latestCancel()
	latest := client.Watch(latestCtx, latestKey, clientv3.WithCreatedNotify())
	require.NotZero(t, base.Header.Revision)
	requireClientWatchCreated(t, ctx, latest)
	latestPut, err := client.Put(ctx, latestKey, "after-create")
	require.NoError(t, err)
	latestEvent := requireSingleClientWatchEvent(t, ctx, latest)
	require.Equal(t, clientv3.EventTypePut, latestEvent.Type)
	require.Equal(t, latestKey, string(latestEvent.Kv.Key))
	require.Equal(t, "after-create", string(latestEvent.Kv.Value))
	require.Equal(t, latestPut.Header.Revision, latestEvent.Kv.ModRevision)

	historicalKey := prefix + "historical"
	historicalPut, err := client.Put(ctx, historicalKey, "seed")
	require.NoError(t, err)
	historicalCtx, historicalCancel := context.WithCancel(ctx)
	defer historicalCancel()
	historical := client.Watch(
		historicalCtx, historicalKey, clientv3.WithRev(historicalPut.Header.Revision), clientv3.WithCreatedNotify(),
	)
	requireClientWatchCreated(t, ctx, historical)
	historicalEvent := requireSingleClientWatchEvent(t, ctx, historical)
	require.Equal(t, clientv3.EventTypePut, historicalEvent.Type)
	require.Equal(t, historicalKey, string(historicalEvent.Kv.Key))
	require.Equal(t, "seed", string(historicalEvent.Kv.Value))
	require.Equal(t, historicalPut.Header.Revision, historicalEvent.Kv.ModRevision)

	futureKey := prefix + "future"
	futureBase, err := client.Get(ctx, futureKey)
	require.NoError(t, err)
	startRevision := futureBase.Header.Revision + 2
	futureCtx, futureCancel := context.WithCancel(ctx)
	defer futureCancel()
	future := client.Watch(futureCtx, futureKey, clientv3.WithRev(startRevision), clientv3.WithCreatedNotify())
	requireClientWatchCreated(t, ctx, future)
	require.NoError(t, client.RequestProgress(ctx))
	select {
	case response := <-future:
		require.NoError(t, response.Err())
		t.Fatalf("future watch emitted early response before start revision %d: %+v", startRevision, response)
	case <-time.After(150 * time.Millisecond):
	case <-ctx.Done():
		t.Fatalf("timed out while checking future watch progress suppression: %v", ctx.Err())
	}

	_, err = client.Put(ctx, prefix+"future-unrelated", "advance")
	require.NoError(t, err)
	futurePut, err := client.Put(ctx, futureKey, "future")
	require.NoError(t, err)
	futureEvent := requireSingleClientWatchEvent(t, ctx, future)
	require.Equal(t, clientv3.EventTypePut, futureEvent.Type)
	require.Equal(t, futureKey, string(futureEvent.Kv.Key))
	require.Equal(t, "future", string(futureEvent.Kv.Value))
	require.Equal(t, futurePut.Header.Revision, futureEvent.Kv.ModRevision)
}

func TestClientWatchNegativeRevisionCancelsAndCloses(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	grpcServer := grpc.NewServer(server.ClientServerOptions()...)
	etcdserverpb.RegisterKVServer(grpcServer, server)
	etcdserverpb.RegisterWatchServer(grpcServer, server)
	listener := bufconn.Listen(1 << 20)
	go func() { _ = grpcServer.Serve(listener) }()
	t.Cleanup(grpcServer.Stop)

	client, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{"bufnet"},
		DialTimeout: time.Second,
		DialOptions: []grpc.DialOption{
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
				return listener.Dial()
			}),
		},
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, client.Close()) })

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	key := fmt.Sprintf("/a2131/watch-negative-revision/%d", time.Now().UnixNano())
	watch := client.Watch(ctx, key, clientv3.WithRev(-1))

	response := requireClientWatchCanceledResponse(t, ctx, watch)
	require.True(t, response.Canceled)
	require.False(t, response.Created)
	requireClientWatchError(t, response.Err(), codes.Unknown, "etcdserver: mvcc: required revision has been compacted", rpctypes.ErrCompacted)
	require.Zero(t, response.CompactRevision)
	require.Empty(t, response.Events)
	requireWatchClientClosed(t, ctx, watch)
}

func TestClientWatchCompactedRevisionCancelsAndCloses(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	grpcServer := grpc.NewServer(server.ClientServerOptions()...)
	etcdserverpb.RegisterKVServer(grpcServer, server)
	etcdserverpb.RegisterWatchServer(grpcServer, server)
	listener := bufconn.Listen(1 << 20)
	go func() { _ = grpcServer.Serve(listener) }()
	t.Cleanup(grpcServer.Stop)

	client, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{"bufnet"},
		DialTimeout: time.Second,
		DialOptions: []grpc.DialOption{
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
				return listener.Dial()
			}),
		},
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, client.Close()) })

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	key := fmt.Sprintf("/a1142/watch-compacted/%d", time.Now().UnixNano())
	var compactRevision int64
	for index := 0; index < 5; index++ {
		put, putErr := client.Put(ctx, key, fmt.Sprintf("value-%d", index))
		require.NoError(t, putErr)
		if index == 3 {
			compactRevision = put.Header.Revision
		}
	}
	_, err = client.Compact(ctx, compactRevision)
	require.NoError(t, err)

	watch := client.Watch(ctx, key, clientv3.WithRev(compactRevision-2))
	response := requireClientWatchCanceledResponse(t, ctx, watch)
	require.True(t, response.Canceled)
	requireClientWatchError(t, response.Err(), codes.Unknown, "etcdserver: mvcc: required revision has been compacted", rpctypes.ErrCompacted)
	require.Equal(t, compactRevision, response.CompactRevision)
	require.Empty(t, response.Events)
	requireWatchClientClosed(t, ctx, watch)
}

func TestRawGRPCWatchIDRangeBoundariesKeepStreamAlive(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()
	_, err := server.Put(context.Background(), &etcdserverpb.PutRequest{
		Key: []byte("/a1021/watch-id/seed"), Value: []byte("seed"),
	})
	require.NoError(t, err)

	grpcServer := grpc.NewServer(server.ClientServerOptions()...)
	etcdserverpb.RegisterWatchServer(grpcServer, server)
	listener := bufconn.Listen(1 << 20)
	go func() { _ = grpcServer.Serve(listener) }()
	t.Cleanup(grpcServer.Stop)

	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return listener.Dial()
		}),
	)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, conn.Close()) })

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	stream, err := etcdserverpb.NewWatchClient(conn).Watch(ctx)
	require.NoError(t, err)
	t.Cleanup(func() { _ = stream.CloseSend() })

	create := func(key string, end []byte, id, revision int64) *etcdserverpb.WatchResponse {
		require.NoError(t, stream.Send(&etcdserverpb.WatchRequest{
			RequestUnion: &etcdserverpb.WatchRequest_CreateRequest{
				CreateRequest: &etcdserverpb.WatchCreateRequest{
					Key: []byte(key), RangeEnd: end, WatchId: id, StartRevision: revision,
				},
			},
		}))
		response, recvErr := stream.Recv()
		require.NoError(t, recvErr)
		requireRawWatchHeaderWellFormed(t, response)
		return response
	}
	cancelWatch := func(id int64) *etcdserverpb.WatchResponse {
		require.NoError(t, stream.Send(&etcdserverpb.WatchRequest{
			RequestUnion: &etcdserverpb.WatchRequest_CancelRequest{
				CancelRequest: &etcdserverpb.WatchCancelRequest{WatchId: id},
			},
		}))
		response, recvErr := stream.Recv()
		require.NoError(t, recvErr)
		requireRawWatchHeaderWellFormed(t, response)
		return response
	}

	for _, id := range []int64{-1, math.MinInt64, math.MaxInt64} {
		response := create(fmt.Sprintf("/a1021/watch-id/%d", id), nil, id, 0)
		require.True(t, response.Created)
		require.False(t, response.Canceled)
		require.Equal(t, id, response.WatchId)
	}

	response := create("/a1021/watch-id/negative-duplicate-and-empty",
		[]byte("/a1021/watch-id/negative-duplicate-and-empty"), math.MaxInt64, -1)
	require.True(t, response.Created)
	require.True(t, response.Canceled)
	require.Equal(t, int64(-1), response.WatchId)
	require.Equal(t, rpctypes.ErrCompacted.Error(), response.CancelReason)

	response = create("/a1021/watch-id/duplicate-and-empty",
		[]byte("/a1021/watch-id/duplicate-and-empty"), math.MaxInt64, 0)
	require.True(t, response.Created)
	require.True(t, response.Canceled)
	require.Equal(t, int64(-1), response.WatchId)
	require.Equal(t, "mvcc: watcher range is empty", response.CancelReason)

	response = create("/a1021/watch-id/duplicate", nil, math.MaxInt64, 0)
	require.True(t, response.Created)
	require.True(t, response.Canceled)
	require.Equal(t, int64(-1), response.WatchId)
	require.Equal(t, "mvcc: duplicate watch ID provided on the WatchStream", response.CancelReason)

	response = create("/a1021/watch-id/after-errors", nil, 102, 0)
	require.True(t, response.Created)
	require.False(t, response.Canceled)
	require.Equal(t, int64(102), response.WatchId)

	response = create("/a1021/watch-id/automatic", nil, 0, 0)
	require.True(t, response.Created)
	require.False(t, response.Canceled)
	require.Equal(t, int64(0), response.WatchId)

	for _, id := range []int64{-1, math.MinInt64, math.MaxInt64, 102, 0} {
		response = cancelWatch(id)
		require.False(t, response.Created)
		require.True(t, response.Canceled)
		require.Equal(t, id, response.WatchId)
		require.Empty(t, response.CancelReason)
	}

	require.NoError(t, stream.Send(&etcdserverpb.WatchRequest{
		RequestUnion: &etcdserverpb.WatchRequest_CancelRequest{
			CancelRequest: &etcdserverpb.WatchCancelRequest{WatchId: 999},
		},
	}))
	response = create("/a1021/watch-id/after-unknown-cancel", nil, 103, 0)
	require.True(t, response.Created)
	require.False(t, response.Canceled)
	require.Equal(t, int64(103), response.WatchId)

	response = cancelWatch(103)
	require.False(t, response.Created)
	require.True(t, response.Canceled)
	require.Equal(t, int64(103), response.WatchId)
	require.Empty(t, response.CancelReason)
}

func TestRawGRPCWatchEmptyControlFramesMatchEtcd(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()
	_, err := server.Put(context.Background(), &etcdserverpb.PutRequest{
		Key: []byte("/a1022/watch-invalid-control/seed"), Value: []byte("seed"),
	})
	require.NoError(t, err)

	grpcServer := grpc.NewServer(server.ClientServerOptions()...)
	etcdserverpb.RegisterWatchServer(grpcServer, server)
	listener := bufconn.Listen(1 << 20)
	go func() { _ = grpcServer.Serve(listener) }()
	t.Cleanup(grpcServer.Stop)

	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return listener.Dial()
		}),
	)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, conn.Close()) })

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	stream, err := etcdserverpb.NewWatchClient(conn).Watch(ctx)
	require.NoError(t, err)
	t.Cleanup(func() { _ = stream.CloseSend() })

	require.NoError(t, stream.Send(&etcdserverpb.WatchRequest{}))
	require.NoError(t, stream.Send(&etcdserverpb.WatchRequest{
		RequestUnion: &etcdserverpb.WatchRequest_CreateRequest{},
	}))

	response, err := stream.Recv()
	require.NoError(t, err)
	requireRawWatchHeaderWellFormed(t, response)
	require.True(t, response.Created)
	require.False(t, response.Canceled)
	require.Equal(t, int64(0), response.WatchId)
	require.Empty(t, response.CancelReason)

	require.NoError(t, stream.Send(&etcdserverpb.WatchRequest{
		RequestUnion: &etcdserverpb.WatchRequest_CancelRequest{},
	}))
	response, err = stream.Recv()
	require.NoError(t, err)
	requireRawWatchHeaderWellFormed(t, response)
	require.False(t, response.Created)
	require.True(t, response.Canceled)
	require.Equal(t, int64(0), response.WatchId)
	require.Empty(t, response.CancelReason)

	require.NoError(t, stream.Send(&etcdserverpb.WatchRequest{
		RequestUnion: &etcdserverpb.WatchRequest_ProgressRequest{},
	}))
	require.NoError(t, stream.Send(&etcdserverpb.WatchRequest{
		RequestUnion: &etcdserverpb.WatchRequest_CreateRequest{
			CreateRequest: &etcdserverpb.WatchCreateRequest{
				Key: []byte("/a1022/watch-invalid-control/live"), WatchId: 404,
			},
		},
	}))
	response, err = stream.Recv()
	require.NoError(t, err)
	requireRawWatchHeaderWellFormed(t, response)
	require.True(t, response.Created)
	require.False(t, response.Canceled)
	require.Equal(t, int64(404), response.WatchId)
	require.Empty(t, response.CancelReason)
}

func TestRawGRPCWatchProgressRequestUsesStreamWideID(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()
	seed, err := server.Put(context.Background(), &etcdserverpb.PutRequest{
		Key: []byte("/a1023/watch-progress/seed"), Value: []byte("seed"),
	})
	require.NoError(t, err)

	grpcServer := grpc.NewServer(server.ClientServerOptions()...)
	etcdserverpb.RegisterKVServer(grpcServer, server)
	etcdserverpb.RegisterWatchServer(grpcServer, server)
	listener := bufconn.Listen(1 << 20)
	go func() { _ = grpcServer.Serve(listener) }()
	t.Cleanup(grpcServer.Stop)

	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return listener.Dial()
		}),
	)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, conn.Close()) })

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	stream, err := etcdserverpb.NewWatchClient(conn).Watch(ctx)
	require.NoError(t, err)
	t.Cleanup(func() { _ = stream.CloseSend() })

	key := []byte(fmt.Sprintf("/a1023/watch-progress/%d", time.Now().UnixNano()))
	require.NoError(t, stream.Send(&etcdserverpb.WatchRequest{
		RequestUnion: &etcdserverpb.WatchRequest_CreateRequest{
			CreateRequest: &etcdserverpb.WatchCreateRequest{Key: key, WatchId: 51},
		},
	}))
	created, err := stream.Recv()
	require.NoError(t, err)
	requireRawWatchHeaderWellFormed(t, created)
	require.True(t, created.Created)
	require.False(t, created.Canceled)
	require.Equal(t, int64(51), created.WatchId)
	require.Equal(t, seed.Header.Revision, created.Header.Revision)

	put, err := etcdserverpb.NewKVClient(conn).Put(ctx, &etcdserverpb.PutRequest{
		Key: key, Value: []byte("value"),
	})
	require.NoError(t, err)
	require.Equal(t, seed.Header.Revision+1, put.Header.Revision)
	events, err := stream.Recv()
	require.NoError(t, err)
	requireRawWatchHeaderWellFormed(t, events)
	require.False(t, events.Created)
	require.False(t, events.Canceled)
	require.Equal(t, int64(51), events.WatchId)
	require.Equal(t, put.Header.Revision, events.Header.Revision)
	require.Len(t, events.Events, 1)
	require.Equal(t, put.Header.Revision, events.Events[0].Kv.ModRevision)

	require.NoError(t, stream.Send(&etcdserverpb.WatchRequest{
		RequestUnion: &etcdserverpb.WatchRequest_ProgressRequest{
			ProgressRequest: &etcdserverpb.WatchProgressRequest{},
		},
	}))
	progress, err := stream.Recv()
	require.NoError(t, err)
	requireRawWatchHeaderWellFormed(t, progress)
	require.False(t, progress.Created)
	require.False(t, progress.Canceled)
	require.Equal(t, int64(-1), progress.WatchId)
	require.Empty(t, progress.Events)
	require.Empty(t, progress.CancelReason)
	require.Equal(t, put.Header.Revision, progress.Header.Revision)
}

func TestRawGRPCWatchFilterEnumUnknownAndDuplicateMatchEtcd(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	grpcServer := grpc.NewServer(server.ClientServerOptions()...)
	etcdserverpb.RegisterKVServer(grpcServer, server)
	etcdserverpb.RegisterWatchServer(grpcServer, server)
	listener := bufconn.Listen(1 << 20)
	go func() { _ = grpcServer.Serve(listener) }()
	t.Cleanup(grpcServer.Stop)

	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return listener.Dial()
		}),
	)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, conn.Close()) })

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	kv := etcdserverpb.NewKVClient(conn)

	run := func(
		name string,
		filters []etcdserverpb.WatchCreateRequest_FilterType,
		want []int32,
	) {
		t.Helper()
		key := []byte(fmt.Sprintf("/a1024/watch-filter/%s/%d", name, time.Now().UnixNano()))
		put, err := kv.Put(ctx, &etcdserverpb.PutRequest{Key: key, Value: []byte("value")})
		require.NoError(t, err)
		_, err = kv.DeleteRange(ctx, &etcdserverpb.DeleteRangeRequest{Key: key})
		require.NoError(t, err)

		stream, err := etcdserverpb.NewWatchClient(conn).Watch(ctx)
		require.NoError(t, err)
		t.Cleanup(func() { _ = stream.CloseSend() })
		require.NoError(t, stream.Send(&etcdserverpb.WatchRequest{
			RequestUnion: &etcdserverpb.WatchRequest_CreateRequest{
				CreateRequest: &etcdserverpb.WatchCreateRequest{
					Key: key, StartRevision: put.Header.Revision, Filters: filters,
				},
			},
		}))
		created, err := stream.Recv()
		require.NoError(t, err)
		requireRawWatchHeaderWellFormed(t, created)
		require.True(t, created.Created)
		require.False(t, created.Canceled)

		got := make([]int32, 0, len(want))
		for len(got) < len(want) {
			response, recvErr := stream.Recv()
			require.NoError(t, recvErr)
			requireRawWatchHeaderWellFormed(t, response)
			for _, event := range response.Events {
				got = append(got, int32(event.Type))
			}
		}
		require.Equal(t, want, got)
	}

	run("unknown",
		[]etcdserverpb.WatchCreateRequest_FilterType{
			etcdserverpb.WatchCreateRequest_FilterType(99),
		},
		[]int32{0, 1},
	)
	run("duplicate-noput",
		[]etcdserverpb.WatchCreateRequest_FilterType{
			etcdserverpb.WatchCreateRequest_NOPUT,
			etcdserverpb.WatchCreateRequest_NOPUT,
		},
		[]int32{1},
	)
}

func TestRawGRPCWatchFutureRevisionSuppressesProgressUntilEvent(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()
	_, err := server.Put(context.Background(), &etcdserverpb.PutRequest{
		Key: []byte("/a1025/watch-future/seed"), Value: []byte("seed"),
	})
	require.NoError(t, err)

	grpcServer := grpc.NewServer(server.ClientServerOptions()...)
	etcdserverpb.RegisterKVServer(grpcServer, server)
	etcdserverpb.RegisterWatchServer(grpcServer, server)
	listener := bufconn.Listen(1 << 20)
	go func() { _ = grpcServer.Serve(listener) }()
	t.Cleanup(grpcServer.Stop)

	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return listener.Dial()
		}),
	)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, conn.Close()) })

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	kv := etcdserverpb.NewKVClient(conn)
	watch := etcdserverpb.NewWatchClient(conn)
	key := []byte(fmt.Sprintf("/a1025/watch-future/%d", time.Now().UnixNano()))
	base, err := kv.Range(ctx, &etcdserverpb.RangeRequest{Key: key})
	require.NoError(t, err)
	requireRawRangeHeaderWellFormed(t, base)
	startRevision := base.Header.Revision + 2

	stream, err := watch.Watch(ctx)
	require.NoError(t, err)
	t.Cleanup(func() { _ = stream.CloseSend() })
	require.NoError(t, stream.Send(&etcdserverpb.WatchRequest{
		RequestUnion: &etcdserverpb.WatchRequest_CreateRequest{
			CreateRequest: &etcdserverpb.WatchCreateRequest{
				Key: key, WatchId: 303, StartRevision: startRevision,
			},
		},
	}))
	created, err := stream.Recv()
	require.NoError(t, err)
	requireRawWatchHeaderWellFormed(t, created)
	require.True(t, created.Created)
	require.False(t, created.Canceled)
	require.Equal(t, int64(303), created.WatchId)
	require.Equal(t, base.Header.Revision, created.Header.Revision)

	require.NoError(t, stream.Send(&etcdserverpb.WatchRequest{
		RequestUnion: &etcdserverpb.WatchRequest_ProgressRequest{
			ProgressRequest: &etcdserverpb.WatchProgressRequest{},
		},
	}))
	type watchReceiveResult struct {
		response *etcdserverpb.WatchResponse
		err      error
	}
	pending := make(chan watchReceiveResult, 1)
	go func() {
		response, recvErr := stream.Recv()
		pending <- watchReceiveResult{response: response, err: recvErr}
	}()
	select {
	case received := <-pending:
		require.NoError(t, received.err)
		t.Fatalf("future watch emitted early response before reaching start revision: %v", received.response)
	case <-time.After(150 * time.Millisecond):
	}

	_, err = kv.Put(ctx, &etcdserverpb.PutRequest{
		Key:   []byte(fmt.Sprintf("/a1025/watch-future/unrelated/%d", time.Now().UnixNano())),
		Value: []byte("advance"),
	})
	require.NoError(t, err)
	put, err := kv.Put(ctx, &etcdserverpb.PutRequest{Key: key, Value: []byte("future")})
	require.NoError(t, err)

	received := <-pending
	require.NoError(t, received.err)
	event := received.response
	requireRawWatchHeaderWellFormed(t, event)
	require.False(t, event.Created)
	require.False(t, event.Canceled)
	require.Equal(t, int64(303), event.WatchId)
	require.Len(t, event.Events, 1)
	require.Equal(t, []byte("future"), event.Events[0].Kv.Value)
	require.Equal(t, put.Header.Revision, event.Events[0].Kv.ModRevision)

	require.NoError(t, stream.Send(&etcdserverpb.WatchRequest{
		RequestUnion: &etcdserverpb.WatchRequest_ProgressRequest{
			ProgressRequest: &etcdserverpb.WatchProgressRequest{},
		},
	}))
	progress, err := stream.Recv()
	require.NoError(t, err)
	requireRawWatchHeaderWellFormed(t, progress)
	require.False(t, progress.Created)
	require.False(t, progress.Canceled)
	require.Equal(t, int64(-1), progress.WatchId)
	require.Empty(t, progress.Events)
	require.GreaterOrEqual(t, progress.Header.Revision, startRevision)
}

func TestRawGRPCWatchMaximumStartRevisionCancelsWithoutEvents(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()
	_, err := server.Put(context.Background(), &etcdserverpb.PutRequest{
		Key: []byte("/a1026/watch-maximum/seed"), Value: []byte("seed"),
	})
	require.NoError(t, err)

	grpcServer := grpc.NewServer(server.ClientServerOptions()...)
	etcdserverpb.RegisterKVServer(grpcServer, server)
	etcdserverpb.RegisterWatchServer(grpcServer, server)
	listener := bufconn.Listen(1 << 20)
	go func() { _ = grpcServer.Serve(listener) }()
	t.Cleanup(grpcServer.Stop)

	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return listener.Dial()
		}),
	)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, conn.Close()) })

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	kv := etcdserverpb.NewKVClient(conn)
	key := []byte(fmt.Sprintf("/a1026/watch-maximum/%d", time.Now().UnixNano()))
	base, err := kv.Range(ctx, &etcdserverpb.RangeRequest{Key: key})
	require.NoError(t, err)
	requireRawRangeHeaderWellFormed(t, base)

	stream, err := etcdserverpb.NewWatchClient(conn).Watch(ctx)
	require.NoError(t, err)
	t.Cleanup(func() { _ = stream.CloseSend() })
	require.NoError(t, stream.Send(&etcdserverpb.WatchRequest{
		RequestUnion: &etcdserverpb.WatchRequest_CreateRequest{
			CreateRequest: &etcdserverpb.WatchCreateRequest{
				Key: key, WatchId: 304, StartRevision: math.MaxInt64,
			},
		},
	}))
	created, err := stream.Recv()
	require.NoError(t, err)
	requireRawWatchHeaderWellFormed(t, created)
	require.True(t, created.Created)
	require.False(t, created.Canceled)
	require.Equal(t, int64(304), created.WatchId)
	require.Equal(t, base.Header.Revision, created.Header.Revision)

	put, err := kv.Put(ctx, &etcdserverpb.PutRequest{Key: key, Value: []byte("below-maximum")})
	require.NoError(t, err)
	require.NoError(t, stream.Send(&etcdserverpb.WatchRequest{
		RequestUnion: &etcdserverpb.WatchRequest_CancelRequest{
			CancelRequest: &etcdserverpb.WatchCancelRequest{WatchId: 304},
		},
	}))
	canceled, err := stream.Recv()
	require.NoError(t, err)
	requireRawWatchHeaderWellFormed(t, canceled)
	require.False(t, canceled.Created)
	require.True(t, canceled.Canceled)
	require.Equal(t, int64(304), canceled.WatchId)
	require.Empty(t, canceled.Events)
	require.Empty(t, canceled.CancelReason)
	require.Equal(t, put.Header.Revision, canceled.Header.Revision)
}

func TestRawGRPCWatchRevisionZeroAndHistoricalCurrent(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()
	_, err := server.Put(context.Background(), &etcdserverpb.PutRequest{
		Key: []byte("/a1027/watch-revision/seed"), Value: []byte("seed"),
	})
	require.NoError(t, err)

	grpcServer := grpc.NewServer(server.ClientServerOptions()...)
	etcdserverpb.RegisterKVServer(grpcServer, server)
	etcdserverpb.RegisterWatchServer(grpcServer, server)
	listener := bufconn.Listen(1 << 20)
	go func() { _ = grpcServer.Serve(listener) }()
	t.Cleanup(grpcServer.Stop)

	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return listener.Dial()
		}),
	)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, conn.Close()) })

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	kv := etcdserverpb.NewKVClient(conn)
	watch := etcdserverpb.NewWatchClient(conn)

	fromNowKey := []byte(fmt.Sprintf("/a1027/watch-revision/latest/%d", time.Now().UnixNano()))
	base, err := kv.Range(ctx, &etcdserverpb.RangeRequest{Key: fromNowKey})
	require.NoError(t, err)
	requireRawRangeHeaderWellFormed(t, base)
	fromNow, err := watch.Watch(ctx)
	require.NoError(t, err)
	t.Cleanup(func() { _ = fromNow.CloseSend() })
	require.NoError(t, fromNow.Send(&etcdserverpb.WatchRequest{
		RequestUnion: &etcdserverpb.WatchRequest_CreateRequest{
			CreateRequest: &etcdserverpb.WatchCreateRequest{Key: fromNowKey, WatchId: 301},
		},
	}))
	created, err := fromNow.Recv()
	require.NoError(t, err)
	requireRawWatchHeaderWellFormed(t, created)
	require.True(t, created.Created)
	require.False(t, created.Canceled)
	require.Equal(t, int64(301), created.WatchId)
	require.Equal(t, base.Header.Revision, created.Header.Revision)

	put, err := kv.Put(ctx, &etcdserverpb.PutRequest{Key: fromNowKey, Value: []byte("after-create")})
	require.NoError(t, err)
	event, err := fromNow.Recv()
	require.NoError(t, err)
	requireRawWatchHeaderWellFormed(t, event)
	require.False(t, event.Created)
	require.False(t, event.Canceled)
	require.Equal(t, int64(301), event.WatchId)
	require.Len(t, event.Events, 1)
	require.Equal(t, []byte("after-create"), event.Events[0].Kv.Value)
	require.Equal(t, put.Header.Revision, event.Events[0].Kv.ModRevision)

	historicalKey := []byte(fmt.Sprintf("/a1027/watch-revision/historical/%d", time.Now().UnixNano()))
	historicalPut, err := kv.Put(ctx, &etcdserverpb.PutRequest{Key: historicalKey, Value: []byte("seed")})
	require.NoError(t, err)
	historical, err := watch.Watch(ctx)
	require.NoError(t, err)
	t.Cleanup(func() { _ = historical.CloseSend() })
	require.NoError(t, historical.Send(&etcdserverpb.WatchRequest{
		RequestUnion: &etcdserverpb.WatchRequest_CreateRequest{
			CreateRequest: &etcdserverpb.WatchCreateRequest{
				Key: historicalKey, WatchId: 302, StartRevision: historicalPut.Header.Revision,
			},
		},
	}))
	historicalCreated, err := historical.Recv()
	require.NoError(t, err)
	requireRawWatchHeaderWellFormed(t, historicalCreated)
	require.True(t, historicalCreated.Created)
	require.False(t, historicalCreated.Canceled)
	require.Equal(t, int64(302), historicalCreated.WatchId)
	require.Equal(t, historicalPut.Header.Revision, historicalCreated.Header.Revision)

	historicalEvent, err := historical.Recv()
	require.NoError(t, err)
	requireRawWatchHeaderWellFormed(t, historicalEvent)
	require.False(t, historicalEvent.Created)
	require.False(t, historicalEvent.Canceled)
	require.Equal(t, int64(302), historicalEvent.WatchId)
	require.Len(t, historicalEvent.Events, 1)
	require.Equal(t, []byte("seed"), historicalEvent.Events[0].Kv.Value)
	require.Equal(t, historicalPut.Header.Revision, historicalEvent.Events[0].Kv.ModRevision)
}

func requireRawWatchHeaderWellFormed(t *testing.T, response *etcdserverpb.WatchResponse) {
	t.Helper()
	require.NotNil(t, response.Header)
	require.Positive(t, response.Header.Revision)
}

func requireClientWatchCreated(t *testing.T, ctx context.Context, watch clientv3.WatchChan) {
	t.Helper()
	response := requireClientWatchCreatedResponse(t, ctx, watch)
	require.NotNil(t, response.Header)
}

func requireClientWatchResponse(t *testing.T, ctx context.Context, watch clientv3.WatchChan) clientv3.WatchResponse {
	t.Helper()
	select {
	case response, ok := <-watch:
		require.True(t, ok)
		require.NoError(t, response.Err())
		return response
	case <-ctx.Done():
		t.Fatalf("timed out waiting for watch response: %v", ctx.Err())
		return clientv3.WatchResponse{}
	}
}

func requireClientWatchCanceledResponse(t *testing.T, ctx context.Context, watch clientv3.WatchChan) clientv3.WatchResponse {
	t.Helper()
	select {
	case response, ok := <-watch:
		require.True(t, ok)
		require.True(t, response.Canceled)
		return response
	case <-ctx.Done():
		t.Fatalf("timed out waiting for canceled watch response: %v", ctx.Err())
		return clientv3.WatchResponse{}
	}
}

func requireClientWatchError(t *testing.T, err error, code codes.Code, message string, wantErrorIs ...error) {
	t.Helper()
	require.EqualError(t, err, message)
	for _, want := range wantErrorIs {
		require.ErrorIs(t, err, want)
	}
	require.Equal(t, code, status.Code(err))
	require.Equal(t, message, status.Convert(err).Message())
}

func requireClientWatchCreatedResponse(t *testing.T, ctx context.Context, watch clientv3.WatchChan) clientv3.WatchResponse {
	t.Helper()
	select {
	case response, ok := <-watch:
		require.True(t, ok)
		require.NoError(t, response.Err())
		require.True(t, response.Created)
		require.False(t, response.Canceled)
		require.False(t, response.IsProgressNotify())
		require.Empty(t, response.Events)
		return response
	case <-ctx.Done():
		t.Fatalf("timed out waiting for watch created response: %v", ctx.Err())
		return clientv3.WatchResponse{}
	}
}

func requireSingleClientWatchEvent(t *testing.T, ctx context.Context, watch clientv3.WatchChan) *clientv3.Event {
	t.Helper()
	select {
	case response, ok := <-watch:
		require.True(t, ok)
		require.NoError(t, response.Err())
		require.False(t, response.Created)
		require.False(t, response.Canceled)
		require.False(t, response.IsProgressNotify())
		require.Len(t, response.Events, 1)
		return response.Events[0]
	case <-ctx.Done():
		t.Fatalf("timed out waiting for watch event: %v", ctx.Err())
		return nil
	}
}

func requireNoClientWatchEventBeforeProgress(
	t *testing.T,
	ctx context.Context,
	client *clientv3.Client,
	watch clientv3.WatchChan,
) {
	t.Helper()
	require.NoError(t, client.RequestProgress(ctx))
	response := requireClientWatchResponse(t, ctx, watch)
	require.False(t, response.Created)
	require.Empty(t, response.Events)
	require.True(t, response.IsProgressNotify())
}

func requireRawRangeHeaderWellFormed(t *testing.T, response *etcdserverpb.RangeResponse) {
	t.Helper()
	require.NotNil(t, response.Header)
	require.Positive(t, response.Header.Revision)
}
