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
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
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
	_, err := server.Put(context.Background(), &etcdserverpb.PutRequest{
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

	put, err := etcdserverpb.NewKVClient(conn).Put(ctx, &etcdserverpb.PutRequest{
		Key: key, Value: []byte("value"),
	})
	require.NoError(t, err)
	events, err := stream.Recv()
	require.NoError(t, err)
	requireRawWatchHeaderWellFormed(t, events)
	require.False(t, events.Created)
	require.False(t, events.Canceled)
	require.Equal(t, int64(51), events.WatchId)
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
	require.GreaterOrEqual(t, progress.Header.Revision, put.Header.Revision)
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
	require.GreaterOrEqual(t, created.Header.Revision, base.Header.Revision)

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
	require.GreaterOrEqual(t, canceled.Header.Revision, put.Header.Revision)
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
	require.GreaterOrEqual(t, created.Header.Revision, base.Header.Revision)

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
	require.GreaterOrEqual(t, historicalCreated.Header.Revision, historicalPut.Header.Revision)

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

func requireRawRangeHeaderWellFormed(t *testing.T, response *etcdserverpb.RangeResponse) {
	t.Helper()
	require.NotNil(t, response.Header)
	require.Positive(t, response.Header.Revision)
}
