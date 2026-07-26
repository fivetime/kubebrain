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
	"errors"
	"fmt"
	"io"
	"math"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

type cancelBlockingRangeStreamShim struct {
	BackendShim
	firstForwarded chan struct{}
	release        chan struct{}
}

func (b *cancelBlockingRangeStreamShim) RangeStreamChan(
	ctx context.Context, startKey, endKey []byte, revision uint64,
) (<-chan rangeStreamChunk, error) {
	input, err := b.BackendShim.RangeStreamChan(ctx, startKey, endKey, revision)
	if err != nil {
		return nil, err
	}
	output := make(chan rangeStreamChunk)
	go func() {
		defer close(output)
		first := true
		for chunk := range input {
			if !first {
				select {
				case <-b.release:
				case <-ctx.Done():
					return
				}
			}
			select {
			case output <- chunk:
			case <-ctx.Done():
				return
			}
			if first {
				first = false
				close(b.firstForwarded)
			}
		}
	}()
	return output, nil
}

func TestClientRangeStreamCommonShapesMatchUnaryRange(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	grpcServer := grpc.NewServer(server.ClientServerOptions()...)
	etcdserverpb.RegisterKVServer(grpcServer, server)
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

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	prefix := fmt.Sprintf("\xff\xfe/a988/rangestream-client/%d/", time.Now().UnixNano())
	first, err := client.Put(ctx, prefix+"a", "v1")
	require.NoError(t, err)
	_, err = client.Put(ctx, prefix+"b", "v2")
	require.NoError(t, err)
	_, err = client.Put(ctx, prefix+"a", "v3")
	require.NoError(t, err)
	prefixEnd := clientv3.GetPrefixRangeEnd(prefix)

	tests := []struct {
		name string
		key  string
		opts []clientv3.OpOption
	}{
		{name: "point-hit", key: prefix + "a"},
		{name: "point-miss", key: prefix + "missing"},
		{name: "equal-empty", key: prefix + "a", opts: []clientv3.OpOption{clientv3.WithRange(prefix + "a")}},
		{name: "reversed-empty", key: prefix + "z", opts: []clientv3.OpOption{clientv3.WithRange(prefix + "a")}},
		{name: "prefix-limit-one", key: prefix, opts: []clientv3.OpOption{clientv3.WithRange(prefixEnd), clientv3.WithLimit(1)}},
		{name: "historical-after-first-put", key: prefix, opts: []clientv3.OpOption{clientv3.WithRange(prefixEnd), clientv3.WithRev(first.Header.Revision)}},
		{name: "from-key", key: prefix, opts: []clientv3.OpOption{clientv3.WithFromKey()}},
		{name: "keys-only", key: prefix, opts: []clientv3.OpOption{clientv3.WithRange(prefixEnd), clientv3.WithKeysOnly()}},
		{name: "count-only-limit", key: prefix, opts: []clientv3.OpOption{clientv3.WithRange(prefixEnd), clientv3.WithCountOnly(), clientv3.WithLimit(1)}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			unary, err := client.Get(ctx, test.key, test.opts...)
			require.NoError(t, err)
			stream, err := client.GetStream(ctx, test.key, test.opts...)
			require.NoError(t, err)
			merged, err := clientv3.GetStreamToGetResponse(stream)
			require.NoError(t, err)
			requireClientRangeStreamMatchesUnary(t, (*clientv3.GetResponse)(merged), unary)
		})
	}
}

func requireClientRangeStreamMatchesUnary(t *testing.T, streamed, unary *clientv3.GetResponse) {
	t.Helper()
	require.NotNil(t, streamed.Header)
	require.NotNil(t, unary.Header)
	require.Equal(t, unary.Header.Revision, streamed.Header.Revision)
	require.Equal(t, unary.Count, streamed.Count)
	require.Equal(t, unary.More, streamed.More)
	require.Len(t, streamed.Kvs, len(unary.Kvs))
	for index := range unary.Kvs {
		require.Equal(t, unary.Kvs[index].Key, streamed.Kvs[index].Key)
		require.Equal(t, unary.Kvs[index].Value, streamed.Kvs[index].Value)
		require.Equal(t, unary.Kvs[index].CreateRevision, streamed.Kvs[index].CreateRevision)
		require.Equal(t, unary.Kvs[index].ModRevision, streamed.Kvs[index].ModRevision)
		require.Equal(t, unary.Kvs[index].Version, streamed.Kvs[index].Version)
		require.Equal(t, unary.Kvs[index].Lease, streamed.Kvs[index].Lease)
	}
}

func TestClientRangeStreamValidationErrorsMatchEtcd(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	grpcServer := grpc.NewServer(server.ClientServerOptions()...)
	etcdserverpb.RegisterKVServer(grpcServer, server)
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
	client := etcdserverpb.NewKVClient(conn)

	tests := []struct {
		name    string
		req     *etcdserverpb.RangeRequest
		code    codes.Code
		message string
	}{
		{
			name:    "empty-key",
			req:     &etcdserverpb.RangeRequest{},
			code:    codes.InvalidArgument,
			message: "etcdserver: key is not provided",
		},
		{
			name:    "invalid-sort-order",
			req:     &etcdserverpb.RangeRequest{Key: []byte("/a989/rangestream-validation"), SortOrder: etcdserverpb.RangeRequest_SortOrder(99)},
			code:    codes.InvalidArgument,
			message: "etcdserver: invalid sort option",
		},
		{
			name:    "invalid-sort-target",
			req:     &etcdserverpb.RangeRequest{Key: []byte("/a989/rangestream-validation"), SortTarget: etcdserverpb.RangeRequest_SortTarget(99)},
			code:    codes.InvalidArgument,
			message: "etcdserver: invalid sort option",
		},
		{
			name: "custom-sort",
			req: &etcdserverpb.RangeRequest{
				Key: []byte("/a989/rangestream-validation"), RangeEnd: []byte("/a989/rangestream-validation0"),
				SortOrder: etcdserverpb.RangeRequest_DESCEND, SortTarget: etcdserverpb.RangeRequest_KEY,
			},
			code:    codes.Unimplemented,
			message: "RangeStream does not support custom sort orders",
		},
		{
			name: "revision-filter",
			req: &etcdserverpb.RangeRequest{
				Key: []byte("/a989/rangestream-validation"), RangeEnd: []byte("/a989/rangestream-validation0"),
				MinModRevision: 1,
			},
			code:    codes.Unimplemented,
			message: "RangeStream does not support revision filters",
		},
		{
			name: "custom-sort-before-revision-filter",
			req: &etcdserverpb.RangeRequest{
				Key: []byte("/a989/rangestream-validation"), RangeEnd: []byte("/a989/rangestream-validation0"),
				SortOrder: etcdserverpb.RangeRequest_DESCEND, SortTarget: etcdserverpb.RangeRequest_KEY,
				MinModRevision: 1,
			},
			code:    codes.Unimplemented,
			message: "RangeStream does not support custom sort orders",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			stream, err := client.RangeStream(ctx, test.req)
			if err == nil {
				_, err = stream.Recv()
			}
			require.Error(t, err)
			require.Equal(t, test.code, status.Code(err))
			require.Equal(t, test.message, status.Convert(err).Message())
		})
	}
}

func TestRawGRPCRangeStreamLimitCountAcrossChunks(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()
	server.SetRequestLimits(defaultMaxTxnOps, 256)

	grpcServer := grpc.NewServer(server.ClientServerOptions()...)
	etcdserverpb.RegisterKVServer(grpcServer, server)
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
	client := etcdserverpb.NewKVClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	prefix := fmt.Sprintf("/a1122/rangestream-count/%d/", time.Now().UnixNano())
	rangeEnd := clientv3.GetPrefixRangeEnd(prefix)
	const totalKeys = 12
	for index := 0; index < totalKeys; index++ {
		_, err = client.Put(ctx, &etcdserverpb.PutRequest{
			Key:   []byte(fmt.Sprintf("%s%02d", prefix, index)),
			Value: []byte(fmt.Sprintf("value-%02d-%090d", index, index)),
		})
		require.NoError(t, err)
	}

	stream, err := client.RangeStream(ctx, &etcdserverpb.RangeRequest{
		Key:      []byte(prefix),
		RangeEnd: []byte(rangeEnd),
		Limit:    5,
	})
	require.NoError(t, err)

	var keys []string
	chunks := 0
	var terminal *etcdserverpb.RangeResponse
	for {
		chunk, recvErr := stream.Recv()
		if recvErr != nil {
			require.ErrorIs(t, recvErr, io.EOF)
			break
		}
		chunks++
		response := chunk.GetRangeResponse()
		require.NotNil(t, response)
		if response.Header == nil {
			require.Zero(t, response.Count)
			require.False(t, response.More)
		} else {
			require.Nil(t, terminal, "RangeStream must send one terminal metadata chunk")
			terminal = response
		}
		for _, kv := range response.Kvs {
			keys = append(keys, string(kv.Key))
		}
	}
	require.Greater(t, chunks, 1)
	require.Len(t, keys, 5)
	for index, key := range keys {
		require.Equal(t, fmt.Sprintf("%s%02d", prefix, index), key)
	}
	require.NotNil(t, terminal)
	require.Equal(t, int64(totalKeys), terminal.Count)
	require.True(t, terminal.More)
	require.Empty(t, terminal.Kvs)
}

func TestClientRangeStreamRevisionBoundaries(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	grpcServer := grpc.NewServer(server.ClientServerOptions()...)
	etcdserverpb.RegisterKVServer(grpcServer, server)
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
	prefix := fmt.Sprintf("/a1038/rangestream-revision/%d/", time.Now().UnixNano())
	key := prefix + "a"
	_, err = client.Put(ctx, key, "value")
	require.NoError(t, err)
	current, err := client.Get(ctx, key)
	require.NoError(t, err)
	require.Len(t, current.Kvs, 1)
	_, err = client.Compact(ctx, current.Header.Revision)
	require.NoError(t, err)

	stream, err := client.GetStream(ctx, prefix, clientv3.WithPrefix(), clientv3.WithRev(-1))
	require.NoError(t, err)
	negative, err := clientv3.GetStreamToGetResponse(stream)
	require.NoError(t, err)
	require.Equal(t, int64(1), negative.Count)
	require.Len(t, negative.Kvs, 1)
	require.Equal(t, key, string(negative.Kvs[0].Key))
	require.Equal(t, "value", string(negative.Kvs[0].Value))

	stream, err = client.GetStream(ctx, key, clientv3.WithRev(math.MaxInt64))
	require.NoError(t, err)
	_, err = clientv3.GetStreamToGetResponse(stream)
	requireClientRangeStreamError(t, err, codes.Unknown, "etcdserver: mvcc: required revision is a future revision")
}

func TestClientRangeStreamCompactedErrorIsTyped(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	grpcServer := grpc.NewServer(server.ClientServerOptions()...)
	etcdserverpb.RegisterKVServer(grpcServer, server)
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
	key := fmt.Sprintf("/a1132/rangestream-compacted/%d", time.Now().UnixNano())
	first, err := client.Put(ctx, key, "one")
	require.NoError(t, err)
	var latest *clientv3.PutResponse
	for i := 0; i < 4; i++ {
		latest, err = client.Put(ctx, key, fmt.Sprintf("value-%d", i))
		require.NoError(t, err)
	}
	_, err = client.Compact(ctx, latest.Header.Revision)
	require.NoError(t, err)

	_, err = client.Get(ctx, key, clientv3.WithRev(first.Header.Revision))
	require.True(t, errors.Is(err, rpctypes.ErrCompacted), "Get returned %T %v", err, err)
	stream, err := client.GetStream(ctx, key, clientv3.WithRev(first.Header.Revision))
	require.NoError(t, err)
	_, err = clientv3.GetStreamToGetResponse(stream)
	require.True(t, errors.Is(err, rpctypes.ErrCompacted), "GetStream returned %T %v", err, err)
}

func TestRawGRPCRangeStreamCancelAfterPartialChunkKeepsConnectionUsable(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()
	release := make(chan struct{})
	t.Cleanup(func() { close(release) })
	blockingBackend := &cancelBlockingRangeStreamShim{
		BackendShim:    server.backend,
		firstForwarded: make(chan struct{}),
		release:        release,
	}
	server.backend = blockingBackend

	grpcServer := grpc.NewServer(server.ClientServerOptions()...)
	etcdserverpb.RegisterKVServer(grpcServer, server)
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
	client := etcdserverpb.NewKVClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	prefix := fmt.Sprintf("/a1091/rangestream-cancel/%d/", time.Now().UnixNano())
	value := make([]byte, 1024)
	for i := range value {
		value[i] = byte('a' + i%26)
	}
	const keyCount = 8
	for i := 0; i < keyCount; i++ {
		_, err = client.Put(ctx, &etcdserverpb.PutRequest{
			Key:   []byte(fmt.Sprintf("%s%03d", prefix, i)),
			Value: value,
		})
		require.NoError(t, err)
	}
	server.SetRequestLimits(defaultMaxTxnOps, 512)

	streamCtx, streamCancel := context.WithCancel(ctx)
	defer streamCancel()
	stream, err := client.RangeStream(streamCtx, &etcdserverpb.RangeRequest{
		Key:      []byte(prefix),
		RangeEnd: []byte(clientv3.GetPrefixRangeEnd(prefix)),
	})
	require.NoError(t, err)
	first, err := stream.Recv()
	require.NoError(t, err)
	select {
	case <-blockingBackend.firstForwarded:
	case <-ctx.Done():
		require.NoError(t, ctx.Err())
	}
	require.NotNil(t, first.RangeResponse)
	require.NotEmpty(t, first.RangeResponse.Kvs)
	require.Nil(t, first.RangeResponse.Header)
	require.Less(t, len(first.RangeResponse.Kvs), keyCount)

	streamCancel()
	for {
		_, err = stream.Recv()
		if err != nil {
			break
		}
	}
	require.Equal(t, codes.Canceled, status.Code(err))

	rangeResp, err := client.Range(ctx, &etcdserverpb.RangeRequest{
		Key:       []byte(prefix),
		RangeEnd:  []byte(clientv3.GetPrefixRangeEnd(prefix)),
		CountOnly: true,
	})
	require.NoError(t, err)
	require.EqualValues(t, keyCount, rangeResp.Count)
}

func requireClientRangeStreamError(t *testing.T, err error, code codes.Code, message string) {
	t.Helper()
	require.Error(t, err)
	require.Equal(t, code, status.Code(err))
	require.Equal(t, message, status.Convert(err).Message())
}
