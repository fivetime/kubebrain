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

func TestClientCompactBoundaryErrorsMatchEtcd(t *testing.T) {
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
	key := fmt.Sprintf("/a998/compact-client/%d", time.Now().UnixNano())
	first, err := client.Put(ctx, key, "v1")
	require.NoError(t, err)
	second, err := client.Put(ctx, key, "v2")
	require.NoError(t, err)
	third, err := client.Put(ctx, key+"-tail", "tail")
	require.NoError(t, err)
	compact, err := client.Compact(ctx, second.Header.Revision)
	require.NoError(t, err)
	require.GreaterOrEqual(t, compact.Header.Revision, second.Header.Revision)
	require.LessOrEqual(t, compact.Header.Revision, third.Header.Revision)

	boundary, err := client.Get(ctx, key, clientv3.WithRev(second.Header.Revision))
	require.NoError(t, err)
	require.Len(t, boundary.Kvs, 1)
	require.Equal(t, "v2", string(boundary.Kvs[0].Value))

	_, err = client.Get(ctx, key, clientv3.WithRev(first.Header.Revision))
	requireCompactClientError(t, err, codes.Unknown, "etcdserver: mvcc: required revision has been compacted")
	for _, revision := range []int64{second.Header.Revision, first.Header.Revision, -1} {
		_, compactErr := client.Compact(ctx, revision)
		requireCompactClientError(t, compactErr, codes.Unknown, "etcdserver: mvcc: required revision has been compacted")
	}
	_, err = client.Compact(ctx, third.Header.Revision+1000)
	requireCompactClientError(t, err, codes.Unknown, "etcdserver: mvcc: required revision is a future revision")

	current, err := client.Get(ctx, key)
	require.NoError(t, err)
	require.Len(t, current.Kvs, 1)
	require.Equal(t, "v2", string(current.Kvs[0].Value))
}

func TestClientCompactTypedErrorsMatchEtcd(t *testing.T) {
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
	key := fmt.Sprintf("/a1145/compact-typed/%d", time.Now().UnixNano())
	var compactRevision int64
	for index := 0; index < 5; index++ {
		put, putErr := client.Put(ctx, key, fmt.Sprintf("value-%d", index))
		require.NoError(t, putErr)
		compactRevision = put.Header.Revision
	}
	_, err = client.Compact(ctx, compactRevision)
	require.NoError(t, err)

	_, err = client.Compact(ctx, compactRevision)
	require.ErrorIs(t, err, rpctypes.ErrCompacted)
	requireCompactClientError(t, err, codes.Unknown, "etcdserver: mvcc: required revision has been compacted")
	_, err = client.Compact(ctx, compactRevision+1000)
	require.ErrorIs(t, err, rpctypes.ErrFutureRev)
	requireCompactClientError(t, err, codes.Unknown, "etcdserver: mvcc: required revision is a future revision")
}

func TestRawGRPCCompactRevisionBoundaryErrorsMatchEtcd(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	grpcServer := grpc.NewServer(server.ClientServerOptions()...)
	etcdserverpb.RegisterKVServer(grpcServer, server)
	listener := bufconn.Listen(1 << 20)
	go func() { _ = grpcServer.Serve(listener) }()
	t.Cleanup(grpcServer.Stop)

	conn, err := grpc.NewClient("passthrough:///compact-client",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return listener.Dial()
		}))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, conn.Close()) })
	kv := etcdserverpb.NewKVClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	key := []byte(fmt.Sprintf("/a1095/compact-client/%d", time.Now().UnixNano()))
	put, err := kv.Put(ctx, &etcdserverpb.PutRequest{Key: key, Value: []byte("value")})
	require.NoError(t, err)
	_, err = kv.Compact(ctx, &etcdserverpb.CompactionRequest{Revision: put.Header.Revision})
	require.NoError(t, err)

	tests := []struct {
		name     string
		revision int64
		physical bool
		message  string
	}{
		{name: "zero-logical", message: "etcdserver: mvcc: required revision has been compacted"},
		{name: "zero-physical", physical: true, message: "etcdserver: mvcc: required revision has been compacted"},
		{name: "negative-physical", revision: -1, physical: true, message: "etcdserver: mvcc: required revision has been compacted"},
		{name: "max-logical", revision: math.MaxInt64, message: "etcdserver: mvcc: required revision is a future revision"},
		{name: "max-physical", revision: math.MaxInt64, physical: true, message: "etcdserver: mvcc: required revision is a future revision"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, compactErr := kv.Compact(ctx, &etcdserverpb.CompactionRequest{
				Revision: tt.revision,
				Physical: tt.physical,
			})
			require.Error(t, compactErr)
			require.Equal(t, codes.OutOfRange, status.Code(compactErr))
			require.Equal(t, tt.message, status.Convert(compactErr).Message())
		})
	}
}

func requireCompactClientError(t *testing.T, err error, code codes.Code, message string) {
	t.Helper()
	require.Error(t, err)
	require.Equal(t, code, status.Code(err))
	require.Equal(t, message, status.Convert(err).Message())
}
