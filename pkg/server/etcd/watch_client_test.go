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
	"net"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
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
