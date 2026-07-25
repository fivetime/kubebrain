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
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	recipe "go.etcd.io/etcd/client/v3/experimental/recipes"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
)

func TestClientExperimentalRecipesBarrierAndQueues(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	grpcServer := grpc.NewServer(server.ClientServerOptions()...)
	etcdserverpb.RegisterKVServer(grpcServer, server)
	etcdserverpb.RegisterWatchServer(grpcServer, server)
	listener := bufconn.Listen(1 << 20)
	go func() { _ = grpcServer.Serve(listener) }()
	t.Cleanup(grpcServer.Stop)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	client, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{"bufnet"},
		DialTimeout: time.Second,
		Context:     ctx,
		DialOptions: []grpc.DialOption{
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
				return listener.Dial()
			}),
		},
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, client.Close()) })

	barrier := recipe.NewBarrier(client, "/a968/recipes/barrier")
	require.NoError(t, barrier.Hold())
	require.Error(t, barrier.Hold())
	waitResults := make(chan error, 3)
	for range 3 {
		go func() { waitResults <- recipe.NewBarrier(client, "/a968/recipes/barrier").Wait() }()
	}
	time.Sleep(100 * time.Millisecond)
	require.Empty(t, waitResults, "barrier waiters must block until Release")
	require.NoError(t, barrier.Release())
	for range 3 {
		select {
		case waitErr := <-waitResults:
			require.NoError(t, waitErr)
		case <-ctx.Done():
			t.Fatalf("barrier waiters did not release: %v", ctx.Err())
		}
	}

	_, err = client.Put(ctx, "/a968/recipes/nonexistent-neighbor", "value")
	require.NoError(t, err)
	neighborWait := make(chan error, 1)
	go func() { neighborWait <- recipe.NewBarrier(client, "/a968/recipes/nonexistent").Wait() }()
	select {
	case waitErr := <-neighborWait:
		require.NoError(t, waitErr)
	case <-time.After(time.Second):
		t.Fatal("barrier wait on a nonexistent exact key was blocked by its prefix neighbor")
	}

	fifo := recipe.NewQueue(client, "/a968/recipes/fifo")
	for _, value := range []string{"zero", "one", "two", "three"} {
		require.NoError(t, fifo.Enqueue(value))
	}
	for _, want := range []string{"zero", "one", "two", "three"} {
		got, dequeueErr := fifo.Dequeue()
		require.NoError(t, dequeueErr)
		require.Equal(t, want, got)
	}

	priority := recipe.NewPriorityQueue(client, "/a968/recipes/priority")
	for _, item := range []struct {
		value    string
		priority uint16
	}{
		{value: "two-a", priority: 2},
		{value: "zero-a", priority: 0},
		{value: "one", priority: 1},
		{value: "zero-b", priority: 0},
		{value: "two-b", priority: 2},
	} {
		require.NoError(t, priority.Enqueue(item.value, item.priority))
	}
	for _, want := range []string{"zero-a", "zero-b", "one", "two-a", "two-b"} {
		got, dequeueErr := priority.Dequeue()
		require.NoError(t, dequeueErr)
		require.Equal(t, want, got)
	}
}
