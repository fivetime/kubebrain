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
	"sort"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/naming/endpoints"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
)

func TestClientNamingManagerUpdateListWatchAndLeaseDeletion(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	grpcServer := grpc.NewServer(server.ClientServerOptions()...)
	etcdserverpb.RegisterKVServer(grpcServer, server)
	etcdserverpb.RegisterWatchServer(grpcServer, server)
	etcdserverpb.RegisterLeaseServer(grpcServer, server)
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
	managerPrefix := "/a967/naming/manager"
	manager, err := endpoints.NewManager(client, managerPrefix)
	require.NoError(t, err)
	watchCtx, watchCancel := context.WithCancel(ctx)
	defer watchCancel()
	updates, err := manager.NewWatchChannel(watchCtx)
	require.NoError(t, err)

	require.NoError(t, manager.Update(ctx, []*endpoints.UpdateWithOpts{
		endpoints.NewAddUpdateOpts(managerPrefix+"/e1", endpoints.Endpoint{Addr: "127.0.0.1:2001", Metadata: "metadata-1"}),
		endpoints.NewAddUpdateOpts(managerPrefix+"/e2", endpoints.Endpoint{Addr: "127.0.0.1:2002", Metadata: "metadata-2"}),
	}))
	require.Equal(t, []string{
		"add:e1:127.0.0.1:2001:metadata-1",
		"add:e2:127.0.0.1:2002:metadata-2",
	}, namingClientUpdates(t, receiveNamingClientUpdates(t, ctx, updates), managerPrefix))
	listed, err := manager.List(ctx)
	require.NoError(t, err)
	require.Equal(t, []string{
		"e1:127.0.0.1:2001:metadata-1",
		"e2:127.0.0.1:2002:metadata-2",
	}, namingClientList(listed, managerPrefix))

	require.NoError(t, manager.Update(ctx, []*endpoints.UpdateWithOpts{
		endpoints.NewDeleteUpdateOpts(managerPrefix + "/e1"),
		endpoints.NewAddUpdateOpts(managerPrefix+"/e3", endpoints.Endpoint{Addr: "127.0.0.1:2003", Metadata: "metadata-3"}),
	}))
	require.Equal(t, []string{
		"add:e3:127.0.0.1:2003:metadata-3",
		"delete:e1::",
	}, namingClientUpdates(t, receiveNamingClientUpdates(t, ctx, updates), managerPrefix))
	listed, err = manager.List(ctx)
	require.NoError(t, err)
	require.Equal(t, []string{
		"e2:127.0.0.1:2002:metadata-2",
		"e3:127.0.0.1:2003:metadata-3",
	}, namingClientList(listed, managerPrefix))

	otherPrefix := "/a967/naming/manager-other"
	other, err := endpoints.NewManager(client, otherPrefix)
	require.NoError(t, err)
	require.NoError(t, other.AddEndpoint(ctx, otherPrefix+"/foreign", endpoints.Endpoint{Addr: "127.0.0.1:2999"}))
	mainAfterOther, err := manager.List(ctx)
	require.NoError(t, err)
	otherList, err := other.List(ctx)
	require.NoError(t, err)
	require.Equal(t, []string{
		"e2:127.0.0.1:2002:metadata-2",
		"e3:127.0.0.1:2003:metadata-3",
	}, namingClientList(mainAfterOther, managerPrefix))
	require.Equal(t, []string{"foreign:127.0.0.1:2999:<nil>"}, namingClientList(otherList, otherPrefix))

	lease, err := client.Grant(ctx, 30)
	require.NoError(t, err)
	leaseKey := managerPrefix + "/leased"
	require.NoError(t, manager.AddEndpoint(ctx, leaseKey,
		endpoints.Endpoint{Addr: "127.0.0.1:2010", Metadata: "leased"}, clientv3.WithLease(lease.ID)))
	require.Equal(t, []string{"add:leased:127.0.0.1:2010:leased"},
		namingClientUpdates(t, receiveNamingClientUpdates(t, ctx, updates), managerPrefix))
	_, err = client.Revoke(ctx, lease.ID)
	require.NoError(t, err)
	require.Equal(t, []string{"delete:leased::"},
		namingClientUpdates(t, receiveNamingClientUpdates(t, ctx, updates), managerPrefix))
}

func receiveNamingClientUpdates(
	t *testing.T, ctx context.Context, updates endpoints.WatchChannel,
) []*endpoints.Update {
	t.Helper()
	select {
	case update, ok := <-updates:
		require.True(t, ok, "endpoint watch closed")
		return update
	case <-ctx.Done():
		t.Fatalf("endpoint watch update timed out: %v", ctx.Err())
		return nil
	}
}

func namingClientUpdates(t *testing.T, updates []*endpoints.Update, prefix string) []string {
	t.Helper()
	result := make([]string, 0, len(updates))
	for _, update := range updates {
		op := "add"
		if update.Op == endpoints.Delete {
			op = "delete"
		}
		metadata := ""
		if update.Endpoint.Metadata != nil {
			metadata = fmt.Sprint(update.Endpoint.Metadata)
		}
		result = append(result, fmt.Sprintf("%s:%s:%s:%v",
			op, update.Key[len(prefix)+1:], update.Endpoint.Addr, metadata))
	}
	sort.Strings(result)
	return result
}

func namingClientList(values endpoints.Key2EndpointMap, prefix string) []string {
	result := make([]string, 0, len(values))
	for key, endpoint := range values {
		result = append(result, fmt.Sprintf("%s:%s:%v", key[len(prefix)+1:], endpoint.Addr, endpoint.Metadata))
	}
	sort.Strings(result)
	return result
}
