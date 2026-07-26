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
	etcdresolver "go.etcd.io/etcd/client/v3/naming/resolver"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
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

func TestClientNamingManagerInitialAtomicWatchAndTxnDelete(t *testing.T) {
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
	managerPrefix := "/a1098/naming/atomic"
	manager, err := endpoints.NewManager(client, managerPrefix)
	require.NoError(t, err)
	require.NoError(t, manager.Update(ctx, []*endpoints.UpdateWithOpts{
		endpoints.NewAddUpdateOpts(managerPrefix+"/host1", endpoints.Endpoint{Addr: "127.0.0.1:2101"}),
		endpoints.NewAddUpdateOpts(managerPrefix+"/host2", endpoints.Endpoint{Addr: "127.0.0.1:2102"}),
	}))

	watchCtx, watchCancel := context.WithCancel(ctx)
	defer watchCancel()
	updates, err := manager.NewWatchChannel(watchCtx)
	require.NoError(t, err)
	require.Equal(t, []string{
		"add:host1:127.0.0.1:2101:",
		"add:host2:127.0.0.1:2102:",
	}, namingClientUpdates(t, receiveNamingClientUpdates(t, ctx, updates), managerPrefix))

	deleteResponse, err := client.Txn(ctx).Then(
		clientv3.OpDelete(managerPrefix+"/host1"),
		clientv3.OpDelete(managerPrefix+"/host2"),
	).Commit()
	require.NoError(t, err)
	require.True(t, deleteResponse.Succeeded)
	require.Equal(t, []string{
		"delete:host1::",
		"delete:host2::",
	}, namingClientUpdates(t, receiveNamingClientUpdates(t, ctx, updates), managerPrefix))
}

func TestClientNamingResolverSwitchesAfterEndpointDelete(t *testing.T) {
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

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	servingAddr, stopServing := startNamingClientHealthServer(t, healthpb.HealthCheckResponse_SERVING)
	defer stopServing()
	notServingAddr, stopNotServing := startNamingClientHealthServer(t, healthpb.HealthCheckResponse_NOT_SERVING)
	defer stopNotServing()

	resolverPrefix := fmt.Sprintf("/a1075/naming-resolver/%d", time.Now().UnixNano())
	manager, err := endpoints.NewManager(client, resolverPrefix)
	require.NoError(t, err)
	require.NoError(t, manager.Update(ctx, []*endpoints.UpdateWithOpts{
		endpoints.NewAddUpdateOpts(resolverPrefix+"/serving", endpoints.Endpoint{Addr: servingAddr}),
		endpoints.NewAddUpdateOpts(resolverPrefix+"/not-serving", endpoints.Endpoint{Addr: notServingAddr}),
	}))

	builder, err := etcdresolver.NewBuilder(client)
	require.NoError(t, err)
	connection, err := grpc.NewClient("etcd:///"+resolverPrefix,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithResolvers(builder),
		grpc.WithDefaultServiceConfig(`{"loadBalancingPolicy":"pick_first"}`))
	require.NoError(t, err)
	defer func() { require.NoError(t, connection.Close()) }()
	healthClient := healthpb.NewHealthClient(connection)

	initialStatus := namingClientHealthStatus(t, ctx, healthClient)
	switch initialStatus {
	case healthpb.HealthCheckResponse_SERVING.String():
		require.NoError(t, manager.DeleteEndpoint(ctx, resolverPrefix+"/serving"))
	case healthpb.HealthCheckResponse_NOT_SERVING.String():
		require.NoError(t, manager.DeleteEndpoint(ctx, resolverPrefix+"/not-serving"))
	default:
		t.Fatalf("unexpected initial resolver health status %q", initialStatus)
	}
	wantStatus := healthpb.HealthCheckResponse_SERVING.String()
	if initialStatus == wantStatus {
		wantStatus = healthpb.HealthCheckResponse_NOT_SERVING.String()
	}
	require.Eventually(t, func() bool {
		callCtx, callCancel := context.WithTimeout(ctx, time.Second)
		defer callCancel()
		response, callErr := healthClient.Check(
			callCtx, &healthpb.HealthCheckRequest{}, grpc.WaitForReady(true),
		)
		return callErr == nil && response.Status.String() == wantStatus
	}, 5*time.Second, 20*time.Millisecond)
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

func startNamingClientHealthServer(
	t *testing.T,
	status healthpb.HealthCheckResponse_ServingStatus,
) (string, func()) {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	server := grpc.NewServer()
	healthServer := health.NewServer()
	healthServer.SetServingStatus("", status)
	healthpb.RegisterHealthServer(server, healthServer)
	go func() { _ = server.Serve(listener) }()
	return listener.Addr().String(), func() {
		server.Stop()
		_ = listener.Close()
	}
}

func namingClientHealthStatus(t *testing.T, ctx context.Context, client healthpb.HealthClient) string {
	t.Helper()
	callCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	response, err := client.Check(callCtx, &healthpb.HealthCheckRequest{}, grpc.WaitForReady(true))
	require.NoError(t, err)
	return response.Status.String()
}

func namingClientList(values endpoints.Key2EndpointMap, prefix string) []string {
	result := make([]string, 0, len(values))
	for key, endpoint := range values {
		result = append(result, fmt.Sprintf("%s:%s:%v", key[len(prefix)+1:], endpoint.Addr, endpoint.Metadata))
	}
	sort.Strings(result)
	return result
}
