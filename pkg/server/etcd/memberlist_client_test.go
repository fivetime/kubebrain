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
	"sort"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
)

func TestClientMemberListAndSyncUseAdvertisedClientURLs(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()
	members, err := ParseInitialCluster(
		"kb-2=http://10.0.0.2:2380,kb-1=http://10.0.0.1:2380,kb-3=http://10.0.0.3:2380",
		2379,
		false,
	)
	require.NoError(t, err)
	server.SetStaticMembers(members)

	grpcServer := grpc.NewServer(server.ClientServerOptions()...)
	etcdserverpb.RegisterKVServer(grpcServer, server)
	etcdserverpb.RegisterClusterServer(grpcServer, server)
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
	list, err := client.MemberList(ctx, clientv3.WithSerializable())
	require.NoError(t, err)
	require.NotNil(t, list.Header)
	require.Zero(t, list.Header.Revision)
	require.NotZero(t, list.Header.ClusterId)
	require.NotZero(t, list.Header.MemberId)
	require.Len(t, list.Members, 3)
	expected := memberListClientURLs(list.Members)
	require.Equal(t, []string{
		"http://10.0.0.1:2379",
		"http://10.0.0.2:2379",
		"http://10.0.0.3:2379",
	}, expected)

	linearizable, err := client.MemberList(ctx)
	require.NoError(t, err)
	require.Zero(t, linearizable.Header.Revision)
	require.Equal(t, expected, memberListClientURLs(linearizable.Members))

	require.NoError(t, client.Sync(ctx))
	actual := client.Endpoints()
	sort.Strings(actual)
	require.Equal(t, expected, actual)

	client.SetEndpoints("bufnet")
	_, err = client.Put(ctx, "/a990/memberlist-sync/data", "ok")
	require.NoError(t, err)
	got, err := client.Get(ctx, "/a990/memberlist-sync/data")
	require.NoError(t, err)
	require.Len(t, got.Kvs, 1)
	require.Equal(t, "ok", string(got.Kvs[0].Value))
}

func memberListClientURLs(members []*etcdserverpb.Member) []string {
	urls := make([]string, 0, len(members))
	for _, member := range members {
		if member.Name == "" || member.IsLearner {
			continue
		}
		urls = append(urls, member.ClientURLs...)
	}
	sort.Strings(urls)
	return urls
}
