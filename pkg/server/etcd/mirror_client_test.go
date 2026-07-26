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
	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/mirror"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

func TestClientMirrorSyncBasePaginationAndUpdates(t *testing.T) {
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
	base := fmt.Sprintf("/a1100/mirror/%d/", time.Now().UnixNano())
	prefix := base + "prefix/"
	neighborKey := base + "prefix0/neighbor"
	const baseKeys = 1005
	for index := 0; index < baseKeys; index++ {
		key := fmt.Sprintf("%s%04d", prefix, index)
		_, err = client.Put(ctx, key, fmt.Sprintf("base-%04d", index))
		require.NoError(t, err)
	}
	_, err = client.Put(ctx, neighborKey, "outside")
	require.NoError(t, err)

	syncer := mirror.NewSyncer(client, prefix, 0)
	baseResponses, baseErrors := syncer.SyncBase(ctx)
	baseCount := 0
	pages := 0
	var baseRevision int64
	firstKey := ""
	lastKey := ""
	for response := range baseResponses {
		pages++
		require.NotNil(t, response.Header)
		if baseRevision == 0 {
			baseRevision = response.Header.Revision
		}
		require.Equal(t, baseRevision, response.Header.Revision)
		for _, kv := range response.Kvs {
			require.True(t, strings.HasPrefix(string(kv.Key), prefix))
			require.NotEqual(t, neighborKey, string(kv.Key))
			if firstKey == "" {
				firstKey = string(kv.Key)
			}
			lastKey = string(kv.Key)
			baseCount++
		}
	}
	for syncErr := range baseErrors {
		require.NoError(t, syncErr)
	}
	require.GreaterOrEqual(t, pages, 2)
	require.Equal(t, baseKeys, baseCount)
	require.Equal(t, fmt.Sprintf("%s0000", prefix), firstKey)
	require.Equal(t, fmt.Sprintf("%s1004", prefix), lastKey)

	updates := syncer.SyncUpdates(ctx)
	updateKey := prefix + "0500"
	deleteKey := prefix + "0001"
	put, err := client.Put(ctx, updateKey, "updated")
	require.NoError(t, err)
	deleted, err := client.Delete(ctx, deleteKey)
	require.NoError(t, err)
	require.Equal(t, int64(1), deleted.Deleted)

	seen := map[string]mvccpb.Event_EventType{}
	for len(seen) < 2 {
		select {
		case response, ok := <-updates:
			require.True(t, ok, "mirror update watch closed")
			require.NoError(t, response.Err())
			for _, event := range response.Events {
				seen[string(event.Kv.Key)] = event.Type
				require.Greater(t, event.Kv.ModRevision, baseRevision)
			}
		case <-ctx.Done():
			t.Fatalf("timed out waiting for mirror updates: %v", ctx.Err())
		}
	}
	require.Equal(t, mvccpb.PUT, seen[updateKey])
	require.Equal(t, mvccpb.DELETE, seen[deleteKey])
	require.Greater(t, put.Header.Revision, baseRevision)
}

func TestClientMirrorSyncHistoricalRevisionAndCompactedError(t *testing.T) {
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
	base := fmt.Sprintf("/a1104/mirror/%d/", time.Now().UnixNano())
	prefix := base + "prefix/"
	neighborKey := base + "prefix0/neighbor"

	_, err = client.Txn(ctx).Then(
		clientv3.OpPut(prefix+"a", "base-a"),
		clientv3.OpPut(prefix+"b", "base-b"),
		clientv3.OpPut(neighborKey, "outside"),
	).Commit()
	require.NoError(t, err)
	update, err := client.Txn(ctx).Then(
		clientv3.OpPut(prefix+"a", "updated-a"),
		clientv3.OpPut(prefix+"c", "created-c"),
	).Commit()
	require.NoError(t, err)
	historicalRevision := update.Header.Revision

	baseSyncer := mirror.NewSyncer(client, prefix, historicalRevision)
	baseResponses, baseErrors := baseSyncer.SyncBase(ctx)
	snapshot := map[string]string{}
	for response := range baseResponses {
		for _, kv := range response.Kvs {
			key := string(kv.Key)
			require.True(t, strings.HasPrefix(key, prefix))
			require.NotEqual(t, neighborKey, key)
			snapshot[key] = string(kv.Value)
			require.LessOrEqual(t, kv.ModRevision, historicalRevision)
		}
	}
	for syncErr := range baseErrors {
		require.NoError(t, syncErr)
	}
	require.Equal(t, map[string]string{
		prefix + "a": "updated-a",
		prefix + "b": "base-b",
		prefix + "c": "created-c",
	}, snapshot)

	postBase, err := client.Txn(ctx).Then(
		clientv3.OpDelete(prefix+"b"),
		clientv3.OpPut(prefix+"d", "post-base-d"),
		clientv3.OpPut(neighborKey, "still-outside"),
	).Commit()
	require.NoError(t, err)

	updateSyncer := mirror.NewSyncer(client, prefix, historicalRevision)
	updates := updateSyncer.SyncUpdates(ctx)
	seen := map[string]mvccpb.Event_EventType{}
	for len(seen) < 2 {
		select {
		case response, ok := <-updates:
			require.True(t, ok, "mirror historical update watch closed")
			require.NoError(t, response.Err())
			require.NotEqual(t, int64(0), response.Header.Revision)
			for _, event := range response.Events {
				key := string(event.Kv.Key)
				require.True(t, strings.HasPrefix(key, prefix))
				require.NotEqual(t, neighborKey, key)
				require.Greater(t, event.Kv.ModRevision, historicalRevision)
				require.LessOrEqual(t, event.Kv.ModRevision, postBase.Header.Revision)
				seen[key] = event.Type
			}
		case <-ctx.Done():
			t.Fatalf("timed out waiting for historical mirror updates: %v", ctx.Err())
		}
	}
	require.Equal(t, mvccpb.DELETE, seen[prefix+"b"])
	require.Equal(t, mvccpb.PUT, seen[prefix+"d"])

	compactedPrefix := base + "compacted/"
	compacted, err := client.Put(ctx, compactedPrefix+"key", "value")
	require.NoError(t, err)
	_, err = client.Put(ctx, compactedPrefix+"newer", "newer")
	require.NoError(t, err)
	_, err = client.Compact(ctx, compacted.Header.Revision)
	require.NoError(t, err)

	compactedSyncer := mirror.NewSyncer(client, compactedPrefix, compacted.Header.Revision-1)
	compactedResponses, compactedErrors := compactedSyncer.SyncBase(ctx)
	for response := range compactedResponses {
		require.Empty(t, response.Kvs)
	}
	var compactedErr error
	for syncErr := range compactedErrors {
		if syncErr != nil {
			compactedErr = syncErr
		}
	}
	require.Error(t, compactedErr)
	require.Equal(t, codes.Unknown, status.Code(compactedErr))
	require.Equal(t, "etcdserver: mvcc: required revision has been compacted", status.Convert(compactedErr).Message())
}
