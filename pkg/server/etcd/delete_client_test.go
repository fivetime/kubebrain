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
	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
)

func TestClientDeleteRangeDifferentialScenario(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	grpcServer := grpc.NewServer(server.ClientServerOptions()...)
	server.Register(grpcServer)
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
	prefix := "/a1052/client-delete/"
	base, err := client.Get(ctx, prefix, clientv3.WithPrefix())
	require.NoError(t, err)
	baseRev := base.Header.Revision
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		_, _ = client.Delete(cleanupCtx, prefix, clientv3.WithPrefix())
	})

	putA, err := client.Put(ctx, prefix+"a", "va")
	require.NoError(t, err)
	putB, err := client.Put(ctx, prefix+"b", "vb")
	require.NoError(t, err)
	putC, err := client.Put(ctx, prefix+"c", "vc")
	require.NoError(t, err)
	updateB, err := client.Put(ctx, prefix+"b", "vb2")
	require.NoError(t, err)
	require.Equal(t, []int64{1, 2, 3, 4}, []int64{
		putA.Header.Revision - baseRev,
		putB.Header.Revision - baseRev,
		putC.Header.Revision - baseRev,
		updateB.Header.Revision - baseRev,
	})

	deleted, err := client.Delete(ctx, prefix+"a",
		clientv3.WithRange(prefix+"c"),
		clientv3.WithPrevKV(),
	)
	require.NoError(t, err)
	require.Equal(t, int64(5), deleted.Header.Revision-baseRev)
	require.Equal(t, int64(2), deleted.Deleted)
	require.Equal(t, []deleteClientKV{
		{key: "a", value: "va", createRevision: putA.Header.Revision, modRevision: putA.Header.Revision, version: 1},
		{key: "b", value: "vb2", createRevision: putB.Header.Revision, modRevision: updateB.Header.Revision, version: 2},
	}, deleteClientKVs(deleted.PrevKvs, prefix))

	historical, err := client.Get(ctx, prefix,
		clientv3.WithPrefix(),
		clientv3.WithRev(deleted.Header.Revision-1),
		clientv3.WithSort(clientv3.SortByKey, clientv3.SortAscend),
	)
	require.NoError(t, err)
	require.Equal(t, []deleteClientKV{
		{key: "a", value: "va", createRevision: putA.Header.Revision, modRevision: putA.Header.Revision, version: 1},
		{key: "b", value: "vb2", createRevision: putB.Header.Revision, modRevision: updateB.Header.Revision, version: 2},
		{key: "c", value: "vc", createRevision: putC.Header.Revision, modRevision: putC.Header.Revision, version: 1},
	}, deleteClientKVs(historical.Kvs, prefix))

	current, err := client.Get(ctx, prefix,
		clientv3.WithPrefix(),
		clientv3.WithSort(clientv3.SortByKey, clientv3.SortAscend),
	)
	require.NoError(t, err)
	require.Equal(t, []deleteClientKV{
		{key: "c", value: "vc", createRevision: putC.Header.Revision, modRevision: putC.Header.Revision, version: 1},
	}, deleteClientKVs(current.Kvs, prefix))

	emptyRange, err := client.Delete(ctx, prefix+"c",
		clientv3.WithRange(prefix+"c"),
		clientv3.WithPrevKV(),
	)
	require.NoError(t, err)
	require.Equal(t, current.Header.Revision, emptyRange.Header.Revision)
	require.Zero(t, emptyRange.Deleted)
	require.Empty(t, emptyRange.PrevKvs)

	missing, err := client.Delete(ctx, prefix+"missing", clientv3.WithPrevKV())
	require.NoError(t, err)
	require.Equal(t, emptyRange.Header.Revision, missing.Header.Revision)
	require.Zero(t, missing.Deleted)
	require.Empty(t, missing.PrevKvs)

	final, err := client.Get(ctx, prefix, clientv3.WithPrefix())
	require.NoError(t, err)
	require.Equal(t, missing.Header.Revision, final.Header.Revision)
	require.Equal(t, []deleteClientKV{
		{key: "c", value: "vc", createRevision: putC.Header.Revision, modRevision: putC.Header.Revision, version: 1},
	}, deleteClientKVs(final.Kvs, prefix))
}

type deleteClientKV struct {
	key            string
	value          string
	createRevision int64
	modRevision    int64
	version        int64
}

func deleteClientKVs(kvs []*mvccpb.KeyValue, prefix string) []deleteClientKV {
	out := make([]deleteClientKV, 0, len(kvs))
	for _, kv := range kvs {
		out = append(out, deleteClientKV{
			key:            string(kv.Key[len(prefix):]),
			value:          string(kv.Value),
			createRevision: kv.CreateRevision,
			modRevision:    kv.ModRevision,
			version:        kv.Version,
		})
	}
	return out
}
