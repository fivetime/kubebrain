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
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
)

func TestClientSTMCreateAbortRetryAndSerializableSnapshot(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	grpcServer := grpc.NewServer(server.ClientServerOptions()...)
	etcdserverpb.RegisterKVServer(grpcServer, server)
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

	newKey := "/a971/stm/new"
	_, err = concurrency.NewSTM(client, func(stm concurrency.STM) error {
		stm.Put(newKey, "new-value")
		return nil
	}, concurrency.WithIsolation(concurrency.RepeatableReads))
	require.NoError(t, err)
	newResponse, err := client.Get(ctx, newKey)
	require.NoError(t, err)
	require.Len(t, newResponse.Kvs, 1)
	require.Equal(t, "new-value", string(newResponse.Kvs[0].Value))
	require.Equal(t, int64(1), newResponse.Kvs[0].Version)

	abortKey := "/a971/stm/abort"
	abortCtx, abortCancel := context.WithCancel(ctx)
	_, abortErr := concurrency.NewSTM(client, func(stm concurrency.STM) error {
		stm.Put(abortKey, "must-not-commit")
		abortCancel()
		stm.Put(abortKey, "still-must-not-commit")
		return nil
	}, concurrency.WithIsolation(concurrency.RepeatableReads), concurrency.WithAbortContext(abortCtx))
	require.ErrorIs(t, abortErr, context.Canceled)
	abortResponse, err := client.Get(ctx, abortKey)
	require.NoError(t, err)
	require.Empty(t, abortResponse.Kvs)

	sourceKey := "/a971/stm/delete-source"
	resultKey := "/a971/stm/delete-result"
	_, err = client.Put(ctx, sourceKey, "source")
	require.NoError(t, err)
	deleted := make(chan struct{})
	releaseDelete := make(chan struct{})
	deleteErrors := make(chan error, 1)
	go func() {
		defer close(deleted)
		<-releaseDelete
		_, deleteErr := client.Delete(ctx, sourceKey)
		deleteErrors <- deleteErr
	}()
	attempts := 0
	firstRead := ""
	_, err = concurrency.NewSTM(client, func(stm concurrency.STM) error {
		attempts++
		value := stm.Get(sourceKey)
		if attempts == 1 {
			firstRead = value
			close(releaseDelete)
			<-deleted
		}
		stm.Put(resultKey, value+"-committed")
		return nil
	}, concurrency.WithIsolation(concurrency.RepeatableReads))
	require.NoError(t, err)
	require.NoError(t, <-deleteErrors)
	require.Equal(t, "source", firstRead)
	require.GreaterOrEqual(t, attempts, 2)
	retryResponse, err := client.Get(ctx, resultKey)
	require.NoError(t, err)
	require.Len(t, retryResponse.Kvs, 1)
	require.Equal(t, "-committed", string(retryResponse.Kvs[0].Value))

	snapshotReadKey := "/a971/stm/snapshot-read"
	snapshotWriteKey := "/a971/stm/snapshot-write"
	_, err = client.Put(ctx, snapshotReadKey, "stable")
	require.NoError(t, err)
	snapshotAttempts := 0
	applySnapshot := func(stm concurrency.STM) error {
		snapshotAttempts++
		stm.Get(snapshotReadKey)
		stm.Put(snapshotWriteKey, "value")
		return nil
	}
	_, err = concurrency.NewSTM(
		client, applySnapshot, concurrency.WithIsolation(concurrency.SerializableSnapshot),
	)
	require.NoError(t, err)
	_, err = concurrency.NewSTM(
		client, applySnapshot, concurrency.WithIsolation(concurrency.SerializableSnapshot),
	)
	require.NoError(t, err)
	require.Equal(t, 2, snapshotAttempts)
	snapshotResponse, err := client.Get(ctx, snapshotWriteKey)
	require.NoError(t, err)
	require.Len(t, snapshotResponse.Kvs, 1)
	require.Equal(t, "value", string(snapshotResponse.Kvs[0].Value))
	require.Equal(t, int64(2), snapshotResponse.Kvs[0].Version)

	serializePrefix := "/a971/stm/serializable/"
	serializeKeys := make([]string, 4)
	for index := range serializeKeys {
		serializeKeys[index] = fmt.Sprintf("%s%d", serializePrefix, index)
		_, err = client.Put(ctx, serializeKeys[index], "0")
		require.NoError(t, err)
	}
	for generation := 1; generation <= 3; generation++ {
		ops := make([]clientv3.Op, 0, len(serializeKeys))
		for _, key := range serializeKeys {
			ops = append(ops, clientv3.OpPut(key, fmt.Sprint(generation)))
		}
		update, txnErr := client.Txn(ctx).Then(ops...).Commit()
		require.NoError(t, txnErr)
		require.True(t, update.Succeeded)
		_, err = concurrency.NewSTM(client, func(stm concurrency.STM) error {
			first := stm.Get(serializeKeys[0])
			for _, key := range serializeKeys[1:] {
				require.Equal(t, first, stm.Get(key))
			}
			return nil
		}, concurrency.WithIsolation(concurrency.Serializable))
		require.NoError(t, err)
	}
}

func TestClientSTMSerializableReadsAtomicBatches(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	grpcServer := grpc.NewServer(server.ClientServerOptions()...)
	etcdserverpb.RegisterKVServer(grpcServer, server)
	listener := bufconn.Listen(1 << 20)
	go func() { _ = grpcServer.Serve(listener) }()
	t.Cleanup(grpcServer.Stop)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	newClient := func() *clientv3.Client {
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
		return client
	}
	client := newClient()

	keys := make([]string, 5)
	for index := range keys {
		keys[index] = fmt.Sprintf("/a2089/stm/serializable-batch/%d", index)
	}
	readerErrs := make(chan error, 5)
	updateErr := make(chan error, 1)
	updated := make(chan struct{})
	go func() {
		defer close(updated)
		for generation := 0; generation < 5; generation++ {
			ops := make([]clientv3.Op, 0, len(keys))
			for _, key := range keys {
				ops = append(ops, clientv3.OpPut(key, fmt.Sprint(generation)))
			}
			txnResp, err := client.Txn(ctx).Then(ops...).Commit()
			if err != nil {
				updateErr <- err
				return
			}
			if !txnResp.Succeeded {
				updateErr <- fmt.Errorf("batch %d transaction did not succeed", generation)
				return
			}
			updated <- struct{}{}
		}
		updateErr <- nil
	}()

	readers := 0
	for range updated {
		readers++
		reader := newClient()
		go func() {
			_, err := concurrency.NewSTM(reader, func(stm concurrency.STM) error {
				values := make([]string, 0, len(keys))
				for _, key := range keys {
					values = append(values, stm.Get(key))
				}
				for index := range values {
					if values[index] != values[0] {
						return fmt.Errorf("got values[%d]=%q, want %q", index, values[index], values[0])
					}
				}
				return nil
			}, concurrency.WithIsolation(concurrency.Serializable))
			readerErrs <- err
		}()
	}
	require.NoError(t, <-updateErr)
	for index := 0; index < readers; index++ {
		require.NoError(t, <-readerErrs)
	}
}
