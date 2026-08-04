// Copyright 2026 ByteDance and/or its affiliates
//
// Licensed under the Apache License, Version 2.0 (the "License");
package etcd

import (
	"bytes"
	"context"
	"fmt"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	bolt "go.etcd.io/bbolt"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/mvccpb"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	"go.etcd.io/etcd/server/v3/storage/schema"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/proto"

	"github.com/kubewharf/kubebrain/pkg/server/service/etcdproxy"
)

type streamingSnapshotBackend struct {
	BackendShim
	fullRangeLists int
	streams        int
	dataChunks     int
	maxChunkKeys   int
}

type pausedSnapshotBackend struct {
	BackendShim
	paused  chan struct{}
	release chan struct{}
	once    sync.Once
}

type localSnapshotTrapBackend struct {
	BackendShim
	called bool
}

type stalledSnapshotBackend struct {
	BackendShim
	afterFirst bool
	started    chan struct{}
	release    chan struct{}
}

func (b *stalledSnapshotBackend) SnapshotStreamChan(ctx context.Context, revision uint64) (<-chan rangeStreamChunk, error) {
	out := make(chan rangeStreamChunk)
	if !b.afterFirst {
		go func() {
			close(b.started)
			<-b.release
			close(out)
		}()
		return out, nil
	}
	source, err := b.BackendShim.SnapshotStreamChan(ctx, revision)
	if err != nil {
		return nil, err
	}
	go func() {
		first, ok := <-source
		if ok {
			out <- first
		}
		close(b.started)
		<-b.release
		close(out)
	}()
	return out, nil
}

func (b *localSnapshotTrapBackend) BeginRangeTxn(ctx context.Context) (context.Context, func()) {
	b.called = true
	return b.BackendShim.BeginRangeTxn(ctx)
}

func TestMaintenanceSnapshotFollowerForwardsCompleteStreamToLeader(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()
	trap := &localSnapshotTrapBackend{BackendShim: server.backend}
	server.backend = trap
	want := []*etcdserverpb.SnapshotResponse{
		{RemainingBytes: 3, Blob: []byte("abc"), Version: "3.7.0"},
		{RemainingBytes: 0, Blob: bytes.Repeat([]byte{1}, 32), Version: "3.7.0"},
	}
	server.peers = testPeerService{
		isLeader: false, proxyEnabled: true,
		snapshotFn: func(_ context.Context, request *etcdserverpb.SnapshotRequest) (<-chan etcdproxy.SnapshotResult, error) {
			require.NotNil(t, request)
			results := make(chan etcdproxy.SnapshotResult, len(want))
			for _, response := range want {
				results <- etcdproxy.SnapshotResult{Response: response}
			}
			close(results)
			return results, nil
		},
	}
	stream := &maintenanceSnapshotServer{ctx: context.Background()}
	require.NoError(t, server.Snapshot(&etcdserverpb.SnapshotRequest{}, stream))
	require.False(t, trap.called, "a follower must not capture independently from concurrent leader metadata mutations")
	require.Equal(t, want, stream.responses)
}

func TestMaintenanceSnapshotFollowerForwardsRootCredential(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()
	_ = setupAuthKVUser(t, server)
	rootToken, err := server.tokens.authenticate(context.Background(), "root", "root-secret")
	require.NoError(t, err)
	rootCtx := metadata.NewIncomingContext(context.Background(), metadata.Pairs(rpctypes.TokenFieldNameGRPC, rootToken))
	server.peers = testPeerService{
		isLeader: false, proxyEnabled: true,
		snapshotFn: func(ctx context.Context, _ *etcdserverpb.SnapshotRequest) (<-chan etcdproxy.SnapshotResult, error) {
			outgoing, ok := metadata.FromOutgoingContext(ctx)
			require.True(t, ok)
			require.Equal(t, []string{rootToken}, outgoing.Get(rpctypes.TokenFieldNameGRPC))
			results := make(chan etcdproxy.SnapshotResult)
			close(results)
			return results, nil
		},
	}
	require.NoError(t, server.Snapshot(&etcdserverpb.SnapshotRequest{}, &maintenanceSnapshotServer{ctx: rootCtx}))
}

func (b *pausedSnapshotBackend) SnapshotStreamChan(ctx context.Context, revision uint64) (<-chan rangeStreamChunk, error) {
	source, err := b.BackendShim.SnapshotStreamChan(ctx, revision)
	if err != nil {
		return nil, err
	}
	out := make(chan rangeStreamChunk)
	go func() {
		defer close(out)
		for chunk := range source {
			select {
			case out <- chunk:
			case <-ctx.Done():
				return
			}
			if chunk.resp != nil && len(chunk.resp.Kvs) > 0 {
				b.once.Do(func() {
					close(b.paused)
					select {
					case <-b.release:
					case <-ctx.Done():
					}
				})
			}
		}
	}()
	return out, nil
}

func TestMaintenanceSnapshotDoesNotBlockWritesAndKeepsPinnedRevision(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()
	seed, err := server.Put(context.Background(), &etcdserverpb.PutRequest{Key: []byte("snapshot-seed"), Value: []byte("before")})
	require.NoError(t, err)
	wrapped := &pausedSnapshotBackend{
		BackendShim: server.backend,
		paused:      make(chan struct{}),
		release:     make(chan struct{}),
	}
	server.backend = wrapped

	path := filepath.Join(t.TempDir(), "snapshot.db")
	snapshotDone := make(chan error, 1)
	go func() { snapshotDone <- server.buildSnapshot(context.Background(), path) }()
	select {
	case <-wrapped.paused:
	case <-time.After(5 * time.Second):
		t.Fatal("snapshot did not reach its first data chunk")
	}
	putDone := make(chan *etcdserverpb.PutResponse, 1)
	putErr := make(chan error, 1)
	go func() {
		response, putError := server.Put(context.Background(), &etcdserverpb.PutRequest{Key: []byte("after-snapshot-pin"), Value: []byte("after")})
		putDone <- response
		putErr <- putError
	}()
	var concurrentPut *etcdserverpb.PutResponse
	writeCompleted := false
	select {
	case concurrentPut = <-putDone:
		writeCompleted = true
	case <-time.After(250 * time.Millisecond):
	}
	close(wrapped.release)
	if !writeCompleted {
		concurrentPut = <-putDone
	}
	require.NoError(t, <-putErr)
	require.True(t, writeCompleted, "a pinned snapshot must not hold the logical-write barrier for its full scan")
	require.NoError(t, <-snapshotDone)
	require.Greater(t, concurrentPut.Header.Revision, seed.Header.Revision)

	db, err := bolt.Open(path, 0o400, &bolt.Options{ReadOnly: true})
	require.NoError(t, err)
	defer db.Close()
	var keys [][]byte
	require.NoError(t, db.View(func(tx *bolt.Tx) error {
		return tx.Bucket(schema.Key.Name()).ForEach(func(_, value []byte) error {
			kv := new(mvccpb.KeyValue)
			if err := proto.Unmarshal(value, kv); err != nil {
				return err
			}
			keys = append(keys, kv.Key)
			return nil
		})
	}))
	// The snapshot linearizes before the concurrent Put: it retains the seed
	// but excludes the later key despite allowing that write to complete.
	require.Contains(t, keys, []byte("snapshot-seed"))
	require.NotContains(t, keys, []byte("after-snapshot-pin"))
}

func TestMaintenanceSnapshotEmptyKeyspaceCompletesAfterTerminalHandshake(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()
	require.NoError(t, server.buildSnapshot(context.Background(), filepath.Join(t.TempDir(), "snapshot.db")))
}

func TestMaintenanceSnapshotCancellationInterruptsStalledStream(t *testing.T) {
	for _, tc := range []struct {
		name       string
		afterFirst bool
	}{
		{name: "before first response"},
		{name: "after first response", afterFirst: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			server, closeFn := newTestRPCServer(t)
			defer closeFn()
			stalled := &stalledSnapshotBackend{
				BackendShim: server.backend,
				afterFirst:  tc.afterFirst,
				started:     make(chan struct{}),
				release:     make(chan struct{}),
			}
			server.backend = stalled
			ctx, cancel := context.WithCancel(context.Background())
			done := make(chan error, 1)
			go func() {
				done <- server.buildSnapshot(ctx, filepath.Join(t.TempDir(), "snapshot.db"))
			}()
			select {
			case <-stalled.started:
			case <-time.After(5 * time.Second):
				t.Fatal("snapshot stream did not reach the injected stall")
			}
			cancel()
			returnedOnCancel := false
			var snapshotErr error
			select {
			case snapshotErr = <-done:
				returnedOnCancel = true
			case <-time.After(250 * time.Millisecond):
			}
			close(stalled.release)
			if !returnedOnCancel {
				snapshotErr = <-done
			}
			require.True(t, returnedOnCancel, "client cancellation must interrupt a stalled snapshot stream")
			require.ErrorIs(t, snapshotErr, context.Canceled)
		})
	}
}

func (b *streamingSnapshotBackend) List(ctx context.Context, req *etcdserverpb.RangeRequest) (*etcdserverpb.RangeResponse, error) {
	if bytes.Equal(req.Key, []byte{0}) && bytes.Equal(req.RangeEnd, []byte{0}) {
		b.fullRangeLists++
		return nil, fmt.Errorf("snapshot must not materialize the complete keyspace with List")
	}
	return b.BackendShim.List(ctx, req)
}

func (b *streamingSnapshotBackend) SnapshotStreamChan(ctx context.Context, revision uint64) (<-chan rangeStreamChunk, error) {
	b.streams++
	source, err := b.BackendShim.SnapshotStreamChan(ctx, revision)
	if err != nil {
		return nil, err
	}
	out := make(chan rangeStreamChunk)
	go func() {
		defer close(out)
		for chunk := range source {
			if chunk.resp != nil && len(chunk.resp.Kvs) > 0 {
				b.dataChunks++
				if len(chunk.resp.Kvs) > b.maxChunkKeys {
					b.maxChunkKeys = len(chunk.resp.Kvs)
				}
			}
			out <- chunk
		}
	}()
	return out, nil
}

func TestMaintenanceSnapshotStreamsCurrentKVAndPreservesMetadata(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()
	aliceCtx := setupAuthKVUser(t, server)
	var lastRevision int64
	const recordCount = 605
	for i := 0; i < recordCount; i++ {
		put, err := server.Put(aliceCtx, &etcdserverpb.PutRequest{
			Key: []byte(fmt.Sprintf("/allowed/snapshot/%03d", i)), Value: []byte(fmt.Sprintf("value-%03d", i)),
		})
		require.NoError(t, err)
		lastRevision = put.Header.Revision
	}
	_, err := server.backend.ArmNoSpace(context.Background(), 29)
	require.NoError(t, err)
	require.NoError(t, server.backend.ArmCorrupt(context.Background(), 31))
	_, err = server.mutateGenericAlarm(context.Background(), etcdserverpb.AlarmType(127), 37, true)
	require.NoError(t, err)

	wrapped := &streamingSnapshotBackend{BackendShim: server.backend}
	server.backend = wrapped
	path := filepath.Join(t.TempDir(), "snapshot.db")
	require.NoError(t, server.buildSnapshot(context.Background(), path))
	require.Zero(t, wrapped.fullRangeLists)
	require.Equal(t, 1, wrapped.streams)
	require.Greater(t, wrapped.dataChunks, 1)
	require.Less(t, wrapped.maxChunkKeys, recordCount)

	db, err := bolt.Open(path, 0o400, &bolt.Options{ReadOnly: true})
	require.NoError(t, err)
	defer db.Close()
	require.NoError(t, db.View(func(tx *bolt.Tx) error {
		var records []*mvccpb.KeyValue
		require.NoError(t, tx.Bucket(schema.Key.Name()).ForEach(func(_, value []byte) error {
			kv := new(mvccpb.KeyValue)
			if err := proto.Unmarshal(value, kv); err != nil {
				return err
			}
			if !bytes.Equal(kv.Key, []byte("\x00kubebrain-snapshot-revision")) {
				records = append(records, kv)
			}
			return nil
		}))
		require.Len(t, records, recordCount)
		require.Equal(t, lastRevision, records[len(records)-1].ModRevision)
		require.Equal(t, []byte("value-604"), records[len(records)-1].Value)
		require.NotNil(t, tx.Bucket(schema.AuthUsers.Name()).Get([]byte("alice")))
		var alarms []string
		require.NoError(t, tx.Bucket(schema.Alarm.Name()).ForEach(func(key, _ []byte) error {
			alarm := new(etcdserverpb.AlarmMember)
			if err := proto.Unmarshal(key, alarm); err != nil {
				return err
			}
			alarms = append(alarms, fmt.Sprintf("%d/%d", alarm.MemberID, alarm.Alarm))
			return nil
		}))
		require.ElementsMatch(t, []string{"29/1", "31/2", "37/127"}, alarms)
		return nil
	}))
}
