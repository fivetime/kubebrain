// Copyright 2026 ByteDance and/or its affiliates
//
// Licensed under the Apache License, Version 2.0 (the "License");
package etcd

import (
	"bytes"
	"context"
	"fmt"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	bolt "go.etcd.io/bbolt"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/mvccpb"
	"go.etcd.io/etcd/server/v3/storage/schema"
	"google.golang.org/protobuf/proto"
)

type streamingSnapshotBackend struct {
	BackendShim
	fullRangeLists int
	streams        int
	dataChunks     int
	maxChunkKeys   int
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
