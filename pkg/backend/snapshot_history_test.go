// Copyright 2026 ByteDance and/or its affiliates
//
// Licensed under the Apache License, Version 2.0 (the "License");
package backend

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"

	proto "github.com/kubewharf/kubebrain-client/api/v2rpc"

	"github.com/kubewharf/kubebrain/pkg/backend/streamerror"
	"github.com/kubewharf/kubebrain/pkg/metrics/mock"
	imemkv "github.com/kubewharf/kubebrain/pkg/storage/memkv"
)

func TestSnapshotHistoryStreamPreservesLegacyMetadataTombstoneAndCurrentVersion(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	kv := imemkv.NewKvStorage()
	defer func() { require.NoError(t, kv.Close()) }()
	// Disable inline metadata to exercise the pre-Approach-A compatibility path:
	// the stream must join each raw value with its historical etcdmeta record and
	// must never export those reserved metadata objects as user keys.
	b := NewBackend(kv, Config{Prefix: prefix, Identity: getStorageIdentity()}, mock.NewMinimalMetrics(ctrl)).(*backend)
	b.SetCurrentRevision(uint64(time.Now().UnixNano()))
	ctx := context.Background()
	key := []byte(prefix + "/snapshot-history/legacy")
	created, err := b.Create(ctx, &proto.CreateRequest{Key: key, Value: []byte("v1")})
	require.NoError(t, err)
	updated, err := b.Update(ctx, &proto.UpdateRequest{Kv: &proto.KeyValue{Key: key, Value: []byte("v2"), Revision: created.Header.Revision}})
	require.NoError(t, err)
	require.True(t, updated.Succeeded)
	deleted, err := b.Delete(ctx, &proto.DeleteRequest{Key: key})
	require.NoError(t, err)
	require.True(t, deleted.Succeeded)
	recreated, err := b.Create(ctx, &proto.CreateRequest{Key: key, Value: []byte("v3")})
	require.NoError(t, err)
	waitCommitted(t, b, recreated.Header.Revision)

	stream, err := b.SnapshotHistoryStream(ctx, recreated.Header.Revision)
	require.NoError(t, err)
	var records []SnapshotHistoryRecord
	terminal := false
	for chunk := range stream {
		require.NoError(t, chunk.Err)
		require.Equal(t, recreated.Header.Revision, chunk.Revision)
		if chunk.Done {
			terminal = true
		}
		records = append(records, chunk.Records...)
	}
	require.True(t, terminal)
	var matching []SnapshotHistoryRecord
	for _, record := range records {
		if string(record.Key) == string(key) {
			matching = append(matching, record)
		}
		require.NotContains(t, string(record.Key), string(etcdMetadataPrefix))
	}
	require.Len(t, matching, 4)
	require.Equal(t, []byte("v1"), matching[0].Value)
	require.Equal(t, created.Header.Revision, matching[0].CreateRevision)
	require.EqualValues(t, 1, matching[0].Version)
	require.Equal(t, []byte("v2"), matching[1].Value)
	require.Equal(t, created.Header.Revision, matching[1].CreateRevision)
	require.EqualValues(t, 2, matching[1].Version)
	require.True(t, matching[2].Tombstone)
	require.Empty(t, matching[2].Value)
	require.Equal(t, []byte("v3"), matching[3].Value)
	require.Equal(t, recreated.Header.Revision, matching[3].CreateRevision)
	require.EqualValues(t, 1, matching[3].Version)
	require.False(t, matching[0].Current)
	require.False(t, matching[1].Current)
	require.False(t, matching[2].Current)
	require.True(t, matching[3].Current)
}

func TestSnapshotHistoryStreamClassifiesInvalidLegacyMetadata(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	kv := imemkv.NewKvStorage()
	defer func() { require.NoError(t, kv.Close()) }()
	b := NewBackend(kv, Config{Prefix: prefix, Identity: getStorageIdentity()}, mock.NewMinimalMetrics(ctrl)).(*backend)
	b.SetCurrentRevision(uint64(time.Now().UnixNano()))
	ctx := context.Background()
	key := []byte(prefix + "/snapshot-history/corrupt-legacy-metadata")
	created, err := b.Create(ctx, &proto.CreateRequest{Key: key, Value: []byte("value")})
	require.NoError(t, err)
	waitCommitted(t, b, created.Header.Revision)

	batch := kv.BeginBatchWrite()
	batch.Put(b.coder.EncodeObjectKey(b.etcdMetadataUserKey(key), created.Header.Revision), []byte{1}, 0)
	require.NoError(t, batch.Commit(ctx))

	stream, err := b.SnapshotHistoryStream(ctx, created.Header.Revision)
	require.NoError(t, err)
	var records []SnapshotHistoryRecord
	var streamErr error
	for chunk := range stream {
		records = append(records, chunk.Records...)
		if chunk.Err != nil {
			streamErr = chunk.Err
		}
	}
	require.ErrorIs(t, streamErr, ErrInvalidMVCCMetadata)
	require.ErrorContains(t, streamErr, "invalid etcd metadata length 1")
	require.Empty(t, records)
}

func TestSnapshotHistoryStreamRejectsMalformedInlineEnvelope(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	kv := imemkv.NewKvStorage()
	defer func() { require.NoError(t, kv.Close()) }()
	b := NewBackend(kv, Config{Prefix: prefix, Identity: getStorageIdentity(), EnableEtcdCompatibility: true}, mock.NewMinimalMetrics(ctrl)).(*backend)
	b.SetCurrentRevision(uint64(time.Now().UnixNano()))
	ctx := context.Background()
	key := []byte(prefix + "/snapshot-history/corrupt-inline")
	created, err := b.Create(ctx, &proto.CreateRequest{Key: key, Value: []byte("value")})
	require.NoError(t, err)
	waitCommitted(t, b, created.Header.Revision)

	batch := kv.BeginBatchWrite()
	batch.Put(b.coder.EncodeObjectKey(key, created.Header.Revision), []byte{0, 'k', 'b', 1}, 0)
	require.NoError(t, batch.Commit(ctx))
	beforeRevision := b.GetCurrentRevision()
	updated, updateErr := b.Update(ctx, &proto.UpdateRequest{Kv: &proto.KeyValue{
		Key: key, Value: []byte("replacement"), Revision: created.Header.Revision,
	}})
	require.ErrorIs(t, updateErr, ErrInvalidMVCCMetadata)
	require.Nil(t, updated)
	require.Equal(t, beforeRevision, b.GetCurrentRevision(), "rejected corruption must not consume a revision")

	stream, err := b.SnapshotHistoryStream(ctx, created.Header.Revision)
	require.NoError(t, err)
	var records []SnapshotHistoryRecord
	var streamErr error
	for chunk := range stream {
		records = append(records, chunk.Records...)
		if chunk.Err != nil {
			streamErr = chunk.Err
		}
	}
	require.ErrorIs(t, streamErr, ErrInvalidMVCCMetadata)
	require.ErrorContains(t, streamErr, "inline value metadata v1 length 4 is shorter than 20")
	require.Empty(t, records)
}

func TestRevisionIndexCorruptionIsClassifiedAndNotHealed(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	kv := imemkv.NewKvStorage()
	defer func() { require.NoError(t, kv.Close()) }()
	b := NewBackend(kv, Config{Prefix: prefix, Identity: getStorageIdentity(), EnableEtcdCompatibility: true}, mock.NewMinimalMetrics(ctrl)).(*backend)
	b.SetCurrentRevision(uint64(time.Now().UnixNano()))
	ctx := context.Background()
	key := []byte(prefix + "/revision-index/corrupt")
	created, err := b.Create(ctx, &proto.CreateRequest{Key: key, Value: []byte("value")})
	require.NoError(t, err)
	waitCommitted(t, b, created.Header.Revision)

	batch := kv.BeginBatchWrite()
	batch.CAS(b.coder.EncodeRevisionKey(key), []byte{1}, uint64ToBytes(created.Header.Revision), 0)
	require.NoError(t, batch.Commit(ctx))

	response, err := b.Get(ctx, &proto.GetRequest{Key: key})
	require.ErrorIs(t, err, ErrInvalidMVCCMetadata)
	require.ErrorContains(t, err, "revision index for key")
	require.Nil(t, response)
	_, err = b.Update(ctx, &proto.UpdateRequest{Kv: &proto.KeyValue{Key: key, Value: []byte("replacement"), Revision: created.Header.Revision}})
	require.ErrorIs(t, err, ErrInvalidMVCCMetadata)
	_, txnRevision, err := b.TxnApply(ctx, []TxnWriteOp{{Key: key, Value: []byte("txn-replacement")}}, nil)
	require.ErrorIs(t, err, ErrInvalidMVCCMetadata)
	require.Zero(t, txnRevision)
	_, txnRevision, err = b.TxnApply(ctx, []TxnWriteOp{{Key: []byte(prefix + "/revision-index/other"), Value: []byte("value")}}, []TxnGuard{{Key: key, Revision: created.Header.Revision}})
	require.ErrorIs(t, err, ErrInvalidMVCCMetadata)
	require.Zero(t, txnRevision)
	stored, getErr := kv.Get(ctx, b.coder.EncodeRevisionKey(key))
	require.NoError(t, getErr)
	require.Equal(t, []byte{1}, stored, "durable corruption must not enter orphan-index healing")
}

func TestSnapshotHistoryStreamRejectsMalformedObjectKey(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	kv := imemkv.NewKvStorage()
	defer func() { require.NoError(t, kv.Close()) }()
	b := NewBackend(kv, Config{Prefix: prefix, Identity: getStorageIdentity(), EnableEtcdCompatibility: true}, mock.NewMinimalMetrics(ctrl)).(*backend)
	b.SetCurrentRevision(uint64(time.Now().UnixNano()))
	ctx := context.Background()
	key := []byte(prefix + "/object-key/corrupt")
	revision := b.GetCurrentRevision()
	malformed := b.coder.EncodeObjectKey(key, revision)
	malformed[len(malformed)-9] = '!'
	batch := kv.BeginBatchWrite()
	batch.Put(malformed, []byte("value"), 0)
	require.NoError(t, batch.Commit(ctx))

	list, listErr := b.List(ctx, &proto.RangeRequest{
		Key: []byte(prefix + "/object-key/"), End: PrefixEnd([]byte(prefix + "/object-key/")),
	})
	require.ErrorIs(t, listErr, ErrInvalidMVCCMetadata)
	require.Nil(t, list)
	streamed, rangeStreamErr := b.RangeStream(ctx, []byte(prefix+"/object-key/"), PrefixEnd([]byte(prefix+"/object-key/")), revision)
	require.NoError(t, rangeStreamErr)
	var encodedErr string
	for response := range streamed {
		if response.GetErr() != "" {
			encodedErr = response.GetErr()
		}
	}
	decodedErr, ok := streamerror.Decode(encodedErr)
	require.True(t, ok)
	require.ErrorIs(t, decodedErr, ErrInvalidMVCCMetadata)

	stream, err := b.SnapshotHistoryStream(ctx, revision)
	require.NoError(t, err)
	var streamErr error
	var records []SnapshotHistoryRecord
	for chunk := range stream {
		records = append(records, chunk.Records...)
		if chunk.Err != nil {
			streamErr = chunk.Err
		}
	}
	require.ErrorIs(t, streamErr, ErrInvalidMVCCMetadata)
	require.ErrorContains(t, streamErr, "decode snapshot object key")
	require.Empty(t, records)

	_, hashErr := b.HashKV(ctx, int64(revision))
	require.ErrorIs(t, hashErr, ErrInvalidMVCCMetadata)
}

func TestSnapshotHistoryStreamJoinsExactTxnSubrevisions(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	kv := imemkv.NewKvStorage()
	defer func() { require.NoError(t, kv.Close()) }()
	b := NewBackend(kv, Config{Prefix: prefix, Identity: getStorageIdentity(), EnableEtcdCompatibility: true}, mock.NewMinimalMetrics(ctrl)).(*backend)
	b.SetCurrentRevision(uint64(time.Now().UnixNano()))
	ctx := context.Background()
	keys := [][]byte{
		[]byte(prefix + "/snapshot-history/order/z"),
		[]byte(prefix + "/snapshot-history/order/a"),
		[]byte(prefix + "/snapshot-history/order/m"),
	}
	_, revision, err := b.TxnApply(ctx, []TxnWriteOp{
		{Key: keys[0], Value: []byte("z")},
		{Key: keys[1], Value: []byte("a")},
		{Key: keys[2], Value: []byte("m")},
	}, nil)
	require.NoError(t, err)
	waitCommitted(t, b, revision)

	stream, err := b.SnapshotHistoryStream(ctx, revision)
	require.NoError(t, err)
	wanted := map[string]uint32{string(keys[0]): 0, string(keys[1]): 1, string(keys[2]): 2}
	seen := make(map[string]uint32, len(wanted))
	for chunk := range stream {
		require.NoError(t, chunk.Err)
		for _, record := range chunk.Records {
			want, ok := wanted[string(record.Key)]
			if !ok || record.ModRevision != revision {
				continue
			}
			require.True(t, record.Ordered)
			require.Equal(t, want, record.SubRevision)
			require.Equal(t, uint32(3), record.TotalChanges)
			seen[string(record.Key)] = record.SubRevision
		}
	}
	require.Len(t, seen, len(wanted))
}

func TestSnapshotHistoryStreamPinsEventLogUntilConsumerStops(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	kv := imemkv.NewKvStorage()
	defer func() { require.NoError(t, kv.Close()) }()
	b := NewBackend(kv, Config{Prefix: prefix, Identity: getStorageIdentity()}, mock.NewMinimalMetrics(ctrl)).(*backend)
	b.SetCurrentRevision(uint64(time.Now().UnixNano()))
	ops := make([]TxnWriteOp, snapshotHistoryChunkRecords+1)
	for i := range ops {
		ops[i] = TxnWriteOp{Key: []byte(fmt.Sprintf("%s/snapshot-history/pin/%03d", prefix, i)), Value: []byte("v")}
	}
	_, revision, err := b.TxnApply(context.Background(), ops, nil)
	require.NoError(t, err)
	waitCommitted(t, b, revision)
	ctx, cancel := context.WithCancel(context.Background())
	stream, err := b.SnapshotHistoryStream(ctx, revision)
	require.NoError(t, err)
	require.EqualValues(t, 1, b.snapshotPins.min())
	require.Zero(t, b.clampCompactRevision(b.GetCurrentRevision()), "snapshot must preserve event rows above its captured compact watermark")

	cancel()
	for range stream {
	}
	require.Eventually(t, func() bool { return b.snapshotPins.min() == 0 }, time.Second, time.Millisecond)
	require.Equal(t, b.GetCurrentRevision(), b.clampCompactRevision(b.GetCurrentRevision()))
}
