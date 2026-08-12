// Copyright 2026 ByteDance and/or its affiliates
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

package backend

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	proto "github.com/kubewharf/kubebrain-client/api/v2rpc"
	"github.com/kubewharf/kubebrain/pkg/storage"
)

func TestDurableRevisionTracksResolvedUserWrites(t *testing.T) {
	b, ctx := newTxnApplyBackend(t)
	resp, err := b.Create(ctx, &proto.CreateRequest{
		Key:   []byte(prefix + "/durable-revision/key"),
		Value: []byte("value"),
	})
	require.NoError(t, err)
	require.True(t, resp.Succeeded)

	revision, err := b.GetDurableRevision(ctx)
	require.NoError(t, err)
	require.Equal(t, resp.Header.Revision, revision,
		"a successful user write must atomically persist its restart watermark")
}

func TestDurableRevisionNeverMovesBackward(t *testing.T) {
	b, _ := newTxnApplyBackend(t)
	b.persistDurableRevision(200)
	b.persistDurableRevision(150)
	revision, err := b.GetDurableRevision(context.Background())
	require.NoError(t, err)
	require.Equal(t, uint64(200), revision)
}

func TestDurableRevisionCorruptionFailsClosed(t *testing.T) {
	validZero := make([]byte, 8)
	valid := make([]byte, 8)
	binary.BigEndian.PutUint64(valid, 42)
	overflow := make([]byte, 8)
	binary.BigEndian.PutUint64(overflow, uint64(math.MaxInt64)+1)
	for _, test := range []struct {
		name string
		raw  []byte
	}{
		{name: "empty", raw: nil},
		{name: "short", raw: []byte{1}},
		{name: "trailing bytes", raw: append(valid, 0)},
		{name: "zero", raw: validZero},
		{name: "wire overflow", raw: overflow},
	} {
		t.Run(test.name, func(t *testing.T) {
			b, ctx := newTxnApplyBackend(t)
			batch := b.kv.BeginBatchWrite()
			batch.Put(b.ks.EncodeInternalKey(durableRevisionKey), test.raw, 0)
			require.NoError(t, batch.Commit(ctx))

			_, err := b.GetDurableRevision(ctx)
			require.ErrorIs(t, err, ErrInvalidMVCCMetadata)
			require.ErrorIs(t, b.persistDurableRevisionContext(ctx, 100), ErrInvalidMVCCMetadata)
			require.ErrorIs(t, b.InitializeLeadershipRevision(ctx, 100), ErrInvalidMVCCMetadata)
		})
	}
}

func TestRevisionAllocatorRejectsWireOverflow(t *testing.T) {
	b, _ := newTxnApplyBackend(t)
	b.tso.Init(math.MaxInt64)
	revision, err := b.deal(math.MaxInt64)
	require.Zero(t, revision)
	require.ErrorIs(t, err, ErrRevisionExhausted)
}

func TestLeadershipRevisionKeepsUserRevisionsContiguous(t *testing.T) {
	b, ctx := newTxnApplyBackend(t)
	publicRevision := b.GetCurrentRevision()
	b.persistDurableRevision(publicRevision)
	allocationFloor := publicRevision + watchersChanCapacity + 10_000
	require.NoError(t, b.InitializeLeadershipRevision(ctx, allocationFloor))
	require.Equal(t, publicRevision, b.GetCurrentRevision())
	require.Equal(t, publicRevision, b.collectorRevision.Load())
	require.Equal(t, publicRevision, b.tso.Dealt(),
		"the PD election timestamp must not leak into the client-visible MVCC sequence")

	response, err := b.Create(ctx, &proto.CreateRequest{
		Key: []byte(prefix + "/leadership-watermarks/key"), Value: []byte("value"),
	})
	require.NoError(t, err)
	require.Equal(t, publicRevision+1, response.Header.Revision)
	require.Eventually(t, func() bool {
		return b.GetCurrentRevision() == response.Header.Revision
	}, time.Second, time.Millisecond)
	durable, err := b.GetDurableRevision(ctx)
	require.NoError(t, err)
	require.Equal(t, response.Header.Revision, durable)
}

func TestTransactionalRevisionAllocatorCommitsContinuousMarkers(t *testing.T) {
	b, ctx := newTxnApplyBackend(t)
	b.persistDurableRevision(1)

	const writers = 32
	revisions := make(chan uint64, writers)
	errCh := make(chan error, writers)
	var wg sync.WaitGroup
	for i := 0; i < writers; i++ {
		wg.Add(1)
		go func(writer int) {
			defer wg.Done()
			for {
				if err := ctx.Err(); err != nil {
					errCh <- err
					return
				}
				batch := b.kv.BeginBatchWrite()
				allocated := b.stageNextDurableRevision(batch, func(_ context.Context, txn storage.AtomicBatch, revision uint64) error {
					marker := b.ks.EncodeInternalKey([]byte(fmt.Sprintf("revision/test/%020d", revision)))
					return txn.Put(marker, []byte(fmt.Sprintf("writer-%d", writer)), 0)
				})
				err := batch.Commit(ctx)
				if errors.Is(err, storage.ErrCASFailed) {
					continue
				}
				if err != nil {
					errCh <- err
					return
				}
				revisions <- *allocated
				return
			}
		}(i)
	}
	wg.Wait()
	close(revisions)
	close(errCh)
	for err := range errCh {
		require.NoError(t, err)
	}
	got := make([]uint64, 0, writers)
	for revision := range revisions {
		got = append(got, revision)
	}
	sort.Slice(got, func(i, j int) bool { return got[i] < got[j] })
	require.Len(t, got, writers)
	for i, revision := range got {
		require.Equal(t, uint64(i+2), revision)
		marker := b.ks.EncodeInternalKey([]byte(fmt.Sprintf("revision/test/%020d", revision)))
		_, err := b.kv.Get(ctx, marker)
		require.NoError(t, err)
	}
	durable, err := b.GetDurableRevision(ctx)
	require.NoError(t, err)
	require.Equal(t, uint64(writers+1), durable)
}

func TestTransactionalRevisionAllocatorCallbackFailureDoesNotAdvance(t *testing.T) {
	b, ctx := newTxnApplyBackend(t)
	b.persistDurableRevision(9)
	wantErr := errors.New("marker encoding failed")
	batch := b.kv.BeginBatchWrite()
	allocated := b.stageNextDurableRevision(batch, func(_ context.Context, txn storage.AtomicBatch, revision uint64) error {
		require.Equal(t, uint64(10), revision)
		require.NoError(t, txn.Put(b.ks.EncodeInternalKey([]byte("revision/test/rollback")), []byte("staged"), 0))
		return wantErr
	})
	require.ErrorIs(t, batch.Commit(ctx), wantErr)
	require.Equal(t, uint64(10), *allocated)
	durable, err := b.GetDurableRevision(ctx)
	require.NoError(t, err)
	require.Equal(t, uint64(9), durable)
	_, err = b.kv.Get(ctx, b.ks.EncodeInternalKey([]byte("revision/test/rollback")))
	require.ErrorIs(t, err, storage.ErrKeyNotFound)
}

func TestTransactionalRevisionAllocatorInitializesAndFailsClosed(t *testing.T) {
	t.Run("missing counter starts after empty etcd revision", func(t *testing.T) {
		b, ctx := newTxnApplyBackend(t)
		batch := b.kv.BeginBatchWrite()
		allocated := b.stageNextDurableRevision(batch, func(_ context.Context, txn storage.AtomicBatch, revision uint64) error {
			return txn.Put(b.ks.EncodeInternalKey([]byte("revision/test/initial")), []byte("ok"), 0)
		})
		require.NoError(t, batch.Commit(ctx))
		require.Equal(t, uint64(2), *allocated)
		durable, err := b.GetDurableRevision(ctx)
		require.NoError(t, err)
		require.Equal(t, uint64(2), durable)
	})

	for _, tc := range []struct {
		name string
		raw  []byte
		err  error
	}{
		{name: "corrupt", raw: []byte{1}, err: ErrInvalidMVCCMetadata},
		{name: "exhausted", raw: func() []byte {
			value := make([]byte, 8)
			binary.BigEndian.PutUint64(value, math.MaxInt64)
			return value
		}(), err: ErrRevisionExhausted},
	} {
		t.Run(tc.name, func(t *testing.T) {
			b, ctx := newTxnApplyBackend(t)
			key := b.ks.EncodeInternalKey(durableRevisionKey)
			seed := b.kv.BeginBatchWrite()
			seed.Put(key, tc.raw, 0)
			require.NoError(t, seed.Commit(ctx))
			batch := b.kv.BeginBatchWrite()
			b.stageNextDurableRevision(batch, func(_ context.Context, txn storage.AtomicBatch, revision uint64) error {
				return txn.Put(b.ks.EncodeInternalKey([]byte("revision/test/invalid")), []byte("must-not-commit"), 0)
			})
			require.ErrorIs(t, batch.Commit(ctx), tc.err)
			current, err := b.kv.Get(ctx, key)
			require.NoError(t, err)
			require.Equal(t, tc.raw, current)
			_, err = b.kv.Get(ctx, b.ks.EncodeInternalKey([]byte("revision/test/invalid")))
			require.ErrorIs(t, err, storage.ErrKeyNotFound)
		})
	}
}

func TestTransactionalRevisionAllocatorReusesMVCCAndEventEncoders(t *testing.T) {
	b, ctx := newTxnApplyBackend(t)
	b.persistDurableRevision(1)
	userKey := []byte(prefix + "/transactional-encoder/key")
	userValue := []byte("value")
	meta := EtcdMetadata{CreateRevision: 2, Version: 1, Lease: 17}

	batch := b.kv.BeginBatchWrite()
	allocated := b.stageNextDurableRevision(batch, func(_ context.Context, txn storage.AtomicBatch, revision uint64) error {
		require.Equal(t, uint64(2), revision)
		objectKey := b.coder.EncodeObjectKey(userKey, revision)
		for _, mutation := range b.encodeTxnObjectMutations(objectKey, userKey, userValue, meta, revision) {
			if err := txn.Put(mutation.key, mutation.value, 0); err != nil {
				return err
			}
		}
		eventKey, eventValue := encodeEventLogEntry(b.ks, revision, userKey, proto.Event_CREATE, 0, 0, 1)
		return txn.Put(eventKey, eventValue, 0)
	})
	require.NoError(t, batch.Commit(ctx))
	require.Equal(t, uint64(2), *allocated)

	object, err := b.kv.Get(ctx, b.coder.EncodeObjectKey(userKey, 2))
	require.NoError(t, err)
	decodedMeta, decodedValue, ok := decodeValueWithMeta(object)
	require.True(t, ok)
	require.Equal(t, userValue, decodedValue)
	require.Equal(t, meta, decodedMeta)
	eventKey, wantEvent := encodeEventLogEntry(b.ks, 2, userKey, proto.Event_CREATE, 0, 0, 1)
	gotEvent, err := b.kv.Get(ctx, eventKey)
	require.NoError(t, err)
	require.Equal(t, wantEvent, gotEvent)
}

func TestTransactionalRevisionAllocatorReusesDeleteEncoder(t *testing.T) {
	b, ctx := newTxnApplyBackend(t)
	b.persistDurableRevision(7)
	userKey := []byte(prefix + "/transactional-delete/key")
	encoded := b.encodeDeleteMutation(userKey, 7, 8)

	// Seed the revision index that the delete must compare and replace.
	seed := b.kv.BeginBatchWrite()
	seed.Put(encoded.revisionKey, encoded.expectedRevisionValue, 0)
	require.NoError(t, seed.Commit(ctx))

	batch := b.kv.BeginBatchWrite()
	allocated := b.stageNextDurableRevision(batch, func(_ context.Context, txn storage.AtomicBatch, revision uint64) error {
		require.Equal(t, uint64(8), revision)
		mutation := b.encodeDeleteMutation(userKey, 7, revision)
		current, err := txn.Get(ctx, mutation.revisionKey)
		if err != nil {
			return err
		}
		if !bytes.Equal(current, mutation.expectedRevisionValue) {
			return storage.ErrCASFailed
		}
		if err := txn.Put(mutation.revisionKey, mutation.newRevisionValue, 0); err != nil {
			return err
		}
		if err := txn.Put(mutation.objectKey, mutation.objectValue, 0); err != nil {
			return err
		}
		return txn.Put(mutation.eventKey, mutation.eventValue, 0)
	})
	require.NoError(t, batch.Commit(ctx))
	require.Equal(t, uint64(8), *allocated)

	gotIndex, err := b.kv.Get(ctx, encoded.revisionKey)
	require.NoError(t, err)
	require.Equal(t, encoded.newRevisionValue, gotIndex)
	gotObject, err := b.kv.Get(ctx, encoded.objectKey)
	require.NoError(t, err)
	require.Equal(t, tombStoneBytes, gotObject)
	gotEvent, err := b.kv.Get(ctx, encoded.eventKey)
	require.NoError(t, err)
	require.Equal(t, encoded.eventValue, gotEvent)
}
