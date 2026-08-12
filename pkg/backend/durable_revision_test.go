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

func TestLeadershipRevisionRejectsMalformedCorruptAlarmGeneration(t *testing.T) {
	b, ctx := newTxnApplyBackend(t)
	require.NoError(t, b.InternalPut(ctx, corruptAlarmGenerationKey, []byte("bad")))

	err := b.InitializeLeadershipRevision(ctx, 0)
	require.ErrorIs(t, err, ErrInvalidAlarmMetadata)
	require.ErrorContains(t, err, "corrupt alarm generation has length 3")
}

func TestLeadershipRevisionAllowsValidActiveCorruptAlarmReadOnlyState(t *testing.T) {
	b, ctx := newTxnApplyBackend(t)
	require.NoError(t, b.ArmCorrupt(ctx, 44101))

	require.NoError(t, b.InitializeLeadershipRevision(ctx, 0))
	members, err := b.CorruptAlarms(ctx)
	require.NoError(t, err)
	require.Equal(t, []uint64{44101}, members)
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
		mutation := b.encodeCreateMutation(userKey, userValue, meta, revision, 0, 1)
		_, err := txn.Get(ctx, mutation.revisionKey)
		if !errors.Is(err, storage.ErrKeyNotFound) {
			return storage.ErrCASFailed
		}
		if err := txn.Put(mutation.revisionKey, mutation.newRevisionValue, 0); err != nil {
			return err
		}
		for _, mutation := range mutation.objectMutations {
			if err := txn.Put(mutation.key, mutation.value, 0); err != nil {
				return err
			}
		}
		return txn.Put(mutation.eventKey, mutation.eventValue, 0)
	})
	require.NoError(t, batch.Commit(ctx))
	require.Equal(t, uint64(2), *allocated)

	object, err := b.kv.Get(ctx, b.coder.EncodeObjectKey(userKey, 2))
	require.NoError(t, err)
	decodedMeta, decodedValue, ok := decodeValueWithMeta(object)
	require.True(t, ok)
	require.Equal(t, userValue, decodedValue)
	require.Equal(t, meta, decodedMeta)
	want := b.encodeCreateMutation(userKey, userValue, meta, 2, 0, 1)
	gotIndex, err := b.kv.Get(ctx, want.revisionKey)
	require.NoError(t, err)
	require.Equal(t, want.newRevisionValue, gotIndex)
	eventKey, wantEvent := want.eventKey, want.eventValue
	gotEvent, err := b.kv.Get(ctx, eventKey)
	require.NoError(t, err)
	require.Equal(t, wantEvent, gotEvent)
}

func TestTransactionalRevisionAllocatorCreateRecreateConflictIsAtomic(t *testing.T) {
	b, ctx := newTxnApplyBackend(t)
	b.persistDurableRevision(20)
	userKey := []byte(prefix + "/transactional-recreate/key")
	tombstoneIndex := append(uint64ToBytes(9), 0)
	seed := b.kv.BeginBatchWrite()
	seed.Put(b.coder.EncodeRevisionKey(userKey), tombstoneIndex, 0)
	require.NoError(t, seed.Commit(ctx))

	batch := b.kv.BeginBatchWrite()
	allocated := b.stageNextDurableRevision(batch, func(_ context.Context, txn storage.AtomicBatch, revision uint64) error {
		mutation := b.encodeCreateMutation(userKey, []byte("recreated"), EtcdMetadata{CreateRevision: revision, Version: 1}, revision, 0, 1)
		current, err := txn.Get(ctx, mutation.revisionKey)
		if err != nil {
			return err
		}
		if !bytes.Equal(current, tombstoneIndex) {
			return storage.ErrCASFailed
		}
		if err := txn.Put(mutation.revisionKey, mutation.newRevisionValue, 0); err != nil {
			return err
		}
		for _, object := range mutation.objectMutations {
			if err := txn.Put(object.key, object.value, 0); err != nil {
				return err
			}
		}
		return txn.Put(mutation.eventKey, mutation.eventValue, 0)
	})
	require.NoError(t, batch.Commit(ctx))
	require.Equal(t, uint64(21), *allocated)

	conflict := b.kv.BeginBatchWrite()
	b.stageNextDurableRevision(conflict, func(_ context.Context, txn storage.AtomicBatch, revision uint64) error {
		mutation := b.encodeCreateMutation(userKey, []byte("must-not-write"), EtcdMetadata{CreateRevision: revision, Version: 1}, revision, 0, 1)
		current, err := txn.Get(ctx, mutation.revisionKey)
		if err != nil {
			return err
		}
		if !bytes.Equal(current, tombstoneIndex) {
			return storage.ErrCASFailed
		}
		return txn.Put(mutation.revisionKey, mutation.newRevisionValue, 0)
	})
	require.ErrorIs(t, conflict.Commit(ctx), storage.ErrCASFailed)
	durable, err := b.GetDurableRevision(ctx)
	require.NoError(t, err)
	require.Equal(t, uint64(21), durable)
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

func TestTransactionalRevisionAllocatorReusesUpdateEncoder(t *testing.T) {
	b, ctx := newTxnApplyBackend(t)
	b.persistDurableRevision(11)
	userKey := []byte(prefix + "/transactional-update/key")
	value := []byte("updated")
	meta := EtcdMetadata{CreateRevision: 3, Version: 4, Lease: 29}
	encoded := b.encodePutMutation(userKey, value, meta, 11, 12, proto.Event_PUT, 0, 1)

	seed := b.kv.BeginBatchWrite()
	seed.Put(encoded.revisionKey, encoded.expectedRevisionValue, 0)
	require.NoError(t, seed.Commit(ctx))

	batch := b.kv.BeginBatchWrite()
	allocated := b.stageNextDurableRevision(batch, func(_ context.Context, txn storage.AtomicBatch, revision uint64) error {
		require.Equal(t, uint64(12), revision)
		mutation := b.encodePutMutation(userKey, value, meta, 11, revision, proto.Event_PUT, 0, 1)
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
		for _, object := range mutation.objectMutations {
			if err := txn.Put(object.key, object.value, 0); err != nil {
				return err
			}
		}
		return txn.Put(mutation.eventKey, mutation.eventValue, 0)
	})
	require.NoError(t, batch.Commit(ctx))
	require.Equal(t, uint64(12), *allocated)

	gotIndex, err := b.kv.Get(ctx, encoded.revisionKey)
	require.NoError(t, err)
	require.Equal(t, encoded.newRevisionValue, gotIndex)
	for _, object := range encoded.objectMutations {
		got, getErr := b.kv.Get(ctx, object.key)
		require.NoError(t, getErr)
		require.Equal(t, object.value, got)
	}
	gotEvent, err := b.kv.Get(ctx, encoded.eventKey)
	require.NoError(t, err)
	require.Equal(t, encoded.eventValue, gotEvent)
}

func TestTransactionalRevisionAllocatorStagesWholeTxn(t *testing.T) {
	b, ctx := newTxnApplyBackend(t)
	b.persistDurableRevision(10)
	createKey := []byte(prefix + "/atomic-txn/create")
	updateKey := []byte(prefix + "/atomic-txn/update")
	deleteKey := []byte(prefix + "/atomic-txn/delete")
	guardKey := []byte(prefix + "/atomic-txn/guard")
	internalKey := []byte("atomic-txn/internal")
	internalDeleteKey := []byte("atomic-txn/internal-delete")

	seed := b.kv.BeginBatchWrite()
	seed.Put(b.coder.EncodeRevisionKey(updateKey), uint64ToBytes(7), 0)
	seed.Put(b.coder.EncodeRevisionKey(deleteKey), uint64ToBytes(8), 0)
	seed.Put(b.ks.EncodeInternalKey(internalDeleteKey), []byte("old-internal"), 0)
	require.NoError(t, seed.Commit(ctx))
	preps := []txnPrep{
		{op: TxnWriteOp{Key: createKey, Value: []byte("created")}, effective: true, create: true},
		{op: TxnWriteOp{Key: updateKey, Value: []byte("updated"), Lease: 31}, rvBytes: uint64ToBytes(7), curRev: 7, effective: true, meta: EtcdMetadata{CreateRevision: 3, Version: 2}},
		{op: TxnWriteOp{Key: deleteKey, Delete: true}, rvBytes: uint64ToBytes(8), curRev: 8, effective: true},
		{op: TxnWriteOp{Key: internalKey, Value: []byte("internal"), Internal: true}, effective: true},
		{op: TxnWriteOp{Key: internalDeleteKey, Delete: true, Internal: true}, rvBytes: []byte("old-internal"), effective: true},
	}
	guards := []txnGuardPrep{{key: b.coder.EncodeRevisionKey(guardKey), missing: true}}

	batch := b.kv.BeginBatchWrite()
	allocated := b.stageNextDurableRevision(batch, func(callbackCtx context.Context, txn storage.AtomicBatch, revision uint64) error {
		return b.stageTxnAtomic(callbackCtx, txn, preps, guards, revision, nil, 0,
			corruptAlarmCommitGuard{key: corruptAlarmFenceControlKey})
	})
	require.NoError(t, batch.Commit(ctx))
	require.Equal(t, uint64(11), *allocated)
	require.Equal(t, uint64(11), preps[0].meta.CreateRevision)
	require.Equal(t, int64(31), preps[1].meta.Lease)

	for sub, key := range [][]byte{createKey, updateKey, deleteKey} {
		verb := proto.Event_PUT
		prev := uint64(7)
		if sub == 0 {
			verb, prev = proto.Event_CREATE, 0
		} else if sub == 2 {
			verb, prev = proto.Event_DELETE, 8
		}
		eventKey, want := encodeEventLogEntry(b.ks, 11, key, verb, prev, uint32(sub), 3)
		got, err := b.kv.Get(ctx, eventKey)
		require.NoError(t, err)
		require.Equal(t, want, got)
	}
	gotInternal, err := b.kv.Get(ctx, b.ks.EncodeInternalKey(internalKey))
	require.NoError(t, err)
	require.Equal(t, []byte("internal"), gotInternal)
	_, err = b.kv.Get(ctx, b.ks.EncodeInternalKey(internalDeleteKey))
	require.ErrorIs(t, err, storage.ErrKeyNotFound)

	// A guard that changed after preparation aborts the counter and every user
	// mutation in the same transaction.
	guardSeed := b.kv.BeginBatchWrite()
	guardSeed.Put(b.coder.EncodeRevisionKey(guardKey), uint64ToBytes(11), 0)
	require.NoError(t, guardSeed.Commit(ctx))
	blockedKey := []byte(prefix + "/atomic-txn/blocked")
	blocked := []txnPrep{{op: TxnWriteOp{Key: blockedKey, Value: []byte("blocked")}, effective: true, create: true}}
	conflict := b.kv.BeginBatchWrite()
	b.stageNextDurableRevision(conflict, func(callbackCtx context.Context, txn storage.AtomicBatch, revision uint64) error {
		return b.stageTxnAtomic(callbackCtx, txn, blocked, guards, revision, nil, 0,
			corruptAlarmCommitGuard{key: corruptAlarmFenceControlKey})
	})
	require.ErrorIs(t, conflict.Commit(ctx), storage.ErrCASFailed)
	durable, err := b.GetDurableRevision(ctx)
	require.NoError(t, err)
	require.Equal(t, uint64(11), durable)
	_, err = b.kv.Get(ctx, b.coder.EncodeRevisionKey(blockedKey))
	require.ErrorIs(t, err, storage.ErrKeyNotFound)
}

func TestTransactionalRevisionAllocatorStagesUncertainRepair(t *testing.T) {
	for _, tc := range []struct {
		name  string
		value []byte
		verb  proto.Event_EventType
	}{
		{name: "put envelope", value: encodeValueWithMeta([]byte("value"), EtcdMetadata{CreateRevision: 4, Version: 2, Lease: 7}), verb: proto.Event_PUT},
		{name: "delete tombstone", value: tombStoneBytes, verb: proto.Event_DELETE},
	} {
		t.Run(tc.name, func(t *testing.T) {
			b, ctx := newTxnApplyBackend(t)
			b.persistDurableRevision(30)
			key := []byte(prefix + "/atomic-repair/" + tc.name)
			oldIndex := uint64ToBytes(30)
			if tc.verb == proto.Event_DELETE {
				oldIndex = append(oldIndex, 0)
			}
			seed := b.kv.BeginBatchWrite()
			seed.Put(b.coder.EncodeRevisionKey(key), oldIndex, 0)
			require.NoError(t, seed.Commit(ctx))

			batch := b.kv.BeginBatchWrite()
			allocated := b.stageUncertainRepairAtomic(batch, key, tc.value, 30, 17)
			require.NoError(t, batch.Commit(ctx))
			require.Equal(t, uint64(31), *allocated)
			stored, err := b.kv.Get(ctx, b.coder.EncodeObjectKey(key, 31))
			require.NoError(t, err)
			require.Equal(t, tc.value, stored, "repair must preserve stored bytes verbatim")
			eventKey, wantEvent := encodeEventLogEntry(b.ks, 31, key, tc.verb, 17, 0, 1)
			gotEvent, err := b.kv.Get(ctx, eventKey)
			require.NoError(t, err)
			require.Equal(t, wantEvent, gotEvent)

			conflict := b.kv.BeginBatchWrite()
			b.stageUncertainRepairAtomic(conflict, key, []byte("must-not-land"), 30, 17)
			require.ErrorIs(t, conflict.Commit(ctx), storage.ErrCASFailed)
			durable, err := b.GetDurableRevision(ctx)
			require.NoError(t, err)
			require.Equal(t, uint64(31), durable)
		})
	}
}

func TestTransactionalRevisionAllocatorBridgesLegacyTSOWriters(t *testing.T) {
	b, ctx := newTxnApplyBackend(t)
	base := b.GetCurrentRevision()

	_, transactionalRevision, err := b.TxnApply(ctx, []TxnWriteOp{{
		Key: []byte(prefix + "/allocator-bridge/txn-1"), Value: []byte("txn-1"),
	}}, nil)
	require.NoError(t, err)
	require.Equal(t, base+1, transactionalRevision)

	legacy, err := b.Create(ctx, &proto.CreateRequest{
		Key: []byte(prefix + "/allocator-bridge/legacy"), Value: []byte("legacy"),
	})
	require.NoError(t, err)
	require.Equal(t, transactionalRevision+1, legacy.Header.Revision)

	_, nextTransactionalRevision, err := b.TxnApply(ctx, []TxnWriteOp{{
		Key: []byte(prefix + "/allocator-bridge/txn-2"), Value: []byte("txn-2"),
	}}, nil)
	require.NoError(t, err)
	require.Equal(t, legacy.Header.Revision+1, nextTransactionalRevision)
	durable, err := b.GetDurableRevision(ctx)
	require.NoError(t, err)
	require.Equal(t, nextTransactionalRevision, durable)
}

func TestPublicSingleKeyWritesAllocateOnlyEffectiveRevisions(t *testing.T) {
	b, ctx := newTxnApplyBackend(t)
	base := b.GetCurrentRevision()
	key := []byte(prefix + "/transactional-single-key/key")

	created, err := b.Create(ctx, &proto.CreateRequest{Key: key, Value: []byte("v1")})
	require.NoError(t, err)
	require.True(t, created.Succeeded)
	require.Equal(t, base+1, created.Header.Revision)

	duplicate, err := b.Create(ctx, &proto.CreateRequest{Key: key, Value: []byte("duplicate")})
	require.NoError(t, err)
	require.False(t, duplicate.Succeeded)
	require.Equal(t, created.Header.Revision, duplicate.Header.Revision)

	stale, err := b.Update(ctx, &proto.UpdateRequest{Kv: &proto.KeyValue{
		Key: key, Value: []byte("stale"), Revision: base,
	}})
	require.NoError(t, err)
	require.False(t, stale.Succeeded)
	require.Equal(t, created.Header.Revision, stale.Header.Revision)

	updated, err := b.Update(ctx, &proto.UpdateRequest{Kv: &proto.KeyValue{
		Key: key, Value: []byte("v2"), Revision: created.Header.Revision,
	}})
	require.NoError(t, err)
	require.True(t, updated.Succeeded)
	require.Equal(t, base+2, updated.Header.Revision)

	missing, err := b.Delete(ctx, &proto.DeleteRequest{Key: []byte(prefix + "/transactional-single-key/missing")})
	require.NoError(t, err)
	require.False(t, missing.Succeeded)
	require.Equal(t, updated.Header.Revision, missing.Header.Revision)

	deleted, err := b.Delete(ctx, &proto.DeleteRequest{Key: key, Revision: updated.Header.Revision})
	require.NoError(t, err)
	require.True(t, deleted.Succeeded)
	require.Equal(t, base+3, deleted.Header.Revision)
	durable, err := b.GetDurableRevision(ctx)
	require.NoError(t, err)
	require.Equal(t, deleted.Header.Revision, durable)
}
