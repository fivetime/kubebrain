// Copyright 2026 ByteDance and/or its affiliates
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

package backend

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"sync/atomic"
	"time"

	"k8s.io/klog/v2"

	"github.com/kubewharf/kubebrain/pkg/backend/coder"
	"github.com/kubewharf/kubebrain/pkg/metrics"
	"github.com/kubewharf/kubebrain/pkg/storage"
)

var durableRevisionKey = []byte("revision/committed")

func initDurableRevisionMetrics(metricCli metrics.Metrics) {
	if metricCli == nil {
		return
	}
	_ = metricCli.EmitCounter("revision.durable.persist_err", int64(0))
}

func (b *backend) queueDurableRevision(target uint64) {
	for {
		current := atomic.LoadUint64(&b.durableRevisionTarget)
		if target <= current || atomic.CompareAndSwapUint64(&b.durableRevisionTarget, current, target) {
			break
		}
	}
	select {
	case b.durableRevisionSignal <- struct{}{}:
	default:
	}
}

func (b *backend) runDurableRevisionPersister(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-b.durableRevisionSignal:
			for {
				target := atomic.LoadUint64(&b.durableRevisionTarget)
				b.persistDurableRevisionWithContext(ctx, target)
				if atomic.LoadUint64(&b.durableRevisionTarget) == target {
					break
				}
			}
		}
	}
}

// GetDurableRevision returns the persisted follower snapshot watermark. The
// marker is intentionally allowed to lag: stale is safe for a serializable read;
// leading committed user state is not.
func (b *backend) GetDurableRevision(ctx context.Context) (uint64, error) {
	value, err := b.kv.Get(ctx, b.ks.EncodeInternalKey(durableRevisionKey))
	if err != nil {
		return 0, err
	}
	return decodeDurableRevisionWatermark(value)
}

// ReadDurableRevision reads the authoritative persisted user-revision
// watermark without constructing a serving backend. Restore tooling uses this
// only after a storage-atomic writer fence has been acquired.
func ReadDurableRevision(ctx context.Context, store storage.KvStorage, keyspace string) (uint64, error) {
	ks, err := coder.NewKeyspace(keyspace)
	if err != nil {
		return 0, err
	}
	value, err := store.Get(ctx, ks.EncodeInternalKey(durableRevisionKey))
	if errors.Is(err, storage.ErrKeyNotFound) {
		return 1, nil
	}
	if err != nil {
		return 0, err
	}
	return decodeDurableRevisionWatermark(value)
}

func decodeDurableRevisionWatermark(value []byte) (uint64, error) {
	revision, err := coder.ParseRevisionWatermark(value)
	if err != nil {
		return 0, invalidMVCCMetadataError(err, "decode durable revision watermark")
	}
	if revision == 0 {
		return 0, invalidMVCCMetadataError(fmt.Errorf("revision is zero"), "decode durable revision watermark")
	}
	return revision, nil
}

// InitializeLeadershipRevision restores the client-visible MVCC sequence from
// TiKV. The PD timestamp belongs to leader-election fencing; using it as the
// user allocator floor makes the first write after every leader change jump by
// an arbitrary amount, unlike etcd's contiguous revisions.
func (b *backend) InitializeLeadershipRevision(ctx context.Context, _ uint64) error {
	durable, err := b.GetDurableRevision(ctx)
	if errors.Is(err, storage.ErrKeyNotFound) {
		durable = 1 // etcd's initialized empty-keyspace revision
	} else if err != nil {
		return err
	}
	// Consume the startup optimization exactly once. A failed promotion or any
	// later re-election deliberately returns to the complete integrity scan.
	prevalidated := b.txnWitnessPrevalidatedRevision.Swap(0)
	if prevalidated > durable {
		return fmt.Errorf("%w: prevalidated transaction witness revision %d is above durable revision %d",
			ErrInvalidMVCCMetadata, prevalidated, durable)
	}
	// A commit-result resolver is process-local, but every new user transaction
	// seals its exact event-marker set in the same TiKV transaction. Validate
	// those durable seals before this leadership term becomes writable so a
	// crash cannot erase evidence of a partial/corrupt transaction outcome.
	validationStarted := time.Now()
	validationErr := b.validatePersistedTxnWitnessesAfter(ctx, false, true, prevalidated)
	validationDuration := time.Since(validationStarted)
	validationMode := "full"
	if prevalidated > 0 {
		validationMode = "incremental"
	}
	if b.metricCli != nil {
		_ = b.metricCli.EmitHistogram("txn.witness.leadership_validation.duration_seconds",
			validationDuration.Seconds(), metrics.Tag("mode", validationMode))
	}
	if validationErr == nil {
		klog.InfoS("leadership transaction witnesses validated", "mode", validationMode,
			"afterRevision", prevalidated, "durableRevision", durable, "duration", validationDuration)
	}
	if validationErr != nil && !errors.Is(validationErr, ErrTxnWitnessCorrupt) {
		return validationErr
	}
	// Alarm metadata is part of the write-safety boundary, not merely an RPC
	// presentation detail. Validate it before publishing this leadership term;
	// a valid active CORRUPT alarm still permits a read-only leader, while an
	// undecodable member set/generation cannot safely admit or disarm writes.
	if err := b.ValidateCorruptAlarmMetadata(ctx); err != nil {
		return err
	}
	// A same-process re-election can retain a committed watermark newer than a
	// lagging background marker. Never move that process backwards; a cold
	// process has current=0 and therefore starts from the durable TiKV value.
	base := durable
	if current := b.tso.GetRevision(); current > base {
		base = current
	}
	b.tso.Init(base)
	b.collectorRevision.Store(base)
	b.commitNotify.advance()
	return nil
}

// PrevalidateLeadershipRevision performs the expensive immutable
// transaction-witness/event merge before this process joins leader election.
// The durable revision is sampled first, so concurrent writes can only make the
// cached floor conservative. Promotion always scans the gap above that floor.
// Corruption is reported but not armed here: only the elected, fenced path may
// mutate the cluster-wide CORRUPT alarm.
func (b *backend) PrevalidateLeadershipRevision(ctx context.Context) error {
	durable, err := b.GetDurableRevision(ctx)
	switch {
	case errors.Is(err, storage.ErrKeyNotFound):
		durable = 0
	case err != nil:
		return err
	}
	if err := b.validatePersistedTxnWitnessesAfter(ctx, false, false, 0); err != nil {
		return err
	}
	b.advanceTxnWitnessPrevalidatedRevision(durable)
	return nil
}

func (b *backend) advanceTxnWitnessPrevalidatedRevision(revision uint64) {
	for {
		current := b.txnWitnessPrevalidatedRevision.Load()
		if revision <= current || b.txnWitnessPrevalidatedRevision.CompareAndSwap(current, revision) {
			return
		}
	}
}

// stageNextDurableRevision makes the durable watermark the transaction-local
// revision allocator. The caller's stage function must write the user mutation
// and its event records through the supplied AtomicBatch; otherwise advancing
// the counter alone would create the same externally visible hole this helper
// is intended to eliminate. Callers must retry the entire freshly constructed
// batch on ErrCASFailed.
func (b *backend) stageNextDurableRevision(batch storage.BatchWrite, stage func(context.Context, storage.AtomicBatch, uint64) error) *uint64 {
	return b.stageNextDurableRevisionAfter(batch, 0, stage)
}

// stageNextDurableRevisionAfter atomically folds an observed committed floor
// into the durable counter before allocating. The floor protects recovery and
// compatibility states that may be newer than a stale durable counter.
func (b *backend) stageNextDurableRevisionAfter(batch storage.BatchWrite, floor uint64, stage func(context.Context, storage.AtomicBatch, uint64) error, readKeys ...[]byte) *uint64 {
	var allocated uint64
	key := b.ks.EncodeInternalKey(durableRevisionKey)
	batch.Atomic(func(ctx context.Context, txn storage.AtomicBatch) error {
		// Fetch the allocator and known guard reads at this transaction's own
		// snapshot. Get and all later comparisons still execute; this must not
		// turn a pre-read outside the transaction into a trusted allocator value.
		if len(readKeys) != 0 {
			if prefetcher, ok := txn.(storage.AtomicBatchPrefetcher); ok {
				keys := make([][]byte, 1, 1+len(readKeys))
				keys[0] = key
				keys = append(keys, readKeys...)
				if err := prefetcher.Prefetch(ctx, keys); err != nil {
					return err
				}
			}
		}
		current, err := txn.Get(ctx, key)
		var revision uint64
		switch {
		case errors.Is(err, storage.ErrKeyNotFound):
			revision = 1
		case err != nil:
			return err
		default:
			revision, err = decodeDurableRevisionWatermark(current)
			if err != nil {
				return err
			}
		}
		if revision < floor {
			revision = floor
		}
		// MaxInt64 is reserved for the persistent lease-format compatibility
		// witness. Never let the final user revision overwrite that leadership
		// fence; the public sequence fails closed one value earlier.
		if revision >= math.MaxInt64-1 {
			return ErrRevisionExhausted
		}
		allocated = revision + 1
		value := make([]byte, 8)
		binary.BigEndian.PutUint64(value, allocated)
		if err := txn.Put(key, value, 0); err != nil {
			return err
		}
		return stage(ctx, txn, allocated)
	})
	return &allocated
}

// persistDurableRevision monotonically advances the cluster-visible watermark.
// The background worker receives targets only after every dealt revision through
// them has a resolved collector slot. Successful writes are already durable then;
// failed or abandoned slots carry no user state. A failed marker update is safe:
// later progress retries with a higher target, while readers keep using the old
// snapshot.
func (b *backend) persistDurableRevision(target uint64) {
	b.persistDurableRevisionWithContext(context.Background(), target)
}

func (b *backend) persistDurableRevisionWithContext(parent context.Context, target uint64) {
	if target == 0 {
		return
	}
	ctx, cancel := context.WithTimeout(parent, unaryRpcTimeout)
	defer cancel()
	if err := b.persistDurableRevisionContext(ctx, target); err != nil {
		if parent.Err() == nil {
			b.logDurableRevisionFailure(target, err)
		}
	}
}

func (b *backend) persistDurableRevisionContext(ctx context.Context, target uint64) error {
	key := b.ks.EncodeInternalKey(durableRevisionKey)
	want := make([]byte, 8)
	binary.BigEndian.PutUint64(want, target)

	for {
		current, err := b.kv.Get(ctx, key)
		switch {
		case errors.Is(err, storage.ErrKeyNotFound):
			batch := b.kv.BeginBatchWrite()
			batch.PutIfNotExist(key, want, 0)
			err = batch.Commit(ctx)
		case err != nil:
			return err
		default:
			revision, decodeErr := decodeDurableRevisionWatermark(current)
			if decodeErr != nil {
				return decodeErr
			}
			if revision >= target {
				return nil
			}
			batch := b.kv.BeginBatchWrite()
			batch.CAS(key, want, current, 0)
			err = batch.Commit(ctx)
		}
		if err == nil {
			return nil
		}
		if !errors.Is(err, storage.ErrCASFailed) {
			return err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(time.Millisecond):
		}
	}
}

func (b *backend) ensureDurableRevision(ctx context.Context, target uint64) bool {
	for {
		if err := b.persistDurableRevisionContext(ctx, target); err == nil {
			return true
		} else {
			b.logDurableRevisionFailure(target, err)
		}
		select {
		case <-ctx.Done():
			return false
		case <-time.After(100 * time.Millisecond):
		}
	}
}

func (b *backend) logDurableRevisionFailure(target uint64, err error) {
	b.metricCli.EmitCounter("revision.durable.persist_err", 1)
	klog.ErrorS(err, "failed to persist durable revision watermark", "revision", target)
}
