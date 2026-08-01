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
	"sync/atomic"
	"time"

	"k8s.io/klog/v2"

	"github.com/kubewharf/kubebrain/pkg/storage"
)

var durableRevisionKey = []byte("revision/committed")

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
				b.persistDurableRevision(target)
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
	if len(value) != 8 {
		return 0, fmt.Errorf("invalid durable revision watermark length %d", len(value))
	}
	return binary.BigEndian.Uint64(value), nil
}

// InitializeLeadershipRevision keeps PD's uniqueness floor private until the
// first mutation. The client-visible revision is restored from the exact TiKV
// watermark, while the collector waits at allocationFloor for Deal()+1.
func (b *backend) InitializeLeadershipRevision(ctx context.Context, allocationFloor uint64) error {
	durable, err := b.GetDurableRevision(ctx)
	if errors.Is(err, storage.ErrKeyNotFound) {
		durable = 1 // etcd's initialized empty-keyspace revision
	} else if err != nil {
		return err
	}
	b.tso.Commit(durable)
	b.tso.AdvanceDealFloor(allocationFloor)
	b.collectorRevision.Store(allocationFloor)
	b.commitNotify.advance()
	return nil
}

func (b *backend) stageDurableRevision(batch storage.BatchWrite, revision uint64) {
	value := make([]byte, 8)
	binary.BigEndian.PutUint64(value, revision)
	batch.Put(b.ks.EncodeInternalKey(durableRevisionKey), value, 0)
}

// persistDurableRevision monotonically advances the cluster-visible watermark.
// The background worker receives targets only after every dealt revision through
// them has a resolved collector slot. Successful writes are already durable then;
// failed or abandoned slots carry no user state. A failed marker update is safe:
// later progress retries with a higher target, while readers keep using the old
// snapshot.
func (b *backend) persistDurableRevision(target uint64) {
	if target == 0 {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), unaryRpcTimeout)
	defer cancel()
	if err := b.persistDurableRevisionContext(ctx, target); err != nil {
		b.logDurableRevisionFailure(target, err)
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
		case len(current) != 8:
			return fmt.Errorf("invalid durable revision watermark length %d", len(current))
		case binary.BigEndian.Uint64(current) >= target:
			return nil
		default:
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
