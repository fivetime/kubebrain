// Copyright 2022 ByteDance and/or its affiliates
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

package tikv

import (
	"bytes"
	"context"
	"fmt"
	"strings"
	"sync/atomic"
	"time"

	"github.com/pkg/errors"
	tikverr "github.com/tikv/client-go/v2/error"
	"github.com/tikv/client-go/v2/txnkv"
	"github.com/tikv/client-go/v2/util"

	"github.com/kubewharf/kubebrain/pkg/storage"
)

type batch struct {
	txn   *txnkv.KVTxn
	begin func(context.Context) (*txnkv.KVTxn, error)
	list  []func(ctx context.Context) error
}

// KubeBrain accepts at most 2MiB raw physical keys. The managed TiKV manifests
// set storage.max-key-size to 2.5MiB because TiKV checks the memcomparable
// encoding, which expands arbitrary bytes by about 12.5%. KubeBrain's patched
// client-go uses uint32 transaction-memdb key lengths; keep the adapter fence
// on the raw-key contract so drift still fails before commit.
const maxTiKVPhysicalKeyBytes = 2 << 20

func validateTiKVPhysicalKey(key []byte) error {
	if len(key) <= maxTiKVPhysicalKeyBytes {
		return nil
	}
	return fmt.Errorf("%w: physical key is %d bytes; managed TiKV limit is %d",
		storage.ErrKeyTooLarge, len(key), maxTiKVPhysicalKeyBytes)
}

func (b *batch) PutIfNotExist(key []byte, val []byte, ttl int64) {
	idx := len(b.list)
	b.list = append(b.list, func(ctx context.Context) error {
		if err := validateTiKVPhysicalKey(key); err != nil {
			return err
		}
		oldVal, err := b.txn.Get(ctx, key)
		if err != nil && tikverr.IsErrNotFound(err) {
			err = b.txn.Set(key, val)
			if err != nil {
				return errors.Wrapf(err, "fail to create key %s", string(key))
			}
			return nil
		} else if err == nil {
			return storage.NewErrConflict(idx, key, oldVal)
		}
		return errors.Wrapf(err, "fail to get key %s", string(key))
	})
}

func (b *batch) CAS(key []byte, newVal []byte, oldVal []byte, ttl int64) {
	idx := len(b.list)
	b.list = append(b.list, func(ctx context.Context) error {
		if err := validateTiKVPhysicalKey(key); err != nil {
			return err
		}
		val, err := b.txn.Get(ctx, key)
		if err != nil {
			if tikverr.IsErrNotFound(err) {
				// A missing key means the CAS precondition (current == oldVal)
				// cannot hold, i.e. the compare failed. Per the storage interface
				// (and to match Badger/memkv) return a Conflict (which Is
				// ErrCASFailed) with a nil current value, NOT ErrKeyNotFound —
				// otherwise the backend treats the same situation as a hard error on
				// TiKV but as a retryable CAS failure on the other engines (#45).
				return storage.NewErrConflict(idx, key, nil)
			}
			return errors.Wrapf(err, "fail to get key %s", string(key))
		}
		if !bytes.Equal(oldVal, val) {
			return storage.NewErrConflict(idx, key, val)
		}
		err = b.txn.Set(key, newVal)
		if err != nil {
			return errors.Wrapf(err, "fail to set key %s", string(key))
		}
		return nil
	})
}

func (b *batch) Put(key []byte, val []byte, ttl int64) {
	b.list = append(b.list, func(ctx context.Context) error {
		if err := validateTiKVPhysicalKey(key); err != nil {
			return err
		}
		err := b.txn.Set(key, val)
		if err != nil {
			return errors.Wrapf(err, "fail to set key %s", string(key))
		}
		return nil
	})
}

func (b *batch) Del(key []byte) {
	b.list = append(b.list, func(ctx context.Context) error {
		if err := validateTiKVPhysicalKey(key); err != nil {
			return err
		}
		err := b.txn.Delete(key)
		if err != nil {
			return errors.Wrapf(err, "fail to set key %s", string(key))
		}
		return nil
	})
}

func (b *batch) DelCurrent(it storage.Iter) {
	idx := len(b.list)
	b.list = append(b.list, func(ctx context.Context) error {
		tiIter := it.(*iter)
		key := tiIter.iter.Key()
		oldVal, err := b.txn.Get(ctx, key)
		if err != nil {
			if tikverr.IsErrNotFound(err) {
				return storage.NewErrConflict(idx, it.Key(), nil)
			}
			return errors.Wrapf(err, "fail to get key %s", string(key))
		}
		if !bytes.Equal(oldVal, tiIter.iter.Value()) {
			return storage.NewErrConflict(idx, it.Key(), oldVal)
		}
		return b.txn.Delete(tiIter.iter.Key())
	})
}

type atomicBatch struct{ txn *txnkv.KVTxn }

func (a atomicBatch) Prefetch(ctx context.Context, keys [][]byte) error {
	// KVTxn.BatchGet reads its own write buffer and snapshot. The snapshot caches
	// both present and absent keys; subsequent Get still checks the write buffer
	// first, so later staged Put/Del cannot be hidden by these cached reads.
	_, err := a.txn.BatchGet(ctx, keys)
	return err
}

func (a atomicBatch) Get(ctx context.Context, key []byte) ([]byte, error) {
	value, err := a.txn.Get(ctx, key)
	if tikverr.IsErrNotFound(err) {
		return nil, storage.ErrKeyNotFound
	}
	return value, err
}

func (a atomicBatch) Put(key []byte, val []byte, _ int64) error {
	if err := validateTiKVPhysicalKey(key); err != nil {
		return err
	}
	return a.txn.Set(key, val)
}

func (a atomicBatch) Del(key []byte) error {
	if err := validateTiKVPhysicalKey(key); err != nil {
		return err
	}
	return a.txn.Delete(key)
}

func (b *batch) Atomic(fn func(context.Context, storage.AtomicBatch) error) {
	b.list = append(b.list, func(ctx context.Context) error {
		return fn(ctx, atomicBatch{txn: b.txn})
	})
}

func (b *batch) Commit(ctx context.Context) (err error) {
	observer := storage.BatchCommitObserverFromContext(ctx)
	var observation storage.BatchCommitObservation
	var primaryTracker *primaryWriteTracker
	var detail *util.CommitDetails
	var prepareLocks, commitLocks *lockRPCTracker
	phaseParent := ctx
	if observer != nil {
		prepareLocks, commitLocks = &lockRPCTracker{}, &lockRPCTracker{}
		observation.HasLockRPCDetails = true
		defer func() {
			observation.PrepareLocks = prepareLocks.finish()
			observation.CommitLocks = commitLocks.finish()
			observation.Err = err
			// These scalar durations are finalized by the synchronous Commit
			// path. Do not read background-updated request/backoff details.
			if detail != nil {
				observation.HasWriteDetails = true
				observation.Prewrite = detail.PrewriteTime
				observation.CommitTS = detail.GetCommitTsTime
				observation.PrimaryCommit = detail.CommitTime
				observation.PrewriteRegionGroups = atomic.LoadInt32(&detail.PrewriteRegionNum)
			}
			observer(observation)
		}()
	}
	defer func() {
		// A context-bound Begin can fail before publishing a transaction. Guard the
		// rollback so the PD/TSO error is returned instead of dereferencing nil.
		if err != nil && b.txn != nil {
			b.txn.Rollback()
		}
	}()
	if b.txn == nil && b.begin != nil {
		start := time.Now()
		b.txn, err = b.begin(ctx)
		if observer != nil {
			observation.Begin = time.Since(start)
		}
		if err != nil {
			return err
		}
	}
	if observer != nil {
		ctx = context.WithValue(ctx, lockRPCTrackerKey{}, prepareLocks)
	}
	prepareStart := time.Now()
	for _, f := range b.list {
		err = f(ctx)
		if err != nil {
			if observer != nil {
				prepareLocks.finish()
				observation.Prepare = time.Since(prepareStart)
			}
			return err
		}
	}
	if observer != nil {
		prepareLocks.finish()
		ctx = context.WithValue(phaseParent, lockRPCTrackerKey{}, commitLocks)
		observation.Prepare = time.Since(prepareStart)
		observation.CommitAttempted = true
		primaryTracker = &primaryWriteTracker{}
		ctx = context.WithValue(ctx, primaryWriteTrackerKey{}, primaryTracker)
		// Preserve any caller-owned SDK diagnostic sink. Each observed batch
		// otherwise gets its own sink, never a pointer shared across retries.
		if ctx.Value(util.CommitDetailCtxKey) == nil {
			ctx = context.WithValue(ctx, util.CommitDetailCtxKey, &detail)
		}
	}
	commitStart := time.Now()
	err = b.txn.Commit(ctx)
	if observer != nil {
		commitLocks.finish()
		observation.Commit = time.Since(commitStart)
		observation.PrimaryWrite = primaryTracker.finish()
	}

	if err != nil {
		if tikverr.IsErrWriteConflict(err) {
			return storage.ErrCASFailed
		}

		if isUncertainCommitError(err) {
			err = storage.NewErrUncertainResult(err)
		}
	}
	return
}

func isUncertainCommitError(err error) bool {
	for _, uncertainErr := range uncertainErrList {
		if errors.Is(err, uncertainErr) {
			return true
		}
	}
	// client-go v2.0.7 does not expose a typed wrapper for TiKV's
	// kvrpcpb.TxnLockNotFound. During 2PC commit it means the primary lock was
	// lost while the outcome may already have been applied, so it must follow
	// the same durable-resolution path as ErrResultUndetermined.
	return strings.Contains(err.Error(), "TxnLockNotFound")
}

// todo: add other errors
var uncertainErrList = []error{
	context.DeadlineExceeded,
	context.Canceled,
	tikverr.ErrBodyMissing,
	tikverr.ErrTiKVServerTimeout,
	tikverr.ErrUnknown,
	// ErrResultUndetermined is returned when the 2PC primary-key commit RPC
	// times out and the outcome is genuinely unknown; the storage contract
	// requires this be surfaced as an uncertain result so the backend's async
	// retry queue re-resolves it, rather than treating it as a definite failure
	// (which risks reporting Succeeded=false for data that was actually written).
	tikverr.ErrResultUndetermined,
	// TiKV's region layer documents StaleCommand as a request sent to a stale
	// leader whose term changed: once the request has entered commit processing,
	// the client cannot prove whether the write became durable.
	tikverr.ErrTiKVStaleCommand,
}
