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

package badger

import (
	"bytes"
	"context"
	"time"

	"github.com/dgraph-io/badger"
	"github.com/pkg/errors"

	"github.com/kubewharf/kubebrain/pkg/storage"
)

type batch struct {
	txn  *badger.Txn
	list []func(context.Context) error
}

func (b *batch) PutIfNotExist(key []byte, val []byte, ttl int64) {
	idx := len(b.list)
	f := func(_ context.Context) error {
		oldItem, err := b.txn.Get(key)
		if err == nil {
			// value exist
			oldVal, copyErr := oldItem.ValueCopy(nil)
			if copyErr != nil {
				// maybe file is broken. Wrap copyErr, not err: err is nil in this
				// branch (the key exists), so errors.Wrapf(err, ...) returned nil,
				// turning a failed read into a silent success (#65).
				return errors.Wrapf(copyErr, "fail to copy value for key %s", key)
			}
			return storage.NewErrConflict(idx, key, oldVal)
		} else if errors.Is(err, badger.ErrKeyNotFound) {
			// value not exist
			if ttl != 0 {
				entry := badger.NewEntry(key, val).WithTTL(time.Duration(ttl) * time.Second)
				return b.txn.SetEntry(entry)
			}
			return b.txn.Set(key, val)
		}

		// unknown error
		return errors.Wrapf(err, "failed to get key %s", key)

	}
	b.list = append(b.list, f)
}

func (b *batch) CAS(key []byte, newVal []byte, oldVal []byte, ttl int64) {
	idx := len(b.list)
	f := func(_ context.Context) error {
		item, err := b.txn.Get(key)
		if err != nil {
			if err == badger.ErrKeyNotFound {
				return storage.NewErrConflict(idx, key, nil)
			}
			return err
		}

		err = item.Value(func(val []byte) error {
			if !bytes.Equal(oldVal, val) {
				return storage.NewErrConflict(idx, key, val)
			}
			return nil
		})
		if err != nil {
			return err
		}
		if ttl != 0 {
			entry := badger.NewEntry(key, newVal).WithTTL(time.Duration(ttl) * time.Second)
			return b.txn.SetEntry(entry)
		}
		return b.txn.Set(key, newVal)
	}
	b.list = append(b.list, f)
}

func (b *batch) Put(key []byte, val []byte, ttl int64) {
	f := func(_ context.Context) error {
		if ttl != 0 {
			entry := badger.NewEntry(key, val).WithTTL(time.Duration(ttl) * time.Second)
			return b.txn.SetEntry(entry)
		}
		return b.txn.Set(key, val)
	}
	b.list = append(b.list, f)
}

func (b *batch) Del(key []byte) {
	f := func(_ context.Context) error {
		return b.txn.Delete(key)
	}
	b.list = append(b.list, f)
}

func (b *batch) DelCurrent(it storage.Iter) {
	idx := len(b.list)
	f := func(_ context.Context) error {
		i := it.(*iter)
		item, err := b.txn.Get(i.Key())
		if err != nil {
			if errors.Is(err, badger.ErrKeyNotFound) {
				err = storage.NewErrConflict(idx, it.Key(), nil)
			}
			return err
		}
		if item.Version() != i.iIter.Item().Version() {
			val, _ := item.ValueCopy(nil)
			return storage.NewErrConflict(idx, it.Key(), val)
		}
		return b.txn.Delete(i.Key())
	}
	b.list = append(b.list, f)
}

type atomicBatch struct{ txn *badger.Txn }

func (a atomicBatch) Get(_ context.Context, key []byte) ([]byte, error) {
	item, err := a.txn.Get(key)
	if errors.Is(err, badger.ErrKeyNotFound) {
		return nil, storage.ErrKeyNotFound
	}
	if err != nil {
		return nil, err
	}
	return item.ValueCopy(nil)
}

func (a atomicBatch) Put(key []byte, val []byte, ttl int64) error {
	if ttl != 0 {
		return a.txn.SetEntry(badger.NewEntry(key, val).WithTTL(time.Duration(ttl) * time.Second))
	}
	return a.txn.Set(key, val)
}

func (a atomicBatch) Del(key []byte) error { return a.txn.Delete(key) }

func (b *batch) Atomic(fn func(context.Context, storage.AtomicBatch) error) {
	b.list = append(b.list, func(ctx context.Context) error { return fn(ctx, atomicBatch{txn: b.txn}) })
}

func (b *batch) Commit(ctx context.Context) error {
	// discard it anyway finally
	defer b.txn.Discard()
	for _, f := range b.list {
		err := f(ctx)
		if err != nil {
			return err
		}
	}

	err := b.txn.Commit()
	if err == nil {
		return nil
	}
	// Map badger's transaction errors to storage errors so callers handle them
	// consistently across engines instead of leaking raw badger errors (#44).
	if errors.Is(err, badger.ErrConflict) {
		// Optimistic-transaction conflict: a concurrent commit touched a key this
		// txn read/wrote. Surface as ErrCASFailed so the backend retries, matching
		// TiKV's write-conflict mapping.
		return storage.ErrCASFailed
	}
	if errors.Is(err, badger.ErrTxnTooBig) {
		// The batch exceeded badger's transaction size limit — a definite,
		// non-retryable failure. Wrap a storage sentinel so it is recognizable
		// rather than a bare badger error.
		return errors.Wrapf(storage.ErrUnexpectedRet, "badger txn too big: %v", err)
	}
	return err
}
