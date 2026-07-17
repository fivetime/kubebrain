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

// Package storagetest holds backend-agnostic conformance tests for the
// storage.KvStorage contract. The three backends (memkv, badger, tikv) each
// implement BatchWrite with different error timing and conflict-detection
// shapes; every campaign fix that had to "make backend X's CAS/error semantics
// match memkv" (#44/#45/#51) re-derived the contract by hand in a different
// control flow. RunBatchWriteContract pins that contract once so the next
// divergence fails in CI instead of at 33M-key scale.
package storagetest

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"

	"github.com/kubewharf/kubebrain/pkg/storage"
)

// RunBatchWriteContract exercises the shared BatchWrite semantics against a
// storage produced by newKV. Keys are scoped by run and subtest so the contract
// is safe against persistent/shared backends where a new client is not empty.
func RunBatchWriteContract(t *testing.T, newKV func(t *testing.T) storage.KvStorage) {
	ctx := context.Background()
	runPrefix := fmt.Sprintf("\x00storagetest/%d/", time.Now().UnixNano())
	key := func(t *testing.T, name string) []byte {
		return []byte(runPrefix + t.Name() + "/" + name)
	}
	get := func(t *testing.T, kv storage.KvStorage, key string) ([]byte, error) {
		return kv.Get(ctx, []byte(runPrefix+t.Name()+"/"+key))
	}
	seed := func(t *testing.T, kv storage.KvStorage, name, val string) {
		b := kv.BeginBatchWrite()
		b.Put(key(t, name), []byte(val), 0)
		require.NoError(t, b.Commit(ctx))
	}
	// mustAbsent asserts a key is not present (a clean miss, not an error).
	mustAbsent := func(t *testing.T, kv storage.KvStorage, key string) {
		v, err := get(t, kv, key)
		if err == nil {
			require.Nil(t, v, "key %q must be absent", key)
			return
		}
		require.True(t, errors.Is(err, storage.ErrKeyNotFound), "absent key must miss cleanly, got %v", err)
	}
	snapshotOne := func(t *testing.T, kv storage.KvStorage, name string) storage.Iter {
		k := key(t, name)
		end := append(append([]byte(nil), k...), 0)
		it, err := kv.Iter(ctx, k, end, 0, 0)
		require.NoError(t, err)
		require.NoError(t, it.Next(ctx))
		require.Equal(t, k, it.Key())
		t.Cleanup(func() { require.NoError(t, it.Close()) })
		return it
	}

	t.Run("PutIfNotExist_on_missing_commits", func(t *testing.T) {
		kv := newKV(t)
		b := kv.BeginBatchWrite()
		b.PutIfNotExist(key(t, "k"), []byte("v"), 0)
		require.NoError(t, b.Commit(ctx))
		v, err := get(t, kv, "k")
		require.NoError(t, err)
		require.Equal(t, []byte("v"), v)
	})

	t.Run("PutIfNotExist_on_existing_fails_CAS", func(t *testing.T) {
		kv := newKV(t)
		seed(t, kv, "k", "v1")
		b := kv.BeginBatchWrite()
		b.PutIfNotExist(key(t, "k"), []byte("v2"), 0)
		err := b.Commit(ctx)
		require.True(t, errors.Is(err, storage.ErrCASFailed), "PutIfNotExist over an existing key must fail CAS, got %v", err)
		v, _ := get(t, kv, "k")
		require.Equal(t, []byte("v1"), v, "a failed batch must not overwrite")
	})

	t.Run("CAS_matching_old_commits", func(t *testing.T) {
		kv := newKV(t)
		seed(t, kv, "k", "v1")
		b := kv.BeginBatchWrite()
		b.CAS(key(t, "k"), []byte("v2"), []byte("v1"), 0)
		require.NoError(t, b.Commit(ctx))
		v, _ := get(t, kv, "k")
		require.Equal(t, []byte("v2"), v)
	})

	t.Run("CAS_mismatched_old_fails_CAS", func(t *testing.T) {
		kv := newKV(t)
		seed(t, kv, "k", "v1")
		b := kv.BeginBatchWrite()
		b.CAS(key(t, "k"), []byte("v2"), []byte("WRONG"), 0)
		err := b.Commit(ctx)
		require.True(t, errors.Is(err, storage.ErrCASFailed), "CAS with a wrong old value must fail CAS, got %v", err)
		v, _ := get(t, kv, "k")
		require.Equal(t, []byte("v1"), v)
	})

	t.Run("CAS_on_missing_fails_CAS", func(t *testing.T) {
		kv := newKV(t)
		b := kv.BeginBatchWrite()
		b.CAS(key(t, "k"), []byte("v2"), []byte("v1"), 0)
		err := b.Commit(ctx)
		require.True(t, errors.Is(err, storage.ErrCASFailed), "CAS on a missing key must fail CAS, got %v", err)
		mustAbsent(t, kv, "k")
	})

	t.Run("Put_overwrites_unconditionally", func(t *testing.T) {
		kv := newKV(t)
		seed(t, kv, "k", "v1")
		b := kv.BeginBatchWrite()
		b.Put(key(t, "k"), []byte("v2"), 0)
		require.NoError(t, b.Commit(ctx))
		v, _ := get(t, kv, "k")
		require.Equal(t, []byte("v2"), v)
	})

	t.Run("Del_removes", func(t *testing.T) {
		kv := newKV(t)
		seed(t, kv, "k", "v1")
		b := kv.BeginBatchWrite()
		b.Del(key(t, "k"))
		require.NoError(t, b.Commit(ctx))
		mustAbsent(t, kv, "k")
	})

	t.Run("DelCurrent_unchanged_snapshot_removes", func(t *testing.T) {
		kv := newKV(t)
		seed(t, kv, "k", "v1")
		it := snapshotOne(t, kv, "k")
		b := kv.BeginBatchWrite()
		b.DelCurrent(it)
		require.NoError(t, b.Commit(ctx))
		mustAbsent(t, kv, "k")
	})

	t.Run("DelCurrent_changed_snapshot_fails_CAS", func(t *testing.T) {
		kv := newKV(t)
		seed(t, kv, "k", "v1")
		it := snapshotOne(t, kv, "k")
		seed(t, kv, "k", "v2")
		b := kv.BeginBatchWrite()
		b.DelCurrent(it)
		err := b.Commit(ctx)
		require.True(t, errors.Is(err, storage.ErrCASFailed),
			"DelCurrent after a snapshot-visible value changed must fail CAS, got %v", err)
		v, getErr := get(t, kv, "k")
		require.NoError(t, getErr)
		require.Equal(t, []byte("v2"), v, "failed DelCurrent must retain the replacement")
	})

	t.Run("batch_is_atomic_on_conflict", func(t *testing.T) {
		kv := newKV(t)
		seed(t, kv, "guard", "g1")
		b := kv.BeginBatchWrite()
		b.Put(key(t, "sibling"), []byte("s"), 0)                 // would-be write
		b.CAS(key(t, "guard"), []byte("g2"), []byte("WRONG"), 0) // conflicts
		err := b.Commit(ctx)
		require.True(t, errors.Is(err, storage.ErrCASFailed), "conflicting batch must fail, got %v", err)
		mustAbsent(t, kv, "sibling") // the sibling write must NOT have applied
		v, _ := get(t, kv, "guard")
		require.Equal(t, []byte("g1"), v)
	})
}
