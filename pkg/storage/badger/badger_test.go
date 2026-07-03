// Copyright 2026 ByteDance and/or its affiliates
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
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/kubewharf/kubebrain/pkg/storage"
)

// TestBadgerBatchAndLifecycle exercises the batch write path (Put / PutIfNotExist
// create + conflict / CAS / Del) and a clean open→use→close lifecycle. Close must
// return without hanging, proving the value-log GC goroutine (#66) stops.
func TestBadgerBatchAndLifecycle(t *testing.T) {
	s, err := NewKvStorage(Config{Dir: t.TempDir()})
	require.NoError(t, err)
	ctx := context.Background()

	// PutIfNotExist creates when absent.
	b := s.BeginBatchWrite()
	b.PutIfNotExist([]byte("k1"), []byte("v1"), 0)
	b.Put([]byte("k2"), []byte("v2"), 0)
	require.NoError(t, b.Commit(ctx))

	v, err := s.Get(ctx, []byte("k1"))
	require.NoError(t, err)
	require.Equal(t, "v1", string(v))

	// PutIfNotExist on an existing key must fail with a conflict, not silently
	// succeed (the region around #65).
	b = s.BeginBatchWrite()
	b.PutIfNotExist([]byte("k1"), []byte("other"), 0)
	err = b.Commit(ctx)
	require.Error(t, err)
	var conflict *storage.Conflict
	require.True(t, errors.As(err, &conflict), "PutIfNotExist over an existing key must return a conflict")
	v, err = s.Get(ctx, []byte("k1"))
	require.NoError(t, err)
	require.Equal(t, "v1", string(v), "value must be unchanged after the failed PutIfNotExist")

	// CAS + iterate.
	b = s.BeginBatchWrite()
	b.CAS([]byte("k2"), []byte("v2b"), []byte("v2"), 0)
	require.NoError(t, b.Commit(ctx))

	it, err := s.Iter(ctx, []byte("k0"), []byte("k9"), 0, 0)
	require.NoError(t, err)
	got := map[string]string{}
	for it.Next(ctx) == nil {
		got[string(it.Key())] = string(it.Val())
	}
	require.NoError(t, it.Close())
	require.Equal(t, map[string]string{"k1": "v1", "k2": "v2b"}, got)

	require.NoError(t, s.Del(ctx, []byte("k1")))
	_, err = s.Get(ctx, []byte("k1"))
	require.True(t, errors.Is(err, storage.ErrKeyNotFound))

	require.NoError(t, s.Close())
}
