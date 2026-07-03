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

func newTestBadger(t *testing.T) storage.KvStorage {
	st, err := NewKvStorage(Config{Dir: t.TempDir()})
	require.NoError(t, err)
	t.Cleanup(func() { _ = st.Close() })
	return st
}

// TestBadgerCommitConflictMapsToCASFailed pins #44: an optimistic-transaction
// conflict at Commit must surface as storage.ErrCASFailed (like TiKV's
// write-conflict mapping), not a raw badger.ErrConflict.
func TestBadgerCommitConflictMapsToCASFailed(t *testing.T) {
	st := newTestBadger(t)
	ctx := context.Background()

	seed := st.BeginBatchWrite()
	seed.Put([]byte("K"), []byte("old"), 0)
	require.NoError(t, seed.Commit(ctx))

	// b1 takes its snapshot now (K=old).
	b1 := st.BeginBatchWrite()

	// b2 overwrites K and commits after b1's snapshot.
	b2 := st.BeginBatchWrite()
	b2.Put([]byte("K"), []byte("newer"), 0)
	require.NoError(t, b2.Commit(ctx))

	// b1's CAS reads K at its snapshot (sees "old", matches) and writes; committing
	// then conflicts because K was modified since b1's snapshot.
	b1.CAS([]byte("K"), []byte("x"), []byte("old"), 0)
	err := b1.Commit(ctx)
	require.Error(t, err)
	require.True(t, errors.Is(err, storage.ErrCASFailed),
		"badger commit conflict must map to ErrCASFailed, got %v", err)
}

// TestBadgerCASMissingKeyIsCASFailed pins the #45 cross-engine consistency for
// Badger: CAS on a missing key is a compare failure -> ErrCASFailed (a Conflict
// Is ErrCASFailed), never ErrKeyNotFound. TiKV was fixed to match this.
func TestBadgerCASMissingKeyIsCASFailed(t *testing.T) {
	st := newTestBadger(t)
	ctx := context.Background()

	b := st.BeginBatchWrite()
	b.CAS([]byte("does-not-exist"), []byte("new"), []byte("expected-old"), 0)
	err := b.Commit(ctx)
	require.Error(t, err)
	require.True(t, errors.Is(err, storage.ErrCASFailed),
		"CAS on a missing key must be ErrCASFailed, not ErrKeyNotFound, got %v", err)
	require.False(t, errors.Is(err, storage.ErrKeyNotFound))
}
