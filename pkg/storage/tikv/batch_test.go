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

package tikv

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	tikverr "github.com/tikv/client-go/v2/error"
	"github.com/tikv/client-go/v2/txnkv"

	"github.com/kubewharf/kubebrain/pkg/storage"
)

// TestBatchCommitWithFailedBeginReturnsErrorNotPanic reproduces the crash where
// BeginBatchWrite's Begin() failure (e.g. PD/TSO briefly unavailable) leaves
// b.txn nil and stashes the error as list[0]; Commit's deferred rollback then
// dereferenced the nil txn and panicked the whole process. Commit must return
// the error instead.
func TestBatchCommitWithFailedBeginReturnsErrorNotPanic(t *testing.T) {
	beginErr := errors.New("pd unavailable")
	b := &batch{
		txn: nil, // Begin() failed
		list: []func(ctx context.Context) error{
			func(ctx context.Context) error { return beginErr }, // stashed by BeginBatchWrite
		},
	}
	require.NotPanics(t, func() {
		err := b.Commit(context.Background())
		require.ErrorIs(t, err, beginErr)
	})
}

func TestBatchCommitBindsBeginToCallerContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	b := &batch{begin: func(got context.Context) (*txnkv.KVTxn, error) {
		require.ErrorIs(t, got.Err(), context.Canceled)
		return nil, got.Err()
	}}
	require.ErrorIs(t, b.Commit(ctx), context.Canceled)
}

func TestUnavailableBeginErrorPreservesRetryableClassification(t *testing.T) {
	err := unavailableBeginError(context.Background(), "read", errors.New("pd unavailable"))
	require.ErrorIs(t, err, storage.ErrUnavailable)
	require.ErrorContains(t, err, "failed to create read txn: pd unavailable")

	empty := unavailableBeginError(context.Background(), "write", errors.New(""))
	require.ErrorIs(t, empty, storage.ErrUnavailable)
	require.ErrorContains(t, empty, "failed to create write txn")

	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	require.ErrorIs(t, unavailableBeginError(canceled, "read", errors.New("pd unavailable")), context.Canceled)
}

func TestUncertainCommitErrorClassifiesTxnLockNotFound(t *testing.T) {
	require.True(t, isUncertainCommitError(errors.New("TxnLockNotFound")))
	require.True(t, isUncertainCommitError(errors.New("commit failed: TxnLockNotFound { start_ts: 1 }")))
	require.False(t, isUncertainCommitError(errors.New("transaction lock conflict")))
}

func TestUncertainCommitErrorClassifiesStaleCommand(t *testing.T) {
	require.True(t, isUncertainCommitError(tikverr.ErrTiKVStaleCommand))
	require.True(t, isUncertainCommitError(fmt.Errorf("commit primary key: %w", tikverr.ErrTiKVStaleCommand)))
}

func TestBatchRejectsKeyBeyondManagedTiKVLimit(t *testing.T) {
	key := make([]byte, maxTiKVPhysicalKeyBytes+1)
	b := &batch{}
	b.Put(key, []byte("must-not-be-truncated"), 0)
	err := b.Commit(context.Background())
	require.ErrorIs(t, err, storage.ErrKeyTooLarge)
	require.ErrorContains(t, err, "2097153 bytes")
	require.ErrorContains(t, err, "limit is 2097152")

	require.NoError(t, validateTiKVPhysicalKey(key[:maxTiKVPhysicalKeyBytes]))
}
