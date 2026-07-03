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
	"testing"

	"github.com/stretchr/testify/require"
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
