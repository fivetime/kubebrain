// Copyright 2026 ByteDance and/or its affiliates
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package backend

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	proto "github.com/kubewharf/kubebrain-client/api/v2rpc"
)

func TestRangeTxnBarrierBlocksExternalWrites(t *testing.T) {
	b, ctx := newTxnApplyBackend(t)
	rangeCtx, unlock := b.BeginRangeTxn(ctx)

	externalDone := make(chan error, 1)
	go func() {
		_, err := b.Create(context.Background(), &proto.CreateRequest{
			Key:   []byte(prefix + "/barrier/external"),
			Value: []byte("external"),
		})
		externalDone <- err
	}()

	select {
	case err := <-externalDone:
		require.FailNow(t, "external write crossed range transaction barrier", "err=%v", err)
	case <-time.After(50 * time.Millisecond):
	}

	// The transaction owning the exclusive barrier must be able to write without
	// recursively acquiring the read side of the same RWMutex.
	_, _, err := b.TxnApply(rangeCtx, []TxnWriteOp{{
		Key:   []byte(prefix + "/barrier/owner"),
		Value: []byte("owner"),
	}}, nil)
	require.NoError(t, err)

	unlock()
	select {
	case err := <-externalDone:
		require.NoError(t, err)
	case <-time.After(time.Second):
		require.FailNow(t, "external write did not resume after range transaction")
	}
}
