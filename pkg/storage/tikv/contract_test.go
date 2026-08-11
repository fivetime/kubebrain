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
	"context"
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/kubewharf/kubebrain/pkg/storage"
	"github.com/kubewharf/kubebrain/pkg/storage/storagetest"
)

// TestBatchWriteContract runs the shared BatchWrite conformance suite against a
// real TiKV cluster. It is skipped unless KUBEBRAIN_TIKV_PD names the PD
// endpoints (comma-separated), since CI has no cluster — the memkv and badger
// contract tests guard the shared semantics in CI; this one is for validating
// TiKV against the same contract on demand.
func TestBatchWriteContract(t *testing.T) {
	pd := os.Getenv("KUBEBRAIN_TIKV_PD")
	if pd == "" {
		t.Skip("set KUBEBRAIN_TIKV_PD=<pd-addrs> to run the TiKV BatchWrite contract")
	}
	storagetest.RunBatchWriteContract(t, func(t *testing.T) storage.KvStorage {
		kv, err := NewKvStorage(strings.Split(pd, ","), 1, Security{})
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, kv.Close()) })
		return kv
	})
}

// TestExplicitSnapshotGetter proves the timestamp-bearing API sees a stable
// historical value and, unlike Get/BatchGet, does not implicitly advance to a
// newer TSO.
func TestExplicitSnapshotGetter(t *testing.T) {
	pd := os.Getenv("KUBEBRAIN_TIKV_PD")
	if pd == "" {
		t.Skip("set KUBEBRAIN_TIKV_PD=<pd-addrs> to run the TiKV snapshot contract")
	}
	kv, err := NewKvStorage(strings.Split(pd, ","), 1, Security{})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, kv.Close()) })
	reader := kv.(storage.SnapshotGetter)
	ctx := context.Background()
	key := []byte("kubebrain/storage-contract/explicit-snapshot")
	cleanup := kv.BeginBatchWrite()
	cleanup.Del(key)
	_ = cleanup.Commit(ctx)

	first := kv.BeginBatchWrite()
	first.Put(key, []byte("before"), 0)
	require.NoError(t, first.Commit(ctx))
	ts, err := kv.GetTimestampOracle(ctx)
	require.NoError(t, err)
	second := kv.BeginBatchWrite()
	second.Put(key, []byte("after"), 0)
	require.NoError(t, second.Commit(ctx))

	got, err := reader.GetAt(ctx, key, ts)
	require.NoError(t, err)
	require.Equal(t, []byte("before"), got)
	batch, err := reader.BatchGetAt(ctx, [][]byte{key, []byte("missing")}, ts)
	require.NoError(t, err)
	require.Equal(t, []byte("before"), batch[string(key)])
	require.NotContains(t, batch, "missing")
}
