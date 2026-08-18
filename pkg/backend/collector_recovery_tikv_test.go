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
	"io"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	proto "github.com/kubewharf/kubebrain-client/api/v2rpc"

	"github.com/kubewharf/kubebrain/pkg/metrics"
	"github.com/kubewharf/kubebrain/pkg/storage"
	storagetikv "github.com/kubewharf/kubebrain/pkg/storage/tikv"
)

// TestCollectorRecoversLostRingPublicationTiKV proves A5056 on an actual TiKV
// transaction. CI without a cluster skips it. The test commits object, ordered
// event-log and witness data, removes only the process-local ring publication,
// and requires a restarted collector to reconstruct the exact watch event
// before advancing its read-visible watermark.
func TestCollectorRecoversLostRingPublicationTiKV(t *testing.T) {
	pd := os.Getenv("KUBEBRAIN_TIKV_PD")
	if pd == "" {
		t.Skip("set KUBEBRAIN_TIKV_PD=<pd-addrs> to run the TiKV collector recovery test")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	kv, err := storagetikv.NewKvStorageWithContext(ctx, strings.Split(pd, ","), 1, storagetikv.Security{})
	require.NoError(t, err)
	keyspace := "collector-recovery-" + time.Now().UTC().Format("20060102t150405000000000")
	recorder := &compactMetricRecorder{}
	b := NewBackend(kv, Config{
		Prefix: "/integration/collector-recovery", Keyspace: keyspace,
		Identity: "collector-recovery-test", EnableEtcdCompatibility: true,
	}, recorder).(*backend)
	b.SetCurrentRevision(uint64(time.Now().UnixNano()))
	defer func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cleanupCancel()
		deleteTiKVKeyspace(t, cleanupCtx, kv, b.ks.ObjectKeyspaceStart(), b.ks.ObjectKeyspaceEnd())
		require.NoError(t, b.Close())
	}()

	b.commitWaitBackstop = 100 * time.Millisecond
	b.stopWorkers()
	key := []byte("/integration/collector-recovery/lost-ring")
	value := []byte("tikv-durable-event")
	results, revision, err := b.TxnApply(ctx, []TxnWriteOp{{Key: key, Value: value}}, nil)
	require.ErrorIs(t, err, storage.ErrUncertainResult)
	require.Nil(t, results)
	require.NotZero(t, revision)
	require.Less(t, b.GetCurrentRevision(), revision)
	require.NotEmpty(t, b.watchEventsRingBuffer[revision%watchersChanCapacity].take(revision),
		"remove the only process-local publication while retaining TiKV state")

	b.collectorStallWarnAfter = 10 * time.Millisecond
	b.collectorStallSkipAfter = 50 * time.Millisecond
	collectorCtx, cancelCollector := context.WithCancel(context.Background())
	collectorDone := make(chan struct{})
	go func() {
		defer close(collectorDone)
		b.collectStorageWriteEvents(collectorCtx)
	}()
	select {
	case batch := <-b.watchChan:
		require.Len(t, batch, 1)
		require.Equal(t, proto.Event_CREATE, batch[0].Type)
		require.Equal(t, revision, batch[0].Revision)
		require.Equal(t, key, batch[0].Kv.Key)
		require.Equal(t, value, StripInlineValue(batch[0].Kv.Value))
	case <-ctx.Done():
		t.Fatalf("collector did not replay the TiKV durable revision: %v", ctx.Err())
	}
	require.Equal(t, revision, b.GetCurrentRevision())
	require.Contains(t, recorder.snapshot(), compactMetricRecord{
		kind: "counter", name: "watch.collector.recovery", value: 1,
		tags: []metrics.T{metrics.Tag("outcome", "replayed")},
	})
	cancelCollector()
	<-collectorDone
}

func deleteTiKVKeyspace(t *testing.T, ctx context.Context, kv storage.KvStorage, start, end []byte) {
	t.Helper()
	iter, err := kv.Iter(ctx, start, end, 0, 0)
	require.NoError(t, err)
	var keys [][]byte
	for {
		err = iter.Next(ctx)
		if err == io.EOF {
			break
		}
		require.NoError(t, err)
		keys = append(keys, append([]byte(nil), iter.Key()...))
	}
	require.NoError(t, iter.Close())
	if len(keys) == 0 {
		return
	}
	batch := kv.BeginBatchWrite()
	for _, key := range keys {
		batch.Del(key)
	}
	require.NoError(t, batch.Commit(ctx))
}
