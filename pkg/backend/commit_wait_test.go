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

package backend

import (
	"context"
	"fmt"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"

	proto "github.com/kubewharf/kubebrain-client/api/v2rpc"

	"github.com/kubewharf/kubebrain/pkg/metrics"
	"github.com/kubewharf/kubebrain/pkg/metrics/mock"
	"github.com/kubewharf/kubebrain/pkg/storage"
	imemkv "github.com/kubewharf/kubebrain/pkg/storage/memkv"
)

type collectorRecoveryFailStorage struct {
	storage.KvStorage
	failIter bool
}

func compactMetricRecordsContain(records []compactMetricRecord, want compactMetricRecord) bool {
	for _, record := range records {
		if reflect.DeepEqual(record, want) {
			return true
		}
	}
	return false
}

func (s *collectorRecoveryFailStorage) Iter(ctx context.Context, start, end []byte, timestamp, limit uint64) (storage.Iter, error) {
	if s.failIter {
		return nil, fmt.Errorf("injected durable recovery scan failure")
	}
	return s.KvStorage.Iter(ctx, start, end, timestamp, limit)
}

// TestWriteThenReadSeesOwnWrite pins #35 (apply-then-ack): once a write RPC
// returns, the committed (read-visible) revision has reached the write's
// revision, so an immediate rev=0 read observes it. Before the fix the ACK
// raced the collector and the committed watermark lagged by tens of revisions
// (~50ms), so a write-then-read missed its own write — the root cause of the
// "read version not as new as written" staleness storms at 500k objects.
func TestWriteThenReadSeesOwnWrite(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	m := mock.NewMinimalMetrics(ctrl)
	kv := imemkv.NewKvStorage()
	defer func() { require.NoError(t, kv.Close()) }()
	b := NewBackend(kv, Config{Prefix: prefix, Identity: getStorageIdentity(), EnableEtcdCompatibility: true}, m).(*backend)
	b.SetCurrentRevision(uint64(time.Now().UnixNano()))
	ctx := context.Background()

	// Concurrent writers each assert their own write is committed (and hence
	// rev=0 readable) the instant their RPC returns — no waitCommitted helper.
	var wg sync.WaitGroup
	errs := make(chan error, 64)
	for w := 0; w < 8; w++ {
		w := w
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 8; i++ {
				key := []byte(fmt.Sprintf("%s/rw/%d-%d", prefix, w, i))
				cr, err := b.Create(ctx, &proto.CreateRequest{Key: key, Value: []byte("v")})
				if err != nil {
					errs <- err
					return
				}
				if got := b.GetCurrentRevision(); got < cr.Header.Revision {
					errs <- fmt.Errorf("write ACKed at rev %d but committed is %d", cr.Header.Revision, got)
					return
				}
				// The immediate rev=0 read must see the key.
				gr, err := b.Get(ctx, &proto.GetRequest{Key: key})
				if err != nil || gr.Kv == nil {
					errs <- fmt.Errorf("write-then-read missed own write %q: kv=%v err=%v", key, gr.GetKv(), err)
					return
				}
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		require.NoError(t, err)
	}
}

// TestWaitCommittedRevisionWakesAndBounds pins the notifier mechanics: a waiter
// blocks until the committed revision reaches its target, wakes promptly on
// advance, and honors ctx cancellation rather than blocking forever.
func TestWaitCommittedRevisionWakesAndBounds(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	m := mock.NewMinimalMetrics(ctrl)
	kv := imemkv.NewKvStorage()
	defer func() { require.NoError(t, kv.Close()) }()
	b := NewBackend(kv, Config{Prefix: prefix, Identity: getStorageIdentity(), EnableEtcdCompatibility: true}, m).(*backend)
	base := uint64(time.Now().UnixNano())
	b.SetCurrentRevision(base)

	// Waker: advances committed to base+10 shortly after the waiter parks.
	done := make(chan struct{})
	go func() {
		defer close(done)
		time.Sleep(50 * time.Millisecond)
		b.SetCurrentRevision(base + 10)
	}()
	start := time.Now()
	require.NoError(t, b.waitCommittedRevision(context.Background(), base+10))
	require.GreaterOrEqual(t, b.GetCurrentRevision(), base+10, "wait must return only once committed reached the target")
	require.Less(t, time.Since(start), 3*time.Second, "wake must be prompt, not backstop-bound")
	<-done

	// ctx cancellation unblocks a waiter whose target never arrives.
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	start = time.Now()
	require.ErrorIs(t, b.waitCommittedRevision(ctx, base+1000), context.DeadlineExceeded)
	require.Less(t, time.Since(start), time.Second, "ctx expiry must unblock the waiter")
}

func TestWaitCommittedRevisionBackstopReturnsErrorAndEmitsFixedMetrics(t *testing.T) {
	recorder := &compactMetricRecorder{}
	kv := imemkv.NewKvStorage()
	defer func() { require.NoError(t, kv.Close()) }()
	b := NewBackend(kv, Config{Prefix: prefix, Identity: getStorageIdentity(), EnableEtcdCompatibility: true}, recorder).(*backend)
	base := uint64(time.Now().UnixNano())
	b.SetCurrentRevision(base)

	require.ErrorIs(t, b.waitCommittedRevisionUntil(context.Background(), base+1, 10*time.Millisecond), errCommitWaitBackstop)
	require.Contains(t, recorder.snapshot(), compactMetricRecord{
		kind: "counter", name: "write.commit_wait.failure", value: 1,
		tags: []metrics.T{metrics.Tag("reason", "backstop")},
	})
}

func TestCommitWaitFailureMetricsInitializeFixedReasons(t *testing.T) {
	recorder := &compactMetricRecorder{}
	initCommitWaitFailureMetrics(recorder)
	emitCommitWaitFailure(recorder, "context_done")

	require.Equal(t, []compactMetricRecord{
		{kind: "counter", name: "write.commit_wait.failure", value: int64(0), tags: []metrics.T{metrics.Tag("reason", "context_done")}},
		{kind: "counter", name: "write.commit_wait.failure", value: int64(0), tags: []metrics.T{metrics.Tag("reason", "backstop")}},
		{kind: "counter", name: "write.commit_wait.failure", value: 1, tags: []metrics.T{metrics.Tag("reason", "context_done")}},
	}, recorder.records)
}

func TestTxnApplyDoesNotAcknowledgeBeforeCommittedRevisionIsVisible(t *testing.T) {
	recorder := &compactMetricRecorder{}
	kv := imemkv.NewKvStorage()
	defer func() { require.NoError(t, kv.Close()) }()
	b := NewBackend(kv, Config{Prefix: prefix, Identity: getStorageIdentity(), EnableEtcdCompatibility: true}, recorder).(*backend)
	b.SetCurrentRevision(uint64(time.Now().UnixNano()))
	b.commitWaitBackstop = 10 * time.Millisecond
	// Model a collector that has stopped advancing the read-visible watermark
	// while TiKV remains writable.
	b.stopWorkers()

	results, revision, err := b.TxnApply(context.Background(), []TxnWriteOp{{
		Key: []byte(prefix + "/commit-wait/stalled"), Value: []byte("durable"),
	}}, nil)
	require.ErrorIs(t, err, storage.ErrUncertainResult)
	require.ErrorIs(t, err, errCommitWaitBackstop)
	require.NotZero(t, revision, "preserve the committed revision for caller reconciliation")
	require.Nil(t, results, "an unreadable committed revision must never be acknowledged with write results")
	require.Less(t, b.GetCurrentRevision(), revision)

	// Simulate the exact crash window: TiKV committed the transaction and its
	// event witness, but the in-memory ring publication was lost. The restarted
	// collector must replay the durable event rather than skip the revision.
	require.NotEmpty(t, b.watchEventsRingBuffer[revision%watchersChanCapacity].take(revision))
	b.collectorStallWarnAfter = time.Millisecond
	b.collectorStallSkipAfter = 5 * time.Millisecond
	collectorCtx, cancelCollector := context.WithCancel(context.Background())
	collectorDone := make(chan struct{})
	go func() {
		defer close(collectorDone)
		b.collectStorageWriteEvents(collectorCtx)
	}()
	select {
	case batch := <-b.watchChan:
		require.Len(t, batch, 1)
		require.Equal(t, []byte(prefix+"/commit-wait/stalled"), batch[0].Kv.Key)
		require.Equal(t, []byte("durable"), StripInlineValue(batch[0].Kv.Value))
		require.Equal(t, revision, batch[0].Revision)
	case <-time.After(time.Second):
		t.Fatal("collector did not replay the durable stalled revision")
	}
	require.Equal(t, revision, b.GetCurrentRevision())
	require.Contains(t, recorder.snapshot(), compactMetricRecord{
		kind: "counter", name: "watch.collector.recovery", value: 1,
		tags: []metrics.T{metrics.Tag("outcome", "replayed")},
	})
	cancelCollector()
	<-collectorDone
}

func TestCollectorRecoveryFailureDoesNotAdvanceCommittedRevision(t *testing.T) {
	recorder := &compactMetricRecorder{}
	raw := imemkv.NewKvStorage()
	store := &collectorRecoveryFailStorage{KvStorage: raw}
	defer func() { require.NoError(t, raw.Close()) }()
	b := NewBackend(store, Config{Prefix: prefix, Identity: getStorageIdentity(), EnableEtcdCompatibility: true}, recorder).(*backend)
	b.SetCurrentRevision(uint64(time.Now().UnixNano()))
	b.commitWaitBackstop = 10 * time.Millisecond
	b.stopWorkers()

	_, revision, err := b.TxnApply(context.Background(), []TxnWriteOp{{
		Key: []byte(prefix + "/commit-wait/recovery-failure"), Value: []byte("durable"),
	}}, nil)
	require.ErrorIs(t, err, storage.ErrUncertainResult)
	require.NotEmpty(t, b.watchEventsRingBuffer[revision%watchersChanCapacity].take(revision))
	store.failIter = true
	b.collectorStallWarnAfter = time.Millisecond
	b.collectorStallSkipAfter = 5 * time.Millisecond
	collectorCtx, cancelCollector := context.WithCancel(context.Background())
	collectorDone := make(chan struct{})
	go func() {
		defer close(collectorDone)
		b.collectStorageWriteEvents(collectorCtx)
	}()

	require.Eventually(t, func() bool {
		return compactMetricRecordsContain(recorder.snapshot(), compactMetricRecord{
			kind: "counter", name: "watch.collector.recovery", value: 1,
			tags: []metrics.T{metrics.Tag("outcome", "failed")},
		})
	}, time.Second, time.Millisecond)
	require.Less(t, b.GetCurrentRevision(), revision, "failed durable recovery must not skip the committed revision")
	cancelCollector()
	<-collectorDone
}
