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
	"sync"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"

	proto "github.com/kubewharf/kubebrain-client/api/v2rpc"

	"github.com/kubewharf/kubebrain/pkg/metrics/mock"
	imemkv "github.com/kubewharf/kubebrain/pkg/storage/memkv"
)

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
	b.waitCommittedRevision(context.Background(), base+10)
	require.GreaterOrEqual(t, b.GetCurrentRevision(), base+10, "wait must return only once committed reached the target")
	require.Less(t, time.Since(start), 3*time.Second, "wake must be prompt, not backstop-bound")
	<-done

	// ctx cancellation unblocks a waiter whose target never arrives.
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	start = time.Now()
	b.waitCommittedRevision(ctx, base+1000)
	require.Less(t, time.Since(start), time.Second, "ctx expiry must unblock the waiter")
}
