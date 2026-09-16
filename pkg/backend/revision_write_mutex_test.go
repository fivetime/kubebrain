package backend

import (
	"context"
	"math"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestTxnApplyCancelsRevisionAdmission(t *testing.T) {
	for _, deadline := range []bool{false, true} {
		name := "cancel"
		if deadline {
			name = "deadline"
		}
		t.Run(name, func(t *testing.T) {
			b, parent := newTxnApplyBackend(t)
			initialRevision := b.GetCurrentRevision()
			b.revisionWriteMu.Lock()
			var once sync.Once
			release := func() { once.Do(b.revisionWriteMu.Unlock) }
			defer release()
			ctx, cancel := context.WithCancel(parent)
			want := context.Canceled
			if deadline {
				cancel()
				ctx, cancel = context.WithTimeout(parent, 30*time.Millisecond)
				want = context.DeadlineExceeded
			}
			defer cancel()
			done := make(chan error, 1)
			go func() {
				_, _, err := b.TxnApply(ctx, []TxnWriteOp{{Key: []byte("canceled"), Value: []byte("value")}}, nil)
				done <- err
			}()
			if !deadline {
				time.Sleep(30 * time.Millisecond)
				cancel()
			}
			select {
			case err := <-done:
				require.ErrorIs(t, err, want)
			case <-time.After(time.Second):
				release()
				<-done // do not leak the intentionally blocked caller on RED
				t.Fatal("canceled transaction waited for revision owner to release")
			}
			// Cancellation must release the shared logical barrier even while
			// the unrelated revision owner is still active.
			barrierCtx, stop := context.WithTimeout(parent, time.Second)
			defer stop()
			require.NoError(t, b.logicalWriteMu.LockContext(barrierCtx))
			b.logicalWriteMu.Unlock()
			release()
			results, revision, err := b.TxnApply(parent, []TxnWriteOp{{Key: []byte("next"), Value: []byte("value")}}, nil)
			require.NoError(t, err)
			require.Len(t, results, 1)
			require.EqualValues(t, initialRevision+1, revision, "canceled admission must not allocate a revision")
		})
	}
}

func TestTxnApplyRechecksPendingRevisionAfterQueuedAdmission(t *testing.T) {
	b, parent := newTxnApplyBackend(t)
	b.revisionWriteMu.Lock()
	var once sync.Once
	release := func() { once.Do(b.revisionWriteMu.Unlock) }
	defer release()
	ctx, cancel := context.WithCancel(parent)
	defer cancel()
	done := make(chan error, 1)
	go func() {
		_, _, err := b.TxnApply(ctx, []TxnWriteOp{{Key: []byte("queued"), Value: []byte("value")}}, nil)
		done <- err
	}()
	// Once the shared barrier is occupied, this writer has passed its first
	// pending-revision check but cannot yet acquire revision admission.
	until := time.Now().Add(time.Second)
	for b.logicalWriteMu.semaphore().TryAcquire(math.MaxInt64) {
		b.logicalWriteMu.semaphore().Release(math.MaxInt64)
		if time.Now().After(until) {
			cancel()
			release()
			<-done
			t.Fatal("transaction did not enter shared barrier")
		}
		time.Sleep(time.Millisecond)
	}
	finish := b.beginPendingRevision()
	defer finish()
	release()
	select {
	case err := <-done:
		t.Fatalf("transaction bypassed newly pending revision: %v", err)
	case <-time.After(30 * time.Millisecond):
	}
	cancel()
	select {
	case err := <-done:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(time.Second):
		finish()
		<-done
		t.Fatal("pending revision wait ignored cancellation")
	}
	finish()
	next, stop := context.WithTimeout(parent, time.Second)
	defer stop()
	require.NoError(t, b.revisionWriteMu.LockContext(next))
	b.revisionWriteMu.Unlock()
}
