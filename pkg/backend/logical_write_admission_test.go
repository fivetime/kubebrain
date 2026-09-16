package backend

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

func TestSharedLogicalAdmissionCancellation(t *testing.T) {
	calls := map[string]func(*backend, context.Context) error{
		"txn": func(b *backend, ctx context.Context) error {
			_, _, err := b.TxnApply(ctx, []TxnWriteOp{{Key: []byte("audit"), Value: []byte("v")}}, nil)
			return err
		},
		"internal_put":    func(b *backend, ctx context.Context) error { return b.InternalPut(ctx, []byte("audit"), []byte("v")) },
		"internal_delete": func(b *backend, ctx context.Context) error { return b.InternalDelete(ctx, []byte("audit")) },
		"internal_cas": func(b *backend, ctx context.Context) error {
			return b.InternalCAS(ctx, []InternalCASOp{{Key: []byte("audit"), Value: []byte("v")}})
		},
		"orphan_heal": func(b *backend, ctx context.Context) error {
			_, err := b.healOrphanIndex(ctx, []byte("audit"))
			return err
		},
	}
	for name, call := range calls {
		t.Run(name, func(t *testing.T) {
			b, parent := newTxnApplyBackend(t)
			b.logicalWriteMu.Lock()
			var once sync.Once
			release := func() { once.Do(b.logicalWriteMu.Unlock) }
			defer release()
			ctx, cancel := context.WithTimeout(parent, 30*time.Millisecond)
			defer cancel()
			done := make(chan error, 1)
			go func() { done <- call(b, ctx) }()
			select {
			case err := <-done:
				if !errors.Is(err, context.DeadlineExceeded) {
					t.Fatalf("got %v", err)
				}
			case <-time.After(500 * time.Millisecond):
				release()
				<-done
				t.Fatal("deadline waited for unrelated exclusive logical owner")
			}
		})
	}
}

func TestSharedLogicalAdmissionOwnerContext(t *testing.T) {
	b := &backend{}
	b.logicalWriteMu.Lock()
	var once sync.Once
	release := func() { once.Do(b.logicalWriteMu.Unlock) }
	defer release()
	ctx := b.withLogicalWriteOwnership(context.Background())
	unlock, err := b.lockLogicalWrite(ctx)
	if err != nil {
		t.Fatal(err)
	}
	unlock()
	if b.logicalWriteMu.semaphore().TryAcquire(1) {
		b.logicalWriteMu.RUnlock()
		t.Fatal("borrowed ownership released outer exclusive barrier")
	}
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	unlock, err = b.lockLogicalWrite(canceled)
	if !errors.Is(err, context.Canceled) || unlock != nil {
		t.Fatalf("canceled owner admitted: unlock_nil=%v err=%v", unlock == nil, err)
	}
	foreign := (&backend{}).withLogicalWriteOwnership(context.Background())
	foreign, stop := context.WithTimeout(foreign, 30*time.Millisecond)
	defer stop()
	unlock, err = b.lockLogicalWrite(foreign)
	if !errors.Is(err, context.DeadlineExceeded) || unlock != nil {
		t.Fatalf("foreign owner bypassed barrier: unlock_nil=%v err=%v", unlock == nil, err)
	}
	release()
	next, finish := context.WithTimeout(context.Background(), time.Second)
	defer finish()
	if err := b.logicalWriteMu.LockContext(next); err != nil {
		t.Fatal(err)
	}
	b.logicalWriteMu.Unlock()
}

// Nil storage is intentional: canceled admission must return before touching
// storage, revision allocation, or leadership providers, even with ownership.
func TestSharedLogicalAdmissionCanceledBeforeStorage(t *testing.T) {
	calls := map[string]func(*backend, context.Context) error{
		"txn": func(b *backend, ctx context.Context) error {
			_, _, err := b.TxnApply(ctx, []TxnWriteOp{{Key: []byte("audit"), Value: []byte("v")}}, nil)
			return err
		},
		"internal_put":    func(b *backend, ctx context.Context) error { return b.InternalPut(ctx, []byte("audit"), []byte("v")) },
		"internal_delete": func(b *backend, ctx context.Context) error { return b.InternalDelete(ctx, []byte("audit")) },
		"internal_cas": func(b *backend, ctx context.Context) error {
			return b.InternalCAS(ctx, []InternalCASOp{{Key: []byte("audit"), Value: []byte("v")}})
		},
		"orphan_heal": func(b *backend, ctx context.Context) error {
			_, err := b.healOrphanIndex(ctx, []byte("audit"))
			return err
		},
	}
	for name, call := range calls {
		t.Run(name, func(t *testing.T) {
			for _, owned := range []bool{false, true} {
				b := &backend{}
				parent := context.Background()
				if owned {
					parent = b.withLogicalWriteOwnership(parent)
				}
				ctx, cancel := context.WithCancel(parent)
				cancel()
				if err := call(b, ctx); !errors.Is(err, context.Canceled) {
					t.Fatalf("owned=%v: %v", owned, err)
				}
			}
		})
	}
}

func TestSharedLogicalAdmissionExplicitCancellation(t *testing.T) {
	b := &backend{}
	b.logicalWriteMu.Lock()
	var once sync.Once
	release := func() { once.Do(b.logicalWriteMu.Unlock) }
	defer release()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		unlock, err := b.lockLogicalWrite(ctx)
		if unlock != nil {
			unlock()
		}
		done <- err
	}()
	time.Sleep(30 * time.Millisecond)
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		release()
		<-done
		t.Fatal("explicit cancellation waited for exclusive owner")
	}
	release()
	next, stop := context.WithTimeout(context.Background(), time.Second)
	defer stop()
	if err := b.logicalWriteMu.LockContext(next); err != nil {
		t.Fatal(err)
	}
	b.logicalWriteMu.Unlock()
}

func TestExclusiveLogicalAdmissionCancellation(t *testing.T) {
	calls := map[string]func(*backend, context.Context) error{
		"snapshot_timestamp": func(b *backend, ctx context.Context) error { _, err := b.GetSnapshotTimestamp(ctx); return err },
		"disarm_corrupt":     func(b *backend, ctx context.Context) error { _, err := b.DisarmCorrupt(ctx, 1); return err },
		"quota": func(b *backend, ctx context.Context) error {
			b.config.QuotaBackendBytes = 1
			return b.EnsureQuotaInitialized(ctx)
		},
		"checkpoint":     func(b *backend, ctx context.Context) error { _, err := b.createSerializableCheckpoint(ctx); return err },
		"compact_record": func(b *backend, ctx context.Context) error { _, err := b.setCompactRecord(ctx, 1); return err },
		"hash":           func(b *backend, ctx context.Context) error { _, err := b.Hash(ctx); return err },
		"hashkv":         func(b *backend, ctx context.Context) error { _, err := b.HashKV(ctx, 0); return err },
	}
	for name, call := range calls {
		t.Run(name, func(t *testing.T) {
			b, parent := newTxnApplyBackend(t)
			b.logicalWriteMu.RLock()
			var once sync.Once
			release := func() { once.Do(b.logicalWriteMu.RUnlock) }
			defer release()
			ctx, cancel := context.WithTimeout(parent, 30*time.Millisecond)
			defer cancel()
			done := make(chan error, 1)
			go func() { done <- call(b, ctx) }()
			select {
			case err := <-done:
				if !errors.Is(err, context.DeadlineExceeded) {
					t.Fatalf("got %v", err)
				}
			case <-time.After(500 * time.Millisecond):
				release()
				<-done
				t.Fatal("deadline waited for unrelated shared owner")
			}
			// Cancellation must remove the writer ahead of later readers even while
			// the original shared owner remains active.
			if !b.logicalWriteMu.semaphore().TryAcquire(1) {
				t.Fatal("canceled exclusive waiter retained queue position")
			}
			b.logicalWriteMu.RUnlock()
		})
	}
}

func TestHashKVAdmissionCancellationRetriesJoinedCaller(t *testing.T) {
	b, parent := newTxnApplyBackend(t)
	b.logicalWriteMu.RLock()
	var once sync.Once
	release := func() { once.Do(b.logicalWriteMu.RUnlock) }
	defer release()
	owner, cancelOwner := context.WithCancel(parent)
	defer cancelOwner()
	ownerDone := make(chan error, 1)
	go func() { _, err := b.HashKV(owner, 0); ownerDone <- err }()
	waitLogicalWriterQueued(t, &b.logicalWriteMu)
	key := b.newHashKVFlightKey(parent, 0)
	joined, cancelJoined := context.WithTimeout(parent, 3*time.Second)
	defer cancelJoined()
	joinedDone := make(chan error, 1)
	go func() { _, err := b.HashKV(joined, 0); joinedDone <- err }()
	until := time.Now().Add(time.Second)
	for hashKVFlightWaiters(b, key) < 2 {
		if time.Now().After(until) {
			t.Fatal("second caller did not join")
		}
		time.Sleep(time.Millisecond)
	}
	cancelOwner()
	select {
	case err := <-ownerDone:
		if !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("owner cancellation blocked on shared holder")
	}
	select {
	case err := <-joinedDone:
		t.Fatalf("joined caller inherited canceled owner or bypassed barrier: %v", err)
	default:
	}
	release()
	select {
	case err := <-joinedDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("joined caller did not retry after canceled owner")
	}
	if count := hashKVFlightCount(b); count != 0 {
		t.Fatalf("retained flights: %d", count)
	}
}
