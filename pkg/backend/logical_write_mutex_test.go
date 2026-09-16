package backend

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

func waitLogicalWriterQueued(t *testing.T, m *logicalWriteMutex) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for m.semaphore().TryAcquire(1) {
		m.semaphore().Release(1)
		if time.Now().After(deadline) {
			t.Fatal("writer did not queue")
		}
		time.Sleep(time.Millisecond)
	}
}

func TestRangeTxnCancellationWhileWriterActive(t *testing.T) {
	b := &backend{}
	b.logicalWriteMu.RLock()
	var release sync.Once
	unlockWriter := func() { release.Do(b.logicalWriteMu.RUnlock) }
	defer unlockWriter()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		got, unlock := b.BeginRangeTxn(ctx)
		unlock()
		if got.Value(rangeTxnOwnerKey{}) != nil {
			done <- errors.New("failed admission acquired transaction ownership")
			return
		}
		done <- got.Err()
	}()
	waitLogicalWriterQueued(t, &b.logicalWriteMu)
	cancel()
	select {
	case err := <-done:
		if err != context.Canceled {
			t.Fatalf("got %v", err)
		}
	case <-time.After(time.Second):
		unlockWriter()
		<-done
		t.Fatal("canceled admission waited for unrelated writer")
	}
	// Cancellation must remove the queued writer and unblock later readers,
	// even though the original writer is still active.
	if !b.logicalWriteMu.semaphore().TryAcquire(1) {
		t.Fatal("canceled waiter retained admission")
	}
	b.logicalWriteMu.RUnlock()
	unlockWriter()
	_, unlock := b.BeginRangeTxn(context.Background())
	unlock()
}

func TestRangeTxnRechecksPendingRevisionAfterAdmission(t *testing.T) {
	b := &backend{}
	b.logicalWriteMu.RLock()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		got, unlock := b.BeginRangeTxn(ctx)
		unlock()
		done <- got.Err()
	}()
	waitLogicalWriterQueued(t, &b.logicalWriteMu)
	finish := b.beginPendingRevision()
	defer finish()
	b.logicalWriteMu.RUnlock()
	// A pending revision registered while admission was queued must still be
	// observed after acquiring the exclusive side. Cancellation must leave no
	// lock held by that recheck/wait path.
	select {
	case err := <-done:
		t.Fatalf("crossed unresolved revision: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	cancel()
	select {
	case err := <-done:
		if err != context.Canceled {
			t.Fatalf("got %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("pending revision wait ignored cancellation")
	}
	b.logicalWriteMu.Lock()
	b.logicalWriteMu.Unlock()
	finish()
	_, unlock := b.BeginRangeTxn(context.Background())
	unlock()
}

func TestLogicalWriteMutexQueuedWriterPrecedesNewReader(t *testing.T) {
	var m logicalWriteMutex
	m.RLock()
	writer := make(chan struct{})
	releaseWriter := make(chan struct{})
	writerDone := make(chan struct{})
	go func() {
		m.Lock()
		close(writer)
		<-releaseWriter
		m.Unlock()
		close(writerDone)
	}()
	waitLogicalWriterQueued(t, &m)
	reader := make(chan struct{})
	go func() { m.RLock(); close(reader); m.RUnlock() }()
	m.RUnlock()
	<-writer
	select {
	case <-reader:
		t.Error("reader bypassed exclusive writer")
	default:
	}
	close(releaseWriter)
	<-writerDone
	<-reader
}

func TestLogicalWriteMutexCancelReleaseRace(t *testing.T) {
	for range 200 {
		var m logicalWriteMutex
		m.RLock()
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan error, 1)
		go func() {
			err := m.LockContext(ctx)
			if err == nil {
				m.Unlock()
			}
			done <- err
		}()
		go cancel()
		m.RUnlock()
		err := <-done
		if err != nil && err != context.Canceled {
			t.Fatal(err)
		}
		if err := m.LockContext(context.Background()); err != nil {
			t.Fatal(err)
		}
		m.Unlock()
	}
}

func TestLogicalWriteMutexAlreadyCanceled(t *testing.T) {
	var m logicalWriteMutex
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := m.LockContext(ctx); err != context.Canceled {
		t.Fatalf("got %v", err)
	}
	m.Lock()
	m.Unlock()
}
