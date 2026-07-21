package compat

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"
	recipe "go.etcd.io/etcd/client/v3/experimental/recipes"
)

type lockRecipeOutcome struct {
	ReadersOverlap                    bool
	WriterWaitedForReaders            bool
	LateReaderWaitedForWriter         bool
	WriterAcquiredBeforeLateReader    bool
	LateReaderAcquiredAfterWriter     bool
	RWWaitersRemoved                  bool
	SuccessorWaitedAfterVictimExpired bool
	SuccessorAcquiredAfterOwner       bool
	MutexWaitersRemoved               bool
}

func TestLockRecipeDifferentialAgainstReferenceEtcd(t *testing.T) {
	reference := os.Getenv("REFERENCE_ETCD_ENDPOINT")
	if reference == "" {
		t.Skip("set REFERENCE_ETCD_ENDPOINT to run lock recipe differential tests")
	}

	referenceOutcome := runLockRecipeScenario(t, reference, "etcd")
	kubeBrainOutcome := runLockRecipeScenario(t, compatEndpoint(), "kubebrain")
	require.Equal(t, referenceOutcome, kubeBrainOutcome)
	require.Equal(t, lockRecipeOutcome{
		ReadersOverlap:                    true,
		WriterWaitedForReaders:            true,
		LateReaderWaitedForWriter:         true,
		WriterAcquiredBeforeLateReader:    true,
		LateReaderAcquiredAfterWriter:     true,
		RWWaitersRemoved:                  true,
		SuccessorWaitedAfterVictimExpired: true,
		SuccessorAcquiredAfterOwner:       true,
		MutexWaitersRemoved:               true,
	}, kubeBrainOutcome)
}

func runLockRecipeScenario(t *testing.T, endpoint, instance string) lockRecipeOutcome {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	client, err := clientv3.New(clientv3.Config{
		Endpoints: []string{endpoint}, DialTimeout: 3 * time.Second, Context: ctx,
	})
	require.NoError(t, err)
	defer func() { require.NoError(t, client.Close()) }()

	base := fmt.Sprintf("/dbaas-lock-recipes/%s/%d/", instance, time.Now().UnixNano())
	defer func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		_, _ = client.Delete(cleanupCtx, base, clientv3.WithPrefix())
	}()
	newSession := func() *concurrency.Session {
		session, sessionErr := concurrency.NewSession(client, concurrency.WithTTL(10))
		require.NoError(t, sessionErr)
		return session
	}
	closeSession := func(session *concurrency.Session) { _ = session.Close() }

	rwName := base + "rw"
	readerOneSession, readerTwoSession := newSession(), newSession()
	writerSession, lateReaderSession := newSession(), newSession()
	defer closeSession(readerOneSession)
	defer closeSession(readerTwoSession)
	defer closeSession(writerSession)
	defer closeSession(lateReaderSession)
	readerOne := recipe.NewRWMutex(readerOneSession, rwName)
	readerTwo := recipe.NewRWMutex(readerTwoSession, rwName)
	writer := recipe.NewRWMutex(writerSession, rwName)
	lateReader := recipe.NewRWMutex(lateReaderSession, rwName)
	require.NoError(t, readerOne.RLock())
	require.NoError(t, readerTwo.RLock())
	readersOverlap := lockPrefixCount(ctx, client, rwName+"/read") == 2

	writerResult := make(chan error, 1)
	go func() { writerResult <- writer.Lock() }()
	require.Eventually(t, func() bool {
		return lockPrefixCount(ctx, client, rwName+"/write") == 1
	}, 5*time.Second, 20*time.Millisecond)
	time.Sleep(100 * time.Millisecond)
	writerWaited := len(writerResult) == 0

	lateReaderResult := make(chan error, 1)
	go func() { lateReaderResult <- lateReader.RLock() }()
	require.Eventually(t, func() bool {
		return lockPrefixCount(ctx, client, rwName+"/read") == 3
	}, 5*time.Second, 20*time.Millisecond)
	time.Sleep(100 * time.Millisecond)
	lateReaderWaited := len(lateReaderResult) == 0

	require.NoError(t, readerOne.RUnlock())
	require.NoError(t, readerTwo.RUnlock())
	require.NoError(t, waitLockResult(ctx, writerResult, "writer acquisition"))
	writerBeforeLateReader := len(lateReaderResult) == 0
	require.NoError(t, writer.Unlock())
	require.NoError(t, waitLockResult(ctx, lateReaderResult, "late reader acquisition"))
	lateReaderAfterWriter := true
	require.NoError(t, lateReader.RUnlock())
	rwWaitersRemoved := lockPrefixCount(ctx, client, rwName+"/") == 0

	mutexName := base + "mutex"
	ownerSession, victimSession, successorSession := newSession(), newSession(), newSession()
	defer closeSession(ownerSession)
	defer closeSession(successorSession)
	owner := concurrency.NewMutex(ownerSession, mutexName)
	victim := concurrency.NewMutex(victimSession, mutexName)
	successor := concurrency.NewMutex(successorSession, mutexName)
	require.NoError(t, owner.Lock(ctx))
	victimResult := make(chan error, 1)
	go func() { victimResult <- victim.Lock(ctx) }()
	require.Eventually(t, func() bool {
		return lockPrefixCount(ctx, client, mutexName) == 2
	}, 5*time.Second, 20*time.Millisecond)
	successorResult := make(chan error, 1)
	go func() { successorResult <- successor.Lock(ctx) }()
	require.Eventually(t, func() bool {
		return lockPrefixCount(ctx, client, mutexName) == 3
	}, 5*time.Second, 20*time.Millisecond)
	closeSession(victimSession)
	require.Eventually(t, func() bool {
		return lockPrefixCount(ctx, client, mutexName) == 2
	}, 5*time.Second, 20*time.Millisecond)
	time.Sleep(100 * time.Millisecond)
	successorWaited := len(successorResult) == 0
	require.NoError(t, owner.Unlock(ctx))
	require.NoError(t, waitLockResult(ctx, successorResult, "successor mutex acquisition"))
	successorAfterOwner := true
	require.NoError(t, successor.Unlock(ctx))
	mutexWaitersRemoved := lockPrefixCount(ctx, client, mutexName) == 0
	select {
	case <-victimResult:
	case <-ctx.Done():
		t.Fatalf("expired middle waiter did not exit: %v", ctx.Err())
	}

	return lockRecipeOutcome{
		ReadersOverlap:                    readersOverlap,
		WriterWaitedForReaders:            writerWaited,
		LateReaderWaitedForWriter:         lateReaderWaited,
		WriterAcquiredBeforeLateReader:    writerBeforeLateReader,
		LateReaderAcquiredAfterWriter:     lateReaderAfterWriter,
		RWWaitersRemoved:                  rwWaitersRemoved,
		SuccessorWaitedAfterVictimExpired: successorWaited,
		SuccessorAcquiredAfterOwner:       successorAfterOwner,
		MutexWaitersRemoved:               mutexWaitersRemoved,
	}
}

func lockPrefixCount(ctx context.Context, client *clientv3.Client, prefix string) int {
	response, err := client.Get(ctx, prefix, clientv3.WithPrefix())
	if err != nil {
		return -1
	}
	return len(response.Kvs)
}

func waitLockResult(ctx context.Context, result <-chan error, phase string) error {
	select {
	case err := <-result:
		return err
	case <-ctx.Done():
		return fmt.Errorf("%s did not complete: %w", phase, ctx.Err())
	}
}
