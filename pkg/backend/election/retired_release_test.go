package election

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/kubewharf/kubebrain/pkg/storage"
	"github.com/kubewharf/kubebrain/pkg/storage/memkv"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/leaderelection/resourcelock"
)

func retiredReleaseFixture(t *testing.T) (*resourceLock, *resourceLock, storage.KvStorage, retiredOwnership) {
	t.Helper()
	kv := memkv.NewKvStorage()
	t.Cleanup(func() { require.NoError(t, kv.Close()) })
	a := NewResourceLockManager(Config{Prefix: "/release-test", Identity: "old", Timeout: time.Second}, kv).GetResourceLock().(*resourceLock)
	b := NewResourceLockManager(Config{Prefix: "/release-test", Identity: "helper", Timeout: time.Second}, kv).GetResourceLock().(*resourceLock)
	record := resourcelock.LeaderElectionRecord{HolderIdentity: "old", LeaseDurationSeconds: 30, LeaderTransitions: 7}
	require.NoError(t, a.Create(context.Background(), record))
	raw, err := kv.Get(context.Background(), a.electionKey)
	require.NoError(t, err)
	_, token, ok := a.StorageFenceToken(0)
	require.True(t, ok)
	return a, b, kv, retiredOwnership{holder: "old", record: raw, token: token}
}

func retiredReleaseSnapshot(t *testing.T, r *resourceLock) map[string]string {
	t.Helper()
	keys := [][]byte{r.electionKey, r.restorationFenceKey}
	keys = append(keys, r.fenceKeys...)
	keys = append(keys, r.restorationFenceKeys...)
	values := make(map[string]string, len(keys))
	for _, key := range keys {
		value, err := r.store.Get(context.Background(), key)
		require.NoError(t, err)
		values[string(key)] = string(value)
	}
	return values
}

func TestRetiredReleaseFencesOldWritesAndRejectsReplay(t *testing.T) {
	a, b, kv, claim := retiredReleaseFixture(t)
	ctx := context.Background()
	require.NoError(t, b.releaseRetiredOwnership(ctx, claim))
	record, _, err := b.Get(ctx)
	require.NoError(t, err)
	require.Empty(t, record.HolderIdentity)
	require.Equal(t, 7, record.LeaderTransitions)
	for _, key := range a.fenceKeys {
		token, err := kv.Get(ctx, key)
		require.NoError(t, err)
		require.NotEqual(t, claim.token, token)
		staleWrite := kv.BeginBatchWrite()
		staleWrite.CAS(key, claim.token, claim.token, 0)
		require.ErrorIs(t, staleWrite.Commit(ctx), storage.ErrCASFailed)
	}
	// Same holder name reacquires: the token and record, not its name, fence replay.
	_, _, err = a.Get(ctx)
	require.NoError(t, err)
	record.HolderIdentity = a.Identity()
	record.LeaderTransitions++
	record.LeaseDurationSeconds = 30
	require.NoError(t, a.Update(ctx, *record))
	// Deliberately advance the receiver cache to the new record before replay.
	_, _, err = b.Get(ctx)
	require.NoError(t, err)
	before := retiredReleaseSnapshot(t, b)
	require.ErrorIs(t, b.releaseRetiredOwnership(ctx, claim), storage.ErrCASFailed)
	require.Equal(t, before, retiredReleaseSnapshot(t, b))
}

func TestRetiredReleaseRejectsChangedPreconditionsAtomically(t *testing.T) {
	for _, name := range []string{"renewed record", "wrong token", "changed shard", "closed control", "closed shard", "wrong holder", "malformed record", "canceled"} {
		t.Run(name, func(t *testing.T) {
			a, b, kv, claim := retiredReleaseFixture(t)
			ctx := context.Background()
			switch name {
			case "renewed record":
				var record resourcelock.LeaderElectionRecord
				require.NoError(t, json.Unmarshal(claim.record, &record))
				record.RenewTime = metav1.NewTime(time.Now())
				require.NoError(t, a.Update(ctx, record))
			case "wrong token":
				claim.token = []byte(uuid.NewString())
			case "wrong holder":
				claim.holder = "other"
			case "malformed record":
				claim.record = []byte(`{"unknown":true}`)
			case "canceled":
				var cancel context.CancelFunc
				ctx, cancel = context.WithCancel(ctx)
				cancel()
			default:
				key := b.restorationFenceKey
				if name == "closed shard" {
					key = b.restorationFenceKeys[len(b.restorationFenceKeys)-1]
				}
				if name == "changed shard" {
					key = b.fenceKeys[len(b.fenceKeys)-1]
				}
				batch := kv.BeginBatchWrite()
				batch.Put(key, []byte("different"), 0)
				require.NoError(t, batch.Commit(ctx))
			}
			before := retiredReleaseSnapshot(t, b)
			require.Error(t, b.releaseRetiredOwnership(ctx, claim))
			require.Equal(t, before, retiredReleaseSnapshot(t, b), "failed transaction must not partially release or rotate tokens")
		})
	}
}

type releaseLostAckStore struct{ storage.KvStorage }
type releaseLostAckBatch struct{ storage.BatchWrite }

func (s releaseLostAckStore) BeginBatchWrite() storage.BatchWrite {
	return releaseLostAckBatch{s.KvStorage.BeginBatchWrite()}
}

func (b releaseLostAckBatch) Commit(ctx context.Context) error {
	if err := b.BatchWrite.Commit(ctx); err != nil {
		return err
	}
	return storage.ErrUnavailable // committed, but acknowledgement was lost
}

func TestRetiredReleaseLostAcknowledgementCannotReleaseReacquisition(t *testing.T) {
	a, b, kv, claim := retiredReleaseFixture(t)
	ctx := context.Background()
	b.store = releaseLostAckStore{kv}
	require.ErrorIs(t, b.releaseRetiredOwnership(ctx, claim), storage.ErrUnavailable)
	record, _, err := a.Get(ctx)
	require.NoError(t, err)
	require.Empty(t, record.HolderIdentity, "error must not imply the transaction did not commit")
	record.HolderIdentity = a.Identity()
	record.LeaderTransitions++
	record.LeaseDurationSeconds = 30
	require.NoError(t, a.Update(ctx, *record))
	before := retiredReleaseSnapshot(t, b)
	require.ErrorIs(t, b.releaseRetiredOwnership(ctx, claim), storage.ErrCASFailed)
	require.Equal(t, before, retiredReleaseSnapshot(t, b))
}

func TestRetiredReleaseAndAcquisitionHaveOneWinner(t *testing.T) {
	_, b, _, claim := retiredReleaseFixture(t)
	ctx := context.Background()
	record, _, err := b.Get(ctx)
	require.NoError(t, err)
	record.HolderIdentity = b.Identity()
	record.LeaderTransitions++
	start := make(chan struct{})
	released := make(chan error, 1)
	acquired := make(chan error, 1)
	go func() { <-start; released <- b.releaseRetiredOwnership(ctx, claim) }()
	go func() { <-start; acquired <- b.Update(ctx, *record) }()
	close(start)
	releaseErr, acquireErr := <-released, <-acquired
	require.NotEqual(t, releaseErr == nil, acquireErr == nil, "exact record CAS must serialize the competing transactions")
	current, _, err := b.Get(ctx)
	require.NoError(t, err)
	if releaseErr == nil {
		require.ErrorIs(t, acquireErr, storage.ErrCASFailed)
		require.Empty(t, current.HolderIdentity)
		require.Equal(t, record.LeaderTransitions-1, current.LeaderTransitions)
	} else {
		require.ErrorIs(t, releaseErr, storage.ErrCASFailed)
		require.Equal(t, b.Identity(), current.HolderIdentity)
		require.Equal(t, record.LeaderTransitions, current.LeaderTransitions)
	}
	var winnerToken []byte
	for _, key := range b.fenceKeys {
		token, err := b.store.Get(ctx, key)
		require.NoError(t, err)
		require.NotEqual(t, claim.token, token)
		if winnerToken == nil {
			winnerToken = token
		}
		require.Equal(t, winnerToken, token, "all shards must belong to the same winner")
	}
}

func TestOwnershipSnapshotIsLocalAndDetached(t *testing.T) {
	a, b, _, expected := retiredReleaseFixture(t)
	// Any accidental backend access would panic; ownership extraction must work
	// during total backend isolation and must not refresh/renew a record.
	a.store = nil
	claim, ok := a.ownershipSnapshot()
	require.True(t, ok)
	require.Equal(t, expected, claim)
	claim.record[0] = '!'
	claim.token[0] = '!'
	again, ok := a.ownershipSnapshot()
	require.True(t, ok)
	require.Equal(t, expected, again, "returned bytes must not alias the cache")
	_, ok = b.ownershipSnapshot()
	require.False(t, ok, "a follower cannot manufacture its own installed ownership")
}

func TestOwnershipSnapshotAfterPostCommitTSOFailure(t *testing.T) {
	a, b, kv, old := retiredReleaseFixture(t)
	a.store = &postCommitTSOFailureStorage{KvStorage: kv, failTSO: true}
	var record resourcelock.LeaderElectionRecord
	require.NoError(t, json.Unmarshal(old.record, &record))
	record.RenewTime = metav1.NewTime(time.Now())
	require.Error(t, a.Update(context.Background(), record))
	a.store = nil
	claim, ok := a.ownershipSnapshot()
	require.True(t, ok)
	require.NotEqual(t, old.record, claim.record, "postcommit TSO failure must retain the committed CAS value")
	require.NoError(t, b.releaseRetiredOwnership(context.Background(), claim))
}

func TestOwnershipSnapshotDoesNotInventUnacknowledgedAcquisition(t *testing.T) {
	kv := memkv.NewKvStorage()
	t.Cleanup(func() { require.NoError(t, kv.Close()) })
	store := &postCommitTSOFailureStorage{KvStorage: kv, failTSO: true}
	r := NewResourceLockManager(Config{Prefix: "/uncertain-acquisition", Identity: "old", Timeout: time.Second}, store).GetResourceLock().(*resourceLock)
	record := resourcelock.LeaderElectionRecord{HolderIdentity: "old", LeaseDurationSeconds: 30, LeaderTransitions: 1}
	require.Error(t, r.Create(context.Background(), record))
	_, err := kv.Get(context.Background(), r.electionKey)
	require.NoError(t, err, "fixture must have committed before losing confirmation")
	claim, ok := r.ownershipSnapshot()
	require.False(t, ok, "without a locally installed token, fall back to normal election")
	require.Empty(t, claim.record)
	require.Empty(t, claim.token)
}

func TestOwnershipSnapshotRejectsWithdrawnOrInconsistentCache(t *testing.T) {
	for _, name := range []string{"withdrawn", "foreign", "different term", "malformed", "missing token", "invalid token"} {
		t.Run(name, func(t *testing.T) {
			a, _, _, _ := retiredReleaseFixture(t)
			a.mu.Lock()
			switch name {
			case "withdrawn":
				a.fenceInstalled = false
			case "foreign":
				a.record.HolderIdentity = "other"
			case "different term":
				a.record.LeaderTransitions++
			case "malformed":
				a.lastVal = []byte("null")
			case "missing token":
				a.fenceToken = nil
			case "invalid token":
				a.fenceToken = []byte("xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx")
			}
			a.mu.Unlock()
			claim, ok := a.ownershipSnapshot()
			require.False(t, ok)
			require.Empty(t, claim.record)
			require.Empty(t, claim.token)
		})
	}
}
