package election

import (
	"context"
	"io"
	"os"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/kubewharf/kubebrain/pkg/backend/restorationfence"
	"github.com/kubewharf/kubebrain/pkg/storage"
	"github.com/kubewharf/kubebrain/pkg/storage/tikv"
	"github.com/stretchr/testify/require"
	tikvconfig "github.com/tikv/client-go/v2/config"
	"k8s.io/client-go/tools/leaderelection/resourcelock"
)

// Run only through the disposable local protocol fixture. This exercises the
// scoped storage capability, not holder authentication or lease continuity.
func TestRealTiKVRetiredRelease(t *testing.T) {
	pd := os.Getenv("KUBEBRAIN_TIKV_PROTOCOL_PD")
	if pd == "" {
		t.Skip("explicit disposable protocol fixture required")
	}
	prefix := os.Getenv("KUBEBRAIN_TIKV_PROTOCOL_PREFIX")
	require.Regexp(t, regexp.MustCompile(`^kubebrain/protocol-smoke/[0-9a-f]{32}/$`), prefix)
	require.Equal(t, "2pc", os.Getenv("KUBEBRAIN_TIKV_PROTOCOL_MODE"))
	expected, err := strconv.ParseUint(os.Getenv("KUBEBRAIN_TIKV_PROTOCOL_CLUSTER_ID"), 10, 64)
	require.NoError(t, err)
	require.NotZero(t, expected)
	t.Cleanup(tikvconfig.UpdateGlobal(func(c *tikvconfig.Config) {
		c.Enable1PC, c.EnableAsyncCommit = false, false
	}))
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	kv, err := tikv.NewKvStorageWithContext(ctx, strings.Split(pd, ","), 1, tikv.Security{})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, kv.Close()) })
	require.Equal(t, expected, kv.(storage.ClusterIdentifier).ClusterID(), "wrong cluster: refuse writes")
	it, err := kv.Iter(ctx, []byte(prefix), []byte(strings.TrimSuffix(prefix, "/")+"0"), 0, 1)
	require.NoError(t, err)
	firstErr := it.Next(ctx)
	require.NoError(t, it.Close())
	require.ErrorIs(t, firstErr, io.EOF, "refuse an occupied test prefix")
	ownerKey, owner := []byte(prefix+"owner"), []byte(uuid.NewString())
	initial := kv.BeginBatchWrite()
	initial.PutIfNotExist(ownerKey, owner, 0)
	require.NoError(t, initial.Commit(ctx))
	a := NewResourceLockManager(Config{Prefix: prefix + "lock", Identity: "old", Timeout: 10 * time.Second}, kv).GetResourceLock().(*resourceLock)
	b := NewResourceLockManager(Config{Prefix: prefix + "lock", Identity: "helper", Timeout: 10 * time.Second}, kv).GetResourceLock().(*resourceLock)
	scope := b.RetirementScope()
	require.NotEmpty(t, scope)
	require.Equal(t, a.RetirementScope(), scope)
	release := func(ctx context.Context, claim retiredOwnership) error {
		return b.ReleaseRetiredOwnership(ctx, scope, OwnershipCondition{claim: claim})
	}
	payload := []byte(prefix + "payload")
	t.Cleanup(func() {
		cleanupCtx, stop := context.WithTimeout(context.Background(), 20*time.Second)
		defer stop()
		cleanup := kv.BeginBatchWrite()
		cleanup.CAS(ownerKey, owner, owner, 0)
		for _, key := range append(append([][]byte{a.electionKey, a.restorationFenceKey, payload}, a.fenceKeys...), a.restorationFenceKeys...) {
			cleanup.Del(key)
		}
		cleanup.Del(ownerKey)
		require.NoError(t, cleanup.Commit(cleanupCtx), "only delete exact keys under the original owner condition")
	})
	require.NoError(t, a.Create(ctx, resourcelock.LeaderElectionRecord{HolderIdentity: "old", LeaseDurationSeconds: 30, LeaderTransitions: 7}))
	claim, ok := a.ownershipSnapshot()
	require.True(t, ok)
	before := retiredReleaseSnapshot(t, a)
	require.Error(t, b.ReleaseRetiredOwnership(ctx, "wrong-instance", OwnershipCondition{claim: claim}))
	require.Equal(t, before, retiredReleaseSnapshot(t, a))
	wrong := claim
	wrong.token = []byte(uuid.NewString())
	require.ErrorIs(t, release(ctx, wrong), storage.ErrCASFailed)
	require.Equal(t, before, retiredReleaseSnapshot(t, a))
	gate := a.restorationFenceKeys[len(a.restorationFenceKeys)-1]
	closed := kv.BeginBatchWrite()
	closed.Put(gate, []byte("closed"), 0)
	require.NoError(t, closed.Commit(ctx))
	before = retiredReleaseSnapshot(t, a)
	require.ErrorIs(t, release(ctx, claim), storage.ErrCASFailed)
	require.Equal(t, before, retiredReleaseSnapshot(t, a), "restoration refusal must not partially rotate tokens")
	opened := kv.BeginBatchWrite()
	opened.Put(gate, []byte(restorationfence.Open), 0)
	require.NoError(t, opened.Commit(ctx))

	// Pause a REAL transaction after its ownership CAS read/staging, not merely
	// after constructing a lazy batch. Release must conflict with its commit.
	staged, resume, done := make(chan struct{}), make(chan struct{}), make(chan error, 1)
	stale := kv.BeginBatchWrite()
	stale.CAS(a.fenceKeys[0], claim.token, claim.token, 0)
	stale.Put(payload, []byte("must-not-commit"), 0)
	stale.Atomic(func(ctx context.Context, _ storage.AtomicBatch) error {
		close(staged)
		select {
		case <-resume:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	})
	go func() { done <- stale.Commit(ctx) }()
	select {
	case <-staged:
	case err := <-done:
		t.Fatalf("old transaction failed before staging: %v", err)
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	releaseErr := release(ctx, claim)
	close(resume)
	staleErr := <-done
	require.NoError(t, releaseErr)
	require.ErrorIs(t, staleErr, storage.ErrCASFailed, "old transaction must fail on write conflict, not an unrelated timeout")
	_, err = kv.Get(ctx, payload)
	require.ErrorIs(t, err, storage.ErrKeyNotFound)
	for _, key := range a.fenceKeys {
		token, err := kv.Get(ctx, key)
		require.NoError(t, err)
		require.NotEqual(t, claim.token, token)
	}
	record, _, err := a.Get(ctx)
	require.NoError(t, err)
	require.Empty(t, record.HolderIdentity)
	record.HolderIdentity, record.LeaseDurationSeconds = "old", 30
	record.LeaderTransitions++
	require.NoError(t, a.Update(ctx, *record))
	before = retiredReleaseSnapshot(t, a)
	require.ErrorIs(t, release(ctx, claim), storage.ErrCASFailed)
	require.Equal(t, before, retiredReleaseSnapshot(t, a), "replay must preserve same-holder reacquisition")
}
