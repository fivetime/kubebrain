package tikv

import (
	"context"
	"crypto/rand"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/kubewharf/kubebrain/pkg/storage"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/stretchr/testify/require"
	tikvconfig "github.com/tikv/client-go/v2/config"
	clienttikv "github.com/tikv/client-go/v2/tikv"
	"github.com/tikv/client-go/v2/tikvrpc"
)

// This test-only transport holds every Commit for one transaction, including
// its primary. The production command still disables async commit. This
// disposable single-store fixture is not a Raft failure or performance proof.
type protocolAsyncCommitHold struct {
	clienttikv.Client
	startTS                                                      uint64
	held, release                                                chan struct{}
	once                                                         sync.Once
	prewrites, asyncResponses, commitsForwarded, secondaryChecks atomic.Int32
	regionErrors                                                 atomic.Int32
	asyncAttempts                                                atomic.Int32
	regionsMu                                                    sync.Mutex
	asyncRegions                                                 map[uint64]struct{}
}

func (c *protocolAsyncCommitHold) SendRequest(ctx context.Context, addr string, req *tikvrpc.Request, timeout time.Duration) (*tikvrpc.Response, error) {
	markedPrewrite := req.Type == tikvrpc.CmdPrewrite && req.Prewrite().StartVersion == c.startTS
	if markedPrewrite {
		c.prewrites.Add(1)
		if req.Prewrite().UseAsyncCommit {
			c.asyncAttempts.Add(1)
		}
	}
	if req.Type == tikvrpc.CmdCommit && req.Commit().StartVersion == c.startTS {
		c.once.Do(func() { close(c.held) })
		timer := time.NewTimer(10 * time.Second)
		defer timer.Stop()
		select {
		case <-c.release:
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-timer.C:
			return nil, context.DeadlineExceeded
		}
		c.commitsForwarded.Add(1)
	}
	if req.Type == tikvrpc.CmdCheckSecondaryLocks && req.CheckSecondaryLocks().StartVersion == c.startTS {
		c.secondaryChecks.Add(1)
	}
	response, err := c.Client.SendRequest(ctx, addr, req, timeout)
	if markedPrewrite && err == nil && response != nil {
		if r, ok := response.Resp.(*kvrpcpb.PrewriteResponse); ok && r != nil {
			if r.RegionError != nil {
				c.regionErrors.Add(1)
			} else if req.Prewrite().UseAsyncCommit && len(r.Errors) == 0 && r.MinCommitTs > c.startTS {
				c.asyncResponses.Add(1)
				c.regionsMu.Lock()
				c.asyncRegions[req.Context.RegionId] = struct{}{}
				c.regionsMu.Unlock()
			}
		}
	}
	return response, err
}

func TestRealTiKVAsyncExperimentLeadershipConflict(t *testing.T) {
	testRealTiKVBackendScenario(t, "async-fenced-election-fence")
}

func TestRealTiKVAsyncExperimentRestorationConflict(t *testing.T) {
	testRealTiKVBackendScenario(t, "async-fenced-restoration-fence-shard")
}

func TestRealTiKVAsyncExperimentReadsBeforeCommitCleanup(t *testing.T) {
	pd := os.Getenv("KUBEBRAIN_TIKV_PROTOCOL_PD")
	if pd == "" || os.Getenv("KUBEBRAIN_TIKV_PROTOCOL_ASYNC_EXPERIMENT") != "1" {
		t.Skip("explicit disposable protocol fixture and async experiment consent required")
	}
	prefix := os.Getenv("KUBEBRAIN_TIKV_PROTOCOL_PREFIX")
	expected, err := validateProtocolSmokeScope(os.Getenv("KUBEBRAIN_TIKV_PROTOCOL_CLUSTER_ID"), prefix, os.Getenv("KUBEBRAIN_TIKV_PROTOCOL_MODE"))
	require.NoError(t, err)
	require.Equal(t, "2pc", os.Getenv("KUBEBRAIN_TIKV_PROTOCOL_MODE"), "fixture initialization and cleanup remain 2PC")
	require.Equal(t, "1", os.Getenv("KUBEBRAIN_TIKV_PROTOCOL_ALLOW_REGION_SPLIT"))
	t.Cleanup(tikvconfig.UpdateGlobal(func(c *tikvconfig.Config) { c.Enable1PC = false; c.EnableAsyncCommit = false }))
	ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
	defer cancel()
	kv, err := NewKvStorageWithContext(ctx, strings.Split(pd, ","), 1, Security{})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, kv.Close()) })
	require.Equal(t, expected, kv.(storage.ClusterIdentifier).ClusterID())
	require.NoError(t, protocolSmokePrefixEmpty(ctx, kv, prefix))
	owner := make([]byte, 32)
	_, err = rand.Read(owner)
	require.NoError(t, err)
	t.Cleanup(func() {
		cleanupCtx, stop := context.WithTimeout(context.Background(), 30*time.Second)
		defer stop()
		require.NoError(t, cleanupProtocolSmoke(cleanupCtx, kv, prefix, owner))
	})
	claim := kv.BeginBatchWrite()
	claim.PutIfNotExist([]byte(prefix+"owner"), owner, 0)
	require.NoError(t, claim.Commit(ctx))
	client := kv.(*store).getClient()
	data, witness := []byte(prefix+"data"), []byte(prefix+"witness")
	_, err = client.SplitRegions(ctx, [][]byte{witness}, false, nil)
	require.NoError(t, err)
	txn, err := client.BeginWithContext(ctx)
	require.NoError(t, err)
	// Only explicit experiment transactions opt in. Do not change global defaults or expose a
	// product flag based on this experiment.
	txn.SetEnable1PC(false)
	txn.SetEnableAsyncCommit(true)
	hold := &protocolAsyncCommitHold{Client: client.GetTiKVClient(), startTS: txn.StartTS(), held: make(chan struct{}), release: make(chan struct{}), asyncRegions: make(map[uint64]struct{})}
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(hold.release) }) }
	t.Cleanup(release) // unblock SDK workers before closing the store
	client.SetTiKVClient(hold)
	b := &batch{txn: txn}
	b.Put(data, []byte("left"), 0)
	b.Put(witness, []byte("right"), 0)
	var observations []storage.BatchCommitObservation
	err = b.Commit(storage.WithBatchCommitObserver(ctx, func(o storage.BatchCommitObservation) {
		observations = append(observations, o)
	}))
	require.NoError(t, err, "async batch must return while all Commit RPCs are held")
	select {
	case <-hold.held:
	case <-ctx.Done():
		t.Fatal("background Commit never reached the hold")
	}
	// Region splitting can invalidate cached routing. Account for those replies
	// separately instead of mistaking RPC attempts for participating Regions.
	require.Equal(t, hold.asyncResponses.Load()+hold.regionErrors.Load(), hold.prewrites.Load())
	require.GreaterOrEqual(t, hold.asyncResponses.Load(), int32(2))
	hold.regionsMu.Lock()
	regionCount := len(hold.asyncRegions)
	hold.regionsMu.Unlock()
	require.Equal(t, 2, regionCount, "both Regions must accept async commit, not fall back to 2PC")
	require.Zero(t, hold.commitsForwarded.Load())
	require.Len(t, observations, 1)
	o := observations[0]
	require.NoError(t, o.Err)
	require.True(t, o.CommitAttempted)
	require.True(t, o.HasPrewriteRPCDetails)
	require.EqualValues(t, hold.prewrites.Load(), o.PrewriteRPCs.Requests)
	require.Zero(t, o.PrewriteRPCs.TransportErrors)
	require.EqualValues(t, hold.regionErrors.Load(), o.PrewriteRPCs.RegionErrors)
	require.Zero(t, o.PrewriteRPCs.KeyErrors)
	require.Zero(t, o.PrewriteRPCs.MissingResponses)
	require.Zero(t, o.PrimaryWrite.SuccessfulRPCs, "background primary cleanup must not appear as foreground primary commit")
	for key, want := range map[string]string{string(data): "left", string(witness): "right"} {
		got, err := kv.Get(ctx, []byte(key))
		require.NoError(t, err)
		require.Equal(t, want, string(got))
	}
	require.Positive(t, hold.secondaryChecks.Load(), "reader must check pending async secondary locks")
	require.Zero(t, hold.commitsForwarded.Load(), "reads must complete without releasing background Commit")
	release()
	require.Eventually(t, func() bool { return hold.commitsForwarded.Load() > 0 }, 5*time.Second, time.Millisecond)
	require.Equal(t, o, observations[0], "late cleanup cannot mutate the delivered observation")
	t.Log("PROTOCOL_ASYNC_EXPERIMENT_OK regions=2 primary_and_secondary_commit_held=true read_visible=true production_enablement=false")

	// The CAS sees its old snapshot, not the winner's later value. Therefore
	// this must fail in real Prewrite, not merely in the local CAS comparison.
	stale, err := client.BeginWithContext(ctx)
	require.NoError(t, err)
	stale.SetEnable1PC(false)
	stale.SetEnableAsyncCommit(true)
	old, err := stale.Get(ctx, data)
	require.NoError(t, err)
	require.Equal(t, "left", string(old))
	winner := kv.BeginBatchWrite()
	winner.Put(data, []byte("winner"), 0)
	require.NoError(t, winner.Commit(ctx))
	conflictRPC := &protocolAsyncCommitHold{Client: client.GetTiKVClient(), startTS: stale.StartTS(),
		held: make(chan struct{}), release: make(chan struct{}), asyncRegions: make(map[uint64]struct{})}
	close(conflictRPC.release) // do not hide an erroneous commit by blocking it
	client.SetTiKVClient(conflictRPC)
	loser := &batch{txn: stale}
	loser.CAS(data, []byte("loser"), []byte("left"), 0)
	loser.Put(witness, []byte("must-not-publish"), 0)
	var failed []storage.BatchCommitObservation
	err = loser.Commit(storage.WithBatchCommitObserver(ctx, func(o storage.BatchCommitObservation) {
		failed = append(failed, o)
	}))
	require.ErrorIs(t, err, storage.ErrCASFailed)
	require.Positive(t, conflictRPC.asyncAttempts.Load(), "conflict must reach async Prewrite")
	require.Len(t, failed, 1)
	require.True(t, failed[0].CommitAttempted)
	require.ErrorIs(t, failed[0].Err, storage.ErrCASFailed)
	require.True(t, failed[0].HasPrewriteRPCDetails)
	require.Positive(t, failed[0].PrewriteRPCs.KeyErrors)
	require.Zero(t, conflictRPC.commitsForwarded.Load())
	for key, want := range map[string]string{string(data): "winner", string(witness): "right"} {
		got, err := kv.Get(ctx, []byte(key))
		require.NoError(t, err)
		require.Equal(t, want, string(got), "failed async batch must not publish either mutation")
	}
	t.Log("PROTOCOL_ASYNC_CONFLICT_OK stale_snapshot=true prewrite_conflict=true sibling_write_unpublished=true")
}
