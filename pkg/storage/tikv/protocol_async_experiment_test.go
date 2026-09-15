package tikv

import (
	"bytes"
	"context"
	"crypto/rand"
	"errors"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	proto "github.com/kubewharf/kubebrain-client/api/v2rpc"
	"github.com/kubewharf/kubebrain/pkg/backend"
	"github.com/kubewharf/kubebrain/pkg/storage"
	"github.com/pingcap/kvproto/pkg/errorpb"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/stretchr/testify/require"
	tikvconfig "github.com/tikv/client-go/v2/config"
	clienttikv "github.com/tikv/client-go/v2/tikv"
	"github.com/tikv/client-go/v2/tikvrpc"
)

// This test-only transport holds every Commit for one transaction, including
// its primary. The production command disables async commit by default. This
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

type protocolProcessAsyncRPC struct {
	clienttikv.Client
	successes, unmarkedAsync atomic.Int32
}

func (c *protocolProcessAsyncRPC) SendRequest(ctx context.Context, addr string, req *tikvrpc.Request, timeout time.Duration) (*tikvrpc.Response, error) {
	response, err := c.Client.SendRequest(ctx, addr, req, timeout)
	if req.Type == tikvrpc.CmdPrewrite && err == nil && response != nil {
		if r, ok := response.Resp.(*kvrpcpb.PrewriteResponse); ok && r != nil && r.RegionError == nil && len(r.Errors) == 0 {
			c.successes.Add(1)
			if req.Prewrite().UseAsyncCommit && r.MinCommitTs > req.Prewrite().StartVersion && ctx.Value(protocolLatencyMarker{}) != true {
				c.unmarkedAsync.Add(1)
			}
		}
	}
	return response, err
}

// Lose a real successful async Prewrite response for one exact transaction.
// Cancellation prevents a subsequent retry from confirming it to the caller.
type protocolAsyncResponseLoss struct {
	clienttikv.Client
	startTS   uint64
	cancel    context.CancelFunc
	drops     atomic.Int32
	mutations atomic.Int32
}

// Accept one real Prewrite group, then fail the other before delivery. Test
// both missing-primary and missing-secondary outcomes, not lost success.
type protocolAsyncPartialLoss struct {
	clienttikv.Client
	startTS                                    uint64
	cancel                                     context.CancelFunc
	peerReady                                  chan struct{}
	acceptPrimary                              bool
	once                                       sync.Once
	secondaryAccepted, primaryBlocked, commits atomic.Int32
	primaryAccepted, secondaryBlocked          atomic.Int32
}

func (c *protocolAsyncPartialLoss) SendRequest(ctx context.Context, addr string, req *tikvrpc.Request, timeout time.Duration) (*tikvrpc.Response, error) {
	if req.Type == tikvrpc.CmdCommit && req.Commit().StartVersion == c.startTS {
		c.commits.Add(1)
	}
	marked := req.Type == tikvrpc.CmdPrewrite && req.Prewrite().StartVersion == c.startTS && req.Prewrite().UseAsyncCommit
	if !marked {
		return c.Client.SendRequest(ctx, addr, req, timeout)
	}
	primary := false
	for _, mutation := range req.Prewrite().Mutations {
		primary = primary || bytes.Equal(mutation.Key, req.Prewrite().PrimaryLock)
	}
	if primary != c.acceptPrimary {
		timer := time.NewTimer(10 * time.Second)
		defer timer.Stop()
		select {
		case <-c.peerReady:
			if primary {
				c.primaryBlocked.Add(1)
			} else {
				c.secondaryBlocked.Add(1)
			}
			c.cancel()
			return nil, errors.New("injected async Prewrite failure before delivery after peer acceptance")
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-timer.C:
			return nil, context.DeadlineExceeded
		}
	}
	response, err := c.Client.SendRequest(ctx, addr, req, timeout)
	if err == nil && response != nil && req.Prewrite().UseAsyncCommit {
		if r, ok := response.Resp.(*kvrpcpb.PrewriteResponse); ok && r != nil && r.RegionError == nil && len(r.Errors) == 0 && r.MinCommitTs > c.startTS {
			if primary {
				c.primaryAccepted.Add(1)
			} else {
				c.secondaryAccepted.Add(1)
			}
			c.once.Do(func() { close(c.peerReady) })
		}
	}
	return response, err
}

func TestProtocolAsyncPartialLossScope(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	accepted := &tikvrpc.Response{Resp: &kvrpcpb.PrewriteResponse{MinCommitTs: 20}}
	stub := &protocolResponseStub{response: accepted}
	loss := &protocolAsyncPartialLoss{Client: stub, startTS: 10, cancel: cancel, peerReady: make(chan struct{})}
	prewrite := func(start uint64, async, primary bool) *tikvrpc.Request {
		key := []byte("s")
		if primary {
			key = []byte("p")
		}
		return tikvrpc.NewRequest(tikvrpc.CmdPrewrite, &kvrpcpb.PrewriteRequest{
			StartVersion: start, UseAsyncCommit: async, PrimaryLock: []byte("p"), Mutations: []*kvrpcpb.Mutation{{Key: key}},
		})
	}
	for _, req := range []*tikvrpc.Request{prewrite(11, true, false), prewrite(10, false, false)} {
		got, err := loss.SendRequest(ctx, "unused", req, time.Second)
		require.NoError(t, err)
		require.Same(t, accepted, got)
		require.Zero(t, loss.secondaryAccepted.Load())
	}
	for _, response := range []*tikvrpc.Response{
		nil,
		{Resp: &kvrpcpb.PrewriteResponse{}},
		{Resp: &kvrpcpb.PrewriteResponse{MinCommitTs: 20, RegionError: &errorpb.Error{Message: "route changed"}}},
		{Resp: &kvrpcpb.PrewriteResponse{MinCommitTs: 20, Errors: []*kvrpcpb.KeyError{{Abort: "conflict"}}}},
	} {
		stub.response = response
		got, err := loss.SendRequest(ctx, "unused", prewrite(10, true, false), time.Second)
		require.NoError(t, err)
		require.Equal(t, response, got)
		require.Zero(t, loss.secondaryAccepted.Load())
	}
	stub.response = accepted
	stub.err = errors.New("original transport failure")
	_, err := loss.SendRequest(ctx, "unused", prewrite(10, true, false), time.Second)
	require.ErrorIs(t, err, stub.err)
	require.Zero(t, loss.secondaryAccepted.Load())
	stub.err = nil
	_, err = loss.SendRequest(ctx, "unused", prewrite(10, true, false), time.Second)
	require.NoError(t, err)
	require.EqualValues(t, 1, loss.secondaryAccepted.Load())
	require.NoError(t, ctx.Err())
	// Even after the barrier opens, ordinary 2PC must not consume an async fault.
	got, err := loss.SendRequest(ctx, "unused", prewrite(10, false, true), time.Second)
	require.NoError(t, err)
	require.Same(t, accepted, got)
	require.NoError(t, ctx.Err())
	calls := stub.calls
	got, err = loss.SendRequest(ctx, "unused", prewrite(10, true, true), time.Second)
	require.Nil(t, got)
	require.ErrorContains(t, err, "before delivery")
	require.ErrorIs(t, ctx.Err(), context.Canceled)
	require.Equal(t, calls, stub.calls, "blocked primary must not reach transport")
	require.EqualValues(t, 1, loss.primaryBlocked.Load())
}

func (c *protocolAsyncResponseLoss) SendRequest(ctx context.Context, addr string, req *tikvrpc.Request, timeout time.Duration) (*tikvrpc.Response, error) {
	response, err := c.Client.SendRequest(ctx, addr, req, timeout)
	if req.Type != tikvrpc.CmdPrewrite || req.Prewrite().StartVersion != c.startTS || !req.Prewrite().UseAsyncCommit || err != nil || response == nil {
		return response, err
	}
	r, ok := response.Resp.(*kvrpcpb.PrewriteResponse)
	if !ok || r == nil || r.RegionError != nil || len(r.Errors) != 0 || r.MinCommitTs <= c.startTS {
		return response, err
	}
	if c.drops.CompareAndSwap(0, 1) {
		c.mutations.Store(int32(len(req.Prewrite().Mutations)))
		c.cancel()
		return nil, errors.New("injected loss of real successful async Prewrite response")
	}
	return response, err
}

func TestProtocolAsyncResponseLossScope(t *testing.T) {
	for _, tc := range []struct {
		name             string
		start, minCommit uint64
		async, drop      bool
	}{
		{"other-transaction", 11, 20, true, false},
		{"ordinary-2pc", 10, 20, false, false},
		{"async-fallback", 10, 0, true, false},
		{"invalid-minimum", 10, 10, true, false},
		{"accepted-async", 10, 20, true, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			response := &tikvrpc.Response{Resp: &kvrpcpb.PrewriteResponse{MinCommitTs: tc.minCommit}}
			stub := &protocolResponseStub{response: response}
			loss := &protocolAsyncResponseLoss{Client: stub, startTS: 10, cancel: cancel}
			req := tikvrpc.NewRequest(tikvrpc.CmdPrewrite, &kvrpcpb.PrewriteRequest{StartVersion: tc.start, UseAsyncCommit: tc.async})
			got, err := loss.SendRequest(ctx, "unused", req, time.Second)
			if tc.drop {
				require.Nil(t, got)
				require.ErrorContains(t, err, "injected loss")
				require.ErrorIs(t, ctx.Err(), context.Canceled)
				require.EqualValues(t, 1, loss.drops.Load())
			} else {
				require.NoError(t, err)
				require.Same(t, response, got)
				require.NoError(t, ctx.Err())
				require.Zero(t, loss.drops.Load())
			}
			// This fault is one-shot; it must never fabricate a retry response.
			got, err = loss.SendRequest(context.Background(), "unused", req, time.Second)
			require.NoError(t, err)
			require.Same(t, response, got)
			require.Equal(t, 2, stub.calls)
		})
	}
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

func TestRealTiKVAsyncExperimentBackendPublication(t *testing.T) {
	testRealTiKVBackendScenario(t, "async-fenced")
}

func TestRealTiKVAsyncExperimentBackendResponseLoss(t *testing.T) {
	testRealTiKVBackendScenario(t, "async-committed")
}

func TestRealTiKVAsyncExperimentGuardedResponseLoss(t *testing.T) {
	testRealTiKVBackendScenario(t, "async-guarded-committed")
}

func TestRealTiKVAsyncExperimentProcessDefaults(t *testing.T) {
	testRealTiKVBackendScenario(t, "async-process-fenced")
}

func verifyAsyncBackendResolution(t *testing.T, ctx context.Context, b backend.Backend, metrics *protocolResolutionMetrics, watch <-chan []*proto.Event, left, right []byte) {
	t.Helper()
	require.Eventually(t, func() bool { return metrics.committed.Load() == 1 && b.GetCurrentRevision() == 101 }, 10*time.Second, 10*time.Millisecond)
	require.Zero(t, metrics.absent.Load())
	events := protocolNextMutation(t, ctx, watch)
	require.Len(t, events, 2)
	seen := make(map[string]string)
	for _, event := range events {
		require.Equal(t, proto.Event_CREATE, event.Type)
		require.EqualValues(t, 101, event.Revision)
		require.EqualValues(t, 101, event.Kv.Revision)
		seen[string(event.Kv.Key)] = string(backend.StripInlineValue(event.Kv.Value))
	}
	require.Equal(t, map[string]string{string(left): "left-value", string(right): "right-value"}, seen)
	for key, value := range seen {
		got, err := b.Get(ctx, &proto.GetRequest{Key: []byte(key)})
		require.NoError(t, err)
		require.NotNil(t, got.Kv)
		require.EqualValues(t, 101, got.Kv.Revision)
		require.Equal(t, value, string(backend.StripInlineValue(got.Kv.Value)))
	}
	_, revision, err := b.TxnApply(ctx, []backend.TxnWriteOp{{Key: left, Value: []byte("next")}}, nil)
	require.NoError(t, err)
	require.EqualValues(t, 102, revision)
	next := protocolNextMutation(t, ctx, watch)
	require.Len(t, next, 1, "resolved transaction must not be published twice")
	require.Equal(t, proto.Event_PUT, next[0].Type)
	require.EqualValues(t, 102, next[0].Revision)
	require.Equal(t, left, next[0].Kv.Key)
	require.Equal(t, []byte("next"), backend.StripInlineValue(next[0].Kv.Value))
	require.EqualValues(t, 1, metrics.committed.Load())
	require.Zero(t, metrics.absent.Load())
	t.Log("PROTOCOL_ASYNC_BACKEND_RESOLVED_OK committed=1 absent=0 revision=101 next=102 watch_events=2")
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
	// Only explicit experiment transactions opt in here. These checks do not
	// establish whole-process protocol coverage or production readiness.
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

	// Both keys are below the earlier split boundary. Require both mutations in
	// the lost successful Prewrite so partial multi-Region prewrite cannot be
	// mistaken for a transaction that was already logically committed.
	lostTxn, err := client.BeginWithContext(ctx)
	require.NoError(t, err)
	lostTxn.SetEnable1PC(false)
	lostTxn.SetEnableAsyncCommit(true)
	lossCtx, cancelLoss := context.WithCancel(ctx)
	defer cancelLoss()
	loss := &protocolAsyncResponseLoss{Client: client.GetTiKVClient(), startTS: lostTxn.StartTS(), cancel: cancelLoss}
	client.SetTiKVClient(loss)
	other := []byte(prefix + "data-loss")
	// Registered after the original fixed-key cleanup, so this exact extra key
	// is deleted first while the owner claim still exists. Never broaden the
	// shared smoke cleanup's deletion set or drop its empty-prefix assertion.
	t.Cleanup(func() {
		cleanupCtx, stop := context.WithTimeout(context.Background(), 5*time.Second)
		defer stop()
		cleanup := kv.BeginBatchWrite()
		cleanup.CAS([]byte(prefix+"owner"), owner, owner, 0)
		cleanup.Del(other)
		require.NoError(t, cleanup.Commit(cleanupCtx))
	})
	lostBatch := &batch{txn: lostTxn}
	lostBatch.Put(data, []byte("after-loss"), 0)
	lostBatch.Put(other, []byte("paired-after-loss"), 0)
	err = lostBatch.Commit(lossCtx)
	require.ErrorIs(t, err, storage.ErrUncertainResult)
	require.ErrorIs(t, lossCtx.Err(), context.Canceled)
	require.EqualValues(t, 1, loss.drops.Load())
	require.EqualValues(t, 2, loss.mutations.Load(), "both mutations must be accepted in the lost reply")
	for key, want := range map[string]string{string(data): "after-loss", string(other): "paired-after-loss"} {
		got, err := kv.Get(ctx, []byte(key))
		require.NoError(t, err)
		require.Equal(t, want, string(got), "lost success response must not roll back committed async data")
	}
	t.Log("PROTOCOL_ASYNC_RESPONSE_LOSS_OK uncertain=true both_mutations_visible=true caller_cancelled=true")

	for _, acceptPrimary := range []bool{false, true} {
		partialTxn, err := client.BeginWithContext(ctx)
		require.NoError(t, err)
		partialTxn.SetEnable1PC(false)
		partialTxn.SetEnableAsyncCommit(true)
		partialCtx, cancelPartial := context.WithCancel(ctx)
		defer cancelPartial()
		partial := &protocolAsyncPartialLoss{Client: client.GetTiKVClient(), startTS: partialTxn.StartTS(), cancel: cancelPartial, peerReady: make(chan struct{}), acceptPrimary: acceptPrimary}
		client.SetTiKVClient(partial)
		partialBatch := &batch{txn: partialTxn}
		partialBatch.Put(data, []byte("must-not-publish-primary"), 0)
		partialBatch.Put(witness, []byte("must-not-publish-secondary"), 0)
		err = partialBatch.Commit(partialCtx)
		require.ErrorIs(t, err, storage.ErrUncertainResult)
		require.ErrorIs(t, partialCtx.Err(), context.Canceled)
		if acceptPrimary {
			require.Positive(t, partial.primaryAccepted.Load(), "real primary must accept async before secondary is dropped")
			require.Positive(t, partial.secondaryBlocked.Load())
			require.Zero(t, partial.secondaryAccepted.Load())
		} else {
			require.Positive(t, partial.secondaryAccepted.Load(), "real secondary must accept async before primary is dropped")
			require.Positive(t, partial.primaryBlocked.Load())
			require.Zero(t, partial.primaryAccepted.Load())
		}
		require.Zero(t, partial.commits.Load())
		for key, want := range map[string]string{string(data): "after-loss", string(witness): "right"} {
			got, err := kv.Get(ctx, []byte(key))
			require.NoError(t, err)
			require.Equal(t, want, string(got), "partial async transaction must publish neither mutation")
		}
		t.Logf("PROTOCOL_ASYNC_PARTIAL_DELIVERY_OK primary_accepted=%t secondary_accepted=%t both_mutations_unpublished=true", acceptPrimary, !acceptPrimary)
	}
}
