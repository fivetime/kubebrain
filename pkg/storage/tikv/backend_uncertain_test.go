package tikv

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	proto "github.com/kubewharf/kubebrain-client/api/v2rpc"
	"github.com/kubewharf/kubebrain/pkg/backend"
	"github.com/kubewharf/kubebrain/pkg/backend/coder"
	"github.com/kubewharf/kubebrain/pkg/metrics"
	metricmock "github.com/kubewharf/kubebrain/pkg/metrics/mock"
	"github.com/kubewharf/kubebrain/pkg/storage"
	"github.com/kubewharf/kubebrain/pkg/storage/memkv"
	"github.com/pingcap/failpoint"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/require"
	tikvconfig "github.com/tikv/client-go/v2/config"
	tikvmetrics "github.com/tikv/client-go/v2/metrics"
)

// Derive BOTH physical ranges from the validated dedicated nonce. Backend
// objects/internal keys use the named keyspace; coordination uses raw Prefix.
func protocolBackendScope(prefix string) (*coder.Keyspace, []coder.KeyRange, error) {
	if _, err := validateProtocolSmokeScope("1", prefix, "1pc"); err != nil {
		return nil, nil, err
	}
	nonce := strings.TrimSuffix(strings.TrimPrefix(prefix, "kubebrain/protocol-smoke/"), "/")
	ks, err := coder.NewKeyspace("protocol-backend-" + nonce)
	if err != nil {
		return nil, nil, err
	}
	return ks, []coder.KeyRange{
		{Start: []byte(prefix), End: []byte(strings.TrimSuffix(prefix, "/") + "0")},
		{Start: ks.ObjectKeyspaceStart(), End: ks.ObjectKeyspaceEnd()},
	}, nil
}

// Bounded collection deliberately refuses unexpectedly large fixtures rather
// than deleting an unbounded namespace. Called only before writers start or
// after backend.Close has joined every worker.
func protocolBackendKeys(ctx context.Context, kv storage.KvStorage, prefix string) ([][]byte, error) {
	return protocolBackendKeysWithFences(ctx, kv, prefix, false)
}

// The larger fixture permits only the exact 512 production coordination shards
// in addition to the unchanged 128-key ordinary budget. Near-matching names
// consume the ordinary budget; they never gain an unbounded exemption.
func protocolBackendKeysWithFences(ctx context.Context, kv storage.KvStorage, prefix string, fenced bool) ([][]byte, error) {
	_, ranges, err := protocolBackendScope(prefix)
	if err != nil {
		return nil, err
	}
	allowed := make(map[string]bool)
	if fenced {
		for i := 0; i < 256; i++ {
			for _, family := range []string{"election-fence", "restoration-fence-shard"} {
				allowed[fmt.Sprintf("%sbackend/%s/%02x", prefix, family, i)] = true
			}
		}
	}
	limit := 128 + len(allowed)
	ordinary := 0
	var keys [][]byte
	for _, r := range ranges {
		iter, err := kv.Iter(ctx, r.Start, r.End, 0, uint64(limit+1))
		if err != nil {
			return nil, err
		}
		for {
			err = iter.Next(ctx)
			if err != nil {
				break
			}
			key := iter.Key()
			if bytes.Compare(key, r.Start) < 0 || bytes.Compare(key, r.End) >= 0 {
				err = fmt.Errorf("fixture iterator escaped its range")
				break
			}
			keys = append(keys, bytes.Clone(key))
			if !allowed[string(key)] {
				ordinary++
			}
			if ordinary > 128 || len(keys) > limit {
				err = fmt.Errorf("fixture exceeds 128 ordinary-key cleanup bound (total bound %d)", limit)
				break
			}
		}
		closeErr := iter.Close()
		if errors.Is(err, io.EOF) {
			err = nil
		}
		if err = errors.Join(err, closeErr); err != nil {
			return nil, err
		}
	}
	return keys, nil
}

func cleanupProtocolBackend(ctx context.Context, kv storage.KvStorage, prefix string, owner []byte) error {
	return cleanupProtocolBackendWithFences(ctx, kv, prefix, owner, false)
}

func cleanupProtocolBackendWithFences(ctx context.Context, kv storage.KvStorage, prefix string, owner []byte, fenced bool) error {
	if _, _, err := protocolBackendScope(prefix); err != nil {
		return err
	}
	if len(owner) != 32 {
		return fmt.Errorf("invalid owner token")
	}
	claim := []byte(prefix + "owner")
	got, claimErr := kv.Get(ctx, claim)
	if claimErr != nil && !errors.Is(claimErr, storage.ErrKeyNotFound) {
		return claimErr
	}
	if claimErr == nil && !bytes.Equal(got, owner) {
		return fmt.Errorf("ownership changed: refuse backend cleanup")
	}
	keys, err := protocolBackendKeysWithFences(ctx, kv, prefix, fenced)
	if err != nil {
		return err
	}
	if errors.Is(claimErr, storage.ErrKeyNotFound) {
		if len(keys) != 0 {
			return fmt.Errorf("owner absent but backend fixture is not empty")
		}
		return nil
	}
	batch := kv.BeginBatchWrite()
	batch.CAS(claim, owner, owner, 0)
	for _, key := range keys {
		batch.Del(key)
	}
	if err := batch.Commit(ctx); err != nil {
		return err
	}
	keys, err = protocolBackendKeysWithFences(ctx, kv, prefix, fenced)
	if err == nil && len(keys) != 0 {
		err = fmt.Errorf("backend cleanup left keys behind")
	}
	return err
}

type protocolResolutionMetrics struct {
	metrics.Metrics
	committed, absent atomic.Int32
}

func (m *protocolResolutionMetrics) EmitCounter(name string, value interface{}, tags ...metrics.T) error {
	positive := false
	switch n := value.(type) {
	case int:
		positive = n > 0
	case int64:
		positive = n > 0
	}
	if positive && name == "txn.uncertain.resolve.committed" {
		m.committed.Add(1)
	}
	if positive && name == "txn.uncertain.resolve.not_committed" {
		m.absent.Add(1)
	}
	return m.Metrics.EmitCounter(name, value, tags...)
}

func protocolNextMutation(t *testing.T, ctx context.Context, watch <-chan []*proto.Event) []*proto.Event {
	t.Helper()
	for {
		select {
		case events, ok := <-watch:
			require.True(t, ok, "watch closed before mutation")
			if backend.IsProgressMarker(events) {
				continue
			}
			require.NotEmpty(t, events)
			for _, event := range events {
				require.NotNil(t, event)
				require.NotNil(t, event.Kv)
			}
			return events
		case <-ctx.Done():
			t.Fatalf("watch mutation deadline: %v", ctx.Err())
		}
	}
}

// Opt-in real adapter AND backend, with no network/server/global-failpoint
// modification. This does not cover process restart, Region split or Raft loss.
func TestRealTiKVBackendResolvesCancelledOnePC(t *testing.T) {
	testRealTiKVBackendScenario(t, "committed")
}

func TestRealTiKVBackendResolvesUndeliveredOnePC(t *testing.T) {
	testRealTiKVBackendScenario(t, "undelivered")
}

func TestRealTiKVBackendRetriesCommittedOnePC(t *testing.T) {
	testRealTiKVBackendScenario(t, "retry-committed")
}

func TestRealTiKVBackendRetriesUndeliveredOnePC(t *testing.T) {
	testRealTiKVBackendScenario(t, "retry-undelivered")
}

func TestRealTiKVBackendNoRPCRetryCommittedOnePC(t *testing.T) {
	testRealTiKVBackendScenario(t, "no-retry-committed")
}

func TestRealTiKVBackendNoRPCRetryUndeliveredOnePC(t *testing.T) {
	testRealTiKVBackendScenario(t, "no-retry-undelivered")
}

func testRealTiKVBackendScenario(t *testing.T, scenario string) {
	t.Helper()
	asyncExperiment := strings.HasPrefix(scenario, "async-")
	if asyncExperiment {
		if os.Getenv("KUBEBRAIN_TIKV_PROTOCOL_ASYNC_EXPERIMENT") != "1" {
			t.Skip("explicit async experiment consent required")
		}
		scenario = strings.TrimPrefix(scenario, "async-")
		require.Contains(t, []string{"fenced", "fenced-election-fence", "fenced-restoration-fence-shard"}, scenario)
	}
	require.Contains(t, []string{"committed", "undelivered", "latency", "concurrent", "compare-conflict", "fenced", "fenced-election-fence", "fenced-restoration-fence-shard", "retry-committed", "retry-undelivered", "no-retry-committed", "no-retry-undelivered", "split"}, scenario)
	fenced := scenario == "fenced" || strings.HasPrefix(scenario, "fenced-")
	retrying := strings.HasPrefix(scenario, "retry-")
	noRPCRetry := strings.HasPrefix(scenario, "no-retry-")
	splitting := scenario == "split"
	scenario = strings.TrimPrefix(scenario, "retry-")
	scenario = strings.TrimPrefix(scenario, "no-retry-")
	beforeDelivery := scenario == "undelivered"
	pd := os.Getenv("KUBEBRAIN_TIKV_PROTOCOL_PD")
	if pd == "" {
		t.Skip("explicit protocol PD endpoint required")
	}
	if splitting {
		require.Equal(t, "1", os.Getenv("KUBEBRAIN_TIKV_PROTOCOL_ALLOW_REGION_SPLIT"), "Region split requires explicit disposable-cluster consent")
	}
	prefix := os.Getenv("KUBEBRAIN_TIKV_PROTOCOL_PREFIX")
	expected, err := validateProtocolSmokeScope(os.Getenv("KUBEBRAIN_TIKV_PROTOCOL_CLUSTER_ID"), prefix, os.Getenv("KUBEBRAIN_TIKV_PROTOCOL_MODE"))
	require.NoError(t, err)
	mode := os.Getenv("KUBEBRAIN_TIKV_PROTOCOL_MODE")
	if scenario == "concurrent" || scenario == "compare-conflict" || fenced {
		require.Equal(t, "2pc", mode)
	} else if scenario != "latency" {
		require.Equal(t, "1pc", mode)
	}
	if noRPCRetry {
		require.True(t, protocolFailpointsEnabled, "explicit startup failpoint consent required before creating clients")
		require.NoError(t, failpoint.Enable("tikvclient/noRetryOnRpcError", "return(true)"))
		t.Cleanup(func() { require.NoError(t, failpoint.Disable("tikvclient/noRetryOnRpcError")) })
	} else {
		_, evalErr := failpoint.Eval("tikvclient/noRetryOnRpcError")
		require.Error(t, evalErr, "normal cases must not inherit disabled RPC retries")
	}
	ks, _, err := protocolBackendScope(prefix)
	require.NoError(t, err)
	ctrl := gomock.NewController(t) // Finish only after backend workers stop.
	t.Cleanup(tikvconfig.UpdateGlobal(func(cfg *tikvconfig.Config) {
		cfg.Enable1PC = mode == "1pc"
		cfg.EnableAsyncCommit = false
	}))
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	kv, err := NewKvStorageWithContext(ctx, strings.Split(pd, ","), 1, Security{})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, kv.Close()) })
	require.Equal(t, expected, kv.(storage.ClusterIdentifier).ClusterID())
	// Backend.Close owns kv. A separate real client is required for cleanup
	// AFTER all backend workers stop, preserving every adapter capability.
	cleanupKV, err := NewKvStorageWithContext(ctx, strings.Split(pd, ","), 1, Security{})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, cleanupKV.Close()) })
	require.Equal(t, expected, cleanupKV.(storage.ClusterIdentifier).ClusterID())
	keys, err := protocolBackendKeysWithFences(ctx, cleanupKV, prefix, fenced)
	require.NoError(t, err)
	require.Empty(t, keys, "both fixture ranges must be empty before claiming")
	owner := make([]byte, 32)
	_, err = rand.Read(owner)
	require.NoError(t, err)
	var closer interface{ Close() error }
	t.Cleanup(func() {
		if closer != nil {
			if err := closer.Close(); err != nil {
				t.Errorf("backend close failed; preserve fixture for recovery: %v", err)
				return
			}
		}
		cleanupCtx, stop := context.WithTimeout(context.Background(), 30*time.Second)
		defer stop()
		require.NoError(t, cleanupProtocolBackendWithFences(cleanupCtx, cleanupKV, prefix, owner, fenced))
		t.Logf("PROTOCOL_BACKEND_CLEANUP_OK prefix=%s keyspace=%s", prefix, ks.Name())
	})
	t.Logf("PROTOCOL_BACKEND_STARTED cluster=%d prefix=%s keyspace=%s owner_sha256=%x", expected, prefix, ks.Name(), sha256.Sum256(owner))
	claim := cleanupKV.BeginBatchWrite()
	claim.PutIfNotExist([]byte(prefix+"owner"), owner, 0)
	require.NoError(t, claim.Commit(ctx))
	commitCtx, cancelCommit := context.WithCancel(context.WithValue(ctx, protocolCommitMarker{}, true))
	defer cancelCommit()
	client := kv.(*store).getClient()
	loss := &protocolResponseLoss{Client: client.GetTiKVClient(), cancelAfterLoss: cancelCommit, beforeDelivery: beforeDelivery}
	if retrying || noRPCRetry {
		loss.cancelAfterLoss = nil // keep the caller active across the injected transport error
	}
	var latencyClient *protocolLatencyClient
	var splitClient *protocolRegionSplit
	var quota int64
	if scenario == "latency" || scenario == "concurrent" || scenario == "compare-conflict" || fenced {
		latencyClient = &protocolLatencyClient{Client: client.GetTiKVClient()}
		client.SetTiKVClient(latencyClient)
		quota = 2 << 30
	} else if splitting {
		splitClient = &protocolRegionSplit{Client: client.GetTiKVClient()}
		splitClient.split = func(splitCtx context.Context, key []byte) ([]uint64, error) {
			return client.SplitRegions(splitCtx, [][]byte{key}, false, nil)
		}
		client.SetTiKVClient(splitClient)
	} else {
		client.SetTiKVClient(loss)
	}
	m := &protocolResolutionMetrics{Metrics: metricmock.NewMinimalMetrics(ctrl)}
	backendKV := kv
	var compareConflict *compareChangeStorage
	if scenario == "compare-conflict" {
		compareConflict = &compareChangeStorage{KvStorage: kv}
		backendKV = compareConflict
	}
	var conflict *fenceChangeAfterPrefetch
	if strings.HasPrefix(scenario, "fenced-") {
		conflict = &fenceChangeAfterPrefetch{KvStorage: kv, target: prefix + "backend/" + strings.TrimPrefix(scenario, "fenced-") + "/"}
		backendKV = conflict
	}
	var asyncWrites []*protocolAsyncCommitHold
	if asyncExperiment && scenario == "fenced" {
		wrapped := &fenceChangeAfterPrefetch{KvStorage: kv}
		wrapped.beforeAtomic = func(callCtx context.Context, atomic storage.AtomicBatch) {
			if callCtx.Value(protocolLatencyMarker{}) != true {
				return
			}
			txn := atomic.(atomicBatch).txn
			for _, tracked := range asyncWrites {
				if tracked.startTS == txn.StartTS() {
					return
				}
			}
			txn.SetEnable1PC(false)
			txn.SetEnableAsyncCommit(true)
			tracked := &protocolAsyncCommitHold{Client: client.GetTiKVClient(), startTS: txn.StartTS(),
				held: make(chan struct{}), release: make(chan struct{}), asyncRegions: make(map[uint64]struct{})}
			close(tracked.release) // observe real replies; do not delay backend commits
			client.SetTiKVClient(tracked)
			asyncWrites = append(asyncWrites, tracked)
		}
		backendKV = wrapped
	}
	b := backend.NewBackend(backendKV, backend.Config{
		Prefix: prefix + "backend", Keyspace: ks.Name(), Identity: ks.Name(),
		EnableEtcdCompatibility: true, StorageGCLifetime: 0, QuotaBackendBytes: quota,
	}, m)
	closer = b.(interface{ Close() error })
	b.SetCurrentRevision(100)
	if compareConflict != nil {
		require.NoError(t, b.EnsureQuotaInitialized(ctx))
		verifyTxnCompareCommitConflict(t, ctx, b, compareConflict, ks)
		return
	}
	if conflict != nil {
		var asyncRPC *protocolAsyncCommitHold
		if asyncExperiment {
			conflict.beforeAtomic = func(callCtx context.Context, atomic storage.AtomicBatch) {
				if callCtx.Value(protocolLatencyMarker{}) != true || !conflict.armed.Load() {
					return
				}
				txn := atomic.(atomicBatch).txn
				if asyncRPC != nil {
					// One backend transaction can stage several Atomic callbacks.
					require.Equal(t, asyncRPC.startTS, txn.StartTS(), "exactly one experimental backend transaction")
					return
				}
				txn.SetEnable1PC(false)
				txn.SetEnableAsyncCommit(true)
				asyncRPC = &protocolAsyncCommitHold{Client: client.GetTiKVClient(), startTS: txn.StartTS(),
					held: make(chan struct{}), release: make(chan struct{}), asyncRegions: make(map[uint64]struct{})}
				close(asyncRPC.release)
				client.SetTiKVClient(asyncRPC)
			}
		}
		verifyRealProtocolFenceConflict(t, ctx, b, conflict, ks, strings.TrimPrefix(scenario, "fenced-"))
		if asyncExperiment {
			require.NotNil(t, asyncRPC)
			require.Positive(t, asyncRPC.asyncAttempts.Load(), "fenced transaction must attempt real async Prewrite")
			require.Zero(t, asyncRPC.commitsForwarded.Load(), "fenced transaction must never commit")
			t.Logf("PROTOCOL_ASYNC_FENCE_OK scenario=%s actual_async_prewrite=true commit_rpcs=0", scenario)
		}
		return
	}
	if fenced {
		rpc := &fencedShapeClient{protocolLatencyClient: latencyClient, coordinationPrefix: prefix + "backend"}
		client.SetTiKVClient(rpc)
		verifyRealProtocolProductionFences(t, ctx, b, rpc, asyncExperiment)
		if asyncExperiment {
			require.Len(t, asyncWrites, 3)
			for _, tracked := range asyncWrites {
				require.Positive(t, tracked.asyncResponses.Load(), "real server must accept async commit")
				require.Equal(t, tracked.prewrites.Load(), tracked.asyncResponses.Load()+tracked.regionErrors.Load(), "no silent fallback or unaccounted Prewrite reply")
			}
			t.Log("PROTOCOL_ASYNC_BACKEND_PUBLICATION_OK transactions=3 actual_async_accepted=true witness_validation=true")
		}
		return
	}
	if scenario == "concurrent" {
		require.NoError(t, b.EnsureQuotaInitialized(ctx))
		verifyProtocolConcurrentWrites(t, ctx, b)
		verifyProtocolMultiPreviousReads(t, ctx, b, latencyClient)
		return
	}
	if scenario == "latency" {
		measureProtocolBackendLatency(t, ctx, b, latencyClient, mode)
		return
	}
	watch, err := b.Watch(ctx, "/integration/onepc/", 101)
	require.NoError(t, err)
	left, right := []byte("/integration/onepc/left"), []byte("/integration/onepc/right")
	// Each real fixture case runs in its own process. Read the SDK counter,
	// rather than substituting a fake health endpoint for the actual TiKV server.
	// The SDK increments this only when its gRPC Check reports SERVING.
	healthOK := func() float64 {
		var metric dto.Metric
		require.NoError(t, tikvmetrics.StatusCountWithOK.Write(&metric))
		return metric.GetCounter().GetValue()
	}
	healthBefore := healthOK()
	result, revision, err := b.TxnApply(commitCtx, []backend.TxnWriteOp{
		{Key: left, Value: []byte("left-value")}, {Key: right, Value: []byte("right-value")},
	}, nil)
	if retrying || splitting {
		require.NoError(t, err, "real server must confirm the retried single transaction")
		require.Len(t, result, 2)
		require.NoError(t, commitCtx.Err())
	} else {
		require.ErrorIs(t, err, storage.ErrUncertainResult)
		require.Nil(t, result)
		if noRPCRetry {
			require.NoError(t, commitCtx.Err(), "plain transport failure must not be replaced by caller cancellation")
		} else {
			require.ErrorIs(t, commitCtx.Err(), context.Canceled)
		}
	}
	require.EqualValues(t, 101, revision)
	stats := loss.snapshot()
	if splitting {
		splitStats := splitClient.snapshot()
		require.Equal(t, 1, splitStats.Splits)
		require.Positive(t, splitStats.EpochErrors, "must exercise a real stale-epoch response")
		require.GreaterOrEqual(t, splitStats.Regions, 2, "must prewrite successfully in multiple Regions")
		require.Positive(t, splitStats.Commits, "must execute two-phase commit")
		require.False(t, splitStats.OnePCCommitted)
		require.False(t, splitStats.Changed)
		stats = protocolLossStats{Attempts: splitStats.Attempts, StartTS: splitStats.StartTS, CommitTS: splitStats.CommitTS}
		t.Logf("PROTOCOL_REAL_SPLIT_CONFIRMED splits=%d epoch_errors=%d regions=%d commits=%d", splitStats.Splits, splitStats.EpochErrors, splitStats.Regions, splitStats.Commits)
	} else {
		require.Equal(t, 1, stats.Drops)
		if retrying {
			require.Equal(t, 2, stats.Attempts, "one injected loss must cause exactly one RPC retry")
			healthDelta := healthOK() - healthBefore
			require.Positive(t, healthDelta, "default retry must complete a real TiKV gRPC health check")
			t.Logf("PROTOCOL_REAL_HEALTH_CONFIRMED successful_checks=%g", healthDelta)
		} else {
			require.Equal(t, 1, stats.Attempts)
		}
	}
	require.NotZero(t, stats.StartTS)
	require.False(t, stats.Changed)
	if noRPCRetry {
		t.Logf("PROTOCOL_BACKEND_NO_RPC_RETRY_CONFIRMED before_delivery=%t context_active=true attempts=%d drops=%d", beforeDelivery, stats.Attempts, stats.Drops)
	}
	if beforeDelivery && !retrying {
		require.Zero(t, stats.CommitTS, "no request was delivered to commit")
		require.Eventually(t, func() bool { return m.absent.Load() == 1 }, 10*time.Second, 10*time.Millisecond,
			"real backend must establish that the candidate did not commit")
		require.Zero(t, m.committed.Load())
		require.EqualValues(t, 100, b.GetCurrentRevision(), "absent candidate must not advance visible revision")
		for _, key := range [][]byte{left, right} {
			got, err := b.Get(ctx, &proto.GetRequest{Key: key})
			require.NoError(t, err)
			require.Nil(t, got.Kv, "undelivered transaction must leave both keys absent")
		}
		_, nextRevision, err := b.TxnApply(ctx, []backend.TxnWriteOp{{Key: left, Value: []byte("next")}}, nil)
		require.NoError(t, err)
		require.Equal(t, revision, nextRevision, "absent candidate must be reused without a revision hole")
		next := protocolNextMutation(t, ctx, watch)
		require.Len(t, next, 1, "undelivered transaction must not publish events before next acknowledged write")
		require.Equal(t, proto.Event_CREATE, next[0].Type)
		require.Equal(t, nextRevision, next[0].Revision)
		require.Equal(t, nextRevision, next[0].Kv.Revision)
		require.Equal(t, left, next[0].Kv.Key)
		require.Equal(t, []byte("next"), backend.StripInlineValue(next[0].Kv.Value))
		require.EqualValues(t, 1, m.absent.Load())
		require.Zero(t, m.committed.Load())
		t.Logf("PROTOCOL_BACKEND_RESOLVED committed=0 absent=1 revision=%d next=%d attempts=%d drops=%d start_ts=%d commit_ts=%d", revision, nextRevision, stats.Attempts, stats.Drops, stats.StartTS, stats.CommitTS)
		return
	}
	require.Greater(t, stats.CommitTS, stats.StartTS)
	var resolvedCommitted int32 = 1
	if retrying || splitting {
		resolvedCommitted = 0 // acknowledged RPC retry must not enter uncertain resolution
	}
	require.Eventually(t, func() bool {
		return m.committed.Load() == resolvedCommitted && b.GetCurrentRevision() == revision
	}, 10*time.Second, 10*time.Millisecond, "committed transaction must publish its revision")
	require.Zero(t, m.absent.Load())
	events := protocolNextMutation(t, ctx, watch)
	require.Len(t, events, 2)
	seen := make(map[string]string)
	for _, event := range events {
		require.Equal(t, proto.Event_CREATE, event.Type)
		require.Equal(t, revision, event.Revision)
		require.Equal(t, revision, event.Kv.Revision)
		seen[string(event.Kv.Key)] = string(backend.StripInlineValue(event.Kv.Value))
	}
	require.Equal(t, map[string]string{string(left): "left-value", string(right): "right-value"}, seen)
	for key, value := range seen {
		got, err := b.Get(ctx, &proto.GetRequest{Key: []byte(key)})
		require.NoError(t, err)
		require.NotNil(t, got.Kv)
		require.Equal(t, revision, got.Kv.Revision)
		require.Equal(t, value, string(backend.StripInlineValue(got.Kv.Value)))
	}
	_, nextRevision, err := b.TxnApply(ctx, []backend.TxnWriteOp{{Key: left, Value: []byte("next")}}, nil)
	require.NoError(t, err)
	require.Equal(t, revision+1, nextRevision)
	next := protocolNextMutation(t, ctx, watch)
	require.Len(t, next, 1, "uncertain transaction must not replay ahead of next write")
	require.Equal(t, proto.Event_PUT, next[0].Type)
	require.Equal(t, nextRevision, next[0].Revision)
	require.Equal(t, left, next[0].Kv.Key)
	require.Equal(t, []byte("next"), backend.StripInlineValue(next[0].Kv.Value))
	require.Equal(t, resolvedCommitted, m.committed.Load())
	require.Zero(t, m.absent.Load())
	if splitting {
		t.Logf("PROTOCOL_BACKEND_SPLIT_ATOMICITY_CONFIRMED revision=%d next=%d attempts=%d start_ts=%d commit_ts=%d", revision, nextRevision, stats.Attempts, stats.StartTS, stats.CommitTS)
	} else if retrying {
		t.Logf("PROTOCOL_BACKEND_RETRY_CONFIRMED before_delivery=%t resolver_committed=0 resolver_absent=0 revision=%d next=%d attempts=%d drops=%d start_ts=%d commit_ts=%d", beforeDelivery, revision, nextRevision, stats.Attempts, stats.Drops, stats.StartTS, stats.CommitTS)
	} else {
		t.Logf("PROTOCOL_BACKEND_RESOLVED committed=1 absent=0 revision=%d next=%d attempts=%d drops=%d start_ts=%d commit_ts=%d", revision, nextRevision, stats.Attempts, stats.Drops, stats.StartTS, stats.CommitTS)
	}
}

func TestProtocolBackendCleanupOwnership(t *testing.T) {
	ctx := context.Background()
	prefix := "kubebrain/protocol-smoke/0123456789abcdef0123456789abcdef/"
	ks, _, err := protocolBackendScope(prefix)
	require.NoError(t, err)
	owner := bytes.Repeat([]byte{1}, 32)
	for _, mode := range []string{"owned", "absent", "foreign", "raced", "orphan", "oversized"} {
		t.Run(mode, func(t *testing.T) {
			kv := memkv.NewKvStorage()
			t.Cleanup(func() { require.NoError(t, kv.Close()) })
			batch := kv.BeginBatchWrite()
			if mode != "absent" && mode != "orphan" {
				token := owner
				if mode == "foreign" {
					token = bytes.Repeat([]byte{2}, 32)
				}
				batch.Put([]byte(prefix+"owner"), token, 0)
			}
			if mode != "absent" {
				batch.Put(ks.EncodeInternalKey([]byte("fixture")), []byte("keep"), 0)
				batch.Put([]byte(prefix+"backend/election"), []byte("keep"), 0)
			}
			if mode == "oversized" {
				for i := 0; i < 129; i++ {
					batch.Put(ks.EncodeInternalKey([]byte(fmt.Sprint(i))), []byte("keep"), 0)
				}
			}
			batch.Put([]byte("unrelated-fixture-key"), []byte("keep"), 0)
			require.NoError(t, batch.Commit(ctx))
			cleanupStore := &protocolCleanupRaceStore{KvStorage: kv}
			if mode == "raced" {
				cleanupStore.afterGet = func() error {
					batch := kv.BeginBatchWrite()
					batch.Put([]byte(prefix+"owner"), bytes.Repeat([]byte{2}, 32), 0)
					return batch.Commit(ctx)
				}
			}
			err := cleanupProtocolBackend(ctx, cleanupStore, prefix, owner)
			if mode == "raced" {
				require.ErrorIs(t, err, storage.ErrCASFailed)
			}
			if mode == "owned" || mode == "absent" {
				require.NoError(t, err)
				require.NoError(t, cleanupProtocolBackend(ctx, kv, prefix, owner))
				keys, err := protocolBackendKeys(ctx, kv, prefix)
				require.NoError(t, err)
				require.Empty(t, keys)
			} else {
				require.Error(t, err)
				got, err := kv.Get(ctx, ks.EncodeInternalKey([]byte("fixture")))
				require.NoError(t, err)
				require.Equal(t, []byte("keep"), got)
			}
			got, err := kv.Get(ctx, []byte("unrelated-fixture-key"))
			require.NoError(t, err)
			require.Equal(t, []byte("keep"), got)
		})
	}
	_, _, err = protocolBackendScope("/registry/")
	require.Error(t, err)
	require.Error(t, cleanupProtocolBackend(ctx, nil, "/registry/", owner))
}
