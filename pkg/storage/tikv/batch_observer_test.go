package tikv

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/kubewharf/kubebrain/pkg/storage"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/testutils"
	clienttikv "github.com/tikv/client-go/v2/tikv"
	"github.com/tikv/client-go/v2/tikvrpc"
	"github.com/tikv/client-go/v2/txnkv"
	"github.com/tikv/client-go/v2/util"
)

func TestBatchObserverEarlyFailuresKeepErrorAndSkipCommit(t *testing.T) {
	for _, stage := range []string{"begin", "prepare"} {
		t.Run(stage, func(t *testing.T) {
			want := errors.New("failure without request data")
			var observations []storage.BatchCommitObservation
			ctx := storage.WithBatchCommitObserver(t.Context(), func(o storage.BatchCommitObservation) {
				observations = append(observations, o)
			})
			b := &batch{}
			if stage == "begin" {
				b.begin = func(got context.Context) (*txnkv.KVTxn, error) {
					require.Same(t, ctx, got)
					return nil, want
				}
			} else {
				b.list = []func(context.Context) error{func(context.Context) error { return want }}
			}
			require.ErrorIs(t, b.Commit(ctx), want)
			require.Len(t, observations, 1)
			require.ErrorIs(t, observations[0].Err, want)
			require.False(t, observations[0].CommitAttempted)
			require.False(t, observations[0].HasWriteDetails)
			require.True(t, observations[0].HasLockRPCDetails)
			require.Zero(t, observations[0].CommitLocks)
			require.Zero(t, observations[0].Commit)
		})
	}
}

func TestBatchObserverRealClientWriteEmptyAndCallerSink(t *testing.T) {
	client, cluster, pd, err := testutils.NewMockTiKV("", nil)
	require.NoError(t, err)
	testutils.BootstrapWithSingleStore(cluster)
	wrapped := &writeResponseClient{Client: client, metrics: newWriteResponseMetrics(prometheus.NewRegistry())}
	store, err := clienttikv.NewKVStore("batch-observer-test", clienttikv.NewCodecPDClient(clienttikv.ModeTxn, pd), clienttikv.NewMockSafePointKV(), wrapped)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	for _, mode := range []string{"write", "empty", "caller_sink", "prepare_failure", "commit_canceled"} {
		t.Run(mode, func(t *testing.T) {
			var observations []storage.BatchCommitObservation
			parent, cancel := context.WithCancel(t.Context())
			defer cancel()
			ctx := storage.WithBatchCommitObserver(parent, func(o storage.BatchCommitObservation) {
				observations = append(observations, o)
			})
			var callerDetail *util.CommitDetails
			if mode == "caller_sink" {
				ctx = context.WithValue(ctx, util.CommitDetailCtxKey, &callerDetail)
			}
			b := &batch{begin: func(got context.Context) (*txnkv.KVTxn, error) {
				require.Same(t, ctx, got)
				return store.Begin()
			}}
			key := []byte("batch-observer/" + mode)
			var preparationTracker *lockRPCTracker
			b.list = append(b.list, func(phaseCtx context.Context) error {
				preparationTracker, _ = phaseCtx.Value(lockRPCTrackerKey{}).(*lockRPCTracker)
				require.NotNil(t, preparationTracker)
				preparationTracker.observe(tikvrpc.CmdResolveLock, time.Millisecond, nil)
				return nil
			})
			if mode != "empty" {
				b.Put(key, []byte("value"), 0)
			}
			prepareErr := errors.New("prepare stopped")
			if mode == "prepare_failure" {
				b.list = append(b.list, func(context.Context) error { return prepareErr })
			}
			if mode == "commit_canceled" {
				b.list = append(b.list, func(context.Context) error { cancel(); return nil })
			}
			err := b.Commit(ctx)
			require.Len(t, observations, 1)
			require.True(t, observations[0].HasLockRPCDetails)
			require.EqualValues(t, 1, observations[0].PrepareLocks.ResolveLock.Requests)
			require.Zero(t, observations[0].CommitLocks, "preparation must not leak into commit")
			preparationTracker.observe(tikvrpc.CmdResolveLock, time.Second, nil)
			require.Equal(t, observations[0].PrepareLocks, preparationTracker.finish(), "observer sees a closed snapshot")
			o := observations[0]
			if mode == "prepare_failure" {
				require.ErrorIs(t, err, prepareErr)
				require.ErrorIs(t, o.Err, prepareErr)
				require.False(t, o.CommitAttempted)
			} else if mode == "commit_canceled" {
				require.ErrorIs(t, err, context.Canceled)
				require.ErrorIs(t, err, storage.ErrUncertainResult)
				require.Equal(t, err, o.Err, "observer must see the mapped storage outcome")
				require.True(t, o.CommitAttempted)
			} else {
				require.NoError(t, err)
				require.NoError(t, o.Err)
				require.True(t, o.CommitAttempted)
			}
			if mode != "commit_canceled" {
				require.Equal(t, mode == "write", o.HasWriteDetails)
			}
			if mode == "write" || mode == "caller_sink" {
				require.EqualValues(t, 1, o.PrimaryWrite.SuccessfulRPCs)
				require.Positive(t, o.PrimaryWrite.RPC)
			} else {
				require.Zero(t, o.PrimaryWrite.SuccessfulRPCs)
			}
			if mode == "write" {
				require.EqualValues(t, 1, o.PrewriteRegionGroups)
				require.Positive(t, o.Prewrite)
				require.Positive(t, o.PrimaryCommit)
				require.GreaterOrEqual(t, o.Commit, o.Prewrite)
				require.GreaterOrEqual(t, o.Commit, o.PrimaryCommit)
			}
			if mode == "caller_sink" {
				require.NotNil(t, callerDetail, "existing SDK sink must not be replaced")
				require.Positive(t, callerDetail.PrewriteTime)
			}
			read, err := store.Begin()
			require.NoError(t, err)
			value, err := read.Get(t.Context(), key)
			if mode == "write" || mode == "caller_sink" {
				require.NoError(t, err)
				require.Equal(t, []byte("value"), value)
			} else {
				require.Error(t, err, "empty/aborted batch must not persist a value")
			}
			require.NoError(t, read.Rollback())
		})
	}
}

func TestBatchObserverPrewriteRegionGroups(t *testing.T) {
	client, cluster, pd, err := testutils.NewMockTiKV("", nil)
	require.NoError(t, err)
	_, _, region := testutils.BootstrapWithSingleStore(cluster)
	newRegion, peer := cluster.AllocID(), cluster.AllocID()
	cluster.Split(region, newRegion, []byte("m"), []uint64{peer}, peer)
	store, err := clienttikv.NewKVStore("batch-region-observer", clienttikv.NewCodecPDClient(clienttikv.ModeTxn, pd), clienttikv.NewMockSafePointKV(), client)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	var observations []storage.BatchCommitObservation
	ctx := storage.WithBatchCommitObserver(t.Context(), func(o storage.BatchCommitObservation) {
		observations = append(observations, o)
	})
	b := &batch{begin: func(context.Context) (*txnkv.KVTxn, error) { return store.Begin() }}
	b.Put([]byte("a"), []byte("left"), 0)
	b.Put([]byte("z"), []byte("right"), 0)
	require.NoError(t, b.Commit(ctx))
	require.Len(t, observations, 1)
	require.True(t, observations[0].HasWriteDetails)
	require.EqualValues(t, 2, observations[0].PrewriteRegionGroups)
	reader, err := store.Begin()
	require.NoError(t, err)
	defer func() { require.NoError(t, reader.Rollback()) }()
	values, err := reader.BatchGet(ctx, [][]byte{[]byte("a"), []byte("z")})
	require.NoError(t, err)
	require.Equal(t, map[string][]byte{"a": []byte("left"), "z": []byte("right")}, values)
}
