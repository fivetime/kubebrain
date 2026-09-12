package tikv

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/kubewharf/kubebrain/pkg/storage"
	"github.com/stretchr/testify/require"
	tikvconfig "github.com/tikv/client-go/v2/config"
	tikverr "github.com/tikv/client-go/v2/error"
	"github.com/tikv/client-go/v2/testutils"
	clienttikv "github.com/tikv/client-go/v2/tikv"
	"github.com/tikv/client-go/v2/tikvrpc"
)

func TestAtomicPrefetchConflictRequiresFreshTransaction(t *testing.T) {
	for _, present := range []bool{false, true} {
		name := "absent"
		if present {
			name = "present"
		}
		t.Run(name, func(t *testing.T) {
			client, cluster, pd, err := testutils.NewMockTiKV("", nil)
			require.NoError(t, err)
			testutils.BootstrapWithSingleStore(cluster)
			store, err := clienttikv.NewKVStore("prefetch-conflict-"+name, clienttikv.NewCodecPDClient(clienttikv.ModeTxn, pd), clienttikv.NewMockSafePointKV(), client)
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, store.Close()) })
			ctx := t.Context()
			key, payload := []byte("allocator"), []byte("payload")
			if present {
				seed, err := store.Begin()
				require.NoError(t, err)
				require.NoError(t, seed.Set(key, []byte("old")))
				require.NoError(t, seed.Commit(ctx))
			}
			stale, err := store.Begin()
			require.NoError(t, err)
			a := atomicBatch{txn: stale}
			require.NoError(t, a.Prefetch(ctx, [][]byte{key, payload}))
			winner, err := store.Begin()
			require.NoError(t, err)
			require.NoError(t, winner.Set(key, []byte("winner")))
			require.NoError(t, winner.Commit(ctx))
			value, err := a.Get(ctx, key)
			if present {
				require.NoError(t, err)
				require.Equal(t, []byte("old"), value)
			} else {
				require.ErrorIs(t, err, storage.ErrKeyNotFound)
			}
			require.NoError(t, a.Put(key, []byte("loser"), 0))
			require.NoError(t, a.Put(payload, []byte("must-not-commit"), 0))
			err = stale.Commit(ctx)
			require.Error(t, err)
			require.True(t, tikverr.IsErrWriteConflict(err), "unexpected commit error: %v", err)

			fresh, err := store.Begin()
			require.NoError(t, err)
			require.Greater(t, fresh.StartTS(), stale.StartTS())
			retry := atomicBatch{txn: fresh}
			require.NoError(t, retry.Prefetch(ctx, [][]byte{key, payload}))
			value, err = retry.Get(ctx, key)
			require.NoError(t, err)
			require.Equal(t, []byte("winner"), value, "new attempt must not reuse the old prefetch cache")
			_, err = retry.Get(ctx, payload)
			require.ErrorIs(t, err, storage.ErrKeyNotFound, "failed attempt must not publish its payload")
			require.NoError(t, retry.Put(key, []byte("retry"), 0))
			require.NoError(t, retry.Put(payload, []byte("committed"), 0))
			require.NoError(t, fresh.Commit(ctx))
			reader, err := store.Begin()
			require.NoError(t, err)
			defer func() { require.NoError(t, reader.Rollback()) }()
			value, err = reader.Get(ctx, key)
			require.NoError(t, err)
			require.Equal(t, []byte("retry"), value)
			value, err = reader.Get(ctx, payload)
			require.NoError(t, err)
			require.Equal(t, []byte("committed"), value)
		})
	}
}

type prefetchRPCRecorder struct {
	clienttikv.Client
	gets          atomic.Int32
	batches       atomic.Int32
	prewrites     atomic.Int32
	onePCRequests atomic.Int32
	mu            sync.Mutex
	batchRegions  map[uint64]bool
}

func (c *prefetchRPCRecorder) SendRequest(ctx context.Context, addr string, req *tikvrpc.Request, timeout time.Duration) (*tikvrpc.Response, error) {
	switch req.Type {
	case tikvrpc.CmdGet:
		c.gets.Add(1)
	case tikvrpc.CmdBatchGet:
		c.batches.Add(1)
		c.mu.Lock()
		if c.batchRegions == nil {
			c.batchRegions = make(map[uint64]bool)
		}
		c.batchRegions[req.Context.RegionId] = true
		c.mu.Unlock()
	case tikvrpc.CmdPrewrite:
		c.prewrites.Add(1)
		if req.Prewrite().TryOnePc {
			c.onePCRequests.Add(1)
		}
	}
	return c.Client.SendRequest(ctx, addr, req, timeout)
}

// Split only the in-process mock. Exercise a conflict on either side of the
// boundary, including SDK 1PC eligibility falling back before multi-Region
// prewrite. This is not a real Raft or partial-response fault test.
func TestAtomicPrefetchCrossRegionConflict(t *testing.T) {
	for _, onePC := range []bool{false, true} {
		mode := "2pc"
		if onePC {
			mode = "1pc-enabled"
		}
		t.Run(mode, func(t *testing.T) {
			t.Cleanup(tikvconfig.UpdateGlobal(func(c *tikvconfig.Config) {
				c.Enable1PC = onePC
				c.EnableAsyncCommit = false
			}))
			for _, conflictKey := range []string{"a", "z"} {
				t.Run(conflictKey, func(t *testing.T) {
					ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
					defer cancel()
					client, cluster, pd, err := testutils.NewMockTiKV("", nil)
					require.NoError(t, err)
					_, _, regionID := testutils.BootstrapWithSingleStore(cluster)
					newRegion, peer := cluster.AllocID(), cluster.AllocID()
					cluster.Split(regionID, newRegion, []byte("m"), []uint64{peer}, peer)
					rpc := &prefetchRPCRecorder{Client: client}
					store, err := clienttikv.NewKVStore("cross-region-prefetch-"+mode+conflictKey, clienttikv.NewCodecPDClient(clienttikv.ModeTxn, pd), clienttikv.NewMockSafePointKV(), rpc)
					require.NoError(t, err)
					t.Cleanup(func() { require.NoError(t, store.Close()) })
					keys := [][]byte{[]byte("a"), []byte("z")}
					seed, err := store.Begin()
					require.NoError(t, err)
					for _, key := range keys {
						require.NoError(t, seed.Set(key, []byte("old")))
					}
					require.NoError(t, seed.Commit(ctx))
					stale, err := store.Begin()
					require.NoError(t, err)
					a := atomicBatch{txn: stale}
					require.NoError(t, a.Prefetch(ctx, keys))
					rpc.mu.Lock()
					regions := len(rpc.batchRegions)
					rpc.mu.Unlock()
					require.Equal(t, 2, regions, "the prefetch must really route to both Regions")
					winner, err := store.Begin()
					require.NoError(t, err)
					require.NoError(t, winner.Set([]byte(conflictKey), []byte("winner")))
					beforeWinner := rpc.onePCRequests.Load()
					require.NoError(t, winner.Commit(ctx))
					wantOnePC := int32(0)
					if onePC {
						wantOnePC = 1
					}
					require.Equal(t, wantOnePC, rpc.onePCRequests.Load()-beforeWinner, "single-Region control must exercise the configured protocol")
					for _, key := range keys {
						value, err := a.Get(ctx, key)
						require.NoError(t, err)
						require.Equal(t, []byte("old"), value, "both Regions retain the original snapshot")
						require.NoError(t, a.Put(key, []byte("loser"), 0))
					}
					err = stale.Commit(ctx)
					require.Error(t, err)
					require.True(t, tikverr.IsErrWriteConflict(err), "unexpected error: %v", err)
					fresh, err := store.Begin()
					require.NoError(t, err)
					require.Greater(t, fresh.StartTS(), stale.StartTS())
					retry := atomicBatch{txn: fresh}
					require.NoError(t, retry.Prefetch(ctx, keys))
					for _, key := range keys {
						want := "old"
						if string(key) == conflictKey {
							want = "winner"
						}
						value, err := retry.Get(ctx, key)
						require.NoError(t, err)
						require.Equal(t, []byte(want), value, "failed transaction must not publish either mutation")
						require.NoError(t, retry.Put(key, []byte("retry"), 0))
					}
					prewrites, onePCRequests := rpc.prewrites.Load(), rpc.onePCRequests.Load()
					require.NoError(t, fresh.Commit(ctx))
					require.GreaterOrEqual(t, rpc.prewrites.Load()-prewrites, int32(2))
					require.Equal(t, onePCRequests, rpc.onePCRequests.Load(), "multi-Region write must disable TryOnePc before sending")
					reader, err := store.Begin()
					require.NoError(t, err)
					defer func() { require.NoError(t, reader.Rollback()) }()
					values, err := reader.BatchGet(ctx, keys)
					require.NoError(t, err)
					require.Equal(t, map[string][]byte{"a": []byte("retry"), "z": []byte("retry")}, values)
				})
			}
		})
	}
}

func TestAtomicPrefetchUsesSameSnapshotAndPreservesReadYourWrites(t *testing.T) {
	client, cluster, pd, err := testutils.NewMockTiKV("", nil)
	require.NoError(t, err)
	testutils.BootstrapWithSingleStore(cluster)
	rpc := &prefetchRPCRecorder{Client: client}
	store, err := clienttikv.NewKVStore("atomic-prefetch-test", clienttikv.NewCodecPDClient(clienttikv.ModeTxn, pd), clienttikv.NewMockSafePointKV(), rpc)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	ctx := t.Context()
	key, absent := []byte("present"), []byte("absent")
	seed, err := store.Begin()
	require.NoError(t, err)
	require.NoError(t, seed.Set(key, []byte("old")))
	require.NoError(t, seed.Commit(ctx))
	txn, err := store.Begin()
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, txn.Rollback()) })
	a := atomicBatch{txn: txn}
	startTS := txn.StartTS()
	rpc.gets.Store(0)
	rpc.batches.Store(0)
	require.NoError(t, a.Prefetch(ctx, [][]byte{key, absent}))
	require.Equal(t, int32(1), rpc.batches.Load())
	require.Equal(t, int32(0), rpc.gets.Load())
	concurrent, err := store.Begin()
	require.NoError(t, err)
	require.NoError(t, concurrent.Set(key, []byte("concurrent")))
	require.NoError(t, concurrent.Commit(ctx))
	value, err := a.Get(ctx, key)
	require.NoError(t, err)
	require.Equal(t, []byte("old"), value, "prefetch must keep the original snapshot despite a later commit")
	_, err = a.Get(ctx, absent)
	require.ErrorIs(t, err, storage.ErrKeyNotFound)
	require.NoError(t, a.Put(key, []byte("new"), 0))
	require.NoError(t, a.Put(absent, []byte("created"), 0))
	value, err = a.Get(ctx, key)
	require.NoError(t, err)
	require.Equal(t, []byte("new"), value)
	value, err = a.Get(ctx, absent)
	require.NoError(t, err)
	require.Equal(t, []byte("created"), value)
	require.NoError(t, a.Del(key))
	_, err = a.Get(ctx, key)
	require.ErrorIs(t, err, storage.ErrKeyNotFound)
	require.Equal(t, startTS, txn.StartTS())
	require.Equal(t, int32(0), rpc.gets.Load(), "all point reads must use the transaction buffer or prefetched snapshot")
	require.Equal(t, int32(1), rpc.batches.Load())
}
