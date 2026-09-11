package tikv

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/kubewharf/kubebrain/pkg/storage"
	"github.com/stretchr/testify/require"
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
	gets    atomic.Int32
	batches atomic.Int32
}

func (c *prefetchRPCRecorder) SendRequest(ctx context.Context, addr string, req *tikvrpc.Request, timeout time.Duration) (*tikvrpc.Response, error) {
	switch req.Type {
	case tikvrpc.CmdGet:
		c.gets.Add(1)
	case tikvrpc.CmdBatchGet:
		c.batches.Add(1)
	}
	return c.Client.SendRequest(ctx, addr, req, timeout)
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
