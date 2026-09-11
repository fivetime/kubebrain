package tikv

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/kubewharf/kubebrain/pkg/storage"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/testutils"
	clienttikv "github.com/tikv/client-go/v2/tikv"
	"github.com/tikv/client-go/v2/tikvrpc"
)

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
