package tikv

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/kubewharf/kubebrain/pkg/backend"
	"github.com/kubewharf/kubebrain/pkg/backend/coder"
	metricmock "github.com/kubewharf/kubebrain/pkg/metrics/mock"
	"github.com/kubewharf/kubebrain/pkg/storage"
	"github.com/kubewharf/kubebrain/pkg/storage/memkv"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/testutils"
	clienttikv "github.com/tikv/client-go/v2/tikv"
	"github.com/tikv/client-go/v2/tikvrpc"
	"github.com/tikv/client-go/v2/txnkv/txnsnapshot"
)

func TestLeadershipWitnessDetectsCorruptionBeyondSDKScanPage(t *testing.T) {
	client, cluster, pd, err := testutils.NewMockTiKV("", nil)
	require.NoError(t, err)
	testutils.BootstrapWithSingleStore(cluster)
	rpc := &scanPageRPCClient{Client: client}
	kvClient, err := clienttikv.NewKVStore("witness-page-corruption", clienttikv.NewCodecPDClient(clienttikv.ModeTxn, pd), clienttikv.NewMockSafePointKV(), rpc)
	require.NoError(t, err)
	kv := NewKvStoreWithStorage([]*clienttikv.KVStore{kvClient})
	ks, err := coder.NewKeyspace("witness-page-corruption")
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, kv.Close()) })
	config := backend.Config{Prefix: "/witness-page-corruption", Keyspace: ks.Name(), Identity: "leader", EnableEtcdCompatibility: true}
	seedKV := memkv.NewKvStorage()
	seed := backend.NewBackend(seedKV, config, metricmock.NewMinimalMetrics(gomock.NewController(t)))
	t.Cleanup(func() { require.NoError(t, seed.(interface{ Close() error }).Close()) })
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
	defer cancel()
	seed.SetCurrentRevision(1)
	key := []byte("/probe/witness-page")
	var last uint64
	for i := 0; i < 2050; i++ {
		key = []byte(fmt.Sprintf("/probe/witness-page/%04d", i))
		_, last, err = seed.TxnApply(ctx, []backend.TxnWriteOp{{Key: key, Value: []byte(fmt.Sprint(i))}}, nil)
		require.NoError(t, err)
	}
	// Preserve backend-generated wire records without spending the test budget
	// on thousands of separate mock SDK write commits. Only this fresh in-memory
	// store is enumerated; validation below uses the actual SDK on mock TiKV.
	rows, err := seedKV.Iter(ctx, []byte{0}, []byte{255}, 0, 0)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, rows.Close()) })
	batchSeed := kv.BeginBatchWrite()
	count := 0
	for {
		err := rows.Next(ctx)
		if err == io.EOF {
			break
		}
		require.NoError(t, err)
		batchSeed.Put(bytes.Clone(rows.Key()), bytes.Clone(rows.Val()), 0)
		count++
		if count%256 == 0 {
			require.NoError(t, batchSeed.Commit(ctx))
			batchSeed = kv.BeginBatchWrite()
		}
	}
	if count%256 != 0 {
		require.NoError(t, batchSeed.Commit(ctx))
	}
	b := backend.NewBackend(kv, config, metricmock.NewMinimalMetrics(gomock.NewController(t)))
	t.Cleanup(func() { require.NoError(t, b.(interface{ Close() error }).Close()) })
	marked := context.WithValue(ctx, protocolLatencyMarker{}, true)
	require.NoError(t, b.InitializeLeadershipRevision(marked, 0))
	members, err := b.CorruptAlarms(ctx)
	require.NoError(t, err)
	require.Empty(t, members)
	rpc.mu.Lock()
	limits := append([]uint32(nil), rpc.limits...)
	rpc.mu.Unlock()
	largePages := 0
	for _, limit := range limits {
		if limit == 2048 {
			largePages++
		}
	}
	require.GreaterOrEqual(t, largePages, 2, "full backend validation must traverse multiple actual SDK witness pages")
	require.Contains(t, limits, uint32(txnsnapshot.DefaultScanBatchSize), "event scans must retain default pages")
	// The last seal is beyond the first 2048-row page. Removing its event
	// after a healthy validation must still arm CORRUPT on the next promotion.
	batch := kv.BeginBatchWrite()
	batch.Del(ks.EncodeEventLogKey(last, key))
	require.NoError(t, batch.Commit(ctx))
	require.NoError(t, b.InitializeLeadershipRevision(ctx, 0))
	members, err = b.CorruptAlarms(ctx)
	require.NoError(t, err)
	require.NotEmpty(t, members)
	t.Logf("WITNESS_PAGE_CORRUPTION_CONFIRMED transactions=2050 witness_pages=%d revision=%d", largePages, last)
}

type scanPageRPCClient struct {
	clienttikv.Client
	mu     sync.Mutex
	limits []uint32
}

func (c *scanPageRPCClient) SendRequest(ctx context.Context, addr string, req *tikvrpc.Request, timeout time.Duration) (*tikvrpc.Response, error) {
	if ctx.Value(protocolLatencyMarker{}) == true && req.Type == tikvrpc.CmdScan {
		c.mu.Lock()
		c.limits = append(c.limits, req.Scan().Limit)
		c.mu.Unlock()
	}
	return c.Client.SendRequest(ctx, addr, req, timeout)
}

// Uses the actual pinned SDK scanner and adapter, but a single-Region mock
// server. Counts wire request attempts, not Raft latency or production speedup.
func TestScanPageHintChangesRPCCountWithoutChangingRows(t *testing.T) {
	client, cluster, pd, err := testutils.NewMockTiKV("", nil)
	require.NoError(t, err)
	testutils.BootstrapWithSingleStore(cluster)
	rpc := &scanPageRPCClient{Client: client}
	kvClient, err := clienttikv.NewKVStore("scan-page-rpc", clienttikv.NewCodecPDClient(clienttikv.ModeTxn, pd), clienttikv.NewMockSafePointKV(), rpc)
	require.NoError(t, err)
	kv := NewKvStoreWithStorage([]*clienttikv.KVStore{kvClient})
	t.Cleanup(func() { require.NoError(t, kv.Close()) })
	ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
	defer cancel()
	const rows = 4097
	value := bytes.Repeat([]byte{1}, 37)
	for first := 0; first < rows; first += 256 {
		batch := kv.BeginBatchWrite()
		for i := first; i < min(first+256, rows); i++ {
			batch.Put([]byte(fmt.Sprintf("scan-seals/%08d", i)), value, 0)
		}
		require.NoError(t, batch.Commit(ctx))
	}
	for _, hint := range []int{0, 128, 2048} {
		t.Run(fmt.Sprint(hint), func(t *testing.T) {
			marked := context.WithValue(ctx, protocolLatencyMarker{}, true)
			if hint != 0 {
				marked = storage.WithScanBatchSize(marked, hint)
			}
			rpc.mu.Lock()
			rpc.limits = nil
			rpc.mu.Unlock()
			iter, err := kv.Iter(marked, []byte("scan-seals/"), []byte("scan-seals0"), 0, 0)
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, iter.Close()) })
			for i := 0; i < rows; i++ {
				require.NoError(t, iter.Next(marked))
				require.Equal(t, []byte(fmt.Sprintf("scan-seals/%08d", i)), iter.Key())
				require.Equal(t, value, iter.Val())
			}
			require.ErrorIs(t, iter.Next(marked), io.EOF)
			size := hint
			if size == 0 {
				size = txnsnapshot.DefaultScanBatchSize
			}
			rpc.mu.Lock()
			limits := append([]uint32(nil), rpc.limits...)
			rpc.mu.Unlock()
			require.Len(t, limits, (rows+size-1)/size)
			for _, limit := range limits {
				require.EqualValues(t, size, limit)
			}
			t.Logf("SCAN_PAGE_RPC rows=%d hint=%d scan_attempts=%d", rows, hint, len(limits))
		})
	}
}
