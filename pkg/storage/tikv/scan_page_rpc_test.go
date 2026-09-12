package tikv

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/kubewharf/kubebrain/pkg/storage"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/testutils"
	clienttikv "github.com/tikv/client-go/v2/tikv"
	"github.com/tikv/client-go/v2/tikvrpc"
	"github.com/tikv/client-go/v2/txnkv/txnsnapshot"
)

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
