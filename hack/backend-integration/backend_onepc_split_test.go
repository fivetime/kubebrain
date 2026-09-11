package backendonepc_test

import (
	"bytes"
	"context"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	proto "github.com/kubewharf/kubebrain-client/api/v2rpc"
	"github.com/kubewharf/kubebrain/pkg/backend"
	metricmock "github.com/kubewharf/kubebrain/pkg/metrics/mock"
	storagetikv "github.com/kubewharf/kubebrain/pkg/storage/tikv"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/tidb/store/mockstore/unistore"
	"github.com/stretchr/testify/require"
	tikvconfig "github.com/tikv/client-go/v2/config"
	clienttikv "github.com/tikv/client-go/v2/tikv"
	"github.com/tikv/client-go/v2/tikvrpc"
)

// Split only the marked transaction's first 1PC prewrite, after the client has
// grouped its mutations. The real client must handle the stale region epoch.
type splitDuringOnePC struct {
	clienttikv.Client
	mu              sync.Mutex
	split           func(uint64, []byte)
	splits          int
	startTS         uint64
	startChanged    bool
	onePCCommitted  bool
	prewriteRegions map[uint64]bool
	commits         int
}

func (c *splitDuringOnePC) SendRequest(ctx context.Context, addr string, req *tikvrpc.Request, timeout time.Duration) (*tikvrpc.Response, error) {
	marked := ctx.Value(userCommitMarker{}) == true
	if marked && req.Type == tikvrpc.CmdPrewrite {
		p := req.Prewrite()
		c.mu.Lock()
		if c.startTS == 0 {
			c.startTS = p.StartVersion
		}
		c.startChanged = c.startChanged || c.startTS != p.StartVersion
		if c.splits == 0 && p.TryOnePc && len(p.Mutations) > 1 {
			keys := make([][]byte, len(p.Mutations))
			for i, mutation := range p.Mutations {
				keys[i] = append([]byte(nil), mutation.Key...)
			}
			sort.Slice(keys, func(i, j int) bool { return bytes.Compare(keys[i], keys[j]) < 0 })
			c.split(req.Context.RegionId, keys[len(keys)/2])
			c.splits++
		}
		c.mu.Unlock()
	}
	response, err := c.Client.SendRequest(ctx, addr, req, timeout)
	if marked && err == nil && response != nil {
		c.mu.Lock()
		switch r := response.Resp.(type) {
		case *kvrpcpb.PrewriteResponse:
			c.onePCCommitted = c.onePCCommitted || r.OnePcCommitTs != 0
			if r.RegionError == nil && len(r.Errors) == 0 && !req.Prewrite().TryOnePc {
				c.prewriteRegions[req.Context.RegionId] = true
			}
		case *kvrpcpb.CommitResponse:
			if r.RegionError == nil && r.Error == nil {
				c.commits++
			}
		}
		c.mu.Unlock()
	}
	return response, err
}

// Mock Region splitting exercises client fallback plus actual backend revision
// and watch batching. It does not establish real TiKV Raft durability.
func TestBackendResolvesActualOnePCRegionSplitFallback(t *testing.T) {
	t.Cleanup(tikvconfig.UpdateGlobal(func(cfg *tikvconfig.Config) {
		cfg.Enable1PC = true
		cfg.EnableAsyncCommit = false
	}))
	rpc, pdClient, cluster, err := unistore.New("")
	require.NoError(t, err)
	unistore.BootstrapWithSingleStore(cluster)
	store, err := clienttikv.NewTestTiKVStore(&compatibleUnistoreClient{rpc}, pdClient, nil, nil, 0)
	require.NoError(t, err)
	fault := &splitDuringOnePC{Client: store.GetTiKVClient(), prewriteRegions: make(map[uint64]bool)}
	fault.split = func(region uint64, key []byte) {
		newRegion, newPeer := cluster.AllocID(), cluster.AllocID()
		cluster.Split(region, newRegion, key, []uint64{newPeer}, newPeer)
	}
	store.SetTiKVClient(fault)
	b := backend.NewBackend(storagetikv.NewKvStoreWithStorage([]*clienttikv.KVStore{store}), backend.Config{
		Prefix: "/integration/onepc-split", Keyspace: "mock-onepc-split",
		Identity: "mock-onepc-split", EnableEtcdCompatibility: true,
	}, metricmock.NewMinimalMetrics(gomock.NewController(t)))
	t.Cleanup(func() { require.NoError(t, b.(interface{ Close() error }).Close()) })
	b.SetCurrentRevision(100)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	watch, err := b.Watch(ctx, "/integration/onepc-split/", 101)
	require.NoError(t, err)
	left, right := []byte("/integration/onepc-split/left"), []byte("/integration/onepc-split/right")
	results, revision, err := b.TxnApply(context.WithValue(ctx, userCommitMarker{}, true), []backend.TxnWriteOp{
		{Key: left, Value: []byte("left")}, {Key: right, Value: []byte("right")},
	}, nil)
	require.NoError(t, err)
	require.Len(t, results, 2)
	require.Equal(t, uint64(101), revision)
	fault.mu.Lock()
	splits, regions, commits := fault.splits, len(fault.prewriteRegions), fault.commits
	changed, onePC := fault.startChanged, fault.onePCCommitted
	fault.mu.Unlock()
	require.Equal(t, 1, splits, "must actually attempt 1PC before splitting")
	require.GreaterOrEqual(t, regions, 2, "fallback must prewrite multiple regions")
	require.Positive(t, commits, "fallback must use the 2PC commit protocol")
	require.False(t, changed, "region retry must preserve transaction start timestamp")
	require.False(t, onePC, "split transaction must not have a 1PC commit result")
	events := nextMutationBatch(t, ctx, watch)
	require.Len(t, events, 2)
	seen := make(map[string]string)
	for _, event := range events {
		require.Equal(t, proto.Event_CREATE, event.Type)
		require.Equal(t, revision, event.Revision)
		require.NotNil(t, event.Kv)
		require.Equal(t, revision, event.Kv.Revision)
		seen[string(event.Kv.Key)] = string(backend.StripInlineValue(event.Kv.Value))
	}
	require.Equal(t, map[string]string{string(left): "left", string(right): "right"}, seen)
	for key, value := range seen {
		read, err := b.Get(ctx, &proto.GetRequest{Key: []byte(key)})
		require.NoError(t, err)
		require.NotNil(t, read.Kv)
		require.Equal(t, value, string(backend.StripInlineValue(read.Kv.Value)))
		require.Equal(t, revision, read.Kv.Revision)
	}
	_, next, err := b.TxnApply(ctx, []backend.TxnWriteOp{{Key: left, Value: []byte("next")}}, nil)
	require.NoError(t, err)
	require.Equal(t, revision+1, next, "fallback must allocate only one revision")
	nextEvents := nextMutationBatch(t, ctx, watch)
	require.Len(t, nextEvents, 1, "no duplicate split-transaction events before next write")
	require.Equal(t, next, nextEvents[0].Revision)
	require.NotNil(t, nextEvents[0].Kv)
	require.Equal(t, left, nextEvents[0].Kv.Key)
	require.Equal(t, []byte("next"), backend.StripInlineValue(nextEvents[0].Kv.Value))
}
