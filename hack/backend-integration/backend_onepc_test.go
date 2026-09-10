package backendonepc_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	proto "github.com/kubewharf/kubebrain-client/api/v2rpc"
	"github.com/kubewharf/kubebrain/pkg/backend"
	"github.com/kubewharf/kubebrain/pkg/metrics"
	metricmock "github.com/kubewharf/kubebrain/pkg/metrics/mock"
	"github.com/kubewharf/kubebrain/pkg/storage"
	storagetikv "github.com/kubewharf/kubebrain/pkg/storage/tikv"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/tidb/store/mockstore/unistore"
	"github.com/stretchr/testify/require"
	tikvconfig "github.com/tikv/client-go/v2/config"
	clienttikv "github.com/tikv/client-go/v2/tikv"
	"github.com/tikv/client-go/v2/tikvrpc"
)

type userCommitMarker struct{}

type compatibleUnistoreClient struct{ *unistore.RPCClient }

func (*compatibleUnistoreClient) CloseAddr(string) error { return nil }

type markedResponseLoss struct {
	clienttikv.Client
	beforeDelivery bool
	armed          atomic.Bool
	hits           atomic.Int32
	commitTS       atomic.Uint64
}

func (c *markedResponseLoss) SendRequest(ctx context.Context, addr string, req *tikvrpc.Request, timeout time.Duration) (*tikvrpc.Response, error) {
	// Background checkpoint/metadata transactions must never consume the fault.
	if ctx.Value(userCommitMarker{}) != true || req.Type != tikvrpc.CmdPrewrite ||
		!req.Prewrite().GetTryOnePc() || !c.armed.CompareAndSwap(true, false) {
		return c.Client.SendRequest(ctx, addr, req, timeout)
	}
	c.hits.Add(1)
	if c.beforeDelivery {
		return nil, errors.New("injected user 1PC transport failure before delivery")
	}
	response, err := c.Client.SendRequest(ctx, addr, req, timeout)
	if err != nil || response == nil {
		return response, err
	}
	if result, ok := response.Resp.(*kvrpcpb.PrewriteResponse); ok && result.OnePcCommitTs != 0 {
		c.commitTS.Store(result.OnePcCommitTs)
		return nil, errors.New("injected loss of user 1PC committed response")
	}
	return response, nil
}

type resolutionMetrics struct {
	metrics.Metrics
	resolved     atomic.Bool
	notCommitted atomic.Bool
}

func (m *resolutionMetrics) EmitCounter(name string, value interface{}, tags ...metrics.T) error {
	positive := false
	switch n := value.(type) {
	case int:
		positive = n > 0
	case int64:
		positive = n > 0
	}
	if name == "txn.uncertain.resolve.committed" && positive {
		m.resolved.Store(true)
	}
	if name == "txn.uncertain.resolve.not_committed" && positive {
		m.notCommitted.Store(true)
	}
	return m.Metrics.EmitCounter(name, value, tags...)
}

// Actual KubeBrain adapter/backend over a local mock server.
// This does not establish real TiKV persistence or upgrade acceptance.
func TestBackendResolvesActualOnePCResponseLoss(t *testing.T) {
	testBackendOnePCOutcome(t, false)
}

func TestBackendResolvesActualOnePCBeforeDelivery(t *testing.T) {
	testBackendOnePCOutcome(t, true)
}

func testBackendOnePCOutcome(t *testing.T, beforeDelivery bool) {
	t.Helper()
	clienttikv.EnableFailpoints()
	t.Cleanup(tikvconfig.UpdateGlobal(func(cfg *tikvconfig.Config) {
		cfg.Enable1PC = true
		cfg.EnableAsyncCommit = false
	}))
	require.NoError(t, failpoint.Enable("tikvclient/noRetryOnRpcError", "return(true)"))
	t.Cleanup(func() { require.NoError(t, failpoint.Disable("tikvclient/noRetryOnRpcError")) })

	rpc, pdClient, cluster, err := unistore.New("")
	require.NoError(t, err)
	unistore.BootstrapWithSingleStore(cluster)
	store, err := clienttikv.NewTestTiKVStore(&compatibleUnistoreClient{rpc}, pdClient, nil, nil, 0)
	require.NoError(t, err)
	fault := &markedResponseLoss{Client: store.GetTiKVClient(), beforeDelivery: beforeDelivery}
	store.SetTiKVClient(fault)
	metric := &resolutionMetrics{Metrics: metricmock.NewMinimalMetrics(gomock.NewController(t))}
	b := backend.NewBackend(storagetikv.NewKvStoreWithStorage([]*clienttikv.KVStore{store}), backend.Config{
		Prefix: "/integration/onepc", Keyspace: "mock-onepc-response-loss",
		Identity: "mock-onepc-backend", EnableEtcdCompatibility: true,
	}, metric)
	closer, ok := b.(interface{ Close() error })
	require.True(t, ok, "backend must expose its shutdown lifecycle")
	t.Cleanup(func() { require.NoError(t, closer.Close()) })
	b.SetCurrentRevision(100)
	watchCtx, watchCancel := context.WithTimeout(context.Background(), 10*time.Second)
	t.Cleanup(watchCancel)
	watch, err := b.Watch(watchCtx, "/integration/onepc/", 101)
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.WithValue(context.Background(), userCommitMarker{}, true), 10*time.Second)
	defer cancel()
	left, right := []byte("/integration/onepc/left"), []byte("/integration/onepc/right")
	fault.armed.Store(true)
	results, revision, err := b.TxnApply(ctx, []backend.TxnWriteOp{
		{Key: left, Value: []byte("left-value")},
		{Key: right, Value: []byte("right-value")},
	}, nil)
	require.ErrorIs(t, err, storage.ErrUncertainResult)
	require.Nil(t, results)
	require.Greater(t, revision, uint64(100))
	require.Equal(t, int32(1), fault.hits.Load())
	if beforeDelivery {
		require.Zero(t, fault.commitTS.Load())
		require.Eventually(t, metric.notCommitted.Load, 5*time.Second, time.Millisecond,
			"real backend resolver did not establish absence of the candidate")
		require.False(t, metric.resolved.Load())
		require.Equal(t, uint64(100), b.GetCurrentRevision(), "uncommitted candidate must not advance public revision")
	} else {
		require.NotZero(t, fault.commitTS.Load(), "fault must follow an actual server-side 1PC commit")
		require.Eventually(t, func() bool {
			return metric.resolved.Load() && b.GetCurrentRevision() >= revision
		}, 5*time.Second, time.Millisecond, "real backend witness resolver did not finish")
		require.False(t, metric.notCommitted.Load())
		batch := nextMutationBatch(t, watchCtx, watch)
		require.Len(t, batch, 2, "resolved atomic transaction must publish both keys together")
		seen := make(map[string]string)
		for _, event := range batch {
			// Internal backend API distinguishes CREATE from PUT; the etcd
			// compatibility adapter maps both to an external PUT event.
			require.Equal(t, proto.Event_CREATE, event.Type)
			require.Equal(t, revision, event.Revision)
			require.NotNil(t, event.Kv)
			require.Equal(t, revision, event.Kv.Revision)
			seen[string(event.Kv.Key)] = string(backend.StripInlineValue(event.Kv.Value))
		}
		require.Equal(t, map[string]string{string(left): "left-value", string(right): "right-value"}, seen)
	}

	readCtx, readCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer readCancel()
	for key, expected := range map[string]string{string(left): "left-value", string(right): "right-value"} {
		result, readErr := b.Get(readCtx, &proto.GetRequest{Key: []byte(key)})
		require.NoError(t, readErr)
		if beforeDelivery {
			require.Nil(t, result.Kv, "undelivered transaction must leave both user keys absent")
			continue
		}
		require.NotNil(t, result.Kv)
		require.Equal(t, expected, string(backend.StripInlineValue(result.Kv.Value)))
		require.Equal(t, revision, result.Kv.Revision)
	}
	_, nextRevision, err := b.TxnApply(readCtx, []backend.TxnWriteOp{{Key: left, Value: []byte("next")}}, nil)
	require.NoError(t, err)
	if beforeDelivery {
		require.Equal(t, revision, nextRevision, "absent candidate must be reused without a revision hole")
	} else {
		require.Equal(t, revision+1, nextRevision, "resolving the uncertain batch must not rewrite keys at new revisions")
	}
	// This subsequent acknowledged write provides an ordered event boundary:
	// an absent candidate must have emitted nothing, and a committed candidate
	// must not be replayed again ahead of this next mutation.
	next := nextMutationBatch(t, watchCtx, watch)
	require.Len(t, next, 1)
	if beforeDelivery {
		require.Equal(t, proto.Event_CREATE, next[0].Type)
	} else {
		require.Equal(t, proto.Event_PUT, next[0].Type)
	}
	require.Equal(t, nextRevision, next[0].Revision)
	require.NotNil(t, next[0].Kv)
	require.Equal(t, string(left), string(next[0].Kv.Key))
	require.Equal(t, "next", string(backend.StripInlineValue(next[0].Kv.Value)))
}

func nextMutationBatch(t *testing.T, ctx context.Context, watch <-chan []*proto.Event) []*proto.Event {
	t.Helper()
	for {
		select {
		case batch, open := <-watch:
			require.True(t, open, "watch closed before expected transaction")
			if backend.IsProgressMarker(batch) {
				continue
			}
			require.NotEmpty(t, batch)
			return batch
		case <-ctx.Done():
			t.Fatal("watch did not publish expected transaction before deadline")
			return nil
		}
	}
}
