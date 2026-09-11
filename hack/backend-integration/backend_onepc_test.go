package backendonepc_test

import (
	"context"
	"errors"
	"net"
	"os"
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
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
)

// The client flag is deliberately not synchronized. Enable it once before any
// client/background worker exists, never between test cases or -count rounds.
func TestMain(m *testing.M) {
	clienttikv.EnableFailpoints()
	os.Exit(m.Run())
}

type userCommitMarker struct{}

type compatibleUnistoreClient struct{ *unistore.RPCClient }

func (*compatibleUnistoreClient) CloseAddr(string) error { return nil }

type markedResponseLoss struct {
	clienttikv.Client
	beforeDelivery bool
	armed          atomic.Bool
	hits           atomic.Int32
	attempts       atomic.Int32
	startTS        atomic.Uint64
	startChanged   atomic.Bool
	commitTS       atomic.Uint64
	commitChanged  atomic.Bool
}

func (c *markedResponseLoss) SendRequest(ctx context.Context, addr string, req *tikvrpc.Request, timeout time.Duration) (*tikvrpc.Response, error) {
	// Background checkpoint/metadata transactions must never consume the fault.
	marked := ctx.Value(userCommitMarker{}) == true && req.Type == tikvrpc.CmdPrewrite
	if marked {
		c.attempts.Add(1)
		startTS := req.Prewrite().GetStartVersion()
		if !c.startTS.CompareAndSwap(0, startTS) && c.startTS.Load() != startTS {
			c.startChanged.Store(true)
		}
	}
	inject := marked && req.Prewrite().GetTryOnePc() && c.armed.CompareAndSwap(true, false)
	if inject {
		c.hits.Add(1)
		if c.beforeDelivery {
			return nil, errors.New("injected user 1PC transport failure before delivery")
		}
	}
	response, err := c.Client.SendRequest(ctx, addr, req, timeout)
	if err != nil || response == nil {
		return response, err
	}
	if result, ok := response.Resp.(*kvrpcpb.PrewriteResponse); marked && ok && result.OnePcCommitTs != 0 {
		if !c.commitTS.CompareAndSwap(0, result.OnePcCommitTs) && c.commitTS.Load() != result.OnePcCommitTs {
			c.commitChanged.Store(true)
		}
		if inject {
			return nil, errors.New("injected loss of user 1PC committed response")
		}
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
	testBackendOnePCOutcome(t, false, true)
}

func TestBackendResolvesActualOnePCBeforeDelivery(t *testing.T) {
	testBackendOnePCOutcome(t, true, true)
}

func TestBackendResolvesActualOnePCDefaultRetryResponseLoss(t *testing.T) {
	testBackendOnePCOutcome(t, false, false)
}

func TestBackendResolvesActualOnePCDefaultRetryBeforeDelivery(t *testing.T) {
	testBackendOnePCOutcome(t, true, false)
}

func testBackendOnePCOutcome(t *testing.T, beforeDelivery, disableRetry bool) {
	t.Helper()
	t.Cleanup(tikvconfig.UpdateGlobal(func(cfg *tikvconfig.Config) {
		cfg.Enable1PC = true
		cfg.EnableAsyncCommit = false
	}))
	if disableRetry {
		require.NoError(t, failpoint.Enable("tikvclient/noRetryOnRpcError", "return(true)"))
		t.Cleanup(func() { require.NoError(t, failpoint.Disable("tikvclient/noRetryOnRpcError")) })
	} else {
		_, evalErr := failpoint.Eval("tikvclient/noRetryOnRpcError")
		require.Error(t, evalErr, "default retry test must not inherit a disabled-retry failpoint")
	}

	rpc, pdClient, cluster, err := unistore.New("")
	require.NoError(t, err)
	storeID, _, _ := unistore.BootstrapWithSingleStore(cluster)
	var healthChecks *atomic.Int32
	if !disableRetry {
		addr, checks := startHealthyMockStore(t)
		healthChecks = checks
		cluster.AddStore(storeID, addr)
	}
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
	t.Logf("beforeDelivery=%v disableRetry=%v err=%v revision=%d results=%d attempts=%d commitTS=%d",
		beforeDelivery, disableRetry, err, revision, len(results), fault.attempts.Load(), fault.commitTS.Load())
	uncertain := errors.Is(err, storage.ErrUncertainResult)
	if disableRetry || !beforeDelivery {
		require.ErrorIs(t, err, storage.ErrUncertainResult)
		require.Nil(t, results)
	} else {
		require.NoError(t, err)
		require.Len(t, results, 2)
	}
	if !disableRetry {
		require.Equal(t, int32(2), fault.attempts.Load(), "one injected loss must produce exactly one RPC retry")
		require.False(t, fault.startChanged.Load(), "retry must retain the original transaction start timestamp")
		require.Positive(t, healthChecks.Load(), "default retry must pass the real gRPC health probe")
	} else {
		require.Equal(t, int32(1), fault.attempts.Load(), "disabled-retry cases must not hide a successful resend")
	}
	require.NotZero(t, fault.startTS.Load())
	require.False(t, fault.commitChanged.Load(), "same transaction must not commit at multiple timestamps")
	uncommitted := beforeDelivery && disableRetry
	require.Greater(t, revision, uint64(100))
	require.Equal(t, int32(1), fault.hits.Load())
	if uncommitted {
		require.Zero(t, fault.commitTS.Load())
		require.Eventually(t, metric.notCommitted.Load, 5*time.Second, time.Millisecond,
			"real backend resolver did not establish absence of the candidate")
		require.False(t, metric.resolved.Load())
		require.Equal(t, uint64(100), b.GetCurrentRevision(), "uncommitted candidate must not advance public revision")
	} else {
		require.NotZero(t, fault.commitTS.Load(), "committed result must follow an actual server-side 1PC commit")
		require.Eventually(t, func() bool {
			return (!uncertain || metric.resolved.Load()) && b.GetCurrentRevision() >= revision
		}, 5*time.Second, time.Millisecond, "real backend witness resolver did not finish")
		require.False(t, metric.notCommitted.Load())
		if !uncertain {
			require.False(t, metric.resolved.Load(), "successful retry must not require background uncertain resolution")
		}
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
		if uncommitted {
			require.Nil(t, result.Kv, "undelivered transaction must leave both user keys absent")
			continue
		}
		require.NotNil(t, result.Kv)
		require.Equal(t, expected, string(backend.StripInlineValue(result.Kv.Value)))
		require.Equal(t, revision, result.Kv.Revision)
	}
	_, nextRevision, err := b.TxnApply(readCtx, []backend.TxnWriteOp{{Key: left, Value: []byte("next")}}, nil)
	require.NoError(t, err)
	if uncommitted {
		require.Equal(t, revision, nextRevision, "absent candidate must be reused without a revision hole")
	} else {
		require.Equal(t, revision+1, nextRevision, "resolving the uncertain batch must not rewrite keys at new revisions")
	}
	// This subsequent acknowledged write provides an ordered event boundary:
	// an absent candidate must have emitted nothing, and a committed candidate
	// must not be replayed again ahead of this next mutation.
	next := nextMutationBatch(t, watchCtx, watch)
	require.Len(t, next, 1)
	if uncommitted {
		require.Equal(t, proto.Event_CREATE, next[0].Type)
	} else {
		require.Equal(t, proto.Event_PUT, next[0].Type)
	}
	require.Equal(t, nextRevision, next[0].Revision)
	require.NotNil(t, next[0].Kv)
	require.Equal(t, string(left), string(next[0].Kv.Key))
	require.Equal(t, "next", string(backend.StripInlineValue(next[0].Kv.Value)))
}

func startHealthyMockStore(t *testing.T) (string, *atomic.Int32) {
	t.Helper()
	// RegionRequestSender probes health through actual gRPC, independently of
	// the injected data client. The unistore default address "store1" cannot
	// answer it. Serve only health on loopback; data still stays in the mock.
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	checks := new(atomic.Int32)
	server := grpc.NewServer(grpc.UnaryInterceptor(func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		resp, err := handler(ctx, req)
		if err == nil && info.FullMethod == "/grpc.health.v1.Health/Check" {
			checks.Add(1)
		}
		return resp, err
	}))
	service := health.NewServer()
	service.SetServingStatus("", healthpb.HealthCheckResponse_SERVING)
	healthpb.RegisterHealthServer(server, service)
	done := make(chan error, 1)
	go func() { done <- server.Serve(listener) }()
	t.Cleanup(func() {
		server.Stop()
		select {
		case err := <-done:
			if !errors.Is(err, grpc.ErrServerStopped) {
				require.NoError(t, err)
			}
		case <-time.After(5 * time.Second):
			t.Error("mock health server did not stop")
		}
	})
	return listener.Addr().String(), checks
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
