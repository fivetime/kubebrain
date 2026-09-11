package tikv

import (
	"bytes"
	"context"
	"sync"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	proto "github.com/kubewharf/kubebrain-client/api/v2rpc"
	"github.com/kubewharf/kubebrain/pkg/backend"
	metricmock "github.com/kubewharf/kubebrain/pkg/metrics/mock"
	"github.com/kubewharf/kubebrain/pkg/storage/memkv"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/stretchr/testify/require"
	clienttikv "github.com/tikv/client-go/v2/tikv"
	"github.com/tikv/client-go/v2/tikvrpc"
)

type protocolLatencyMarker struct{}

type protocolLatencyStats struct {
	Prewrite, OnePC, Commit, Errors int
}

// Count only measured user RPCs, not seed/warmup, checkpoint or cleanup work.
// No fault injection and no process-global success counters.
type protocolLatencyClient struct {
	clienttikv.Client
	mu       sync.Mutex
	stats    protocolLatencyStats
	attempts map[string]int
}

func (c *protocolLatencyClient) requestSnapshot() map[string]int {
	c.mu.Lock()
	defer c.mu.Unlock()
	copy := make(map[string]int, len(c.attempts))
	for method, count := range c.attempts {
		copy[method] = count
	}
	return copy
}

func (c *protocolLatencyClient) snapshot() protocolLatencyStats {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.stats
}

func (c *protocolLatencyClient) SendRequest(ctx context.Context, addr string, req *tikvrpc.Request, timeout time.Duration) (*tikvrpc.Response, error) {
	if ctx.Value(protocolLatencyMarker{}) == true {
		method := "other"
		switch req.Type {
		case tikvrpc.CmdGet:
			method = "get"
		case tikvrpc.CmdBatchGet:
			method = "batch_get"
		case tikvrpc.CmdScan:
			method = "scan"
		case tikvrpc.CmdPrewrite:
			method = "prewrite"
		case tikvrpc.CmdCommit:
			method = "commit"
		case tikvrpc.CmdCheckTxnStatus:
			method = "check_txn_status"
		case tikvrpc.CmdResolveLock:
			method = "resolve_lock"
		}
		c.mu.Lock()
		if c.attempts == nil {
			c.attempts = make(map[string]int)
		}
		c.attempts[method]++
		c.mu.Unlock()
	}
	response, err := c.Client.SendRequest(ctx, addr, req, timeout)
	if ctx.Value(protocolLatencyMarker{}) != true {
		return response, err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if err != nil || response == nil {
		c.stats.Errors++
		return response, err
	}
	switch r := response.Resp.(type) {
	case *kvrpcpb.PrewriteResponse:
		if r.RegionError != nil || len(r.Errors) != 0 {
			c.stats.Errors++
		} else {
			c.stats.Prewrite++
			if r.OnePcCommitTs != 0 {
				c.stats.OnePC++
			}
		}
	case *kvrpcpb.CommitResponse:
		if r.RegionError != nil || r.Error != nil {
			c.stats.Errors++
		} else {
			c.stats.Commit++
		}
	}
	return response, err
}

func TestProtocolLatencyClientScope(t *testing.T) {
	stub := &protocolResponseStub{response: &tikvrpc.Response{Resp: &kvrpcpb.PrewriteResponse{OnePcCommitTs: 20}}}
	c := &protocolLatencyClient{Client: stub}
	req := tikvrpc.NewRequest(tikvrpc.CmdPrewrite, &kvrpcpb.PrewriteRequest{})
	_, err := c.SendRequest(context.Background(), "unused", req, time.Second)
	require.NoError(t, err)
	require.Equal(t, protocolLatencyStats{}, c.snapshot())
	require.Empty(t, c.requestSnapshot())
	ctx := context.WithValue(context.Background(), protocolLatencyMarker{}, true)
	_, err = c.SendRequest(ctx, "unused", req, time.Second)
	require.NoError(t, err)
	stub.response = &tikvrpc.Response{Resp: &kvrpcpb.CommitResponse{}}
	_, err = c.SendRequest(ctx, "unused", req, time.Second)
	require.NoError(t, err)
	stub.response = nil
	_, _ = c.SendRequest(ctx, "unused", req, time.Second)
	require.Equal(t, protocolLatencyStats{Prewrite: 1, OnePC: 1, Commit: 1, Errors: 1}, c.snapshot())
	// Requests, including failed ones, are counted independently of response
	// bodies. This deliberately mismatched stub must not relabel the request.
	require.Equal(t, map[string]int{"prewrite": 3}, c.requestSnapshot())
	copy := c.requestSnapshot()
	copy["prewrite"] = 99
	require.Equal(t, 3, c.requestSnapshot()["prewrite"])
}

func TestRealTiKVBackendProtocolLatency(t *testing.T) {
	testRealTiKVBackendScenario(t, "latency")
}

func TestProtocolLatencyFixtureBound(t *testing.T) {
	ctx := context.Background()
	prefix := "kubebrain/protocol-smoke/0123456789abcdef0123456789abcdef/"
	ks, _, err := protocolBackendScope(prefix)
	require.NoError(t, err)
	kv := memkv.NewKvStorage()
	b := backend.NewBackend(kv, backend.Config{Prefix: prefix + "backend", Keyspace: ks.Name(),
		Identity: ks.Name(), EnableEtcdCompatibility: true, QuotaBackendBytes: 2 << 30},
		metricmock.NewMinimalMetrics(gomock.NewController(t)))
	t.Cleanup(func() { require.NoError(t, b.(interface{ Close() error }).Close()) })
	b.SetCurrentRevision(100)
	require.NoError(t, b.EnsureQuotaInitialized(ctx))
	for i := 0; i < 1+10+20; i++ {
		_, _, err := b.TxnApply(ctx, []backend.TxnWriteOp{{Key: []byte("/integration/latency/key"), Value: bytes.Repeat([]byte("v"), 256)}}, nil)
		require.NoError(t, err)
	}
	keys, err := protocolBackendKeys(ctx, kv, prefix)
	require.NoError(t, err)
	require.LessOrEqual(t, len(keys), 100, "reserve cleanup headroom for real checkpoint metadata and owner claim")
}

// Fixed small blocks fit the existing 128-key ownership-fenced cleanup bound.
// Repeat separate processes in ABBA order; never compare one cold transaction.
func measureProtocolBackendLatency(t *testing.T, ctx context.Context, b backend.Backend, client *protocolLatencyClient, mode string) {
	t.Helper()
	const warmup, samples = 10, 20
	require.NoError(t, b.EnsureQuotaInitialized(ctx))
	key, value := []byte("/integration/latency/key"), bytes.Repeat([]byte("v"), 256)
	last := uint64(100)
	for i := 0; i <= warmup; i++ {
		_, revision, err := b.TxnApply(ctx, []backend.TxnWriteOp{{Key: key, Value: value}}, nil)
		require.NoError(t, err)
		require.Equal(t, last+1, revision)
		last = revision
	}
	watch, err := b.Watch(ctx, string(key), last+1)
	require.NoError(t, err)
	measured := context.WithValue(ctx, protocolLatencyMarker{}, true)
	durations := make([]int64, 0, samples)
	for i := 0; i < samples; i++ {
		started := time.Now()
		_, revision, err := b.TxnApply(measured, []backend.TxnWriteOp{{Key: key, Value: value}}, nil)
		durations = append(durations, time.Since(started).Nanoseconds())
		require.NoError(t, err)
		require.Equal(t, last+1, revision)
		last = revision
		events := protocolNextMutation(t, ctx, watch)
		require.Len(t, events, 1)
		require.Equal(t, proto.Event_PUT, events[0].Type)
		require.Equal(t, revision, events[0].Revision)
		require.Equal(t, key, events[0].Kv.Key)
		require.Equal(t, value, backend.StripInlineValue(events[0].Kv.Value))
	}
	got, err := b.Get(ctx, &proto.GetRequest{Key: key})
	require.NoError(t, err)
	require.NotNil(t, got.Kv)
	require.Equal(t, last, got.Kv.Revision)
	require.Equal(t, value, backend.StripInlineValue(got.Kv.Value))
	usage, quota, alarm, err := b.QuotaStatus(ctx)
	require.NoError(t, err)
	require.False(t, alarm)
	require.EqualValues(t, 2<<30, quota)
	require.EqualValues(t, len(key)+len(value), usage)
	stats := client.snapshot()
	want := protocolLatencyStats{Prewrite: samples, Commit: samples}
	if mode == "1pc" {
		want = protocolLatencyStats{Prewrite: samples, OnePC: samples}
	}
	require.Equal(t, want, stats, "every measured transaction must use the requested single-Region protocol without retries")
	attempts := client.requestSnapshot()
	require.Equal(t, samples, attempts["prewrite"], "include unsuccessful RPC attempts, not only successful responses")
	require.Equal(t, want.Commit, attempts["commit"])
	require.Equal(t, 2*samples, attempts["get"], "allocator read must share the transactional guard prefetch")
	require.Equal(t, 3*samples, attempts["batch_get"], "prefetch must not add an extra wire request")
	t.Logf("PROTOCOL_BACKEND_LATENCY mode=%s warmup=%d samples=%d value_bytes=%d quota=%d durations_ns=%v counts=%+v scope=backend_only", mode, warmup, samples, len(value), quota, durations, stats)
	t.Logf("PROTOCOL_BACKEND_RPC_ATTEMPTS counts=%v scope=marked_foreground_only excludes=background_and_unmarked_work", attempts)
}
