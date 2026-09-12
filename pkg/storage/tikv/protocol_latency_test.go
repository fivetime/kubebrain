package tikv

import (
	"bytes"
	"context"
	"sync"
	"sync/atomic"
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

func TestRealTiKVBackendConcurrentWrites(t *testing.T) {
	testRealTiKVBackendScenario(t, "concurrent")
}

func TestProtocolConcurrentFixtureBound(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
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
	verifyProtocolConcurrentWrites(t, ctx, b)
	keys, err := protocolBackendKeys(ctx, kv, prefix)
	require.NoError(t, err)
	require.LessOrEqual(t, len(keys), 100)
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

// Contend on both user rows and the allocator in an independently owned fixture.
// This is separate from the sequential RPC-count measurement and cleanup budget.
func verifyProtocolConcurrentWrites(t *testing.T, ctx context.Context, b backend.Backend) {
	t.Helper()
	const workers = 4
	left, right := []byte("/integration/contention/left"), []byte("/integration/contention/right")
	base := b.GetCurrentRevision()
	usageBefore, _, _, err := b.QuotaStatus(ctx)
	require.NoError(t, err)
	watchCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	watch, err := b.Watch(watchCtx, "/integration/contention/", base+1)
	require.NoError(t, err)
	type result struct {
		revision uint64
		value    []byte
		err      error
	}
	results := make(chan result, workers)
	start := make(chan struct{})
	type readResult struct {
		response        *proto.RangeResponse
		err             error
		completedBefore uint64
	}
	var acknowledged atomic.Uint64
	const readSamples = 16
	reads := make(chan []readResult, 1)
	go func() {
		<-start
		observed := make([]readResult, 0, readSamples)
		for i := 0; i < readSamples; i++ {
			floor := acknowledged.Load()
			response, err := b.List(ctx, &proto.RangeRequest{
				Key: []byte("/integration/contention/"), End: []byte("/integration/contention0"),
			})
			observed = append(observed, readResult{response: response, err: err, completedBefore: floor})
			if err != nil {
				break
			}
		}
		reads <- observed
	}()
	for i := 0; i < workers; i++ {
		go func(i int) {
			<-start
			value := []byte{byte('a' + i)}
			_, revision, err := b.TxnApply(ctx, []backend.TxnWriteOp{
				{Key: left, Value: value}, {Key: right, Value: value},
			}, nil)
			if err == nil {
				for old := acknowledged.Load(); revision > old; old = acknowledged.Load() {
					if acknowledged.CompareAndSwap(old, revision) {
						break
					}
				}
			}
			results <- result{revision: revision, value: value, err: err}
		}(i)
	}
	close(start)
	// Drain all workers before assertions so failure cannot race backend cleanup.
	outcomes := make([]result, workers)
	for i := range outcomes {
		outcomes[i] = <-results
	}
	observations := <-reads
	// This sample cannot race an unfinished writer, and must exercise the
	// acknowledgement floor even if all concurrent reads ran before any commit.
	finalRange, finalErr := b.List(ctx, &proto.RangeRequest{
		Key: []byte("/integration/contention/"), End: []byte("/integration/contention0"),
	})
	observations = append(observations, readResult{response: finalRange, err: finalErr, completedBefore: acknowledged.Load()})
	values := make(map[uint64][]byte, workers)
	for _, outcome := range outcomes {
		require.NoError(t, outcome.err)
		require.Greater(t, outcome.revision, base)
		require.LessOrEqual(t, outcome.revision, base+workers)
		require.NotContains(t, values, outcome.revision, "concurrent transactions must not reuse a revision")
		values[outcome.revision] = outcome.value
	}
	require.Equal(t, base+workers, acknowledged.Load())
	require.Len(t, observations, readSamples+1)
	var lastObserved uint64
	for _, observation := range observations {
		require.NoError(t, observation.err)
		require.NotNil(t, observation.response)
		require.False(t, observation.response.More)
		kvs := observation.response.Kvs
		if len(kvs) == 0 {
			require.Zero(t, observation.completedBefore, "Range after acknowledged write must not be empty")
			require.Zero(t, lastObserved, "sequential Range snapshots must not regress to empty")
			continue // Valid if this snapshot predates all four commits.
		}
		require.Len(t, kvs, 2, "concurrent Range must not observe half a transaction")
		require.NotNil(t, kvs[0])
		require.NotNil(t, kvs[1])
		require.ElementsMatch(t, [][]byte{left, right}, [][]byte{kvs[0].Key, kvs[1].Key})
		require.Equal(t, kvs[0].Revision, kvs[1].Revision)
		require.GreaterOrEqual(t, kvs[0].Revision, observation.completedBefore, "Range must include writes acknowledged before its invocation")
		require.GreaterOrEqual(t, kvs[0].Revision, lastObserved, "sequential Range revisions must not regress")
		lastObserved = kvs[0].Revision
		require.Contains(t, values, kvs[0].Revision)
		for _, kv := range kvs {
			require.Equal(t, values[kv.Revision], backend.StripInlineValue(kv.Value))
		}
	}
	for revision := base + 1; revision <= base+workers; revision++ {
		events := protocolNextMutation(t, ctx, watch)
		require.Len(t, events, 2, "both keys must be published in one revision batch")
		wantType := proto.Event_PUT
		if revision == base+1 {
			wantType = proto.Event_CREATE
		}
		keys := make([][]byte, 0, 2)
		for _, event := range events {
			require.Equal(t, wantType, event.Type)
			require.Equal(t, revision, event.Revision)
			require.Equal(t, values[revision], backend.StripInlineValue(event.Kv.Value))
			keys = append(keys, event.Kv.Key)
		}
		require.ElementsMatch(t, [][]byte{left, right}, keys)
	}
	for _, key := range [][]byte{left, right} {
		got, err := b.Get(ctx, &proto.GetRequest{Key: key})
		require.NoError(t, err)
		require.NotNil(t, got.Kv)
		require.Equal(t, base+workers, got.Kv.Revision)
		require.Equal(t, values[base+workers], backend.StripInlineValue(got.Kv.Value))
	}
	usage, _, alarm, err := b.QuotaStatus(ctx)
	require.NoError(t, err)
	require.False(t, alarm)
	require.EqualValues(t, usageBefore+int64(len(left)+len(right)+2), usage)
	t.Logf("PROTOCOL_CONCURRENT_WRITES_PASSED workers=%d writes_per_txn=2 range_samples=%d post_ack_samples=1 revisions=%d..%d scope=bounded_contention_not_fault_or_soak", workers, readSamples, base+1, base+workers)
}
