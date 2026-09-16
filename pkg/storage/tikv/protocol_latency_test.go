package tikv

import (
	"bytes"
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	proto "github.com/kubewharf/kubebrain-client/api/v2rpc"
	"github.com/kubewharf/kubebrain/pkg/backend"
	metricmock "github.com/kubewharf/kubebrain/pkg/metrics/mock"
	"github.com/kubewharf/kubebrain/pkg/storage"
	"github.com/kubewharf/kubebrain/pkg/storage/memkv"
	"github.com/pingcap/kvproto/pkg/errorpb"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/stretchr/testify/require"
	clienttikv "github.com/tikv/client-go/v2/tikv"
	"github.com/tikv/client-go/v2/tikvrpc"
)

type protocolLatencyMarker struct{}
type protocolAsyncLatencyMarker struct{}

type protocolLatencyStats struct {
	Prewrite, OnePC, Commit, Errors int
	// RPC-level evidence, NOT a transaction-wide protocol verdict. Retries and
	// different Regions can produce both accepted and fallback replies. Requested
	// includes failed attempts; 1PC successes are never counted as async acceptance.
	AsyncRequested, AsyncAccepted, AsyncFallback int
}

// Count only measured user RPCs, not seed/warmup, checkpoint or cleanup work.
// No fault injection and no process-global success counters.
type protocolLatencyClient struct {
	clienttikv.Client
	mu       sync.Mutex
	stats    protocolLatencyStats
	attempts map[string]int
	reads    protocolLatencyReadStats
}

type protocolLatencyReadStats struct {
	GetLocked, BatchGetLocked, Errors int
}

func (c *protocolLatencyClient) readSnapshot() protocolLatencyReadStats {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.reads
}

// Count responses, not locked keys: one BatchGet reply may contain many locks,
// but causes only one next read attempt in the SDK's single-Region loop.
func protocolReadReply(regionError bool, keyErrors ...*kvrpcpb.KeyError) (locked, failed bool) {
	if regionError {
		return false, true
	}
	for _, err := range keyErrors {
		if err == nil {
			continue
		}
		if err.Locked == nil {
			return false, true
		}
		locked = true
	}
	return locked, false
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
		if req.Type == tikvrpc.CmdPrewrite && req.Prewrite().UseAsyncCommit {
			c.stats.AsyncRequested++
		}
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
	case *kvrpcpb.GetResponse:
		locked, failed := protocolReadReply(r.RegionError != nil, r.Error)
		if locked {
			c.reads.GetLocked++
		}
		if failed {
			c.reads.Errors++
		}
	case *kvrpcpb.BatchGetResponse:
		keyErrors := []*kvrpcpb.KeyError{r.Error}
		if r.Error == nil {
			for _, pair := range r.Pairs {
				keyErrors = append(keyErrors, pair.Error)
			}
		}
		locked, failed := protocolReadReply(r.RegionError != nil, keyErrors...)
		if locked {
			c.reads.BatchGetLocked++
		}
		if failed {
			c.reads.Errors++
		}
	case *kvrpcpb.PrewriteResponse:
		if r.RegionError != nil || len(r.Errors) != 0 {
			c.stats.Errors++
		} else {
			c.stats.Prewrite++
			if r.OnePcCommitTs != 0 {
				c.stats.OnePC++
			} else if req.Type == tikvrpc.CmdPrewrite && req.Prewrite().UseAsyncCommit {
				if r.MinCommitTs > req.Prewrite().StartVersion {
					c.stats.AsyncAccepted++
				} else if r.MinCommitTs == 0 {
					c.stats.AsyncFallback++
				}
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

func TestRealTiKVBackendAsyncProtocolLatency(t *testing.T) {
	testRealTiKVBackendScenario(t, "async-latency")
}

func TestRealTiKVBackendFencedProtocolLatency(t *testing.T) {
	testRealTiKVBackendScenario(t, "fenced-latency")
}

func TestRealTiKVBackendAsyncFencedProtocolLatency(t *testing.T) {
	testRealTiKVBackendScenario(t, "async-fenced-latency")
}

func TestProtocolLatencyAsyncEvidence(t *testing.T) {
	for _, tc := range []struct {
		name            string
		unmarked, plain bool
		response        *kvrpcpb.PrewriteResponse
		err             error
		want            protocolLatencyStats
	}{
		{name: "accepted", response: &kvrpcpb.PrewriteResponse{MinCommitTs: 11}, want: protocolLatencyStats{Prewrite: 1, AsyncRequested: 1, AsyncAccepted: 1}},
		{name: "fallback", response: &kvrpcpb.PrewriteResponse{}, want: protocolLatencyStats{Prewrite: 1, AsyncRequested: 1, AsyncFallback: 1}},
		{name: "one-pc-wins", response: &kvrpcpb.PrewriteResponse{OnePcCommitTs: 12, MinCommitTs: 11}, want: protocolLatencyStats{Prewrite: 1, OnePC: 1, AsyncRequested: 1}},
		{name: "invalid-timestamp", response: &kvrpcpb.PrewriteResponse{MinCommitTs: 10}, want: protocolLatencyStats{Prewrite: 1, AsyncRequested: 1}},
		{name: "not-requested", plain: true, response: &kvrpcpb.PrewriteResponse{MinCommitTs: 11}, want: protocolLatencyStats{Prewrite: 1}},
		{name: "unmarked", unmarked: true, response: &kvrpcpb.PrewriteResponse{MinCommitTs: 11}},
		{name: "region-error", response: &kvrpcpb.PrewriteResponse{MinCommitTs: 11, RegionError: &errorpb.Error{}}, want: protocolLatencyStats{Errors: 1, AsyncRequested: 1}},
		{name: "key-error", response: &kvrpcpb.PrewriteResponse{MinCommitTs: 11, Errors: []*kvrpcpb.KeyError{{}}}, want: protocolLatencyStats{Errors: 1, AsyncRequested: 1}},
		{name: "transport-error", err: errors.New("test transport failure"), want: protocolLatencyStats{Errors: 1, AsyncRequested: 1}},
		{name: "nil-response", want: protocolLatencyStats{Errors: 1, AsyncRequested: 1}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			stub := &protocolResponseStub{err: tc.err}
			if tc.response != nil {
				stub.response = &tikvrpc.Response{Resp: tc.response}
			}
			client := &protocolLatencyClient{Client: stub}
			ctx := context.Background()
			if !tc.unmarked {
				ctx = context.WithValue(ctx, protocolLatencyMarker{}, true)
			}
			req := tikvrpc.NewRequest(tikvrpc.CmdPrewrite, &kvrpcpb.PrewriteRequest{StartVersion: 10, UseAsyncCommit: !tc.plain})
			_, _ = client.SendRequest(ctx, "unused", req, time.Second)
			require.Equal(t, tc.want, client.snapshot())
		})
	}
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
	verifyProtocolMultiPreviousReads(t, ctx, b, nil)
	keys, err := protocolBackendKeys(ctx, kv, prefix)
	require.NoError(t, err)
	require.LessOrEqual(t, len(keys), 100)
}

// Reuse the two live rows after the contention check, without concurrent writers
// or retries in this measured window. Keep the original single-key measurement
// separate: this verifies multi-key dispatch, not rollout latency acceptance.
func verifyProtocolMultiPreviousReads(t *testing.T, ctx context.Context, b backend.Backend, client *protocolLatencyClient) {
	t.Helper()
	keys := [][]byte{[]byte("/integration/contention/left"), []byte("/integration/contention/right")}
	previous := make([][]byte, len(keys))
	last := b.GetCurrentRevision()
	for i, key := range keys {
		got, err := b.Get(ctx, &proto.GetRequest{Key: key})
		require.NoError(t, err)
		require.NotNil(t, got.Kv)
		require.Equal(t, last, got.Kv.Revision)
		previous[i] = bytes.Clone(backend.StripInlineValue(got.Kv.Value))
	}
	measured := context.WithValue(ctx, protocolLatencyMarker{}, true)
	const samples = 2
	for i := 0; i < samples; i++ {
		value := []byte{byte('x' + i)}
		result, revision, err := b.TxnApply(measured, []backend.TxnWriteOp{
			{Key: keys[0], Value: value}, {Key: keys[1], Value: value},
		}, nil)
		require.NoError(t, err)
		require.Equal(t, last+1, revision)
		require.Len(t, result, len(keys))
		for j, key := range keys {
			require.Equal(t, previous[j], backend.StripInlineValue(result[j].PrevValue))
			require.Equal(t, last, result[j].PrevRevision)
			got, err := b.Get(ctx, &proto.GetRequest{Key: key})
			require.NoError(t, err)
			require.NotNil(t, got.Kv)
			require.Equal(t, revision, got.Kv.Revision)
			require.Equal(t, value, backend.StripInlineValue(got.Kv.Value))
			previous[j] = bytes.Clone(value)
		}
		last = revision
	}
	if client != nil {
		require.Equal(t, protocolLatencyStats{Prewrite: samples, Commit: samples}, client.snapshot())
		attempts := client.requestSnapshot()
		require.Equal(t, map[string]int{"batch_get": 3 * samples, "prewrite": samples, "commit": samples}, attempts,
			"single-Region 2PC must batch both previous objects, with no point reads or retries")
		t.Logf("PROTOCOL_MULTI_PREVIOUS_RPC_ATTEMPTS samples=%d keys_per_txn=2 counts=%v scope=marked_foreground_only", samples, attempts)
	}
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
func measureProtocolBackendLatency(t *testing.T, ctx context.Context, b backend.Backend, client *protocolLatencyClient, mode string, fenced bool) {
	t.Helper()
	const warmup, samples = 10, 20
	require.NoError(t, b.EnsureQuotaInitialized(ctx))
	key, value := []byte("/integration/latency/key"), bytes.Repeat([]byte("v"), 256)
	writeCtx := ctx
	if mode == "async" {
		writeCtx = context.WithValue(ctx, protocolAsyncLatencyMarker{}, true)
	}
	last := uint64(100)
	for i := 0; i <= warmup; i++ {
		_, revision, err := b.TxnApply(writeCtx, []backend.TxnWriteOp{{Key: key, Value: value}}, nil)
		require.NoError(t, err)
		require.Equal(t, last+1, revision)
		last = revision
	}
	watch, err := b.Watch(ctx, string(key), last+1)
	require.NoError(t, err)
	measured := context.WithValue(writeCtx, protocolLatencyMarker{}, true)
	var observationMu sync.Mutex
	var observations []storage.BatchCommitObservation
	measured = storage.WithBatchCommitObserver(measured, func(o storage.BatchCommitObservation) {
		observationMu.Lock()
		observations = append(observations, o)
		observationMu.Unlock()
	})
	durations := make([]int64, 0, samples)
	prewrites := make([]int, 0, samples)
	for i := 0; i < samples; i++ {
		before := client.snapshot()
		started := time.Now()
		_, revision, err := b.TxnApply(measured, []backend.TxnWriteOp{{Key: key, Value: value}}, nil)
		durations = append(durations, time.Since(started).Nanoseconds())
		require.NoError(t, err)
		after := client.snapshot()
		writes := after.Prewrite - before.Prewrite
		require.Positive(t, writes)
		prewrites = append(prewrites, writes)
		if mode == "async" {
			// Every reply in this sequential batch must accept async; one
			// accepted Region is insufficient to classify the whole batch.
			require.Equal(t, writes, after.AsyncRequested-before.AsyncRequested)
			require.Equal(t, writes, after.AsyncAccepted-before.AsyncAccepted)
			require.Equal(t, before.AsyncFallback, after.AsyncFallback)
			require.Equal(t, before.OnePC, after.OnePC)
		}
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
	checked := stats
	if fenced {
		want.Prewrite = 0
		for _, count := range prewrites {
			want.Prewrite += count
		}
		// Secondary commit cleanup is not necessarily synchronous in 2PC
		// either. The per-batch observer verifies the foreground primary.
		want.Commit, checked.Commit = 0, 0
	}
	if mode == "async" {
		want = protocolLatencyStats{Prewrite: want.Prewrite, AsyncRequested: want.Prewrite, AsyncAccepted: want.Prewrite}
		// Async cleanup may inherit the marker and race this snapshot. Neither
		// its count nor its absence proves foreground commit latency.
		checked.Commit = 0
	}
	require.Equal(t, want, checked, "every measured batch must use the requested protocol without write retries")
	attempts := client.requestSnapshot()
	reads := client.readSnapshot()
	t.Logf("PROTOCOL_LATENCY_READ_REPLIES attempts=%v locked_replies=%+v", attempts, reads)
	require.Zero(t, reads.Errors, "read errors must not be mistaken for lock-resolution retries")
	require.Equal(t, want.Prewrite, attempts["prewrite"], "include unsuccessful RPC attempts, not only successful responses")
	if mode != "async" && !fenced {
		require.Equal(t, want.Commit, attempts["commit"])
	}
	getAttempts, batchAttempts := samples, 2*samples
	if mode == "async" || fenced {
		// Async success can precede lock cleanup; the next snapshot legitimately
		// resolves a lock and re-reads. Require evidence for every extra attempt,
		// not an arbitrary >= bound. Its latency remains inside TxnApply.
		getAttempts += reads.GetLocked
		batchAttempts += reads.BatchGetLocked
	}
	require.Equal(t, getAttempts, attempts["get"], "only previous-object reads plus observed lock-resolution retries")
	if fenced {
		require.GreaterOrEqual(t, attempts["batch_get"], batchAttempts+samples, "include production fence groups and observed lock-resolution retries")
	} else {
		require.Equal(t, batchAttempts, attempts["batch_get"], "quota admission must share prefetch; extra attempts require actual lock replies")
	}
	observationMu.Lock()
	observed := append([]storage.BatchCommitObservation(nil), observations...)
	observationMu.Unlock()
	require.Len(t, observed, samples, "one synchronous observation per measured user storage batch")
	for i, o := range observed {
		require.NoError(t, o.Err)
		outside, valid := protocolLatencyOutsideBatch(time.Duration(durations[i]), o)
		require.True(t, valid, "sample %d must contain the three disjoint batch phases", i)
		// Outside includes backend admission/preparation and completion; it is
		// not a measurement of just reads. SDK stages nest inside Commit and
		// lock RPC sums may overlap, so neither is subtracted from wall time.
		t.Logf("PROTOCOL_LATENCY_PHASE sample=%d total_ns=%d outside_batch_ns=%d begin_ns=%d prepare_ns=%d commit_ns=%d prewrite_ns=%d commit_ts_ns=%d primary_commit_ns=%d region_groups=%d lock_details=%t prepare_locks=%+v commit_locks=%+v scope=one_synchronous_batch nested_sdk_stages=true overlapping_lock_rpc_sums=true",
			i, durations[i], outside.Nanoseconds(), o.Begin.Nanoseconds(), o.Prepare.Nanoseconds(), o.Commit.Nanoseconds(),
			o.Prewrite.Nanoseconds(), o.CommitTS.Nanoseconds(), o.PrimaryCommit.Nanoseconds(), o.PrewriteRegionGroups,
			o.HasLockRPCDetails, o.PrepareLocks, o.CommitLocks)
		require.True(t, o.CommitAttempted)
		require.True(t, o.HasWriteDetails)
		if !fenced {
			require.Equal(t, 1, prewrites[i], "single-Region fixture without retries")
		}
		require.EqualValues(t, prewrites[i], o.PrewriteRegionGroups, "one successful RPC per group, without retries")
		require.True(t, o.HasPrewriteRPCDetails)
		require.EqualValues(t, prewrites[i], o.PrewriteRPCs.Requests)
		require.Positive(t, o.PrewriteRPCs.MaxDuration)
		if prewrites[i] == 1 {
			require.Equal(t, o.PrewriteRPCs.Duration, o.PrewriteRPCs.MaxDuration)
		}
		if mode == "async" {
			require.Zero(t, o.PrimaryWrite.SuccessfulRPCs)
		} else if fenced {
			require.Positive(t, o.PrimaryWrite.SuccessfulRPCs)
		}
		require.Zero(t, o.PrewriteRPCs.TransportErrors+o.PrewriteRPCs.RegionErrors+o.PrewriteRPCs.KeyErrors+o.PrewriteRPCs.MissingResponses)
	}
	t.Logf("PROTOCOL_PREWRITE_RPC_SCOPE samples=%d requests_per_batch=%v errors=0 scope=marked_foreground_only", len(observed), prewrites)
	t.Logf("PROTOCOL_BATCH_REGION_GROUPS samples=%d groups_per_batch=%v scope=marked_foreground_only", len(observed), prewrites)
	t.Logf("PROTOCOL_BACKEND_LATENCY mode=%s fenced=%t warmup=%d samples=%d value_bytes=%d quota=%d durations_ns=%v counts=%+v scope=backend_only", mode, fenced, warmup, samples, len(value), quota, durations, stats)
	t.Logf("PROTOCOL_BACKEND_RPC_ATTEMPTS counts=%v scope=marked_context excludes=unmarked_work async_commit_cleanup_may_be_included=true", attempts)
}

// Subtract one phase at a time to reject overflow as well as invalid timings.
// Only valid for the single synchronous batch asserted by this fixture.
func protocolLatencyOutsideBatch(total time.Duration, o storage.BatchCommitObservation) (time.Duration, bool) {
	if total < 0 {
		return 0, false
	}
	for _, phase := range []time.Duration{o.Begin, o.Prepare, o.Commit} {
		if phase < 0 || phase > total {
			return 0, false
		}
		total -= phase
	}
	return total, true
}

func TestProtocolLatencyOutsideBatch(t *testing.T) {
	for _, tc := range []struct {
		name        string
		total       time.Duration
		observation storage.BatchCommitObservation
		want        time.Duration
		valid       bool
	}{
		{"zero", 0, storage.BatchCommitObservation{}, 0, true},
		{"disjoint", 20, storage.BatchCommitObservation{Begin: 2, Prepare: 3, Commit: 5}, 10, true},
		{"nested-not-subtracted", 20, storage.BatchCommitObservation{Begin: 2, Prepare: 3, Commit: 5, Prewrite: 4, CommitTS: 1, PrimaryCommit: 2, PrepareLocks: storage.LockRPCObservation{CheckTxnStatus: storage.LockRPCSample{Duration: 30}}}, 10, true},
		{"negative-total", -1, storage.BatchCommitObservation{}, 0, false},
		{"negative-begin", 20, storage.BatchCommitObservation{Begin: -1}, 0, false},
		{"negative-prepare", 20, storage.BatchCommitObservation{Prepare: -1}, 0, false},
		{"negative-commit", 20, storage.BatchCommitObservation{Commit: -1}, 0, false},
		{"exceeds-wall", 20, storage.BatchCommitObservation{Begin: 10, Prepare: 10, Commit: 1}, 0, false},
		{"overflow", 1<<63 - 1, storage.BatchCommitObservation{Begin: 1<<63 - 1, Commit: 1}, 0, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, valid := protocolLatencyOutsideBatch(tc.total, tc.observation)
			require.Equal(t, tc.valid, valid)
			require.Equal(t, tc.want, got)
		})
	}
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
