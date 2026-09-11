package tikv

import (
	"bytes"
	"context"
	"crypto/rand"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/kubewharf/kubebrain/pkg/storage"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
	tikvconfig "github.com/tikv/client-go/v2/config"
	clienttikv "github.com/tikv/client-go/v2/tikv"
	"github.com/tikv/client-go/v2/tikvrpc"
)

type protocolSecondaryReadMarker struct{}

// Hold only secondary Commit RPCs for the explicitly marked transaction.
// The primary really commits; reader CheckTxnStatus/ResolveLock reach TiKV.
// No successful response is fabricated and no server configuration is changed.
type protocolSecondaryHold struct {
	clienttikv.Client
	mu               sync.Mutex
	startTS          uint64
	primary          []byte
	checks, resolves int
	held             chan struct{}
	release          chan struct{}
	once             sync.Once
	resolveHeld      chan struct{}
	resolveOnce      sync.Once
	writeDetails     []protocolWriteResponseDetail
}

// Record raw response presence, not SDK-converted zero-valued placeholders.
// All fields are scalar diagnostics: no keys, store addresses or transaction IDs.
type protocolWriteResponseDetail struct {
	method                                string
	present, writePresent, success        bool
	persist, syncLog, commitLog, applyLog uint64
}

func (c *protocolSecondaryHold) SendRequest(ctx context.Context, addr string, req *tikvrpc.Request, timeout time.Duration) (*tikvrpc.Response, error) {
	c.mu.Lock()
	if ctx.Value(protocolCommitMarker{}) == true && req.Type == tikvrpc.CmdPrewrite {
		c.startTS = req.Prewrite().StartVersion
		c.primary = bytes.Clone(req.Prewrite().PrimaryLock)
	}
	hold := req.Type == tikvrpc.CmdCommit && c.startTS != 0 && req.Commit().StartVersion == c.startTS
	if hold {
		for _, key := range req.Commit().Keys {
			if bytes.Equal(key, c.primary) {
				hold = false
			}
		}
	}
	holdResolve := c.resolveHeld != nil && req.Type == tikvrpc.CmdResolveLock && c.startTS != 0 && req.ResolveLock().StartVersion == c.startTS
	c.mu.Unlock()
	if hold || holdResolve {
		if hold {
			c.once.Do(func() { close(c.held) })
		}
		if holdResolve {
			c.resolveOnce.Do(func() { close(c.resolveHeld) })
		}
		timer := time.NewTimer(20 * time.Second)
		defer timer.Stop()
		select {
		case <-c.release:
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-timer.C:
			return nil, context.DeadlineExceeded
		}
	}
	response, err := c.Client.SendRequest(ctx, addr, req, timeout)
	if ctx.Value(protocolCommitMarker{}) == true && err == nil && response != nil {
		var detail *kvrpcpb.ExecDetailsV2
		var success bool
		switch r := response.Resp.(type) {
		case *kvrpcpb.PrewriteResponse:
			detail = r.ExecDetailsV2
			success = r.RegionError == nil && len(r.Errors) == 0
		case *kvrpcpb.CommitResponse:
			detail = r.ExecDetailsV2
			success = r.RegionError == nil && r.Error == nil
		}
		write := detail.GetWriteDetail()
		c.mu.Lock()
		c.writeDetails = append(c.writeDetails, protocolWriteResponseDetail{
			method: req.Type.String(), present: detail != nil, writePresent: write != nil, success: success,
			persist: write.GetPersistLogNanos(), syncLog: write.GetRaftDbSyncLogNanos(),
			commitLog: write.GetCommitLogNanos(), applyLog: write.GetApplyLogNanos(),
		})
		c.mu.Unlock()
	}
	if ctx.Value(protocolSecondaryReadMarker{}) == true && err == nil && response != nil {
		c.mu.Lock()
		if req.Type == tikvrpc.CmdCheckTxnStatus {
			c.checks++
		}
		if req.Type == tikvrpc.CmdResolveLock {
			c.resolves++
		}
		c.mu.Unlock()
	}
	return response, err
}

func TestProtocolSecondaryHoldScope(t *testing.T) {
	stub := &protocolResponseStub{response: &tikvrpc.Response{Resp: &kvrpcpb.CommitResponse{}}}
	hold := &protocolSecondaryHold{Client: stub, held: make(chan struct{}), release: make(chan struct{})}
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(hold.release) }) }
	t.Cleanup(release)
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	prewrite := tikvrpc.NewRequest(tikvrpc.CmdPrewrite, &kvrpcpb.PrewriteRequest{StartVersion: 10, PrimaryLock: []byte("primary")})
	_, err := hold.SendRequest(context.WithValue(ctx, protocolCommitMarker{}, true), "unused", prewrite, time.Second)
	require.NoError(t, err)
	for _, request := range []*kvrpcpb.CommitRequest{
		{StartVersion: 11, Keys: [][]byte{[]byte("secondary")}},
		{StartVersion: 10, Keys: [][]byte{[]byte("primary")}},
	} {
		_, err = hold.SendRequest(ctx, "unused", tikvrpc.NewRequest(tikvrpc.CmdCommit, request), time.Second)
		require.NoError(t, err, "other transactions and the primary must not be held")
	}
	done := make(chan error, 1)
	go func() {
		_, err := hold.SendRequest(ctx, "unused", tikvrpc.NewRequest(tikvrpc.CmdCommit,
			&kvrpcpb.CommitRequest{StartVersion: 10, Keys: [][]byte{[]byte("secondary")}}), time.Second)
		done <- err
	}()
	select {
	case <-hold.held:
	case <-ctx.Done():
		t.Fatal("marked secondary did not enter hold")
	}
	require.Equal(t, 3, stub.calls, "held secondary must not reach transport yet")
	release()
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-ctx.Done():
		t.Fatal("released secondary did not finish")
	}
	require.Equal(t, 4, stub.calls)
}

func TestRealTiKVReadBypassesPendingSecondaryCleanup(t *testing.T) {
	pd := os.Getenv("KUBEBRAIN_TIKV_PROTOCOL_PD")
	if pd == "" {
		t.Skip("explicit protocol PD endpoint required")
	}
	prefix := os.Getenv("KUBEBRAIN_TIKV_PROTOCOL_PREFIX")
	expected, err := validateProtocolSmokeScope(os.Getenv("KUBEBRAIN_TIKV_PROTOCOL_CLUSTER_ID"), prefix, os.Getenv("KUBEBRAIN_TIKV_PROTOCOL_MODE"))
	require.NoError(t, err)
	require.Equal(t, "2pc", os.Getenv("KUBEBRAIN_TIKV_PROTOCOL_MODE"))
	require.Equal(t, "1", os.Getenv("KUBEBRAIN_TIKV_PROTOCOL_ALLOW_REGION_SPLIT"))
	t.Cleanup(tikvconfig.UpdateGlobal(func(cfg *tikvconfig.Config) { cfg.Enable1PC = false; cfg.EnableAsyncCommit = false }))
	ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
	defer cancel()
	kv, err := NewKvStorageWithContext(ctx, strings.Split(pd, ","), 1, Security{})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, kv.Close()) })
	require.Equal(t, expected, kv.(storage.ClusterIdentifier).ClusterID())
	require.NoError(t, protocolSmokePrefixEmpty(ctx, kv, prefix))
	owner := make([]byte, 32)
	_, err = rand.Read(owner)
	require.NoError(t, err)
	t.Cleanup(func() {
		cleanupCtx, stop := context.WithTimeout(context.Background(), 30*time.Second)
		defer stop()
		require.NoError(t, cleanupProtocolSmoke(cleanupCtx, kv, prefix, owner))
		t.Logf("PROTOCOL_FIXTURE_CLEANUP_OK prefix=%s", prefix)
	})
	claim := kv.BeginBatchWrite()
	claim.PutIfNotExist([]byte(prefix+"owner"), owner, 0)
	require.NoError(t, claim.Commit(ctx))
	client := kv.(*store).getClient()
	require.IsType(t, &writeResponseClient{}, client.GetTiKVClient(), "production constructor must install write response telemetry")
	data, witness := []byte(prefix+"data"), []byte(prefix+"witness")
	_, err = client.SplitRegions(ctx, [][]byte{witness}, false, nil)
	require.NoError(t, err)
	hold := &protocolSecondaryHold{Client: client.GetTiKVClient(), held: make(chan struct{}), release: make(chan struct{}), resolveHeld: make(chan struct{})}
	client.SetTiKVClient(hold)
	defer close(hold.release) // unblock background commits before fixture cleanup/client Close
	txn, err := client.BeginWithContext(ctx)
	require.NoError(t, err)
	require.NoError(t, txn.Set(data, []byte("committed")))
	require.NoError(t, txn.Set(witness, []byte("committed")))
	require.NoError(t, txn.Commit(context.WithValue(ctx, protocolCommitMarker{}, true)))
	hold.mu.Lock()
	writeDetails := append([]protocolWriteResponseDetail(nil), hold.writeDetails...)
	hold.mu.Unlock()
	for _, d := range writeDetails {
		t.Logf("WRITE_RESPONSE_DETAIL method=%s success=%t exec_present=%t write_present=%t persist_ns=%d sync_ns=%d commit_log_ns=%d apply_ns=%d scope=diagnostic_only",
			d.method, d.success, d.present, d.writePresent, d.persist, d.syncLog, d.commitLog, d.applyLog)
	}
	select {
	case <-hold.held:
	case <-ctx.Done():
		t.Fatal("secondary Commit was not held")
	}
	hold.mu.Lock()
	secondary := data
	if bytes.Equal(hold.primary, data) {
		secondary = witness
	}
	hold.mu.Unlock()
	readCtx, stopRead := context.WithTimeout(context.WithValue(ctx, protocolSecondaryReadMarker{}, true), 5*time.Second)
	defer stopRead()
	value, err := kv.Get(readCtx, secondary)
	require.NoError(t, err)
	require.Equal(t, []byte("committed"), value)
	hold.mu.Lock()
	checks, resolves := hold.checks, hold.resolves
	hold.mu.Unlock()
	require.Positive(t, checks, "the marked reader must check actual primary status")
	require.Zero(t, resolves, "read must finish while physical lock cleanup remains held")
	select {
	case <-hold.resolveHeld:
	case <-readCtx.Done():
		t.Fatal("transaction-specific background ResolveLock was not observed")
	}
	t.Logf("SECONDARY_READ_BYPASS_CONFIRMED checks=%d foreground_resolves=%d secondary_commit_held=true background_resolve_held=true scope=mechanism_only", checks, resolves)
	// No direct metric Observe calls: these samples must come from real RPCs
	// through the installed production wrapper and the /metrics registry.
	families, err := prometheus.DefaultGatherer.Gather()
	require.NoError(t, err)
	exported := map[string]bool{"prewrite": false, "commit": false}
	for _, family := range families {
		if family.GetName() != "kubebrain_tikv_write_stage_seconds" {
			continue
		}
		for _, metric := range family.Metric {
			method, stage := "", ""
			for _, label := range metric.Label {
				if label.GetName() == "method" {
					method = label.GetValue()
				}
				if label.GetName() == "stage" {
					stage = label.GetValue()
				}
			}
			if stage == "persist_log" && metric.Histogram.GetSampleCount() > 0 {
				exported[method] = true
			}
		}
	}
	require.True(t, exported["prewrite"] && exported["commit"], "real successful write details must be exported")
}
