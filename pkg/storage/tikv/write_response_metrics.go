package tikv

import (
	"context"
	"math"
	"time"

	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/prometheus/client_golang/prometheus"
	clienttikv "github.com/tikv/client-go/v2/tikv"
	"github.com/tikv/client-go/v2/tikvrpc"
)

type writeResponseMetrics struct {
	responses *prometheus.CounterVec
	stages    *prometheus.HistogramVec
	invalid   *prometheus.CounterVec
}

func newWriteResponseMetrics(reg prometheus.Registerer) *writeResponseMetrics {
	m := &writeResponseMetrics{
		responses: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "kubebrain_tikv_write_response_total",
			Help: "TiKV Prewrite/Commit RPC outcomes and raw detail presence; includes background work and retries, not logical Put counts.",
		}, []string{"method", "outcome", "details"}),
		stages: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "kubebrain_tikv_write_stage_seconds",
			Help:    "Successful TiKV write RPCs with representable raw WriteDetail only; invalid responses are excluded from every stage. RPC and server stages overlap; never add them. Zero fields are reported zeros, not missing detail messages.",
			Buckets: prometheus.ExponentialBuckets(0.00001, 4, 12),
		}, []string{"method", "stage"}),
		invalid: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "kubebrain_tikv_write_invalid_duration_total",
			Help: "Successful write responses containing an observed stage above signed nanosecond duration range. Each offending stage is counted; no clamping or reinterpretation is performed.",
		}, []string{"method", "stage"}),
	}
	reg.MustRegister(m.responses, m.stages, m.invalid)
	return m
}

var defaultWriteResponseMetrics = newWriteResponseMetrics(prometheus.DefaultRegisterer)

type writeResponseClient struct {
	clienttikv.Client
	metrics *writeResponseMetrics
}

func (c *writeResponseClient) SendRequest(ctx context.Context, addr string, req *tikvrpc.Request, timeout time.Duration) (*tikvrpc.Response, error) {
	method := ""
	switch req.Type {
	case tikvrpc.CmdPrewrite:
		method = "prewrite"
	case tikvrpc.CmdCommit:
		method = "commit"
	default:
		return c.Client.SendRequest(ctx, addr, req, timeout)
	}
	tracker, _ := ctx.Value(primaryWriteTrackerKey{}).(*primaryWriteTracker)
	if tracker != nil && method == "prewrite" {
		prewrite, _ := req.Req.(*kvrpcpb.PrewriteRequest)
		tracker.register(prewrite)
	}
	start := time.Now()
	response, err := c.Client.SendRequest(ctx, addr, req, timeout)
	elapsed := time.Since(start)
	c.metrics.observe(method, response, err, elapsed)
	if tracker != nil && method == "commit" {
		commit, _ := req.Req.(*kvrpcpb.CommitRequest)
		var result *kvrpcpb.CommitResponse
		if response != nil {
			result, _ = response.Resp.(*kvrpcpb.CommitResponse)
		}
		tracker.observe(commit, result, err, elapsed)
	}
	return response, err // preserve response/error identity and perform no retry
}

func (m *writeResponseMetrics) observe(method string, response *tikvrpc.Response, err error, elapsed time.Duration) {
	outcome, presence := "success", "absent"
	var detail *kvrpcpb.ExecDetailsV2
	switch {
	case err != nil:
		outcome = "transport_error"
	case response == nil || response.Resp == nil:
		outcome = "response_missing"
	default:
		switch r := response.Resp.(type) {
		case *kvrpcpb.PrewriteResponse:
			if r == nil || method != "prewrite" {
				outcome = "unexpected_response"
				break
			}
			detail = r.ExecDetailsV2
			if r.RegionError != nil {
				outcome = "region_error"
			} else if len(r.Errors) != 0 {
				outcome = "key_error"
			}
		case *kvrpcpb.CommitResponse:
			if r == nil || method != "commit" {
				outcome = "unexpected_response"
				break
			}
			detail = r.ExecDetailsV2
			if r.RegionError != nil {
				outcome = "region_error"
			} else if r.Error != nil {
				outcome = "key_error"
			}
		default:
			outcome = "unexpected_response"
		}
	}
	if detail != nil {
		presence = "exec_only"
	}
	write := detail.GetWriteDetail()
	if write != nil {
		presence = "write"
	}
	if outcome != "success" || write == nil {
		m.responses.WithLabelValues(method, outcome, presence).Inc()
		return
	}
	stages := []struct {
		name  string
		nanos uint64
	}{
		{"persist_log", write.PersistLogNanos}, {"raft_sync", write.RaftDbSyncLogNanos},
		{"commit_log", write.CommitLogNanos}, {"apply_log", write.ApplyLogNanos},
		{"store_wait", write.StoreBatchWaitNanos}, {"proposal_wait", write.ProposeSendWaitNanos},
		{"apply_wait", write.ApplyBatchWaitNanos}, {"process", write.ProcessNanos},
		{"throttle", write.ThrottleNanos},
	}
	// A raw uint64 may encode an underflowed/sentinel duration. Converting it
	// directly to float64 avoids a Go overflow but still poisons the histogram.
	// Reject, don't reinterpret: the source of an out-of-range value is unknown.
	for _, stage := range stages {
		if stage.nanos > math.MaxInt64 {
			presence = "invalid_write"
			m.invalid.WithLabelValues(method, stage.name).Inc()
		}
	}
	m.responses.WithLabelValues(method, outcome, presence).Inc()
	if presence == "invalid_write" {
		return
	}
	// Reject the entire duration sample, including RPC, to keep populations
	// matched. The response counter retains all successes, including invalids.
	m.stages.WithLabelValues(method, "rpc").Observe(elapsed.Seconds())
	for _, stage := range stages {
		m.stages.WithLabelValues(method, stage.name).Observe(float64(stage.nanos) / 1e9)
	}
}
