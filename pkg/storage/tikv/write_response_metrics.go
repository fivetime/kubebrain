package tikv

import (
	"context"
	"time"

	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/prometheus/client_golang/prometheus"
	clienttikv "github.com/tikv/client-go/v2/tikv"
	"github.com/tikv/client-go/v2/tikvrpc"
)

type writeResponseMetrics struct {
	responses *prometheus.CounterVec
	stages    *prometheus.HistogramVec
}

func newWriteResponseMetrics(reg prometheus.Registerer) *writeResponseMetrics {
	m := &writeResponseMetrics{
		responses: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "kubebrain_tikv_write_response_total",
			Help: "TiKV Prewrite/Commit RPC outcomes and raw detail presence; includes background work and retries, not logical Put counts.",
		}, []string{"method", "outcome", "details"}),
		stages: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "kubebrain_tikv_write_stage_seconds",
			Help:    "Successful TiKV write RPCs with raw WriteDetail only. RPC and server stages overlap; never add them. Zero fields are reported zeros, not missing detail messages.",
			Buckets: prometheus.ExponentialBuckets(0.00001, 4, 12),
		}, []string{"method", "stage"}),
	}
	reg.MustRegister(m.responses, m.stages)
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
	start := time.Now()
	response, err := c.Client.SendRequest(ctx, addr, req, timeout)
	c.metrics.observe(method, response, err, time.Since(start))
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
	m.responses.WithLabelValues(method, outcome, presence).Inc()
	if outcome != "success" || write == nil {
		return
	}
	// Raw uint64 nanoseconds are converted without a signed duration overflow.
	// Every stage uses exactly the same successful-response population.
	m.stages.WithLabelValues(method, "rpc").Observe(elapsed.Seconds())
	for _, stage := range []struct {
		name  string
		nanos uint64
	}{
		{"persist_log", write.PersistLogNanos}, {"raft_sync", write.RaftDbSyncLogNanos},
		{"commit_log", write.CommitLogNanos}, {"apply_log", write.ApplyLogNanos},
		{"store_wait", write.StoreBatchWaitNanos}, {"proposal_wait", write.ProposeSendWaitNanos},
		{"apply_wait", write.ApplyBatchWaitNanos}, {"process", write.ProcessNanos},
		{"throttle", write.ThrottleNanos},
	} {
		m.stages.WithLabelValues(method, stage.name).Observe(float64(stage.nanos) / 1e9)
	}
}
