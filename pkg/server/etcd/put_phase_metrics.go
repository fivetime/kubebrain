package etcd

import (
	"context"
	"time"

	"github.com/kubewharf/kubebrain/pkg/metrics"
	"github.com/kubewharf/kubebrain/pkg/storage"
)

// Child batch attempts may outnumber public Put calls (CAS/uncertain retries).
// Keep their population and outcomes distinct from the parent Put histograms.
func observePutBatchCommits(ctx context.Context, metricCli metrics.Metrics) context.Context {
	if metricCli == nil {
		return ctx
	}
	return storage.WithBatchCommitObserver(ctx, func(o storage.BatchCommitObservation) {
		tags := []metrics.T{metrics.Tag("method", "put"), getSuccessMetricTagByErr(o.Err)}
		emit := func(phase string, elapsed time.Duration) {
			_ = metricCli.EmitHistogram("write.batch."+phase+".latency", elapsed.Seconds(), tags...)
		}
		emit("begin", o.Begin)
		emit("prepare", o.Prepare)
		if o.CommitAttempted {
			emit("commit", o.Commit)
			emitPrimaryWriteSample(metricCli, o.PrimaryWrite, tags)
		}
		if o.HasWriteDetails {
			emit("prewrite", o.Prewrite)
			emit("commit_ts", o.CommitTS)
			emit("primary_commit", o.PrimaryCommit)
			// A batch-scoped numeric observation, not a Region-ID label. SDK
			// retries can count the same Region more than once.
			_ = metricCli.EmitHistogram("write.batch.prewrite_region_groups", float64(o.PrewriteRegionGroups), tags...)
		}
	})
}

func emitPrimaryWriteSample(metricCli metrics.Metrics, sample storage.PrimaryWriteObservation, tags []metrics.T) {
	if sample.SuccessfulRPCs == 0 {
		return
	}
	details := sample.Details
	switch details {
	case "absent", "exec_only", "write", "invalid_write":
	default:
		details = "invalid_write" // never export arbitrary provider text as a label
	}
	if sample.RPC < 0 || sample.PersistLog < 0 || sample.RaftSync < 0 || sample.CommitLog < 0 {
		details = "invalid_write"
	}
	sampleTags := append(append([]metrics.T(nil), tags...), metrics.Tag("details", details))
	_ = metricCli.EmitCounter("write.batch.primary_rpc.samples", 1, sampleTags...)
	_ = metricCli.EmitHistogram("write.batch.primary_rpc.successful_requests", float64(sample.SuccessfulRPCs), tags...)
	if details != "write" {
		return
	}
	// All four histograms share the same selected, representable raw-detail
	// population. The batch outcome tag is NOT a claim of exactly-once RPCs.
	for _, phase := range []struct {
		name  string
		value time.Duration
	}{{"rpc", sample.RPC}, {"persist_log", sample.PersistLog}, {"raft_sync", sample.RaftSync}, {"commit_log", sample.CommitLog}} {
		_ = metricCli.EmitHistogram("write.batch.primary_rpc."+phase.name+".latency", phase.value.Seconds(), tags...)
	}
}

// These paired observations cover only local Put requests that reach the
// backend call. Pre-backend includes admission, leadership, auth, quota and
// lease-lock acquisition. Backend includes shim reads/CAS and any leased
// atomic write; it is NOT pure TiKV commit latency. Early rejects and follower
// proxy calls emit neither sample. Both samples carry the backend outcome,
// allowing comparisons over the same population without logging request data.
func emitPutBackendPhaseDurations(metricCli metrics.Metrics, before, backend time.Duration, err error) {
	if metricCli == nil {
		return
	}
	tags := []metrics.T{metrics.Tag("method", "put"), getSuccessMetricTagByErr(err)}
	_ = metricCli.EmitHistogram("write.pre_backend.latency", before.Seconds(), tags...)
	_ = metricCli.EmitHistogram("write.backend.latency", backend.Seconds(), tags...)
}
