package etcd

import (
	"context"
	"time"

	"github.com/kubewharf/kubebrain/pkg/metrics"
	"github.com/kubewharf/kubebrain/pkg/storage"
)

// These adjacent boundaries partition the existing pre-backend observation.
// They are emitted only after a local Put reaches its backend call; rejected
// admissions and follower proxy calls deliberately have no samples here.
type putAdmissionTimings struct {
	start, routed, quotaChecked, leaderReady, authAdmitted time.Time
	authApplied, corruptChecked, leaseLocked, prepared     time.Time
}

func emitPutAdmissionPhaseDurations(metricCli metrics.Metrics, p putAdmissionTimings, err error) {
	if metricCli == nil {
		return
	}
	tags := []metrics.T{metrics.Tag("method", "put"), getSuccessMetricTagByErr(err)}
	for _, phase := range [...]struct {
		name       string
		start, end time.Time
	}{
		{"route", p.start, p.routed},
		{"quota", p.routed, p.quotaChecked},
		{"leader_ready", p.quotaChecked, p.leaderReady},
		{"auth_admission", p.leaderReady, p.authAdmitted},
		{"auth_apply", p.authAdmitted, p.authApplied},
		{"corrupt", p.authApplied, p.corruptChecked},
		{"lease_guard", p.corruptChecked, p.leaseLocked},
		{"effective_options", p.leaseLocked, p.prepared},
	} {
		// Names are fixed here: neither request values nor errors become labels.
		_ = metricCli.EmitHistogram("write.admission."+phase.name+".latency", phase.end.Sub(phase.start).Seconds(), tags...)
	}
}

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
		if o.HasLockRPCDetails {
			emitBatchLockRPCs(metricCli, "prepare", o.PrepareLocks, tags)
			if o.CommitAttempted {
				emitBatchLockRPCs(metricCli, "commit", o.CommitLocks, tags)
			}
		}
		if o.CommitAttempted {
			emit("commit", o.Commit)
			if o.HasPrewriteRPCDetails {
				p := o.PrewriteRPCs
				for _, field := range []struct {
					name  string
					value float64
				}{
					{"requests", float64(p.Requests)}, {"transport_errors", float64(p.TransportErrors)},
					{"region_errors", float64(p.RegionErrors)}, {"key_errors", float64(p.KeyErrors)},
					{"missing_responses", float64(p.MissingResponses)},
					{"total_latency", p.Duration.Seconds()}, {"max_latency", p.MaxDuration.Seconds()},
				} {
					_ = metricCli.EmitHistogram("write.batch.prewrite_rpc."+field.name, field.value, tags...)
				}
			}
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

func emitBatchLockRPCs(metricCli metrics.Metrics, phase string, o storage.LockRPCObservation, tags []metrics.T) {
	for _, rpc := range []struct {
		name   string
		sample storage.LockRPCSample
	}{
		{"check_txn_status", o.CheckTxnStatus}, {"resolve_lock", o.ResolveLock},
	} {
		prefix := "write.batch.lock_rpc." + phase + "." + rpc.name
		_ = metricCli.EmitHistogram(prefix+".requests", float64(rpc.sample.Requests), tags...)
		_ = metricCli.EmitHistogram(prefix+".transport_errors", float64(rpc.sample.TransportErrors), tags...)
		_ = metricCli.EmitHistogram(prefix+".latency", rpc.sample.Duration.Seconds(), tags...)
	}
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
