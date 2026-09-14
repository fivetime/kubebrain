package etcd

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/kubewharf/kubebrain/pkg/metrics"
	"github.com/kubewharf/kubebrain/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestPutBackendPhaseMetricsPairUnitsAndOutcome(t *testing.T) {
	for _, err := range []error{nil, errors.New("private request detail")} {
		rec := &recordingMetrics{}
		emitPutBackendPhaseDurations(rec, 25*time.Millisecond, 75*time.Millisecond, err)
		tags := []metrics.T{metrics.Tag("method", "put"), getSuccessMetricTagByErr(err)}
		require.Equal(t, []recordedHistogram{
			{name: "write.pre_backend.latency", value: 0.025, tags: tags},
			{name: "write.backend.latency", value: 0.075, tags: tags},
		}, rec.histograms)
	}
	require.NotPanics(t, func() { emitPutBackendPhaseDurations(nil, 0, 0, nil) })
}

func TestBatchLockRPCMetricsUnitsAndZeroPopulation(t *testing.T) {
	rec := &recordingMetrics{}
	callback := storage.BatchCommitObserverFromContext(observePutBatchCommits(context.Background(), rec))
	callback(storage.BatchCommitObservation{HasLockRPCDetails: true, CommitAttempted: true,
		PrepareLocks: storage.LockRPCObservation{ResolveLock: storage.LockRPCSample{Requests: 2, TransportErrors: 1, Duration: 25 * time.Millisecond}},
	})
	values := map[string]interface{}{}
	for _, h := range rec.histograms {
		values[h.name] = h.value
	}
	require.Equal(t, float64(2), values["write.batch.lock_rpc.prepare.resolve_lock.requests"])
	require.Equal(t, float64(1), values["write.batch.lock_rpc.prepare.resolve_lock.transport_errors"])
	require.Equal(t, .025, values["write.batch.lock_rpc.prepare.resolve_lock.latency"])
	require.Equal(t, float64(0), values["write.batch.lock_rpc.commit.resolve_lock.requests"])
	require.Len(t, rec.histograms, 15)
	rec.histograms = nil
	callback(storage.BatchCommitObservation{HasLockRPCDetails: true, Err: errors.New("private")})
	require.Len(t, rec.histograms, 8, "early failure has prepare samples but no attempted commit phase")
}

func TestPutBatchMetricsUnitsPopulationAndOutcome(t *testing.T) {
	ctx := context.Background()
	require.True(t, ctx == observePutBatchCommits(ctx, nil))
	for _, result := range []error{nil, errors.New("not a metric label")} {
		rec := &recordingMetrics{}
		observed := observePutBatchCommits(ctx, rec)
		callback := storage.BatchCommitObserverFromContext(observed)
		require.NotNil(t, callback)
		callback(storage.BatchCommitObservation{
			Begin: time.Millisecond, Prepare: 2 * time.Millisecond,
			Commit: 9 * time.Millisecond, CommitAttempted: true,
			HasWriteDetails: true, Prewrite: 3 * time.Millisecond,
			CommitTS: time.Millisecond, PrimaryCommit: 4 * time.Millisecond, Err: result,
			PrewriteRegionGroups: 2,
		})
		tags := []metrics.T{metrics.Tag("method", "put"), getSuccessMetricTagByErr(result)}
		require.Equal(t, []recordedHistogram{
			{name: "write.batch.begin.latency", value: .001, tags: tags},
			{name: "write.batch.prepare.latency", value: .002, tags: tags},
			{name: "write.batch.commit.latency", value: .009, tags: tags},
			{name: "write.batch.prewrite.latency", value: .003, tags: tags},
			{name: "write.batch.commit_ts.latency", value: .001, tags: tags},
			{name: "write.batch.primary_commit.latency", value: .004, tags: tags},
			{name: "write.batch.prewrite_region_groups", value: float64(2), tags: tags},
		}, rec.histograms)
		rec.histograms = nil
		callback(storage.BatchCommitObservation{Err: result})
		require.Len(t, rec.histograms, 2, "early failure has no commit or SDK sample")
		rec.histograms = nil
		callback(storage.BatchCommitObservation{CommitAttempted: true, Err: result})
		require.Len(t, rec.histograms, 3, "no-write transaction has no SDK write details")
	}
}

func TestPrimaryWriteMetricsPresenceUnitsAndBatchOutcome(t *testing.T) {
	for _, outcome := range []error{nil, errors.New("not exported")} {
		for _, details := range []string{"absent", "exec_only", "write", "invalid_write", "private provider text"} {
			rec := &recordingMetrics{}
			tags := []metrics.T{metrics.Tag("method", "put"), getSuccessMetricTagByErr(outcome)}
			sample := storage.PrimaryWriteObservation{SuccessfulRPCs: 2, Details: details,
				RPC: 10 * time.Millisecond, PersistLog: 3 * time.Millisecond, RaftSync: time.Millisecond, CommitLog: 5 * time.Millisecond}
			emitPrimaryWriteSample(rec, sample, tags)
			label := details
			if details == "private provider text" {
				label = "invalid_write"
			}
			require.Equal(t, []recordedCounter{{name: "write.batch.primary_rpc.samples", value: 1,
				tags: append(append([]metrics.T(nil), tags...), metrics.Tag("details", label))}}, rec.counters)
			want := []recordedHistogram{{name: "write.batch.primary_rpc.successful_requests", value: float64(2), tags: tags}}
			if details == "write" {
				want = append(want,
					recordedHistogram{name: "write.batch.primary_rpc.rpc.latency", value: .010, tags: tags},
					recordedHistogram{name: "write.batch.primary_rpc.persist_log.latency", value: .003, tags: tags},
					recordedHistogram{name: "write.batch.primary_rpc.raft_sync.latency", value: .001, tags: tags},
					recordedHistogram{name: "write.batch.primary_rpc.commit_log.latency", value: .005, tags: tags})
			}
			require.Equal(t, want, rec.histograms)
			require.Len(t, tags, 2, "detail tags must not mutate the caller tag slice")
		}
	}
}

func TestPrimaryWriteMetricsNoSampleAndNegativeDurations(t *testing.T) {
	rec := &recordingMetrics{}
	emitPrimaryWriteSample(rec, storage.PrimaryWriteObservation{}, nil)
	require.Empty(t, rec.counters)
	require.Empty(t, rec.histograms)
	for _, field := range []string{"rpc", "persist", "raft", "commit"} {
		rec := &recordingMetrics{}
		sample := storage.PrimaryWriteObservation{SuccessfulRPCs: 1, Details: "write"}
		switch field {
		case "rpc":
			sample.RPC = -1
		case "persist":
			sample.PersistLog = -1
		case "raft":
			sample.RaftSync = -1
		case "commit":
			sample.CommitLog = -1
		}
		emitPrimaryWriteSample(rec, sample, nil)
		require.Len(t, rec.histograms, 1, "only request count, no invalid duration samples")
		require.Equal(t, []metrics.T{metrics.Tag("details", "invalid_write")}, rec.counters[0].tags)
	}
	rec = &recordingMetrics{}
	callback := storage.BatchCommitObserverFromContext(observePutBatchCommits(context.Background(), rec))
	callback(storage.BatchCommitObservation{PrimaryWrite: storage.PrimaryWriteObservation{SuccessfulRPCs: 1, Details: "write"}})
	require.Empty(t, rec.counters, "no commit attempt means no primary sample")
}
