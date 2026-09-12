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
