package etcd

import (
	"errors"
	"testing"
	"time"

	"github.com/kubewharf/kubebrain/pkg/metrics"
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
