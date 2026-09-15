package etcd

import (
	"context"
	"errors"
	"testing"

	"github.com/kubewharf/kubebrain/pkg/metrics"
	"github.com/stretchr/testify/require"
)

type watchCompactMetricsBackend struct {
	BackendShim
	read func(context.Context) (uint64, error)
}

func (b watchCompactMetricsBackend) GetCompactRevisionFresh(ctx context.Context) (uint64, error) {
	return b.read(ctx)
}

func TestWatchPrevKVCompactMetricsPreserveRead(t *testing.T) {
	for _, readErr := range []error{nil, errors.New("storage unavailable"), context.Canceled} {
		for _, sink := range []metrics.Metrics{nil, &recordingMetrics{}, &failingHeaderTermMetrics{}} {
			ctx, cancel := context.WithCancel(context.Background())
			if errors.Is(readErr, context.Canceled) {
				cancel()
			}
			calls := 0
			w := &watcher{metricCli: sink, backend: watchCompactMetricsBackend{read: func(got context.Context) (uint64, error) {
				require.Same(t, ctx, got)
				calls++
				return 19, readErr
			}}}
			revision, err := w.watchPrevKVCompactRevision(ctx)
			cancel()
			require.EqualValues(t, 19, revision)
			require.Equal(t, readErr, err)
			require.Equal(t, 1, calls)
			var rec *recordingMetrics
			switch s := sink.(type) {
			case *recordingMetrics:
				rec = s
			case *failingHeaderTermMetrics:
				rec = &s.recordingMetrics
			default:
				continue
			}
			require.Len(t, rec.histograms, 1)
			h := rec.histograms[0]
			require.Equal(t, "watch.prev_kv.compact_revision.duration.seconds", h.name)
			outcome := "success"
			if readErr != nil {
				outcome = "error"
			}
			require.Equal(t, []metrics.T{metrics.Tag("outcome", outcome)}, h.tags)
			require.GreaterOrEqual(t, h.value.(float64), float64(0))
		}
	}
}
