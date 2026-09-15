package prometheus

import (
	"testing"

	"github.com/kubewharf/kubebrain/pkg/metrics"
	prom "github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
)

func TestWatchCompactDurationExport(t *testing.T) {
	registry := prom.NewRegistry()
	previousRegisterer, previousGather := registerer, gather
	registerer, gather = registry, registry
	t.Cleanup(func() { registerer, gather = previousRegisterer, previousGather })
	p := NewMetrics(metrics.Tag("cluster", "test"))
	require.NoError(t, p.EmitHistogram("watch.prev_kv.compact_revision.duration.seconds", .04, metrics.Tag("outcome", "success")))
	families, err := registry.Gather()
	require.NoError(t, err)
	require.Len(t, families, 1)
	f := families[0]
	require.Equal(t, "watch_prev_kv_compact_revision_duration_seconds", f.GetName())
	require.Contains(t, f.GetHelp(), "not end-to-end Watch latency")
	require.Len(t, f.Metric, 1)
	m := f.Metric[0]
	labels := map[string]string{}
	for _, label := range m.Label {
		labels[label.GetName()] = label.GetValue()
	}
	require.Equal(t, map[string]string{"cluster": "test", "outcome": "success"}, labels)
	require.EqualValues(t, 1, m.GetHistogram().GetSampleCount())
	require.Equal(t, .04, m.GetHistogram().GetSampleSum())
}
