package prometheus

import (
	"testing"

	"github.com/kubewharf/kubebrain/pkg/metrics"
	prom "github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
)

func TestHeaderTermExportScopeLabelsAndSeconds(t *testing.T) {
	registry := prom.NewRegistry()
	previousRegisterer, previousGather := registerer, gather
	registerer, gather = registry, registry
	t.Cleanup(func() { registerer, gather = previousRegisterer, previousGather })
	p := NewMetrics(metrics.Tag("cluster", "test"))
	require.NoError(t, p.EmitHistogram("kubebrain.header.term.duration.seconds", .025,
		metrics.Tag("path", "read"), metrics.Tag("outcome", "error")))
	families, err := registry.Gather()
	require.NoError(t, err)
	require.Len(t, families, 1)
	f := families[0]
	require.Equal(t, "kubebrain_header_term_duration_seconds", f.GetName())
	require.Contains(t, f.GetHelp(), "not logical requests or end-to-end Watch delivery")
	require.Len(t, f.Metric, 1)
	m := f.Metric[0]
	labels := map[string]string{}
	for _, label := range m.Label {
		labels[label.GetName()] = label.GetValue()
	}
	require.Equal(t, map[string]string{"cluster": "test", "path": "read", "outcome": "error"}, labels)
	require.EqualValues(t, 1, m.GetHistogram().GetSampleCount())
	require.Equal(t, .025, m.GetHistogram().GetSampleSum())
}
