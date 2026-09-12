package prometheus

import (
	"testing"

	"github.com/kubewharf/kubebrain/pkg/metrics"
	prom "github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
)

func TestPrewriteRegionGroupsExportPopulationAndUnits(t *testing.T) {
	registry := prom.NewRegistry()
	previousRegisterer, previousGather := registerer, gather
	registerer, gather = registry, registry
	t.Cleanup(func() { registerer, gather = previousRegisterer, previousGather })
	p := NewMetrics(metrics.Tag("cluster", "test"))
	const name = "write.batch.prewrite_region_groups"
	tags := []metrics.T{metrics.Tag("method", "put"), metrics.Tag("success", "true")}
	registrar, ok := p.(metrics.HistogramRegistrar)
	require.True(t, ok)
	require.NoError(t, registrar.RegisterHistogram(name, tags...))
	families, err := registry.Gather()
	require.NoError(t, err)
	require.Len(t, families, 1)
	require.Zero(t, families[0].GetMetric()[0].GetHistogram().GetSampleCount(), "registration is not a batch attempt")
	for _, groups := range []float64{0, 1, 3} {
		require.NoError(t, p.EmitHistogram(name, groups, tags...))
	}
	require.NoError(t, p.EmitHistogram(name, float64(2), metrics.Tag("method", "put"), metrics.Tag("success", "false")))
	families, err = registry.Gather()
	require.NoError(t, err)
	require.Len(t, families, 1)
	family := families[0]
	require.Equal(t, "write_batch_prewrite_region_groups", family.GetName())
	require.Contains(t, family.GetHelp(), "including retries")
	require.Contains(t, family.GetHelp(), "not distinct Regions")
	require.Len(t, family.GetMetric(), 2)
	seen := map[string]bool{}
	for _, sample := range family.GetMetric() {
		labels := map[string]string{}
		for _, label := range sample.GetLabel() {
			labels[label.GetName()] = label.GetValue()
		}
		outcome := labels["success"]
		require.Contains(t, []string{"true", "false"}, outcome)
		require.False(t, seen[outcome])
		seen[outcome] = true
		require.Equal(t, map[string]string{"cluster": "test", "method": "put", "success": outcome}, labels)
		histogram := sample.GetHistogram()
		require.NotNil(t, histogram)
		if outcome == "true" {
			require.EqualValues(t, 3, histogram.GetSampleCount(), "zero groups is still one observed batch")
			require.Equal(t, float64(4), histogram.GetSampleSum(), "groups must not be converted into seconds")
		} else {
			require.EqualValues(t, 1, histogram.GetSampleCount())
			require.Equal(t, float64(2), histogram.GetSampleSum())
		}
	}
}
