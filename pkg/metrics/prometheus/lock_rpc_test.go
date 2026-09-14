package prometheus

import (
	"testing"

	"github.com/kubewharf/kubebrain/pkg/metrics"
	prom "github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
)

func TestLockRPCExportDocumentsPopulationAndUnits(t *testing.T) {
	registry := prom.NewRegistry()
	oldRegisterer, oldGather := registerer, gather
	registerer, gather = registry, registry
	t.Cleanup(func() { registerer, gather = oldRegisterer, oldGather })
	p := NewMetrics(metrics.Tag("cluster", "test"))
	for _, name := range []string{"requests", "transport_errors", "latency"} {
		require.NoError(t, p.EmitHistogram("write.batch.lock_rpc.prepare.resolve_lock."+name, float64(0), metrics.Tag("method", "put")))
	}
	families, err := registry.Gather()
	require.NoError(t, err)
	require.Len(t, families, 3)
	for _, f := range families {
		require.Contains(t, f.GetHelp(), "not logical lock wait")
		require.Contains(t, f.GetHelp(), "Zero samples included")
		require.EqualValues(t, 1, f.Metric[0].GetHistogram().GetSampleCount())
		require.Zero(t, f.Metric[0].GetHistogram().GetSampleSum())
	}
}

func TestPrewriteRPCExportDocumentsPopulationAndUnits(t *testing.T) {
	registry := prom.NewRegistry()
	oldRegisterer, oldGather := registerer, gather
	registerer, gather = registry, registry
	t.Cleanup(func() { registerer, gather = oldRegisterer, oldGather })
	p := NewMetrics(metrics.Tag("cluster", "test"))
	for _, name := range []string{"requests", "transport_errors", "region_errors", "key_errors", "missing_responses", "total_latency", "max_latency"} {
		require.NoError(t, p.EmitHistogram("write.batch.prewrite_rpc."+name, float64(0), metrics.Tag("method", "put")))
	}
	families, err := registry.Gather()
	require.NoError(t, err)
	require.Len(t, families, 7)
	for _, f := range families {
		require.Contains(t, f.GetHelp(), "not a transaction critical path")
		require.Contains(t, f.GetHelp(), "including retries and all outcomes")
		require.Contains(t, f.GetHelp(), "Zero samples included")
		require.EqualValues(t, 1, f.Metric[0].GetHistogram().GetSampleCount())
		require.Zero(t, f.Metric[0].GetHistogram().GetSampleSum())
	}
}
