package prometheus

import (
	"testing"

	"github.com/kubewharf/kubebrain/pkg/metrics"
	prom "github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
)

func TestPrimaryWriteExportDocumentsSelectionAndUnits(t *testing.T) {
	registry := prom.NewRegistry()
	previousRegisterer, previousGather := registerer, gather
	registerer, gather = registry, registry
	t.Cleanup(func() { registerer, gather = previousRegisterer, previousGather })
	p := NewMetrics(metrics.Tag("cluster", "test"))
	tags := []metrics.T{metrics.Tag("method", "put"), metrics.Tag("success", "false")}
	for _, phase := range []string{"rpc", "persist_log", "raft_sync", "commit_log"} {
		require.NoError(t, p.EmitHistogram("write.batch.primary_rpc."+phase+".latency", float64(.005), tags...))
	}
	families, err := registry.Gather()
	require.NoError(t, err)
	require.Len(t, families, 4)
	for _, family := range families {
		require.Contains(t, family.GetHelp(), "slowest successful primary Commit RPC")
		require.Contains(t, family.GetHelp(), "success labels the batch outcome")
		require.Contains(t, family.GetHelp(), "never add")
		require.Len(t, family.Metric, 1)
		h := family.Metric[0].GetHistogram()
		require.EqualValues(t, 1, h.GetSampleCount())
		require.Equal(t, .005, h.GetSampleSum())
	}
}
