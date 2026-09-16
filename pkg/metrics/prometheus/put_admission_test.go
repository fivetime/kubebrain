package prometheus

import (
	"testing"

	"github.com/kubewharf/kubebrain/pkg/metrics"
	prom "github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
)

func TestPutAdmissionExportPopulationAndUnits(t *testing.T) {
	registry := prom.NewRegistry()
	previousRegisterer, previousGather := registerer, gather
	registerer, gather = registry, registry
	t.Cleanup(func() { registerer, gather = previousRegisterer, previousGather })
	p := NewMetrics(metrics.Tag("cluster", "test"))
	for _, phase := range []string{"route", "quota", "leader_ready", "auth_admission", "auth_apply", "corrupt", "lease_guard", "effective_options"} {
		require.NoError(t, p.EmitHistogram("write.admission."+phase+".latency", .001,
			metrics.Tag("method", "put"), metrics.Tag("success", "false")))
	}
	families, err := registry.Gather()
	require.NoError(t, err)
	require.Len(t, families, 8)
	for _, family := range families {
		require.Contains(t, family.GetName(), "write_admission_")
		require.Contains(t, family.GetHelp(), "wall seconds")
		require.Contains(t, family.GetHelp(), "partition write_pre_backend_latency")
		require.Contains(t, family.GetHelp(), "rejected admissions and follower proxy calls excluded")
		require.Contains(t, family.GetHelp(), "Success labels the backend outcome")
		require.Len(t, family.Metric, 1)
		m := family.Metric[0]
		labels := map[string]string{}
		for _, label := range m.Label {
			labels[label.GetName()] = label.GetValue()
		}
		require.Equal(t, map[string]string{"cluster": "test", "method": "put", "success": "false"}, labels)
		require.EqualValues(t, 1, m.GetHistogram().GetSampleCount())
		require.Equal(t, .001, m.GetHistogram().GetSampleSum())
	}
}
