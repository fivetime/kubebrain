package prometheus

import (
	"testing"

	"github.com/kubewharf/kubebrain/pkg/metrics"
	prom "github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
)

func TestPeerRetirementMetricsExportScopeAndUnits(t *testing.T) {
	registry := prom.NewRegistry()
	oldRegisterer, oldGather := registerer, gather
	registerer, gather = registry, registry
	t.Cleanup(func() { registerer, gather = oldRegisterer, oldGather })
	m := NewMetrics()
	for _, outcome := range []string{"missing_condition", "canceled_before_send", "confirmed", "unconfirmed"} {
		tag := metrics.Tag("outcome", outcome)
		require.NoError(t, m.EmitCounter("leader.retirement.peer.result", 1, tag))
		require.NoError(t, m.EmitHistogram("leader.retirement.peer.duration.seconds", .25, tag))
	}
	families, err := registry.Gather()
	require.NoError(t, err)
	require.Len(t, families, 2)
	for _, family := range families {
		require.Contains(t, family.GetHelp(), "successor readiness")
		require.Len(t, family.Metric, 4)
		for _, sample := range family.Metric {
			require.Len(t, sample.Label, 1)
			require.Equal(t, "outcome", sample.Label[0].GetName())
			if family.GetName() == "leader_retirement_peer_duration_seconds" {
				require.Contains(t, family.GetHelp(), "excludes lifecycle join and local release")
				require.EqualValues(t, 1, sample.GetHistogram().GetSampleCount())
				require.Equal(t, .25, sample.GetHistogram().GetSampleSum())
			} else {
				require.Equal(t, "leader_retirement_peer_result", family.GetName())
				require.Equal(t, float64(1), sample.GetCounter().GetValue())
			}
		}
	}
}

func TestLocalRetirementMetricsExportScopeAndUnits(t *testing.T) {
	registry := prom.NewRegistry()
	oldRegisterer, oldGather := registerer, gather
	registerer, gather = registry, registry
	t.Cleanup(func() { registerer, gather = oldRegisterer, oldGather })
	m := NewMetrics()
	for _, outcome := range []string{"confirmed", "unconfirmed", "deadline", "canceled"} {
		tag := metrics.Tag("outcome", outcome)
		require.NoError(t, m.EmitCounter("leader.retirement.local.result", 1, tag))
		require.NoError(t, m.EmitHistogram("leader.retirement.local.duration.seconds", .5, tag))
	}
	families, err := registry.Gather()
	require.NoError(t, err)
	require.Len(t, families, 2)
	for _, family := range families {
		require.Contains(t, family.GetHelp(), "successor readiness")
		require.Len(t, family.Metric, 4)
		for _, sample := range family.Metric {
			require.Len(t, sample.Label, 1)
			require.Equal(t, "outcome", sample.Label[0].GetName())
			if family.GetName() == "leader_retirement_local_duration_seconds" {
				require.Contains(t, family.GetHelp(), "excludes lifecycle join")
				require.EqualValues(t, 1, sample.GetHistogram().GetSampleCount())
				require.Equal(t, .5, sample.GetHistogram().GetSampleSum())
			} else {
				require.Equal(t, "leader_retirement_local_result", family.GetName())
				require.Equal(t, float64(1), sample.GetCounter().GetValue())
			}
		}
	}
}

func TestRetirementZeroBaselineDoesNotInventEventsOrReset(t *testing.T) {
	registry := prom.NewRegistry()
	oldRegisterer, oldGather := registerer, gather
	registerer, gather = registry, registry
	t.Cleanup(func() { registerer, gather = oldRegisterer, oldGather })
	m := NewMetrics()
	registrar := m.(metrics.HistogramRegistrar)
	tag := metrics.Tag("outcome", "confirmed")
	for _, stage := range []string{"local", "peer"} {
		name := "leader.retirement." + stage
		require.NoError(t, m.EmitCounter(name+".result", 0, tag))
		require.NoError(t, registrar.RegisterHistogram(name+".duration.seconds", tag))
	}
	families, err := registry.Gather()
	require.NoError(t, err)
	require.Len(t, families, 4)
	for _, family := range families {
		require.Len(t, family.Metric, 1)
		if family.Metric[0].Counter != nil {
			require.Zero(t, family.Metric[0].GetCounter().GetValue())
		} else {
			require.Zero(t, family.Metric[0].GetHistogram().GetSampleCount())
			require.Zero(t, family.Metric[0].GetHistogram().GetSampleSum())
		}
	}
	for _, stage := range []string{"local", "peer"} {
		name := "leader.retirement." + stage
		require.NoError(t, m.EmitCounter(name+".result", 1, tag))
		require.NoError(t, m.EmitHistogram(name+".duration.seconds", 0.25, tag))
		require.NoError(t, m.EmitCounter(name+".result", 0, tag))
		require.NoError(t, registrar.RegisterHistogram(name+".duration.seconds", tag))
	}
	families, err = registry.Gather()
	require.NoError(t, err)
	for _, family := range families {
		if family.Metric[0].Counter != nil {
			require.Equal(t, float64(1), family.Metric[0].GetCounter().GetValue())
		} else {
			require.EqualValues(t, 1, family.Metric[0].GetHistogram().GetSampleCount())
			require.Equal(t, 0.25, family.Metric[0].GetHistogram().GetSampleSum())
		}
	}
}
