package server

import (
	"errors"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/kubewharf/kubebrain/pkg/metrics"
	metricmock "github.com/kubewharf/kubebrain/pkg/metrics/mock"
	"github.com/stretchr/testify/require"
)

type retirementHistogramRegistration struct {
	metrics.Metrics
	names []string
	tags  []metrics.T
}

func (m *retirementHistogramRegistration) RegisterHistogram(name string, tags ...metrics.T) error {
	m.names = append(m.names, name)
	m.tags = append(m.tags, tags...)
	return errors.New("registration unavailable")
}

func TestPeerRetirementMetricInitialization(t *testing.T) {
	initPeerRetirementMetrics(nil)
	for _, withRegistrar := range []bool{false, true} {
		t.Run(map[bool]string{false: "counters-only", true: "histogram-registrar"}[withRegistrar], func(t *testing.T) {
			mock := metricmock.NewMockMetrics(gomock.NewController(t))
			for _, stage := range []struct {
				name     string
				outcomes []string
			}{
				{"local", []string{"confirmed", "unconfirmed", "deadline", "canceled"}},
				{"peer", []string{"confirmed", "unconfirmed", "missing_condition", "canceled_before_send"}},
			} {
				for _, outcome := range stage.outcomes {
					mock.EXPECT().EmitCounter("leader.retirement."+stage.name+".result", 0, metrics.Tag("outcome", outcome)).Return(errors.New("counter unavailable")).Times(2)
				}
			}
			var m metrics.Metrics = mock
			registrar := &retirementHistogramRegistration{Metrics: mock}
			if withRegistrar {
				m = registrar
			}
			// Reinitialization adds zero again, never resets counters or emits a
			// fake histogram sample. Strict mock rejects other emissions.
			initPeerRetirementMetrics(m)
			initPeerRetirementMetrics(m)
			if withRegistrar {
				require.Len(t, registrar.names, 16)
				require.Len(t, registrar.tags, 16)
				for _, name := range registrar.names {
					require.Contains(t, []string{"leader.retirement.local.duration.seconds", "leader.retirement.peer.duration.seconds"}, name)
				}
				for _, tag := range registrar.tags {
					require.Equal(t, "outcome", tag.Name)
				}
			}
		})
	}
}
