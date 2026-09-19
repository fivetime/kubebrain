package server

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/kubewharf/kubebrain/pkg/metrics"
	metricmock "github.com/kubewharf/kubebrain/pkg/metrics/mock"
	"github.com/stretchr/testify/require"
)

type retirementStartupSnapshot struct {
	counters   map[string]interface{}
	histograms map[string]int
}

type retirementStartupMetrics struct {
	metrics.Metrics
	mu       sync.Mutex
	snapshot retirementStartupSnapshot
}

func (m *retirementStartupMetrics) EmitCounter(name string, value interface{}, tags ...metrics.T) error {
	if strings.HasPrefix(name, "leader.retirement.") {
		m.mu.Lock()
		if len(tags) == 1 {
			m.snapshot.counters[name+"/"+tags[0].Value] = value
		}
		m.mu.Unlock()
	}
	return m.Metrics.EmitCounter(name, value, tags...)
}

func (m *retirementStartupMetrics) RegisterHistogram(name string, tags ...metrics.T) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if strings.HasPrefix(name, "leader.retirement.") && len(tags) == 1 {
		m.snapshot.histograms[name+"/"+tags[0].Value]++
	}
	return nil
}

type retirementMetricsBeforeCampaignBackend struct {
	*blockingLeadershipPrevalidationBackend
	metrics  *retirementStartupMetrics
	observed chan retirementStartupSnapshot
}

func (b *retirementMetricsBeforeCampaignBackend) PrevalidateLeadershipRevision(ctx context.Context) error {
	m := b.metrics
	m.mu.Lock()
	snapshot := retirementStartupSnapshot{counters: map[string]interface{}{}, histograms: map[string]int{}}
	for k, v := range m.snapshot.counters {
		snapshot.counters[k] = v
	}
	for k, v := range m.snapshot.histograms {
		snapshot.histograms[k] = v
	}
	m.mu.Unlock()
	select {
	case b.observed <- snapshot:
	case <-ctx.Done():
		return ctx.Err()
	}
	return b.blockingLeadershipPrevalidationBackend.PrevalidateLeadershipRevision(ctx)
}

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
