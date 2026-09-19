package server

import "github.com/kubewharf/kubebrain/pkg/metrics"

// Establish an observable baseline before starting the opted-in campaign.
// Adding zero never resets an existing counter; histogram registration must
// not Observe(0), which would invent a completed callback and a duration sample.
func initPeerRetirementMetrics(m metrics.Metrics) {
	if m == nil {
		return
	}
	for _, stage := range []struct {
		name     string
		outcomes []string
	}{
		{"local", []string{"confirmed", "unconfirmed", "deadline", "canceled"}},
		{"peer", []string{"confirmed", "unconfirmed", "missing_condition", "canceled_before_send"}},
	} {
		for _, outcome := range stage.outcomes {
			tag := metrics.Tag("outcome", outcome)
			_ = m.EmitCounter("leader.retirement."+stage.name+".result", 0, tag)
			if registrar, ok := m.(metrics.HistogramRegistrar); ok {
				_ = registrar.RegisterHistogram("leader.retirement."+stage.name+".duration.seconds", tag)
			}
		}
	}
}
