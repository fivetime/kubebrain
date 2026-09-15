package etcd

import (
	"context"
	"errors"
	"testing"

	"github.com/kubewharf/kubebrain/pkg/metrics"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/status"
)

type failingHeaderTermMetrics struct{ recordingMetrics }

func (r *failingHeaderTermMetrics) EmitHistogram(name string, value interface{}, tags ...metrics.T) error {
	_ = r.recordingMetrics.EmitHistogram(name, value, tags...)
	return errors.New("metrics sink unavailable")
}

func TestResponseRaftTermMetricsPreserveResults(t *testing.T) {
	for _, tc := range []struct {
		name, path, outcome string
		cached, fetched     uint64
		err                 error
	}{
		{name: "cache", path: "cache", outcome: "success", cached: 7},
		{name: "read", path: "read", outcome: "success", fetched: 9},
		{name: "zero_read", path: "read", outcome: "success"},
		{name: "failure", path: "read", outcome: "error", err: errors.New("record unavailable")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rec := &recordingMetrics{}
			calls := 0
			s := &RPCServer{metricCli: rec, peers: testPeerService{
				currentTermFn:    func() uint64 { return tc.cached },
				leadershipTermFn: func(context.Context) (uint64, error) { calls++; return tc.fetched, tc.err },
			}}
			term, err := s.responseRaftTerm(context.Background())
			if tc.err != nil {
				requireRaftTermUnavailable(t, err, tc.err.Error())
				require.Zero(t, term)
			} else {
				require.NoError(t, err)
				require.Equal(t, tc.cached+tc.fetched, term)
			}
			if tc.cached != 0 {
				require.Zero(t, calls)
			} else {
				require.Equal(t, 1, calls)
			}
			require.Len(t, rec.histograms, 1)
			h := rec.histograms[0]
			require.Equal(t, "kubebrain.header.term.duration.seconds", h.name)
			require.Equal(t, []metrics.T{metrics.Tag("path", tc.path), metrics.Tag("outcome", tc.outcome)}, h.tags)
			require.GreaterOrEqual(t, h.value.(float64), float64(0))
			// Instrumentation must remain optional.
			s.metricCli = nil
			termAgain, errAgain := s.responseRaftTerm(context.Background())
			require.Equal(t, term, termAgain)
			require.Equal(t, status.Convert(err).Proto(), status.Convert(errAgain).Proto())
			failedSink := &failingHeaderTermMetrics{}
			s.metricCli = failedSink
			termAgain, errAgain = s.responseRaftTerm(context.Background())
			require.Equal(t, term, termAgain)
			require.Equal(t, status.Convert(err).Proto(), status.Convert(errAgain).Proto())
			require.Len(t, failedSink.histograms, 1)
		})
	}
}
