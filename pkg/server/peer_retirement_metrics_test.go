package server

import (
	"context"
	"crypto/tls"
	"errors"
	"net/http"
	"sync/atomic"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/kubewharf/kubebrain/pkg/metrics"
	metricmock "github.com/kubewharf/kubebrain/pkg/metrics/mock"
	"github.com/stretchr/testify/require"
)

func TestPeerRetirementCallbackMetrics(t *testing.T) {
	_, pool, certs, _, condition := retirementHandlerFixture(t)
	for _, tc := range []struct {
		outcome             string
		available, canceled bool
		status, calls       int
	}{
		{"missing_condition", false, false, 204, 0},
		{"canceled_before_send", true, true, 204, 0},
		{"confirmed", true, false, 204, 1},
		{"unconfirmed", true, false, 503, 1},
	} {
		t.Run(tc.outcome, func(t *testing.T) {
			var calls atomic.Int32
			srv := retirementHandlerServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				w.WriteHeader(tc.status)
			}), pool, certs, false)
			sender, err := newPeerRetirementSender("instance", "old", []string{srv.URL}, &tls.Config{RootCAs: pool, Certificates: []tls.Certificate{certs[0]}}, time.Second)
			require.NoError(t, err)
			m := metricmock.NewMockMetrics(gomock.NewController(t))
			tag := metrics.Tag("outcome", tc.outcome)
			m.EXPECT().EmitCounter("leader.retirement.peer.result", 1, tag).Return(errors.New("metrics unavailable"))
			m.EXPECT().EmitHistogram("leader.retirement.peer.duration.seconds", gomock.Any(), tag).
				DoAndReturn(func(_ string, value interface{}, _ ...metrics.T) error {
					seconds, ok := value.(float64)
					require.True(t, ok)
					require.GreaterOrEqual(t, seconds, float64(0))
					return errors.New("metrics unavailable")
				})
			sender.metricCli = m
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			if tc.canceled {
				cancel()
			}
			sender.onTermRetired(ctx, condition, tc.available)
			require.Equal(t, int32(tc.calls), calls.Load(), "metrics errors must not retry or send skipped claims")
		})
	}
}

func TestPeerRetirementCallbackInvalidDependenciesRemainNoop(t *testing.T) {
	_, _, _, _, condition := retirementHandlerFixture(t)
	var absent *peerRetirementSender
	require.NotPanics(t, func() { absent.onTermRetired(nil, condition, true) })
	empty := &peerRetirementSender{}
	require.NotPanics(t, func() { empty.onTermRetired(nil, condition, true) })
}
