package leader

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/kubewharf/kubebrain/pkg/metrics"
	metricmock "github.com/kubewharf/kubebrain/pkg/metrics/mock"
	"github.com/stretchr/testify/require"
)

func TestRetiredReleaseMetricsDoNotConfirmUncertainOrLateCommit(t *testing.T) {
	for _, tc := range []struct {
		name, outcome   string
		err, contextErr error
	}{
		{"success", "confirmed", nil, nil},
		{"uncertain_commit", "unconfirmed", errors.New("private backend detail"), nil},
		{"wrapped_deadline", "deadline", fmt.Errorf("operation: %w", context.DeadlineExceeded), nil},
		{"late_success", "deadline", nil, context.DeadlineExceeded},
		{"canceled", "canceled", context.Canceled, nil},
		{"late_canceled", "canceled", nil, context.Canceled},
	} {
		t.Run(tc.name, func(t *testing.T) {
			m := metricmock.NewMockMetrics(gomock.NewController(t))
			tag := metrics.Tag("outcome", tc.outcome)
			m.EXPECT().EmitCounter("leader.retirement.local.result", 1, tag).Return(errors.New("metrics unavailable"))
			m.EXPECT().EmitHistogram("leader.retirement.local.duration.seconds", gomock.Any(), tag).
				DoAndReturn(func(_ string, value interface{}, _ ...metrics.T) error {
					require.GreaterOrEqual(t, value.(float64), float64(0))
					return errors.New("metrics unavailable")
				})
			recordRetiredRelease(m, time.Now(), tc.err, tc.contextErr)
		})
	}
	require.NotPanics(t, func() { recordRetiredRelease(nil, time.Now(), nil, nil) })
}
