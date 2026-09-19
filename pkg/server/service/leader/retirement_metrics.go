package leader

import (
	"context"
	"errors"
	"time"

	"github.com/kubewharf/kubebrain/pkg/metrics"
)

// Completion of the local conditional release is not successor readiness.
// An error (even after a possible commit) must never be reported as confirmed.
// Never attach the condition, scope, holder or raw storage error to telemetry.
func recordRetiredRelease(m metrics.Metrics, started time.Time, err, contextErr error) {
	if m == nil {
		return
	}
	outcome := "unconfirmed"
	switch {
	case errors.Is(err, context.DeadlineExceeded) || errors.Is(contextErr, context.DeadlineExceeded):
		outcome = "deadline"
	case errors.Is(err, context.Canceled) || errors.Is(contextErr, context.Canceled):
		outcome = "canceled"
	case err == nil && contextErr == nil:
		outcome = "confirmed"
	}
	elapsed := time.Since(started).Seconds()
	tag := metrics.Tag("outcome", outcome)
	_ = m.EmitCounter("leader.retirement.local.result", 1, tag)
	_ = m.EmitHistogram("leader.retirement.local.duration.seconds", elapsed, tag)
}
