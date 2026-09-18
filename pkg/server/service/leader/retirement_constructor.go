package leader

import (
	"context"

	"github.com/kubewharf/kubebrain/pkg/backend"
	"github.com/kubewharf/kubebrain/pkg/backend/election"
	"github.com/kubewharf/kubebrain/pkg/metrics"
)

// NewLeaderElectionWithRetirement installs the callback before Campaign can
// start, avoiding a mutable runtime hook. The callback runs synchronously only
// after an acquired term's elector, initialization and cleanup have joined,
// while that term's freshness is irreversibly retired. available=false means
// no exact frozen condition exists and MUST NOT cause an assisted release.
// The callback must honor ctx (including shutdown cancellation), bound its work,
// and never reactivate the retired term on error or uncertain acknowledgement.
// Installing a callback does not itself authorize any remote storage mutation.
// With a scoped exact-ownership release capability, local best-effort release
// also moves after lifecycle join and is bounded by one retry period. Without
// that capability (or callback), retain client-go's ordinary release behavior.
func NewLeaderElectionWithRetirement(
	b backend.Backend, metricCli metrics.Metrics,
	onPreparingLeading func(), onStartedLeading func(context.Context), onStoppedLeading func(),
	cfg Config, onTermRetired func(context.Context, election.OwnershipCondition, bool),
) LeaderElection {
	l := NewLeaderElection(b, metricCli, onPreparingLeading, onStartedLeading, onStoppedLeading, cfg).(*leaderElection)
	l.onTermRetired = onTermRetired
	_, hasCondition := l.resourceLock.(election.OwnershipConditionProvider)
	if onTermRetired != nil && hasCondition {
		if releaser, ok := l.resourceLock.(election.RetiredOwnershipReleaser); ok {
			if scope := releaser.RetirementScope(); scope != "" {
				l.retiredReleaser = releaser
				l.retirementScope = scope
			}
		}
	}
	return l
}
