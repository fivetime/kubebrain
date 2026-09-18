package leader

import (
	"sync"

	"github.com/kubewharf/kubebrain/pkg/backend/election"
	"k8s.io/client-go/tools/leaderelection/resourcelock"
)

// renewalTerm owns freshness callbacks for exactly one elector.Run. Retirement
// is irreversible even if a successful storage notification arrives afterward.
// This only fences freshness publication: it is not a receipt that in-flight
// lease mutations have drained or that durable ownership has been released.
type renewalTerm struct {
	mu           sync.Mutex
	retired      bool
	leader       *leaderElection
	condition    election.OwnershipCondition
	hasCondition bool
}

func (t *renewalTerm) capture(record resourcelock.LeaderElectionRecord) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.retired {
		return
	}
	if provider, ok := t.leader.resourceLock.(election.OwnershipConditionProvider); ok {
		t.condition, t.hasCondition = provider.OwnershipConditionFor(record)
	}
}

// Called only after Campaign has joined initialization and completed cleanup.
// Never consult the shared lock cache here: it may now describe another term.
func (t *renewalTerm) retiredCondition() (election.OwnershipCondition, bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if !t.retired || !t.hasCondition {
		return election.OwnershipCondition{}, false
	}
	return t.condition, true
}

func (t *renewalTerm) renew() {
	t.mu.Lock()
	defer t.mu.Unlock()
	if !t.retired {
		t.leader.stampRenew()
	}
}

func (t *renewalTerm) retire() {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.retired {
		return
	}
	t.retired = true
	t.leader.invalidateRenew()
}
