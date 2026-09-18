package leader

import "sync"

// renewalTerm owns freshness callbacks for exactly one elector.Run. Retirement
// is irreversible even if a successful storage notification arrives afterward.
// This only fences freshness publication: it is not a receipt that in-flight
// lease mutations have drained or that durable ownership has been released.
type renewalTerm struct {
	mu      sync.Mutex
	retired bool
	leader  *leaderElection
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
