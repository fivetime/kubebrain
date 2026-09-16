package etcd

import (
	"sync"
	"time"

	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"k8s.io/klog/v2"
)

type snapshotRejectionReason uint8

const (
	snapshotRejectedCapture snapshotRejectionReason = iota
	snapshotRejectedInflight
	snapshotRejectedRate
	snapshotRejectionReasonCount
)

// Logs supplement counters when a short-lived probe cannot scrape them. Keep
// independent, bounded per-reason limits so a rate flood cannot hide a capture
// collision. No caller metadata or request/error text is logged.
type snapshotRejectionDiagnostics struct {
	mu   sync.Mutex
	last [snapshotRejectionReasonCount]time.Time
}

func (d *snapshotRejectionDiagnostics) allow(reason snapshotRejectionReason, now time.Time) bool {
	if reason >= snapshotRejectionReasonCount {
		return false
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if !d.last[reason].IsZero() && now.Sub(d.last[reason]) < time.Second {
		return false
	}
	d.last[reason] = now
	return true
}

func (s *RPCServer) observeSnapshotRejection(method string, reason snapshotRejectionReason) {
	if method != etcdserverpb.Maintenance_Snapshot_FullMethodName || !klog.V(2).Enabled() {
		return
	}
	if !s.snapshotRejections.allow(reason, time.Now()) {
		return
	}
	names := [...]string{"local_capture_concurrency", "public_request_concurrency", "public_request_rate"}
	klog.V(2).InfoS("Snapshot admission rejected", "reason", names[reason], "scope", "local_guard", "log_interval", "1s_per_reason")
}
