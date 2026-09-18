package election

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/google/uuid"
	"github.com/kubewharf/kubebrain/pkg/backend/restorationfence"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/leaderelection/resourcelock"
)

// retiredOwnership is an internal transaction precondition, NOT authentication
// or proof of local retirement. No network handler may accept it without a
// separate authenticated, irrevocable holder-retirement protocol.
type retiredOwnership struct {
	holder string
	record []byte
	token  []byte
}

// OwnershipCondition is an opaque, detached transaction condition. It is NOT a
// retirement receipt or authorization. Release requires a separately authorized
// post-retirement request and the locally bound storage scope.
type OwnershipCondition struct{ claim retiredOwnership }

func (OwnershipCondition) String() string   { return "ownership condition (redacted)" }
func (OwnershipCondition) GoString() string { return "ownership condition (redacted)" }

// OwnershipConditionProvider captures only the exact successful own mutation
// supplied by the serialized election loop. Implementations must perform no I/O.
type OwnershipConditionProvider interface {
	OwnershipConditionFor(resourcelock.LeaderElectionRecord) (OwnershipCondition, bool)
}

func (r *resourceLock) OwnershipConditionFor(expected resourcelock.LeaderElectionRecord) (OwnershipCondition, bool) {
	claim, ok := r.ownershipSnapshot()
	if !ok {
		return OwnershipCondition{}, false
	}
	raw, err := json.Marshal(expected)
	if err != nil || !bytes.Equal(raw, claim.record) {
		return OwnershipCondition{}, false
	}
	return OwnershipCondition{claim: claim}, true
}

// ownershipSnapshot copies one coherent locally known ownership condition
// without accessing storage. It is useful after the old holder loses backend
// connectivity, but says NOTHING about whether its campaign/leases have drained.
// A stale snapshot is safe only because release compares every condition again
// atomically; callers must never treat this as current ownership authorization.
func (r *resourceLock) ownershipSnapshot() (retiredOwnership, bool) {
	r.mu.Lock()
	if !r.fenceInstalled || r.record.HolderIdentity != r.lockConfig.Identity || r.lockConfig.Identity == "" {
		r.mu.Unlock()
		return retiredOwnership{}, false
	}
	claim := retiredOwnership{
		holder: r.lockConfig.Identity,
		record: append([]byte(nil), r.lastVal...),
		token:  append([]byte(nil), r.fenceToken...),
	}
	transitions := r.record.LeaderTransitions
	r.mu.Unlock()
	if len(claim.record) == 0 || len(claim.record) > 64<<10 || len(claim.token) != 36 {
		return retiredOwnership{}, false
	}
	if _, err := uuid.ParseBytes(claim.token); err != nil {
		return retiredOwnership{}, false
	}
	record, err := decodeLeaderElectionRecord(claim.record)
	if err != nil || record.HolderIdentity != claim.holder || record.LeaderTransitions != transitions || transitions < 0 || record.LeaseDurationSeconds <= 0 {
		return retiredOwnership{}, false
	}
	return claim, true
}

// releaseRetiredOwnership deliberately ignores this receiver's cached lastVal
// and ownership token. A delayed request must never release a later acquisition.
// The scoped exported adapter delegates here, but no network route is enabled
// yet. Errors, including an uncertain commit, do not authorize reactivation
// of the old holder. A replay fails its original preconditions without changing
// newer ownership; it is not reported as an independently confirmed success.
func (r *resourceLock) releaseRetiredOwnership(parent context.Context, claim retiredOwnership) error {
	record, err := validateRetiredOwnership(claim)
	if err != nil {
		return err
	}
	now := metav1.NewTime(time.Now())
	record.HolderIdentity = ""
	record.LeaseDurationSeconds = 1
	record.AcquireTime, record.RenewTime = now, now
	released, err := json.Marshal(record)
	if err != nil {
		return err
	}
	ctx, cancel := r.genContext(parent)
	defer cancel()
	if err := ctx.Err(); err != nil {
		return err
	}
	batch := r.store.BeginBatchWrite()
	batch.CAS(r.electionKey, released, claim.record, 0)
	open := []byte(restorationfence.Open)
	batch.CAS(r.restorationFenceKey, open, open, 0)
	for _, key := range r.restorationFenceKeys {
		batch.CAS(key, open, open, 0)
	}
	// Invalidate old durable writes in the SAME transaction as release, rather
	// than leaving them live until a successor eventually installs its token.
	retiredToken := []byte(uuid.NewString())
	for _, key := range r.fenceKeys {
		batch.CAS(key, retiredToken, claim.token, 0)
	}
	return batch.Commit(ctx)
}

func validateRetiredOwnership(claim retiredOwnership) (resourcelock.LeaderElectionRecord, error) {
	if claim.holder == "" || len(claim.record) == 0 || len(claim.record) > 64<<10 || len(claim.token) != 36 {
		return resourcelock.LeaderElectionRecord{}, errors.New("invalid retired ownership preconditions")
	}
	if _, err := uuid.ParseBytes(claim.token); err != nil {
		return resourcelock.LeaderElectionRecord{}, errors.New("invalid retired ownership token")
	}
	record, err := decodeLeaderElectionRecord(claim.record)
	if err != nil || record.HolderIdentity != claim.holder || record.LeaderTransitions < 0 || record.LeaseDurationSeconds <= 0 {
		return resourcelock.LeaderElectionRecord{}, errors.New("invalid retired ownership record")
	}
	return record, nil
}
