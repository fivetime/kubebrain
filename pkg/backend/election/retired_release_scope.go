package election

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"

	"github.com/kubewharf/kubebrain/pkg/storage"
)

// RetiredOwnershipReleaser is an optional capability for an authenticated
// post-retirement protocol, not permission to preempt an active holder. Callers
// MUST authenticate the holder, enforce irreversible retirement, and bind their
// configured instance identity to RetirementScope before accepting requests.
// It never accepts a storage address/key supplied by a remote peer.
type RetiredOwnershipReleaser interface {
	RetirementScope() string
	ReleaseRetiredOwnership(context.Context, string, OwnershipCondition) error
}

func retirementScopeFor(store storage.KvStorage, config Config) string {
	identifier, ok := storage.FindCapability[storage.ClusterIdentifier](store)
	if !ok || config.Prefix == "" {
		// Do not substitute backend's synthetic memkv/badger cluster ID. Such an
		// ID is not an independent storage cluster identity for this protocol.
		return ""
	}
	clusterID := identifier.ClusterID()
	scope, err := ComputeRetirementScope(clusterID, config.Keyspace, config.Prefix)
	if err != nil {
		return ""
	}
	return scope
}

// ComputeRetirementScope is the versioned, offline encoding used by the lock
// and operator tooling. Inputs must come from an independently verified storage
// cluster and the effective election namespace. Computing a hash neither proves
// those inputs nor grants authority to release an owner.
func ComputeRetirementScope(clusterID uint64, keyspace, electionPrefix string) (string, error) {
	if clusterID == 0 || electionPrefix == "" {
		return "", errors.New("retirement scope requires a storage cluster ID and election prefix")
	}
	encoded, err := json.Marshal(struct {
		Version        int
		ClusterID      uint64
		Keyspace       []byte
		ElectionPrefix []byte
	}{1, clusterID, []byte(keyspace), []byte(electionPrefix)})
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(encoded)
	return "retirement-v1:" + hex.EncodeToString(sum[:]), nil
}

func (r *resourceLock) RetirementScope() string { return r.retirementScope }

func (r *resourceLock) ReleaseRetiredOwnership(ctx context.Context, scope string, condition OwnershipCondition) error {
	if ctx == nil || scope == "" || scope != r.retirementScope {
		return errors.New("invalid retirement storage scope")
	}
	return r.releaseRetiredOwnership(ctx, condition.claim)
}
