package election

import (
	"context"
	"testing"
	"time"

	"github.com/kubewharf/kubebrain/pkg/storage"
	"github.com/kubewharf/kubebrain/pkg/storage/memkv"
	"github.com/stretchr/testify/require"
)

type retirementIdentifiedStorage struct {
	storage.KvStorage
	id uint64
}

func (s retirementIdentifiedStorage) ClusterID() uint64 { return s.id }

func TestRetirementScopeBindsStorageAndExactNamespace(t *testing.T) {
	kv := memkv.NewKvStorage()
	t.Cleanup(func() { require.NoError(t, kv.Close()) })
	config := Config{Prefix: "/election", Keyspace: "tenant", Identity: "old", Timeout: time.Second}
	base := retirementScopeFor(retirementIdentifiedStorage{kv, 42}, config)
	require.NotEmpty(t, base)
	config.Identity = "helper"
	require.Equal(t, base, retirementScopeFor(retirementIdentifiedStorage{kv, 42}, config), "members agree on scope")
	require.NotEqual(t, base, retirementScopeFor(retirementIdentifiedStorage{kv, 43}, config))
	config.Keyspace = "other"
	require.NotEqual(t, base, retirementScopeFor(retirementIdentifiedStorage{kv, 42}, config))
	config.Keyspace = "tenant"
	config.Prefix = "/another"
	require.NotEqual(t, base, retirementScopeFor(retirementIdentifiedStorage{kv, 42}, config))
	require.Empty(t, retirementScopeFor(kv, config), "no synthetic cluster identity")
	require.Empty(t, retirementScopeFor(retirementIdentifiedStorage{kv, 0}, config))
	config.Prefix = ""
	require.Empty(t, retirementScopeFor(retirementIdentifiedStorage{kv, 42}, config))
	config.Prefix = string([]byte{0xff})
	invalidUTF8 := retirementScopeFor(retirementIdentifiedStorage{kv, 42}, config)
	config.Prefix = string([]byte{0xfe})
	require.NotEqual(t, invalidUTF8, retirementScopeFor(retirementIdentifiedStorage{kv, 42}, config), "do not normalize distinct key bytes")
}

func TestScopedRetiredReleaseRejectsWrongScopeBeforeMutation(t *testing.T) {
	a, b, kv, claim := retiredReleaseFixture(t)
	// This fixture deliberately supplies a cluster identifier; unadorned memkv
	// cannot enable the transport capability in normal construction.
	bound := NewResourceLockManager(Config{Prefix: "/release-test", Keyspace: "tenant", Identity: "helper", Timeout: time.Second}, retirementIdentifiedStorage{kv, 42}).GetResourceLock().(RetiredOwnershipReleaser)
	condition := OwnershipCondition{claim: claim}
	before := retiredReleaseSnapshot(t, b)
	for _, scope := range []string{"", "different", b.RetirementScope()} {
		require.Error(t, bound.ReleaseRetiredOwnership(context.Background(), scope, condition))
		require.Equal(t, before, retiredReleaseSnapshot(t, b))
	}
	require.Error(t, b.ReleaseRetiredOwnership(context.Background(), bound.RetirementScope(), condition))
	require.Error(t, bound.ReleaseRetiredOwnership(nil, bound.RetirementScope(), condition))
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	require.ErrorIs(t, bound.ReleaseRetiredOwnership(ctx, bound.RetirementScope(), condition), context.Canceled)
	require.Equal(t, before, retiredReleaseSnapshot(t, b))
	require.NoError(t, bound.ReleaseRetiredOwnership(context.Background(), bound.RetirementScope(), condition))
	after := retiredReleaseSnapshot(t, a)
	require.NotEqual(t, before, after)
	require.ErrorIs(t, bound.ReleaseRetiredOwnership(context.Background(), bound.RetirementScope(), condition), storage.ErrCASFailed)
	require.Equal(t, after, retiredReleaseSnapshot(t, a))
}
