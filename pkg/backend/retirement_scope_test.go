package backend

import (
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/kubewharf/kubebrain/pkg/backend/election"
	"github.com/kubewharf/kubebrain/pkg/metrics/mock"
	"github.com/kubewharf/kubebrain/pkg/storage"
	"github.com/kubewharf/kubebrain/pkg/storage/memkv"
	"github.com/stretchr/testify/require"
)

func TestBackendRetirementScopeUsesConfiguredKeyspaceAndRealCluster(t *testing.T) {
	for _, identified := range []bool{false, true} {
		t.Run(map[bool]string{false: "synthetic", true: "identified"}[identified], func(t *testing.T) {
			var kv storage.KvStorage = memkv.NewKvStorage()
			if identified {
				kv = &clusterIDStorage{KvStorage: kv, id: 42}
			}
			config := Config{Prefix: "/scope-test", Keyspace: "tenant", Identity: "peer"}
			b := NewBackend(kv, config, mock.NewMinimalMetrics(gomock.NewController(t))).(*backend)
			t.Cleanup(func() { require.NoError(t, b.Close()) })
			scope := b.GetResourceLock().(election.RetiredOwnershipReleaser).RetirementScope()
			if !identified {
				require.NotZero(t, b.ClusterID(), "public etcd compatibility still has synthetic identity")
				require.Empty(t, scope, "synthetic identity cannot enable retirement transport")
				return
			}
			expected := election.NewResourceLockManager(election.Config{Prefix: config.Prefix, Keyspace: config.Keyspace}, kv).GetResourceLock().(election.RetiredOwnershipReleaser).RetirementScope()
			require.NotEmpty(t, scope)
			require.Equal(t, expected, scope)
			wrong := election.NewResourceLockManager(election.Config{Prefix: config.Prefix, Keyspace: "other"}, kv).GetResourceLock().(election.RetiredOwnershipReleaser).RetirementScope()
			require.NotEqual(t, wrong, scope)
		})
	}
}
