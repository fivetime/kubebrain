package election

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestComputeRetirementScopePreservesVersionedEncoding(t *testing.T) {
	scope, err := ComputeRetirementScope(42, "", "/endpoint-pair")
	require.NoError(t, err)
	// Recorded by the real two-Endpoint resource locks before exporting this
	// encoder. Refactoring tooling must not silently create a new namespace.
	require.Equal(t, "retirement-v1:16b5b4e34c199a3080ea2e0c871ef4588b2d605a208cad2dd961ed14e7511306", scope)
	for _, change := range []struct {
		cluster          uint64
		keyspace, prefix string
	}{
		{43, "", "/endpoint-pair"}, {42, "tenant", "/endpoint-pair"}, {42, "", "/endpoint-pair/"},
	} {
		other, err := ComputeRetirementScope(change.cluster, change.keyspace, change.prefix)
		require.NoError(t, err)
		require.NotEqual(t, scope, other)
	}
	_, err = ComputeRetirementScope(0, "tenant", "/prefix")
	require.Error(t, err)
	_, err = ComputeRetirementScope(42, "tenant", "")
	require.Error(t, err)
}
