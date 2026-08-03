package compat

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestConfiguredCompatEndpointRequiresExplicitTarget(t *testing.T) {
	t.Setenv("KUBEBRAIN_ETCD_ENDPOINT", "")
	t.Setenv("ENDPOINT", "")
	endpoint, ok := configuredCompatEndpoint()
	require.False(t, ok)
	require.Empty(t, endpoint)
}

func TestConfiguredCompatEndpointSelection(t *testing.T) {
	t.Run("preferred", func(t *testing.T) {
		t.Setenv("KUBEBRAIN_ETCD_ENDPOINT", "http://preferred:2379")
		t.Setenv("ENDPOINT", "http://legacy:2379")
		endpoint, ok := configuredCompatEndpoint()
		require.True(t, ok)
		require.Equal(t, "http://preferred:2379", endpoint)
	})
	t.Run("legacy", func(t *testing.T) {
		t.Setenv("KUBEBRAIN_ETCD_ENDPOINT", "")
		t.Setenv("ENDPOINT", "http://legacy:2379")
		endpoint, ok := configuredCompatEndpoint()
		require.True(t, ok)
		require.Equal(t, "http://legacy:2379", endpoint)
	})
}
