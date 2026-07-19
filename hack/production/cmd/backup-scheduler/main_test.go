package main

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestParseNamespaces(t *testing.T) {
	namespaces, err := parseNamespaces("default", "tenant-a, tenant-b")
	require.NoError(t, err)
	require.Equal(t, []string{"tenant-a", "tenant-b"}, namespaces)

	namespaces, err = parseNamespaces("single", "")
	require.NoError(t, err)
	require.Equal(t, []string{"single"}, namespaces)
}

func TestParseNamespacesRejectsUnsafeAllowlist(t *testing.T) {
	for _, value := range []string{"", "tenant-a,", "tenant-a,tenant-a", "Tenant_A"} {
		_, err := parseNamespaces(value, value)
		require.Error(t, err, value)
	}
}
