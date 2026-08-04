package compat

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDirectReplicaConsistencyRunnerFailsClosed(t *testing.T) {
	script, err := os.ReadFile("run-direct-replica-consistency.sh")
	require.NoError(t, err)
	require.Contains(t, string(script), "ALLOW_MUTATING_DIRECT_REPLICA_CONSISTENCY")
	require.Contains(t, string(script), "one healthy three-member topology with an in-set leader")
	require.Contains(t, string(script), "assert_test_prefixes_empty postflight")
	require.Contains(t, string(script), "changed the live lease set")
	require.Contains(t, string(script), "TestHashKVSnapshotIsConsistentAcrossKubeBrainReplicas")
	require.Contains(t, string(script), "TestLeaseReadAndRevokeAcrossDirectReplicas")
	require.Contains(t, string(script), "TestWatchLocalControlResponsesAcrossDirectReplicas")
}

func TestDirectReplicaConsistencyRunnerRejectsInvalidApprovalBeforeDependencies(t *testing.T) {
	output, err := runCompatScriptCommand(t, "run-direct-replica-consistency.sh", []string{
		"PATH=" + t.TempDir() + ":/usr/bin:/bin",
		"ALLOW_MUTATING_DIRECT_REPLICA_CONSISTENCY=maybe",
	})
	require.Error(t, err)
	require.Contains(t, string(output), "ALLOW_MUTATING_DIRECT_REPLICA_CONSISTENCY must be true or false")
	require.NotContains(t, string(output), "missing required command")
}

func TestDirectReplicaConsistencyRunnerRequiresEndpointsBeforeDependencies(t *testing.T) {
	output, err := runCompatScriptCommand(t, "run-direct-replica-consistency.sh", []string{
		"PATH=" + t.TempDir() + ":/usr/bin:/bin",
		"ALLOW_MUTATING_DIRECT_REPLICA_CONSISTENCY=true",
	})
	require.Error(t, err)
	require.Contains(t, string(output), "set KUBEBRAIN_DIRECT_ENDPOINTS")
}

func TestDirectReplicaConsistencyRunnerRejectsEndpointCountBeforeDependencies(t *testing.T) {
	output, err := runCompatScriptCommand(t, "run-direct-replica-consistency.sh", []string{
		"PATH=" + t.TempDir() + ":/usr/bin:/bin",
		"KUBEBRAIN_DIRECT_ENDPOINTS=127.0.0.1:1,127.0.0.1:2",
		"ALLOW_MUTATING_DIRECT_REPLICA_CONSISTENCY=true",
	})
	require.Error(t, err)
	require.Contains(t, string(output), "must contain exactly three non-empty")
	require.NotContains(t, string(output), "missing required command")
}

func TestDirectReplicaConsistencyRunnerRequiresMutationApproval(t *testing.T) {
	output, err := runCompatScriptCommand(t, "run-direct-replica-consistency.sh", []string{
		"PATH=" + t.TempDir() + ":/usr/bin:/bin",
		"KUBEBRAIN_DIRECT_ENDPOINTS=127.0.0.1:1,127.0.0.1:2,127.0.0.1:3",
	})
	require.Error(t, err)
	require.Contains(t, string(output), "refusing mutating direct-replica consistency suite")
	require.NotContains(t, string(output), "missing required command")
}
