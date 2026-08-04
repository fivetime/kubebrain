package compat

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestReplicaRestartRevisionRunnerFailsClosed(t *testing.T) {
	script, err := os.ReadFile("run-replica-restart-revision.sh")
	require.NoError(t, err)
	require.Contains(t, string(script), "ALLOW_DESTRUCTIVE_REPLICA_RESTART")
	require.Contains(t, string(script), "simultaneously deletes all three serving Pods")
	require.Contains(t, string(script), "controlled by a StatefulSet")
	require.Contains(t, string(script), "assert_compat_prefix_empty postflight")
	require.Contains(t, string(script), "TestIdleReplicaReplacementDoesNotAdvanceRevision")
	require.Contains(t, string(script), "TestReferenceEtcdLatestCompactionIdleRestartPreservesRevision")
	require.Contains(t, string(script), "TestLeaseExpiryReplicaReplacementPreservesRevision")
	require.Contains(t, string(script), "TestReferenceEtcdLeaseExpiryRestartPreservesRevision")
	require.Contains(t, string(script), "TestTxnSnapshotAndWatchRecoverAcrossAllReplicaReplacements")
	require.Contains(t, string(script), "TestReferenceEtcdTxnSnapshotAndWatchRecoverAfterRestart")
	require.Contains(t, string(script), "TestCompactedTxnWatchOrderRecoversAcrossAllReplicaReplacements")
	require.Contains(t, string(script), "TestReferenceEtcdCompactedTxnWatchOrderRecoversAfterRestart")
	require.Contains(t, string(script), "TestPriorCompactedTxnPrevKVRecoversAcrossAllReplicaReplacements")
	require.Contains(t, string(script), "TestReferenceEtcdPriorCompactedTxnPrevKVRecoversAfterRestart")
}

func TestReplicaRestartRevisionRunnerRejectsInvalidApprovalBeforeDependencies(t *testing.T) {
	output, err := runCompatScriptCommand(t, "run-replica-restart-revision.sh", []string{
		"PATH=" + t.TempDir() + ":/usr/bin:/bin",
		"ALLOW_DESTRUCTIVE_REPLICA_RESTART=maybe",
	})
	require.Error(t, err)
	require.Contains(t, string(output), "ALLOW_DESTRUCTIVE_REPLICA_RESTART must be true or false")
	require.NotContains(t, string(output), "missing required command")
}

func TestReplicaRestartRevisionRunnerRequiresEndpoint(t *testing.T) {
	output, err := runCompatScriptCommand(t, "run-replica-restart-revision.sh", []string{
		"PATH=" + t.TempDir() + ":/usr/bin:/bin",
		"ALLOW_DESTRUCTIVE_REPLICA_RESTART=true",
	})
	require.Error(t, err)
	require.Contains(t, string(output), "set KUBEBRAIN_IDLE_RESTART_ENDPOINT")
}

func TestReplicaRestartRevisionRunnerRequiresExplicitContext(t *testing.T) {
	output, err := runCompatScriptCommand(t, "run-replica-restart-revision.sh", []string{
		"PATH=" + t.TempDir() + ":/usr/bin:/bin",
		"KUBEBRAIN_IDLE_RESTART_ENDPOINT=127.0.0.1:30479",
		"ALLOW_DESTRUCTIVE_REPLICA_RESTART=true",
	})
	require.Error(t, err)
	require.Contains(t, string(output), "set KUBEBRAIN_IDLE_RESTART_CONTEXT explicitly")
}

func TestReplicaRestartRevisionRunnerRequiresDestructiveApproval(t *testing.T) {
	output, err := runCompatScriptCommand(t, "run-replica-restart-revision.sh", []string{
		"PATH=" + t.TempDir() + ":/usr/bin:/bin",
		"KUBEBRAIN_IDLE_RESTART_ENDPOINT=127.0.0.1:30479",
		"KUBEBRAIN_IDLE_RESTART_CONTEXT=kind-test",
	})
	require.Error(t, err)
	require.Contains(t, string(output), "refusing destructive replica-restart suite")
	require.NotContains(t, string(output), "missing required command")
}
