package compat

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestColdHeaderRecoveryRunnerFailsClosed(t *testing.T) {
	script, err := os.ReadFile("run-cold-header-recovery.sh")
	require.NoError(t, err)
	text := string(script)
	require.Contains(t, text, "ALLOW_DESTRUCTIVE_COLD_HEADER_RESTART")
	require.Contains(t, text, "controlled by a three-replica StatefulSet")
	require.Contains(t, text, "statefulset.kubernetes.io/pod-name")
	require.Contains(t, text, "assert_alarm_set_empty postflight")
	require.Contains(t, text, "assert_compat_prefix_empty postflight")
	require.Contains(t, text, "alarm disarm")
	require.Contains(t, text, "TestAlarmMutationColdReplicaHeader")
	require.Contains(t, text, "TestAuthStatusColdReplicaHeader")
	require.Contains(t, text, "TestReferenceEtcdAlarmMutationColdHeaderAfterRestart")
	require.Contains(t, text, "TestReferenceEtcdAuthStatusColdHeaderAfterRestart")
}

func TestColdHeaderRecoveryRunnerRejectsInvalidApprovalBeforeDependencies(t *testing.T) {
	output, err := runCompatScriptCommand(t, "run-cold-header-recovery.sh", []string{
		"PATH=" + t.TempDir() + ":/usr/bin:/bin",
		"ALLOW_DESTRUCTIVE_COLD_HEADER_RESTART=maybe",
	})
	require.Error(t, err)
	require.Contains(t, string(output), "ALLOW_DESTRUCTIVE_COLD_HEADER_RESTART must be true or false")
	require.NotContains(t, string(output), "missing required command")
}

func TestColdHeaderRecoveryRunnerRequiresEndpoints(t *testing.T) {
	output, err := runCompatScriptCommand(t, "run-cold-header-recovery.sh", []string{
		"PATH=" + t.TempDir() + ":/usr/bin:/bin",
		"ALLOW_DESTRUCTIVE_COLD_HEADER_RESTART=true",
	})
	require.Error(t, err)
	require.Contains(t, string(output), "set KUBEBRAIN_COLD_HEADER_BASELINE_ENDPOINT")
}

func TestColdHeaderRecoveryRunnerRequiresExplicitContext(t *testing.T) {
	output, err := runCompatScriptCommand(t, "run-cold-header-recovery.sh", []string{
		"PATH=" + t.TempDir() + ":/usr/bin:/bin",
		"KUBEBRAIN_COLD_HEADER_BASELINE_ENDPOINT=127.0.0.1:30479",
		"KUBEBRAIN_COLD_HEADER_DIRECT_ENDPOINT=127.0.0.1:30481",
		"ALLOW_DESTRUCTIVE_COLD_HEADER_RESTART=true",
	})
	require.Error(t, err)
	require.Contains(t, string(output), "set KUBEBRAIN_COLD_HEADER_CONTEXT explicitly")
}

func TestColdHeaderRecoveryRunnerRequiresDestructiveApproval(t *testing.T) {
	output, err := runCompatScriptCommand(t, "run-cold-header-recovery.sh", []string{
		"PATH=" + t.TempDir() + ":/usr/bin:/bin",
		"KUBEBRAIN_COLD_HEADER_BASELINE_ENDPOINT=127.0.0.1:30479",
		"KUBEBRAIN_COLD_HEADER_DIRECT_ENDPOINT=127.0.0.1:30481",
		"KUBEBRAIN_COLD_HEADER_CONTEXT=kind-test",
	})
	require.Error(t, err)
	require.Contains(t, string(output), "refusing destructive cold-header suite")
	require.NotContains(t, string(output), "missing required command")
}
