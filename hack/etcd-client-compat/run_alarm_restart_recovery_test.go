package compat

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestAlarmRestartRecoveryRunnerFailsClosed(t *testing.T) {
	script, err := os.ReadFile("run-alarm-restart-recovery.sh")
	require.NoError(t, err)
	text := string(script)
	require.Contains(t, text, "ALLOW_DESTRUCTIVE_ALARM_RESTART")
	require.Contains(t, text, "controlled by a StatefulSet")
	require.Contains(t, text, "assert_alarm_set_empty postflight")
	require.Contains(t, text, "assert_compat_prefix_empty postflight")
	require.Contains(t, text, "alarm disarm")
	require.Contains(t, text, "TestUnknownAlarmMetricRecoversAcrossAllReplicaReplacements")
	require.Contains(t, text, "TestCorruptAlarmSurvivesAllReplicaReplacements")
	require.Contains(t, text, "TestReferenceEtcdUnknownAlarmSurvivesRestart")
	require.Contains(t, text, "TestReferenceEtcdCorruptAlarmSurvivesRestart")
}

func TestAlarmRestartRecoveryRunnerRejectsInvalidApprovalBeforeDependencies(t *testing.T) {
	output, err := runCompatScriptCommand(t, "run-alarm-restart-recovery.sh", []string{
		"PATH=" + t.TempDir() + ":/usr/bin:/bin",
		"ALLOW_DESTRUCTIVE_ALARM_RESTART=maybe",
	})
	require.Error(t, err)
	require.Contains(t, string(output), "ALLOW_DESTRUCTIVE_ALARM_RESTART must be true or false")
	require.NotContains(t, string(output), "missing required command")
}

func TestAlarmRestartRecoveryRunnerRequiresEndpoint(t *testing.T) {
	output, err := runCompatScriptCommand(t, "run-alarm-restart-recovery.sh", []string{
		"PATH=" + t.TempDir() + ":/usr/bin:/bin",
		"ALLOW_DESTRUCTIVE_ALARM_RESTART=true",
	})
	require.Error(t, err)
	require.Contains(t, string(output), "set KUBEBRAIN_ALARM_RESTART_ENDPOINT")
}

func TestAlarmRestartRecoveryRunnerRequiresExplicitContext(t *testing.T) {
	output, err := runCompatScriptCommand(t, "run-alarm-restart-recovery.sh", []string{
		"PATH=" + t.TempDir() + ":/usr/bin:/bin",
		"KUBEBRAIN_ALARM_RESTART_ENDPOINT=127.0.0.1:30479",
		"KUBEBRAIN_ALARM_RESTART_INFO_ENDPOINT=http://127.0.0.1:30480",
		"ALLOW_DESTRUCTIVE_ALARM_RESTART=true",
	})
	require.Error(t, err)
	require.Contains(t, string(output), "set KUBEBRAIN_ALARM_RESTART_CONTEXT explicitly")
}

func TestAlarmRestartRecoveryRunnerRequiresDestructiveApproval(t *testing.T) {
	output, err := runCompatScriptCommand(t, "run-alarm-restart-recovery.sh", []string{
		"PATH=" + t.TempDir() + ":/usr/bin:/bin",
		"KUBEBRAIN_ALARM_RESTART_ENDPOINT=127.0.0.1:30479",
		"KUBEBRAIN_ALARM_RESTART_INFO_ENDPOINT=http://127.0.0.1:30480",
		"KUBEBRAIN_ALARM_RESTART_CONTEXT=kind-test",
	})
	require.Error(t, err)
	require.Contains(t, string(output), "refusing destructive alarm-restart suite")
	require.NotContains(t, string(output), "missing required command")
}
