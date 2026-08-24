package compat

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestAutomaticQuotaDifferentialRunnerFailsClosed(t *testing.T) {
	script, err := os.ReadFile("run-automatic-quota-differential.sh")
	require.NoError(t, err)
	require.Contains(t, string(script), "ALLOW_DESTRUCTIVE_AUTOMATIC_QUOTA")
	require.Contains(t, string(script), "data revision must be 1")
	require.Contains(t, string(script), "auth revision must be 1")
	require.Contains(t, string(script), "does not report configured quota")
	require.Contains(t, string(script), "advertised client URL is unreachable")
	require.Contains(t, string(script), "assert_clean_endpoint postflight")
	require.Contains(t, string(script), "TestAutomaticQuotaAlarmDifferentialAgainstReferenceEtcd")
}

func TestAutomaticQuotaDifferentialRunnerRejectsUnreachableAdvertisedClientURLBeforeMutation(t *testing.T) {
	requireRunnerRejectsUnreachableAdvertisedClientURLBeforeMutation(t,
		"run-automatic-quota-differential.sh",
		"KUBEBRAIN_AUTOMATIC_QUOTA_ENDPOINT",
		"automatic-quota advertised client URL is unreachable")
}

func TestAutomaticQuotaDifferentialRunnerRejectsInvalidApprovalBeforeDependencies(t *testing.T) {
	output, err := runCompatScriptCommand(t, "run-automatic-quota-differential.sh", []string{
		"PATH=" + t.TempDir() + ":/usr/bin:/bin",
		"ALLOW_DESTRUCTIVE_AUTOMATIC_QUOTA=maybe",
	})
	require.Error(t, err)
	require.Contains(t, string(output), "ALLOW_DESTRUCTIVE_AUTOMATIC_QUOTA must be true or false")
	require.NotContains(t, string(output), "missing required command")
}

func TestAutomaticQuotaDifferentialRunnerRejectsInvalidQuotaBeforeDependencies(t *testing.T) {
	output, err := runCompatScriptCommand(t, "run-automatic-quota-differential.sh", []string{
		"PATH=" + t.TempDir() + ":/usr/bin:/bin",
		"KUBEBRAIN_AUTOMATIC_QUOTA_BYTES=0",
	})
	require.Error(t, err)
	require.Contains(t, string(output), "KUBEBRAIN_QUOTA_BYTES must be a positive integer")
	require.NotContains(t, string(output), "missing required command")
}

func TestAutomaticQuotaDifferentialRunnerRejectsFillThatCannotExerciseQuota(t *testing.T) {
	output, err := runCompatScriptCommand(t, "run-automatic-quota-differential.sh", []string{
		"PATH=" + t.TempDir() + ":/usr/bin:/bin",
		"KUBEBRAIN_AUTOMATIC_QUOTA_BYTES=1024",
		"KUBEBRAIN_AUTOMATIC_QUOTA_FILL_BYTES=1024",
	})
	require.Error(t, err)
	require.Contains(t, string(output), "KUBEBRAIN_FILL_BYTES must exceed KUBEBRAIN_QUOTA_BYTES")
	require.NotContains(t, string(output), "missing required command")
}

func TestAutomaticQuotaDifferentialRunnerRejectsFillAboveRequestBoundary(t *testing.T) {
	output, err := runCompatScriptCommand(t, "run-automatic-quota-differential.sh", []string{
		"PATH=" + t.TempDir() + ":/usr/bin:/bin",
		"KUBEBRAIN_AUTOMATIC_QUOTA_FILL_BYTES=1500001",
	})
	require.Error(t, err)
	require.Contains(t, string(output), "fill bytes must not exceed 1500000")
	require.NotContains(t, string(output), "missing required command")
}

func TestAutomaticQuotaDifferentialRunnerRequiresDisposableEndpoint(t *testing.T) {
	output, err := runCompatScriptCommand(t, "run-automatic-quota-differential.sh", []string{
		"PATH=" + t.TempDir() + ":/usr/bin:/bin",
		"ALLOW_DESTRUCTIVE_AUTOMATIC_QUOTA=true",
	})
	require.Error(t, err)
	require.Contains(t, string(output), "set KUBEBRAIN_AUTOMATIC_QUOTA_ENDPOINT")
}

func TestAutomaticQuotaDifferentialRunnerRequiresDestructiveApproval(t *testing.T) {
	output, err := runCompatScriptCommand(t, "run-automatic-quota-differential.sh", []string{
		"PATH=" + t.TempDir() + ":/usr/bin:/bin",
		"KUBEBRAIN_AUTOMATIC_QUOTA_ENDPOINT=127.0.0.1:24379",
	})
	require.Error(t, err)
	require.Contains(t, string(output), "refusing destructive automatic-quota differential")
}
