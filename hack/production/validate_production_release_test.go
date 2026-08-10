package production_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestValidateProductionReleaseRunsBothGatesFailFast(t *testing.T) {
	tempDir := t.TempDir()
	logPath := filepath.Join(tempDir, "gates.log")
	instanceGate := filepath.Join(tempDir, "instance-gate")
	regionGate := filepath.Join(tempDir, "region-gate")
	require.NoError(t, os.WriteFile(instanceGate, []byte(`#!/usr/bin/env bash
set -euo pipefail
printf 'instance\n' >>"$GATE_LOG"
[[ "${FAIL_INSTANCE:-false}" != "true" ]]
`), 0o755))
	require.NoError(t, os.WriteFile(regionGate, []byte(`#!/usr/bin/env bash
set -euo pipefail
printf 'region\n' >>"$GATE_LOG"
[[ "${FAIL_REGION:-false}" != "true" ]]
`), 0o755))
	baseEnv := []string{
		"KUBE_CONTEXT=test-context",
		"INSTANCE_READY_COMMAND=" + instanceGate,
		"REGION_HEALTH_COMMAND=" + regionGate,
		"GATE_LOG=" + logPath,
	}

	output, err := runProductionScriptCommand(t, "validate-production-release.sh", baseEnv)
	require.NoError(t, err, string(output))
	require.Contains(t, string(output), "production release gate passed")
	log, err := os.ReadFile(logPath)
	require.NoError(t, err)
	require.Equal(t, "instance\nregion\n", string(log))

	require.NoError(t, os.WriteFile(logPath, nil, 0o600))
	output, err = runProductionScriptCommand(t, "validate-production-release.sh", append(baseEnv, "FAIL_INSTANCE=true"))
	require.Error(t, err, string(output))
	log, err = os.ReadFile(logPath)
	require.NoError(t, err)
	require.Equal(t, "instance\n", string(log), "Region gate must not run after instance readiness fails")

	require.NoError(t, os.WriteFile(logPath, nil, 0o600))
	output, err = runProductionScriptCommand(t, "validate-production-release.sh", append(baseEnv, "FAIL_REGION=true"))
	require.Error(t, err, string(output))
	log, err = os.ReadFile(logPath)
	require.NoError(t, err)
	require.Equal(t, "instance\nregion\n", string(log))
}

func TestValidateProductionReleaseRejectsMissingContextBeforeEitherGate(t *testing.T) {
	output, err := runProductionScriptCommand(t, "validate-production-release.sh", nil)
	require.Error(t, err)
	require.Contains(t, string(output), "KUBE_CONTEXT is required")
}
