package production_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestValidateProductionReleaseRunsAllGatesFailFast(t *testing.T) {
	tempDir := t.TempDir()
	logPath := filepath.Join(tempDir, "gates.log")
	operatorGate := filepath.Join(tempDir, "operator-gate")
	instanceGate := filepath.Join(tempDir, "instance-gate")
	regionGate := filepath.Join(tempDir, "region-gate")
	latencyGate := filepath.Join(tempDir, "latency-gate")
	require.NoError(t, os.WriteFile(operatorGate, []byte(`#!/usr/bin/env bash
set -euo pipefail
printf 'operator\n' >>"$GATE_LOG"
count=0; [[ ! -f "$OPERATOR_CALL_COUNT" ]] || count="$(<"$OPERATOR_CALL_COUNT")"
printf '%s' "$((count + 1))" >"$OPERATOR_CALL_COUNT"
if ((count > 0)); then [[ "${FAIL_OPERATOR_FENCE:-false}" != "true" ]]; else [[ "${FAIL_OPERATOR:-false}" != "true" ]]; fi
`), 0o755))
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
	require.NoError(t, os.WriteFile(latencyGate, []byte(`#!/usr/bin/env bash
set -euo pipefail
printf 'latency\n' >>"$GATE_LOG"
[[ "${FAIL_LATENCY:-false}" != "true" ]]
`), 0o755))
	baseEnv := []string{
		"KUBE_CONTEXT=test-context",
		"TIDB_OPERATOR_COMMAND=" + operatorGate,
		"INSTANCE_READY_COMMAND=" + instanceGate,
		"REGION_HEALTH_COMMAND=" + regionGate,
		"STORAGE_LATENCY_COMMAND=" + latencyGate,
		"GATE_LOG=" + logPath,
		"OPERATOR_CALL_COUNT=" + filepath.Join(tempDir, "operator-calls"),
	}

	require.NoError(t, os.RemoveAll(filepath.Join(tempDir, "operator-calls")))
	output, err := runProductionScriptCommand(t, "validate-production-release.sh", baseEnv)
	require.NoError(t, err, string(output))
	require.Contains(t, string(output), "production release gate passed")
	log, err := os.ReadFile(logPath)
	require.NoError(t, err)
	require.Equal(t, "operator\ninstance\nregion\nlatency\noperator\n", string(log))

	require.NoError(t, os.WriteFile(logPath, nil, 0o600))
	require.NoError(t, os.RemoveAll(filepath.Join(tempDir, "operator-calls")))
	output, err = runProductionScriptCommand(t, "validate-production-release.sh", append(baseEnv, "FAIL_OPERATOR=true"))
	require.Error(t, err, string(output))
	log, err = os.ReadFile(logPath)
	require.NoError(t, err)
	require.Equal(t, "operator\n", string(log))

	require.NoError(t, os.WriteFile(logPath, nil, 0o600))
	require.NoError(t, os.RemoveAll(filepath.Join(tempDir, "operator-calls")))
	output, err = runProductionScriptCommand(t, "validate-production-release.sh", append(baseEnv, "FAIL_INSTANCE=true"))
	require.Error(t, err, string(output))
	log, err = os.ReadFile(logPath)
	require.NoError(t, err)
	require.Equal(t, "operator\ninstance\n", string(log), "Region gate must not run after instance readiness fails")

	require.NoError(t, os.WriteFile(logPath, nil, 0o600))
	require.NoError(t, os.RemoveAll(filepath.Join(tempDir, "operator-calls")))
	output, err = runProductionScriptCommand(t, "validate-production-release.sh", append(baseEnv, "FAIL_REGION=true"))
	require.Error(t, err, string(output))
	log, err = os.ReadFile(logPath)
	require.NoError(t, err)
	require.Equal(t, "operator\ninstance\nregion\n", string(log))

	require.NoError(t, os.WriteFile(logPath, nil, 0o600))
	require.NoError(t, os.RemoveAll(filepath.Join(tempDir, "operator-calls")))
	output, err = runProductionScriptCommand(t, "validate-production-release.sh", append(baseEnv, "FAIL_LATENCY=true"))
	require.Error(t, err, string(output))
	log, err = os.ReadFile(logPath)
	require.NoError(t, err)
	require.Equal(t, "operator\ninstance\nregion\nlatency\n", string(log))

	require.NoError(t, os.WriteFile(logPath, nil, 0o600))
	require.NoError(t, os.RemoveAll(filepath.Join(tempDir, "operator-calls")))
	output, err = runProductionScriptCommand(t, "validate-production-release.sh", append(baseEnv, "FAIL_OPERATOR_FENCE=true"))
	require.Error(t, err, string(output))
	log, err = os.ReadFile(logPath)
	require.NoError(t, err)
	require.Equal(t, "operator\ninstance\nregion\nlatency\noperator\n", string(log))
}

func TestValidateProductionReleaseRejectsMissingContextBeforeEitherGate(t *testing.T) {
	output, err := runProductionScriptCommand(t, "validate-production-release.sh", nil)
	require.Error(t, err)
	require.Contains(t, string(output), "KUBE_CONTEXT is required")
}

func TestProductionReadinessRunbookUsesCompositeReleaseGate(t *testing.T) {
	docPath := filepath.Join("..", "..", "docs", "production_readiness_cn.md")
	contents, err := os.ReadFile(docPath)
	require.NoError(t, err)
	doc := string(contents)
	require.GreaterOrEqual(t, strings.Count(doc, "hack/production/validate-production-release.sh"), 2)
	require.NotContains(t, doc, "  hack/production/validate-instance-ready.sh\n```",
		"formal release command blocks must not bypass the composite Region/storage gate")
}
