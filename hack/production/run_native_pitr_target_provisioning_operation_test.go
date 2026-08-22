package production_test

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestNativePITRTargetProvisioningOperationPublishesVerifiedReceipt(t *testing.T) {
	dir := t.TempDir()
	for _, name := range []string{"retirement.json", "old.json", "manifest.json"} {
		require.NoError(t, os.WriteFile(filepath.Join(dir, name), []byte(name), 0o600))
	}
	parameters := filepath.Join(dir, "parameters.json")
	payload := fmt.Sprintf(`{"manifest":%q,"manifest_sha256":%q,"old_target_provisioning":%q,"old_target_provisioning_sha256":%q,"retirement":%q,"retirement_sha256":%q}`,
		filepath.Join(dir, "manifest.json"), fileSHA(t, filepath.Join(dir, "manifest.json")), filepath.Join(dir, "old.json"), fileSHA(t, filepath.Join(dir, "old.json")), filepath.Join(dir, "retirement.json"), fileSHA(t, filepath.Join(dir, "retirement.json")))
	require.NoError(t, os.WriteFile(parameters, []byte(payload), 0o600))
	digest := fileSHA(t, parameters)
	operationctl := filepath.Join(dir, "operationctl")
	writeExecutable(t, operationctl, fmt.Sprintf(`#!/usr/bin/env bash
set -euo pipefail
echo "$*" >>%q
if [[ "$*" == *"--action claim"* ]];then printf '%%s\n' '{"name":"native-pitr-provision-%s","operation_id":"native-pitr-provision-%s","instance":"kubebrain","type":"NativePITRTargetProvisioning","requested_by":"platform:native-pitr-target-provisioning","attempt":1,"parameters_sha256":"%s"}';fi
if [[ "$*" == *"--action heartbeat"* && -s "${BLOCK_MARKER:-/nonexistent}" ]];then exit 1;fi
`, filepath.Join(dir, "log"), digest[:20], digest[:20], digest))
	provision := filepath.Join(dir, "provision")
	writeExecutable(t, provision, `#!/usr/bin/env bash
set -euo pipefail
if [[ -n "${BLOCK_MARKER:-}" ]]; then
  printf started >"$BLOCK_MARKER"
  trap 'printf terminated >"$BLOCK_MARKER"; exit 143' TERM
  while :; do sleep 0.05; done
fi
printf provisioning >"$PROVISIONING_OUTPUT"; printf qualification >"$QUALIFICATION_OUTPUT"; printf target >"$TARGET_EMPTY_OUTPUT"; printf '{"namespace":"tidb-cluster","tidb_cluster":"kb","pd_replicas":1}' >"$AUTHORIZATION_OUTPUT"; printf creation >"$CREATION_OUTPUT"; printf writers >"$WRITER_EXCLUSION_OUTPUT"
chmod 600 "$PROVISIONING_OUTPUT" "$QUALIFICATION_OUTPUT" "$TARGET_EMPTY_OUTPUT" "$AUTHORIZATION_OUTPUT" "$CREATION_OUTPUT" "$WRITER_EXCLUSION_OUTPUT"
chmod "${UNSAFE_RECEIPT_MODE:-600}" "$QUALIFICATION_OUTPUT"
`)
	controlLog := filepath.Join(dir, "control.log")
	control := filepath.Join(dir, "control")
	writeExecutable(t, control, `#!/usr/bin/env bash
set -euo pipefail
printf '%s\n' "$*" >>"$CONTROL_LOG"
if [[ "${MUTATE_QUALIFICATION:-false}" == true ]]; then
  output=""; for arg in "$@"; do case "$arg" in --created-object=*) output="${arg#*=}";; esac; done
  printf x >>"$output"
fi
`)
	base := []string{"WORKER_ID=worker", "PARAMETERS_INPUT=" + parameters, "EXPECTED_DIGEST=" + digest, "INPUT_ROOT=" + dir, "WORK_DIR=" + dir, "OPERATIONCTL=" + operationctl, "PROVISION=" + provision, "CONTROL=" + control, "CONTROL_LOG=" + controlLog}
	_, err := runProductionScriptCommand(t, "run-native-pitr-target-provisioning-operation.sh", base)
	require.NoError(t, err)
	require.Contains(t, string(mustReadProductionFile(t, filepath.Join(dir, "log"))), "--action succeed")
	require.Equal(t, 2, len(strings.Split(strings.TrimSpace(string(mustReadProductionFile(t, controlLog))), "\n")), "final lineage must be verified twice")

	require.NoError(t, os.WriteFile(filepath.Join(dir, "log"), nil, 0o600))
	unsafeOutput, unsafeErr := runProductionScriptCommand(t, "run-native-pitr-target-provisioning-operation.sh", append(base, "UNSAFE_RECEIPT_MODE=640"))
	require.Error(t, unsafeErr, string(unsafeOutput))
	require.Contains(t, string(unsafeOutput), "final evidence is invalid")
	require.NotContains(t, string(mustReadProductionFile(t, filepath.Join(dir, "log"))), "--action succeed")

	require.NoError(t, os.WriteFile(filepath.Join(dir, "log"), nil, 0o600))
	changedOutput, changedErr := runProductionScriptCommand(t, "run-native-pitr-target-provisioning-operation.sh", append(base, "MUTATE_QUALIFICATION=true"))
	require.Error(t, changedErr, string(changedOutput))
	require.Contains(t, string(changedOutput), "changed during verification")
	require.NotContains(t, string(mustReadProductionFile(t, filepath.Join(dir, "log"))), "--action succeed")

	require.NoError(t, os.Remove(filepath.Join(dir, "log")))
	marker := filepath.Join(dir, "blocked-provision")
	output, err := runProductionScriptCommand(t, "run-native-pitr-target-provisioning-operation.sh", []string{
		"WORKER_ID=worker", "PARAMETERS_INPUT=" + parameters, "EXPECTED_DIGEST=" + digest,
		"INPUT_ROOT=" + dir, "WORK_DIR=" + dir, "OPERATIONCTL=" + operationctl, "PROVISION=" + provision, "CONTROL=/bin/true",
		"HEARTBEAT_INTERVAL_SECONDS=0.05", "BLOCK_MARKER=" + marker,
	})
	require.Error(t, err)
	require.Contains(t, string(output), "target provisioning ownership was fenced")
	require.Eventually(t, func() bool {
		body, readErr := os.ReadFile(marker)
		return readErr == nil && string(body) == "terminated"
	}, 2*time.Second, 20*time.Millisecond, "fenced provisioning process did not finish handling TERM")
	require.NotContains(t, string(mustReadProductionFile(t, filepath.Join(dir, "log"))), "--action succeed")
}

func TestNativePITRTargetProvisioningSecondAttemptDelegatesDurableDryRunRecovery(t *testing.T) {
	dir := t.TempDir()
	for _, name := range []string{"retirement.json", "old.json", "manifest.json"} {
		require.NoError(t, os.WriteFile(filepath.Join(dir, name), []byte(name), 0o600))
	}
	parameters := filepath.Join(dir, "parameters.json")
	payload := fmt.Sprintf(`{"manifest":%q,"manifest_sha256":%q,"old_target_provisioning":%q,"old_target_provisioning_sha256":%q,"retirement":%q,"retirement_sha256":%q}`,
		filepath.Join(dir, "manifest.json"), fileSHA(t, filepath.Join(dir, "manifest.json")), filepath.Join(dir, "old.json"), fileSHA(t, filepath.Join(dir, "old.json")), filepath.Join(dir, "retirement.json"), fileSHA(t, filepath.Join(dir, "retirement.json")))
	require.NoError(t, os.WriteFile(parameters, []byte(payload), 0o600))
	digest := fileSHA(t, parameters)
	operationctl := filepath.Join(dir, "operationctl")
	writeExecutable(t, operationctl, fmt.Sprintf(`#!/usr/bin/env bash
if [[ "$*" == *"--action claim"* ]];then printf '%%s\n' '{"name":"native-pitr-provision-%s","operation_id":"native-pitr-provision-%s","instance":"kubebrain","type":"NativePITRTargetProvisioning","requested_by":"platform:native-pitr-target-provisioning","attempt":2,"parameters_sha256":"%s"}';fi
`, digest[:20], digest[:20], digest))
	provision := filepath.Join(dir, "provision")
	writeExecutable(t, provision, `#!/usr/bin/env bash
set -euo pipefail
printf provisioning >"$PROVISIONING_OUTPUT"; printf qualification >"$QUALIFICATION_OUTPUT"; printf target >"$TARGET_EMPTY_OUTPUT"; printf creation >"$CREATION_OUTPUT"; printf dry-run >"$DRY_RUN_OUTPUT"; printf writers >"$WRITER_EXCLUSION_OUTPUT"; printf '{"namespace":"tidb-cluster","tidb_cluster":"kb","pd_replicas":1}' >"$AUTHORIZATION_OUTPUT"
chmod 600 "$PROVISIONING_OUTPUT" "$QUALIFICATION_OUTPUT" "$TARGET_EMPTY_OUTPUT" "$CREATION_OUTPUT" "$DRY_RUN_OUTPUT" "$WRITER_EXCLUSION_OUTPUT" "$AUTHORIZATION_OUTPUT"
`)
	_, err := runProductionScriptCommand(t, "run-native-pitr-target-provisioning-operation.sh", []string{"WORKER_ID=worker", "PARAMETERS_INPUT=" + parameters, "EXPECTED_DIGEST=" + digest, "INPUT_ROOT=" + dir, "WORK_DIR=" + dir, "OPERATIONCTL=" + operationctl, "PROVISION=" + provision, "CONTROL=/bin/true"})
	require.NoError(t, err)
}

func TestRequestNativePITRTargetProvisioningIsApprovalBound(t *testing.T) {
	data, err := os.ReadFile("request-native-pitr-target-provisioning.sh")
	require.NoError(t, err)
	text := string(data)
	for _, expected := range []string{"NativePITRTargetProvisioning", "platform:native-pitr-target-provisioning", "--max-attempts 2", ".immutable=true", "native-pitr-provision-${digest:0:20}"} {
		require.Contains(t, text, expected)
	}
}

func fileSHA(t *testing.T, path string) string {
	t.Helper()
	data, err := os.ReadFile(path)
	require.NoError(t, err)
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}
