package production_test

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"testing"

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
`, filepath.Join(dir, "log"), digest[:20], digest[:20], digest))
	provision := filepath.Join(dir, "provision")
	writeExecutable(t, provision, `#!/usr/bin/env bash
set -euo pipefail
printf receipt >"$PROVISIONING_OUTPUT"; printf auth >"$AUTHORIZATION_OUTPUT"; printf creation >"$CREATION_OUTPUT"
`)
	_, err := runProductionScriptCommand(t, "run-native-pitr-target-provisioning-operation.sh", []string{"WORKER_ID=worker", "PARAMETERS_INPUT=" + parameters, "EXPECTED_DIGEST=" + digest, "INPUT_ROOT=" + dir, "WORK_DIR=" + dir, "OPERATIONCTL=" + operationctl, "PROVISION=" + provision})
	require.NoError(t, err)
	require.Contains(t, string(mustReadProductionFile(t, filepath.Join(dir, "log"))), "--action succeed")
}

func TestNativePITRTargetProvisioningSecondAttemptFailsWithoutCreationReceipt(t *testing.T) {
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
	_, err := runProductionScriptCommand(t, "run-native-pitr-target-provisioning-operation.sh", []string{"WORKER_ID=worker", "PARAMETERS_INPUT=" + parameters, "EXPECTED_DIGEST=" + digest, "INPUT_ROOT=" + dir, "WORK_DIR=" + dir, "OPERATIONCTL=" + operationctl, "PROVISION=/bin/false"})
	require.Error(t, err)
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
