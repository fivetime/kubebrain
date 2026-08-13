package production_test

import (
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestInspectNativePITRTargetProvisioningBindsKubernetesAndCSIIdentity(t *testing.T) {
	dir := t.TempDir()
	receiptCommand := filepath.Join(dir, "receipt")
	build := exec.Command("go", "build", "-o", receiptCommand, "../backup/cmd/native-pitr-target-provisioning-receipt")
	build.Dir = "."
	output, err := build.CombinedOutput()
	require.NoError(t, err, string(output))
	kubectl := writeProvisioningKubectl(t, dir)
	receipt := filepath.Join(dir, "provisioning.json")
	output, err = runProductionScriptCommand(t, "inspect-native-pitr-target-provisioning.sh", []string{
		"KUBE_CONTEXT=in-cluster", "KUBECTL=" + kubectl, "RECEIPT_COMMAND=" + receiptCommand, "OUTPUT=" + receipt,
	})
	require.NoError(t, err, string(output))
	var value map[string]any
	require.NoError(t, json.Unmarshal(mustReadProductionFile(t, receipt), &value))
	require.Equal(t, "tc-uid-new", value["tidb_cluster_uid"])
	require.Equal(t, float64(9001), value["cluster_id"])
	volumes := value["volumes"].([]any)
	require.Len(t, volumes, 2)
	require.Equal(t, "disk-pd", volumes[0].(map[string]any)["volume_handle"])
	require.Equal(t, "disk-tikv", volumes[1].(map[string]any)["volume_handle"])

	badReceipt := filepath.Join(dir, "bad.json")
	output, err = runProductionScriptCommand(t, "inspect-native-pitr-target-provisioning.sh", []string{
		"KUBE_CONTEXT=in-cluster", "KUBECTL=" + kubectl, "RECEIPT_COMMAND=" + receiptCommand, "OUTPUT=" + badReceipt, "REUSE_VOLUME_HANDLE=true",
	})
	require.Error(t, err, string(output))
	require.NoFileExists(t, badReceipt)
}

func writeProvisioningKubectl(t *testing.T, dir string) string {
	t.Helper()
	path := filepath.Join(dir, "kubectl")
	script := `#!/usr/bin/env bash
set -euo pipefail
args="$*"
if [[ "$args" == *"get tidbcluster kb -o json"* ]]; then
  printf '%s' '{"apiVersion":"pingcap.com/v1alpha1","kind":"TidbCluster","metadata":{"namespace":"tidb-cluster","name":"kb","uid":"tc-uid-new"},"spec":{"pd":{"replicas":1},"tikv":{"replicas":1}},"status":{"clusterID":"9001","conditions":[{"type":"Ready","status":"True"}]}}'
elif [[ "$args" == *"get pvc -l"* ]]; then
  printf '%s' '{"items":[{"metadata":{"name":"pd-kb-pd-0","uid":"pvc-pd","labels":{"app.kubernetes.io/instance":"kb","app.kubernetes.io/component":"pd"}},"spec":{"volumeName":"pv-pd"},"status":{"phase":"Bound"}},{"metadata":{"name":"tikv-kb-tikv-0","uid":"pvc-tikv","labels":{"app.kubernetes.io/instance":"kb","app.kubernetes.io/component":"tikv"}},"spec":{"volumeName":"pv-tikv"},"status":{"phase":"Bound"}}]}'
elif [[ "$args" == *"get pv pv-pd"* ]]; then
  printf '%s' '{"metadata":{"name":"pv-pd","uid":"pvuid-pd"},"spec":{"claimRef":{"apiVersion":"v1","kind":"PersistentVolumeClaim","namespace":"tidb-cluster","name":"pd-kb-pd-0","uid":"pvc-pd"},"csi":{"driver":"csi.test","volumeHandle":"disk-pd"}},"status":{"phase":"Bound"}}'
elif [[ "$args" == *"get pv pv-tikv"* ]]; then
  handle=disk-tikv; [[ "${REUSE_VOLUME_HANDLE:-false}" != true ]] || handle=disk-pd
  printf '{"metadata":{"name":"pv-tikv","uid":"pvuid-tikv"},"spec":{"claimRef":{"apiVersion":"v1","kind":"PersistentVolumeClaim","namespace":"tidb-cluster","name":"tikv-kb-tikv-0","uid":"pvc-tikv"},"csi":{"driver":"csi.test","volumeHandle":"%s"}},"status":{"phase":"Bound"}}' "$handle"
else
  echo "unexpected kubectl: $args" >&2; exit 1
fi
`
	require.NoError(t, os.WriteFile(path, []byte(script), 0o755))
	return path
}
