package production_test

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestInspectNativePITRTargetRetirementRequiresDestroyedStorageAndUnreachablePD(t *testing.T) {
	dir := t.TempDir()
	receiptCommand := filepath.Join(dir, "receipt")
	build := exec.Command("go", "build", "-o", receiptCommand, "../backup/cmd/native-pitr-target-retirement-receipt")
	build.Dir = "."
	output, err := build.CombinedOutput()
	require.NoError(t, err, string(output))
	provisioning, target, admission := writeRetirementEvidence(t, dir)
	kubectl := writeRetirementKubectl(t, dir)
	probe := filepath.Join(dir, "probe")
	require.NoError(t, os.WriteFile(probe, []byte("#!/usr/bin/env sh\n[ \"${OLD_PD_REACHABLE:-false}\" = true ]\n"), 0o755))
	receipt := filepath.Join(dir, "retirement.json")
	env := []string{"KUBE_CONTEXT=in-cluster", "KUBECTL=" + kubectl, "TCP_PROBE=" + probe, "RECEIPT_COMMAND=" + receiptCommand,
		"OLD_PROVISIONING=" + provisioning, "OLD_TARGET=" + target, "OLD_ADMISSION=" + admission, "OUTPUT=" + receipt}
	output, err = runProductionScriptCommand(t, "inspect-native-pitr-target-retirement.sh", env)
	require.NoError(t, err, string(output))
	var value map[string]any
	require.NoError(t, json.Unmarshal(mustReadProductionFile(t, receipt), &value))
	info, err := os.Stat(receipt)
	require.NoError(t, err)
	require.Equal(t, os.FileMode(0o600), info.Mode().Perm())
	require.Equal(t, true, value["old_tidb_cluster_uid_absent"])
	require.Equal(t, true, value["all_old_pd_endpoints_unreachable"])
	volumes := value["volumes"].([]any)
	require.Equal(t, "Released", volumes[0].(map[string]any)["pv_phase"])
	require.Equal(t, "Absent", volumes[1].(map[string]any)["pv_phase"])

	bad := filepath.Join(dir, "reachable.json")
	output, err = runProductionScriptCommand(t, "inspect-native-pitr-target-retirement.sh", append(env, "OUTPUT="+bad, "OLD_PD_REACHABLE=true"))
	require.Error(t, err, string(output))
	require.Contains(t, string(output), "remains reachable")
	require.NoFileExists(t, bad)

	for _, tc := range []struct{ name, setting, message string }{
		{"old workload", "OLD_UID_PRESENT=true", "old TidbCluster UID is still present"},
		{"old pvc", "OLD_PVC_PRESENT=true", "old PVC UID pvc-pd is still present"},
		{"old attachment", "OLD_VOLUME_ATTACHED=true", "still has a VolumeAttachment"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			failed := filepath.Join(dir, tc.name+".json")
			output, err := runProductionScriptCommand(t, "inspect-native-pitr-target-retirement.sh", append(env, "OUTPUT="+failed, tc.setting))
			require.Error(t, err, string(output))
			require.Contains(t, string(output), tc.message)
			require.NoFileExists(t, failed)
		})
	}
}

func writeRetirementEvidence(t *testing.T, dir string) (string, string, string) {
	t.Helper()
	provisioning := filepath.Join(dir, "old-provisioning.json")
	target := filepath.Join(dir, "old-target.json")
	admission := filepath.Join(dir, "old-admission.json")
	provisioningJSON := `{"format":"kubebrain.native-pitr-target-provisioning.v1","namespace":"tidb-cluster","tidb_cluster":"kb","tidb_cluster_uid":"tc-old","cluster_id":42,"pd_replicas":1,"tikv_replicas":1,"volumes":[{"component":"pd","pvc_name":"pd-kb-pd-0","pvc_uid":"pvc-pd","pv_name":"pv-pd","pv_uid":"pvuid-pd","csi_driver":"csi.test","volume_handle":"disk-pd"},{"component":"tikv","pvc_name":"tikv-kb-tikv-0","pvc_uid":"pvc-tikv","pv_name":"pv-tikv","pv_uid":"pvuid-tikv","csi_driver":"csi.test","volume_handle":"disk-tikv"}],"ready":true,"observed_at_unix":100,"read_only_inspection":true}`
	targetJSON := `{"format":"kubebrain.native-pitr-target-snapshot-empty.v1","cluster_id":42,"pd_addrs":["127.0.0.1:2379"],"stores":[{"id":1,"address":"127.0.0.1:20160"}],"snapshot_ts":10,"scan_scope":"whole-transactional-keyspace","visible_committed_key_count":0,"historical_mvcc_absence_proven":false,"raw_kv_absence_proven":false,"checked_at_unix":101,"read_only":true}`
	planSHA := fmt.Sprintf("%064x", 1)
	token := struct {
		Format          string `json:"format"`
		OperationID     string `json:"operation_id"`
		PlanSHA256      string `json:"plan_sha256"`
		TargetClusterID uint64 `json:"target_cluster_id"`
		Keyspace        string `json:"keyspace"`
	}{"kubebrain.restore-admission-token.v1", "old-restore", planSHA, 42, ""}
	tokenBytes, err := json.Marshal(token)
	require.NoError(t, err)
	sum := sha256.Sum256(tokenBytes)
	admissionJSON := fmt.Sprintf(`{"format":"kubebrain.native-pitr-restore-admission.v1","operation_id":"old-restore","plan_sha256":"%s","target_cluster_id":42,"keyspace":"","metadata_prefix":"/kubebrain/native-pitr/admission/v1/_default","token_sha256":"%s","active_sessions":0,"acquired_at_unix":99,"resumed_existing_ownership":false,"gate_held":true}`, planSHA, hex.EncodeToString(sum[:]))
	require.NoError(t, os.WriteFile(provisioning, []byte(provisioningJSON), 0o600))
	require.NoError(t, os.WriteFile(target, []byte(targetJSON), 0o600))
	require.NoError(t, os.WriteFile(admission, []byte(admissionJSON), 0o600))
	return provisioning, target, admission
}

func writeRetirementKubectl(t *testing.T, dir string) string {
	t.Helper()
	path := filepath.Join(dir, "kubectl")
	script := `#!/usr/bin/env bash
set -euo pipefail
args="$*"
if [[ "$args" == *"get tidbcluster kb --ignore-not-found"* ]]; then
  [[ "${OLD_UID_PRESENT:-false}" != true ]] || printf '%s' '{"metadata":{"uid":"tc-old"}}'
elif [[ "$args" == *"get pvc "* ]]; then
  [[ "${OLD_PVC_PRESENT:-false}" != true ]] || printf '%s' '{"metadata":{"uid":"pvc-pd"}}'
elif [[ "$args" == *"get volumeattachments.storage.k8s.io"* ]]; then
  if [[ "${OLD_VOLUME_ATTACHED:-false}" == true ]]; then printf '%s' '{"items":[{"spec":{"source":{"persistentVolumeName":"pv-pd"}}}]}'
  else printf '%s' '{"items":[]}'
  fi
elif [[ "$args" == *"get pv pv-pd"* ]]; then
  printf '%s' '{"metadata":{"name":"pv-pd","uid":"pvuid-pd"},"status":{"phase":"Released"}}'
elif [[ "$args" == *"get pv pv-tikv"* ]]; then
  exit 0
else
  echo "unexpected kubectl: $args" >&2; exit 1
fi
`
	require.NoError(t, os.WriteFile(path, []byte(script), 0o755))
	return path
}
