package production_test

import (
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestColdRestoreExecute(t *testing.T) {
	for _, tc := range []struct {
		name          string
		existing      bool
		tampered      bool
		wrongCluster  bool
		wantReceipt   bool
		wantEmergency bool
	}{
		{name: "restores storage and publishes receipt", wantReceipt: true},
		{name: "existing target fails before create", existing: true},
		{name: "tampered manifest fails before target access", tampered: true},
		{name: "cluster identity mismatch fences storage", wrongCluster: true, wantEmergency: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			receiptPath := filepath.Join(dir, "snapshot.json")
			manifestPath := filepath.Join(dir, "restore.json")
			restoreReceiptPath := filepath.Join(dir, "restore-receipt.json")
			logPath := filepath.Join(dir, "kubectl.log")
			require.NoError(t, os.WriteFile(receiptPath, coldRestoreSnapshotReceipt(t), 0o600))
			render := exec.Command("go", "run", "../backup/cmd/cold-restore-render",
				"--receipt", receiptPath, "--target-snapshot-class", "target-snapshots",
				"--target-storage-class", "target-storage", "--output", manifestPath, "--confirm-isolated-target")
			renderOutput, err := render.CombinedOutput()
			require.NoError(t, err, string(renderOutput))
			if tc.tampered {
				data, readErr := os.ReadFile(manifestPath)
				require.NoError(t, readErr)
				var manifest map[string]any
				require.NoError(t, json.Unmarshal(data, &manifest))
				items := manifest["items"].([]any)
				items[len(items)-1].(map[string]any)["spec"].(map[string]any)["version"] = "tampered"
				data, readErr = json.Marshal(manifest)
				require.NoError(t, readErr)
				require.NoError(t, os.WriteFile(manifestPath, data, 0o600))
			}

			fakeKubectl := filepath.Join(dir, "kubectl")
			require.NoError(t, os.WriteFile(fakeKubectl, []byte(coldRestoreFakeKubectl), 0o755))
			command := exec.Command("bash", "../backup/cold-restore-execute.sh")
			command.Env = append(os.Environ(),
				"KUBECTL="+fakeKubectl,
				"KUBE_CONTEXT=isolated-target",
				"RECEIPT_FILE="+receiptPath,
				"RESTORE_MANIFEST="+manifestPath,
				"RESTORE_RECEIPT_FILE="+restoreReceiptPath,
				"EXPECTED_TARGET_KUBE_SYSTEM_UID=uid-kube-system-target",
				"EXPECTED_TARGET_NAMESPACE_UID=uid-target-namespace",
				"ALLOW_COLD_PHYSICAL_RESTORE=true",
				"FAKE_LOG="+logPath,
				"FAKE_EXISTING="+strconv.FormatBool(tc.existing),
				"FAKE_CLUSTER_ID="+map[bool]string{true: "99999", false: "12345"}[tc.wrongCluster],
				"FAKE_PVC_JSON="+coldRestorePVCResult(t),
				"FAKE_CONTENT_JSON="+coldRestoreContentResult(t),
			)
			output, err := command.CombinedOutput()
			if tc.wantReceipt {
				require.NoError(t, err, string(output))
				value, readErr := os.ReadFile(restoreReceiptPath)
				require.NoError(t, readErr)
				var receipt map[string]any
				require.NoError(t, json.Unmarshal(value, &receipt))
				require.Equal(t, "kubebrain.cold-physical-restore.v1", receipt["format"])
				require.Equal(t, "12345", receipt["target"].(map[string]any)["cluster_id"])
				require.Len(t, receipt["pvcs"], 6)
				require.Len(t, receipt["volume_snapshot_contents"], 6)
			} else {
				require.Error(t, err, string(output))
				require.NoFileExists(t, restoreReceiptPath)
			}

			logValue, readErr := os.ReadFile(logPath)
			if tc.tampered && os.IsNotExist(readErr) {
				logValue = nil
			} else {
				require.NoError(t, readErr)
			}
			log := string(logValue)
			if tc.existing || tc.tampered {
				require.NotContains(t, log, "create -f")
				if tc.existing {
					require.Contains(t, string(output), "target resource already exists")
				} else {
					require.Contains(t, string(output), "differs from the canonical rendering")
				}
			} else {
				require.Contains(t, log, "create -f "+manifestPath)
				requireOrder(t, log, "create -f", "patch tidbcluster kb --type=json", "wait --for=condition=Ready", "rollout status statefulset/kb-pd", "rollout status statefulset/kb-tikv")
			}
			if tc.wantEmergency {
				require.Contains(t, string(output), "identity/readiness mismatch")
				requireOrder(t, log, "wait --for=condition=Ready", "patch tidbcluster kb --type=json", "patch statefulset kb-tikv --type=json", "patch statefulset kb-pd --type=json")
			}
		})
	}
}

func coldRestoreSnapshotReceipt(t *testing.T) []byte {
	t.Helper()
	var inventory map[string]any
	require.NoError(t, json.Unmarshal(coldSnapshotInventory(t), &inventory))
	inventory["storage"].(map[string]any)["cluster_id"] = "12345"
	items := append(inventory["pd_pvcs"].([]any), inventory["tikv_pvcs"].([]any)...)
	snapshots := make([]map[string]any, 0, len(items))
	for _, raw := range items {
		volume := raw.(map[string]any)
		name := volume["name"].(string)
		component := volume["labels"].(map[string]any)["app.kubernetes.io/component"].(string)
		snapshots = append(snapshots, map[string]any{
			"source_pvc": name, "component": component, "snapshot_handle": "handle-" + name, "restore_size": "1Gi",
		})
	}
	value, err := json.Marshal(map[string]any{
		"format": "kubebrain.cold-physical-snapshot.v2", "operation_id": "restore-test",
		"created_at": "2026-07-21T00:00:00Z", "inventory": inventory, "snapshots": snapshots,
		"semantic_witness": map[string]any{
			"format": "kubebrain.logical.v2", "prefix": "/registry", "revision": 100, "records": 1, "leases": 0,
			"sha256": strings.Repeat("a", 64), "file_sha256": strings.Repeat("b", 64),
		},
	})
	require.NoError(t, err)
	return value
}

func coldRestorePVCResult(t *testing.T) string {
	t.Helper()
	var inventory map[string]any
	require.NoError(t, json.Unmarshal(coldSnapshotInventory(t), &inventory))
	volumes := append(inventory["pd_pvcs"].([]any), inventory["tikv_pvcs"].([]any)...)
	items := make([]map[string]any, 0, len(volumes))
	for _, raw := range volumes {
		name := raw.(map[string]any)["name"].(string)
		items = append(items, map[string]any{
			"metadata": map[string]any{"name": name, "uid": "target-uid-" + name},
			"spec":     map[string]any{"volumeName": "target-pv-" + name}, "status": map[string]any{"phase": "Bound"},
		})
	}
	value, err := json.Marshal(map[string]any{"items": items})
	require.NoError(t, err)
	return string(value)
}

func coldRestoreContentResult(t *testing.T) string {
	t.Helper()
	var value map[string]any
	require.NoError(t, json.Unmarshal(coldRestoreSnapshotReceipt(t), &value))
	items := make([]map[string]any, 0, 6)
	for _, raw := range value["snapshots"].([]any) {
		snapshot := raw.(map[string]any)
		name := snapshot["source_pvc"].(string)
		items = append(items, map[string]any{
			"metadata": map[string]any{"name": "content-" + name, "uid": "target-content-uid-" + name},
			"spec":     map[string]any{"driver": "csi.example.test", "source": map[string]any{"snapshotHandle": snapshot["snapshot_handle"]}},
		})
	}
	encoded, err := json.Marshal(map[string]any{"items": items})
	require.NoError(t, err)
	return string(encoded)
}

const coldRestoreFakeKubectl = `#!/usr/bin/env bash
set -euo pipefail
printf '%s\n' "$*" >>"$FAKE_LOG"
args="$*"
if [[ "$args" == *"get namespace kube-system"* ]]; then
  printf 'uid-kube-system-target'
elif [[ "$args" == *"get namespace tidb-cluster"* ]]; then
  printf 'uid-target-namespace'
elif [[ "$args" == *"api-resources --api-group=snapshot.storage.k8s.io"* ]]; then
  printf '%s\n' volumesnapshots.snapshot.storage.k8s.io volumesnapshotcontents.snapshot.storage.k8s.io
elif [[ "$args" == *"api-resources --api-group=pingcap.com"* ]]; then
  printf 'tidbclusters.pingcap.com\n'
elif [[ "$args" == *"get volumesnapshotclass"* ]]; then
  printf 'csi.example.test\tRetain'
elif [[ "$args" == *"get storageclass"* ]]; then
  printf 'csi.example.test'
elif [[ "$args" == *"--ignore-not-found"* ]]; then
  if [[ "$FAKE_EXISTING" == true && "$args" == *"get tidbcluster kb"* ]]; then printf 'tidbcluster.pingcap.com/kb'; fi
elif [[ "$args" == *"create -f"* ]]; then
  :
elif [[ "$args" == *"wait --for=jsonpath="* || "$args" == *"wait --for=condition=Ready"* || "$args" == *"rollout status"* ]]; then
  :
elif [[ "$args" == *"get tidbcluster kb -o jsonpath"* ]]; then
  printf 'uid-restored-tidb\t%s\tTrue' "$FAKE_CLUSTER_ID"
elif [[ "$args" == *"get tidbcluster kb -o json"* ]]; then
  printf '{"metadata":{"uid":"uid-restored-tidb","resourceVersion":"77"},"spec":{"paused":true}}'
elif [[ "$args" == *"get statefulset kb-"* && "$args" == *"-o json"* ]]; then
  name="$(sed -n 's/.*get statefulset \([^ ]*\).*/\1/p' <<<"$args")"
  printf '{"metadata":{"uid":"uid-%s","resourceVersion":"88"},"spec":{"replicas":3}}' "$name"
elif [[ "$args" == *"patch tidbcluster"* || "$args" == *"patch statefulset"* ]]; then
  :
elif [[ "$args" == *"get pvc -l kubebrain.io/operation-id=restore-test"* ]]; then
  printf '%s' "$FAKE_PVC_JSON"
elif [[ "$args" == *"get volumesnapshotcontent -l kubebrain.io/operation-id=restore-test"* ]]; then
  printf '%s' "$FAKE_CONTENT_JSON"
else
  echo "unsupported fake kubectl call: $args" >&2
  exit 1
fi
`
