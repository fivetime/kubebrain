package production_test

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestColdSnapshotExecuteAndRollback(t *testing.T) {
	for _, tc := range []struct {
		name          string
		failSnapshot  bool
		contentDriver string
		wantReceipt   bool
	}{
		{name: "success restores service and publishes receipt", wantReceipt: true},
		{name: "snapshot failure restores service without receipt", failSnapshot: true},
		{name: "content driver mismatch restores service without receipt", contentDriver: "wrong.csi.test"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			inventoryFile := filepath.Join(dir, "inventory.json")
			receiptFile := filepath.Join(dir, "receipt.json")
			witnessFile := filepath.Join(dir, "witness.jsonl")
			logFile := filepath.Join(dir, "kubectl.log")
			require.NoError(t, os.WriteFile(inventoryFile, coldSnapshotInventory(t), 0o600))
			require.NoError(t, os.WriteFile(witnessFile, coldSemanticWitness(t, "/registry"), 0o600))
			fakeKubectl := filepath.Join(dir, "kubectl")
			require.NoError(t, os.WriteFile(fakeKubectl, []byte(coldSnapshotFakeKubectl), 0o755))

			command := exec.Command("bash", "../backup/cold-snapshot-execute.sh")
			command.Env = append(os.Environ(),
				"KUBECTL="+fakeKubectl,
				"PREFLIGHT_FILE="+inventoryFile,
				"RECEIPT_FILE="+receiptFile,
				"OPERATION_ID=op-20260721",
				"SEMANTIC_WITNESS_FILE="+witnessFile,
				"EXPECTED_WITNESS_PREFIX=/registry",
				"FAKE_LOG="+logFile,
				"FAKE_PVC_JSON="+coldSnapshotPVCJSON("Bound"),
				"FENCE_SETTLE_SECONDS=0",
				"FAKE_FAIL_SNAPSHOT="+map[bool]string{true: "true", false: "false"}[tc.failSnapshot],
				"FAKE_CONTENT_DRIVER="+map[bool]string{true: "csi.example.test", false: tc.contentDriver}[tc.contentDriver == ""],
			)
			output, err := command.CombinedOutput()
			if tc.wantReceipt {
				require.NoError(t, err, string(output))
				value, readErr := os.ReadFile(receiptFile)
				require.NoError(t, readErr)
				var receipt map[string]any
				require.NoError(t, json.Unmarshal(value, &receipt))
				require.Equal(t, "kubebrain.cold-physical-snapshot.v2", receipt["format"])
				require.Len(t, receipt["snapshots"], 6)
				require.Equal(t, "kubebrain.logical.v2", receipt["semantic_witness"].(map[string]any)["format"])
			} else {
				require.Error(t, err, string(output))
				require.NoFileExists(t, receiptFile)
				require.Contains(t, string(output), "service restoration was attempted")
			}

			logValue, readErr := os.ReadFile(logFile)
			require.NoError(t, readErr)
			log := string(logValue)
			requireOrder(t, log,
				"patch tidbcluster kb --type=json",
				"patch statefulset kubebrain --type=json",
				"patch statefulset kb-tikv --type=json",
				"patch statefulset kb-pd --type=json",
				"patch statefulset kb-pd --type=json",
				"patch statefulset kb-tikv --type=json",
				"patch tidbcluster kb --type=json",
				"patch statefulset kubebrain --type=json",
			)
		})
	}
}

func TestColdSnapshotExecuteRejectsLegacyLeaseWitnessBeforeMutation(t *testing.T) {
	dir := t.TempDir()
	inventoryFile := filepath.Join(dir, "inventory.json")
	receiptFile := filepath.Join(dir, "receipt.json")
	witnessFile := filepath.Join(dir, "witness.jsonl")
	logFile := filepath.Join(dir, "kubectl.log")
	require.NoError(t, os.WriteFile(inventoryFile, coldSnapshotInventory(t), 0o600))
	require.NoError(t, os.WriteFile(witnessFile, coldLeasedSemanticWitness(t, "/registry", false), 0o600))
	fakeKubectl := filepath.Join(dir, "kubectl")
	require.NoError(t, os.WriteFile(fakeKubectl, []byte(coldSnapshotFakeKubectl), 0o755))

	command := exec.Command("bash", "../backup/cold-snapshot-execute.sh")
	command.Env = append(os.Environ(),
		"KUBECTL="+fakeKubectl,
		"PREFLIGHT_FILE="+inventoryFile,
		"RECEIPT_FILE="+receiptFile,
		"OPERATION_ID=op-legacy-witness",
		"SEMANTIC_WITNESS_FILE="+witnessFile,
		"EXPECTED_WITNESS_PREFIX=/registry",
		"FAKE_LOG="+logFile,
		"FAKE_PVC_JSON="+coldSnapshotPVCJSON("Bound"),
	)
	output, err := command.CombinedOutput()
	require.Error(t, err, string(output))
	require.Contains(t, string(output), "lacks a valid granted_ttl")
	require.NoFileExists(t, receiptFile)
	logValue, readErr := os.ReadFile(logFile)
	require.NoError(t, readErr)
	require.NotContains(t, string(logValue), " patch ")
	require.NotContains(t, string(logValue), "create -f -")
}

func coldSemanticWitness(t *testing.T, prefix string) []byte {
	t.Helper()
	header, err := json.Marshal(map[string]any{
		"type": "kubebrain.logical.v2", "prefix": prefix, "revision": 10, "created_at_unix": time.Now().Unix(),
	})
	require.NoError(t, err)
	record, err := json.Marshal(map[string]any{
		"key":          base64.StdEncoding.EncodeToString([]byte(prefix + "/key")),
		"value":        base64.StdEncoding.EncodeToString([]byte("value")),
		"mod_revision": 10, "create_revision": 10, "version": 1, "lease": 0,
	})
	require.NoError(t, err)
	hashed := append(append(append([]byte(nil), header...), '\n'), record...)
	hashed = append(hashed, '\n')
	digest := sha256.Sum256(hashed)
	footer, err := json.Marshal(map[string]any{
		"type": "footer", "records": 1, "sha256": fmt.Sprintf("%x", digest[:]),
	})
	require.NoError(t, err)
	return append(hashed, append(footer, '\n')...)
}

func coldLeasedSemanticWitness(t *testing.T, prefix string, includeGrantedTTL bool) []byte {
	t.Helper()
	header, err := json.Marshal(map[string]any{
		"type": "kubebrain.logical.v2", "prefix": prefix, "revision": 10, "created_at_unix": time.Now().Unix(),
	})
	require.NoError(t, err)
	lease := map[string]any{"type": "lease", "id": 7, "ttl": 30}
	if includeGrantedTTL {
		lease["granted_ttl"] = 60
	}
	leaseLine, err := json.Marshal(lease)
	require.NoError(t, err)
	recordLine, err := json.Marshal(map[string]any{
		"key":          base64.StdEncoding.EncodeToString([]byte(prefix + "/key")),
		"value":        base64.StdEncoding.EncodeToString([]byte("value")),
		"mod_revision": 10, "create_revision": 10, "version": 1, "lease": 7,
	})
	require.NoError(t, err)
	hashed := append(append(append([]byte(nil), header...), '\n'), leaseLine...)
	hashed = append(hashed, '\n')
	hashed = append(hashed, recordLine...)
	hashed = append(hashed, '\n')
	digest := sha256.Sum256(hashed)
	footer, err := json.Marshal(map[string]any{
		"type": "footer", "records": 1, "leases": 1, "sha256": fmt.Sprintf("%x", digest[:]),
	})
	require.NoError(t, err)
	return append(hashed, append(footer, '\n')...)
}

func requireOrder(t *testing.T, value string, parts ...string) {
	t.Helper()
	position := 0
	for _, part := range parts {
		next := strings.Index(value[position:], part)
		require.NotEqualf(t, -1, next, "missing or out-of-order %q in:\n%s", part, value)
		position += next + len(part)
	}
}

func coldSnapshotInventory(t *testing.T) []byte {
	t.Helper()
	var pvc map[string]any
	require.NoError(t, json.Unmarshal([]byte(coldSnapshotPVCJSON("Bound")), &pvc))
	rawItems := pvc["items"].([]any)
	items := make([]any, 0, len(rawItems))
	for _, raw := range rawItems {
		item := raw.(map[string]any)
		metadata := item["metadata"].(map[string]any)
		spec := item["spec"].(map[string]any)
		status := item["status"].(map[string]any)
		items = append(items, map[string]any{
			"name": metadata["name"], "uid": metadata["uid"], "pv": spec["volumeName"],
			"labels":        metadata["labels"],
			"storage_class": spec["storageClassName"], "volume_mode": spec["volumeMode"],
			"access_modes": spec["accessModes"], "requested_storage": spec["resources"].(map[string]any)["requests"].(map[string]any)["storage"],
			"phase": status["phase"],
		})
	}
	value := map[string]any{
		"format":                "kubebrain.cold-physical-snapshot-preflight.v2",
		"volume_snapshot_class": map[string]any{"name": "retained", "driver": "csi.example.test", "deletion_policy": "Retain"},
		"kubebrain":             map[string]any{"namespace": "kubebrain-system", "statefulset": "kubebrain", "uid": "uid-kubebrain"},
		"storage":               map[string]any{"namespace": "tidb-cluster", "tidb_cluster": "kb", "uid": "uid-tidb", "cluster_id": "7662961163671170154"},
		"recovery_blueprint": map[string]any{"tidbcluster": map[string]any{
			"apiVersion": "pingcap.com/v1alpha1", "kind": "TidbCluster",
			"metadata": map[string]any{"name": "kb", "namespace": "tidb-cluster"},
			"spec":     map[string]any{"version": "v8.5.3", "pd": map[string]any{"replicas": 3}, "tikv": map[string]any{"replicas": 3}},
		}},
		"pd_pvcs":   items[:3],
		"tikv_pvcs": items[3:],
	}
	result, err := json.Marshal(value)
	require.NoError(t, err)
	return result
}

const coldSnapshotFakeKubectl = `#!/usr/bin/env bash
set -euo pipefail
printf '%s\n' "$*" >>"$FAKE_LOG"
args="$*"
if [[ "$args" == *"api-resources"* ]]; then
  printf '%s\n' volumesnapshots.snapshot.storage.k8s.io volumesnapshotclasses.snapshot.storage.k8s.io
elif [[ "$args" == *"get volumesnapshotclass"* ]]; then
  printf 'csi.example.test\tRetain'
elif [[ "$args" == *"get tidbcluster"* ]]; then
  printf '{"apiVersion":"pingcap.com/v1alpha1","kind":"TidbCluster","metadata":{"name":"kb","namespace":"tidb-cluster","uid":"uid-tidb","resourceVersion":"10"},"spec":{"version":"v8.5.3","pd":{"replicas":3},"tikv":{"replicas":3}},"status":{"clusterID":"7662961163671170154"}}'
elif [[ "$args" == *"get statefulset kubebrain"* && "$args" == *"jsonpath"* ]]; then
  printf 'uid-kubebrain'
elif [[ "$args" == *"get statefulset"* ]]; then
  name="$(sed -n 's/.*get statefulset \([^ ]*\).*/\1/p' <<<"$args")"
  uid="uid-${name}"
  [[ "$name" != kubebrain ]] || uid=uid-kubebrain
  printf '{"metadata":{"uid":"%s","resourceVersion":"20"},"spec":{"replicas":3}}' "$uid"
elif [[ "$args" == *"get pvc -l"* ]]; then
  printf '%s' "$FAKE_PVC_JSON"
elif [[ "$args" == *"get pvc"* && "$args" == *"jsonpath"* ]]; then
  name="$(sed -n 's/.*get pvc \([^ ]*\).*/\1/p' <<<"$args")"
  printf 'uid-%s' "$name"
elif [[ "$args" == *"create -f -"* ]]; then
  cat >/dev/null
elif [[ "$args" == *"wait --for=jsonpath={.status.readyToUse}=true"* ]]; then
  [[ "$FAKE_FAIL_SNAPSHOT" != true ]] || exit 1
elif [[ "$args" == *"get volumesnapshotcontent"* ]]; then
  name="$(sed -n 's/.*get volumesnapshotcontent \([^ ]*\).*/\1/p' <<<"$args")"
  snapshot="${name#content-}"
  printf '{"metadata":{"name":"%s","uid":"uid-%s"},"spec":{"deletionPolicy":"Retain","driver":"%s","volumeSnapshotClassName":"retained","volumeSnapshotRef":{"uid":"uid-%s"}},"status":{"snapshotHandle":"handle-%s"}}' "$name" "$name" "$FAKE_CONTENT_DRIVER" "$snapshot" "$name"
elif [[ "$args" == *"get volumesnapshot"* ]]; then
  name="$(sed -n 's/.*get volumesnapshot \([^ ]*\).*/\1/p' <<<"$args")"
  printf '{"metadata":{"name":"%s","uid":"uid-%s"},"status":{"readyToUse":true,"boundVolumeSnapshotContentName":"content-%s","restoreSize":"1Gi"}}' "$name" "$name" "$name"
elif [[ "$args" == *"patch "* || "$args" == *"wait "* || "$args" == *"scale "* || "$args" == *"rollout status"* ]]; then
  :
else
  echo "unsupported fake kubectl call: $args" >&2
  exit 1
fi
`
