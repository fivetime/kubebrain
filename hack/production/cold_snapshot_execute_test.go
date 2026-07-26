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
		name                string
		failSnapshot        bool
		contentDriver       string
		precreateReceipt    bool
		witnessPathDrift    bool
		wantReceipt         bool
		wantExistingReceipt bool
		wantError           string
	}{
		{name: "success restores service and publishes receipt", wantReceipt: true},
		{name: "witness path drift after capture still publishes receipt", witnessPathDrift: true, wantReceipt: true},
		{name: "snapshot failure restores service without receipt", failSnapshot: true},
		{name: "content driver mismatch restores service without receipt", contentDriver: "wrong.csi.test"},
		{name: "concurrent receipt publish is non overwriting", precreateReceipt: true, wantExistingReceipt: true, wantError: "cold snapshot receipt already exists"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			inventoryFile := filepath.Join(dir, "inventory.json")
			receiptFile := filepath.Join(dir, "receipt.json")
			witnessFile := filepath.Join(dir, "witness.jsonl")
			tamperedWitnessFile := filepath.Join(dir, "tampered-witness.jsonl")
			logFile := filepath.Join(dir, "kubectl.log")
			require.NoError(t, os.WriteFile(inventoryFile, coldSnapshotInventory(t), 0o600))
			require.NoError(t, os.WriteFile(witnessFile, coldSemanticWitness(t, "/registry"), 0o600))
			require.NoError(t, os.WriteFile(tamperedWitnessFile, coldLeasedSemanticWitness(t, "/registry", true), 0o600))
			fakeKubectl := filepath.Join(dir, "kubectl")
			require.NoError(t, os.WriteFile(fakeKubectl, []byte(coldSnapshotFakeKubectl), 0o755))
			realGo, err := exec.LookPath("go")
			require.NoError(t, err)
			writeTrafficExecutable(t, filepath.Join(dir, "go"), coldSnapshotFakeGo)

			env := []string{
				"PATH=" + dir + string(os.PathListSeparator) + os.Getenv("PATH"),
				"REAL_GO=" + realGo,
				"KUBECTL=" + fakeKubectl,
				"KUBE_CONTEXT=preproduction",
				"PREFLIGHT_FILE=" + inventoryFile,
				"RECEIPT_FILE=" + receiptFile,
				"OPERATION_ID=op-20260721",
				"SEMANTIC_WITNESS_FILE=" + witnessFile,
				"TAMPERED_WITNESS_FILE=" + tamperedWitnessFile,
				"TAMPER_WITNESS_AFTER_STATUS=" + map[bool]string{true: "true", false: "false"}[tc.witnessPathDrift],
				"EXPECTED_WITNESS_PREFIX=/registry",
				"FAKE_LOG=" + logFile,
				"FAKE_PVC_JSON=" + coldSnapshotPVCJSON("Bound"),
				"FENCE_SETTLE_SECONDS=0",
				"FAKE_FAIL_SNAPSHOT=" + map[bool]string{true: "true", false: "false"}[tc.failSnapshot],
				"FAKE_CONTENT_DRIVER=" + map[bool]string{true: "csi.example.test", false: tc.contentDriver}[tc.contentDriver == ""],
				"PRECREATE_COLD_SNAPSHOT_RECEIPT_DURING_CONTENT=" + map[bool]string{true: "true", false: "false"}[tc.precreateReceipt],
			}
			output, err := runColdSnapshotExecute(t, env)
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
				if tc.wantExistingReceipt {
					require.Equal(t, `{"format":"preexisting"}`+"\n", string(mustRead(t, receiptFile)))
				} else {
					require.NoFileExists(t, receiptFile)
				}
				require.Contains(t, string(output), "service restoration was attempted")
				if tc.wantError != "" {
					require.Contains(t, string(output), tc.wantError)
				}
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

func TestColdSnapshotExecuteRejectsWitnessDriftBeforeMutation(t *testing.T) {
	dir := t.TempDir()
	inventoryFile := filepath.Join(dir, "inventory.json")
	receiptFile := filepath.Join(dir, "receipt.json")
	witnessFile := filepath.Join(dir, "witness.jsonl")
	tamperedWitnessFile := filepath.Join(dir, "tampered-witness.jsonl")
	logFile := filepath.Join(dir, "kubectl.log")
	require.NoError(t, os.WriteFile(inventoryFile, coldSnapshotInventory(t), 0o600))
	require.NoError(t, os.WriteFile(witnessFile, coldSemanticWitness(t, "/registry"), 0o600))
	require.NoError(t, os.WriteFile(tamperedWitnessFile, coldLeasedSemanticWitness(t, "/registry", true), 0o600))
	fakeKubectl := filepath.Join(dir, "kubectl")
	require.NoError(t, os.WriteFile(fakeKubectl, []byte(coldSnapshotFakeKubectl), 0o755))
	realSHA, err := exec.LookPath("sha256sum")
	require.NoError(t, err)
	writeTrafficExecutable(t, filepath.Join(dir, "sha256sum"), `#!/usr/bin/env bash
set -euo pipefail
"$REAL_SHA256SUM" "$@"
if [[ "${TAMPER_WITNESS_AFTER_SHA256:-false}" == true &&
  "$#" -ge 1 && "$1" == "$SEMANTIC_WITNESS_FILE" &&
  ! -f "$FAKE_LOG.witness-tampered-after-sha256" ]]; then
  cp "$TAMPERED_WITNESS_FILE" "$SEMANTIC_WITNESS_FILE"
  chmod 600 "$SEMANTIC_WITNESS_FILE"
  touch "$FAKE_LOG.witness-tampered-after-sha256"
fi
`)

	env := []string{
		"PATH=" + dir + string(os.PathListSeparator) + os.Getenv("PATH"),
		"REAL_SHA256SUM=" + realSHA,
		"KUBECTL=" + fakeKubectl,
		"KUBE_CONTEXT=preproduction",
		"PREFLIGHT_FILE=" + inventoryFile,
		"RECEIPT_FILE=" + receiptFile,
		"OPERATION_ID=op-witness-drift",
		"SEMANTIC_WITNESS_FILE=" + witnessFile,
		"TAMPERED_WITNESS_FILE=" + tamperedWitnessFile,
		"TAMPER_WITNESS_AFTER_SHA256=true",
		"EXPECTED_WITNESS_PREFIX=/registry",
		"FAKE_LOG=" + logFile,
		"FAKE_PVC_JSON=" + coldSnapshotPVCJSON("Bound"),
		"FENCE_SETTLE_SECONDS=0",
		"FAKE_FAIL_SNAPSHOT=false",
		"FAKE_CONTENT_DRIVER=csi.example.test",
	}
	output, err := runColdSnapshotExecute(t, env)
	require.Error(t, err, string(output))
	require.Contains(t, string(output), "semantic witness file changed during capture")
	require.NoFileExists(t, receiptFile)
	logValue, readErr := os.ReadFile(logFile)
	require.NoError(t, readErr)
	require.NotContains(t, string(logValue), " patch ")
	require.NotContains(t, string(logValue), "create -f -")
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

	env := []string{
		"KUBECTL=" + fakeKubectl,
		"KUBE_CONTEXT=preproduction",
		"PREFLIGHT_FILE=" + inventoryFile,
		"RECEIPT_FILE=" + receiptFile,
		"OPERATION_ID=op-legacy-witness",
		"SEMANTIC_WITNESS_FILE=" + witnessFile,
		"EXPECTED_WITNESS_PREFIX=/registry",
		"FAKE_LOG=" + logFile,
		"FAKE_PVC_JSON=" + coldSnapshotPVCJSON("Bound"),
	}
	output, err := runColdSnapshotExecute(t, env)
	require.Error(t, err, string(output))
	require.Contains(t, string(output), "lacks a valid granted_ttl")
	require.NoFileExists(t, receiptFile)
	logValue, readErr := os.ReadFile(logFile)
	require.NoError(t, readErr)
	require.NotContains(t, string(logValue), " patch ")
	require.NotContains(t, string(logValue), "create -f -")
}

func TestColdSnapshotExecuteRequiresExplicitContextBeforeMutation(t *testing.T) {
	dir := t.TempDir()
	inventoryFile := filepath.Join(dir, "inventory.json")
	receiptFile := filepath.Join(dir, "receipt.json")
	witnessFile := filepath.Join(dir, "witness.jsonl")
	logFile := filepath.Join(dir, "kubectl.log")
	require.NoError(t, os.WriteFile(inventoryFile, coldSnapshotInventory(t), 0o600))
	require.NoError(t, os.WriteFile(witnessFile, coldSemanticWitness(t, "/registry"), 0o600))

	env := []string{
		"KUBECTL=/does/not/exist",
		"PREFLIGHT_FILE=" + inventoryFile,
		"RECEIPT_FILE=" + receiptFile,
		"OPERATION_ID=op-missing-context",
		"SEMANTIC_WITNESS_FILE=" + witnessFile,
		"EXPECTED_WITNESS_PREFIX=/registry",
		"FAKE_LOG=" + logFile,
	}
	output, err := runColdSnapshotExecute(t, env)
	require.Error(t, err, string(output))
	require.Contains(t, string(output), "KUBE_CONTEXT is required; the current context is never accepted implicitly")
	require.NoFileExists(t, receiptFile)
	require.NoFileExists(t, logFile)
}

func runColdSnapshotExecute(t *testing.T, env []string) ([]byte, error) {
	t.Helper()
	return runProductionScriptCommand(t, "../backup/cold-snapshot-execute.sh", env)
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
  if [[ "${PRECREATE_COLD_SNAPSHOT_RECEIPT_DURING_CONTENT:-false}" == true &&
    ! -f "${FAKE_LOG}.cold-snapshot-receipt-precreated" ]]; then
    printf '{"format":"preexisting"}\n' >"$RECEIPT_FILE"
    chmod 600 "$RECEIPT_FILE"
    touch "${FAKE_LOG}.cold-snapshot-receipt-precreated"
  fi
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

const coldSnapshotFakeGo = `#!/usr/bin/env bash
set -euo pipefail
status=0
"$REAL_GO" "$@" || status=$?
if [[ "$status" -eq 0 &&
  "${TAMPER_WITNESS_AFTER_STATUS:-false}" == true &&
  "$*" == *"hack/backup/cmd/logical-status"* &&
  ! -f "$FAKE_LOG.witness-tampered-after-status" ]]; then
  cp "$TAMPERED_WITNESS_FILE" "$SEMANTIC_WITNESS_FILE"
  chmod 600 "$SEMANTIC_WITNESS_FILE"
  touch "$FAKE_LOG.witness-tampered-after-status"
fi
exit "$status"
`
