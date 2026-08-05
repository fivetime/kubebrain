package production_test

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestProductionReadinessColdRestoreExecuteExampleRequiresExplicitTarget(t *testing.T) {
	data, err := os.ReadFile(filepath.Join("..", "..", "docs", "production_readiness_cn.md"))
	require.NoError(t, err)
	doc := string(data)

	end := strings.Index(doc, "hack/backup/cold-restore-execute.sh")
	require.NotEqual(t, -1, end, "cold restore execute command is missing")
	start := strings.LastIndex(doc[:end], "```shell")
	require.NotEqual(t, -1, start, "cold restore execute shell example is missing")
	example := doc[start:end]
	for _, required := range []string{
		"KUBE_CONTEXT=",
		"EXPECTED_TARGET_KUBE_SYSTEM_UID=",
		"EXPECTED_TARGET_NAMESPACE_UID=",
		"ALLOW_COLD_PHYSICAL_RESTORE=true",
	} {
		require.Contains(t, example, required)
	}
	require.Contains(t, doc, "不能隐式使用当前 context")
}

func TestColdRestoreExecute(t *testing.T) {
	for _, tc := range []struct {
		name                           string
		existing                       bool
		tampered                       bool
		sourceReceiptDrift             bool
		sourceReceiptDriftDuringRender bool
		restoreManifestDriftDuringHash bool
		restoreManifestPathDrift       bool
		precreateRestoreReceipt        bool
		wrongCluster                   bool
		wrongContent                   bool
		wrongPVC                       bool
		wantReceipt                    bool
		wantPreexistingRestoreReceipt  bool
		wantEmergency                  bool
		wantCreate                     bool
		wantError                      string
	}{
		{name: "restores storage and publishes receipt", wantReceipt: true, wantCreate: true},
		{name: "existing target fails before create", existing: true, wantError: "target resource already exists"},
		{name: "tampered manifest fails before target access", tampered: true, wantError: "differs from the canonical rendering"},
		{name: "source receipt drift fails before create", sourceReceiptDrift: true, wantError: "cold snapshot receipt changed after validation"},
		{name: "source receipt drift during render uses captured receipt", sourceReceiptDriftDuringRender: true, wantError: "cold snapshot receipt changed after validation"},
		{name: "restore manifest drift during capture fails before target access", restoreManifestDriftDuringHash: true, wantError: "restore manifest changed during capture"},
		{name: "manifest path drift still applies validated manifest", restoreManifestPathDrift: true, wantReceipt: true, wantCreate: true},
		{name: "concurrent restore receipt publish is non overwriting", precreateRestoreReceipt: true, wantPreexistingRestoreReceipt: true, wantEmergency: true, wantCreate: true, wantError: "restore receipt already exists"},
		{name: "cluster identity mismatch fences storage", wrongCluster: true, wantEmergency: true, wantCreate: true, wantError: "identity/readiness mismatch"},
		{name: "content inventory drift fences storage", wrongContent: true, wantEmergency: true, wantCreate: true, wantError: "restored VolumeSnapshotContent inventory does not match restore manifest"},
		{name: "PVC inventory drift fences storage", wrongPVC: true, wantEmergency: true, wantCreate: true, wantError: "restored PVC inventory does not match restore manifest"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			receiptPath := filepath.Join(dir, "snapshot.json")
			manifestPath := filepath.Join(dir, "restore.json")
			restoreReceiptPath := filepath.Join(dir, "restore-receipt.json")
			logPath := filepath.Join(dir, "kubectl.log")
			require.NoError(t, os.WriteFile(receiptPath, coldRestoreSnapshotReceipt(t), 0o600))
			tamperedReceiptPath := filepath.Join(dir, "tampered-snapshot.json")
			require.NoError(t, os.WriteFile(tamperedReceiptPath, coldRestoreTamperedSnapshotReceipt(t), 0o600))
			renderOutput, err := runColdRestoreRender(t, receiptPath, manifestPath)
			require.NoError(t, err, string(renderOutput))
			tamperedManifestPath := filepath.Join(dir, "tampered-restore.json")
			require.NoError(t, os.WriteFile(tamperedManifestPath, coldRestoreTamperedManifest(t, manifestPath), 0o600))
			if tc.tampered {
				require.NoError(t, os.WriteFile(manifestPath, mustRead(t, tamperedManifestPath), 0o600))
			}

			fakeKubectl := filepath.Join(dir, "kubectl")
			require.NoError(t, os.WriteFile(fakeKubectl, []byte(coldRestoreFakeKubectl), 0o755))
			realGo, err := exec.LookPath("go")
			require.NoError(t, err)
			fakeGo := filepath.Join(dir, "go")
			require.NoError(t, os.WriteFile(fakeGo, []byte(coldRestoreFakeGo), 0o755))
			realSHA, err := exec.LookPath("sha256sum")
			require.NoError(t, err)
			fakeSHA := filepath.Join(dir, "sha256sum")
			require.NoError(t, os.WriteFile(fakeSHA, []byte(coldRestoreFakeSHA256Sum), 0o755))
			env := []string{
				"PATH=" + dir + string(os.PathListSeparator) + os.Getenv("PATH"),
				"REAL_GO=" + realGo,
				"REAL_SHA256SUM=" + realSHA,
				"KUBECTL=" + fakeKubectl,
				"KUBE_CONTEXT=isolated-target",
				"RECEIPT_FILE=" + receiptPath,
				"RESTORE_MANIFEST=" + manifestPath,
				"RESTORE_RECEIPT_FILE=" + restoreReceiptPath,
				"EXPECTED_TARGET_KUBE_SYSTEM_UID=uid-kube-system-target",
				"EXPECTED_TARGET_NAMESPACE_UID=uid-target-namespace",
				"ALLOW_COLD_PHYSICAL_RESTORE=true",
				"FAKE_LOG=" + logPath,
				"FAKE_EXISTING=" + strconv.FormatBool(tc.existing),
				"FAKE_CLUSTER_ID=" + map[bool]string{true: "99999", false: "12345"}[tc.wrongCluster],
				"FAKE_PVC_JSON=" + coldRestorePVCResult(t, tc.wrongPVC),
				"FAKE_CONTENT_JSON=" + coldRestoreContentResult(t, tc.wrongContent),
				"TAMPER_SOURCE_RECEIPT_DURING_KUBECTL=" + strconv.FormatBool(tc.sourceReceiptDrift),
				"TAMPER_SOURCE_RECEIPT_DURING_RENDER=" + strconv.FormatBool(tc.sourceReceiptDriftDuringRender),
				"TAMPERED_RECEIPT_FILE=" + tamperedReceiptPath,
				"TAMPER_RESTORE_MANIFEST_DURING_SHA256=" + strconv.FormatBool(tc.restoreManifestDriftDuringHash),
				"TAMPER_RESTORE_MANIFEST_DURING_KUBECTL=" + strconv.FormatBool(tc.restoreManifestPathDrift),
				"TAMPERED_RESTORE_MANIFEST=" + tamperedManifestPath,
				"PRECREATE_RESTORE_RECEIPT_DURING_INVENTORY=" + strconv.FormatBool(tc.precreateRestoreReceipt),
			}
			output, err := runColdRestoreExecute(t, env)
			if tc.wantReceipt {
				require.NoError(t, err, string(output))
				value, readErr := os.ReadFile(restoreReceiptPath)
				require.NoError(t, readErr)
				var receipt map[string]any
				require.NoError(t, json.Unmarshal(value, &receipt))
				require.Equal(t, "kubebrain.cold-physical-restore.v1", receipt["format"])
				require.Equal(t, fileDigest(t, receiptPath), receipt["source_receipt_sha256"])
				require.Equal(t, "12345", receipt["target"].(map[string]any)["cluster_id"])
				restoreManifest := receipt["restore_manifest"].(map[string]any)
				require.Equal(t, "kubernetes-list.canonical-json.v1", restoreManifest["format"])
				require.Regexp(t, "^[0-9a-f]{64}$", restoreManifest["sha256"])
				require.Equal(t, float64(19), restoreManifest["item_count"])
				require.Equal(t, float64(6), restoreManifest["volume_snapshot_contents"])
				require.Equal(t, float64(6), restoreManifest["volume_snapshots"])
				require.Equal(t, float64(6), restoreManifest["persistent_volume_claims"])
				require.Equal(t, float64(1), restoreManifest["tidbclusters"])
				require.Len(t, receipt["pvcs"], 6)
				require.Len(t, receipt["volume_snapshot_contents"], 6)
			} else {
				require.Error(t, err, string(output))
				if tc.wantPreexistingRestoreReceipt {
					require.Equal(t, `{"format":"preexisting"}`+"\n", string(mustRead(t, restoreReceiptPath)))
				} else {
					require.NoFileExists(t, restoreReceiptPath)
				}
				if tc.wantError != "" {
					require.Contains(t, string(output), tc.wantError)
				}
			}

			logValue, readErr := os.ReadFile(logPath)
			if (tc.tampered || tc.restoreManifestDriftDuringHash) && os.IsNotExist(readErr) {
				logValue = nil
			} else {
				require.NoError(t, readErr)
			}
			log := string(logValue)
			if !tc.wantCreate {
				require.NotContains(t, log, "create -f")
			} else {
				require.Contains(t, log, "create -f ")
				require.NotContains(t, log, "tampered restore manifest path was used")
				requireOrder(t, log, "create -f", "patch tidbcluster kb --type=json", "wait --for=condition=Ready", "rollout status statefulset/kb-pd", "rollout status statefulset/kb-tikv")
			}
			if tc.wantEmergency {
				requireOrder(t, log, "wait --for=condition=Ready", "patch tidbcluster kb --type=json", "patch statefulset kb-tikv --type=json", "patch statefulset kb-pd --type=json")
			}
			if tc.sourceReceiptDriftDuringRender {
				require.FileExists(t, logPath+".source-receipt-render-tampered")
			}
		})
	}
}

func TestColdRestoreExecuteRequiresExplicitAdmissionBeforeTargetAccess(t *testing.T) {
	for _, tc := range []struct {
		name    string
		omit    string
		message string
	}{
		{name: "approval", omit: "ALLOW_COLD_PHYSICAL_RESTORE", message: "set ALLOW_COLD_PHYSICAL_RESTORE=true"},
		{name: "context", omit: "KUBE_CONTEXT", message: "KUBE_CONTEXT is required; the current context is never accepted implicitly"},
		{name: "kube system uid", omit: "EXPECTED_TARGET_KUBE_SYSTEM_UID", message: "EXPECTED_TARGET_KUBE_SYSTEM_UID is required"},
		{name: "namespace uid", omit: "EXPECTED_TARGET_NAMESPACE_UID", message: "EXPECTED_TARGET_NAMESPACE_UID is required"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			receiptPath := filepath.Join(dir, "snapshot.json")
			manifestPath := filepath.Join(dir, "restore.json")
			restoreReceiptPath := filepath.Join(dir, "restore-receipt.json")
			require.NoError(t, os.WriteFile(receiptPath, coldRestoreSnapshotReceipt(t), 0o600))
			renderOutput, err := runColdRestoreRender(t, receiptPath, manifestPath)
			require.NoError(t, err, string(renderOutput))

			envByName := map[string]string{
				"KUBECTL":                         "/does/not/exist",
				"KUBE_CONTEXT":                    "isolated-target",
				"RECEIPT_FILE":                    receiptPath,
				"RESTORE_MANIFEST":                manifestPath,
				"RESTORE_RECEIPT_FILE":            restoreReceiptPath,
				"EXPECTED_TARGET_KUBE_SYSTEM_UID": "uid-kube-system-target",
				"EXPECTED_TARGET_NAMESPACE_UID":   "uid-target-namespace",
				"ALLOW_COLD_PHYSICAL_RESTORE":     "true",
			}
			env := make([]string, 0, len(envByName)-1)
			for name, value := range envByName {
				if name == tc.omit {
					continue
				}
				env = append(env, name+"="+value)
			}

			output, err := runColdRestoreExecute(t, env)
			require.Error(t, err, string(output))
			require.Contains(t, string(output), tc.message)
			require.NoFileExists(t, restoreReceiptPath)
		})
	}
}

func TestColdRestoreExecuteRechecksTargetIdentityBeforeCreate(t *testing.T) {
	dir := t.TempDir()
	receiptPath := filepath.Join(dir, "snapshot.json")
	manifestPath := filepath.Join(dir, "restore.json")
	restoreReceiptPath := filepath.Join(dir, "restore-receipt.json")
	logPath := filepath.Join(dir, "kubectl.log")
	require.NoError(t, os.WriteFile(receiptPath, coldRestoreSnapshotReceipt(t), 0o600))
	renderOutput, err := runColdRestoreRender(t, receiptPath, manifestPath)
	require.NoError(t, err, string(renderOutput))
	fakeKubectl := filepath.Join(dir, "kubectl")
	require.NoError(t, os.WriteFile(fakeKubectl, []byte(coldRestoreFakeKubectl), 0o755))

	output, err := runColdRestoreExecute(t, []string{
		"KUBECTL=" + fakeKubectl, "KUBE_CONTEXT=isolated-target",
		"RECEIPT_FILE=" + receiptPath, "RESTORE_MANIFEST=" + manifestPath,
		"RESTORE_RECEIPT_FILE=" + restoreReceiptPath,
		"EXPECTED_TARGET_KUBE_SYSTEM_UID=uid-kube-system-target",
		"EXPECTED_TARGET_NAMESPACE_UID=uid-target-namespace", "ALLOW_COLD_PHYSICAL_RESTORE=true",
		"FAKE_LOG=" + logPath, "FAKE_EXISTING=false", "FAKE_CLUSTER_ID=12345",
		"FAKE_PVC_JSON=" + coldRestorePVCResult(t, false),
		"FAKE_CONTENT_JSON=" + coldRestoreContentResult(t, false),
		"FAKE_TARGET_NAMESPACE_UID_DRIFT_BEFORE_CREATE=true",
	})
	require.Error(t, err, string(output))
	require.Contains(t, string(output), "target namespace UID mismatch")
	require.NoFileExists(t, restoreReceiptPath)
	log := string(mustRead(t, logPath))
	require.NotContains(t, log, "create -f", "a replacement namespace must never receive restore resources")
}

func runColdRestoreRender(t *testing.T, receiptPath, manifestPath string) ([]byte, error) {
	t.Helper()
	return runProductionCommand(t, "go", []string{
		"run", "../backup/cmd/cold-restore-render",
		"--receipt", receiptPath,
		"--target-snapshot-class", "target-snapshots",
		"--target-storage-class", "target-storage",
		"--output", manifestPath,
		"--confirm-isolated-target",
	}, nil)
}

func runColdRestoreExecute(t *testing.T, env []string) ([]byte, error) {
	t.Helper()
	return runProductionScriptCommand(t, "../backup/cold-restore-execute.sh", env)
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

func coldRestoreTamperedSnapshotReceipt(t *testing.T) []byte {
	t.Helper()
	var receipt map[string]any
	require.NoError(t, json.Unmarshal(coldRestoreSnapshotReceipt(t), &receipt))
	receipt["operation_id"] = "restore-test-drift"
	value, err := json.Marshal(receipt)
	require.NoError(t, err)
	return value
}

func coldRestoreTamperedManifest(t *testing.T, path string) []byte {
	t.Helper()
	var manifest map[string]any
	require.NoError(t, json.Unmarshal(mustRead(t, path), &manifest))
	items := manifest["items"].([]any)
	items[len(items)-1].(map[string]any)["spec"].(map[string]any)["version"] = "tampered"
	value, err := json.Marshal(manifest)
	require.NoError(t, err)
	return value
}

func coldRestorePVCResult(t *testing.T, wrongPhase bool) string {
	t.Helper()
	var inventory map[string]any
	require.NoError(t, json.Unmarshal(coldSnapshotInventory(t), &inventory))
	volumes := append(inventory["pd_pvcs"].([]any), inventory["tikv_pvcs"].([]any)...)
	items := make([]map[string]any, 0, len(volumes))
	for _, raw := range volumes {
		name := raw.(map[string]any)["name"].(string)
		phase := "Bound"
		if wrongPhase && len(items) == 0 {
			phase = "Pending"
		}
		items = append(items, map[string]any{
			"metadata": map[string]any{"name": name, "uid": "target-uid-" + name},
			"spec":     map[string]any{"volumeName": "target-pv-" + name}, "status": map[string]any{"phase": phase},
		})
	}
	value, err := json.Marshal(map[string]any{"items": items})
	require.NoError(t, err)
	return string(value)
}

func coldRestoreContentResult(t *testing.T, wrongHandle bool) string {
	t.Helper()
	var value map[string]any
	require.NoError(t, json.Unmarshal(coldRestoreSnapshotReceipt(t), &value))
	operationID := value["operation_id"].(string)
	items := make([]map[string]any, 0, 6)
	for _, raw := range value["snapshots"].([]any) {
		snapshot := raw.(map[string]any)
		name := snapshot["source_pvc"].(string)
		handle := snapshot["snapshot_handle"].(string)
		if wrongHandle && len(items) == 0 {
			handle = "wrong-" + handle
		}
		items = append(items, map[string]any{
			"metadata": map[string]any{"name": coldRestoreObjectName(operationID, name), "uid": "target-content-uid-" + name},
			"spec":     map[string]any{"driver": "csi.example.test", "source": map[string]any{"snapshotHandle": handle}},
		})
	}
	encoded, err := json.Marshal(map[string]any{"items": items})
	require.NoError(t, err)
	return string(encoded)
}

func coldRestoreObjectName(operation, pvcName string) string {
	sum := sha256.Sum256([]byte(operation + "\x00" + pvcName))
	prefix := strings.Trim(operation, "-")
	if len(prefix) > 40 {
		prefix = prefix[:40]
	}
	return "kb-restore-" + prefix + "-" + hex.EncodeToString(sum[:6])
}

const coldRestoreFakeKubectl = `#!/usr/bin/env bash
set -euo pipefail
printf '%s\n' "$*" >>"$FAKE_LOG"
args="$*"
maybe_tamper_inputs() {
  if [[ "${TAMPER_SOURCE_RECEIPT_DURING_KUBECTL:-false}" == true &&
    ! -f "${FAKE_LOG}.source-receipt-tampered" ]]; then
    cp "$TAMPERED_RECEIPT_FILE" "$RECEIPT_FILE"
    chmod 600 "$RECEIPT_FILE"
    touch "${FAKE_LOG}.source-receipt-tampered"
  fi
  if [[ "${TAMPER_RESTORE_MANIFEST_DURING_KUBECTL:-false}" == true &&
    ! -f "${FAKE_LOG}.restore-manifest-tampered" ]]; then
    cp "$TAMPERED_RESTORE_MANIFEST" "$RESTORE_MANIFEST"
    chmod 600 "$RESTORE_MANIFEST"
    touch "${FAKE_LOG}.restore-manifest-tampered"
  fi
}
maybe_precreate_restore_receipt() {
  if [[ "${PRECREATE_RESTORE_RECEIPT_DURING_INVENTORY:-false}" == true &&
    ! -f "${FAKE_LOG}.restore-receipt-precreated" ]]; then
    printf '{"format":"preexisting"}\n' >"$RESTORE_RECEIPT_FILE"
    chmod 600 "$RESTORE_RECEIPT_FILE"
    touch "${FAKE_LOG}.restore-receipt-precreated"
  fi
}
maybe_tamper_inputs
if [[ "$args" == *"get namespace kube-system"* ]]; then
  printf 'uid-kube-system-target'
elif [[ "$args" == *"get namespace tidb-cluster"* ]]; then
  if [[ "${FAKE_TARGET_NAMESPACE_UID_DRIFT_BEFORE_CREATE:-false}" == true ]] &&
    grep -q -- '--ignore-not-found' "$FAKE_LOG"; then
    printf 'replacement-uid-target-namespace'
  else
    printf 'uid-target-namespace'
  fi
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
  create_path="$(sed -n 's/.*create -f \([^ ]*\).*/\1/p' <<<"$args")"
  if [[ "${TAMPER_RESTORE_MANIFEST_DURING_KUBECTL:-false}" == true &&
    "$create_path" == "$RESTORE_MANIFEST" ]]; then
    echo "tampered restore manifest path was used" >&2
    exit 1
  fi
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
  maybe_precreate_restore_receipt
  printf '%s' "$FAKE_CONTENT_JSON"
else
  echo "unsupported fake kubectl call: $args" >&2
  exit 1
fi
`

const coldRestoreFakeGo = `#!/usr/bin/env bash
set -euo pipefail
if [[ "${TAMPER_SOURCE_RECEIPT_DURING_RENDER:-false}" == true &&
  "$*" == *"hack/backup/cmd/cold-restore-render"* &&
  ! -f "${FAKE_LOG}.source-receipt-render-tampered" ]]; then
  cp "$TAMPERED_RECEIPT_FILE" "$RECEIPT_FILE"
  chmod 600 "$RECEIPT_FILE"
  touch "${FAKE_LOG}.source-receipt-render-tampered"
fi
exec "$REAL_GO" "$@"
`

const coldRestoreFakeSHA256Sum = `#!/usr/bin/env bash
set -euo pipefail
"$REAL_SHA256SUM" "$@"
if [[ "${TAMPER_RESTORE_MANIFEST_DURING_SHA256:-false}" == true &&
  "$#" -ge 1 && "$1" == "$RESTORE_MANIFEST" &&
  ! -f "${FAKE_LOG}.restore-manifest-sha256-tampered" ]]; then
  cp "$TAMPERED_RESTORE_MANIFEST" "$RESTORE_MANIFEST"
  chmod 600 "$RESTORE_MANIFEST"
  touch "${FAKE_LOG}.restore-manifest-sha256-tampered"
fi
`
