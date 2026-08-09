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
		name                    string
		failSnapshot            bool
		contentDriver           string
		contentSourceDrift      bool
		duplicateSnapshotHandle bool
		postRestoreContentDrift bool
		precreateReceipt        bool
		witnessPathDrift        bool
		wantReceipt             bool
		wantExistingReceipt     bool
		wantError               string
	}{
		{name: "success restores service and publishes receipt", wantReceipt: true},
		{name: "witness path drift after capture still publishes receipt", witnessPathDrift: true, wantReceipt: true},
		{name: "snapshot failure restores service without receipt", failSnapshot: true},
		{name: "content driver mismatch restores service without receipt", contentDriver: "wrong.csi.test"},
		{name: "content source volume mismatch restores service without receipt", contentSourceDrift: true, wantError: "snapshot content source volume does not match"},
		{name: "duplicate CSI snapshot handle restores service without receipt", duplicateSnapshotHandle: true, wantError: "snapshot set is incomplete or contains duplicate identities"},
		{name: "post restore content drift prevents receipt", postRestoreContentDrift: true, wantError: "retained VolumeSnapshotContent changed before receipt publication"},
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
			writeTrafficExecutable(t, filepath.Join(dir, "date"), `#!/usr/bin/env bash
set -euo pipefail
if [[ "$(grep -c 'patch statefulset kubebrain' "$FAKE_LOG")" -gt 1 ]]; then
  printf '2040-01-01T00:00:00Z\n'
else
  printf '2030-01-01T00:00:00Z\n'
fi
`)

			env := []string{
				"PATH=" + dir + string(os.PathListSeparator) + os.Getenv("PATH"),
				"REAL_GO=" + realGo,
				"KUBECTL=" + fakeKubectl,
				"KUBE_CONTEXT=preproduction",
				"ALLOW_COLD_PHYSICAL_SNAPSHOT=true",
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
				"FAKE_CONTENT_SOURCE_DRIFT=" + map[bool]string{true: "true", false: "false"}[tc.contentSourceDrift],
				"FAKE_DUPLICATE_SNAPSHOT_HANDLE=" + map[bool]string{true: "true", false: "false"}[tc.duplicateSnapshotHandle],
				"FAKE_POST_RESTORE_CONTENT_DRIFT=" + map[bool]string{true: "true", false: "false"}[tc.postRestoreContentDrift],
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
				require.Equal(t, "2030-01-01T00:00:00Z", receipt["created_at"],
					"created_at must describe snapshot capture completion, not later service restoration")
				require.Len(t, receipt["snapshots"], 6)
				firstSnapshot := receipt["snapshots"].([]any)[0].(map[string]any)
				require.Equal(t, "handle-pv-pd-kb-pd-0", firstSnapshot["source_volume_handle"])
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
		"ALLOW_COLD_PHYSICAL_SNAPSHOT=true",
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
		"ALLOW_COLD_PHYSICAL_SNAPSHOT=true",
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

func TestColdSnapshotExecuteRejectsDegradedTopologyBeforeMutation(t *testing.T) {
	for _, tc := range []struct {
		name, degradedStatefulSet, degradeAfterPause, tidbReady, want string
	}{
		{name: "KubeBrain not ready", degradedStatefulSet: "kubebrain", tidbReady: "true", want: "KubeBrain StatefulSet is not fully ready"},
		{name: "PD not ready", degradedStatefulSet: "kb-pd", tidbReady: "true", want: "PD StatefulSet is not fully ready"},
		{name: "TiKV not ready", degradedStatefulSet: "kb-tikv", tidbReady: "true", want: "TiKV StatefulSet is not fully ready"},
		{name: "TidbCluster not ready", tidbReady: "false", want: "TidbCluster is not Ready"},
		{name: "TiKV degrades after operator pause", degradeAfterPause: "kb-tikv", tidbReady: "true", want: "TiKV StatefulSet is not fully ready"},
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

			output, err := runColdSnapshotExecute(t, []string{
				"KUBECTL=" + fakeKubectl,
				"KUBE_CONTEXT=preproduction",
				"ALLOW_COLD_PHYSICAL_SNAPSHOT=true",
				"PREFLIGHT_FILE=" + inventoryFile,
				"RECEIPT_FILE=" + receiptFile,
				"OPERATION_ID=op-degraded-topology",
				"SEMANTIC_WITNESS_FILE=" + witnessFile,
				"EXPECTED_WITNESS_PREFIX=/registry",
				"FAKE_LOG=" + logFile,
				"FAKE_PVC_JSON=" + coldSnapshotPVCJSON("Bound"),
				"FAKE_DEGRADED_STATEFULSET=" + tc.degradedStatefulSet,
				"FAKE_DEGRADE_AFTER_PAUSE=" + tc.degradeAfterPause,
				"FAKE_TIDB_READY=" + tc.tidbReady,
			})
			require.Error(t, err, string(output))
			require.Contains(t, string(output), tc.want)
			require.NoFileExists(t, receiptFile)
			log := string(mustRead(t, logFile))
			require.NotContains(t, log, "patch statefulset")
			require.NotContains(t, log, "create -f -")
		})
	}
}

func TestColdSnapshotExecuteRejectsBackendStatefulSetReplacementAfterPause(t *testing.T) {
	for _, tc := range []struct {
		name, statefulSet, want string
	}{
		{name: "PD replaced", statefulSet: "kb-pd", want: "PD StatefulSet UID changed at the maintenance fence"},
		{name: "TiKV replaced", statefulSet: "kb-tikv", want: "TiKV StatefulSet UID changed at the maintenance fence"},
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

			output, err := runColdSnapshotExecute(t, []string{
				"KUBECTL=" + fakeKubectl,
				"KUBE_CONTEXT=preproduction",
				"ALLOW_COLD_PHYSICAL_SNAPSHOT=true",
				"PREFLIGHT_FILE=" + inventoryFile,
				"RECEIPT_FILE=" + receiptFile,
				"OPERATION_ID=op-backend-replaced",
				"SEMANTIC_WITNESS_FILE=" + witnessFile,
				"EXPECTED_WITNESS_PREFIX=/registry",
				"FAKE_LOG=" + logFile,
				"FAKE_PVC_JSON=" + coldSnapshotPVCJSON("Bound"),
				"FAKE_TIDB_READY=true",
				"FAKE_UID_DRIFT_AFTER_PAUSE=" + tc.statefulSet,
			})
			require.Error(t, err, string(output))
			require.Contains(t, string(output), tc.want)
			require.NoFileExists(t, receiptFile)
			log := string(mustRead(t, logFile))
			require.NotContains(t, log, "patch statefulset")
			require.NotContains(t, log, "create -f -")
		})
	}
}

func TestColdSnapshotExecuteValidatesEveryPVCBeforeCreatingSnapshots(t *testing.T) {
	dir := t.TempDir()
	inventoryFile := filepath.Join(dir, "inventory.json")
	receiptFile := filepath.Join(dir, "receipt.json")
	witnessFile := filepath.Join(dir, "witness.jsonl")
	logFile := filepath.Join(dir, "kubectl.log")
	require.NoError(t, os.WriteFile(inventoryFile, coldSnapshotInventory(t), 0o600))
	require.NoError(t, os.WriteFile(witnessFile, coldSemanticWitness(t, "/registry"), 0o600))
	fakeKubectl := filepath.Join(dir, "kubectl")
	require.NoError(t, os.WriteFile(fakeKubectl, []byte(coldSnapshotFakeKubectl), 0o755))

	output, err := runColdSnapshotExecute(t, []string{
		"KUBECTL=" + fakeKubectl,
		"KUBE_CONTEXT=preproduction",
		"ALLOW_COLD_PHYSICAL_SNAPSHOT=true",
		"PREFLIGHT_FILE=" + inventoryFile,
		"RECEIPT_FILE=" + receiptFile,
		"OPERATION_ID=op-pvc-fence",
		"SEMANTIC_WITNESS_FILE=" + witnessFile,
		"EXPECTED_WITNESS_PREFIX=/registry",
		"FAKE_LOG=" + logFile,
		"FAKE_PVC_JSON=" + coldSnapshotPVCJSON("Bound"),
		"FAKE_PVC_UID_DRIFT=tikv-kb-tikv-0",
		"FAKE_TIDB_READY=true",
	})
	require.Error(t, err, string(output))
	require.Contains(t, string(output), "PVC UID changed while quiesced: tikv-kb-tikv-0")
	require.NoFileExists(t, receiptFile)
	log := string(mustRead(t, logFile))
	require.NotContains(t, log, "create -f -",
		"the complete PVC identity fence must pass before any partial snapshot is created")
}

func TestColdSnapshotExecuteRejectsPVReplacementBeforeCreatingSnapshots(t *testing.T) {
	dir := t.TempDir()
	inventoryFile := filepath.Join(dir, "inventory.json")
	receiptFile := filepath.Join(dir, "receipt.json")
	witnessFile := filepath.Join(dir, "witness.jsonl")
	logFile := filepath.Join(dir, "kubectl.log")
	require.NoError(t, os.WriteFile(inventoryFile, coldSnapshotInventory(t), 0o600))
	require.NoError(t, os.WriteFile(witnessFile, coldSemanticWitness(t, "/registry"), 0o600))
	fakeKubectl := filepath.Join(dir, "kubectl")
	require.NoError(t, os.WriteFile(fakeKubectl, []byte(coldSnapshotFakeKubectl), 0o755))

	output, err := runColdSnapshotExecute(t, []string{
		"KUBECTL=" + fakeKubectl,
		"KUBE_CONTEXT=preproduction",
		"ALLOW_COLD_PHYSICAL_SNAPSHOT=true",
		"PREFLIGHT_FILE=" + inventoryFile,
		"RECEIPT_FILE=" + receiptFile,
		"OPERATION_ID=op-pv-fence",
		"SEMANTIC_WITNESS_FILE=" + witnessFile,
		"EXPECTED_WITNESS_PREFIX=/registry",
		"FAKE_LOG=" + logFile,
		"FAKE_PVC_JSON=" + coldSnapshotPVCJSON("Bound"),
		"FAKE_PV_HANDLE_DRIFT=pv-tikv-kb-tikv-0",
		"FAKE_TIDB_READY=true",
	})
	require.Error(t, err, string(output))
	require.Contains(t, string(output), "PV identity changed while quiesced: pv-tikv-kb-tikv-0")
	require.NoFileExists(t, receiptFile)
	log := string(mustRead(t, logFile))
	require.NotContains(t, log, "create -f -",
		"the complete PV identity fence must pass before any partial snapshot is created")
}

func TestColdSnapshotExecuteRejectsOversizedDerivedNameBeforeMutation(t *testing.T) {
	for _, tc := range []struct{ name, pvcName, want string }{
		{
			name: "total DNS subdomain length",
			pvcName: strings.Join([]string{
				strings.Repeat("a", 60), strings.Repeat("b", 60), strings.Repeat("c", 60), strings.Repeat("d", 35),
			}, "."),
			want: "snapshot name is too long",
		},
		{name: "first DNS label length", pvcName: strings.Repeat("a", 30) + ".pvc", want: "snapshot name has an invalid DNS label"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			pvcJSON := coldSnapshotPVCJSON("Bound")
			var pvcList map[string]any
			require.NoError(t, json.Unmarshal([]byte(pvcJSON), &pvcList))
			item := pvcList["items"].([]any)[3].(map[string]any)
			metadata := item["metadata"].(map[string]any)
			spec := item["spec"].(map[string]any)
			metadata["name"] = tc.pvcName
			metadata["uid"] = "uid-" + tc.pvcName
			spec["volumeName"] = "pv-" + tc.pvcName
			pvcBytes, err := json.Marshal(pvcList)
			require.NoError(t, err)
			pvcJSON = string(pvcBytes)

			inventoryFile := filepath.Join(dir, "inventory.json")
			receiptFile := filepath.Join(dir, "receipt.json")
			witnessFile := filepath.Join(dir, "witness.jsonl")
			logFile := filepath.Join(dir, "kubectl.log")
			require.NoError(t, os.WriteFile(inventoryFile, coldSnapshotInventoryFromPVCJSON(t, pvcJSON), 0o600))
			require.NoError(t, os.WriteFile(witnessFile, coldSemanticWitness(t, "/registry"), 0o600))
			fakeKubectl := filepath.Join(dir, "kubectl")
			require.NoError(t, os.WriteFile(fakeKubectl, []byte(coldSnapshotFakeKubectl), 0o755))

			output, err := runColdSnapshotExecute(t, []string{
				"KUBECTL=" + fakeKubectl, "KUBE_CONTEXT=preproduction", "ALLOW_COLD_PHYSICAL_SNAPSHOT=true",
				"PREFLIGHT_FILE=" + inventoryFile, "RECEIPT_FILE=" + receiptFile,
				"OPERATION_ID=" + strings.Repeat("o", 40), "SEMANTIC_WITNESS_FILE=" + witnessFile,
				"EXPECTED_WITNESS_PREFIX=/registry", "FAKE_LOG=" + logFile, "FAKE_PVC_JSON=" + pvcJSON,
				"FAKE_TIDB_READY=true",
			})
			require.Error(t, err, string(output))
			require.Contains(t, string(output), tc.want)
			require.NoFileExists(t, receiptFile)
			if log, readErr := os.ReadFile(logFile); readErr == nil {
				require.NotContains(t, string(log), " patch ",
					"derived names must be validated before entering the maintenance window")
			}
		})
	}
}

func TestColdSnapshotExecuteRejectsExistingTargetBeforeMutation(t *testing.T) {
	dir := t.TempDir()
	inventoryFile := filepath.Join(dir, "inventory.json")
	receiptFile := filepath.Join(dir, "receipt.json")
	witnessFile := filepath.Join(dir, "witness.jsonl")
	logFile := filepath.Join(dir, "kubectl.log")
	require.NoError(t, os.WriteFile(inventoryFile, coldSnapshotInventory(t), 0o600))
	require.NoError(t, os.WriteFile(witnessFile, coldSemanticWitness(t, "/registry"), 0o600))
	fakeKubectl := filepath.Join(dir, "kubectl")
	require.NoError(t, os.WriteFile(fakeKubectl, []byte(coldSnapshotFakeKubectl), 0o755))

	output, err := runColdSnapshotExecute(t, []string{
		"KUBECTL=" + fakeKubectl, "KUBE_CONTEXT=preproduction", "ALLOW_COLD_PHYSICAL_SNAPSHOT=true",
		"PREFLIGHT_FILE=" + inventoryFile, "RECEIPT_FILE=" + receiptFile,
		"OPERATION_ID=op-existing", "SEMANTIC_WITNESS_FILE=" + witnessFile,
		"EXPECTED_WITNESS_PREFIX=/registry", "FAKE_LOG=" + logFile,
		"FAKE_PVC_JSON=" + coldSnapshotPVCJSON("Bound"),
		"FAKE_EXISTING_SNAPSHOT=op-existing-pd-kb-pd-0", "FAKE_TIDB_READY=true",
		"FAKE_FAIL_SNAPSHOT=false", "FAKE_CONTENT_DRIVER=csi.example.test",
	})
	require.Error(t, err, string(output))
	require.Contains(t, string(output), "target VolumeSnapshot already exists: op-existing-pd-kb-pd-0")
	require.NoFileExists(t, receiptFile)
	log := string(mustRead(t, logFile))
	require.NotContains(t, log, " patch ", "target collisions must fail before the maintenance window")
	require.NotContains(t, log, "create -f -")
}

func TestColdSnapshotExecuteRechecksTargetsAfterOperatorPause(t *testing.T) {
	dir := t.TempDir()
	inventoryFile := filepath.Join(dir, "inventory.json")
	receiptFile := filepath.Join(dir, "receipt.json")
	witnessFile := filepath.Join(dir, "witness.jsonl")
	logFile := filepath.Join(dir, "kubectl.log")
	require.NoError(t, os.WriteFile(inventoryFile, coldSnapshotInventory(t), 0o600))
	require.NoError(t, os.WriteFile(witnessFile, coldSemanticWitness(t, "/registry"), 0o600))
	fakeKubectl := filepath.Join(dir, "kubectl")
	require.NoError(t, os.WriteFile(fakeKubectl, []byte(coldSnapshotFakeKubectl), 0o755))

	output, err := runColdSnapshotExecute(t, []string{
		"KUBECTL=" + fakeKubectl, "KUBE_CONTEXT=preproduction", "ALLOW_COLD_PHYSICAL_SNAPSHOT=true",
		"PREFLIGHT_FILE=" + inventoryFile, "RECEIPT_FILE=" + receiptFile,
		"OPERATION_ID=op-late-target", "SEMANTIC_WITNESS_FILE=" + witnessFile,
		"EXPECTED_WITNESS_PREFIX=/registry", "FAKE_LOG=" + logFile,
		"FAKE_PVC_JSON=" + coldSnapshotPVCJSON("Bound"),
		"FAKE_EXISTING_SNAPSHOT_AFTER_PAUSE=op-late-target-pd-kb-pd-0", "FAKE_TIDB_READY=true",
		"FAKE_FAIL_SNAPSHOT=false", "FAKE_CONTENT_DRIVER=csi.example.test", "FENCE_SETTLE_SECONDS=0",
	})
	require.Error(t, err, string(output))
	require.Contains(t, string(output), "target VolumeSnapshot already exists: op-late-target-pd-kb-pd-0")
	require.NoFileExists(t, receiptFile)
	log := string(mustRead(t, logFile))
	require.Contains(t, log, "patch tidbcluster kb --type=json")
	require.NotContains(t, log, "patch statefulset", "late collisions must fail before service shutdown")
	require.NotContains(t, log, "create -f -")
}

func TestColdSnapshotExecuteRechecksSnapshotClassAfterOperatorPause(t *testing.T) {
	dir := t.TempDir()
	inventoryFile := filepath.Join(dir, "inventory.json")
	receiptFile := filepath.Join(dir, "receipt.json")
	witnessFile := filepath.Join(dir, "witness.jsonl")
	logFile := filepath.Join(dir, "kubectl.log")
	require.NoError(t, os.WriteFile(inventoryFile, coldSnapshotInventory(t), 0o600))
	require.NoError(t, os.WriteFile(witnessFile, coldSemanticWitness(t, "/registry"), 0o600))
	fakeKubectl := filepath.Join(dir, "kubectl")
	require.NoError(t, os.WriteFile(fakeKubectl, []byte(coldSnapshotFakeKubectl), 0o755))

	output, err := runColdSnapshotExecute(t, []string{
		"KUBECTL=" + fakeKubectl, "KUBE_CONTEXT=preproduction", "ALLOW_COLD_PHYSICAL_SNAPSHOT=true",
		"PREFLIGHT_FILE=" + inventoryFile, "RECEIPT_FILE=" + receiptFile,
		"OPERATION_ID=op-class-drift", "SEMANTIC_WITNESS_FILE=" + witnessFile,
		"EXPECTED_WITNESS_PREFIX=/registry", "FAKE_LOG=" + logFile,
		"FAKE_PVC_JSON=" + coldSnapshotPVCJSON("Bound"), "FAKE_TIDB_READY=true",
		"FAKE_SNAPSHOT_POLICY_AFTER_PAUSE=Delete", "FAKE_FAIL_SNAPSHOT=false",
		"FAKE_CONTENT_DRIVER=csi.example.test", "FENCE_SETTLE_SECONDS=0",
	})
	require.Error(t, err, string(output))
	require.Contains(t, string(output), "VolumeSnapshotClass retained changed at the maintenance fence")
	require.NoFileExists(t, receiptFile)
	log := string(mustRead(t, logFile))
	require.Contains(t, log, "patch tidbcluster kb --type=json")
	require.NotContains(t, log, "patch statefulset", "snapshot class drift must fail before service shutdown")
	require.NotContains(t, log, "create -f -")
}

func TestColdSnapshotExecuteRejectsTidbClusterFenceLossAfterPause(t *testing.T) {
	for _, tc := range []struct {
		name, uidDrift, pauseLost, specDrift, clusterIDDrift, readyLost, want string
	}{
		{name: "controller replaced", uidDrift: "true", want: "TidbCluster UID changed at the maintenance fence"},
		{name: "pause lost", pauseLost: "true", want: "TidbCluster pause was lost at the maintenance fence"},
		{name: "spec drift", specDrift: "true", want: "TidbCluster spec changed at the maintenance fence"},
		{name: "cluster ID drift", clusterIDDrift: "true", want: "TiKV cluster ID changed at the maintenance fence"},
		{name: "ready lost", readyLost: "true", want: "TidbCluster is not Ready at the maintenance fence"},
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

			output, err := runColdSnapshotExecute(t, []string{
				"KUBECTL=" + fakeKubectl,
				"KUBE_CONTEXT=preproduction",
				"ALLOW_COLD_PHYSICAL_SNAPSHOT=true",
				"PREFLIGHT_FILE=" + inventoryFile,
				"RECEIPT_FILE=" + receiptFile,
				"OPERATION_ID=op-tidb-fence-lost",
				"SEMANTIC_WITNESS_FILE=" + witnessFile,
				"EXPECTED_WITNESS_PREFIX=/registry",
				"FAKE_LOG=" + logFile,
				"FAKE_PVC_JSON=" + coldSnapshotPVCJSON("Bound"),
				"FAKE_TIDB_READY=true",
				"FAKE_TIDB_UID_DRIFT_AFTER_PAUSE=" + tc.uidDrift,
				"FAKE_TIDB_PAUSE_LOST_AFTER_PAUSE=" + tc.pauseLost,
				"FAKE_TIDB_SPEC_DRIFT_AFTER_PAUSE=" + tc.specDrift,
				"FAKE_TIDB_CLUSTER_ID_DRIFT_AFTER_PAUSE=" + tc.clusterIDDrift,
				"FAKE_TIDB_READY_LOST_AFTER_PAUSE=" + tc.readyLost,
			})
			require.Error(t, err, string(output))
			require.Contains(t, string(output), tc.want)
			require.NoFileExists(t, receiptFile)
			log := string(mustRead(t, logFile))
			require.NotContains(t, log, "patch statefulset")
			require.NotContains(t, log, "create -f -")
		})
	}
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
		"ALLOW_COLD_PHYSICAL_SNAPSHOT=true",
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

func TestColdSnapshotExecuteRequiresExplicitApprovalBeforeMutation(t *testing.T) {
	dir := t.TempDir()
	inventoryFile := filepath.Join(dir, "inventory.json")
	receiptFile := filepath.Join(dir, "receipt.json")
	witnessFile := filepath.Join(dir, "witness.jsonl")
	logFile := filepath.Join(dir, "kubectl.log")
	require.NoError(t, os.WriteFile(inventoryFile, coldSnapshotInventory(t), 0o600))
	require.NoError(t, os.WriteFile(witnessFile, coldSemanticWitness(t, "/registry"), 0o600))

	env := []string{
		"KUBECTL=/does/not/exist",
		"KUBE_CONTEXT=preproduction",
		"PREFLIGHT_FILE=" + inventoryFile,
		"RECEIPT_FILE=" + receiptFile,
		"OPERATION_ID=op-missing-approval",
		"SEMANTIC_WITNESS_FILE=" + witnessFile,
		"EXPECTED_WITNESS_PREFIX=/registry",
		"FAKE_LOG=" + logFile,
	}
	output, err := runColdSnapshotExecute(t, env)
	require.Error(t, err, string(output))
	require.Contains(t, string(output), "set ALLOW_COLD_PHYSICAL_SNAPSHOT=true")
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
	return coldSnapshotInventoryFromPVCJSON(t, coldSnapshotPVCJSON("Bound"))
}

func coldSnapshotInventoryFromPVCJSON(t *testing.T, pvcJSON string) []byte {
	t.Helper()
	var pvc map[string]any
	require.NoError(t, json.Unmarshal([]byte(pvcJSON), &pvc))
	rawItems := pvc["items"].([]any)
	items := make([]any, 0, len(rawItems))
	for _, raw := range rawItems {
		item := raw.(map[string]any)
		metadata := item["metadata"].(map[string]any)
		spec := item["spec"].(map[string]any)
		status := item["status"].(map[string]any)
		items = append(items, map[string]any{
			"name": metadata["name"], "uid": metadata["uid"], "pv": spec["volumeName"],
			"pv_uid": "uid-" + spec["volumeName"].(string), "csi_driver": "csi.example.test",
			"volume_handle": "handle-" + spec["volumeName"].(string),
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
  policy=Retain
  if [[ -n "${FAKE_SNAPSHOT_POLICY_AFTER_PAUSE:-}" ]] && grep -q 'patch tidbcluster kb' "$FAKE_LOG"; then
    policy="$FAKE_SNAPSHOT_POLICY_AFTER_PAUSE"
  fi
  printf 'csi.example.test\t%s' "$policy"
elif [[ "$args" == *"get storageclass"* ]]; then
  printf 'csi.example.test'
elif [[ "$args" == *"get tidbcluster"* ]]; then
  ready=True
  [[ "${FAKE_TIDB_READY:-true}" == true ]] || ready=False
  uid=uid-tidb
  version=v8.5.3
  cluster_id=7662961163671170154
  paused_field=
  if grep -q 'patch tidbcluster kb' "$FAKE_LOG"; then
    paused_field='"paused":true,'
    [[ "${FAKE_TIDB_SPEC_DRIFT_AFTER_PAUSE:-false}" != true ]] || version=v8.5.4
    [[ "${FAKE_TIDB_CLUSTER_ID_DRIFT_AFTER_PAUSE:-false}" != true ]] || cluster_id=7662961163671170999
    [[ "${FAKE_TIDB_READY_LOST_AFTER_PAUSE:-false}" != true ]] || ready=False
  fi
  if [[ "${FAKE_TIDB_PAUSE_LOST_AFTER_PAUSE:-false}" == true ]] && [[ -n "$paused_field" ]]; then
    paused_field=
  fi
  if [[ "${FAKE_TIDB_UID_DRIFT_AFTER_PAUSE:-false}" == true ]] && grep -q 'patch tidbcluster kb' "$FAKE_LOG"; then
    uid=replacement-uid-tidb
  fi
  printf '{"apiVersion":"pingcap.com/v1alpha1","kind":"TidbCluster","metadata":{"name":"kb","namespace":"tidb-cluster","uid":"%s","resourceVersion":"10"},"spec":{%s"version":"%s","pd":{"replicas":3},"tikv":{"replicas":3}},"status":{"clusterID":"%s","conditions":[{"type":"Ready","status":"%s"}]}}' "$uid" "$paused_field" "$version" "$cluster_id" "$ready"
elif [[ "$args" == *"get statefulset kubebrain"* && "$args" == *"jsonpath"* ]]; then
  printf 'uid-kubebrain'
elif [[ "$args" == *"get statefulset"* ]]; then
  name="$(sed -n 's/.*get statefulset \([^ ]*\).*/\1/p' <<<"$args")"
  uid="uid-${name}"
  [[ "$name" != kubebrain ]] || uid=uid-kubebrain
  if [[ "${FAKE_UID_DRIFT_AFTER_PAUSE:-}" == "$name" ]] && grep -q 'patch tidbcluster kb' "$FAKE_LOG"; then
    uid="replacement-${uid}"
  fi
  ready=3
  degraded="${FAKE_DEGRADED_STATEFULSET:-}"
  if [[ -n "${FAKE_DEGRADE_AFTER_PAUSE:-}" ]] && grep -q 'patch tidbcluster kb' "$FAKE_LOG"; then
    degraded="$FAKE_DEGRADE_AFTER_PAUSE"
  fi
  [[ "$degraded" != "$name" ]] || ready=2
  printf '{"metadata":{"uid":"%s","resourceVersion":"20","generation":1},"spec":{"replicas":3},"status":{"observedGeneration":1,"replicas":3,"readyReplicas":%s,"currentReplicas":3,"updatedReplicas":3,"currentRevision":"rev-a","updateRevision":"rev-a"}}' "$uid" "$ready"
elif [[ "$args" == *"get pvc -l"* ]]; then
  printf '%s' "$FAKE_PVC_JSON"
elif [[ "$args" == *"get pvc"* && "$args" == *"jsonpath"* ]]; then
  name="$(sed -n 's/.*get pvc \([^ ]*\).*/\1/p' <<<"$args")"
  if [[ "${FAKE_PVC_UID_DRIFT:-}" == "$name" ]]; then
    printf 'replacement-uid-%s' "$name"
  else
    printf 'uid-%s' "$name"
  fi
elif [[ "$args" == *"get pv "* ]]; then
  name="$(sed -n 's/.*get pv \([^ ]*\).*/\1/p' <<<"$args")"
  pvc="${name#pv-}"
  uid="uid-${name}"
  handle="handle-${name}"
  if grep -q 'patch statefulset kb-pd' "$FAKE_LOG"; then
    [[ "${FAKE_PV_UID_DRIFT:-}" != "$name" ]] || uid="replacement-${uid}"
    [[ "${FAKE_PV_HANDLE_DRIFT:-}" != "$name" ]] || handle="replacement-${handle}"
  fi
  printf '{"metadata":{"name":"%s","uid":"%s"},"spec":{"storageClassName":"fast","volumeMode":"Filesystem","claimRef":{"apiVersion":"v1","kind":"PersistentVolumeClaim","namespace":"tidb-cluster","name":"%s","uid":"uid-%s"},"csi":{"driver":"csi.example.test","volumeHandle":"%s"}},"status":{"phase":"Bound"}}' "$name" "$uid" "$pvc" "$pvc" "$handle"
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
  pvc="${snapshot#${OPERATION_ID}-}"
  source_handle="handle-pv-${pvc}"
  snapshot_handle="handle-${name}"
  [[ "${FAKE_CONTENT_SOURCE_DRIFT:-false}" != true ]] || source_handle="wrong-${source_handle}"
  [[ "${FAKE_DUPLICATE_SNAPSHOT_HANDLE:-false}" != true ]] || snapshot_handle="duplicate-snapshot-handle"
  if [[ "${FAKE_POST_RESTORE_CONTENT_DRIFT:-false}" == true ]] &&
    [[ "$(grep -c 'get volumesnapshotcontent' "$FAKE_LOG")" -gt 6 ]]; then
    snapshot_handle="drifted-${snapshot_handle}"
  fi
  printf '{"metadata":{"name":"%s","uid":"uid-%s"},"spec":{"deletionPolicy":"Retain","driver":"%s","volumeSnapshotClassName":"retained","volumeSnapshotRef":{"uid":"uid-%s"},"source":{"volumeHandle":"%s"}},"status":{"snapshotHandle":"%s"}}' "$name" "$name" "$FAKE_CONTENT_DRIVER" "$snapshot" "$source_handle" "$snapshot_handle"
elif [[ "$args" == *"get volumesnapshot"* && "$args" == *"--ignore-not-found"* ]]; then
  name="$(sed -n 's/.*get volumesnapshot \([^ ]*\).*/\1/p' <<<"$args")"
  if [[ "${FAKE_EXISTING_SNAPSHOT:-}" == "$name" ]] ||
    { [[ "${FAKE_EXISTING_SNAPSHOT_AFTER_PAUSE:-}" == "$name" ]] && grep -q 'patch tidbcluster kb' "$FAKE_LOG"; }; then
    printf 'volumesnapshot.snapshot.storage.k8s.io/%s' "$name"
  fi
elif [[ "$args" == *"get volumesnapshot"* ]]; then
  name="$(sed -n 's/.*get volumesnapshot \([^ ]*\).*/\1/p' <<<"$args")"
  pvc="${name#${OPERATION_ID}-}"
  printf '{"metadata":{"name":"%s","uid":"uid-%s"},"spec":{"volumeSnapshotClassName":"retained","source":{"persistentVolumeClaimName":"%s"}},"status":{"readyToUse":true,"boundVolumeSnapshotContentName":"content-%s","restoreSize":"1Gi"}}' "$name" "$name" "$pvc" "$name"
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
