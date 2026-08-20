package production_test

import (
	"crypto/sha256"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRequestLegacySnapshotRemediationCreatesOnlyUnapprovedPendingOperation(t *testing.T) {
	dir := t.TempDir()
	diagnose := writeLegacyExecutable(t, dir, "diagnose", `#!/usr/bin/env bash
printf 'cluster_id=18446744073709551615\nrevision=41\nendpoint=%s\nsnapshot_status=legacy_lease_history_ambiguous\nminimum_compact_revision=3\n' "$ENDPOINT"
exit 3
`)
	kubectl := writeLegacyExecutable(t, dir, "kubectl", `#!/usr/bin/env bash
printf '%s\n' "$*" >>"$TEST_LOG"
if [[ "$*" == *" get secret "* ]]; then exit 1; fi
if [[ "$*" == *"--dry-run=client"* ]]; then printf '{"apiVersion":"v1","kind":"Secret","metadata":{},"type":"Opaque","data":{}}\n'; else cat >/dev/null; fi
`)
	operationctl := writeLegacyExecutable(t, dir, "operationctl", `#!/usr/bin/env bash
printf '%s\n' "$*" >>"$OPERATION_LOG"
`)
	out, err := runProductionScriptCommand(t, "request-legacy-snapshot-remediation.sh", []string{
		"REQUEST_ID=change-4312", "ENDPOINT=https://kubebrain:2379", "KUBE_CONTEXT=in-cluster",
		"DIAGNOSE_COMMAND=" + diagnose, "KUBECTL=" + kubectl, "OPERATIONCTL=" + operationctl,
		"TEST_LOG=" + filepath.Join(dir, "kubectl.log"), "OPERATION_LOG=" + filepath.Join(dir, "operation.log"),
	})
	require.NoError(t, err, string(out))
	require.Contains(t, string(out), "unapproved Pending LegacySnapshotHistoryRemediation")
	operation := string(requireFile(t, filepath.Join(dir, "operation.log")))
	require.Contains(t, operation, "--type LegacySnapshotHistoryRemediation")
	require.Contains(t, operation, "--max-attempts 1")
	require.NotContains(t, operation, "--action approve")
}

func TestRequestLegacySnapshotRemediationRejectsHealthySnapshotBeforeKubernetes(t *testing.T) {
	dir := t.TempDir()
	diagnose := writeLegacyExecutable(t, dir, "diagnose", "#!/usr/bin/env bash\nprintf 'snapshot_status=healthy\\n'\n")
	kubectl := writeLegacyExecutable(t, dir, "kubectl", "#!/usr/bin/env bash\ntouch \"$MUTATION_MARKER\"\n")
	operationctl := writeLegacyExecutable(t, dir, "operationctl", "#!/usr/bin/env bash\ntouch \"$MUTATION_MARKER\"\n")
	marker := filepath.Join(dir, "mutation")
	out, err := runProductionScriptCommand(t, "request-legacy-snapshot-remediation.sh", []string{
		"REQUEST_ID=change-healthy", "ENDPOINT=https://kubebrain:2379", "KUBE_CONTEXT=in-cluster",
		"DIAGNOSE_COMMAND=" + diagnose, "KUBECTL=" + kubectl, "OPERATIONCTL=" + operationctl, "MUTATION_MARKER=" + marker,
	})
	require.Error(t, err, string(out))
	require.Contains(t, string(out), "did not prove ambiguous legacy lease history")
	require.NoFileExists(t, marker)
}

func TestRequestLegacySnapshotRemediationBoundsDiagnosisOutput(t *testing.T) {
	for _, tc := range []struct {
		name string
		size int
		ok   bool
	}{{"at limit", 1048576, true}, {"over limit", 1048577, false}} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			base := "cluster_id=7671\nrevision=41\nminimum_compact_revision=3\nsnapshot_status=legacy_lease_history_ambiguous\n"
			diagnosis := filepath.Join(dir, "diagnosis.input")
			require.NoError(t, os.WriteFile(diagnosis, []byte(base+strings.Repeat(" ", tc.size-len(base))), 0o600))
			diagnose := writeLegacyExecutable(t, dir, "diagnose", "#!/usr/bin/env bash\ncat \"$DIAGNOSIS_INPUT\"\nexit 3\n")
			controlLog := filepath.Join(dir, "control.log")
			kubectl := writeLegacyExecutable(t, dir, "kubectl", `#!/usr/bin/env bash
printf 'kubectl %s\n' "$*" >>"$CONTROL_LOG"
if [[ "$*" == *" get secret "* ]]; then exit 1; fi
if [[ "$*" == *"--dry-run=client"* ]]; then printf '{"kind":"Secret"}\n'; else cat >/dev/null; fi
`)
			operationctl := writeLegacyExecutable(t, dir, "operationctl", "#!/usr/bin/env bash\nprintf 'operationctl %s\\n' \"$*\" >>\"$CONTROL_LOG\"\n")
			output, err := runProductionScriptCommand(t, "request-legacy-snapshot-remediation.sh", []string{
				"REQUEST_ID=change-diagnosis-budget", "ENDPOINT=https://kubebrain:2379", "KUBE_CONTEXT=in-cluster",
				"DIAGNOSE_COMMAND=" + diagnose, "DIAGNOSIS_INPUT=" + diagnosis, "KUBECTL=" + kubectl,
				"OPERATIONCTL=" + operationctl, "CONTROL_LOG=" + controlLog,
			})
			if tc.ok {
				require.NoError(t, err, string(output))
				require.Contains(t, string(requireFile(t, controlLog)), "operationctl")
				return
			}
			require.Error(t, err)
			require.Contains(t, string(output), "diagnosis output exceeds 1048576 bytes")
			require.NoFileExists(t, controlLog)
		})
	}
}

func TestRequestLegacySnapshotRemediationBoundsDiagnosisErrorOutput(t *testing.T) {
	dir := t.TempDir()
	diagnose := writeLegacyExecutable(t, dir, "diagnose", "#!/usr/bin/env bash\nhead -c 1048577 /dev/zero | tr '\\0' x >&2\nexit 2\n")
	marker := filepath.Join(dir, "mutation")
	control := writeLegacyExecutable(t, dir, "control", "#!/usr/bin/env bash\ntouch \"$MUTATION_MARKER\"\n")
	output, err := runProductionScriptCommand(t, "request-legacy-snapshot-remediation.sh", []string{
		"REQUEST_ID=change-diagnosis-error-budget", "ENDPOINT=https://kubebrain:2379", "KUBE_CONTEXT=in-cluster",
		"DIAGNOSE_COMMAND=" + diagnose, "KUBECTL=" + control, "OPERATIONCTL=" + control, "MUTATION_MARKER=" + marker,
	})
	require.Error(t, err)
	require.Contains(t, string(output), "diagnosis output exceeds 1048576 bytes")
	require.Less(t, len(output), 4096, "oversized diagnosis stderr must not be echoed")
	require.NoFileExists(t, marker)
}

func TestRunLegacySnapshotRemediationOperationBindsAndPublishesReceipt(t *testing.T) {
	dir := t.TempDir()
	params := []byte(`{"cluster_id":"7301","compact_revision":"3","endpoint":"https://kubebrain:2379","request_id":"change-4312","revision":"41"}` + "\n")
	paramsPath := filepath.Join(dir, "parameters.json")
	require.NoError(t, os.WriteFile(paramsPath, params, 0o600))
	digest := fmt.Sprintf("%x", sha256.Sum256(params))
	identityDigest := sha256.Sum256([]byte("change-4312\nhttps://kubebrain:2379\n7301\n41\n3\n"))
	name := "legacy-snapshot-remediation-" + fmt.Sprintf("%x", identityDigest)[:20]
	operationLog := filepath.Join(dir, "operation.log")
	operationctl := writeLegacyExecutable(t, dir, "operationctl", `#!/usr/bin/env bash
printf '%s\n' "$*" >>"$OPERATION_LOG"
case "$*" in
  *"--action claim"*) printf '{"namespace":"kubebrain-operations","name":"%s","operation_id":"%s","instance":"kubebrain","type":"LegacySnapshotHistoryRemediation","requested_by":"platform:legacy-snapshot-remediation","owner":"worker-1","parameters_secret":"%s-parameters","parameters_key":"parameters.json","attempt":1,"parameters_sha256":"%s"}\n' "$OPERATION_NAME" "$OPERATION_NAME" "$OPERATION_NAME" "$EXPECTED_SHA" ;;
esac
`)
	remediation := writeLegacyExecutable(t, dir, "remediation", `#!/usr/bin/env bash
[[ "$ACTION" == compact && "$ENDPOINT" == "$CONFIRM_ENDPOINT" && "$EXPECTED_CLUSTER_ID" == 7301 && "$EXPECTED_REVISION" == 41 && "$ALLOW_IRREVERSIBLE_LEGACY_HISTORY_COMPACTION" == true ]]
printf 'validated snapshot\n' >"$OUTPUT"
printf 'compacted_revision=3\n'
`)
	_ = writeLegacyExecutable(t, dir, "sleep", "#!/usr/bin/env bash\nexit 0")
	out, err := runProductionScriptCommand(t, "run-legacy-snapshot-remediation-operation.sh", []string{
		"WORKER_ID=worker-1", "OPERATIONCTL=" + operationctl, "REMEDIATION_COMMAND=" + remediation,
		"PARAMETERS_INPUT=" + paramsPath, "WORK_DIR=" + dir, "OPERATION_LOG=" + operationLog,
		"OPERATION_NAME=" + name, "EXPECTED_SHA=" + digest, "HEARTBEAT_INTERVAL_SECONDS=60", "PATH=" + dir + ":" + os.Getenv("PATH"),
	})
	require.NoError(t, err, string(out))
	operations := string(requireFile(t, operationLog))
	require.Contains(t, operations, "--action claim --owner worker-1 --type LegacySnapshotHistoryRemediation")
	require.Contains(t, operations, "--action succeed --name "+name)
	require.NotContains(t, operations, "--action fail")
	receipt := string(requireFile(t, filepath.Join(dir, name+".receipt.json")))
	require.Contains(t, receipt, `"format":"kubebrain.legacy-snapshot-remediation.v1"`)
	require.Contains(t, receipt, `"compacted_revision":"3"`)
	require.FileExists(t, filepath.Join(dir, name+".snapshot.db"))

	oversized := append(append([]byte{}, params...), []byte(strings.Repeat(" ", 65536))...)
	require.NoError(t, os.WriteFile(paramsPath, oversized, 0o600))
	oversizedDigest := fmt.Sprintf("%x", sha256.Sum256(oversized))
	require.NoError(t, os.WriteFile(operationLog, nil, 0o600))
	oversizedOut, oversizedErr := runProductionScriptCommand(t, "run-legacy-snapshot-remediation-operation.sh", []string{
		"WORKER_ID=worker-1", "OPERATIONCTL=" + operationctl, "REMEDIATION_COMMAND=" + remediation,
		"PARAMETERS_INPUT=" + paramsPath, "WORK_DIR=" + dir, "OPERATION_LOG=" + operationLog,
		"OPERATION_NAME=" + name, "EXPECTED_SHA=" + oversizedDigest, "HEARTBEAT_INTERVAL_SECONDS=60", "PATH=" + dir + ":" + os.Getenv("PATH"),
	})
	require.Error(t, oversizedErr)
	require.Contains(t, string(oversizedOut), "operation parameters exceed 65536 bytes")
	oversizedOperations := string(requireFile(t, operationLog))
	require.Contains(t, oversizedOperations, "--action fail")
	require.NotContains(t, oversizedOperations, "--action succeed")
}

func TestRunLegacySnapshotRemediationOperationFailureIsTerminal(t *testing.T) {
	data, err := os.ReadFile("run-legacy-snapshot-remediation-operation.sh")
	require.NoError(t, err)
	script := string(data)
	require.Contains(t, script, "compaction may already be committed, inspect before any new operation")
	require.Contains(t, script, "--action fail")
	require.NotContains(t, script, "--action requeue")
}

func writeLegacyExecutable(t *testing.T, dir, name, contents string) string {
	t.Helper()
	path := filepath.Join(dir, name)
	require.NoError(t, os.WriteFile(path, []byte(strings.TrimSpace(contents)+"\n"), 0o755))
	return path
}
