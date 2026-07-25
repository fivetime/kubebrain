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

const runnerCutoverArtifactSHA256 = "2222222222222222222222222222222222222222222222222222222222222222"

func TestRestoreCutoverOperationCompletesAllPhases(t *testing.T) {
	f := newCutoverRunnerFixture(t)
	f.run(t, true, "")
	log := f.log(t)
	requireOrdered(t, log, "phase prepare", "phase cutover", "phase verify", "phase complete")
	require.NotContains(t, log, "phase rollback")
	require.Contains(t, log, "--action succeed")
	require.Contains(t, log, "--namespace tenant-a-operations --action succeed")
	require.NotContains(t, log, "--namespace ops --namespace tenant-a-operations")
}

func TestRestoreCutoverOperationPassesFrozenEvidenceToPhases(t *testing.T) {
	f := newCutoverRunnerFixture(t)
	f.run(t, true, "ASSERT_FROZEN_INPUTS=true")
	log := f.log(t)
	require.NotContains(t, log, f.restoreReceipt)
	require.NotContains(t, log, f.backup)
}

func TestRestoreCutoverOperationRejectsInvalidReceipt(t *testing.T) {
	f := newCutoverRunnerFixture(t)
	f.run(t, false, "INVALID_CUTOVER_RECEIPT=true", "invalid receipt")
	log := f.log(t)
	require.Contains(t, log, "--action fail")
	require.NotContains(t, log, "--action succeed")
}

func TestRestoreCutoverOperationRejectsReceiptTamperedDuringDigest(t *testing.T) {
	f := newCutoverRunnerFixture(t)
	f.run(t, false, "TAMPER_RECEIPT_DURING_SHA256=true", "invalid receipt")
	log := f.log(t)
	require.Contains(t, log, "--action fail")
	require.NotContains(t, log, "--action succeed")
}

func TestRestoreCutoverOperationRejectsValidReceiptChangedAfterDigest(t *testing.T) {
	f := newCutoverRunnerFixture(t)
	tamperedReceipt := filepath.Join(f.dir, "valid-tampered-receipt.json")
	receipt := strings.Replace(cutoverRunnerReceipt(), `"completed_at_unix":100`, `"completed_at_unix":101`, 1)
	require.NoError(t, os.WriteFile(tamperedReceipt, []byte(receipt), 0o600))
	f.env = withReceiptAfterSHA256Tamper(f.env, tamperedReceipt)

	f.run(t, false, "", "invalid receipt")
	log := f.log(t)
	require.Contains(t, log, "--action fail")
	require.NotContains(t, log, "--action succeed")
}

func TestRestoreCutoverOperationRejectsNonCanonicalStateBeforeSucceed(t *testing.T) {
	f := newCutoverRunnerFixture(t)
	f.run(t, false, "TAMPER_STATE_BEFORE_RECEIPT=true", "invalid receipt")
	log := f.log(t)
	require.Contains(t, log, "--action fail")
	require.NotContains(t, log, "--action succeed")
}

func TestRestoreCutoverOperationRejectsStateWithInvalidPrefixes(t *testing.T) {
	f := newCutoverRunnerFixture(t)
	f.run(t, false, "TAMPER_STATE_PREFIX_BEFORE_RECEIPT=true", "invalid receipt")
	log := f.log(t)
	require.Contains(t, log, "--action fail")
	require.NotContains(t, log, "--action succeed")
}

func TestRestoreCutoverOperationRejectsNonCanonicalMarkersBeforeSucceed(t *testing.T) {
	for _, tc := range []struct {
		name, env string
	}{
		{name: "cutover marker", env: "TAMPER_CUTOVER_MARKER_BEFORE_RECEIPT=true"},
		{name: "verified marker", env: "TAMPER_VERIFIED_MARKER_BEFORE_RECEIPT=true"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newCutoverRunnerFixture(t)
			f.run(t, false, tc.env, "invalid receipt")
			log := f.log(t)
			require.Contains(t, log, "--action fail")
			require.NotContains(t, log, "--action succeed")
		})
	}
}

func TestRestoreCutoverOperationRequeuesPrepareFailure(t *testing.T) {
	f := newCutoverRunnerFixture(t)
	f.run(t, false, "FAIL_PHASE=prepare", "prepare failed and was requeued")
	log := f.log(t)
	require.Contains(t, log, "--action retry")
	require.NotContains(t, log, "phase rollback")
	require.NotContains(t, log, "--action fail")
}

func TestRestoreCutoverOperationRollsBackPostCutoverFailure(t *testing.T) {
	for _, phase := range []string{"cutover", "verify", "complete"} {
		t.Run(phase, func(t *testing.T) {
			f := newCutoverRunnerFixture(t)
			f.run(t, false, "FAIL_PHASE="+phase, "rollback succeeded")
			log := f.log(t)
			require.Contains(t, log, "phase rollback")
			require.Contains(t, log, "--action fail")
			require.NotContains(t, log, "--action retry")
		})
	}
}

func TestRestoreCutoverOperationRecordsRollbackFailure(t *testing.T) {
	f := newCutoverRunnerFixture(t)
	f.run(t, false, "FAIL_PHASE=verify,rollback", "rollback failed")
	require.Contains(t, f.log(t), "rollback failed(8)")
}

func TestRestoreCutoverOperationRejectsParameterDrift(t *testing.T) {
	f := newCutoverRunnerFixture(t)
	f.run(t, false, "CLAIM_DIGEST="+strings.Repeat("f", 64), "parameters digest")
	require.Contains(t, f.log(t), "--action retry")
}

func TestRestoreCutoverOperationRejectsParametersTamperedDuringDigest(t *testing.T) {
	f := newCutoverRunnerFixture(t)
	f.run(t, false, "TAMPER_PARAMETERS_DURING_SHA256=true", "parameters digest")
	log := f.log(t)
	require.Contains(t, log, "--action retry")
	require.NotContains(t, log, "phase prepare")
	require.NotContains(t, log, "--action succeed")
}

func TestRestoreCutoverOperationRejectsEvidenceDrift(t *testing.T) {
	for _, tc := range []struct {
		name    string
		path    func(*cutoverRunnerFixture) string
		message string
	}{
		{
			name: "restore receipt",
			path: func(f *cutoverRunnerFixture) string {
				return f.restoreReceipt
			},
			message: "restore receipt bytes do not match",
		},
		{
			name: "backup input",
			path: func(f *cutoverRunnerFixture) string {
				return f.backup
			},
			message: "backup input bytes do not match",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newCutoverRunnerFixture(t)
			require.NoError(t, os.WriteFile(tc.path(f), []byte("changed\n"), 0o600))
			f.run(t, false, "", tc.message)
			log := f.log(t)
			require.Contains(t, log, "--action retry")
			require.NotContains(t, log, "phase prepare")
			require.NotContains(t, log, "--action succeed")
		})
	}
}

func TestRestoreCutoverOperationRejectsEvidenceTamperedDuringCapture(t *testing.T) {
	for _, tc := range []struct {
		name    string
		env     string
		message string
	}{
		{
			name:    "restore receipt",
			env:     "TAMPER_EVIDENCE_DURING_SHA256=true",
			message: "restore receipt bytes changed",
		},
		{
			name:    "backup input",
			env:     "TAMPER_BACKUP_DURING_SHA256=true",
			message: "backup input bytes changed",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newCutoverRunnerFixture(t)
			f.run(t, false, tc.env, tc.message)
			log := f.log(t)
			require.Contains(t, log, "--action retry")
			require.NotContains(t, log, "phase prepare")
			require.NotContains(t, log, "--action succeed")
		})
	}
}

func TestRestoreCutoverOperationRejectsEmptyRequiredParameters(t *testing.T) {
	f := newCutoverRunnerFixture(t)
	parameters := strings.ReplaceAll(
		string(mustRead(t, f.parameters)),
		`"public_endpoint":"https://service:2379"`,
		`"public_endpoint":""`,
	)
	require.NoError(t, os.WriteFile(f.parameters, []byte(parameters), 0o600))

	f.run(t, false, "CLAIM_DIGEST="+fileDigest(t, f.parameters), "empty required field")
	log := f.log(t)
	require.NotContains(t, log, "phase ")
	require.NotContains(t, log, "--action retry")
	require.NotContains(t, log, "--action succeed")
}

func TestRestoreCutoverOperationRejectsInvalidIdentityParameters(t *testing.T) {
	for _, tc := range []struct {
		name string
		edit func(string) string
		want string
	}{
		{
			name: "invalid service namespace",
			edit: func(parameters string) string {
				return strings.Replace(parameters, `"service_namespace":"ns-a"`, `"service_namespace":"Bad_Namespace"`, 1)
			},
			want: "service_namespace must be a lowercase DNS label",
		},
		{
			name: "same source and target",
			edit: func(parameters string) string {
				return strings.Replace(parameters, `"target_instance":"target"`, `"target_instance":"source"`, 1)
			},
			want: "source_instance and target_instance must differ",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newCutoverRunnerFixture(t)
			parameters := tc.edit(string(mustRead(t, f.parameters)))
			require.NoError(t, os.WriteFile(f.parameters, []byte(parameters), 0o600))

			f.run(t, false, "CLAIM_DIGEST="+fileDigest(t, f.parameters), tc.want)
			log := f.log(t)
			require.NotContains(t, log, "phase ")
			require.NotContains(t, log, "--action retry")
			require.NotContains(t, log, "--action fail")
			require.NotContains(t, log, "--action succeed")
		})
	}
}

func TestRestoreCutoverOperationRejectsInvalidClaimIdentityBeforePhases(t *testing.T) {
	for _, tc := range []struct {
		name string
		env  string
	}{
		{name: "operation id", env: "CLAIM_OPERATION_ID=cutover/1"},
		{name: "instance", env: "CLAIM_INSTANCE=instance/a"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newCutoverRunnerFixture(t)
			f.run(t, false, tc.env, "cutover claim identity contains unsupported characters")
			log := f.log(t)
			require.NotContains(t, log, "phase ")
			require.NotContains(t, log, "--action retry")
			require.NotContains(t, log, "--action fail")
			require.NotContains(t, log, "--action succeed")
		})
	}
}

func TestRestoreCutoverOperationStopsWhenHeartbeatIsFenced(t *testing.T) {
	f := newCutoverRunnerFixture(t)
	f.run(t, false, "SLEEP_PHASE=prepare", "heartbeat failed")
	log := f.log(t)
	require.Contains(t, log, "--action heartbeat")
	require.NotContains(t, log, "--action retry")
	require.NotContains(t, log, "--action fail")
}

func TestRestoreCutoverOperationResumesFromDurableEvidence(t *testing.T) {
	for _, tc := range []struct {
		name, evidence string
		wanted         []string
		unwanted       []string
	}{
		{
			name: "prepared state", evidence: "state",
			wanted:   []string{"phase cutover", "phase verify", "phase complete"},
			unwanted: []string{"phase prepare"},
		},
		{
			name: "cutover marker", evidence: "cutover",
			wanted:   []string{"phase verify", "phase complete"},
			unwanted: []string{"phase prepare", "phase cutover"},
		},
		{
			name: "completion receipt", evidence: "receipt",
			wanted:   []string{"phase complete", "--action succeed"},
			unwanted: []string{"phase prepare", "phase cutover", "phase verify"},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newCutoverRunnerFixture(t)
			f.publishEvidence(t, tc.evidence)
			f.run(t, true, "")
			log := f.log(t)
			for _, wanted := range tc.wanted {
				require.Contains(t, log, wanted)
			}
			for _, unwanted := range tc.unwanted {
				require.NotContains(t, log, unwanted)
			}
		})
	}
}

func TestRestoreCutoverOperationTurnsExistingRollbackIntoTerminalFailure(t *testing.T) {
	f := newCutoverRunnerFixture(t)
	f.publishEvidence(t, "rollback")
	f.run(t, false, "", "already rolled back")
	log := f.log(t)
	require.Contains(t, log, "--action fail")
	require.NotContains(t, log, "phase prepare")
}

func TestRestoreCutoverOperationRejectsInvalidRollbackEvidence(t *testing.T) {
	f := newCutoverRunnerFixture(t)
	f.publishEvidence(t, "rollback")
	require.NoError(t, os.WriteFile(
		filepath.Join(f.dir, "state", "cutover-1.rollback"),
		[]byte("ROLLBACK\tkubebrain.restore-cutover.marker.v1\tsource\t100\textra\n"),
		0o600,
	))
	f.run(t, false, "", "rollback marker invalid")
	log := f.log(t)
	require.Contains(t, log, "--action fail")
	require.NotContains(t, log, "phase prepare")
}

type cutoverRunnerFixture struct {
	dir, parameters, restoreReceipt, backup string
	env                                     []string
}

func newCutoverRunnerFixture(t *testing.T) *cutoverRunnerFixture {
	t.Helper()
	dir := t.TempDir()
	parameters := filepath.Join(dir, "parameters.json")
	restoreReceipt := filepath.Join(dir, "restore.json")
	backup := filepath.Join(dir, "backup.jsonl")
	receipt := filepath.Join(dir, "receipt.json")
	require.NoError(t, os.WriteFile(restoreReceipt, []byte(fmt.Sprintf(`{
	  "format":"kubebrain.restore-verification.v1","artifact_format":"kubebrain.logical.v2",
	  "artifact_sha256":"%s","snapshot_revision":42,"source_prefix":"/registry",
	  "target_prefix":"/restored","records":2,"artifact_leases":1,
	  "verified_target_leases":1,"verified_at_unix":200
	}`+"\n", restoreArtifactSHA256)), 0o600))
	require.NoError(t, os.WriteFile(backup, []byte("backup\n"), 0o600))
	require.NoError(t, os.WriteFile(parameters, []byte(fmt.Sprintf(`{
	  "state_dir":%q,"restore_receipt_input":%q,"restore_receipt_sha256":%q,
	  "backup_input":%q,"backup_file_sha256":%q,
	  "service_namespace":"ns-a","service_name":"kubebrain","source_instance":"source",
	  "target_instance":"target","expected_replicas":2,"public_endpoint":"https://service:2379",
	  "receipt_output":%q,"timeout_seconds":30,"poll_interval_seconds":0
	}`, filepath.Join(dir, "state"), restoreReceipt, fileDigest(t, restoreReceipt),
		backup, fileDigest(t, backup), receipt)), 0o600))
	data, err := os.ReadFile(parameters)
	require.NoError(t, err)
	digest := fmt.Sprintf("%x", sha256.Sum256(data))

	operationctl := filepath.Join(dir, "operationctl")
	writeTrafficExecutable(t, operationctl, `#!/usr/bin/env bash
set -euo pipefail
printf 'operationctl %s\n' "$*" >>"$FAKE_DIR/actions.log"
if [[ " $* " == *" --action claim "* ]]; then
  digest="${CLAIM_DIGEST:-$PARAMETERS_DIGEST}"
  operation_id="${CLAIM_OPERATION_ID:-cutover-1}"
  instance="${CLAIM_INSTANCE:-instance-a}"
  printf '{"namespace":"tenant-a-operations","name":"cutover-1","uid":"uid-op","resource_version":"1","operation_id":"%s","instance":"%s","type":"RestoreCutover","parameters_sha256":"%s","owner":"worker-a","attempt":1,"lease_until_unix":999999}\n' "$operation_id" "$instance" "$digest"
elif [[ " $* " == *" --action heartbeat "* && "${HEARTBEAT_FAIL:-true}" == true ]]; then
  exit 1
else
  echo '{}'
fi
`)
	cutover := filepath.Join(dir, "cutover")
	writeTrafficExecutable(t, cutover, `#!/usr/bin/env bash
set -euo pipefail
printf 'phase %s\n' "$ACTION" >>"$FAKE_DIR/actions.log"
if [[ "${ASSERT_FROZEN_INPUTS:-false}" == true ]]; then
  [[ "$RESTORE_RECEIPT_INPUT" != "$ORIGINAL_RESTORE_RECEIPT_INPUT" ]] ||
    { echo "restore receipt input was not frozen" >&2; exit 9; }
  [[ "$BACKUP_INPUT" != "$ORIGINAL_BACKUP_INPUT" ]] ||
    { echo "backup input was not frozen" >&2; exit 9; }
  [[ -f "$RESTORE_RECEIPT_INPUT" && -f "$BACKUP_INPUT" ]] ||
    { echo "frozen input missing" >&2; exit 9; }
fi
printf 'restore input %s\n' "$RESTORE_RECEIPT_INPUT" >>"$FAKE_DIR/actions.log"
printf 'backup input %s\n' "$BACKUP_INPUT" >>"$FAKE_DIR/actions.log"
if [[ "${SLEEP_PHASE:-}" == "$ACTION" ]]; then sleep 3; fi
if [[ ",${FAIL_PHASE:-}," == *",$ACTION,"* ]]; then exit 8; fi
state_file="$STATE_DIR/$OPERATION_ID.state"
mkdir -p "$STATE_DIR"
case "$ACTION" in
  prepare)
    {
      printf 'HEADER\tkubebrain.restore-cutover.state.v1\t%s\t%s\t%s\t%s\t%s\t%s\tuid-service\t%s\t42\t/registry\t/restored\n' \
        "$INSTANCE" "$OPERATION_ID" "$SERVICE_NAMESPACE" "$SERVICE_NAME" "$SOURCE_INSTANCE" "$TARGET_INSTANCE" "`+runnerCutoverArtifactSHA256+`"
      printf 'SERVICE\tuid-service\t10\n'
      printf 'POD\tsource\tkb-source-0\tuid-source-0\t0\n'
      printf 'POD\tsource\tkb-source-1\tuid-source-1\t0\n'
      printf 'POD\ttarget\tkb-target-0\tuid-target-0\t0\n'
      printf 'POD\ttarget\tkb-target-1\tuid-target-1\t0\n'
    } >"$state_file"
    ;;
  cutover)
    printf 'CUTOVER\tkubebrain.restore-cutover.marker.v1\t%s\t100\n' "$TARGET_INSTANCE" >"$STATE_DIR/$OPERATION_ID.cutover"
    ;;
  verify)
    printf 'VERIFIED\tkubebrain.restore-cutover.marker.v1\t100\n' >"$STATE_DIR/$OPERATION_ID.verified"
    ;;
  complete)
    artifact_sha="`+runnerCutoverArtifactSHA256+`"
    [[ "${INVALID_CUTOVER_RECEIPT:-false}" != true ]] || artifact_sha=abc123
    [[ "${TAMPER_STATE_BEFORE_RECEIPT:-false}" != true ]] || printf 'UNKNOWN\trow\n' >>"$state_file"
    [[ "${TAMPER_STATE_PREFIX_BEFORE_RECEIPT:-false}" != true ]] || sed -i 's#\t/registry\t/restored#\trelative\t/restored#' "$state_file"
    [[ "${TAMPER_CUTOVER_MARKER_BEFORE_RECEIPT:-false}" != true ]] || printf 'UNKNOWN\trow\n' >>"$STATE_DIR/$OPERATION_ID.cutover"
    [[ "${TAMPER_VERIFIED_MARKER_BEFORE_RECEIPT:-false}" != true ]] || printf 'UNKNOWN\trow\n' >>"$STATE_DIR/$OPERATION_ID.verified"
    state_sha="$(sha256sum "$state_file" | cut -d ' ' -f1)"
    printf '{"format":"kubebrain.restore-cutover.receipt.v1","operation_id":"%s","instance":"%s","service_namespace":"%s","service_name":"%s","service_uid":"uid-service","source_instance":"%s","target_instance":"%s","artifact_sha256":"%s","cutover_state_sha256":"%s","snapshot_revision":42,"replicas":%s,"pod_uids_unchanged":true,"endpoint_uids_matched":true,"public_data_verified":true,"completed_at_unix":100}\n' \
      "$OPERATION_ID" "$INSTANCE" "$SERVICE_NAMESPACE" "$SERVICE_NAME" "$SOURCE_INSTANCE" "$TARGET_INSTANCE" "$artifact_sha" "$state_sha" "$EXPECTED_REPLICAS" >"$RECEIPT_OUTPUT"
    chmod 600 "$RECEIPT_OUTPUT"
    ;;
esac
`)
	env := []string{
		"WORKER_ID=worker-a", "PARAMETERS_INPUT=" + parameters,
		"OPERATION_NAMESPACE=ops", "LEASE_SECONDS=6", "HEARTBEAT_INTERVAL_SECONDS=1", "OPERATIONCTL=" + operationctl,
		"CUTOVER_COMMAND=" + cutover, "FAKE_DIR=" + dir, "PARAMETERS_DIGEST=" + digest,
		"RUNNER_PARAMETERS_INPUT=" + parameters,
		"RUNNER_BACKUP_INPUT=" + backup,
		"RUNNER_EVIDENCE_INPUT=" + restoreReceipt,
		"ORIGINAL_RESTORE_RECEIPT_INPUT=" + restoreReceipt,
		"ORIGINAL_BACKUP_INPUT=" + backup,
	}
	env = append(env, receiptDigestTamperEnv(t, dir, receipt)...)
	return &cutoverRunnerFixture{
		dir: dir, parameters: parameters, restoreReceipt: restoreReceipt, backup: backup,
		env: env,
	}
}

func (f *cutoverRunnerFixture) run(t *testing.T, ok bool, extra string, outputs ...string) {
	t.Helper()
	env := append([]string{}, f.env...)
	for _, item := range strings.Split(extra, "\n") {
		if item != "" {
			env = append(env, item)
		}
	}
	out, err := runProductionRunnerCommand(t, "run-restore-cutover-operation.sh", env)
	if ok {
		require.NoError(t, err, string(out))
	} else {
		require.Error(t, err, string(out))
	}
	for _, wanted := range outputs {
		require.Contains(t, string(out), wanted)
	}
}

func (f *cutoverRunnerFixture) log(t *testing.T) string {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(f.dir, "actions.log"))
	require.NoError(t, err)
	return string(data)
}

func (f *cutoverRunnerFixture) publishEvidence(t *testing.T, kind string) {
	t.Helper()
	stateDir := filepath.Join(f.dir, "state")
	require.NoError(t, os.MkdirAll(stateDir, 0o700))
	statePath := filepath.Join(stateDir, "cutover-1.state")
	if kind == "state" || kind == "cutover" || kind == "receipt" {
		require.NoError(t, os.WriteFile(statePath, []byte(cutoverRunnerState()), 0o600))
	}
	if kind == "state" {
		return
	}
	if kind == "rollback" {
		require.NoError(t, os.WriteFile(
			filepath.Join(stateDir, "cutover-1.rollback"),
			[]byte("ROLLBACK\tkubebrain.restore-cutover.marker.v1\tsource\t100\n"),
			0o600,
		))
		return
	}
	require.NoError(t, os.WriteFile(
		filepath.Join(stateDir, "cutover-1.cutover"),
		[]byte("CUTOVER\tkubebrain.restore-cutover.marker.v1\ttarget\t100\n"),
		0o600,
	))
	if kind == "cutover" {
		return
	}
	require.NoError(t, os.WriteFile(
		filepath.Join(stateDir, "cutover-1.verified"),
		[]byte("VERIFIED\tkubebrain.restore-cutover.marker.v1\t100\n"),
		0o600,
	))
	require.NoError(t, os.WriteFile(filepath.Join(f.dir, "receipt.json"), []byte(cutoverRunnerReceipt()), 0o600))
}

func cutoverRunnerState() string {
	return "HEADER\tkubebrain.restore-cutover.state.v1\tinstance-a\tcutover-1\tns-a\tkubebrain\tsource\ttarget\tuid-service\t" + runnerCutoverArtifactSHA256 + "\t42\t/registry\t/restored\n" +
		"SERVICE\tuid-service\t10\n" +
		"POD\tsource\tkb-source-0\tuid-source-0\t0\n" +
		"POD\tsource\tkb-source-1\tuid-source-1\t0\n" +
		"POD\ttarget\tkb-target-0\tuid-target-0\t0\n" +
		"POD\ttarget\tkb-target-1\tuid-target-1\t0\n"
}

func cutoverRunnerReceipt() string {
	stateSHA := fmt.Sprintf("%x", sha256.Sum256([]byte(cutoverRunnerState())))
	return fmt.Sprintf(`{"format":"kubebrain.restore-cutover.receipt.v1","operation_id":"cutover-1","instance":"instance-a","service_namespace":"ns-a","service_name":"kubebrain","service_uid":"uid-service","source_instance":"source","target_instance":"target","artifact_sha256":"%s","cutover_state_sha256":"%s","snapshot_revision":42,"replicas":2,"pod_uids_unchanged":true,"endpoint_uids_matched":true,"public_data_verified":true,"completed_at_unix":100}`+"\n", runnerCutoverArtifactSHA256, stateSHA)
}

func requireOrdered(t *testing.T, text string, values ...string) {
	t.Helper()
	position := -1
	for _, value := range values {
		next := strings.Index(text, value)
		require.Greater(t, next, position, "%q must follow the previous phase", value)
		position = next
	}
}
