package production_test

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

const operationAuditArtifactSHA256 = "1111111111111111111111111111111111111111111111111111111111111111"

func TestPostRestoreAuditOperationCompletesAndBindsReceipt(t *testing.T) {
	f := newOperationRunnerFixture(t)
	f.run(t, true, "")
	log := f.log(t)
	require.Contains(t, log, "--action claim")
	require.Contains(t, log, "--action succeed")
	require.Contains(t, log, "--receipt-sha256")
	require.Contains(t, log, "--namespace tenant-a-operations --action succeed")
	require.NotContains(t, log, "--namespace ops --namespace tenant-a-operations")
	require.NotContains(t, log, "--action retry")
}

func TestPostRestoreAuditOperationPassesFrozenEvidenceToAudit(t *testing.T) {
	f := newOperationRunnerFixture(t)
	f.run(t, true, "ASSERT_FROZEN_INPUTS=true")
	log := f.auditLog(t)
	require.NotContains(t, log, f.cutoverState)
	require.NotContains(t, log, f.cutoverReceipt)
}

func TestPostRestoreAuditOperationRejectsInvalidReceipt(t *testing.T) {
	f := newOperationRunnerFixture(t)
	f.run(t, false, "INVALID_AUDIT_RECEIPT=true", "invalid receipt")
	log := f.log(t)
	require.Contains(t, log, "--action retry")
	require.NotContains(t, log, "--action succeed")
}

func TestPostRestoreAuditOperationRejectsReceiptTamperedDuringDigest(t *testing.T) {
	f := newOperationRunnerFixture(t)
	f.run(t, false, "TAMPER_RECEIPT_DURING_SHA256=true", "invalid receipt")
	log := f.log(t)
	require.Contains(t, log, "--action retry")
	require.NotContains(t, log, "--action succeed")
}

func TestPostRestoreAuditOperationRejectsValidReceiptChangedAfterDigest(t *testing.T) {
	f := newOperationRunnerFixture(t)
	tamperedReceipt := filepath.Join(f.dir, "valid-tampered-receipt.json")
	receipt := fmt.Sprintf(`{"format":"kubebrain.post-restore-audit.receipt.v1","operation_id":"audit-1","instance":"instance-a","cutover_operation_id":"cutover-1","service_uid":"uid-service","target_instance":"target","artifact_sha256":"%s","cutover_state_sha256":"%s","cutover_receipt_sha256":"%s","snapshot_revision":42,"replicas":2,"duration_seconds":1,"interval_seconds":0,"samples":1,"first_probe_revision":1,"last_probe_revision":2,"topology_unchanged":true,"all_probes_succeeded":true,"completed":true,"started_at_unix":1,"completed_at_unix":3}`+"\n",
		operationAuditArtifactSHA256,
		fileDigest(t, filepath.Join(f.dir, "cutover.state")),
		fileDigest(t, filepath.Join(f.dir, "cutover.json")),
	)
	require.NoError(t, os.WriteFile(tamperedReceipt, []byte(receipt), 0o600))
	f.env = withReceiptAfterSHA256Tamper(f.env, tamperedReceipt)

	f.run(t, false, "", "invalid receipt")
	log := f.log(t)
	require.Contains(t, log, "--action retry")
	require.NotContains(t, log, "--action succeed")
}

func TestPostRestoreAuditOperationRejectsNonCanonicalCutoverState(t *testing.T) {
	f := newOperationRunnerFixture(t)
	path := filepath.Join(f.dir, "cutover.state")
	require.NoError(t, os.WriteFile(path, append(mustRead(t, path), []byte("UNKNOWN\trow\n")...), 0o600))
	parameters := strings.ReplaceAll(
		string(mustRead(t, f.parameters)),
		f.cutoverStateSHA,
		fileDigest(t, path),
	)
	require.NoError(t, os.WriteFile(f.parameters, []byte(parameters), 0o600))
	f.run(t, false, "CLAIM_DIGEST="+fileDigest(t, f.parameters), "invalid receipt")
	log := f.log(t)
	require.Contains(t, log, "--action retry")
	require.NotContains(t, log, "--action succeed")
}

func TestPostRestoreAuditOperationRejectsCutoverStateWithInvalidPrefixes(t *testing.T) {
	f := newOperationRunnerFixture(t)
	statePath := filepath.Join(f.dir, "cutover.state")
	state := strings.Replace(string(mustRead(t, statePath)), "\t/registry\t/restored", "\trelative\t/restored", 1)
	require.NoError(t, os.WriteFile(statePath, []byte(state), 0o600))
	newStateSHA := fileDigest(t, statePath)

	receipt := strings.Replace(string(mustRead(t, f.cutoverReceipt)), f.cutoverStateSHA, newStateSHA, 1)
	require.NoError(t, os.WriteFile(f.cutoverReceipt, []byte(receipt), 0o600))
	newReceiptSHA := fileDigest(t, f.cutoverReceipt)

	parameters := strings.ReplaceAll(string(mustRead(t, f.parameters)), f.cutoverStateSHA, newStateSHA)
	parameters = strings.ReplaceAll(parameters, f.cutoverReceiptSHA, newReceiptSHA)
	require.NoError(t, os.WriteFile(f.parameters, []byte(parameters), 0o600))
	f.run(t, false, "CLAIM_DIGEST="+fileDigest(t, f.parameters), "invalid receipt")
	log := f.log(t)
	require.Contains(t, log, "--action retry")
	require.NotContains(t, log, "--action succeed")
}

func TestPostRestoreAuditOperationRejectsCutoverEvidenceDriftAfterAudit(t *testing.T) {
	for _, tc := range []struct {
		name, env string
	}{
		{name: "state", env: "TAMPER_CUTOVER_STATE_AFTER_AUDIT=true"},
		{name: "receipt", env: "TAMPER_CUTOVER_RECEIPT_AFTER_AUDIT=true"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newOperationRunnerFixture(t)
			f.run(t, false, tc.env, "invalid receipt")
			log := f.log(t)
			require.Contains(t, log, "--action retry")
			require.NotContains(t, log, "--action succeed")
		})
	}
}

func TestPostRestoreAuditOperationRejectsCutoverEvidenceDrift(t *testing.T) {
	for _, tc := range []struct {
		name    string
		path    func(*operationRunnerFixture) string
		message string
	}{
		{
			name: "state",
			path: func(f *operationRunnerFixture) string {
				return f.cutoverState
			},
			message: "cutover state bytes do not match",
		},
		{
			name: "receipt",
			path: func(f *operationRunnerFixture) string {
				return f.cutoverReceipt
			},
			message: "cutover receipt bytes do not match",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newOperationRunnerFixture(t)
			require.NoError(t, os.WriteFile(tc.path(f), []byte("changed\n"), 0o600))
			f.run(t, false, "", tc.message)
			log := f.log(t)
			require.Contains(t, log, "--action retry")
			require.NotContains(t, log, "--action succeed")
			require.NoFileExists(t, filepath.Join(f.dir, "audit.log"))
		})
	}
}

func TestPostRestoreAuditOperationRejectsCutoverEvidenceTamperedDuringCapture(t *testing.T) {
	for _, tc := range []struct {
		name    string
		path    func(*operationRunnerFixture) string
		message string
	}{
		{
			name: "state",
			path: func(f *operationRunnerFixture) string {
				return f.cutoverState
			},
			message: "cutover state bytes changed",
		},
		{
			name: "receipt",
			path: func(f *operationRunnerFixture) string {
				return f.cutoverReceipt
			},
			message: "cutover receipt bytes changed",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newOperationRunnerFixture(t)
			f.env = append(f.env, "RUNNER_EVIDENCE_INPUT="+tc.path(f))
			f.run(t, false, "TAMPER_EVIDENCE_DURING_SHA256=true", tc.message)
			log := f.log(t)
			require.Contains(t, log, "--action retry")
			require.NotContains(t, log, "--action succeed")
			require.NoFileExists(t, filepath.Join(f.dir, "audit.log"))
		})
	}
}

func TestPostRestoreAuditOperationRequeuesFailures(t *testing.T) {
	f := newOperationRunnerFixture(t)
	f.run(t, false, "AUDIT_FAIL=true", "failed and was requeued")
	log := f.log(t)
	require.Contains(t, log, "--action retry")
	require.NotContains(t, log, "--action succeed")
}

func TestPostRestoreAuditOperationRejectsParameterDrift(t *testing.T) {
	f := newOperationRunnerFixture(t)
	f.run(t, false, "CLAIM_DIGEST="+strings.Repeat("f", 64), "parameters digest")
	require.Contains(t, f.log(t), "--action retry")
}

func TestPostRestoreAuditOperationRejectsParametersTamperedDuringDigest(t *testing.T) {
	f := newOperationRunnerFixture(t)
	f.run(t, false, "TAMPER_PARAMETERS_DURING_SHA256=true", "parameters digest")
	log := f.log(t)
	require.Contains(t, log, "--action retry")
	require.NotContains(t, log, "--action succeed")
	require.NoFileExists(t, filepath.Join(f.dir, "receipt.json"))
}

func TestPostRestoreAuditOperationRejectsEmptyRequiredParameters(t *testing.T) {
	f := newOperationRunnerFixture(t)
	parameters := strings.ReplaceAll(
		string(mustRead(t, f.parameters)),
		`"public_endpoint":"https://service:2379"`,
		`"public_endpoint":""`,
	)
	require.NoError(t, os.WriteFile(f.parameters, []byte(parameters), 0o600))

	f.run(t, false, "CLAIM_DIGEST="+fileDigest(t, f.parameters), "empty required field")
	log := f.log(t)
	require.NotContains(t, log, "--action retry")
	require.NotContains(t, log, "--action succeed")
}

func TestPostRestoreAuditOperationLoadsManagedParameters(t *testing.T) {
	f := newOperationRunnerFixture(t)
	env := make([]string, 0, len(f.env))
	for _, value := range f.env {
		if !strings.HasPrefix(value, "PARAMETERS_INPUT=") {
			env = append(env, value)
		}
	}
	f.env = env
	f.run(t, true, "")
	require.Contains(t, f.log(t), "--action parameters")
}

func TestPostRestoreAuditOperationRejectsUnsafeAuditPrefix(t *testing.T) {
	for _, tc := range []struct {
		name, prefix, want string
	}{
		{name: "relative", prefix: "relative", want: "absolute key prefix"},
		{name: "root", prefix: "/", want: "must not target"},
		{name: "registry", prefix: "/registry", want: "must not target"},
		{name: "registry child", prefix: "/registry/pods", want: "must not target"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newOperationRunnerFixture(t)
			f.replaceAuditPrefix(t, tc.prefix)
			f.run(t, false, "CLAIM_DIGEST="+fileDigest(t, f.parameters), tc.want)
			require.NotContains(t, f.log(t), "--action retry")
			require.NotContains(t, f.log(t), "--action succeed")
			require.NoFileExists(t, filepath.Join(f.dir, "audit.log"))
		})
	}
}

type operationRunnerFixture struct {
	dir, parameters, cutoverState, cutoverReceipt, cutoverStateSHA, cutoverReceiptSHA string
	env                                                                               []string
}

func newOperationRunnerFixture(t *testing.T) *operationRunnerFixture {
	t.Helper()
	dir := t.TempDir()
	parameters := filepath.Join(dir, "parameters.json")
	receipt := filepath.Join(dir, "receipt.json")
	cutoverState := filepath.Join(dir, "cutover.state")
	cutoverReceipt := filepath.Join(dir, "cutover.json")
	require.NoError(t, os.WriteFile(cutoverState, []byte(operationAuditCutoverState()), 0o600))
	require.NoError(t, os.WriteFile(cutoverReceipt, []byte(operationAuditCutoverReceipt()), 0o600))
	cutoverStateSHA := fileDigest(t, cutoverState)
	cutoverReceiptSHA := fileDigest(t, cutoverReceipt)
	require.NoError(t, os.WriteFile(parameters, []byte(fmt.Sprintf(`{
	  "state_dir":%q,"cutover_state_input":%q,"cutover_state_sha256":%q,
	  "cutover_receipt_input":%q,"cutover_receipt_sha256":%q,
	  "service_namespace":"ns-a","service_name":"kubebrain","target_instance":"target",
	  "expected_replicas":2,"public_endpoint":"https://service:2379",
	  "audit_duration_seconds":1,"audit_interval_seconds":0,"min_samples":1,
	  "audit_prefix":"/audit","receipt_output":%q
	}`, filepath.Join(dir, "state"), cutoverState, cutoverStateSHA,
		cutoverReceipt, cutoverReceiptSHA, receipt)), 0o600))
	data, err := os.ReadFile(parameters)
	require.NoError(t, err)
	digest := fmt.Sprintf("%x", sha256.Sum256(data))

	operationctl := filepath.Join(dir, "operationctl")
	writeTrafficExecutable(t, operationctl, `#!/usr/bin/env bash
set -euo pipefail
printf '%s\n' "$*" >>"$FAKE_DIR/operationctl.log"
if [[ " $* " == *" --action claim "* ]]; then
  digest="${CLAIM_DIGEST:-$PARAMETERS_DIGEST}"
  printf '{"namespace":"tenant-a-operations","name":"audit-1","uid":"uid-op","resource_version":"1","operation_id":"audit-1","instance":"instance-a","type":"PostRestoreAudit","parameters_sha256":"%s","owner":"worker-a","attempt":1,"lease_until_unix":999999}\n' "$digest"
elif [[ " $* " == *" --action parameters "* ]]; then
  cat "$MANAGED_PARAMETERS"
else
  echo '{}'
fi
`)
	audit := filepath.Join(dir, "audit")
	writeTrafficExecutable(t, audit, `#!/usr/bin/env bash
set -euo pipefail
printf 'audit inputs %s %s\n' "$CUTOVER_STATE_INPUT" "$CUTOVER_RECEIPT_INPUT" >>"$FAKE_DIR/audit.log"
if [[ "${ASSERT_FROZEN_INPUTS:-false}" == true ]]; then
  [[ "$CUTOVER_STATE_INPUT" != "$ORIGINAL_CUTOVER_STATE_INPUT" ]] ||
    { echo "cutover state input was not frozen" >&2; exit 9; }
  [[ "$CUTOVER_RECEIPT_INPUT" != "$ORIGINAL_CUTOVER_RECEIPT_INPUT" ]] ||
    { echo "cutover receipt input was not frozen" >&2; exit 9; }
  [[ -f "$CUTOVER_STATE_INPUT" && -f "$CUTOVER_RECEIPT_INPUT" ]] ||
    { echo "frozen cutover evidence missing" >&2; exit 9; }
fi
[[ "${AUDIT_FAIL:-false}" != true ]] || exit 7
artifact_sha="`+operationAuditArtifactSHA256+`"
[[ "${INVALID_AUDIT_RECEIPT:-false}" != true ]] || artifact_sha=abc123
cutover_state_sha="$(sha256sum "$CUTOVER_STATE_INPUT" | cut -d ' ' -f1)"
cutover_receipt_sha="$(sha256sum "$CUTOVER_RECEIPT_INPUT" | cut -d ' ' -f1)"
printf '{"format":"kubebrain.post-restore-audit.receipt.v1","operation_id":"%s","instance":"%s","cutover_operation_id":"cutover-1","service_uid":"uid-service","target_instance":"%s","artifact_sha256":"%s","cutover_state_sha256":"%s","cutover_receipt_sha256":"%s","snapshot_revision":42,"replicas":%s,"duration_seconds":%s,"interval_seconds":%s,"samples":%s,"first_probe_revision":1,"last_probe_revision":2,"topology_unchanged":true,"all_probes_succeeded":true,"completed":true,"started_at_unix":1,"completed_at_unix":2}\n' \
  "$OPERATION_ID" "$INSTANCE" "$TARGET_INSTANCE" "$artifact_sha" "$cutover_state_sha" "$cutover_receipt_sha" "$EXPECTED_REPLICAS" "$AUDIT_DURATION_SECONDS" "$AUDIT_INTERVAL_SECONDS" "$MIN_SAMPLES" >"$RECEIPT_OUTPUT"
chmod 600 "$RECEIPT_OUTPUT"
[[ "${TAMPER_CUTOVER_STATE_AFTER_AUDIT:-false}" != true ]] || printf 'UNKNOWN\trow\n' >>"$CUTOVER_STATE_INPUT"
[[ "${TAMPER_CUTOVER_RECEIPT_AFTER_AUDIT:-false}" != true ]] || printf ' ' >>"$CUTOVER_RECEIPT_INPUT"
`)
	env := []string{
		"WORKER_ID=worker-a", "PARAMETERS_INPUT=" + parameters,
		"OPERATION_NAMESPACE=ops", "LEASE_SECONDS=6", "HEARTBEAT_INTERVAL_SECONDS=1",
		"OPERATIONCTL=" + operationctl, "AUDIT_COMMAND=" + audit,
		"FAKE_DIR=" + dir, "PARAMETERS_DIGEST=" + digest,
		"MANAGED_PARAMETERS=" + parameters,
		"RUNNER_PARAMETERS_INPUT=" + parameters,
		"ORIGINAL_CUTOVER_STATE_INPUT=" + cutoverState,
		"ORIGINAL_CUTOVER_RECEIPT_INPUT=" + cutoverReceipt,
	}
	env = append(env, receiptDigestTamperEnv(t, dir, receipt)...)
	return &operationRunnerFixture{
		dir: dir, parameters: parameters, cutoverState: cutoverState, cutoverReceipt: cutoverReceipt,
		cutoverStateSHA: cutoverStateSHA, cutoverReceiptSHA: cutoverReceiptSHA,
		env: env,
	}
}

func operationAuditCutoverState() string {
	return "HEADER\tkubebrain.restore-cutover.state.v1\tinstance-a\tcutover-1\tns-a\tkubebrain\tsource\ttarget\tuid-service\t" + operationAuditArtifactSHA256 + "\t42\t/registry\t/restored\n" +
		"SERVICE\tuid-service\t10\n" +
		"POD\tsource\tkb-source-0\tuid-source-0\t0\n" +
		"POD\tsource\tkb-source-1\tuid-source-1\t0\n" +
		"POD\ttarget\tkb-target-0\tuid-target-0\t0\n" +
		"POD\ttarget\tkb-target-1\tuid-target-1\t0\n"
}

func operationAuditCutoverReceipt() string {
	stateSHA := fmt.Sprintf("%x", sha256.Sum256([]byte(operationAuditCutoverState())))
	return fmt.Sprintf(`{"format":"kubebrain.restore-cutover.receipt.v1","operation_id":"cutover-1","instance":"instance-a","service_namespace":"ns-a","service_name":"kubebrain","service_uid":"uid-service","source_instance":"source","target_instance":"target","artifact_sha256":"%s","cutover_state_sha256":"%s","snapshot_revision":42,"replicas":2,"pod_uids_unchanged":true,"endpoint_uids_matched":true,"public_data_verified":true,"completed_at_unix":100}`+"\n", operationAuditArtifactSHA256, stateSHA)
}

func (f *operationRunnerFixture) replaceAuditPrefix(t *testing.T, prefix string) {
	t.Helper()
	var parameters map[string]any
	require.NoError(t, json.Unmarshal(mustRead(t, f.parameters), &parameters))
	parameters["audit_prefix"] = prefix
	encoded, err := json.Marshal(parameters)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(f.parameters, append(encoded, '\n'), 0o600))
}

func (f *operationRunnerFixture) run(t *testing.T, ok bool, extra string, outputs ...string) {
	t.Helper()
	env := append([]string{}, f.env...)
	if extra != "" {
		env = append(env, extra)
	}
	out, err := runProductionRunnerCommand(t, "run-post-restore-audit-operation.sh", env)
	if ok {
		require.NoError(t, err, string(out))
	} else {
		require.Error(t, err, string(out))
	}
	for _, wanted := range outputs {
		require.Contains(t, string(out), wanted)
	}
}

func (f *operationRunnerFixture) log(t *testing.T) string {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(f.dir, "operationctl.log"))
	require.NoError(t, err)
	return string(data)
}

func (f *operationRunnerFixture) auditLog(t *testing.T) string {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(f.dir, "audit.log"))
	require.NoError(t, err)
	return string(data)
}
