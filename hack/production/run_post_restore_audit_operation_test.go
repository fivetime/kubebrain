package production_test

import (
	"crypto/sha256"
	"fmt"
	"os"
	"os/exec"
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

func TestPostRestoreAuditOperationRejectsInvalidReceipt(t *testing.T) {
	f := newOperationRunnerFixture(t)
	f.run(t, false, "INVALID_AUDIT_RECEIPT=true", "invalid receipt")
	log := f.log(t)
	require.Contains(t, log, "--action retry")
	require.NotContains(t, log, "--action succeed")
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

type operationRunnerFixture struct {
	dir, parameters string
	env             []string
}

func newOperationRunnerFixture(t *testing.T) *operationRunnerFixture {
	t.Helper()
	dir := t.TempDir()
	parameters := filepath.Join(dir, "parameters.json")
	receipt := filepath.Join(dir, "receipt.json")
	cutoverState := filepath.Join(dir, "cutover.state")
	require.NoError(t, os.WriteFile(cutoverState, []byte(
		"HEADER\tkubebrain.restore-cutover.state.v1\tinstance-a\tcutover-1\tns-a\tkubebrain\tsource\ttarget\tuid-service\t"+operationAuditArtifactSHA256+"\t42\t/registry\t/restored\n",
	), 0o600))
	require.NoError(t, os.WriteFile(parameters, []byte(fmt.Sprintf(`{
	  "state_dir":%q,"cutover_state_input":%q,"cutover_receipt_input":%q,
	  "service_namespace":"ns-a","service_name":"kubebrain","target_instance":"target",
	  "expected_replicas":2,"public_endpoint":"https://service:2379",
	  "audit_duration_seconds":1,"audit_interval_seconds":0,"min_samples":1,
	  "audit_prefix":"/audit","receipt_output":%q
	}`, filepath.Join(dir, "state"), cutoverState,
		filepath.Join(dir, "cutover.json"), receipt)), 0o600))
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
[[ "${AUDIT_FAIL:-false}" != true ]] || exit 7
artifact_sha="`+operationAuditArtifactSHA256+`"
[[ "${INVALID_AUDIT_RECEIPT:-false}" != true ]] || artifact_sha=abc123
printf '{"format":"kubebrain.post-restore-audit.receipt.v1","operation_id":"%s","instance":"%s","cutover_operation_id":"cutover-1","service_uid":"uid-service","target_instance":"%s","artifact_sha256":"%s","snapshot_revision":42,"replicas":%s,"duration_seconds":%s,"interval_seconds":%s,"samples":%s,"first_probe_revision":1,"last_probe_revision":2,"topology_unchanged":true,"all_probes_succeeded":true,"completed":true,"started_at_unix":1,"completed_at_unix":2}\n' \
  "$OPERATION_ID" "$INSTANCE" "$TARGET_INSTANCE" "$artifact_sha" "$EXPECTED_REPLICAS" "$AUDIT_DURATION_SECONDS" "$AUDIT_INTERVAL_SECONDS" "$MIN_SAMPLES" >"$RECEIPT_OUTPUT"
chmod 600 "$RECEIPT_OUTPUT"
`)
	return &operationRunnerFixture{
		dir: dir, parameters: parameters,
		env: []string{
			"WORKER_ID=worker-a", "PARAMETERS_INPUT=" + parameters,
			"OPERATION_NAMESPACE=ops", "LEASE_SECONDS=6",
			"OPERATIONCTL=" + operationctl, "AUDIT_COMMAND=" + audit,
			"FAKE_DIR=" + dir, "PARAMETERS_DIGEST=" + digest,
			"MANAGED_PARAMETERS=" + parameters,
		},
	}
}

func (f *operationRunnerFixture) run(t *testing.T, ok bool, extra string, outputs ...string) {
	t.Helper()
	cmd := exec.Command("bash", "run-post-restore-audit-operation.sh")
	cmd.Env = append(os.Environ(), append(f.env, extra)...)
	out, err := cmd.CombinedOutput()
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
