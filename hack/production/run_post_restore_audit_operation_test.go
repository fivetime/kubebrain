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

func TestPostRestoreAuditOperationCompletesAndBindsReceipt(t *testing.T) {
	f := newOperationRunnerFixture(t)
	f.run(t, true, "")
	log := f.log(t)
	require.Contains(t, log, "--action claim")
	require.Contains(t, log, "--action succeed")
	require.Contains(t, log, "--receipt-sha256")
	require.NotContains(t, log, "--action retry")
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

type operationRunnerFixture struct {
	dir, parameters string
	env             []string
}

func newOperationRunnerFixture(t *testing.T) *operationRunnerFixture {
	t.Helper()
	dir := t.TempDir()
	parameters := filepath.Join(dir, "parameters.json")
	receipt := filepath.Join(dir, "receipt.json")
	require.NoError(t, os.WriteFile(parameters, []byte(fmt.Sprintf(`{
	  "state_dir":%q,"cutover_state_input":%q,"cutover_receipt_input":%q,
	  "service_namespace":"ns-a","service_name":"kubebrain","target_instance":"target",
	  "expected_replicas":2,"public_endpoint":"https://service:2379",
	  "audit_duration_seconds":1,"audit_interval_seconds":0,"min_samples":1,
	  "audit_prefix":"/audit","receipt_output":%q
	}`, filepath.Join(dir, "state"), filepath.Join(dir, "cutover.state"),
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
  printf '{"name":"audit-1","uid":"uid-op","resource_version":"1","operation_id":"audit-1","instance":"instance-a","type":"PostRestoreAudit","parameters_sha256":"%s","owner":"worker-a","attempt":1,"lease_until_unix":999999}\n' "$digest"
else
  echo '{}'
fi
`)
	audit := filepath.Join(dir, "audit")
	writeTrafficExecutable(t, audit, `#!/usr/bin/env bash
set -euo pipefail
[[ "${AUDIT_FAIL:-false}" != true ]] || exit 7
printf '{"format":"receipt"}\n' >"$RECEIPT_OUTPUT"
chmod 600 "$RECEIPT_OUTPUT"
`)
	return &operationRunnerFixture{
		dir: dir, parameters: parameters,
		env: []string{
			"WORKER_ID=worker-a", "PARAMETERS_INPUT=" + parameters,
			"OPERATION_NAMESPACE=ops", "LEASE_SECONDS=6",
			"OPERATIONCTL=" + operationctl, "AUDIT_COMMAND=" + audit,
			"FAKE_DIR=" + dir, "PARAMETERS_DIGEST=" + digest,
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
