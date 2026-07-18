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

func TestRestoreCutoverOperationCompletesAllPhases(t *testing.T) {
	f := newCutoverRunnerFixture(t)
	f.run(t, true, "")
	log := f.log(t)
	requireOrdered(t, log, "phase prepare", "phase cutover", "phase verify", "phase complete")
	require.NotContains(t, log, "phase rollback")
	require.Contains(t, log, "--action succeed")
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

type cutoverRunnerFixture struct {
	dir, parameters string
	env             []string
}

func newCutoverRunnerFixture(t *testing.T) *cutoverRunnerFixture {
	t.Helper()
	dir := t.TempDir()
	parameters := filepath.Join(dir, "parameters.json")
	receipt := filepath.Join(dir, "receipt.json")
	require.NoError(t, os.WriteFile(parameters, []byte(fmt.Sprintf(`{
	  "state_dir":%q,"restore_receipt_input":%q,"backup_input":%q,
	  "service_namespace":"ns-a","service_name":"kubebrain","source_instance":"source",
	  "target_instance":"target","expected_replicas":2,"public_endpoint":"https://service:2379",
	  "receipt_output":%q,"timeout_seconds":30,"poll_interval_seconds":0
	}`, filepath.Join(dir, "state"), filepath.Join(dir, "restore.json"),
		filepath.Join(dir, "backup.jsonl"), receipt)), 0o600))
	data, err := os.ReadFile(parameters)
	require.NoError(t, err)
	digest := fmt.Sprintf("%x", sha256.Sum256(data))

	operationctl := filepath.Join(dir, "operationctl")
	writeTrafficExecutable(t, operationctl, `#!/usr/bin/env bash
set -euo pipefail
printf 'operationctl %s\n' "$*" >>"$FAKE_DIR/actions.log"
if [[ " $* " == *" --action claim "* ]]; then
  digest="${CLAIM_DIGEST:-$PARAMETERS_DIGEST}"
  printf '{"name":"cutover-1","uid":"uid-op","resource_version":"1","operation_id":"cutover-1","instance":"instance-a","type":"RestoreCutover","parameters_sha256":"%s","owner":"worker-a","attempt":1,"lease_until_unix":999999}\n' "$digest"
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
if [[ "${SLEEP_PHASE:-}" == "$ACTION" ]]; then sleep 3; fi
if [[ ",${FAIL_PHASE:-}," == *",$ACTION,"* ]]; then exit 8; fi
if [[ "$ACTION" == complete ]]; then
  printf '{"format":"kubebrain.restore-cutover.receipt.v1"}\n' >"$RECEIPT_OUTPUT"
  chmod 600 "$RECEIPT_OUTPUT"
fi
`)
	return &cutoverRunnerFixture{
		dir: dir, parameters: parameters,
		env: []string{
			"WORKER_ID=worker-a", "PARAMETERS_INPUT=" + parameters,
			"OPERATION_NAMESPACE=ops", "LEASE_SECONDS=6", "OPERATIONCTL=" + operationctl,
			"CUTOVER_COMMAND=" + cutover, "FAKE_DIR=" + dir, "PARAMETERS_DIGEST=" + digest,
		},
	}
}

func (f *cutoverRunnerFixture) run(t *testing.T, ok bool, extra string, outputs ...string) {
	t.Helper()
	cmd := exec.Command("bash", "run-restore-cutover-operation.sh")
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
	path := filepath.Join(stateDir, "cutover-1."+kind)
	if kind == "receipt" {
		path = filepath.Join(f.dir, "receipt.json")
	}
	require.NoError(t, os.WriteFile(path, []byte("evidence\n"), 0o600))
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
