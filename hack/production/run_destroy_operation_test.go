package production_test

import (
	"crypto/sha256"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDestroyOperationCompletesLifecycle(t *testing.T) {
	f := newDestroyRunnerFixture(t, true)
	f.run(t, true, "")
	log := f.log(t)
	requireOrdered(t, log, "phase prepare", "phase quiesce", "phase destroy", "phase complete")
	require.Contains(t, log, "--action succeed")
}

func TestDestroyOperationRequeuesPhaseFailures(t *testing.T) {
	for _, phase := range []string{"prepare", "quiesce", "destroy", "complete"} {
		t.Run(phase, func(t *testing.T) {
			f := newDestroyRunnerFixture(t, true)
			f.run(t, false, "FAIL_PHASE="+phase, "was requeued")
			require.Contains(t, f.log(t), "--action retry")
		})
	}
}

func TestDestroyOperationResumesFromEvidence(t *testing.T) {
	for _, tc := range []struct {
		evidence string
		wanted   []string
		unwanted []string
	}{
		{"state", []string{"phase quiesce", "phase destroy", "phase complete"}, []string{"phase prepare"}},
		{"quiesced", []string{"phase destroy", "phase complete"}, []string{"phase prepare", "phase quiesce"}},
		{"destroyed", []string{"phase complete"}, []string{"phase prepare", "phase quiesce", "phase destroy"}},
		{"receipt", []string{"phase complete", "--action succeed"}, []string{"phase prepare", "phase quiesce", "phase destroy"}},
	} {
		t.Run(tc.evidence, func(t *testing.T) {
			f := newDestroyRunnerFixture(t, true)
			f.publishEvidence(t, tc.evidence)
			f.run(t, true, "")
			log := f.log(t)
			for _, value := range tc.wanted {
				require.Contains(t, log, value)
			}
			for _, value := range tc.unwanted {
				require.NotContains(t, log, value)
			}
		})
	}
}

func TestDestroyOperationRejectsConfirmationAndBackupDrift(t *testing.T) {
	f := newDestroyRunnerFixture(t, false)
	f.run(t, false, "", "must exactly equal")
	require.Contains(t, f.log(t), "--action fail")
	require.NotContains(t, f.log(t), "phase prepare")

	f = newDestroyRunnerFixture(t, true)
	require.NoError(t, os.WriteFile(filepath.Join(f.dir, "backup.jsonl"), []byte("changed"), 0o600))
	f.run(t, false, "", "backup bytes")
	require.Contains(t, f.log(t), "--action retry")
	require.NotContains(t, f.log(t), "phase prepare")
}

func TestDestroyOperationStopsWhenHeartbeatIsFenced(t *testing.T) {
	f := newDestroyRunnerFixture(t, true)
	f.run(t, false, "SLEEP_PHASE=prepare", "heartbeat failed")
	log := f.log(t)
	require.Contains(t, log, "--action heartbeat")
	require.NotContains(t, log, "--action retry")
	require.NotContains(t, log, "--action succeed")
}

type destroyRunnerFixture struct {
	dir, parameters string
	env             []string
}

func newDestroyRunnerFixture(t *testing.T, validConfirmation bool) *destroyRunnerFixture {
	t.Helper()
	dir := t.TempDir()
	backup := filepath.Join(dir, "backup.jsonl")
	require.NoError(t, os.WriteFile(backup, []byte("backup"), 0o600))
	backupData, err := os.ReadFile(backup)
	require.NoError(t, err)
	confirmation := "destroy:instance-a:destroy-1"
	if !validConfirmation {
		confirmation = "destroy:wrong"
	}
	parameters := filepath.Join(dir, "parameters.json")
	receipt := filepath.Join(dir, "receipt.json")
	require.NoError(t, os.WriteFile(parameters, []byte(fmt.Sprintf(`{
	  "state_dir":%q,"backup_input":%q,"backup_file_sha256":"%x",
	  "backup_prefix":"/registry","backup_max_age_seconds":3600,"backup_min_records":1,
	  "confirm_destroy":%q,"receipt_output":%q,"kubebrain_namespace":"instance-a",
	  "kubebrain_statefulset":"kubebrain","tidb_namespace":"storage-a","tidb_cluster":"kb",
	  "expected_pvcs":2,"timeout_seconds":30,"poll_interval_seconds":0
	}`, filepath.Join(dir, "state"), backup, sha256.Sum256(backupData), confirmation, receipt)), 0o600))
	data, err := os.ReadFile(parameters)
	require.NoError(t, err)
	digest := fmt.Sprintf("%x", sha256.Sum256(data))

	operationctl := filepath.Join(dir, "operationctl")
	writeTrafficExecutable(t, operationctl, `#!/usr/bin/env bash
set -euo pipefail
printf 'operationctl %s\n' "$*" >>"$FAKE_DIR/actions.log"
if [[ " $* " == *" --action claim "* ]]; then
  digest="${CLAIM_DIGEST:-$PARAMETERS_DIGEST}"
  printf '{"name":"destroy-1","uid":"uid-op","resource_version":"1","operation_id":"destroy-1","instance":"instance-a","type":"Destroy","parameters_sha256":"%s","owner":"worker-a","attempt":1,"lease_until_unix":999999}\n' "$digest"
elif [[ " $* " == *" --action heartbeat "* && -n "${SLEEP_PHASE:-}" ]]; then
  exit 1
else
  echo '{}'
fi
`)
	destroy := filepath.Join(dir, "destroy")
	writeTrafficExecutable(t, destroy, `#!/usr/bin/env bash
set -euo pipefail
printf 'phase %s\n' "$ACTION" >>"$FAKE_DIR/actions.log"
if [[ "${SLEEP_PHASE:-}" == "$ACTION" ]]; then sleep 1; fi
if [[ "${FAIL_PHASE:-}" == "$ACTION" ]]; then exit 8; fi
mkdir -p "$STATE_DIR"
case "$ACTION" in
  prepare) printf 'state\n' >"$STATE_DIR/$OPERATION_ID.state" ;;
  quiesce) printf 'quiesced\n' >"$STATE_DIR/$OPERATION_ID.quiesced" ;;
  destroy) printf 'destroyed\n' >"$STATE_DIR/$OPERATION_ID.destroyed" ;;
  complete) printf '{"format":"receipt"}\n' >"$RECEIPT_OUTPUT" ;;
esac
`)
	return &destroyRunnerFixture{
		dir: dir, parameters: parameters,
		env: []string{
			"WORKER_ID=worker-a", "PARAMETERS_INPUT=" + parameters,
			"OPERATION_NAMESPACE=ops", "LEASE_SECONDS=6", "HEARTBEAT_INTERVAL_SECONDS=0.02",
			"OPERATIONCTL=" + operationctl, "DESTROY_COMMAND=" + destroy,
			"FAKE_DIR=" + dir, "PARAMETERS_DIGEST=" + digest,
		},
	}
}

func (f *destroyRunnerFixture) run(t *testing.T, ok bool, extra string, outputs ...string) {
	t.Helper()
	cmd := exec.Command("bash", "run-destroy-operation.sh")
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

func (f *destroyRunnerFixture) log(t *testing.T) string {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(f.dir, "actions.log"))
	require.NoError(t, err)
	return string(data)
}

func (f *destroyRunnerFixture) publishEvidence(t *testing.T, kind string) {
	t.Helper()
	stateDir := filepath.Join(f.dir, "state")
	require.NoError(t, os.MkdirAll(stateDir, 0o700))
	path := filepath.Join(stateDir, "destroy-1."+kind)
	if kind == "receipt" {
		path = filepath.Join(f.dir, "receipt.json")
	}
	require.NoError(t, os.WriteFile(path, []byte("evidence\n"), 0o600))
}
