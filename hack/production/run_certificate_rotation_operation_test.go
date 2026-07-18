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

func TestCertificateRotationOperationCompletesLifecycle(t *testing.T) {
	f := newRotationRunnerFixture(t)
	f.run(t, true, "")
	log := f.log(t)
	requireOrdered(t, log, "gate begin", "hook overlap", "gate overlap", "hook final", "gate complete")
	require.Contains(t, log, "--action succeed")
}

func TestCertificateRotationOperationRequeuesEveryStepFailure(t *testing.T) {
	for _, step := range []string{"begin", "publish-overlap", "overlap", "publish-final", "complete"} {
		t.Run(step, func(t *testing.T) {
			f := newRotationRunnerFixture(t)
			f.run(t, false, "FAIL_STEP="+step, "was requeued")
			log := f.log(t)
			require.Contains(t, log, "--action retry")
			require.NotContains(t, log, "--action succeed")
		})
	}
}

func TestCertificateRotationOperationResumesFromEvidence(t *testing.T) {
	for _, tc := range []struct {
		evidence string
		wanted   []string
		unwanted []string
	}{
		{"state", []string{"hook overlap", "gate overlap", "hook final", "gate complete"}, []string{"gate begin"}},
		{"overlap", []string{"hook final", "gate complete"}, []string{"gate begin", "hook overlap", "gate overlap"}},
		{"receipt", []string{"gate complete", "--action succeed"}, []string{"gate begin", "hook overlap", "gate overlap", "hook final"}},
	} {
		t.Run(tc.evidence, func(t *testing.T) {
			f := newRotationRunnerFixture(t)
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

func TestCertificateRotationOperationRejectsCredentialAndParameterDrift(t *testing.T) {
	f := newRotationRunnerFixture(t)
	require.NoError(t, os.WriteFile(filepath.Join(f.dir, "old-cert"), []byte("changed"), 0o600))
	f.run(t, false, "", "credential content")
	require.NotContains(t, f.log(t), "gate begin")

	f = newRotationRunnerFixture(t)
	f.run(t, false, "CLAIM_DIGEST="+strings.Repeat("f", 64), "parameters digest")
	require.Contains(t, f.log(t), "--action retry")
}

func TestCertificateRotationOperationStopsWhenHeartbeatIsFenced(t *testing.T) {
	f := newRotationRunnerFixture(t)
	f.run(t, false, "SLEEP_STEP=begin", "heartbeat failed")
	log := f.log(t)
	require.Contains(t, log, "--action heartbeat")
	require.NotContains(t, log, "--action retry")
	require.NotContains(t, log, "--action succeed")
}

type rotationRunnerFixture struct {
	dir, parameters string
	env             []string
}

func newRotationRunnerFixture(t *testing.T) *rotationRunnerFixture {
	t.Helper()
	dir := t.TempDir()
	files := []string{"old-ca", "old-cert", "old-key", "new-ca", "new-cert", "new-key", "overlap-ca"}
	hashes := make(map[string]string)
	for _, name := range files {
		content := []byte(name)
		require.NoError(t, os.WriteFile(filepath.Join(dir, name), content, 0o600))
		hashes[name] = fmt.Sprintf("%x", sha256.Sum256(content))
	}
	parameters := filepath.Join(dir, "parameters.json")
	receipt := filepath.Join(dir, "receipt.json")
	require.NoError(t, os.WriteFile(parameters, []byte(fmt.Sprintf(`{
	  "state_dir":%q,"endpoint":"https://instance.example:2379",
	  "old_cacert":%q,"old_cert":%q,"old_key":%q,
	  "new_cacert":%q,"new_cert":%q,"new_key":%q,"overlap_cacert":%q,
	  "receipt_output":%q,"kubebrain_namespace":"instance-a",
	  "pod_selector":"app.kubernetes.io/name=kubebrain","expected_replicas":3,
	  "old_cacert_sha256":%q,"old_cert_sha256":%q,"old_key_sha256":%q,
	  "new_cacert_sha256":%q,"new_cert_sha256":%q,"new_key_sha256":%q,
	  "overlap_cacert_sha256":%q
	}`, filepath.Join(dir, "state"), filepath.Join(dir, "old-ca"), filepath.Join(dir, "old-cert"),
		filepath.Join(dir, "old-key"), filepath.Join(dir, "new-ca"), filepath.Join(dir, "new-cert"),
		filepath.Join(dir, "new-key"), filepath.Join(dir, "overlap-ca"), receipt,
		hashes["old-ca"], hashes["old-cert"], hashes["old-key"], hashes["new-ca"],
		hashes["new-cert"], hashes["new-key"], hashes["overlap-ca"])), 0o600))
	data, err := os.ReadFile(parameters)
	require.NoError(t, err)
	digest := fmt.Sprintf("%x", sha256.Sum256(data))

	operationctl := filepath.Join(dir, "operationctl")
	writeTrafficExecutable(t, operationctl, `#!/usr/bin/env bash
set -euo pipefail
printf 'operationctl %s\n' "$*" >>"$FAKE_DIR/actions.log"
if [[ " $* " == *" --action claim "* ]]; then
  digest="${CLAIM_DIGEST:-$PARAMETERS_DIGEST}"
  printf '{"name":"rotation-1","uid":"uid-op","resource_version":"1","operation_id":"rotation-1","instance":"instance-a","type":"CertificateRotation","parameters_sha256":"%s","owner":"worker-a","attempt":1,"lease_until_unix":999999}\n' "$digest"
elif [[ " $* " == *" --action heartbeat "* && ( "${HEARTBEAT_FAIL:-false}" == true || -n "${SLEEP_STEP:-}" ) ]]; then
  exit 1
else
  echo '{}'
fi
`)
	gate := filepath.Join(dir, "gate")
	writeTrafficExecutable(t, gate, `#!/usr/bin/env bash
set -euo pipefail
printf 'gate %s\n' "$ACTION" >>"$FAKE_DIR/actions.log"
if [[ "${SLEEP_STEP:-}" == "$ACTION" ]]; then sleep 1; fi
if [[ "${FAIL_STEP:-}" == "$ACTION" ]]; then exit 8; fi
mkdir -p "$STATE_DIR"
case "$ACTION" in
  begin) printf 'state\n' >"$STATE_DIR/$ROTATION_ID.state" ;;
  overlap) printf 'overlap\n' >"$STATE_DIR/$ROTATION_ID.overlap" ;;
  complete) printf '{"format":"receipt"}\n' >"$RECEIPT_OUTPUT" ;;
esac
`)
	overlapHook := filepath.Join(dir, "publish-overlap")
	writeTrafficExecutable(t, overlapHook, `#!/usr/bin/env bash
printf 'hook overlap\n' >>"$FAKE_DIR/actions.log"
[[ "${FAIL_STEP:-}" != publish-overlap ]]
`)
	finalHook := filepath.Join(dir, "publish-final")
	writeTrafficExecutable(t, finalHook, `#!/usr/bin/env bash
printf 'hook final\n' >>"$FAKE_DIR/actions.log"
[[ "${FAIL_STEP:-}" != publish-final ]]
`)
	return &rotationRunnerFixture{
		dir: dir, parameters: parameters,
		env: []string{
			"WORKER_ID=worker-a", "PARAMETERS_INPUT=" + parameters,
			"OPERATION_NAMESPACE=ops", "LEASE_SECONDS=6", "HEARTBEAT_INTERVAL_SECONDS=0.02",
			"OPERATIONCTL=" + operationctl, "ROTATION_COMMAND=" + gate,
			"PUBLISH_OVERLAP_COMMAND=" + overlapHook, "PUBLISH_FINAL_COMMAND=" + finalHook,
			"FAKE_DIR=" + dir, "PARAMETERS_DIGEST=" + digest,
		},
	}
}

func (f *rotationRunnerFixture) run(t *testing.T, ok bool, extra string, outputs ...string) {
	t.Helper()
	cmd := exec.Command("bash", "run-certificate-rotation-operation.sh")
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

func (f *rotationRunnerFixture) log(t *testing.T) string {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(f.dir, "actions.log"))
	require.NoError(t, err)
	return string(data)
}

func (f *rotationRunnerFixture) publishEvidence(t *testing.T, kind string) {
	t.Helper()
	stateDir := filepath.Join(f.dir, "state")
	require.NoError(t, os.MkdirAll(stateDir, 0o700))
	path := filepath.Join(stateDir, "rotation-1."+kind)
	if kind == "receipt" {
		path = filepath.Join(f.dir, "receipt.json")
	}
	require.NoError(t, os.WriteFile(path, []byte("evidence\n"), 0o600))
}
