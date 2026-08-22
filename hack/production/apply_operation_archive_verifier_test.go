package production_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestOperationArchiveVerifierManifestIsSuspended(t *testing.T) {
	out, err := runProductionCommand(t, "bash", []string{"apply-operation-archive-verifier.sh", "--verify"}, nil)
	require.NoError(t, err, string(out))
	require.Contains(t, string(out), "verified suspended read-only")
}

func TestOperationArchiveVerifierEnablesOnlyAfterManualSuccess(t *testing.T) {
	f := newArchiveVerifierApplyFixture(t)
	out, err := runProductionCommand(t, "bash", []string{"apply-operation-archive-verifier.sh", "--enable"}, f.env())
	require.NoError(t, err, string(out))
	require.Contains(t, string(out), "retained job/kubebrain-operation-archive-verifier-enable")
	log := string(mustRead(t, f.log))
	require.Contains(t, log, "create job kubebrain-operation-archive-verifier-enable")
	require.Contains(t, log, `"path":"/spec/suspend","value":false`)
	out, err = runProductionCommand(t, "bash", []string{"apply-operation-archive-verifier.sh", "--check-enabled"}, f.env())
	require.NoError(t, err, string(out))
}

func TestOperationArchiveVerifierManualFailureKeepsScheduleSuspended(t *testing.T) {
	f := newArchiveVerifierApplyFixture(t)
	out, err := runProductionCommand(t, "bash", []string{"apply-operation-archive-verifier.sh", "--enable"}, append(f.env(), "FAIL_JOB=true"))
	require.Error(t, err)
	require.Contains(t, string(out), "CronJob remains suspended")
	require.NotContains(t, string(mustRead(t, f.log)), "patch cronjob")
	_, err = os.Stat(f.state)
	require.ErrorIs(t, err, os.ErrNotExist)
}

type archiveVerifierApplyFixture struct{ kubectl, log, state string }

func newArchiveVerifierApplyFixture(t *testing.T) archiveVerifierApplyFixture {
	t.Helper()
	dir := t.TempDir()
	f := archiveVerifierApplyFixture{kubectl: filepath.Join(dir, "kubectl"), log: filepath.Join(dir, "calls.log"), state: filepath.Join(dir, "enabled")}
	writeTrafficExecutable(t, f.kubectl, `#!/usr/bin/env bash
set -euo pipefail
printf '%s\n' "$*" >>"$CALL_LOG"
args=" $* "
if [[ "$args" == *" auth can-i "* ]]; then
  if [[ "$args" == *" get configmap/kubebrain-backup-scheduler-inventory "* || "$args" == *" get kubebrainoperations.dbaas.kubebrain.io "* || "$args" == *" list kubebrainoperations.dbaas.kubebrain.io "* ]]; then echo yes; else echo no; fi
  exit 0
fi
if [[ "$args" == *" get configmap kubebrain-backup-scheduler-inventory "* ]]; then echo '{"data":{"namespaces.json":"[\"tenant-a\",\"tenant-b\"]"}}'; exit 0; fi
if [[ "$args" == *" get secret kubebrain-operation-archive-verifier-object-store "* ]]; then
  echo '{"data":{"access-key-id":"YWNjZXNz","bucket":"YXVkaXQtYnVja2V0","endpoint":"aHR0cHM6Ly9zMy5leGFtcGxl","force-path-style":"ZmFsc2U=","object-store-id":"c3RvcmUtYQ==","region":"dXMtZWFzdC0x","secret-access-key":"c2VjcmV0"}}'; exit 0
fi
if [[ "$args" == *" get cronjob kubebrain-operation-archive-verifier "* ]]; then
  suspend=true; [[ ! -f "$STATE_FILE" ]] || suspend=false
  printf '{"metadata":{"resourceVersion":"7"},"spec":{"concurrencyPolicy":"Forbid","suspend":%s,"jobTemplate":{"spec":{"backoffLimit":0,"template":{"spec":{"restartPolicy":"Never","serviceAccountName":"kubebrain-operation-archive-verifier"}}}}}}\n' "$suspend"; exit 0
fi
if [[ "$args" == *" wait "* ]]; then [[ "${FAIL_JOB:-false}" != true ]]; exit; fi
if [[ "$args" == *" logs "* ]]; then echo 'verified 2 released terminal operation archives'; exit 0; fi
if [[ "$args" == *" patch cronjob "* ]]; then printf x >"$STATE_FILE"; exit 0; fi
if [[ "$args" == *" create job "* ]]; then exit 0; fi
exit 1
`)
	return f
}

func (f archiveVerifierApplyFixture) env() []string {
	return []string{"KUBE_CONTEXT=production", "KUBECTL=" + f.kubectl, "CALL_LOG=" + f.log, "STATE_FILE=" + f.state, "ENABLE_OPERATION_ARCHIVE_VERIFIER=yes"}
}
