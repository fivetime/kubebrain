package production_test

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

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

func TestOperationArchiveVerifierRejectsStaleIAMEvidenceBeforeJob(t *testing.T) {
	f := newArchiveVerifierApplyFixture(t)
	require.NoError(t, os.WriteFile(f.evidence, []byte(iamEvidenceJSON(1)), 0o600))
	out, err := runProductionCommand(t, "bash", []string{"apply-operation-archive-verifier.sh", "--enable"}, f.env())
	require.Error(t, err)
	require.Contains(t, string(out), "IAM simulation evidence is invalid, stale")
	require.NotContains(t, string(mustRead(t, f.log)), "create job")
}

func TestOperationArchiveVerifierRejectsIAMScopeAndDecisionDriftBeforeJob(t *testing.T) {
	for _, tc := range []struct{ name, old, replacement string }{
		{name: "bucket", old: `"bucket":"audit-bucket"`, replacement: `"bucket":"other-bucket"`},
		{name: "put allowed", old: `"action":"s3:PutObject","decision":"denied"`, replacement: `"action":"s3:PutObject","decision":"allowed"`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newArchiveVerifierApplyFixture(t)
			data := strings.Replace(string(mustRead(t, f.evidence)), tc.old, tc.replacement, 1)
			require.NoError(t, os.WriteFile(f.evidence, []byte(data), 0o600))
			out, err := runProductionCommand(t, "bash", []string{"apply-operation-archive-verifier.sh", "--enable"}, f.env())
			require.Error(t, err)
			require.Contains(t, string(out), "does not prove the exact allow/deny matrix")
			require.NotContains(t, string(mustRead(t, f.log)), "create job")
		})
	}
}

type archiveVerifierApplyFixture struct{ kubectl, log, state, evidence string }

func newArchiveVerifierApplyFixture(t *testing.T) archiveVerifierApplyFixture {
	t.Helper()
	dir := t.TempDir()
	f := archiveVerifierApplyFixture{kubectl: filepath.Join(dir, "kubectl"), log: filepath.Join(dir, "calls.log"), state: filepath.Join(dir, "enabled"), evidence: filepath.Join(dir, "iam.json")}
	require.NoError(t, os.WriteFile(f.evidence, []byte(iamEvidenceJSON(time.Now().Unix())), 0o600))
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
  suspend=true; binding=pending
  if [[ -f "$STATE_FILE" ]]; then suspend=false; binding="$(sha256sum "$IAM_SIMULATION_EVIDENCE" | awk '{print $1}')"; fi
  printf '{"metadata":{"annotations":{"dbaas.kubebrain.io/iam-simulation-sha256":"%s"},"resourceVersion":"7"},"spec":{"concurrencyPolicy":"Forbid","suspend":%s,"jobTemplate":{"spec":{"backoffLimit":0,"template":{"spec":{"restartPolicy":"Never","serviceAccountName":"kubebrain-operation-archive-verifier"}}}}}}\n' "$binding" "$suspend"; exit 0
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
	return []string{"KUBE_CONTEXT=production", "KUBECTL=" + f.kubectl, "CALL_LOG=" + f.log, "STATE_FILE=" + f.state, "IAM_SIMULATION_EVIDENCE=" + f.evidence, "ENABLE_OPERATION_ARCHIVE_VERIFIER=yes"}
}

func iamEvidenceJSON(checked int64) string {
	decisions := `[{"action":"s3:DeleteObject","decision":"denied","resource":"object"},{"action":"s3:GetBucketVersioning","decision":"allowed","resource":"bucket"},{"action":"s3:GetObject","decision":"allowed","resource":"object"},{"action":"s3:GetObjectLockConfiguration","decision":"allowed","resource":"bucket"},{"action":"s3:GetObjectRetention","decision":"allowed","resource":"object"},{"action":"s3:ListBucket","decision":"denied","resource":"bucket"},{"action":"s3:ListBucketVersions","decision":"denied","resource":"bucket"},{"action":"s3:PutObject","decision":"denied","resource":"object"}]`
	return fmt.Sprintf(`{"bucket":"audit-bucket","checked_at_unix":%d,"decisions":%s,"format":"kubebrain.object-store-iam-simulation.v1","object_store_id":"store-a","principal":"arn:aws:iam::123456789012:role/verifier","provider":"aws-s3","valid_until_unix":%d}`+"\n", checked, decisions, checked+3600)
}
