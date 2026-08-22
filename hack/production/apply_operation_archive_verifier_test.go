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
	require.Contains(t, string(out), "retained job/kubebrain-archive-verifier-enable-abcde")
	log := string(mustRead(t, f.log))
	require.Contains(t, log, "create -f - -o json")
	require.Contains(t, string(mustRead(t, f.payloadLog)), `"generateName":"kubebrain-archive-verifier-enable-"`)
	require.Contains(t, log, `"path":"/spec/suspend","value":false`)
	require.Contains(t, log, `iam-simulation-valid-until-unix`)
	out, err = runProductionCommand(t, "bash", []string{"apply-operation-archive-verifier.sh", "--check-enabled"}, f.env())
	require.NoError(t, err, string(out))
}

func TestOperationArchiveVerifierManualFailureKeepsScheduleSuspended(t *testing.T) {
	f := newArchiveVerifierApplyFixture(t)
	out, err := runProductionCommand(t, "bash", []string{"apply-operation-archive-verifier.sh", "--enable"}, append(f.env(), "FAIL_JOB=true"))
	require.Error(t, err)
	require.Contains(t, string(out), "CronJob IAM binding was not changed")
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

func TestOperationArchiveVerifierCheckEnabledRejectsExpiredRuntimeBinding(t *testing.T) {
	f := newArchiveVerifierApplyFixture(t)
	require.NoError(t, os.WriteFile(f.state, []byte("expired"), 0o600))
	out, err := runProductionCommand(t, "bash", []string{"apply-operation-archive-verifier.sh", "--check-enabled"}, f.env())
	require.Error(t, err)
	require.Contains(t, string(out), "expired or its runtime binding drifted")
}

func TestOperationArchiveVerifierRefreshesEnabledIAMBindingAfterManualSuccess(t *testing.T) {
	f := newArchiveVerifierApplyFixture(t)
	require.NoError(t, os.WriteFile(f.state, []byte("enabled"), 0o600))
	out, err := runProductionCommand(t, "bash", []string{"apply-operation-archive-verifier.sh", "--refresh-iam"}, append(f.env(), "REFRESH_OPERATION_ARCHIVE_VERIFIER_IAM=yes"))
	require.NoError(t, err, string(out))
	require.Contains(t, string(out), "refreshed hourly verifier IAM evidence binding")
	log := string(mustRead(t, f.log))
	require.Contains(t, log, "create -f - -o json")
	require.Contains(t, string(mustRead(t, f.payloadLog)), `"generateName":"kubebrain-archive-verifier-iam-"`)
	require.Contains(t, log, `"path":"/spec/suspend","value":false`)
	require.Contains(t, log, `"op":"test","path":"/metadata/annotations/dbaas.kubebrain.io~1iam-simulation-valid-until-unix"`)
}

func TestOperationArchiveVerifierRefreshFailurePreservesOldBinding(t *testing.T) {
	f := newArchiveVerifierApplyFixture(t)
	require.NoError(t, os.WriteFile(f.state, []byte("enabled"), 0o600))
	out, err := runProductionCommand(t, "bash", []string{"apply-operation-archive-verifier.sh", "--refresh-iam"}, append(f.env(), "REFRESH_OPERATION_ARCHIVE_VERIFIER_IAM=yes", "FAIL_JOB=true"))
	require.Error(t, err)
	require.Contains(t, string(out), "IAM binding was not changed")
	require.NotContains(t, string(mustRead(t, f.log)), "patch cronjob")
}

func TestOperationArchiveVerifierAcceptsExplicitAuditJobName(t *testing.T) {
	f := newArchiveVerifierApplyFixture(t)
	out, err := runProductionCommand(t, "bash", []string{"apply-operation-archive-verifier.sh", "--enable"}, append(f.env(), "VERIFICATION_JOB_NAME=audit-verifier-20260822"))
	require.NoError(t, err, string(out))
	require.Contains(t, string(out), "retained job/audit-verifier-20260822")
	payload := string(mustRead(t, f.payloadLog))
	require.Contains(t, payload, `"name":"audit-verifier-20260822"`)
	require.NotContains(t, payload, `"generateName"`)
}

func TestOperationArchiveVerifierRejectsUnexpectedCreatedJobIdentity(t *testing.T) {
	for _, tc := range []struct {
		name string
		env  string
	}{
		{name: "namespace", env: "RETURN_JOB_NAMESPACE=other"},
		{name: "generated prefix", env: "RETURN_JOB_NAME=untrusted-job"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newArchiveVerifierApplyFixture(t)
			out, err := runProductionCommand(t, "bash", []string{"apply-operation-archive-verifier.sh", "--enable"}, append(f.env(), tc.env))
			require.Error(t, err)
			require.Contains(t, string(out), "manual verifier Job")
			log := string(mustRead(t, f.log))
			require.NotContains(t, log, " wait ")
			require.NotContains(t, log, "patch cronjob")
		})
	}
}

type archiveVerifierApplyFixture struct{ kubectl, log, payloadLog, state, evidence string }

func newArchiveVerifierApplyFixture(t *testing.T) archiveVerifierApplyFixture {
	t.Helper()
	dir := t.TempDir()
	f := archiveVerifierApplyFixture{kubectl: filepath.Join(dir, "kubectl"), log: filepath.Join(dir, "calls.log"), payloadLog: filepath.Join(dir, "payloads.log"), state: filepath.Join(dir, "enabled"), evidence: filepath.Join(dir, "iam.json")}
	require.NoError(t, os.WriteFile(f.payloadLog, nil, 0o600))
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
  suspend=true; binding=pending; expiry=pending; runtime_expiry=1
  if [[ -f "$STATE_FILE" ]]; then
    suspend=false; binding="$(sha256sum "$IAM_SIMULATION_EVIDENCE" | awk '{print $1}')"
    expiry="$(jq -r .valid_until_unix "$IAM_SIMULATION_EVIDENCE")"; runtime_expiry="$expiry"
    [[ "$(<"$STATE_FILE")" != expired ]] || { expiry=1; runtime_expiry=1; }
  fi
  printf '{"metadata":{"annotations":{"dbaas.kubebrain.io/iam-simulation-sha256":"%s","dbaas.kubebrain.io/iam-simulation-valid-until-unix":"%s"},"resourceVersion":"7"},"spec":{"concurrencyPolicy":"Forbid","suspend":%s,"jobTemplate":{"spec":{"backoffLimit":0,"template":{"spec":{"restartPolicy":"Never","serviceAccountName":"kubebrain-operation-archive-verifier","containers":[{"env":[{"name":"S3_ENDPOINT"},{"name":"AWS_REGION"},{"name":"AWS_ACCESS_KEY_ID"},{"name":"AWS_SECRET_ACCESS_KEY"},{"name":"S3_FORCE_PATH_STYLE"},{"name":"OBJECT_STORE_ID"},{"name":"S3_BUCKET"},{"name":"IAM_SIMULATION_VALID_UNTIL_UNIX","value":"%s"}]}]}}}}}}\n' "$binding" "$expiry" "$suspend" "$runtime_expiry"; exit 0
fi
if [[ "$args" == *" wait "* ]]; then [[ "${FAIL_JOB:-false}" != true ]]; exit; fi
if [[ "$args" == *" logs "* ]]; then echo 'verified 2 released terminal operation archives'; exit 0; fi
if [[ "$args" == *" patch cronjob "* ]]; then printf x >"$STATE_FILE"; exit 0; fi
if [[ "$args" == *" create -f - -o json "* ]]; then
  payload="$(cat)"; grep -q 'IAM_SIMULATION_VALID_UNTIL_UNIX' <<<"$payload"; printf '%s\n' "$payload" >>"$CREATE_PAYLOAD_LOG"
  name="$(jq -r '.metadata.name // (.metadata.generateName + "abcde")' <<<"$payload")"
  namespace="$(jq -r '.metadata.namespace' <<<"$payload")"
  name="${RETURN_JOB_NAME:-$name}"; namespace="${RETURN_JOB_NAMESPACE:-$namespace}"
  printf '{"metadata":{"name":"%s","namespace":"%s"}}\n' "$name" "$namespace"; exit 0
fi
exit 1
`)
	return f
}

func (f archiveVerifierApplyFixture) env() []string {
	return []string{"KUBE_CONTEXT=production", "KUBECTL=" + f.kubectl, "CALL_LOG=" + f.log, "CREATE_PAYLOAD_LOG=" + f.payloadLog, "STATE_FILE=" + f.state, "IAM_SIMULATION_EVIDENCE=" + f.evidence, "ENABLE_OPERATION_ARCHIVE_VERIFIER=yes"}
}

func iamEvidenceJSON(checked int64) string {
	decisions := `[{"action":"s3:DeleteObject","decision":"denied","resource":"object"},{"action":"s3:GetBucketVersioning","decision":"allowed","resource":"bucket"},{"action":"s3:GetObject","decision":"allowed","resource":"object"},{"action":"s3:GetObjectLockConfiguration","decision":"allowed","resource":"bucket"},{"action":"s3:GetObjectRetention","decision":"allowed","resource":"object"},{"action":"s3:ListBucket","decision":"denied","resource":"bucket"},{"action":"s3:ListBucketVersions","decision":"denied","resource":"bucket"},{"action":"s3:PutObject","decision":"denied","resource":"object"}]`
	return fmt.Sprintf(`{"bucket":"audit-bucket","checked_at_unix":%d,"decisions":%s,"format":"kubebrain.object-store-iam-simulation.v1","object_store_id":"store-a","principal":"arn:aws:iam::123456789012:role/verifier","provider":"aws-s3","valid_until_unix":%d}`+"\n", checked, decisions, checked+7200)
}
