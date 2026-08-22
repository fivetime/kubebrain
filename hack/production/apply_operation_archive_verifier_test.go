package production_test

import (
	"crypto/ed25519"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"os"
	"os/exec"
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
	evidenceSHA := sha256.Sum256(mustRead(t, f.evidence))
	require.Contains(t, string(mustRead(t, f.payloadLog)), fmt.Sprintf(`"dbaas.kubebrain.io/iam-simulation-sha256":"%x"`, evidenceSHA))
	require.Contains(t, string(mustRead(t, f.payloadLog)), `"dbaas.kubebrain.io/credential-secret-uid":"secret-uid-123"`)
	require.Contains(t, string(mustRead(t, f.payloadLog)), `"dbaas.kubebrain.io/iam-simulation-signature-sha256":"`)
	require.Contains(t, string(mustRead(t, f.payloadLog)), `"dbaas.kubebrain.io/iam-simulation-trust-public-key-sha256":"`)
	require.Contains(t, log, `"path":"/spec/suspend","value":false`)
	require.Contains(t, log, `iam-simulation-valid-until-unix`)
	require.Contains(t, log, `credential-secret-resource-version`)
	require.Contains(t, log, `"path":"/spec/jobTemplate/metadata/annotations/dbaas.kubebrain.io~1credential-secret-uid"`)
	require.Contains(t, log, `"path":"/spec/jobTemplate/spec/template/metadata/annotations/dbaas.kubebrain.io~1credential-secret-uid"`)
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

func TestOperationArchiveVerifierRejectsForgedIAMEvidenceBeforeJob(t *testing.T) {
	f := newArchiveVerifierApplyFixture(t)
	forged := strings.Replace(string(mustRead(t, f.evidence)), "role/verifier", "role/forgedxx", 1)
	require.NoError(t, os.WriteFile(f.evidence, []byte(forged), 0o600))
	out, err := runProductionCommand(t, "bash", []string{"apply-operation-archive-verifier.sh", "--enable"}, f.env())
	require.Error(t, err)
	require.Contains(t, string(out), "evidence signature is invalid")
	require.NotContains(t, string(mustRead(t, f.log)), "create -f")
}

func TestOperationArchiveVerifierRejectsUnsafeEvidenceSignature(t *testing.T) {
	f := newArchiveVerifierApplyFixture(t)
	require.NoError(t, os.WriteFile(f.signature, make([]byte, 63), 0o600))
	out, err := runProductionCommand(t, "bash", []string{"apply-operation-archive-verifier.sh", "--enable"}, f.env())
	require.Error(t, err)
	require.Contains(t, string(out), "exactly 64 bytes")
	require.NotContains(t, string(mustRead(t, f.log)), "create -f")
}

func TestOperationArchiveVerifierRejectsUnpinnedIAMTrustRoot(t *testing.T) {
	f := newArchiveVerifierApplyFixture(t)
	out, err := runProductionCommand(t, "bash", []string{"apply-operation-archive-verifier.sh", "--enable"}, append(f.env(), "CRON_TRUST_SHA=ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"))
	require.Error(t, err)
	require.Contains(t, string(out), "does not match the pinned SHA-256")
	require.NotContains(t, string(mustRead(t, f.log)), "create -f")
}

func TestOperationArchiveVerifierRejectsNonEd25519IAMTrustRoot(t *testing.T) {
	f := newArchiveVerifierApplyFixture(t)
	privateKey, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)
	publicDER, err := x509.MarshalPKIXPublicKey(&privateKey.PublicKey)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(f.publicKey, pem.EncodeToMemory(&pem.Block{Type: "PUBLIC KEY", Bytes: publicDER}), 0o600))
	out, err := runProductionCommand(t, "bash", []string{"apply-operation-archive-verifier.sh", "--enable"}, f.env())
	require.Error(t, err)
	require.Contains(t, string(out), "must be Ed25519")
	require.NotContains(t, string(mustRead(t, f.log)), "create -f")
}

func TestOperationArchiveVerifierRejectsEvidenceChangedDuringSignatureVerification(t *testing.T) {
	f := newArchiveVerifierApplyFixture(t)
	realOpenSSL, err := exec.LookPath("openssl")
	require.NoError(t, err)
	wrapper := filepath.Join(t.TempDir(), "openssl")
	writeTrafficExecutable(t, wrapper, `#!/usr/bin/env bash
set -euo pipefail
"$REAL_OPENSSL" "$@"
if [[ " $* " == *" pkeyutl -verify "* ]]; then printf x >>"$IAM_SIMULATION_EVIDENCE"; fi
`)
	out, err := runProductionCommand(t, "bash", []string{"apply-operation-archive-verifier.sh", "--enable"}, append(f.env(), "OPENSSL="+wrapper, "REAL_OPENSSL="+realOpenSSL))
	require.Error(t, err)
	require.Contains(t, string(out), "evidence changed during signature verification")
	require.NotContains(t, string(mustRead(t, f.log)), "create -f")
}

func TestOperationArchiveVerifierRejectsIAMScopeAndDecisionDriftBeforeJob(t *testing.T) {
	secretSHA := fmt.Sprintf("%x", sha256.Sum256([]byte(verifierSecretDataJSON)))
	for _, tc := range []struct{ name, old, replacement string }{
		{name: "bucket", old: `"bucket":"audit-bucket"`, replacement: `"bucket":"other-bucket"`},
		{name: "put allowed", old: `"action":"s3:PutObject","decision":"denied"`, replacement: `"action":"s3:PutObject","decision":"allowed"`},
		{name: "credential uid", old: `"credential_secret_uid":"secret-uid-123"`, replacement: `"credential_secret_uid":"other-uid"`},
		{name: "credential digest", old: `"credential_secret_data_sha256":"` + secretSHA + `"`, replacement: `"credential_secret_data_sha256":"ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"`},
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

func TestOperationArchiveVerifierRequiresImmutableCredentialSecret(t *testing.T) {
	f := newArchiveVerifierApplyFixture(t)
	out, err := runProductionCommand(t, "bash", []string{"apply-operation-archive-verifier.sh", "--enable"}, append(f.env(), "SECRET_IMMUTABLE=false"))
	require.Error(t, err)
	require.Contains(t, string(out), "Secret is incomplete or unsafe")
	require.NotContains(t, string(mustRead(t, f.log)), "create -f")
}

func TestOperationArchiveVerifierRejectsCredentialSecretReplacement(t *testing.T) {
	for _, at := range []string{"2", "3"} {
		t.Run("read-"+at, func(t *testing.T) {
			f := newArchiveVerifierApplyFixture(t)
			out, err := runProductionCommand(t, "bash", []string{"apply-operation-archive-verifier.sh", "--enable"}, append(f.env(), "SECRET_DRIFT_AT="+at))
			require.Error(t, err)
			require.Contains(t, string(out), "Secret")
			require.NotContains(t, string(mustRead(t, f.log)), "patch cronjob")
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

func TestOperationArchiveVerifierCheckEnabledRejectsScheduledCredentialBindingDrift(t *testing.T) {
	for _, tc := range []struct {
		name string
		env  string
	}{
		{name: "cronjob", env: "CRON_CREDENTIAL_UID=other-uid"},
		{name: "job template", env: "CRON_JOB_CREDENTIAL_UID=other-uid"},
		{name: "pod template", env: "CRON_POD_CREDENTIAL_UID=other-uid"},
		{name: "runtime digest", env: "CRON_RUNTIME_CREDENTIAL_SHA=ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"},
		{name: "job signature", env: "CRON_JOB_SIGNATURE_SHA=ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"},
		{name: "pod trust", env: "CRON_POD_TRUST_SHA=ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newArchiveVerifierApplyFixture(t)
			require.NoError(t, os.WriteFile(f.state, []byte("enabled"), 0o600))
			out, err := runProductionCommand(t, "bash", []string{"apply-operation-archive-verifier.sh", "--check-enabled"}, append(f.env(), tc.env))
			require.Error(t, err)
			require.Contains(t, string(out), "binding drifted")
		})
	}
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
	require.Contains(t, log, `"op":"test","path":"/metadata/annotations/dbaas.kubebrain.io~1credential-secret-uid"`)
	require.Contains(t, log, `"path":"/spec/jobTemplate/metadata/annotations/dbaas.kubebrain.io~1credential-secret-uid"`)
	require.Contains(t, log, `"path":"/spec/jobTemplate/spec/template/metadata/annotations/dbaas.kubebrain.io~1credential-secret-uid"`)
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
		{name: "evidence sha", env: "RETURN_JOB_SHA=ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"},
		{name: "evidence expiry", env: "RETURN_JOB_EXPIRY=1"},
		{name: "missing annotations", env: "RETURN_JOB_ANNOTATIONS=omit"},
		{name: "credential uid", env: "RETURN_CREDENTIAL_UID=other-uid"},
		{name: "signature sha", env: "RETURN_SIGNATURE_SHA=ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"},
		{name: "trust sha", env: "RETURN_TRUST_SHA=ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"},
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

func TestOperationArchiveVerifierRejectsCompletedJobDriftBeforePatch(t *testing.T) {
	for _, tc := range []struct {
		name string
		env  string
	}{
		{name: "uid", env: "FINAL_JOB_UID=other-uid"},
		{name: "sha", env: "FINAL_JOB_SHA=ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"},
		{name: "expiry", env: "FINAL_JOB_EXPIRY=1"},
		{name: "runtime expiry", env: "FINAL_RUNTIME_EXPIRY=1"},
		{name: "runtime credential digest", env: "FINAL_RUNTIME_CREDENTIAL_SHA=ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"},
		{name: "condition", env: "FINAL_JOB_COMPLETE=false"},
		{name: "missing", env: "FINAL_JOB_MISSING=true"},
		{name: "credential uid", env: "FINAL_JOB_CREDENTIAL_UID=other-uid"},
		{name: "signature sha", env: "FINAL_JOB_SIGNATURE_SHA=ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"},
		{name: "trust sha", env: "FINAL_JOB_TRUST_SHA=ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newArchiveVerifierApplyFixture(t)
			out, err := runProductionCommand(t, "bash", []string{"apply-operation-archive-verifier.sh", "--enable"}, append(f.env(), tc.env))
			require.Error(t, err)
			require.NotContains(t, string(mustRead(t, f.log)), "patch cronjob")
			if tc.name == "missing" {
				require.Contains(t, string(out), "cannot re-read completed")
			} else {
				require.Contains(t, string(out), "completed manual verifier Job")
			}
		})
	}
}

func TestOperationArchiveVerifierRejectsExecutionPodDriftBeforePatch(t *testing.T) {
	for _, tc := range []struct {
		name string
		env  string
	}{
		{name: "owner", env: "FINAL_POD_OWNER_UID=other-uid"},
		{name: "sha", env: "FINAL_POD_SHA=ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"},
		{name: "expiry", env: "FINAL_POD_EXPIRY=1"},
		{name: "runtime", env: "FINAL_POD_RUNTIME_EXPIRY=1"},
		{name: "runtime credential digest", env: "FINAL_POD_RUNTIME_CREDENTIAL_SHA=ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"},
		{name: "phase", env: "FINAL_POD_PHASE=Failed"},
		{name: "sidecar", env: "FINAL_POD_SIDECAR=true"},
		{name: "init container", env: "FINAL_POD_INIT=true"},
		{name: "ephemeral container", env: "FINAL_POD_EPHEMERAL=true"},
		{name: "image id", env: "FINAL_POD_IMAGE_ID=docker-pullable://registry.example/kubebrain@sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"},
		{name: "exit code", env: "FINAL_POD_EXIT_CODE=1"},
		{name: "restart", env: "FINAL_POD_RESTART_COUNT=1"},
		{name: "missing status", env: "FINAL_POD_STATUS=omit"},
		{name: "missing", env: "FINAL_POD_COUNT=0"},
		{name: "multiple", env: "FINAL_POD_COUNT=2"},
		{name: "credential uid", env: "FINAL_POD_CREDENTIAL_UID=other-uid"},
		{name: "signature sha", env: "FINAL_POD_SIGNATURE_SHA=ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"},
		{name: "trust sha", env: "FINAL_POD_TRUST_SHA=ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newArchiveVerifierApplyFixture(t)
			out, err := runProductionCommand(t, "bash", []string{"apply-operation-archive-verifier.sh", "--enable"}, append(f.env(), tc.env))
			require.Error(t, err)
			require.Contains(t, string(out), "execution Pod")
			log := string(mustRead(t, f.log))
			require.NotContains(t, log, " logs ")
			require.NotContains(t, log, "patch cronjob")
		})
	}
}

func TestOperationArchiveVerifierRejectsMutableLiveImageBeforeJob(t *testing.T) {
	f := newArchiveVerifierApplyFixture(t)
	out, err := runProductionCommand(t, "bash", []string{"apply-operation-archive-verifier.sh", "--enable"}, append(f.env(), "LIVE_IMAGE=registry.example/kubebrain:latest"))
	require.Error(t, err)
	require.Contains(t, string(out), "CronJob runtime contract drifted")
	require.NotContains(t, string(mustRead(t, f.log)), "create -f")
}

func TestOperationArchiveVerifierRejectsDriftedLiveBatchLimitBeforeJob(t *testing.T) {
	f := newArchiveVerifierApplyFixture(t)
	out, err := runProductionCommand(t, "bash", []string{"apply-operation-archive-verifier.sh", "--enable"}, append(f.env(), "LIVE_MAX_BATCH=512"))
	require.Error(t, err)
	require.Contains(t, string(out), "CronJob runtime contract drifted")
	require.NotContains(t, string(mustRead(t, f.log)), "create -f")
}

func TestOperationArchiveVerifierRejectsEmptyOrNonCanonicalSuccessLog(t *testing.T) {
	for _, tc := range []struct {
		name string
		log  string
	}{
		{name: "zero", log: "verified 0 released terminal operation archives"},
		{name: "too many", log: "verified 257 released terminal operation archives"},
		{name: "malformed", log: "verification complete"},
		{name: "multiline", log: "verified 1 released terminal operation archives\nextra"},
		{name: "oversize", log: strings.Repeat("x", 4097)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newArchiveVerifierApplyFixture(t)
			out, err := runProductionCommand(t, "bash", []string{"apply-operation-archive-verifier.sh", "--enable"}, append(f.env(), "LOG_OUTPUT="+tc.log))
			require.Error(t, err)
			require.Contains(t, string(out), "manual verifier Job")
			require.NotContains(t, string(mustRead(t, f.log)), "patch cronjob")
		})
	}
}

type archiveVerifierApplyFixture struct {
	kubectl, log, payloadLog, jobState, secretCount, state, evidence, signature, publicKey string
}

func newArchiveVerifierApplyFixture(t *testing.T) archiveVerifierApplyFixture {
	t.Helper()
	dir := t.TempDir()
	f := archiveVerifierApplyFixture{kubectl: filepath.Join(dir, "kubectl"), log: filepath.Join(dir, "calls.log"), payloadLog: filepath.Join(dir, "payloads.log"), jobState: filepath.Join(dir, "job.json"), secretCount: filepath.Join(dir, "secret-count"), state: filepath.Join(dir, "enabled"), evidence: filepath.Join(dir, "iam.json"), signature: filepath.Join(dir, "iam.sig"), publicKey: filepath.Join(dir, "iam-public.pem")}
	require.NoError(t, os.WriteFile(f.payloadLog, nil, 0o600))
	evidence := []byte(iamEvidenceJSON(time.Now().Unix()))
	publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	publicDER, err := x509.MarshalPKIXPublicKey(publicKey)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(f.publicKey, pem.EncodeToMemory(&pem.Block{Type: "PUBLIC KEY", Bytes: publicDER}), 0o600))
	require.NoError(t, os.WriteFile(f.evidence, evidence, 0o600))
	require.NoError(t, os.WriteFile(f.signature, ed25519.Sign(privateKey, evidence), 0o600))
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
  count=0; [[ ! -f "$SECRET_COUNT_FILE" ]] || count="$(<"$SECRET_COUNT_FILE")"; count=$((count+1)); printf '%s' "$count" >"$SECRET_COUNT_FILE"
  uid=secret-uid-123; rv=11; access=YWNjZXNz
  if [[ "${SECRET_DRIFT_AT:-0}" == "$count" ]]; then uid=secret-uid-replaced; rv=12; access=ZHJpZnRlZA==; fi
  printf '{"metadata":{"name":"kubebrain-operation-archive-verifier-object-store","namespace":"kubebrain-operations","uid":"%s","resourceVersion":"%s"},"immutable":%s,"data":{"access-key-id":"%s","bucket":"YXVkaXQtYnVja2V0","endpoint":"aHR0cHM6Ly9zMy5leGFtcGxl","force-path-style":"ZmFsc2U=","object-store-id":"c3RvcmUtYQ==","region":"dXMtZWFzdC0x","secret-access-key":"c2VjcmV0"}}\n' "$uid" "$rv" "${SECRET_IMMUTABLE:-true}" "$access"; exit 0
fi
if [[ "$args" == *" get cronjob kubebrain-operation-archive-verifier "* ]]; then
  suspend=true; binding=pending; expiry=pending; runtime_expiry=1
  signature_sha=pending; trust_sha="$(sha256sum "$IAM_SIMULATION_TRUSTED_PUBLIC_KEY" | awk '{print $1}')"; template_trust_sha=pending
  trust_sha="${CRON_TRUST_SHA:-$trust_sha}"
  credential_uid=pending; credential_rv=pending; credential_sha=pending
  image="${LIVE_IMAGE:-registry.example/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa}"
  max_batch="${LIVE_MAX_BATCH:-256}"
  if [[ -f "$STATE_FILE" ]]; then
    suspend=false; binding="$(sha256sum "$IAM_SIMULATION_EVIDENCE" | awk '{print $1}')"
    signature_sha="$(sha256sum "$IAM_SIMULATION_EVIDENCE_SIGNATURE" | awk '{print $1}')"; template_trust_sha="$trust_sha"
    expiry="$(jq -r .valid_until_unix "$IAM_SIMULATION_EVIDENCE")"; runtime_expiry="$expiry"
    credential_uid=secret-uid-123; credential_rv=11; credential_sha="$(printf '%s' '`+verifierSecretDataJSON+`' | sha256sum | awk '{print $1}')"
    [[ "$(<"$STATE_FILE")" != expired ]] || { expiry=1; runtime_expiry=1; }
  fi
  credential_uid="${CRON_CREDENTIAL_UID:-$credential_uid}"; credential_rv="${CRON_CREDENTIAL_RV:-$credential_rv}"; credential_sha="${CRON_CREDENTIAL_SHA:-$credential_sha}"
  runtime_credential_sha="${CRON_RUNTIME_CREDENTIAL_SHA:-$credential_sha}"
  annotations="$(jq -cn --arg sha "$binding" --arg signature_sha "$signature_sha" --arg trust_sha "$trust_sha" --arg expiry "$expiry" --arg uid "$credential_uid" --arg rv "$credential_rv" --arg secret_sha "$credential_sha" '{"example.com/managed-by":"fixture","dbaas.kubebrain.io/iam-simulation-sha256":$sha,"dbaas.kubebrain.io/iam-simulation-signature-sha256":$signature_sha,"dbaas.kubebrain.io/iam-simulation-trust-public-key-sha256":$trust_sha,"dbaas.kubebrain.io/iam-simulation-valid-until-unix":$expiry,"dbaas.kubebrain.io/credential-secret-uid":$uid,"dbaas.kubebrain.io/credential-secret-resource-version":$rv,"dbaas.kubebrain.io/credential-secret-data-sha256":$secret_sha}')"
  job_annotations="$(jq -c --arg trust_sha "$template_trust_sha" '.["dbaas.kubebrain.io/iam-simulation-trust-public-key-sha256"]=$trust_sha' <<<"$annotations")"; pod_annotations="$job_annotations"
  [[ -z "${CRON_JOB_CREDENTIAL_UID:-}" ]] || job_annotations="$(jq -c --arg value "$CRON_JOB_CREDENTIAL_UID" '.["dbaas.kubebrain.io/credential-secret-uid"]=$value' <<<"$job_annotations")"
  [[ -z "${CRON_POD_CREDENTIAL_UID:-}" ]] || pod_annotations="$(jq -c --arg value "$CRON_POD_CREDENTIAL_UID" '.["dbaas.kubebrain.io/credential-secret-uid"]=$value' <<<"$pod_annotations")"
  [[ -z "${CRON_JOB_SIGNATURE_SHA:-}" ]] || job_annotations="$(jq -c --arg value "$CRON_JOB_SIGNATURE_SHA" '.["dbaas.kubebrain.io/iam-simulation-signature-sha256"]=$value' <<<"$job_annotations")"
  [[ -z "${CRON_POD_TRUST_SHA:-}" ]] || pod_annotations="$(jq -c --arg value "$CRON_POD_TRUST_SHA" '.["dbaas.kubebrain.io/iam-simulation-trust-public-key-sha256"]=$value' <<<"$pod_annotations")"
  printf '{"metadata":{"annotations":%s,"resourceVersion":"7"},"spec":{"concurrencyPolicy":"Forbid","suspend":%s,"jobTemplate":{"metadata":{"annotations":%s},"spec":{"backoffLimit":0,"template":{"metadata":{"annotations":%s},"spec":{"restartPolicy":"Never","serviceAccountName":"kubebrain-operation-archive-verifier","containers":[{"name":"verifier","image":"%s","args":["--max-batch=%s"],"env":[{"name":"S3_ENDPOINT"},{"name":"AWS_REGION"},{"name":"AWS_ACCESS_KEY_ID"},{"name":"AWS_SECRET_ACCESS_KEY"},{"name":"S3_FORCE_PATH_STYLE"},{"name":"OBJECT_STORE_ID"},{"name":"S3_BUCKET"},{"name":"IAM_SIMULATION_VALID_UNTIL_UNIX","value":"%s"},{"name":"CREDENTIAL_SECRET_DATA_SHA256","value":"%s"}]}]}}}}}}\n' "$annotations" "$suspend" "$job_annotations" "$pod_annotations" "$image" "$max_batch" "$runtime_expiry" "$runtime_credential_sha"; exit 0
fi
if [[ "$args" == *" get job "* ]]; then
  [[ "${FINAL_JOB_MISSING:-false}" != true && -f "$JOB_STATE_FILE" ]] || exit 1
  job="$(<"$JOB_STATE_FILE")"
  job="$(jq -c '.status.conditions=[{"type":"Complete","status":"True"}]' <<<"$job")"
  if [[ -n "${FINAL_JOB_UID:-}" ]]; then job="$(jq -c --arg value "$FINAL_JOB_UID" '.metadata.uid=$value' <<<"$job")"; fi
  if [[ -n "${FINAL_JOB_SHA:-}" ]]; then job="$(jq -c --arg value "$FINAL_JOB_SHA" '.metadata.annotations["dbaas.kubebrain.io/iam-simulation-sha256"]=$value' <<<"$job")"; fi
  if [[ -n "${FINAL_JOB_EXPIRY:-}" ]]; then job="$(jq -c --arg value "$FINAL_JOB_EXPIRY" '.metadata.annotations["dbaas.kubebrain.io/iam-simulation-valid-until-unix"]=$value' <<<"$job")"; fi
  if [[ -n "${FINAL_JOB_CREDENTIAL_UID:-}" ]]; then job="$(jq -c --arg value "$FINAL_JOB_CREDENTIAL_UID" '.metadata.annotations["dbaas.kubebrain.io/credential-secret-uid"]=$value' <<<"$job")"; fi
  if [[ -n "${FINAL_JOB_SIGNATURE_SHA:-}" ]]; then job="$(jq -c --arg value "$FINAL_JOB_SIGNATURE_SHA" '.metadata.annotations["dbaas.kubebrain.io/iam-simulation-signature-sha256"]=$value' <<<"$job")"; fi
  if [[ -n "${FINAL_JOB_TRUST_SHA:-}" ]]; then job="$(jq -c --arg value "$FINAL_JOB_TRUST_SHA" '.metadata.annotations["dbaas.kubebrain.io/iam-simulation-trust-public-key-sha256"]=$value' <<<"$job")"; fi
  if [[ -n "${FINAL_RUNTIME_EXPIRY:-}" ]]; then job="$(jq -c --arg value "$FINAL_RUNTIME_EXPIRY" '(.spec.template.spec.containers[0].env[] | select(.name=="IAM_SIMULATION_VALID_UNTIL_UNIX") | .value)=$value' <<<"$job")"; fi
  if [[ -n "${FINAL_RUNTIME_CREDENTIAL_SHA:-}" ]]; then job="$(jq -c --arg value "$FINAL_RUNTIME_CREDENTIAL_SHA" '(.spec.template.spec.containers[0].env[] | select(.name=="CREDENTIAL_SECRET_DATA_SHA256") | .value)=$value' <<<"$job")"; fi
  [[ "${FINAL_JOB_COMPLETE:-true}" == true ]] || job="$(jq -c '.status.conditions[0].status="False"' <<<"$job")"
  printf '%s\n' "$job"; exit 0
fi
if [[ "$args" == *" get pods "* ]]; then
  job="$(<"$JOB_STATE_FILE")"; count="${FINAL_POD_COUNT:-1}"
  pod="$(jq -c '{metadata:{namespace:.metadata.namespace,labels:{"batch.kubernetes.io/job-name":.metadata.name},annotations:.spec.template.metadata.annotations,ownerReferences:[{apiVersion:"batch/v1",kind:"Job",name:.metadata.name,uid:.metadata.uid,controller:true}]},spec:{serviceAccountName:.spec.template.spec.serviceAccountName,restartPolicy:.spec.template.spec.restartPolicy,containers:.spec.template.spec.containers},status:{phase:"Succeeded",containerStatuses:[{name:.spec.template.spec.containers[0].name,image:.spec.template.spec.containers[0].image,imageID:("docker-pullable://"+.spec.template.spec.containers[0].image),restartCount:0,state:{terminated:{exitCode:0,reason:"Completed"}}}]}}' <<<"$job")"
  if [[ -n "${FINAL_POD_OWNER_UID:-}" ]]; then pod="$(jq -c --arg value "$FINAL_POD_OWNER_UID" '.metadata.ownerReferences[0].uid=$value' <<<"$pod")"; fi
  if [[ -n "${FINAL_POD_SHA:-}" ]]; then pod="$(jq -c --arg value "$FINAL_POD_SHA" '.metadata.annotations["dbaas.kubebrain.io/iam-simulation-sha256"]=$value' <<<"$pod")"; fi
  if [[ -n "${FINAL_POD_EXPIRY:-}" ]]; then pod="$(jq -c --arg value "$FINAL_POD_EXPIRY" '.metadata.annotations["dbaas.kubebrain.io/iam-simulation-valid-until-unix"]=$value' <<<"$pod")"; fi
  if [[ -n "${FINAL_POD_CREDENTIAL_UID:-}" ]]; then pod="$(jq -c --arg value "$FINAL_POD_CREDENTIAL_UID" '.metadata.annotations["dbaas.kubebrain.io/credential-secret-uid"]=$value' <<<"$pod")"; fi
  if [[ -n "${FINAL_POD_SIGNATURE_SHA:-}" ]]; then pod="$(jq -c --arg value "$FINAL_POD_SIGNATURE_SHA" '.metadata.annotations["dbaas.kubebrain.io/iam-simulation-signature-sha256"]=$value' <<<"$pod")"; fi
  if [[ -n "${FINAL_POD_TRUST_SHA:-}" ]]; then pod="$(jq -c --arg value "$FINAL_POD_TRUST_SHA" '.metadata.annotations["dbaas.kubebrain.io/iam-simulation-trust-public-key-sha256"]=$value' <<<"$pod")"; fi
  if [[ -n "${FINAL_POD_RUNTIME_EXPIRY:-}" ]]; then pod="$(jq -c --arg value "$FINAL_POD_RUNTIME_EXPIRY" '(.spec.containers[0].env[] | select(.name=="IAM_SIMULATION_VALID_UNTIL_UNIX") | .value)=$value' <<<"$pod")"; fi
  if [[ -n "${FINAL_POD_RUNTIME_CREDENTIAL_SHA:-}" ]]; then pod="$(jq -c --arg value "$FINAL_POD_RUNTIME_CREDENTIAL_SHA" '(.spec.containers[0].env[] | select(.name=="CREDENTIAL_SECRET_DATA_SHA256") | .value)=$value' <<<"$pod")"; fi
  if [[ -n "${FINAL_POD_PHASE:-}" ]]; then pod="$(jq -c --arg value "$FINAL_POD_PHASE" '.status.phase=$value' <<<"$pod")"; fi
  [[ "${FINAL_POD_SIDECAR:-false}" != true ]] || pod="$(jq -c '.spec.containers += [{name:"sidecar",image:"untrusted"}]' <<<"$pod")"
  [[ "${FINAL_POD_INIT:-false}" != true ]] || pod="$(jq -c '.spec.initContainers = [{name:"init",image:"untrusted"}]' <<<"$pod")"
  [[ "${FINAL_POD_EPHEMERAL:-false}" != true ]] || pod="$(jq -c '.spec.ephemeralContainers = [{name:"debug",image:"untrusted"}]' <<<"$pod")"
  if [[ -n "${FINAL_POD_IMAGE_ID:-}" ]]; then pod="$(jq -c --arg value "$FINAL_POD_IMAGE_ID" '.status.containerStatuses[0].imageID=$value' <<<"$pod")"; fi
  if [[ -n "${FINAL_POD_EXIT_CODE:-}" ]]; then pod="$(jq -c --argjson value "$FINAL_POD_EXIT_CODE" '.status.containerStatuses[0].state.terminated.exitCode=$value' <<<"$pod")"; fi
  if [[ -n "${FINAL_POD_RESTART_COUNT:-}" ]]; then pod="$(jq -c --argjson value "$FINAL_POD_RESTART_COUNT" '.status.containerStatuses[0].restartCount=$value' <<<"$pod")"; fi
  [[ "${FINAL_POD_STATUS:-}" != omit ]] || pod="$(jq -c 'del(.status.containerStatuses)' <<<"$pod")"
  case "$count" in 0) echo '{"items":[]}' ;; 1) jq -cn --argjson pod "$pod" '{items:[$pod]}' ;; 2) jq -cn --argjson pod "$pod" '{items:[$pod,$pod]}' ;; *) exit 1 ;; esac
  exit 0
fi
if [[ "$args" == *" wait "* ]]; then [[ "${FAIL_JOB:-false}" != true ]]; exit; fi
if [[ "$args" == *" logs "* ]]; then printf '%s\n' "${LOG_OUTPUT:-verified 2 released terminal operation archives}"; exit 0; fi
if [[ "$args" == *" patch cronjob "* ]]; then printf x >"$STATE_FILE"; exit 0; fi
if [[ "$args" == *" create -f - -o json "* ]]; then
  payload="$(cat)"; grep -q 'IAM_SIMULATION_VALID_UNTIL_UNIX' <<<"$payload"; printf '%s\n' "$payload" >>"$CREATE_PAYLOAD_LOG"
  name="$(jq -r '.metadata.name // (.metadata.generateName + "abcde")' <<<"$payload")"
  namespace="$(jq -r '.metadata.namespace' <<<"$payload")"
  annotations="$(jq -c '.metadata.annotations' <<<"$payload")"
  name="${RETURN_JOB_NAME:-$name}"; namespace="${RETURN_JOB_NAMESPACE:-$namespace}"
  if [[ -n "${RETURN_JOB_SHA:-}" ]]; then annotations="$(jq -c --arg value "$RETURN_JOB_SHA" '.["dbaas.kubebrain.io/iam-simulation-sha256"]=$value' <<<"$annotations")"; fi
  if [[ -n "${RETURN_JOB_EXPIRY:-}" ]]; then annotations="$(jq -c --arg value "$RETURN_JOB_EXPIRY" '.["dbaas.kubebrain.io/iam-simulation-valid-until-unix"]=$value' <<<"$annotations")"; fi
  if [[ -n "${RETURN_CREDENTIAL_UID:-}" ]]; then annotations="$(jq -c --arg value "$RETURN_CREDENTIAL_UID" '.["dbaas.kubebrain.io/credential-secret-uid"]=$value' <<<"$annotations")"; fi
  if [[ -n "${RETURN_SIGNATURE_SHA:-}" ]]; then annotations="$(jq -c --arg value "$RETURN_SIGNATURE_SHA" '.["dbaas.kubebrain.io/iam-simulation-signature-sha256"]=$value' <<<"$annotations")"; fi
  if [[ -n "${RETURN_TRUST_SHA:-}" ]]; then annotations="$(jq -c --arg value "$RETURN_TRUST_SHA" '.["dbaas.kubebrain.io/iam-simulation-trust-public-key-sha256"]=$value' <<<"$annotations")"; fi
  [[ "${RETURN_JOB_ANNOTATIONS:-}" != omit ]] || annotations=null
  created="$(jq -c --arg name "$name" --arg namespace "$namespace" --argjson annotations "$annotations" '.metadata.name=$name | .metadata.namespace=$namespace | .metadata.annotations=$annotations | .metadata.uid="verifier-uid-123" | del(.metadata.generateName)' <<<"$payload")"
  printf '%s\n' "$created" >"$JOB_STATE_FILE"; printf '%s\n' "$created"; exit 0
fi
exit 1
`)
	return f
}

func (f archiveVerifierApplyFixture) env() []string {
	return []string{"KUBE_CONTEXT=production", "KUBECTL=" + f.kubectl, "CALL_LOG=" + f.log, "CREATE_PAYLOAD_LOG=" + f.payloadLog, "JOB_STATE_FILE=" + f.jobState, "SECRET_COUNT_FILE=" + f.secretCount, "STATE_FILE=" + f.state, "IAM_SIMULATION_EVIDENCE=" + f.evidence, "IAM_SIMULATION_EVIDENCE_SIGNATURE=" + f.signature, "IAM_SIMULATION_TRUSTED_PUBLIC_KEY=" + f.publicKey, "ENABLE_OPERATION_ARCHIVE_VERIFIER=yes"}
}

func iamEvidenceJSON(checked int64) string {
	decisions := `[{"action":"s3:DeleteObject","decision":"denied","resource":"object"},{"action":"s3:GetBucketVersioning","decision":"allowed","resource":"bucket"},{"action":"s3:GetObject","decision":"allowed","resource":"object"},{"action":"s3:GetObjectLockConfiguration","decision":"allowed","resource":"bucket"},{"action":"s3:GetObjectRetention","decision":"allowed","resource":"object"},{"action":"s3:ListBucket","decision":"denied","resource":"bucket"},{"action":"s3:ListBucketVersions","decision":"denied","resource":"bucket"},{"action":"s3:PutObject","decision":"denied","resource":"object"}]`
	secretSHA := sha256.Sum256([]byte(verifierSecretDataJSON))
	return fmt.Sprintf(`{"bucket":"audit-bucket","checked_at_unix":%d,"credential_secret_data_sha256":"%x","credential_secret_uid":"secret-uid-123","decisions":%s,"format":"kubebrain.object-store-iam-simulation.v2","object_store_id":"store-a","principal":"arn:aws:iam::123456789012:role/verifier","provider":"aws-s3","valid_until_unix":%d}`+"\n", checked, secretSHA, decisions, checked+7200)
}

const verifierSecretDataJSON = `{"access-key-id":"YWNjZXNz","bucket":"YXVkaXQtYnVja2V0","endpoint":"aHR0cHM6Ly9zMy5leGFtcGxl","force-path-style":"ZmFsc2U=","object-store-id":"c3RvcmUtYQ==","region":"dXMtZWFzdC0x","secret-access-key":"c2VjcmV0"}`
