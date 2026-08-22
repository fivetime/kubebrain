package production_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestOperationArchiverObjectLockDrillRecoversReceiptAndReleases(t *testing.T) {
	fixture := newOperationArchiverObjectLockDrillFixture(t)
	out, err := runProductionCommand(t, "bash", []string{"drill-operation-archiver-object-lock.sh"}, fixture.env())
	require.NoError(t, err, string(out))
	require.Contains(t, string(out), "byte-identical receipt-loss recovery")
	require.Equal(t, string(mustRead(t, filepath.Join(fixture.evidence, "receipt-first.json"))),
		string(mustRead(t, filepath.Join(fixture.evidence, "receipt-recovered.json"))))
	require.FileExists(t, fixture.state)
	require.Contains(t, string(mustRead(t, fixture.log)), "checker --check-enabled")
}

func TestOperationArchiverObjectLockDrillRejectsRecoveryDriftBeforeRelease(t *testing.T) {
	fixture := newOperationArchiverObjectLockDrillFixture(t)
	env := append(fixture.env(), "DRIFT_SECOND_RECEIPT=true")
	out, err := runProductionCommand(t, "bash", []string{"drill-operation-archiver-object-lock.sh"}, env)
	require.Error(t, err)
	require.Contains(t, string(out), "byte-identical canonical evidence")
	_, statErr := os.Stat(fixture.state)
	require.ErrorIs(t, statErr, os.ErrNotExist)
}

func TestOperationArchiverObjectLockDrillRequiresExplicitConfirmation(t *testing.T) {
	out, err := runProductionCommand(t, "bash", []string{"drill-operation-archiver-object-lock.sh"}, nil)
	require.Error(t, err)
	require.Contains(t, string(out), "CONFIRM_OPERATION_ARCHIVE_DRILL=yes")
}

type operationArchiverObjectLockDrillFixture struct {
	dir, evidence, kubeconfig, checker, kubectl, audit, object, state, log, count string
}

func newOperationArchiverObjectLockDrillFixture(t *testing.T) operationArchiverObjectLockDrillFixture {
	t.Helper()
	dir := t.TempDir()
	fixture := operationArchiverObjectLockDrillFixture{
		dir: dir, evidence: filepath.Join(dir, "evidence"), kubeconfig: filepath.Join(dir, "archiver.kubeconfig"),
		checker: filepath.Join(dir, "checker"), kubectl: filepath.Join(dir, "kubectl"),
		audit: filepath.Join(dir, "audit"), object: filepath.Join(dir, "object"),
		state: filepath.Join(dir, "released.json"), log: filepath.Join(dir, "calls.log"), count: filepath.Join(dir, "count"),
	}
	require.NoError(t, os.Mkdir(fixture.evidence, 0o700))
	require.NoError(t, os.WriteFile(fixture.kubeconfig, []byte("kubeconfig"), 0o600))
	writeTrafficExecutable(t, fixture.checker, "#!/usr/bin/env bash\nprintf 'checker %s\\n' \"$*\" >>\"$CALL_LOG\"\n[[ \"$1\" == --check-enabled ]]\n")
	writeTrafficExecutable(t, fixture.kubectl, `#!/usr/bin/env bash
set -euo pipefail
printf 'kubectl %s\n' "$*" >>"$CALL_LOG"
if [[ -f "$STATE_FILE" ]]; then cat "$STATE_FILE"; exit 0; fi
printf '{"metadata":{"uid":"uid-a","finalizers":["dbaas.kubebrain.io/operation-audit"]},"status":{"phase":"Succeeded","completedAtUnix":2000000000}}\n'
`)
	writeTrafficExecutable(t, fixture.object, `#!/usr/bin/env bash
set -euo pipefail
count=0; [[ ! -f "$COUNT_FILE" ]] || count="$(cat "$COUNT_FILE")"; count=$((count + 1)); printf '%s' "$count" >"$COUNT_FILE"
version=version-1
if [[ "${DRIFT_SECOND_RECEIPT:-false}" == true && "$count" == 2 ]]; then version=version-2; fi
printf '{"format":"kubebrain.object-operation-audit.receipt.v1","operation_id":"op-a","operation_uid":"uid-a","instance":"instance-a","operation_type":"Backup","phase":"Succeeded","object_store_id":"store-a","bucket":"audit-bucket","object_key":"operation-audit/kubebrain-operations/uid-a.json","version_id":"%s","artifact_sha256":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","object_bytes":100,"retention_mode":"COMPLIANCE","retain_until_unix":%s,"remote_verified":true,"archived_at_unix":2000000100}\n' "$version" "$RETAIN_UNTIL_UNIX" >"$RECEIPT_OUTPUT"
chmod 600 "$RECEIPT_OUTPUT"
`)
	writeTrafficExecutable(t, fixture.audit, `#!/usr/bin/env bash
set -euo pipefail
action=capture; output=""; receipt=""
while [[ "$#" -gt 0 ]]; do
  case "$1" in
    --action) action="$2"; shift 2 ;;
    --output) output="$2"; shift 2 ;;
    --receipt) receipt="$2"; shift 2 ;;
    *) shift ;;
  esac
done
if [[ "$action" == capture ]]; then
  printf '{"format":"kubebrain.operation-audit.v1"}\n' >"$output"; chmod 600 "$output"; exit 0
fi
receipt_sha="$(sha256sum "$receipt" | awk '{print $1}')"
artifact_sha="$(jq -r .artifact_sha256 "$receipt")"
version="$(jq -r .version_id "$receipt")"
printf '{"metadata":{"uid":"uid-a","annotations":{"dbaas.kubebrain.io/audit-receipt-sha256":"%s","dbaas.kubebrain.io/audit-artifact-sha256":"%s","dbaas.kubebrain.io/audit-version-id":"%s"}},"status":{"phase":"Succeeded","completedAtUnix":2000000000}}\n' "$receipt_sha" "$artifact_sha" "$version" >"$STATE_FILE"
`)
	return fixture
}

func (f operationArchiverObjectLockDrillFixture) env() []string {
	return []string{
		"OPERATION_NAMESPACE=kubebrain-operations", "OPERATION_NAME=operation-a", "EXPECTED_OPERATION_UID=uid-a",
		"KUBECONFIG_PATH=" + f.kubeconfig, "KUBE_CONTEXT=production", "EVIDENCE_DIR=" + f.evidence,
		"OBJECT_STORE_ID=store-a", "S3_BUCKET=audit-bucket", "RETENTION_MODE=COMPLIANCE",
		"CONFIRM_OPERATION_ARCHIVE_DRILL=yes", "KUBECTL=" + f.kubectl, "OPERATION_AUDIT=" + f.audit,
		"LOGICAL_OBJECT=" + f.object, "ARCHIVER_CHECKER=" + f.checker, "STATE_FILE=" + f.state,
		"CALL_LOG=" + f.log, "COUNT_FILE=" + f.count,
	}
}
