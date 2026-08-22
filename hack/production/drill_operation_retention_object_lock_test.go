package production_test

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestOperationRetentionObjectLockDrillUsesDedicatedVerifierAndDeletesExactObject(t *testing.T) {
	f := newOperationRetentionDrillFixture(t)
	out, err := runProductionCommand(t, "bash", []string{"drill-operation-retention-object-lock.sh"}, f.env())
	require.NoError(t, err, string(out))
	require.Contains(t, string(out), "dedicated-verifier Object Lock revalidation")
	require.FileExists(t, f.deleted)
	log := string(mustRead(t, f.log))
	require.Contains(t, log, "checker --check-enabled")
	require.Contains(t, log, "audit --action reap")
	require.Contains(t, log, "--expected-uid uid-a --expected-resource-version 7")
}

func TestOperationRetentionObjectLockDrillRejectsWrongIdentityBeforeChecker(t *testing.T) {
	f := newOperationRetentionDrillFixture(t)
	out, err := runProductionCommand(t, "bash", []string{"drill-operation-retention-object-lock.sh"}, append(f.env(), "WHOAMI=cluster-admin"))
	require.Error(t, err)
	require.Contains(t, string(out), "dedicated Operation archive verifier")
	require.NotContains(t, string(mustRead(t, f.log)), "checker")
	require.NoFileExists(t, f.deleted)
}

func TestOperationRetentionObjectLockDrillRejectsYoungOrDriftedObjectBeforeDelete(t *testing.T) {
	for _, tc := range []struct {
		name string
		env  []string
	}{
		{name: "young", env: []string{fmt.Sprintf("COMPLETED_AT=%d", time.Now().Unix())}},
		{name: "resource version", env: []string{"OBJECT_RV=8"}},
		{name: "finalizer", env: []string{"FINALIZERS=[\"unexpected\"]"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newOperationRetentionDrillFixture(t)
			out, err := runProductionCommand(t, "bash", []string{"drill-operation-retention-object-lock.sh"}, append(f.env(), tc.env...))
			require.Error(t, err)
			require.Contains(t, string(out), "not the exact expired released archive")
			require.NoFileExists(t, f.deleted)
		})
	}
}

func TestOperationRetentionObjectLockDrillPreservesObjectWhenRemoteVerificationFails(t *testing.T) {
	f := newOperationRetentionDrillFixture(t)
	out, err := runProductionCommand(t, "bash", []string{"drill-operation-retention-object-lock.sh"}, append(f.env(), "FAIL_REAP=true"))
	require.Error(t, err)
	require.Contains(t, string(out), "retention delete failed")
	require.NoFileExists(t, f.deleted)
}

func TestOperationRetentionObjectLockDrillRequiresExplicitConfirmation(t *testing.T) {
	out, err := runProductionCommand(t, "bash", []string{"drill-operation-retention-object-lock.sh"}, nil)
	require.Error(t, err)
	require.Contains(t, string(out), "CONFIRM_OPERATION_RETENTION_REAP=yes")
}

type operationRetentionDrillFixture struct {
	dir, kubeconfig, controlKubeconfig, checker, kubectl, audit, object, deleted, log string
}

func newOperationRetentionDrillFixture(t *testing.T) operationRetentionDrillFixture {
	t.Helper()
	dir := t.TempDir()
	f := operationRetentionDrillFixture{
		dir: dir, kubeconfig: filepath.Join(dir, "verifier.kubeconfig"), controlKubeconfig: filepath.Join(dir, "control.kubeconfig"),
		checker: filepath.Join(dir, "checker"), kubectl: filepath.Join(dir, "kubectl"), audit: filepath.Join(dir, "audit"),
		object: filepath.Join(dir, "object"), deleted: filepath.Join(dir, "deleted"), log: filepath.Join(dir, "calls.log"),
	}
	require.NoError(t, os.WriteFile(f.kubeconfig, []byte("verifier"), 0o600))
	require.NoError(t, os.WriteFile(f.controlKubeconfig, []byte("control"), 0o600))
	writeTrafficExecutable(t, f.checker, "#!/usr/bin/env bash\nprintf 'checker %s\\n' \"$*\" >>\"$CALL_LOG\"\n[[ \"$1\" == --check-enabled ]]\n")
	writeTrafficExecutable(t, f.kubectl, `#!/usr/bin/env bash
set -euo pipefail
printf 'kubectl %s\n' "$*" >>"$CALL_LOG"
if [[ "$*" == *" auth whoami "* ]]; then
  printf '{"status":{"userInfo":{"username":"%s","groups":["system:serviceaccounts","system:authenticated"]}}}\n' "${WHOAMI:-system:serviceaccount:kubebrain-operations:kubebrain-operation-archive-verifier}"
  exit 0
fi
if [[ "$*" == *"--ignore-not-found"* && -f "$DELETED_FILE" ]]; then exit 0; fi
printf '{"metadata":{"uid":"uid-a","resourceVersion":"%s","finalizers":%s,"annotations":{"dbaas.kubebrain.io/audit-receipt-sha256":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","dbaas.kubebrain.io/audit-artifact-sha256":"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb","dbaas.kubebrain.io/audit-version-id":"version-1"}},"status":{"phase":"Succeeded","completedAtUnix":%s}}\n' "${OBJECT_RV:-7}" "${FINALIZERS:-[]}" "${COMPLETED_AT}"
`)
	writeTrafficExecutable(t, f.audit, `#!/usr/bin/env bash
set -euo pipefail
printf 'audit %s\n' "$*" >>"$CALL_LOG"
[[ "$*" == *"--action reap"* ]] || exit 1
[[ "${FAIL_REAP:-false}" != true ]] || exit 1
touch "$DELETED_FILE"
`)
	writeTrafficExecutable(t, f.object, "#!/usr/bin/env bash\nexit 0\n")
	return f
}

func (f operationRetentionDrillFixture) env() []string {
	return []string{
		"OPERATION_NAMESPACE=kubebrain-operations", "OPERATION_NAME=operation-a", "EXPECTED_OPERATION_UID=uid-a", "EXPECTED_OPERATION_RESOURCE_VERSION=7",
		"KUBECONFIG_PATH=" + f.kubeconfig, "CONTROL_KUBECONFIG_PATH=" + f.controlKubeconfig, "KUBE_CONTEXT=verifier", "CONTROL_KUBE_CONTEXT=production",
		"OBJECT_STORE_ID=store-a", "S3_BUCKET=audit-bucket", "RETENTION_MODE=COMPLIANCE", "CONFIRM_OPERATION_RETENTION_REAP=yes",
		"KUBECTL=" + f.kubectl, "JQ=jq", "OPERATION_AUDIT=" + f.audit, "LOGICAL_OBJECT=" + f.object, "VERIFIER_CHECKER=" + f.checker,
		"DELETED_FILE=" + f.deleted, "CALL_LOG=" + f.log, fmt.Sprintf("COMPLETED_AT=%d", time.Now().Add(-31*24*time.Hour).Unix()),
	}
}
