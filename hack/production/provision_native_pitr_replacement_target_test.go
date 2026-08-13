package production_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestProvisionNativePITRReplacementTargetPersistsCreationBeforeReadiness(t *testing.T) {
	dir := t.TempDir()
	log := filepath.Join(dir, "log")
	control := filepath.Join(dir, "control")
	kubectl := filepath.Join(dir, "kubectl")
	inspector := filepath.Join(dir, "inspect")
	writeExecutable(t, control, `#!/usr/bin/env bash
set -euo pipefail
echo "control $*" >>"$TEST_LOG"
mode= output=; for arg; do case "$arg" in --mode=*) mode="${arg#*=}";; --output=*) output="${arg#*=}";; esac; done
case "$mode" in
authorize) printf '%s\n' '{"namespace":"tidb-cluster","tidb_cluster":"kb","old_tidb_cluster_uid":"old-uid","authorization_id":"provision-1"}' >"$output";;
verify-authorization|verify-dry-run|verify-current|verify-completion) ;;
record-dry-run) printf '%s\n' dry-run >"$output";;
record-creation) printf '%s\n' '{"tidb_cluster_uid":"new-uid"}' >"$output";;
*) exit 1;; esac
`)
	writeExecutable(t, kubectl, `#!/usr/bin/env bash
set -euo pipefail
echo "kubectl $*" >>"$TEST_LOG"
case "$*" in
*"get tidbcluster"*) ;;
*"get pvc -l"*) printf '%s' '{"items":[]}' ;;
*"create --dry-run=server"*) printf '%s' '{"dryRun":true}' ;;
*"create -f"*) printf '%s' '{"apiVersion":"pingcap.com/v1alpha1","kind":"TidbCluster","metadata":{"namespace":"tidb-cluster","name":"kb","uid":"new-uid","resourceVersion":"12","labels":{},"annotations":{"dbaas.kubebrain.io/native-pitr-provision-authorization":"provision-1"}},"spec":{}}' ;;
*"wait --for=condition=Ready"*) ;;
*) echo "unexpected kubectl $*" >&2; exit 1;; esac
`)
	writeExecutable(t, inspector, `#!/usr/bin/env bash
set -euo pipefail
echo inspect >>"$TEST_LOG"; printf '%s\n' provisioning >"$OUTPUT"
`)
	for _, name := range []string{"retirement", "old.json", "manifest.json"} {
		require.NoError(t, os.WriteFile(filepath.Join(dir, name), []byte("evidence\n"), 0o600))
	}
	creation := filepath.Join(dir, "creation.json")
	_, err := runProductionScriptCommand(t, "provision-native-pitr-replacement-target.sh", []string{
		"TEST_LOG=" + log, "RETIREMENT=" + filepath.Join(dir, "retirement"), "OLD_PROVISIONING=" + filepath.Join(dir, "old.json"),
		"MANIFEST=" + filepath.Join(dir, "manifest.json"), "AUTHORIZATION_ID=provision-1", "AUTHORIZATION_OUTPUT=" + filepath.Join(dir, "auth.json"),
		"DRY_RUN_OUTPUT=" + filepath.Join(dir, "dry-run.json"), "CREATION_OUTPUT=" + creation, "PROVISIONING_OUTPUT=" + filepath.Join(dir, "new.json"), "KUBE_CONTEXT=in-cluster",
		"CONTROL=" + control, "KUBECTL=" + kubectl, "INSPECT=" + inspector,
	})
	require.NoError(t, err)
	contents := string(mustReadProductionFile(t, log))
	require.Regexp(t, `(?s)create --dry-run=server.*record-dry-run.*create -f.*record-creation.*wait --for=condition=Ready.*inspect.*verify-completion`, contents)
	require.FileExists(t, creation)
}

func TestProvisionNativePITRReplacementTargetFailsClosedOnCollision(t *testing.T) {
	dir := t.TempDir()
	control := filepath.Join(dir, "control")
	kubectl := filepath.Join(dir, "kubectl")
	writeExecutable(t, control, `#!/usr/bin/env bash
set -euo pipefail
for arg; do [[ "$arg" != --output=* ]] || printf '%s\n' '{"namespace":"tidb-cluster","tidb_cluster":"kb","old_tidb_cluster_uid":"old","authorization_id":"provision-1"}' >"${arg#*=}"; done
`)
	writeExecutable(t, kubectl, `#!/usr/bin/env bash
printf '%s' '{"apiVersion":"pingcap.com/v1alpha1","kind":"TidbCluster","metadata":{"name":"kb"}}'
`)
	for _, name := range []string{"retirement", "old.json", "manifest.json"} {
		require.NoError(t, os.WriteFile(filepath.Join(dir, name), []byte("x"), 0o600))
	}
	_, err := runProductionScriptCommand(t, "provision-native-pitr-replacement-target.sh", []string{
		"RETIREMENT=" + filepath.Join(dir, "retirement"), "OLD_PROVISIONING=" + filepath.Join(dir, "old.json"), "MANIFEST=" + filepath.Join(dir, "manifest.json"),
		"AUTHORIZATION_ID=provision-1", "AUTHORIZATION_OUTPUT=" + filepath.Join(dir, "auth"), "DRY_RUN_OUTPUT=" + filepath.Join(dir, "dry-run"), "CREATION_OUTPUT=" + filepath.Join(dir, "creation"),
		"PROVISIONING_OUTPUT=" + filepath.Join(dir, "new"), "KUBE_CONTEXT=in-cluster", "CONTROL=" + control, "KUBECTL=" + kubectl, "INSPECT=/bin/false",
	})
	require.Error(t, err)
	require.NoFileExists(t, filepath.Join(dir, "creation"))
}

func TestProvisionNativePITRReplacementTargetRecoversCreateBeforeReceipt(t *testing.T) {
	dir := t.TempDir()
	log := filepath.Join(dir, "log")
	control := filepath.Join(dir, "control")
	kubectl := filepath.Join(dir, "kubectl")
	inspector := filepath.Join(dir, "inspect")
	writeExecutable(t, control, `#!/usr/bin/env bash
set -euo pipefail
echo "$*" >>"$TEST_LOG"
for arg; do case "$arg" in --output=*) output="${arg#*=}";; esac; done
[[ "$*" != *"--mode=record-creation"* ]] || printf creation >"$output"
`)
	writeExecutable(t, kubectl, `#!/usr/bin/env bash
set -euo pipefail
echo "$*" >>"$TEST_LOG"
if [[ "$*" == *"get tidbcluster"* ]];then
  printf '%s' '{"apiVersion":"pingcap.com/v1alpha1","kind":"TidbCluster","metadata":{"namespace":"tidb-cluster","name":"kb","uid":"new-uid","resourceVersion":"12","annotations":{}},"spec":{}}'
elif [[ "$*" == *"wait --for=condition=Ready"* ]];then :
else exit 1
fi
`)
	writeExecutable(t, inspector, `#!/usr/bin/env bash
printf provisioning >"$OUTPUT"
`)
	for _, name := range []string{"retirement", "old", "manifest", "dry-run"} {
		require.NoError(t, os.WriteFile(filepath.Join(dir, name), []byte(name), 0o600))
	}
	require.NoError(t, os.WriteFile(filepath.Join(dir, "auth"), []byte(`{"namespace":"tidb-cluster","tidb_cluster":"kb","old_tidb_cluster_uid":"old","authorization_id":"provision-1"}`), 0o600))
	_, err := runProductionScriptCommand(t, "provision-native-pitr-replacement-target.sh", []string{
		"TEST_LOG=" + log, "RETIREMENT=" + filepath.Join(dir, "retirement"), "OLD_PROVISIONING=" + filepath.Join(dir, "old"), "MANIFEST=" + filepath.Join(dir, "manifest"),
		"AUTHORIZATION_ID=provision-1", "AUTHORIZATION_OUTPUT=" + filepath.Join(dir, "auth"), "DRY_RUN_OUTPUT=" + filepath.Join(dir, "dry-run"), "CREATION_OUTPUT=" + filepath.Join(dir, "creation"),
		"PROVISIONING_OUTPUT=" + filepath.Join(dir, "provisioning"), "KUBE_CONTEXT=in-cluster", "CONTROL=" + control, "KUBECTL=" + kubectl, "INSPECT=" + inspector,
	})
	require.NoError(t, err)
	contents := string(mustReadProductionFile(t, log))
	require.Contains(t, contents, "--mode=record-creation")
	require.NotContains(t, contents, "create -f")
	require.FileExists(t, filepath.Join(dir, "creation"))
}
