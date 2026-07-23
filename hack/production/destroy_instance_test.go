package production_test

import (
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

const destroyBackupSHA256 = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"

func TestDestroyInstanceLifecycleIsRetrySafe(t *testing.T) {
	fixture := newDestroyFixture(t)
	fixture.run(t, "prepare", true, "")
	fixture.run(t, "prepare", true, "")
	fixture.run(t, "quiesce", true, "")
	fixture.run(t, "destroy", true, "")
	fixture.run(t, "destroy", true, "")
	fixture.run(t, "complete", true, "")
	fixture.run(t, "complete", true, "")

	data, err := os.ReadFile(filepath.Join(fixture.stateDir, "destroy-1.receipt.json"))
	require.NoError(t, err)
	var receipt map[string]any
	require.NoError(t, json.Unmarshal(data, &receipt))
	require.Equal(t, "kubebrain.destroy.receipt.v1", receipt["format"])
	require.Equal(t, true, receipt["resources_absent"])
	require.Equal(t, destroyBackupSHA256, receipt["backup_sha256"])
	require.Equal(t, float64(987654321), receipt["backup_revision"])

	deleteLog, err := os.ReadFile(filepath.Join(fixture.dir, "uid-delete.log"))
	require.NoError(t, err)
	require.Contains(t, string(deleteLog), "--api-version pingcap.com/v1alpha1 --resource tidbclusters")
	require.Contains(t, string(deleteLog), "--resource persistentvolumeclaims")
	require.Contains(t, string(deleteLog), "--uid uid-kb-sts")
}

func TestDestroyInstanceTreatsConcurrentReceiptPublishAsIdempotent(t *testing.T) {
	fixture := newDestroyFixture(t)
	fixture.run(t, "prepare", true, "")
	fixture.run(t, "quiesce", true, "")
	fixture.run(t, "destroy", true, "")
	fixture.run(t, "complete", true, "PUBLISH_DESTROY_RECEIPT_DURING_JQ=true")

	data, err := os.ReadFile(filepath.Join(fixture.stateDir, "destroy-1.receipt.json"))
	require.NoError(t, err)
	var receipt map[string]any
	require.NoError(t, json.Unmarshal(data, &receipt))
	require.Equal(t, "kubebrain.destroy.receipt.v1", receipt["format"])
	require.Equal(t, float64(1), receipt["completed_at_unix"])
	require.Equal(t, destroyBackupSHA256, receipt["backup_sha256"])
}

func TestDestroyInstanceRejectsNestedExistingReceiptFields(t *testing.T) {
	fixture := newDestroyFixture(t)
	fixture.run(t, "prepare", true, "")
	fixture.run(t, "quiesce", true, "")
	fixture.run(t, "destroy", true, "")

	receiptPath := filepath.Join(fixture.stateDir, "destroy-1.receipt.json")
	shadowReceipt := `{"format":"wrong","resources_absent":false,"shadow":{"format":"kubebrain.destroy.receipt.v1","instance":"instance-a","operation_id":"destroy-1","kubebrain_namespace":"instance-a","tidb_namespace":"storage-a","tidb_cluster":"kb","backup_sha256":"` + destroyBackupSHA256 + `","backup_revision":987654321,"resources_absent":true,"completed_at_unix":1}}` + "\n"
	require.NoError(t, os.WriteFile(receiptPath, []byte(shadowReceipt), 0o600))

	fixture.run(t, "complete", false, "", "existing destroy receipt does not match the completed operation")
	data, err := os.ReadFile(receiptPath)
	require.NoError(t, err)
	require.Equal(t, shadowReceipt, string(data))
}

func TestDestroyInstanceFailsClosed(t *testing.T) {
	for _, tc := range []struct {
		name       string
		prepare    func(*testing.T, *destroyFixture)
		action     string
		extraEnv   string
		wantOutput string
	}{
		{
			name:       "quiesce before prepare",
			action:     "quiesce",
			wantOutput: "prepare evidence is missing",
		},
		{
			name: "wrong confirmation",
			prepare: func(t *testing.T, f *destroyFixture) {
				f.run(t, "prepare", true, "")
			},
			action:     "quiesce",
			extraEnv:   "CONFIRM_DESTROY=destroy:wrong",
			wantOutput: "must exactly equal",
		},
		{
			name: "statefulset UID changed",
			prepare: func(t *testing.T, f *destroyFixture) {
				f.run(t, "prepare", true, "")
				require.NoError(t, os.WriteFile(filepath.Join(f.dir, "drift-statefulset-kubebrain"), []byte("uid-new"), 0o600))
			},
			action:     "quiesce",
			wantOutput: "UID fence failed",
		},
		{
			name: "unrecorded PVC appeared",
			prepare: func(t *testing.T, f *destroyFixture) {
				f.run(t, "prepare", true, "")
				require.NoError(t, os.WriteFile(filepath.Join(f.dir, "extra-pvc"), []byte("1"), 0o600))
			},
			action:     "quiesce",
			wantOutput: "unrecorded or replaced instance PVC",
		},
		{
			name: "non canonical state",
			prepare: func(t *testing.T, f *destroyFixture) {
				f.run(t, "prepare", true, "")
				statePath := filepath.Join(f.stateDir, "destroy-1.state")
				require.NoError(t, os.WriteFile(statePath, append(mustRead(t, statePath), []byte("UNKNOWN\trow\n")...), 0o600))
			},
			action:     "quiesce",
			wantOutput: "destroy state has invalid schema",
		},
		{
			name: "non canonical quiesced marker",
			prepare: func(t *testing.T, f *destroyFixture) {
				f.run(t, "prepare", true, "")
				f.run(t, "quiesce", true, "")
				markerPath := filepath.Join(f.stateDir, "destroy-1.quiesced")
				require.NoError(t, os.WriteFile(markerPath, append(mustRead(t, markerPath), []byte("UNKNOWN\trow\n")...), 0o600))
			},
			action:     "destroy",
			wantOutput: "destroy quiesced marker has invalid schema",
		},
		{
			name: "non canonical destroyed marker",
			prepare: func(t *testing.T, f *destroyFixture) {
				f.run(t, "prepare", true, "")
				f.run(t, "quiesce", true, "")
				f.run(t, "destroy", true, "")
				markerPath := filepath.Join(f.stateDir, "destroy-1.destroyed")
				require.NoError(t, os.WriteFile(markerPath, append(mustRead(t, markerPath), []byte("UNKNOWN\trow\n")...), 0o600))
			},
			action:     "complete",
			wantOutput: "destroy destroyed marker has invalid schema",
		},
		{
			name:       "backup completion gate failed",
			action:     "prepare",
			extraEnv:   "FAKE_BACKUP_FAIL=true",
			wantOutput: "backup rejected",
		},
		{
			name:       "backup digest is invalid",
			action:     "prepare",
			extraEnv:   "FAKE_BACKUP_SHA=abc123",
			wantOutput: "logical backup status is invalid",
		},
		{
			name:       "backup changed during status",
			action:     "prepare",
			extraEnv:   "TAMPER_BACKUP_DURING_STATUS=true",
			wantOutput: "logical backup changed during destroy prepare",
		},
		{
			name: "storage workload residue blocks completion",
			prepare: func(t *testing.T, f *destroyFixture) {
				f.run(t, "prepare", true, "")
				f.run(t, "quiesce", true, "")
				require.NoError(t, os.WriteFile(filepath.Join(f.dir, "storage-residue"), []byte("1"), 0o600))
			},
			action:     "destroy",
			wantOutput: "storage workloads=statefulset.apps/kb-tikv",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fixture := newDestroyFixture(t)
			if tc.prepare != nil {
				tc.prepare(t, fixture)
			}
			fixture.run(t, tc.action, false, tc.extraEnv, tc.wantOutput)
			_, err := os.Stat(filepath.Join(fixture.stateDir, "destroy-1.receipt.json"))
			require.ErrorIs(t, err, os.ErrNotExist)
		})
	}
}

type destroyFixture struct {
	dir      string
	stateDir string
	env      []string
}

func newDestroyFixture(t *testing.T) *destroyFixture {
	t.Helper()
	dir := t.TempDir()
	stateDir := filepath.Join(dir, "state")
	require.NoError(t, os.Mkdir(stateDir, 0o700))
	backup := filepath.Join(dir, "backup.jsonl")
	require.NoError(t, os.WriteFile(backup, []byte("fixture"), 0o600))

	logicalStatus := filepath.Join(dir, "logical-status")
	writeDestroyExecutable(t, logicalStatus, `#!/usr/bin/env bash
set -euo pipefail
if [[ "${FAKE_BACKUP_FAIL:-false}" == true ]]; then
  echo "backup rejected" >&2
  exit 1
fi
if [[ -z "${FIELD:-}" ]]; then
  printf '{"created_at_unix":1,"format":"kubebrain.logical.v2","leases":0,"prefix":"%s","records":2,"revision":987654321,"sha256":"%s"}\n' "${EXPECTED_PREFIX:-/registry}" "${FAKE_BACKUP_SHA:-`+destroyBackupSHA256+`}"
  if [[ "${TAMPER_BACKUP_DURING_STATUS:-false}" == true && ! -f "$FAKE_RESOURCE_DIR/backup-tampered-during-status" ]]; then
    printf 'changed\n' >>"$INPUT"
    touch "$FAKE_RESOURCE_DIR/backup-tampered-during-status"
  fi
  exit 0
fi
case "$FIELD" in
  sha256) echo "${FAKE_BACKUP_SHA:-`+destroyBackupSHA256+`}" ;;
  revision) echo 987654321 ;;
  *) exit 1 ;;
esac
`)
	kubectl := filepath.Join(dir, "kubectl")
	writeDestroyExecutable(t, kubectl, `#!/usr/bin/env bash
set -euo pipefail
kind=
name=
namespace=
args=("$@")
for ((i=0; i<${#args[@]}; i++)); do
  case "${args[$i]}" in
    -n) namespace="${args[$((i+1))]}" ;;
    get)
      kind="${args[$((i+1))]}"
      if ((i+2 < ${#args[@]})) && [[ "${args[$((i+2))]}" != -* ]]; then
        name="${args[$((i+2))]}"
      fi
      ;;
  esac
done
if [[ " $* " == *" scale statefulset kubebrain "* ]]; then
  touch "$FAKE_RESOURCE_DIR/quiesced"
  exit 0
fi
if [[ "$kind" == "pods" ]]; then
  [[ -f "$FAKE_RESOURCE_DIR/quiesced" ]] || printf 'pod/kubebrain-0\n'
  exit 0
fi
if [[ "$kind" == "pods,statefulsets" ]]; then
  [[ -f "$FAKE_RESOURCE_DIR/storage-residue" ]] && printf 'statefulset.apps/kb-tikv\n'
  exit 0
fi
if [[ "$kind" == "persistentvolumeclaims" ]]; then
  [[ -f "$FAKE_RESOURCE_DIR/deleted-persistentvolumeclaim-pd-kb-pd-0" ]] ||
    printf 'pd-kb-pd-0\tuid-pvc-pd\tpd\n'
  [[ -f "$FAKE_RESOURCE_DIR/deleted-persistentvolumeclaim-tikv-kb-tikv-0" ]] ||
    printf 'tikv-kb-tikv-0\tuid-pvc-tikv\ttikv\n'
  if [[ -f "$FAKE_RESOURCE_DIR/extra-pvc" ]]; then
    printf 'tikv-kb-tikv-extra\tuid-pvc-extra\ttikv\n'
  fi
  exit 0
fi
key="$kind-$name"
if [[ -f "$FAKE_RESOURCE_DIR/deleted-$key" ]]; then
  exit 1
fi
uid=
label_name=kubebrain
label_instance=instance-a
case "$key" in
  statefulset-kubebrain) uid=uid-kb-sts ;;
  service-kubebrain-client) uid=uid-kb-client ;;
  service-kubebrain-peer) uid=uid-kb-peer ;;
  poddisruptionbudget-kubebrain) uid=uid-kb-pdb ;;
  serviceaccount-kubebrain) uid=uid-kb-sa ;;
  tidbcluster-kb) uid=uid-tc; label_name=tidb-cluster; label_instance=kb ;;
  poddisruptionbudget-kb-pd) uid=uid-pd-pdb; label_name=tidb-cluster; label_instance=kb ;;
  poddisruptionbudget-kb-tikv) uid=uid-tikv-pdb; label_name=tidb-cluster; label_instance=kb ;;
  service-kb-pd-metrics) uid=uid-pd-service; label_name=tidb-cluster; label_instance=kb ;;
  service-kb-tikv-metrics) uid=uid-tikv-service; label_name=tidb-cluster; label_instance=kb ;;
  *) echo "unexpected kubectl call: $*" >&2; exit 1 ;;
esac
if [[ -f "$FAKE_RESOURCE_DIR/drift-$key" ]]; then
  uid="$(cat "$FAKE_RESOURCE_DIR/drift-$key")"
fi
if [[ " $* " == *'jsonpath={.metadata.uid}'* && " $* " != *'metadata.labels'* ]]; then
  printf '%s' "$uid"
elif [[ " $* " == *'.spec.replicas'* ]]; then
  [[ -f "$FAKE_RESOURCE_DIR/quiesced" ]] && printf '0\t0' || printf '3\t3'
else
  printf '%s\t%s\t%s' "$uid" "$label_name" "$label_instance"
fi
`)
	uidDelete := filepath.Join(dir, "uid-delete")
	writeDestroyExecutable(t, uidDelete, `#!/usr/bin/env bash
set -euo pipefail
printf '%s\n' "$*" >>"$FAKE_RESOURCE_DIR/uid-delete.log"
resource=
name=
uid=
while (($#)); do
  case "$1" in
    --resource) resource="$2"; shift 2 ;;
    --name) name="$2"; shift 2 ;;
    --uid) uid="$2"; shift 2 ;;
    *) shift ;;
  esac
done
kind="${resource%s}"
[[ "$resource" == "poddisruptionbudgets" ]] && kind=poddisruptionbudget
[[ "$resource" == "persistentvolumeclaims" ]] && kind=persistentvolumeclaim
[[ "$resource" == "tidbclusters" ]] && kind=tidbcluster
[[ "$resource" == "statefulsets" ]] && kind=statefulset
[[ "$resource" == "serviceaccounts" ]] && kind=serviceaccount
[[ "$resource" == "services" ]] && kind=service
[[ -n "$uid" ]]
touch "$FAKE_RESOURCE_DIR/deleted-$kind-$name"
`)
	realJQ, err := exec.LookPath("jq")
	require.NoError(t, err)
	jq := filepath.Join(dir, "jq-wrapper")
	writeDestroyExecutable(t, jq, `#!/usr/bin/env bash
set -euo pipefail
"$REAL_JQ" "$@"
if [[ "${PUBLISH_DESTROY_RECEIPT_DURING_JQ:-false}" == true &&
  " $* " == *" -cnS "* && " $* " == *"kubebrain.destroy.receipt.v1"* &&
  ! -f "$STATE_DIR/$OPERATION_ID.receipt.json" ]]; then
  IFS=$'\t' read -r _ _ _ _ _ _ _ _ backup_sha backup_revision <"$STATE_DIR/$OPERATION_ID.state"
  printf '{"backup_revision":%s,"backup_sha256":"%s","completed_at_unix":1,"format":"kubebrain.destroy.receipt.v1","instance":"%s","kubebrain_namespace":"%s","operation_id":"%s","resources_absent":true,"tidb_cluster":"%s","tidb_namespace":"%s"}\n' \
    "$backup_revision" "$backup_sha" "$INSTANCE" "$KUBEBRAIN_NAMESPACE" "$OPERATION_ID" "$TIDB_CLUSTER" "$TIDB_NAMESPACE" >"$STATE_DIR/$OPERATION_ID.receipt.json"
  chmod 600 "$STATE_DIR/$OPERATION_ID.receipt.json"
fi
`)

	return &destroyFixture{
		dir:      dir,
		stateDir: stateDir,
		env: []string{
			"OPERATION_ID=destroy-1",
			"INSTANCE=instance-a",
			"STATE_DIR=" + stateDir,
			"BACKUP_INPUT=" + backup,
			"KUBEBRAIN_NAMESPACE=instance-a",
			"KUBEBRAIN_STATEFULSET=kubebrain",
			"TIDB_NAMESPACE=storage-a",
			"TIDB_CLUSTER=kb",
			"EXPECTED_PVCS=2",
			"TIMEOUT_SECONDS=2",
			"POLL_INTERVAL_SECONDS=0",
			"KUBECTL=" + kubectl,
			"UID_DELETE=" + uidDelete,
			"LOGICAL_STATUS=" + logicalStatus,
			"FAKE_RESOURCE_DIR=" + dir,
			"CONFIRM_DESTROY=destroy:instance-a:destroy-1",
			"JQ=" + jq,
			"REAL_JQ=" + realJQ,
		},
	}
}

func (f *destroyFixture) run(t *testing.T, action string, wantOK bool, extraEnv string, outputs ...string) string {
	t.Helper()
	command := exec.Command("bash", "destroy-instance.sh")
	command.Env = append(os.Environ(), append(f.env, "ACTION="+action, extraEnv)...)
	output, err := command.CombinedOutput()
	if wantOK {
		require.NoError(t, err, string(output))
	} else {
		require.Error(t, err, string(output))
	}
	for _, expected := range outputs {
		require.Contains(t, strings.TrimSpace(string(output)), expected)
	}
	return string(output)
}

func writeDestroyExecutable(t *testing.T, path, contents string) {
	t.Helper()
	require.NoError(t, os.WriteFile(path, []byte(contents), 0o755))
}
