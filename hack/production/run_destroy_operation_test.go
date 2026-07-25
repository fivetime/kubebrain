package production_test

import (
	"crypto/sha256"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

const (
	destroyLogicalSHA      = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
	destroyLogicalRevision = 101
)

func TestDestroyOperationCompletesLifecycle(t *testing.T) {
	f := newDestroyRunnerFixture(t, true)
	f.run(t, true, "")
	log := f.log(t)
	requireOrdered(t, log, "phase prepare", "phase quiesce", "phase destroy", "phase complete")
	require.Contains(t, log, "--action succeed")
	require.Contains(t, log, "--namespace tenant-a-operations --action succeed")
	require.NotContains(t, log, "--namespace ops --namespace tenant-a-operations")
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

func TestDestroyOperationRejectsBackupTamperedDuringCapture(t *testing.T) {
	f := newDestroyRunnerFixture(t, true)
	f.run(t, false, "TAMPER_BACKUP_DURING_SHA256=true", "backup bytes changed")
	log := f.log(t)
	require.Contains(t, log, "--action retry")
	require.NotContains(t, log, "phase prepare")
	require.NotContains(t, log, "--action succeed")
}

func TestDestroyOperationRejectsParametersTamperedDuringDigest(t *testing.T) {
	f := newDestroyRunnerFixture(t, true)
	f.run(t, false, "TAMPER_PARAMETERS_DURING_SHA256=true", "parameters digest")
	log := f.log(t)
	require.Contains(t, log, "--action retry")
	require.NotContains(t, log, "phase prepare")
	require.NotContains(t, log, "--action succeed")
}

func TestDestroyOperationRejectsEmptyRequiredParameters(t *testing.T) {
	f := newDestroyRunnerFixture(t, true)
	parameters := strings.ReplaceAll(
		string(mustRead(t, f.parameters)),
		`"kubebrain_namespace":"instance-a"`,
		`"kubebrain_namespace":""`,
	)
	require.NoError(t, os.WriteFile(f.parameters, []byte(parameters), 0o600))

	f.run(t, false, "CLAIM_DIGEST="+fileDigest(t, f.parameters), "empty required field")
	log := f.log(t)
	require.NotContains(t, log, "phase ")
	require.NotContains(t, log, "--action retry")
	require.NotContains(t, log, "--action succeed")
}

func TestDestroyOperationRejectsInvalidBackupPrefix(t *testing.T) {
	for _, tc := range []struct {
		name   string
		prefix string
	}{
		{name: "empty", prefix: ""},
		{name: "relative", prefix: "registry"},
		{name: "control character", prefix: "/registry\tshadow"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newDestroyRunnerFixture(t, true)
			parameters := strings.Replace(
				string(mustRead(t, f.parameters)),
				`"backup_prefix":"/registry"`,
				fmt.Sprintf(`"backup_prefix":%q`, tc.prefix),
				1,
			)
			require.NoError(t, os.WriteFile(f.parameters, []byte(parameters), 0o600))

			f.run(t, false, "CLAIM_DIGEST="+fileDigest(t, f.parameters),
				"destroy backup_prefix must be an absolute key prefix without control characters")
			log := f.log(t)
			require.NotContains(t, log, "phase ")
			require.NotContains(t, log, "--action retry")
			require.NotContains(t, log, "--action succeed")
		})
	}
}

func TestDestroyOperationRejectsInvalidReceipt(t *testing.T) {
	f := newDestroyRunnerFixture(t, true)
	f.run(t, false, "INVALID_RECEIPT=1", "invalid receipt")
	log := f.log(t)
	require.Contains(t, log, "--action retry")
	require.NotContains(t, log, "--action succeed")
}

func TestDestroyOperationRejectsReceiptTamperedDuringDigest(t *testing.T) {
	f := newDestroyRunnerFixture(t, true)
	f.run(t, false, "TAMPER_RECEIPT_DURING_SHA256=true", "invalid receipt")
	log := f.log(t)
	require.Contains(t, log, "--action retry")
	require.NotContains(t, log, "--action succeed")
}

func TestDestroyOperationRejectsValidReceiptChangedAfterDigest(t *testing.T) {
	f := newDestroyRunnerFixture(t, true)
	tamperedReceipt := filepath.Join(f.dir, "valid-tampered-receipt.json")
	receipt := fmt.Sprintf(`{"backup_revision":%d,"backup_sha256":%q,"completed_at_unix":124,"format":"kubebrain.destroy.receipt.v1","instance":"instance-a","kubebrain_namespace":"instance-a","operation_id":"destroy-1","resources_absent":true,"tidb_cluster":"kb","tidb_namespace":"storage-a"}`+"\n", destroyLogicalRevision, destroyLogicalSHA)
	require.NoError(t, os.WriteFile(tamperedReceipt, []byte(receipt), 0o600))
	f.env = withReceiptAfterSHA256Tamper(f.env, tamperedReceipt)

	f.run(t, false, "", "invalid receipt")
	log := f.log(t)
	require.Contains(t, log, "--action retry")
	require.NotContains(t, log, "--action succeed")
}

func TestDestroyOperationRejectsNonCanonicalStateBeforeSucceed(t *testing.T) {
	f := newDestroyRunnerFixture(t, true)
	f.run(t, false, "TAMPER_STATE_BEFORE_RECEIPT=1", "invalid receipt")
	log := f.log(t)
	require.Contains(t, log, "--action retry")
	require.NotContains(t, log, "--action succeed")
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
  printf '{"namespace":"tenant-a-operations","name":"destroy-1","uid":"uid-op","resource_version":"1","operation_id":"destroy-1","instance":"instance-a","type":"Destroy","parameters_sha256":"%s","owner":"worker-a","attempt":1,"lease_until_unix":999999}\n' "$digest"
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
  prepare)
    {
      printf 'HEADER\tkubebrain.destroy.state.v1\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' \
        "$INSTANCE" "$OPERATION_ID" "$KUBEBRAIN_NAMESPACE" "$KUBEBRAIN_STATEFULSET" \
        "$TIDB_NAMESPACE" "$TIDB_CLUSTER" "$DESTROY_BACKUP_SHA" "$DESTROY_BACKUP_REVISION"
      printf 'RESOURCE\tapps/v1\tstatefulsets\tstatefulset\t%s\t%s\tuid-kb-sts\n' "$KUBEBRAIN_NAMESPACE" "$KUBEBRAIN_STATEFULSET"
      printf 'RESOURCE\tv1\tservices\tservice\t%s\t%s-client\tuid-kb-client\n' "$KUBEBRAIN_NAMESPACE" "$KUBEBRAIN_STATEFULSET"
      printf 'RESOURCE\tv1\tservices\tservice\t%s\t%s-peer\tuid-kb-peer\n' "$KUBEBRAIN_NAMESPACE" "$KUBEBRAIN_STATEFULSET"
      printf 'RESOURCE\tpolicy/v1\tpoddisruptionbudgets\tpoddisruptionbudget\t%s\t%s\tuid-kb-pdb\n' "$KUBEBRAIN_NAMESPACE" "$KUBEBRAIN_STATEFULSET"
      printf 'RESOURCE\tv1\tserviceaccounts\tserviceaccount\t%s\t%s\tuid-kb-sa\n' "$KUBEBRAIN_NAMESPACE" "$KUBEBRAIN_STATEFULSET"
      printf 'RESOURCE\tpingcap.com/v1alpha1\ttidbclusters\ttidbcluster\t%s\t%s\tuid-tc\n' "$TIDB_NAMESPACE" "$TIDB_CLUSTER"
      printf 'RESOURCE\tpolicy/v1\tpoddisruptionbudgets\tpoddisruptionbudget\t%s\t%s-pd\tuid-pd-pdb\n' "$TIDB_NAMESPACE" "$TIDB_CLUSTER"
      printf 'RESOURCE\tv1\tservices\tservice\t%s\t%s-pd-metrics\tuid-pd-service\n' "$TIDB_NAMESPACE" "$TIDB_CLUSTER"
      printf 'RESOURCE\tpolicy/v1\tpoddisruptionbudgets\tpoddisruptionbudget\t%s\t%s-tikv\tuid-tikv-pdb\n' "$TIDB_NAMESPACE" "$TIDB_CLUSTER"
      printf 'RESOURCE\tv1\tservices\tservice\t%s\t%s-tikv-metrics\tuid-tikv-service\n' "$TIDB_NAMESPACE" "$TIDB_CLUSTER"
      printf 'PVC\tv1\tpersistentvolumeclaims\tpersistentvolumeclaim\tpd-kb-pd-0\tuid-pvc-pd\tpd\t%s\n' "$TIDB_NAMESPACE"
      printf 'PVC\tv1\tpersistentvolumeclaims\tpersistentvolumeclaim\ttikv-kb-tikv-0\tuid-pvc-tikv\ttikv\t%s\n' "$TIDB_NAMESPACE"
    } >"$STATE_DIR/$OPERATION_ID.state"
    ;;
  quiesce) printf 'kubebrain.destroy.quiesced.v1\t%s\t%s\n' "$INSTANCE" "$OPERATION_ID" >"$STATE_DIR/$OPERATION_ID.quiesced" ;;
  destroy) printf 'kubebrain.destroy.resources-absent.v1\t%s\t%s\n' "$INSTANCE" "$OPERATION_ID" >"$STATE_DIR/$OPERATION_ID.destroyed" ;;
  complete)
    [[ -z "${TAMPER_STATE_BEFORE_RECEIPT:-}" ]] || printf 'UNKNOWN\trow\n' >>"$STATE_DIR/$OPERATION_ID.state"
    if [[ -n "${INVALID_RECEIPT:-}" ]]; then
      printf '{"format":"kubebrain.destroy.receipt.v1","backup_sha256":"short"}\n' >"$RECEIPT_OUTPUT"
    else
      printf '{"backup_revision":%s,"backup_sha256":"%s","completed_at_unix":123,"format":"kubebrain.destroy.receipt.v1","instance":"%s","kubebrain_namespace":"%s","operation_id":"%s","resources_absent":true,"tidb_cluster":"%s","tidb_namespace":"%s"}\n' \
        "$DESTROY_BACKUP_REVISION" "$DESTROY_BACKUP_SHA" "$INSTANCE" "$KUBEBRAIN_NAMESPACE" \
        "$OPERATION_ID" "$TIDB_CLUSTER" "$TIDB_NAMESPACE" >"$RECEIPT_OUTPUT"
    fi
    ;;
esac
`)
	env := []string{
		"WORKER_ID=worker-a", "PARAMETERS_INPUT=" + parameters,
		"OPERATION_NAMESPACE=ops", "LEASE_SECONDS=6", "HEARTBEAT_INTERVAL_SECONDS=0.02",
		"OPERATIONCTL=" + operationctl, "DESTROY_COMMAND=" + destroy,
		"FAKE_DIR=" + dir, "PARAMETERS_DIGEST=" + digest,
		"RUNNER_BACKUP_INPUT=" + backup,
		"RUNNER_PARAMETERS_INPUT=" + parameters,
		"DESTROY_BACKUP_SHA=" + destroyLogicalSHA, fmt.Sprintf("DESTROY_BACKUP_REVISION=%d", destroyLogicalRevision),
	}
	env = append(env, receiptDigestTamperEnv(t, dir, receipt)...)
	return &destroyRunnerFixture{
		dir: dir, parameters: parameters,
		env: env,
	}
}

func (f *destroyRunnerFixture) run(t *testing.T, ok bool, extra string, outputs ...string) {
	t.Helper()
	env := append([]string{}, f.env...)
	if extra != "" {
		env = append(env, extra)
	}
	out, err := runProductionRunnerCommand(t, "run-destroy-operation.sh", env)
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
	statePath := filepath.Join(stateDir, "destroy-1.state")
	stateContent := destroyRunnerState()
	if kind == "state" {
		require.NoError(t, os.WriteFile(statePath, []byte(stateContent), 0o600))
		return
	}
	require.NoError(t, os.WriteFile(statePath, []byte(stateContent), 0o600))
	if kind == "receipt" {
		require.NoError(t, os.WriteFile(filepath.Join(stateDir, "destroy-1.quiesced"), []byte("kubebrain.destroy.quiesced.v1\tinstance-a\tdestroy-1\n"), 0o600))
		require.NoError(t, os.WriteFile(filepath.Join(stateDir, "destroy-1.destroyed"), []byte("kubebrain.destroy.resources-absent.v1\tinstance-a\tdestroy-1\n"), 0o600))
		receipt := fmt.Sprintf(`{"backup_revision":%d,"backup_sha256":%q,"completed_at_unix":123,"format":"kubebrain.destroy.receipt.v1","instance":"instance-a","kubebrain_namespace":"instance-a","operation_id":"destroy-1","resources_absent":true,"tidb_cluster":"kb","tidb_namespace":"storage-a"}`+"\n", destroyLogicalRevision, destroyLogicalSHA)
		require.NoError(t, os.WriteFile(filepath.Join(f.dir, "receipt.json"), []byte(receipt), 0o600))
		return
	}
	marker := "evidence\n"
	if kind == "quiesced" {
		marker = "kubebrain.destroy.quiesced.v1\tinstance-a\tdestroy-1\n"
	}
	if kind == "destroyed" {
		require.NoError(t, os.WriteFile(filepath.Join(stateDir, "destroy-1.quiesced"), []byte("kubebrain.destroy.quiesced.v1\tinstance-a\tdestroy-1\n"), 0o600))
		marker = "kubebrain.destroy.resources-absent.v1\tinstance-a\tdestroy-1\n"
	}
	require.NoError(t, os.WriteFile(filepath.Join(stateDir, "destroy-1."+kind), []byte(marker), 0o600))
}

func destroyRunnerState() string {
	return fmt.Sprintf("HEADER\tkubebrain.destroy.state.v1\tinstance-a\tdestroy-1\tinstance-a\tkubebrain\tstorage-a\tkb\t%s\t%d\n", destroyLogicalSHA, destroyLogicalRevision) +
		"RESOURCE\tapps/v1\tstatefulsets\tstatefulset\tinstance-a\tkubebrain\tuid-kb-sts\n" +
		"RESOURCE\tv1\tservices\tservice\tinstance-a\tkubebrain-client\tuid-kb-client\n" +
		"RESOURCE\tv1\tservices\tservice\tinstance-a\tkubebrain-peer\tuid-kb-peer\n" +
		"RESOURCE\tpolicy/v1\tpoddisruptionbudgets\tpoddisruptionbudget\tinstance-a\tkubebrain\tuid-kb-pdb\n" +
		"RESOURCE\tv1\tserviceaccounts\tserviceaccount\tinstance-a\tkubebrain\tuid-kb-sa\n" +
		"RESOURCE\tpingcap.com/v1alpha1\ttidbclusters\ttidbcluster\tstorage-a\tkb\tuid-tc\n" +
		"RESOURCE\tpolicy/v1\tpoddisruptionbudgets\tpoddisruptionbudget\tstorage-a\tkb-pd\tuid-pd-pdb\n" +
		"RESOURCE\tv1\tservices\tservice\tstorage-a\tkb-pd-metrics\tuid-pd-service\n" +
		"RESOURCE\tpolicy/v1\tpoddisruptionbudgets\tpoddisruptionbudget\tstorage-a\tkb-tikv\tuid-tikv-pdb\n" +
		"RESOURCE\tv1\tservices\tservice\tstorage-a\tkb-tikv-metrics\tuid-tikv-service\n" +
		"PVC\tv1\tpersistentvolumeclaims\tpersistentvolumeclaim\tpd-kb-pd-0\tuid-pvc-pd\tpd\tstorage-a\n" +
		"PVC\tv1\tpersistentvolumeclaims\tpersistentvolumeclaim\ttikv-kb-tikv-0\tuid-pvc-tikv\ttikv\tstorage-a\n"
}
