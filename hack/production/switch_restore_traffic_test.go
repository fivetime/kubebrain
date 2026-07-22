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

const restoreArtifactSHA256 = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"

func TestRestoreTrafficCutoverLifecycleAndRollback(t *testing.T) {
	f := newTrafficFixture(t)
	f.run(t, "prepare", true, "")
	f.run(t, "cutover", true, "")
	f.run(t, "verify", true, "")
	f.run(t, "complete", true, "")
	f.run(t, "complete", true, "")

	data, err := os.ReadFile(filepath.Join(f.state, "restore-1.receipt.json"))
	require.NoError(t, err)
	var receipt map[string]any
	require.NoError(t, json.Unmarshal(data, &receipt))
	require.Equal(t, "kubebrain.restore-cutover.receipt.v1", receipt["format"])
	require.Equal(t, "uid-service", receipt["service_uid"])
	require.NotEmpty(t, receipt["cutover_state_sha256"])
	require.Equal(t, true, receipt["endpoint_uids_matched"])

	r := newTrafficFixture(t)
	r.run(t, "prepare", true, "")
	r.run(t, "cutover", true, "")
	r.run(t, "rollback", true, "")
	r.run(t, "rollback", true, "")
	r.run(t, "complete", false, "", "rolled back operation cannot complete")
}

func TestRestoreTrafficCutoverRejectsExistingReceiptWithUnknownFields(t *testing.T) {
	f := newTrafficFixture(t)
	f.run(t, "prepare", true, "")
	f.run(t, "cutover", true, "")
	f.run(t, "verify", true, "")

	receiptPath := filepath.Join(f.state, "restore-1.receipt.json")
	receipt := `{"artifact_sha256":"` + restoreArtifactSHA256 + `","completed_at_unix":1,"cutover_state_sha256":"` + fileDigest(t, filepath.Join(f.state, "restore-1.state")) + `","endpoint_uids_matched":true,"format":"kubebrain.restore-cutover.receipt.v1","instance":"instance-a","operation_id":"restore-1","pod_uids_unchanged":true,"public_data_verified":true,"replicas":2,"service_name":"kubebrain","service_namespace":"instance-a","service_uid":"uid-service","snapshot_revision":42,"source_instance":"source","target_instance":"target","unexpected":true}` + "\n"
	require.NoError(t, os.WriteFile(receiptPath, []byte(receipt), 0o600))

	f.run(t, "complete", false, "", "existing restore cutover receipt does not match")
	data, err := os.ReadFile(receiptPath)
	require.NoError(t, err)
	require.Equal(t, receipt, string(data))
}

func TestRestoreTrafficCutoverRejectsRestoreReceiptWithUnknownFields(t *testing.T) {
	f := newTrafficFixture(t)
	path := filepath.Join(f.dir, "restore.json")
	receipt := strings.TrimSpace(string(mustRead(t, path)))
	receipt = strings.TrimSuffix(receipt, "}") + `,"unexpected":true}` + "\n"
	require.NoError(t, os.WriteFile(path, []byte(receipt), 0o600))

	f.run(t, "prepare", false, "", "restore verification receipt is invalid")
	require.NoFileExists(t, filepath.Join(f.state, "restore-1.state"))
}

func TestRestoreTrafficCutoverRejectsRestoreReceiptWithInvalidDigest(t *testing.T) {
	for _, tc := range []struct {
		name   string
		digest string
	}{
		{name: "short", digest: "abc123"},
		{name: "uppercase", digest: strings.ToUpper(restoreArtifactSHA256)},
		{name: "non-hex", digest: strings.Repeat("g", 64)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newTrafficFixture(t)
			path := filepath.Join(f.dir, "restore.json")
			receipt := strings.ReplaceAll(
				strings.TrimSpace(string(mustRead(t, path))),
				restoreArtifactSHA256,
				tc.digest,
			) + "\n"
			require.NoError(t, os.WriteFile(path, []byte(receipt), 0o600))

			f.run(t, "prepare", false, "", "restore verification receipt is invalid")
			require.NoFileExists(t, filepath.Join(f.state, "restore-1.state"))
		})
	}
}

func TestRestoreTrafficCutoverRejectsNonCanonicalState(t *testing.T) {
	f := newTrafficFixture(t)
	f.run(t, "prepare", true, "")
	statePath := filepath.Join(f.state, "restore-1.state")
	require.NoError(t, os.WriteFile(statePath, append(mustRead(t, statePath), []byte("UNKNOWN\trow\n")...), 0o600))

	f.run(t, "cutover", false, "", "state has invalid schema")
	require.NoFileExists(t, filepath.Join(f.state, "restore-1.cutover"))
}

func TestRestoreTrafficCutoverFailsClosed(t *testing.T) {
	for _, tc := range []struct {
		name, action, drift, want string
		prior                     []string
	}{
		{"cutover before prepare", "cutover", "", "prepare evidence is missing", nil},
		{"wrong initial selector", "prepare", "selector-target", "must select SOURCE_INSTANCE", nil},
		{"target pod replaced", "cutover", "target-uid-drift", "target Pod UID/readiness/restart fence failed", []string{"prepare"}},
		{"service UID replaced", "cutover", "service-uid-drift", "Service identity or selector fence failed", []string{"prepare"}},
		{"CAS conflict", "cutover", "cas-conflict", "conflict", []string{"prepare"}},
		{"foreign endpoint", "cutover", "foreign-endpoint", "timed out waiting", []string{"prepare"}},
		{"public data mismatch", "verify", "verify-fail", "verification failed", []string{"prepare", "cutover"}},
		{"complete without verify", "complete", "", "cutover verification evidence is missing", []string{"prepare", "cutover"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newTrafficFixture(t)
			for _, action := range tc.prior {
				f.run(t, action, true, "")
			}
			if tc.drift != "" {
				require.NoError(t, os.WriteFile(filepath.Join(f.dir, tc.drift), []byte("1"), 0o600))
			}
			f.run(t, tc.action, false, "", tc.want)
			_, err := os.Stat(filepath.Join(f.state, "restore-1.receipt.json"))
			require.ErrorIs(t, err, os.ErrNotExist)
		})
	}
}

func TestRestoreTrafficRollbackRecoversPatchBeforeCutoverMarker(t *testing.T) {
	f := newTrafficFixture(t)
	f.run(t, "prepare", true, "")
	require.NoError(t, os.WriteFile(filepath.Join(f.dir, "selector-target"), []byte("1"), 0o600))
	f.run(t, "rollback", true, "")
	data, err := os.ReadFile(filepath.Join(f.stateDir(), "restore-1.rollback"))
	require.NoError(t, err)
	require.Contains(t, string(data), "ROLLBACK")
}

type trafficFixture struct {
	dir, state string
	env        []string
}

func (f *trafficFixture) stateDir() string {
	return f.state
}

func newTrafficFixture(t *testing.T) *trafficFixture {
	t.Helper()
	dir := t.TempDir()
	state := filepath.Join(dir, "state")
	require.NoError(t, os.Mkdir(state, 0o700))
	backup := filepath.Join(dir, "backup.jsonl")
	require.NoError(t, os.WriteFile(backup, []byte("backup"), 0o600))
	restoreReceipt := filepath.Join(dir, "restore.json")
	require.NoError(t, os.WriteFile(restoreReceipt, []byte(`{
	  "format":"kubebrain.restore-verification.v1","artifact_format":"kubebrain.logical.v2",
	  "artifact_sha256":"`+restoreArtifactSHA256+`","snapshot_revision":42,"source_prefix":"/registry",
	  "target_prefix":"/restored","records":2,"artifact_leases":1,
	  "verified_target_leases":1,"verified_at_unix":100
	}`), 0o600))

	kubectl := filepath.Join(dir, "kubectl")
	writeTrafficExecutable(t, kubectl, `#!/usr/bin/env bash
set -euo pipefail
if [[ " $* " == *" patch service "* ]]; then
  [[ -f "$FAKE_DIR/cas-conflict" ]] && { echo conflict >&2; exit 1; }
  if [[ " $* " == *'"op":"replace"'*'"value":"target"'* ]]; then
    touch "$FAKE_DIR/selector-target"
  else
    rm -f "$FAKE_DIR/selector-target"
  fi
  exit 0
fi
if [[ " $* " == *" get service "* ]]; then
  selector=source
  [[ -f "$FAKE_DIR/selector-target" ]] && selector=target
  uid=uid-service
  [[ -f "$FAKE_DIR/service-uid-drift" ]] && uid=uid-new
  rv=10
  [[ "$selector" == target ]] && rv=11
  printf '{"metadata":{"uid":"%s","resourceVersion":"%s"},"spec":{"selector":{"app.kubernetes.io/name":"kubebrain","app.kubernetes.io/instance":"%s"}}}\n' "$uid" "$rv" "$selector"
  exit 0
fi
if [[ " $* " == *" get pods "* ]]; then
  instance=source
  [[ " $* " == *"instance=target"* ]] && instance=target
  uid1="uid-${instance}-0"; uid2="uid-${instance}-1"
  [[ "$instance" == target && -f "$FAKE_DIR/target-uid-drift" ]] && uid1=uid-target-new
  printf '{"items":[
    {"metadata":{"name":"kb-%s-0","uid":"%s"},"status":{"conditions":[{"type":"Ready","status":"True"}],"containerStatuses":[{"ready":true,"restartCount":0}]}},
    {"metadata":{"name":"kb-%s-1","uid":"%s"},"status":{"conditions":[{"type":"Ready","status":"True"}],"containerStatuses":[{"ready":true,"restartCount":0}]}}
  ]}\n' "$instance" "$uid1" "$instance" "$uid2"
  exit 0
fi
if [[ " $* " == *" get endpointslices "* ]]; then
  instance=source
  [[ -f "$FAKE_DIR/selector-target" ]] && instance=target
  uid1="uid-${instance}-0"; uid2="uid-${instance}-1"
  [[ -f "$FAKE_DIR/foreign-endpoint" ]] && uid2=uid-foreign
  printf '{"items":[{"metadata":{"ownerReferences":[{"kind":"Service","uid":"uid-service","controller":true}]},"endpoints":[
    {"conditions":{"ready":true,"serving":true,"terminating":false},"targetRef":{"kind":"Pod","uid":"%s"}},
    {"conditions":{"ready":true,"serving":true,"terminating":false},"targetRef":{"kind":"Pod","uid":"%s"}}
  ]}]}\n' "$uid1" "$uid2"
  exit 0
fi
echo "unexpected kubectl call: $*" >&2
exit 1
`)
	verify := filepath.Join(dir, "logical-verify")
	writeTrafficExecutable(t, verify, `#!/usr/bin/env bash
set -euo pipefail
[[ ! -f "$FAKE_DIR/verify-fail" ]] || { echo verification failed >&2; exit 1; }
cat >"$RECEIPT_OUTPUT" <<EOF
{"format":"kubebrain.restore-verification.v1","artifact_format":"kubebrain.logical.v2","artifact_sha256":"`+restoreArtifactSHA256+`","snapshot_revision":42,"source_prefix":"/registry","target_prefix":"/restored","records":2,"artifact_leases":1,"verified_target_leases":1,"verified_at_unix":200}
EOF
chmod 600 "$RECEIPT_OUTPUT"
`)
	return &trafficFixture{dir: dir, state: state, env: []string{
		"OPERATION_ID=restore-1", "INSTANCE=instance-a", "STATE_DIR=" + state,
		"RESTORE_RECEIPT_INPUT=" + restoreReceipt, "BACKUP_INPUT=" + backup,
		"SERVICE_NAMESPACE=instance-a", "SERVICE_NAME=kubebrain",
		"SOURCE_INSTANCE=source", "TARGET_INSTANCE=target", "EXPECTED_REPLICAS=2",
		"PUBLIC_ENDPOINT=https://service:2379", "TIMEOUT_SECONDS=1", "POLL_INTERVAL_SECONDS=0",
		"KUBECTL=" + kubectl, "LOGICAL_VERIFY=" + verify, "FAKE_DIR=" + dir,
	}}
}

func (f *trafficFixture) run(t *testing.T, action string, ok bool, extra string, outputs ...string) {
	t.Helper()
	cmd := exec.Command("bash", "switch-restore-traffic.sh")
	cmd.Env = append(os.Environ(), append(f.env, "ACTION="+action, extra)...)
	out, err := cmd.CombinedOutput()
	if ok {
		require.NoError(t, err, string(out))
	} else {
		require.Error(t, err, string(out))
	}
	for _, wanted := range outputs {
		require.Contains(t, strings.ToLower(string(out)), strings.ToLower(wanted))
	}
}

func writeTrafficExecutable(t *testing.T, path, content string) {
	t.Helper()
	require.NoError(t, os.WriteFile(path, []byte(content), 0o700))
}
