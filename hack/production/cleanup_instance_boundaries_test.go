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

func TestBoundaryCleanupLifecycleIsRetrySafe(t *testing.T) {
	f := newBoundaryCleanupFixture(t)
	f.run(t, "prepare", true, "")
	f.run(t, "prepare", true, "")
	f.run(t, "delete", true, "")
	f.run(t, "delete", true, "")
	f.run(t, "complete", true, "")
	f.run(t, "complete", true, "")

	data, err := os.ReadFile(filepath.Join(f.stateDir, "cleanup-1.receipt.json"))
	require.NoError(t, err)
	var receipt map[string]any
	require.NoError(t, json.Unmarshal(data, &receipt))
	require.Equal(t, "kubebrain.boundary-cleanup.receipt.v1", receipt["format"])
	require.Equal(t, true, receipt["namespaces_absent"])
	require.Equal(t, true, receipt["credentials_absent"])
	require.Equal(t, []any{"client-tls", "object-store"}, receipt["credential_secrets"])

	log, err := os.ReadFile(filepath.Join(f.dir, "uid-delete.log"))
	require.NoError(t, err)
	require.Contains(t, string(log), "--resource secrets --name client-tls --uid uid-client-tls --timeout 2s --namespace control")
	require.Contains(t, string(log), "--resource namespaces --name instance-a --uid uid-instance-a")
	for _, line := range strings.Split(strings.TrimSpace(string(log)), "\n") {
		if strings.Contains(line, "--resource namespaces") {
			require.NotContains(t, line, "--namespace")
		}
	}
}

func TestBoundaryCleanupTreatsConcurrentReceiptPublishAsIdempotent(t *testing.T) {
	f := newBoundaryCleanupFixture(t)
	f.run(t, "prepare", true, "")
	f.run(t, "delete", true, "")
	f.run(t, "complete", true, "PUBLISH_CLEANUP_RECEIPT_DURING_JQ=true")

	data, err := os.ReadFile(filepath.Join(f.stateDir, "cleanup-1.receipt.json"))
	require.NoError(t, err)
	var receipt map[string]any
	require.NoError(t, json.Unmarshal(data, &receipt))
	require.Equal(t, "kubebrain.boundary-cleanup.receipt.v1", receipt["format"])
	require.Equal(t, float64(1), receipt["completed_at_unix"])
	require.Equal(t, []any{"client-tls", "object-store"}, receipt["credential_secrets"])
}

func TestBoundaryCleanupRejectsExistingReceiptWithUnknownFields(t *testing.T) {
	f := newBoundaryCleanupFixture(t)
	f.run(t, "prepare", true, "")
	f.run(t, "delete", true, "")

	receiptPath := filepath.Join(f.stateDir, "cleanup-1.receipt.json")
	receipt := `{"cleanup_id":"cleanup-1","completed_at_unix":1,"credential_namespace":"control","credential_secrets":["client-tls","object-store"],"credentials_absent":true,"destroy_receipt_sha256":"` + fileDigest(t, f.receipt) + `","format":"kubebrain.boundary-cleanup.receipt.v1","instance":"instance-a","kubebrain_namespace":"instance-a","namespaces_absent":true,"tidb_namespace":"storage-a","unexpected":true}` + "\n"
	require.NoError(t, os.WriteFile(receiptPath, []byte(receipt), 0o600))

	f.run(t, "complete", false, "", "existing boundary cleanup receipt does not match")
	data, err := os.ReadFile(receiptPath)
	require.NoError(t, err)
	require.Equal(t, receipt, string(data))
}

func TestBoundaryCleanupFailsClosed(t *testing.T) {
	tests := []struct {
		name       string
		prepare    func(*testing.T, *boundaryCleanupFixture)
		action     string
		extraEnv   string
		wantOutput string
	}{
		{
			name:       "wrong confirmation",
			prepare:    func(t *testing.T, f *boundaryCleanupFixture) { f.run(t, "prepare", true, "") },
			action:     "delete",
			extraEnv:   "CONFIRM_CLEANUP=wrong",
			wantOutput: "must exactly equal",
		},
		{
			name:       "namespace is not dedicated",
			action:     "prepare",
			extraEnv:   "FAKE_NAMESPACE_DEDICATED=false",
			wantOutput: "is not a dedicated boundary",
		},
		{
			name: "destroy receipt unknown field",
			prepare: func(t *testing.T, f *boundaryCleanupFixture) {
				receipt := strings.TrimSpace(string(mustRead(t, f.receipt)))
				receipt = strings.TrimSuffix(receipt, "}") + `,"unexpected":true}` + "\n"
				require.NoError(t, os.WriteFile(f.receipt, []byte(receipt), 0o600))
			},
			action:     "prepare",
			wantOutput: "destroy receipt does not authorize this boundary cleanup",
		},
		{
			name:       "destroy receipt changed during capture",
			action:     "prepare",
			extraEnv:   "TAMPER_DESTROY_RECEIPT_DURING_SHA256=true",
			wantOutput: "destroy receipt changed while being captured",
		},
		{
			name:       "secret is not owned",
			action:     "prepare",
			extraEnv:   "FAKE_SECRET_CREDENTIAL=false",
			wantOutput: "is not an owned instance credential",
		},
		{
			name: "namespace UID changed",
			prepare: func(t *testing.T, f *boundaryCleanupFixture) {
				f.run(t, "prepare", true, "")
			},
			action:     "delete",
			extraEnv:   "FAKE_NAMESPACE_UID_DRIFT=true",
			wantOutput: "UID fence failed",
		},
		{
			name: "secret UID changed",
			prepare: func(t *testing.T, f *boundaryCleanupFixture) {
				f.run(t, "prepare", true, "")
			},
			action:     "delete",
			extraEnv:   "FAKE_SECRET_UID_DRIFT=true",
			wantOutput: "UID fence failed",
		},
		{
			name: "destroy receipt changed",
			prepare: func(t *testing.T, f *boundaryCleanupFixture) {
				f.run(t, "prepare", true, "")
				require.NoError(t, os.WriteFile(f.receipt, append(mustRead(t, f.receipt), '\n'), 0o600))
			},
			action:     "delete",
			wantOutput: "does not match request or destroy receipt",
		},
		{
			name: "non canonical state",
			prepare: func(t *testing.T, f *boundaryCleanupFixture) {
				f.run(t, "prepare", true, "")
				statePath := filepath.Join(f.stateDir, "cleanup-1.boundaries")
				require.NoError(t, os.WriteFile(statePath, append(mustRead(t, statePath), []byte("UNKNOWN\trow\n")...), 0o600))
			},
			action:     "delete",
			wantOutput: "boundary cleanup state has invalid schema",
		},
		{
			name: "missing credential state row",
			prepare: func(t *testing.T, f *boundaryCleanupFixture) {
				f.run(t, "prepare", true, "")
				statePath := filepath.Join(f.stateDir, "cleanup-1.boundaries")
				state := strings.ReplaceAll(string(mustRead(t, statePath)), "SECRET\tobject-store\tuid-object-store\n", "")
				require.NoError(t, os.WriteFile(statePath, []byte(state), 0o600))
			},
			action:     "complete",
			wantOutput: "boundary cleanup state has invalid schema",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			f := newBoundaryCleanupFixture(t)
			if tc.prepare != nil {
				tc.prepare(t, f)
			}
			f.run(t, tc.action, false, tc.extraEnv, tc.wantOutput)
			_, err := os.Stat(filepath.Join(f.stateDir, "cleanup-1.receipt.json"))
			require.ErrorIs(t, err, os.ErrNotExist)
		})
	}
}

type boundaryCleanupFixture struct {
	dir      string
	stateDir string
	receipt  string
	env      []string
}

func newBoundaryCleanupFixture(t *testing.T) *boundaryCleanupFixture {
	t.Helper()
	dir := t.TempDir()
	stateDir := filepath.Join(dir, "state")
	require.NoError(t, os.Mkdir(stateDir, 0o700))
	receipt := filepath.Join(dir, "destroy.json")
	require.NoError(t, os.WriteFile(receipt, []byte(`{
  "format":"kubebrain.destroy.receipt.v1",
  "instance":"instance-a",
  "operation_id":"destroy-1",
  "kubebrain_namespace":"instance-a",
  "tidb_namespace":"storage-a",
  "tidb_cluster":"kb",
  "backup_sha256":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
  "backup_revision":42,
  "resources_absent":true,
  "completed_at_unix":1
}`), 0o600))

	kubectl := filepath.Join(dir, "kubectl")
	writeDestroyExecutable(t, kubectl, `#!/usr/bin/env bash
set -euo pipefail
namespace=
kind=
name=
args=("$@")
for ((i=0; i<${#args[@]}; i++)); do
  case "${args[$i]}" in
    -n) namespace="${args[$((i+1))]}" ;;
    get) kind="${args[$((i+1))]}"; name="${args[$((i+2))]}" ;;
  esac
done
key="$kind-$name"
[[ ! -f "$FAKE_RESOURCE_DIR/deleted-$key" ]] || exit 1
case "$key" in
  namespace-instance-a) uid=uid-instance-a ;;
  namespace-storage-a) uid=uid-storage-a ;;
  secret-client-tls) uid=uid-client-tls ;;
  secret-object-store) uid=uid-object-store ;;
  *) echo "unexpected kubectl call: $*" >&2; exit 1 ;;
esac
if [[ "$kind" == namespace && "${FAKE_NAMESPACE_UID_DRIFT:-false}" == true ]]; then
  uid=uid-drifted
fi
if [[ "$kind" == secret && "${FAKE_SECRET_UID_DRIFT:-false}" == true ]]; then
  uid=uid-drifted
fi
if [[ " $* " == *'jsonpath={.metadata.uid}'* && " $* " != *'metadata.labels'* ]]; then
  printf '%s' "$uid"
elif [[ "$kind" == namespace ]]; then
  printf '%s\tinstance-a\t%s' "$uid" "${FAKE_NAMESPACE_DEDICATED:-true}"
else
  [[ "$namespace" == control ]]
  printf '%s\tinstance-a\t%s' "$uid" "${FAKE_SECRET_CREDENTIAL:-true}"
fi
`)
	uidDelete := filepath.Join(dir, "uid-delete")
	writeDestroyExecutable(t, uidDelete, `#!/usr/bin/env bash
set -euo pipefail
printf '%s\n' "$*" >>"$FAKE_RESOURCE_DIR/uid-delete.log"
resource=
name=
while (($#)); do
  case "$1" in
    --resource) resource="$2"; shift 2 ;;
    --name) name="$2"; shift 2 ;;
    *) shift ;;
  esac
done
kind="${resource%s}"
[[ "$resource" == namespaces ]] && kind=namespace
touch "$FAKE_RESOURCE_DIR/deleted-$kind-$name"
`)
	realSHA, err := exec.LookPath("sha256sum")
	require.NoError(t, err)
	writeDestroyExecutable(t, filepath.Join(dir, "sha256sum"), `#!/usr/bin/env bash
set -euo pipefail
if [[ "${TAMPER_DESTROY_RECEIPT_DURING_SHA256:-false}" == true &&
  "$#" -ge 1 && "$1" == "$BOUNDARY_CLEANUP_DESTROY_RECEIPT_INPUT" &&
  ! -f "$FAKE_RESOURCE_DIR/destroy-receipt-tampered-during-sha256" ]]; then
  "$REAL_SHA256SUM" "$@"
  printf 'changed\n' >"$BOUNDARY_CLEANUP_DESTROY_RECEIPT_INPUT"
  touch "$FAKE_RESOURCE_DIR/destroy-receipt-tampered-during-sha256"
  exit 0
fi
exec "$REAL_SHA256SUM" "$@"
`)
	realJQ, err := exec.LookPath("jq")
	require.NoError(t, err)
	jq := filepath.Join(dir, "jq-wrapper")
	writeDestroyExecutable(t, jq, `#!/usr/bin/env bash
set -euo pipefail
"$REAL_JQ" "$@"
if [[ "${PUBLISH_CLEANUP_RECEIPT_DURING_JQ:-false}" == true &&
  " $* " == *" -cnS "* && " $* " == *"kubebrain.boundary-cleanup.receipt.v1"* &&
  ! -f "$STATE_DIR/$CLEANUP_ID.receipt.json" ]]; then
  state_path="$STATE_DIR/$CLEANUP_ID.boundaries"
  destroy_sha="$(sha256sum "$DESTROY_RECEIPT_INPUT" | cut -d ' ' -f1)"
  secrets="$("$REAL_JQ" -Rn '[inputs | split("\t") | select(.[0] == "SECRET") | .[1]]' <"$state_path")"
  printf '{"cleanup_id":"%s","completed_at_unix":1,"credential_namespace":"%s","credential_secrets":%s,"credentials_absent":true,"destroy_receipt_sha256":"%s","format":"kubebrain.boundary-cleanup.receipt.v1","instance":"%s","kubebrain_namespace":"%s","namespaces_absent":true,"tidb_namespace":"%s"}\n' \
    "$CLEANUP_ID" "$CREDENTIAL_NAMESPACE" "$secrets" "$destroy_sha" "$INSTANCE" "$KUBEBRAIN_NAMESPACE" "$TIDB_NAMESPACE" >"$STATE_DIR/$CLEANUP_ID.receipt.json"
  chmod 600 "$STATE_DIR/$CLEANUP_ID.receipt.json"
fi
`)

	return &boundaryCleanupFixture{
		dir:      dir,
		stateDir: stateDir,
		receipt:  receipt,
		env: []string{
			"CLEANUP_ID=cleanup-1",
			"INSTANCE=instance-a",
			"STATE_DIR=" + stateDir,
			"DESTROY_RECEIPT_INPUT=" + receipt,
			"KUBEBRAIN_NAMESPACE=instance-a",
			"TIDB_NAMESPACE=storage-a",
			"CREDENTIAL_NAMESPACE=control",
			"CREDENTIAL_SECRETS=client-tls,object-store",
			"CONFIRM_CLEANUP=cleanup:instance-a:cleanup-1",
			"KUBECTL=" + kubectl,
			"UID_DELETE=" + uidDelete,
			"FAKE_RESOURCE_DIR=" + dir,
			"TIMEOUT_SECONDS=2",
			"POLL_INTERVAL_SECONDS=0",
			"JQ=" + jq,
			"REAL_JQ=" + realJQ,
			"PATH=" + dir + string(os.PathListSeparator) + os.Getenv("PATH"),
			"REAL_SHA256SUM=" + realSHA,
			"BOUNDARY_CLEANUP_DESTROY_RECEIPT_INPUT=" + receipt,
		},
	}
}

func (f *boundaryCleanupFixture) run(t *testing.T, action string, success bool, extraEnv string, outputs ...string) {
	t.Helper()
	env := append([]string{}, f.env...)
	env = append(env, "ACTION="+action)
	if extraEnv != "" {
		env = append(env, extraEnv)
	}
	output, err := runBoundaryCleanup(t, env)
	if success {
		require.NoError(t, err, "%s", output)
	} else {
		require.Error(t, err, "%s", output)
	}
	for _, expected := range outputs {
		require.Contains(t, string(output), expected)
	}
}

func runBoundaryCleanup(t *testing.T, env []string) ([]byte, error) {
	t.Helper()
	return runProductionScriptCommand(t, "cleanup-instance-boundaries.sh", env)
}

func mustRead(t *testing.T, path string) []byte {
	t.Helper()
	data, err := os.ReadFile(path)
	require.NoError(t, err)
	return data
}
