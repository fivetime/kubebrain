package production_test

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

const auditArtifactSHA256 = "abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789"

func TestPostRestoreAuditPublishesReceiptAndRechecksOnRetry(t *testing.T) {
	f := newAuditFixture(t)
	f.run(t, true, "")
	f.run(t, true, "")

	data, err := os.ReadFile(filepath.Join(f.state, "audit-1.receipt.json"))
	require.NoError(t, err)
	var receipt map[string]any
	require.NoError(t, json.Unmarshal(data, &receipt))
	require.Equal(t, "kubebrain.post-restore-audit.receipt.v1", receipt["format"])
	require.Equal(t, true, receipt["topology_unchanged"])
	require.GreaterOrEqual(t, receipt["samples"].(float64), float64(2))
	require.Equal(t, fileDigest(t, filepath.Join(f.dir, "cutover.state")), receipt["cutover_state_sha256"])
	require.Equal(t, fileDigest(t, filepath.Join(f.dir, "cutover.json")), receipt["cutover_receipt_sha256"])

	probes, err := os.ReadFile(filepath.Join(f.dir, "probes"))
	require.NoError(t, err)
	require.GreaterOrEqual(t, len(strings.Split(strings.TrimSpace(string(probes)), "\n")), 3)
}

func TestPostRestoreAuditTreatsConcurrentReceiptPublishAsIdempotent(t *testing.T) {
	f := newAuditFixture(t)
	f.run(t, true, "PUBLISH_RECEIPT_DURING_PROBE=true")

	data, err := os.ReadFile(filepath.Join(f.state, "audit-1.receipt.json"))
	require.NoError(t, err)
	var receipt map[string]any
	require.NoError(t, json.Unmarshal(data, &receipt))
	require.Equal(t, float64(1), receipt["started_at_unix"])
	require.Equal(t, float64(2), receipt["completed_at_unix"])
	require.Equal(t, float64(2), receipt["samples"])
}

func TestPostRestoreAuditRejectsCutoverEvidenceDriftDuringReceiptPublish(t *testing.T) {
	for _, tc := range []struct {
		name, env, want string
	}{
		{
			name: "state", env: "TAMPER_CUTOVER_STATE_DURING_AUDIT_RECEIPT_JQ=true",
			want: "cutover state changed",
		},
		{
			name: "receipt", env: "TAMPER_CUTOVER_RECEIPT_DURING_AUDIT_RECEIPT_JQ=true",
			want: "cutover receipt changed",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newAuditFixture(t)
			f.run(t, false, tc.env, tc.want)
			require.NoFileExists(t, filepath.Join(f.state, "audit-1.receipt.json"))
		})
	}
}

func TestPostRestoreAuditRejectsCutoverEvidenceDriftDuringCapture(t *testing.T) {
	for _, tc := range []struct {
		name, env, want string
	}{
		{
			name: "state", env: "TAMPER_CUTOVER_STATE_DURING_SHA256=true",
			want: "cutover state changed while being captured",
		},
		{
			name: "receipt", env: "TAMPER_CUTOVER_RECEIPT_DURING_SHA256=true",
			want: "cutover receipt changed while being captured",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newAuditFixture(t)
			f.run(t, false, tc.env, tc.want)
			require.NoFileExists(t, filepath.Join(f.state, "audit-1.receipt.json"))
		})
	}
}

func TestPostRestoreAuditRejectsExistingReceiptWithUnknownFields(t *testing.T) {
	f := newAuditFixture(t)
	receiptPath := filepath.Join(f.state, "audit-1.receipt.json")
	receipt := `{"all_probes_succeeded":true,"artifact_sha256":"` + auditArtifactSHA256 + `","completed":true,"completed_at_unix":2,"cutover_operation_id":"cutover-1","duration_seconds":1,"first_probe_revision":1,"format":"kubebrain.post-restore-audit.receipt.v1","instance":"instance-a","interval_seconds":1,"last_probe_revision":2,"operation_id":"audit-1","replicas":2,"samples":2,"service_uid":"uid-service","snapshot_revision":42,"started_at_unix":1,"target_instance":"target","topology_unchanged":true,"unexpected":true}` + "\n"
	require.NoError(t, os.WriteFile(receiptPath, []byte(receipt), 0o600))

	f.run(t, false, "", "existing post-restore audit receipt does not match the operation")
	data, err := os.ReadFile(receiptPath)
	require.NoError(t, err)
	require.Equal(t, receipt, string(data))
}

func TestPostRestoreAuditRejectsCutoverStateWithInvalidArtifactDigest(t *testing.T) {
	for _, tc := range []struct {
		name   string
		digest string
	}{
		{name: "short", digest: "abc123"},
		{name: "uppercase", digest: strings.ToUpper(auditArtifactSHA256)},
		{name: "non-hex", digest: strings.Repeat("g", 64)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newAuditFixture(t)
			f.replaceCutoverArtifactDigest(t, tc.digest)

			f.run(t, false, "", "cutover state does not match the audit operation")
			require.NoFileExists(t, filepath.Join(f.state, "audit-1.receipt.json"))
		})
	}
}

func TestPostRestoreAuditFailsClosed(t *testing.T) {
	for _, tc := range []struct {
		name, drift, want string
	}{
		{"cutover receipt mismatch", "receipt-drift", "cutover receipt does not match"},
		{"cutover receipt unknown field", "receipt-unknown", "cutover receipt does not match"},
		{"source receipt mismatch", "source-receipt-drift", "cutover receipt does not match"},
		{"cutover state tampered", "state-drift", "cutover state has invalid schema"},
		{"equal restore prefixes", "prefix-state-invalid", "cutover state does not match"},
		{"service UID drift", "service-drift", "Service UID or target selector changed"},
		{"pod replacement", "pod-drift", "target Pod UID/readiness/restart fence failed"},
		{"foreign endpoint", "endpoint-drift", "EndpointSlice target Pod UID set changed"},
		{"probe failure", "probe-fail", "probe failed"},
		{"invalid probe evidence", "probe-invalid", "invalid evidence"},
		{"revision moved backwards", "probe-backwards", "revision moved backwards"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newAuditFixture(t)
			require.NoError(t, os.WriteFile(filepath.Join(f.dir, tc.drift), []byte("1"), 0o600))
			f.run(t, false, "", tc.want)
			_, err := os.Stat(filepath.Join(f.state, "audit-1.receipt.json"))
			require.ErrorIs(t, err, os.ErrNotExist)
		})
	}
}

func TestPostRestoreAuditRejectsUnsafeAuditPrefix(t *testing.T) {
	for _, tc := range []struct {
		name, prefix, want string
	}{
		{name: "relative", prefix: "relative", want: "absolute key prefix"},
		{name: "root", prefix: "/", want: "must not target"},
		{name: "registry", prefix: "/registry", want: "must not target"},
		{name: "registry child", prefix: "/registry/pods", want: "must not target"},
		{name: "control", prefix: "/__kubebrain/audit\nprobe", want: "control characters"},
		{name: "del", prefix: "/__kubebrain/audit\x7fprobe", want: "control characters"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newAuditFixture(t)
			f.run(t, false, "AUDIT_PREFIX="+tc.prefix, tc.want)
			require.NoFileExists(t, filepath.Join(f.state, "audit-1.receipt.json"))
			require.NoFileExists(t, filepath.Join(f.dir, "probes"))
		})
	}
}

func TestPostRestoreAuditRejectsUnsafePublicEndpoint(t *testing.T) {
	for _, tc := range []struct {
		name     string
		endpoint string
	}{
		{name: "control character", endpoint: "https://service:2379\nother"},
		{name: "DEL", endpoint: "https://service:2379\x7fother"},
		{name: "quote", endpoint: `https://service:2379"other`},
		{name: "backslash", endpoint: `https://service:2379\other`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newAuditFixture(t)
			f.run(t, false, "PUBLIC_ENDPOINT="+tc.endpoint, "PUBLIC_ENDPOINT contains unsupported characters")
			require.NoFileExists(t, filepath.Join(f.state, "audit-1.receipt.json"))
			require.NoFileExists(t, filepath.Join(f.dir, "probes"))
		})
	}
}

type auditFixture struct {
	dir, state string
	env        []string
}

func newAuditFixture(t *testing.T) *auditFixture {
	t.Helper()
	dir := t.TempDir()
	state := filepath.Join(dir, "state")
	require.NoError(t, os.Mkdir(state, 0o700))
	cutoverState := filepath.Join(dir, "cutover.state")
	require.NoError(t, os.WriteFile(cutoverState, []byte(
		"HEADER\tkubebrain.restore-cutover.state.v1\tinstance-a\tcutover-1\tns-a\tkubebrain\tsource\ttarget\tuid-service\t"+auditArtifactSHA256+"\t42\t/registry\t/restored\n"+
			"SERVICE\tuid-service\t10\n"+
			"POD\tsource\tkb-source-0\tuid-source-0\t0\n"+
			"POD\tsource\tkb-source-1\tuid-source-1\t0\n"+
			"POD\ttarget\tkb-target-0\tuid-target-0\t0\n"+
			"POD\ttarget\tkb-target-1\tuid-target-1\t0\n"), 0o600))
	cutoverReceipt := filepath.Join(dir, "cutover.json")
	stateData, err := os.ReadFile(cutoverState)
	require.NoError(t, err)
	stateSHA := sha256.Sum256(stateData)
	require.NoError(t, os.WriteFile(cutoverReceipt, []byte(fmt.Sprintf(`{
	  "format":"kubebrain.restore-cutover.receipt.v1","operation_id":"cutover-1",
	  "instance":"instance-a","service_namespace":"ns-a","service_name":"kubebrain",
	  "service_uid":"uid-service","source_instance":"source","target_instance":"target","artifact_sha256":"%s",
	  "cutover_state_sha256":"%x",
	  "snapshot_revision":42,"replicas":2,"pod_uids_unchanged":true,"endpoint_uids_matched":true,
	  "public_data_verified":true,"completed_at_unix":100
	}`, auditArtifactSHA256, stateSHA)), 0o600))

	kubectl := filepath.Join(dir, "kubectl")
	writeTrafficExecutable(t, kubectl, `#!/usr/bin/env bash
set -euo pipefail
if [[ " $* " == *" get service "* ]]; then
  uid=uid-service; [[ -f "$FAKE_DIR/service-drift" ]] && uid=uid-new
  printf '{"metadata":{"uid":"%s"},"spec":{"selector":{"app.kubernetes.io/name":"kubebrain","app.kubernetes.io/instance":"target"}}}\n' "$uid"
elif [[ " $* " == *" get pods "* ]]; then
  uid=uid-target-0; [[ -f "$FAKE_DIR/pod-drift" ]] && uid=uid-new
  printf '{"items":[
    {"metadata":{"name":"kb-target-0","uid":"%s"},"status":{"conditions":[{"type":"Ready","status":"True"}],"containerStatuses":[{"ready":true,"restartCount":0}]}},
    {"metadata":{"name":"kb-target-1","uid":"uid-target-1"},"status":{"conditions":[{"type":"Ready","status":"True"}],"containerStatuses":[{"ready":true,"restartCount":0}]}}
  ]}\n' "$uid"
elif [[ " $* " == *" get endpointslices "* ]]; then
  uid=uid-target-1; [[ -f "$FAKE_DIR/endpoint-drift" ]] && uid=uid-foreign
  printf '{"items":[{"metadata":{"ownerReferences":[{"kind":"Service","uid":"uid-service","controller":true}]},"endpoints":[
    {"conditions":{"ready":true,"serving":true,"terminating":false},"targetRef":{"kind":"Pod","uid":"uid-target-0"}},
    {"conditions":{"ready":true,"serving":true,"terminating":false},"targetRef":{"kind":"Pod","uid":"%s"}}
  ]}]}\n' "$uid"
else
  echo "unexpected kubectl call: $*" >&2; exit 1
fi
`)
	probe := filepath.Join(dir, "probe")
	writeTrafficExecutable(t, probe, `#!/usr/bin/env bash
set -euo pipefail
[[ ! -f "$FAKE_DIR/probe-fail" ]] || { echo probe failed >&2; exit 1; }
printf 'x\n' >>"$FAKE_DIR/probes"
if [[ -f "$FAKE_DIR/probe-invalid" ]]; then echo '{}'; exit 0; fi
count=$(wc -l <"$FAKE_DIR/probes")
if [[ "${PUBLISH_RECEIPT_DURING_PROBE:-false}" == true && ! -f "$STATE_DIR/$OPERATION_ID.receipt.json" ]]; then
  cutover_state_sha="$(sha256sum "$CUTOVER_STATE_INPUT" | cut -d ' ' -f1)"
  cutover_receipt_sha="$(sha256sum "$CUTOVER_RECEIPT_INPUT" | cut -d ' ' -f1)"
  printf '{"all_probes_succeeded":true,"artifact_sha256":"`+auditArtifactSHA256+`","completed":true,"completed_at_unix":2,"cutover_operation_id":"cutover-1","cutover_receipt_sha256":"%s","cutover_state_sha256":"%s","duration_seconds":%s,"first_probe_revision":1,"format":"kubebrain.post-restore-audit.receipt.v1","instance":"%s","interval_seconds":%s,"last_probe_revision":2,"operation_id":"%s","replicas":%s,"samples":2,"service_uid":"uid-service","snapshot_revision":42,"started_at_unix":1,"target_instance":"%s","topology_unchanged":true}\n' \
    "$cutover_receipt_sha" "$cutover_state_sha" "$AUDIT_DURATION_SECONDS" "$INSTANCE" "$AUDIT_INTERVAL_SECONDS" "$OPERATION_ID" "$EXPECTED_REPLICAS" "$TARGET_INSTANCE" >"$STATE_DIR/$OPERATION_ID.receipt.json"
  chmod 600 "$STATE_DIR/$OPERATION_ID.receipt.json"
fi
if [[ -f "$FAKE_DIR/probe-backwards" ]]; then count=$((100-count)); fi
printf '{"format":"kubebrain.etcd-audit-probe.v1","put_revision":%d,"read_revision":%d,"delete_revision":%d,"lease_ttl":60}\n' "$count" "$count" "$count"
`)
	realSHA256Sum, err := exec.LookPath("sha256sum")
	require.NoError(t, err)
	sha256sum := filepath.Join(dir, "sha256sum")
	writeTrafficExecutable(t, sha256sum, `#!/usr/bin/env bash
set -euo pipefail
"$REAL_SHA256SUM" "$@"
if [[ "$#" -ge 1 && "$1" == "$AUDIT_CUTOVER_STATE_SOURCE" &&
  "${TAMPER_CUTOVER_STATE_DURING_SHA256:-false}" == true &&
  ! -f "$FAKE_DIR/state-sha256-tampered" ]]; then
  touch "$FAKE_DIR/state-sha256-tampered"
  printf 'UNKNOWN\trow\n' >>"$AUDIT_CUTOVER_STATE_SOURCE"
fi
if [[ "$#" -ge 1 && "$1" == "$AUDIT_CUTOVER_RECEIPT_SOURCE" &&
  "${TAMPER_CUTOVER_RECEIPT_DURING_SHA256:-false}" == true &&
  ! -f "$FAKE_DIR/receipt-sha256-tampered" ]]; then
  touch "$FAKE_DIR/receipt-sha256-tampered"
  printf ' ' >>"$AUDIT_CUTOVER_RECEIPT_SOURCE"
fi
`)
	realJQ, err := exec.LookPath("jq")
	require.NoError(t, err)
	jq := filepath.Join(dir, "jq-wrapper")
	writeTrafficExecutable(t, jq, `#!/usr/bin/env bash
set -euo pipefail
"$REAL_JQ" "$@"
if [[ " $* " == *" -cnS "* && " $* " == *" kubebrain.post-restore-audit.receipt.v1 "* ]]; then
  [[ "${TAMPER_CUTOVER_STATE_DURING_AUDIT_RECEIPT_JQ:-false}" != true ]] ||
    printf 'UNKNOWN\trow\n' >>"$CUTOVER_STATE_INPUT"
  [[ "${TAMPER_CUTOVER_RECEIPT_DURING_AUDIT_RECEIPT_JQ:-false}" != true ]] ||
    printf ' ' >>"$CUTOVER_RECEIPT_INPUT"
fi
`)
	return &auditFixture{dir: dir, state: state, env: []string{
		"OPERATION_ID=audit-1", "INSTANCE=instance-a", "STATE_DIR=" + state,
		"CUTOVER_STATE_INPUT=" + cutoverState, "CUTOVER_RECEIPT_INPUT=" + cutoverReceipt,
		"SERVICE_NAMESPACE=ns-a", "SERVICE_NAME=kubebrain", "TARGET_INSTANCE=target",
		"EXPECTED_REPLICAS=2", "PUBLIC_ENDPOINT=https://service:2379",
		"AUDIT_DURATION_SECONDS=1", "AUDIT_INTERVAL_SECONDS=1", "MIN_SAMPLES=2",
		"KUBECTL=" + kubectl, "PROBE=" + probe, "FAKE_DIR=" + dir,
		"PATH=" + dir + string(os.PathListSeparator) + os.Getenv("PATH"),
		"REAL_SHA256SUM=" + realSHA256Sum,
		"AUDIT_CUTOVER_STATE_SOURCE=" + cutoverState,
		"AUDIT_CUTOVER_RECEIPT_SOURCE=" + cutoverReceipt,
		"JQ=" + jq, "REAL_JQ=" + realJQ,
	}}
}

func (f *auditFixture) replaceCutoverArtifactDigest(t *testing.T, digest string) {
	t.Helper()
	statePath := filepath.Join(f.dir, "cutover.state")
	state := strings.Replace(string(mustRead(t, statePath)), auditArtifactSHA256, digest, 1)
	require.NoError(t, os.WriteFile(statePath, []byte(state), 0o600))

	receiptPath := filepath.Join(f.dir, "cutover.json")
	var receipt map[string]any
	require.NoError(t, json.Unmarshal(mustRead(t, receiptPath), &receipt))
	receipt["artifact_sha256"] = digest
	receipt["cutover_state_sha256"] = fileDigest(t, statePath)
	encoded, err := json.Marshal(receipt)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(receiptPath, append(encoded, '\n'), 0o600))
}

func (f *auditFixture) run(t *testing.T, ok bool, extra string, outputs ...string) {
	t.Helper()
	if _, err := os.Stat(filepath.Join(f.dir, "receipt-drift")); err == nil {
		path := filepath.Join(f.dir, "cutover.json")
		data, readErr := os.ReadFile(path)
		require.NoError(t, readErr)
		require.NoError(t, os.WriteFile(path,
			[]byte(strings.Replace(string(data), `"service_uid":"uid-service"`, `"service_uid":"uid-new"`, 1)),
			0o600))
	}
	if _, err := os.Stat(filepath.Join(f.dir, "receipt-unknown")); err == nil {
		path := filepath.Join(f.dir, "cutover.json")
		data, readErr := os.ReadFile(path)
		require.NoError(t, readErr)
		receipt := strings.TrimSpace(string(data))
		receipt = strings.TrimSuffix(receipt, "}") + `,"unexpected":true}` + "\n"
		require.NoError(t, os.WriteFile(path, []byte(receipt), 0o600))
	}
	if _, err := os.Stat(filepath.Join(f.dir, "source-receipt-drift")); err == nil {
		path := filepath.Join(f.dir, "cutover.json")
		data, readErr := os.ReadFile(path)
		require.NoError(t, readErr)
		require.NoError(t, os.WriteFile(path,
			[]byte(strings.Replace(string(data), `"source_instance":"source"`, `"source_instance":"foreign"`, 1)),
			0o600))
	}
	if _, err := os.Stat(filepath.Join(f.dir, "state-drift")); err == nil {
		path := filepath.Join(f.dir, "cutover.state")
		data, readErr := os.ReadFile(path)
		require.NoError(t, readErr)
		require.NoError(t, os.WriteFile(path, append(data, '\n'), 0o600))
	}
	if _, err := os.Stat(filepath.Join(f.dir, "prefix-state-invalid")); err == nil {
		path := filepath.Join(f.dir, "cutover.state")
		data, readErr := os.ReadFile(path)
		require.NoError(t, readErr)
		require.NoError(t, os.WriteFile(path,
			[]byte(strings.Replace(string(data), "/registry\t/restored", "/same\t/same", 1)),
			0o600))
	}
	env := append([]string{}, f.env...)
	if extra != "" {
		env = append(env, extra)
	}
	out, err := runAuditRestoredInstance(t, env)
	if ok {
		require.NoError(t, err, string(out))
	} else {
		require.Error(t, err, string(out))
	}
	for _, wanted := range outputs {
		require.Contains(t, string(out), wanted)
	}
}

func runAuditRestoredInstance(t *testing.T, env []string) ([]byte, error) {
	t.Helper()
	return runProductionScriptCommand(t, "audit-restored-instance.sh", env)
}
