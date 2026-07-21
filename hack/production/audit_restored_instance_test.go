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

	probes, err := os.ReadFile(filepath.Join(f.dir, "probes"))
	require.NoError(t, err)
	require.GreaterOrEqual(t, len(strings.Split(strings.TrimSpace(string(probes)), "\n")), 3)
}

func TestPostRestoreAuditFailsClosed(t *testing.T) {
	for _, tc := range []struct {
		name, drift, want string
	}{
		{"cutover receipt mismatch", "receipt-drift", "cutover receipt does not match"},
		{"source receipt mismatch", "source-receipt-drift", "cutover receipt does not match"},
		{"cutover state tampered", "state-drift", "cutover receipt does not match"},
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
		"HEADER\tkubebrain.restore-cutover.state.v1\tinstance-a\tcutover-1\tns-a\tkubebrain\tsource\ttarget\tuid-service\tabc123\t42\t/registry\t/restored\n"+
			"SERVICE\tuid-service\t10\n"+
			"POD\ttarget\tkb-target-0\tuid-target-0\t0\n"+
			"POD\ttarget\tkb-target-1\tuid-target-1\t0\n"), 0o600))
	cutoverReceipt := filepath.Join(dir, "cutover.json")
	stateData, err := os.ReadFile(cutoverState)
	require.NoError(t, err)
	stateSHA := sha256.Sum256(stateData)
	require.NoError(t, os.WriteFile(cutoverReceipt, []byte(fmt.Sprintf(`{
	  "format":"kubebrain.restore-cutover.receipt.v1","operation_id":"cutover-1",
	  "instance":"instance-a","service_namespace":"ns-a","service_name":"kubebrain",
	  "service_uid":"uid-service","source_instance":"source","target_instance":"target","artifact_sha256":"abc123",
	  "cutover_state_sha256":"%x",
	  "snapshot_revision":42,"pod_uids_unchanged":true,"endpoint_uids_matched":true,
	  "public_data_verified":true,"completed_at_unix":100
	}`, stateSHA)), 0o600))

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
if [[ -f "$FAKE_DIR/probe-backwards" ]]; then count=$((100-count)); fi
printf '{"format":"kubebrain.etcd-audit-probe.v1","put_revision":%d,"read_revision":%d,"delete_revision":%d,"lease_ttl":60}\n' "$count" "$count" "$count"
`)
	return &auditFixture{dir: dir, state: state, env: []string{
		"OPERATION_ID=audit-1", "INSTANCE=instance-a", "STATE_DIR=" + state,
		"CUTOVER_STATE_INPUT=" + cutoverState, "CUTOVER_RECEIPT_INPUT=" + cutoverReceipt,
		"SERVICE_NAMESPACE=ns-a", "SERVICE_NAME=kubebrain", "TARGET_INSTANCE=target",
		"EXPECTED_REPLICAS=2", "PUBLIC_ENDPOINT=https://service:2379",
		"AUDIT_DURATION_SECONDS=1", "AUDIT_INTERVAL_SECONDS=1", "MIN_SAMPLES=2",
		"KUBECTL=" + kubectl, "PROBE=" + probe, "FAKE_DIR=" + dir,
	}}
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
	cmd := exec.Command("bash", "audit-restored-instance.sh")
	cmd.Env = append(os.Environ(), append(f.env, extra)...)
	out, err := cmd.CombinedOutput()
	if ok {
		require.NoError(t, err, string(out))
	} else {
		require.Error(t, err, string(out))
	}
	for _, wanted := range outputs {
		require.Contains(t, string(out), wanted)
	}
}
