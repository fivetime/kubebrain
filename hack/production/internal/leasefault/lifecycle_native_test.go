package leasefault

import (
	"context"
	"crypto/sha256"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/kubewharf/kubebrain/hack/production/internal/metricsworker"
	"github.com/stretchr/testify/require"
)

// Synthetic evidence isolates native lifecycle wiring. Real protected worker
// capture/receipt behavior is tested separately by production script tests.
func nativeObservationFixture(t *testing.T, p FaultPreparation, stderr *os.File, retained func(string)) (OriginalObservation, func()) {
	t.Helper()
	dir := p.Directory
	b := Binding{LeaseID: p.Protocol.LeaseID, ClusterID: p.Protocol.ClusterID, InitialMemberID: p.Protocol.AlarmMemberID, InitialTerm: 2}
	prefix := encode(t, []map[string]any{
		{"phase": "expired_preflight", "at": time.Now().Add(-time.Second), "lease_id": b.LeaseID, "ttl": -1, "member_id": b.InitialMemberID, "raft_term": b.InitialTerm, "cluster_id": b.ClusterID},
		{"phase": "request_sent", "at": time.Now(), "lease_id": b.LeaseID},
	})
	probe := metricsworker.Command{Executable: "/bin/bash", Stderr: stderr, Args: []string{"-c", `set -eu
printf '%s\n' "$$" > "$1/fault.pid"
printf '%s' "$2"
while [[ ! -f "$1/native-release" ]]; do /bin/sleep 0.01; done
/bin/cat "$1/native-response"
`, "probe", dir, string(prefix)}}
	source := "e3914449b57ab6e211ece94cb88fd3318acb2970"
	hash := "3c98f802359a5f185dc6e618691ad6098641a54afa528668c6dfcaf8091ccd88"
	makeWait := func(suffix string, count int) WaitObservation {
		worker := filepath.Join(dir, "stack-worker."+suffix)
		capture := filepath.Join(dir, "stack."+suffix)
		classified := filepath.Join(dir, "expired-wait."+suffix)
		for _, path := range []string{worker, capture, classified} {
			require.NoError(t, os.Mkdir(path, 0700))
		}
		manifest := ""
		for _, name := range []string{"namespace.json", "sts.json", "pod-before.json", "pod-after.json", "probe.json", "goroutines.txt", "timing.tsv", "probe.stderr"} {
			data := []byte("synthetic fixture\n")
			path := filepath.Join(capture, name)
			require.NoError(t, os.WriteFile(path, data, 0600))
			manifest += fmt.Sprintf("%x  %s\n", sha256.Sum256(data), path)
		}
		require.NoError(t, os.WriteFile(filepath.Join(capture, "evidence.sha256"), []byte(manifest), 0600))
		require.NoError(t, os.WriteFile(filepath.Join(capture, "COMPLETE"), []byte("CAPTURE_COMPLETE_WITHIN_CALLER_BUDGET\n"), 0600))
		script := `set -euo pipefail
umask 077
trap 'printf "%s\n" "$?" > "$1/exit-code"' EXIT
printf 'FAULT_READY\n'
IFS= read -r clock
jq -n --arg capture "$2" --arg source "$4" --arg hash "$5" --arg count "$6" --arg clock "$clock" '{capture:$capture,source:$source,source_file_sha256:$hash,expected_candidates:$count,clock:$clock,lease_identity_proven:false,fault_acceptance_proven:false}' > "$3/input.json"
if [[ $6 == 1 ]]; then printf '[{}]\n' > "$3/frames.json"; else printf '[]\n' > "$3/frames.json"; fi
sha256sum "$3/input.json" "$3/frames.json" "$2/evidence.sha256" > "$3/evidence.sha256"
printf 'SOURCE_BOUND_WAIT_CANDIDATES_NOT_LEASE_OR_FAULT_PROOF\n' > "$3/COMPLETE"
printf '%s\n' "$2" > "$1/capture-path"
printf '%s\n' "$3" > "$1/classification-path"
printf '%s\n' "$clock" > "$1/origin-ns"
printf 'FAULT_DONE\t%s\n' "$clock"
`
		return WaitObservation{Directory: worker, Source: source, SourceHash: hash, Count: count, Command: metricsworker.Command{Executable: "/bin/bash", Stderr: stderr, Env: os.Environ(), Args: []string{"-c", script, "stack", worker, capture, classified, source, hash, fmt.Sprint(count)}}, Admit: func(context.Context) error { return nil }, Retain: func(_ context.Context, _ WaitReceipt, err error) error { return err }}
	}
	o := OriginalObservation{Probe: probe, Initial: b, Before: makeWait("beforeaa", 1), After: makeWait("afteraaa", 0), Admit: func(context.Context) error { return nil }, Retain: func(_ context.Context, stage string, _ Binding, _ []byte, err error) error {
		if err == nil {
			retained(stage)
		}
		return err
	}}
	release := func() {
		response := encode(t, []map[string]any{{"phase": "response", "at": time.Now(), "lease_id": b.LeaseID, "ttl": 10, "header": map[string]any{"cluster_id": b.ClusterID, "member_id": b.InitialMemberID, "raft_term": 3, "revision": 1}}})
		require.NoError(t, os.WriteFile(filepath.Join(dir, "native-response"), response, 0600))
		require.NoError(t, os.WriteFile(filepath.Join(dir, "native-release"), nil, 0600))
	}
	return o, release
}
