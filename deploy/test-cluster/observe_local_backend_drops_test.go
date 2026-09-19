package testcluster_test

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// Real observer and jq rules, mocked capture/API boundaries. This verifies
// binding/rejection, not packet enforcement in a real Cilium cluster.
func TestBackendDropsObserver(t *testing.T) {
	for _, mode := range []string{"success", "pd-only", "changed-pod", "stale-window", "capture-fails", "malformed-stream", "oversized-stream", "backend-replaced", "input-changed", "expired"} {
		t.Run(mode, func(t *testing.T) {
			t.Parallel()
			root := t.TempDir()
			dir := filepath.Join(root, "deploy", "test-cluster")
			library := filepath.Join(root, "hack", "production")
			owner := filepath.Join(root, "owner")
			for _, path := range []string{dir, library, owner} {
				require.NoError(t, os.MkdirAll(path, 0700))
			}
			write := func(path, contents string) { require.NoError(t, os.WriteFile(path, []byte(contents), 0700)) }
			for _, name := range []string{"observe-local-backend-drops.sh", "../../hack/production/monitor-stream.jq", "../../hack/production/backend-drops.jq", "../../hack/production/same-pod-process.jq"} {
				data, err := os.ReadFile(name)
				require.NoError(t, err)
				write(filepath.Join(dir, name), string(data))
			}
			expected := filepath.Join(owner, "expected.json")
			targets := filepath.Join(owner, "targets.json")
			write(expected, `{"apiVersion":"v1","kind":"Pod","metadata":{"name":"kubebrain-local-0","namespace":"kubebrain-dbaas-test","uid":"original"},"spec":{"nodeName":"worker1","containers":[{"name":"brain"}]},"status":{"podIP":"10.0.0.1","containerStatuses":[{"name":"brain","containerID":"container","imageID":"image","restartCount":0,"state":{"running":{"startedAt":"2026-09-19T00:00:00Z"}}}]}}`)
			var rows []string
			for i := 0; i < 3; i++ {
				rows = append(rows, fmt.Sprintf(`{"name":"kb-local-pd-%d","uid":"pd-%d","ip":"10.0.1.%d","port":2379}`, i, i, i+1))
				rows = append(rows, fmt.Sprintf(`{"name":"kb-local-tikv-%d","uid":"tikv-%d","ip":"10.0.2.%d","port":20160}`, i, i, i+1))
			}
			write(targets, "["+strings.Join(rows, ",")+"]")
			write(filepath.Join(dir, "kubectl"), `#!/usr/bin/env bash
set -euo pipefail
[[ "$*" == *'get pods -o json' ]] || exit 99
jq --arg scenario "$SCENARIO" '{items:map({apiVersion:"v1",kind:"Pod",metadata:{name,uid:(if $scenario=="backend-replaced" then "replacement" else .uid end),namespace:"kubebrain-dbaas-test"},status:{podIP:.ip}})}' "$TARGETS"
`)
			write(filepath.Join(dir, "capture-local-cilium-drops.sh"), `set -euo pipefail
[[ $2 == kubebrain-local-0 && $3 == 1 ]] || exit 99
[[ $SCENARIO != capture-fails ]] || exit 9
umask 077
out=$(mktemp -d "$1/drops.XXXXXXXX")
before=$(mktemp -d "$1/endpoint.XXXXXXXX")
after=$(mktemp -d "$1/endpoint.XXXXXXXX")
echo "EVIDENCE=$out"
for snapshot in "$before" "$after"; do
 cp "$EXPECTED" "$snapshot/pod.json"
 if [[ $SCENARIO == changed-pod ]]; then
  jq '.metadata.uid="replacement"' "$EXPECTED" > "$snapshot/pod.json"
 fi
 printf '{"status":{"id":42}}' > "$snapshot/cep.json"
 printf '0\n' > "$snapshot/capture.exit"
 sha256sum "$snapshot"/*.json > "$snapshot/evidence.sha256"
done
date -u +%Y-%m-%dT%H:%M:%S.%NZ > "$out/started.utc"
[[ $SCENARIO != stale-window ]] || printf '2020-01-01T00:00:00Z\n' > "$out/started.utc"
printf '%s\n' '{"type":"drop","source":42,"reason":"Policy denied","summary":{"tcp":"SYN","l3":{"src":"10.0.0.1","dst":"10.0.1.1"},"l4":{"src":"40000","dst":"2379"}}}' > "$out/monitor.stdout"
if [[ $SCENARIO != pd-only ]]; then
 printf '%s\n' '{"type":"drop","source":42,"reason":"Policy denied","summary":{"tcp":"SYN","l3":{"src":"10.0.0.1","dst":"10.0.2.1"},"l4":{"src":"40000","dst":"20160"}}}' >> "$out/monitor.stdout"
fi
[[ $SCENARIO != malformed-stream ]] || printf '{' >> "$out/monitor.stdout"
[[ $SCENARIO != oversized-stream ]] || truncate -s 4194305 "$out/monitor.stdout"
date -u +%Y-%m-%dT%H:%M:%S.%NZ > "$out/finished.utc"
jq -n --arg before "$before" --arg after "$after" '{scope:"bounded_same_process_drop_capture_only",before:$before,after:$after,endpoint:42,requested_seconds:1,packet_enforcement_proven:false,term_loss_proven:false}' > "$out/result.json"
printf '0\n' > "$out/capture.exit"
sha256sum "$out"/*.utc "$out/monitor.stdout" "$out/result.json" > "$out/evidence.sha256"
[[ $SCENARIO != input-changed ]] || printf ' ' >> "$TARGETS"
echo BOUNDED_CAPTURE_COMPLETED_NOT_ENFORCEMENT_VERDICT
`)
			origin := time.Now()
			if mode == "expired" {
				origin = origin.Add(-31 * time.Second)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			cmd := exec.CommandContext(ctx, "bash", filepath.Join(dir, "observe-local-backend-drops.sh"), owner, expected, targets, "1", strconv.FormatInt(origin.UnixNano(), 10))
			cmd.Env = []string{"PATH=" + dir + ":" + os.Getenv("PATH"), "SCENARIO=" + mode, "EXPECTED=" + expected, "TARGETS=" + targets}
			output, err := cmd.CombinedOutput()
			require.NoError(t, ctx.Err())
			if mode == "success" {
				require.NoError(t, err, string(output))
				require.Contains(t, string(output), "SAME_SOURCE_PD_AND_TIKV_POLICY_DROPS_NOT_TERM_OR_RPC_PROOF")
				matches, globErr := filepath.Glob(filepath.Join(owner, "backend-drops.*", "evidence.sha256"))
				require.NoError(t, globErr)
				require.Len(t, matches, 1)
				data, checkErr := exec.Command("sha256sum", "-c", matches[0]).CombinedOutput()
				require.NoError(t, checkErr, string(data))
			} else {
				require.Error(t, err, string(output))
				require.NotContains(t, string(output), "SAME_SOURCE_PD_AND_TIKV_POLICY_DROPS_NOT_TERM_OR_RPC_PROOF")
			}
		})
	}
}
