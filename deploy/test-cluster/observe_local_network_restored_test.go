package testcluster_test

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// Stub only subprocess boundaries here; the individual real observer scripts
// have separate fixture tests and live read-only baseline evidence.
func TestNetworkRestoredObserverOrder(t *testing.T) {
	source, err := os.ReadFile("observe-local-network-restored.sh")
	require.NoError(t, err)
	for _, mode := range []string{"success", "labelled-success", "pending-identity", "fatal-identity", "pending-before", "tcp-fails", "pending-after", "fatal-after", "input-changed"} {
		t.Run(mode, func(t *testing.T) {
			t.Parallel() // Each case owns its scripts, evidence and environment.
			dir := t.TempDir()
			require.NoError(t, os.Chmod(dir, 0700))
			require.NoError(t, os.WriteFile(filepath.Join(dir, "observe-local-network-restored.sh"), source, 0600))
			expected, targets := filepath.Join(dir, "expected.json"), filepath.Join(dir, "targets.json")
			require.NoError(t, os.WriteFile(expected, []byte("{}"), 0600))
			require.NoError(t, os.WriteFile(targets, []byte("[]"), 0600))
			policy := `set -euo pipefail
[[ $2 == "$OBSERVATION_MODE" && $3 == policy-uid && $4 == kb-term-test ]] || exit 99
stage=before
[[ ! -f "$1/policy-seen" ]] || stage=after
touch "$1/policy-seen"
printf 'policy-%s\n' "$stage" >> "$1/order"
[[ $SCENARIO != pending-$stage ]] || exit 75
[[ $SCENARIO != fatal-$stage ]] || exit 65
if [[ $SCENARIO == input-changed && $stage == after ]]; then printf changed >> "$5"; fi
`
			tcp := `set -euo pipefail
printf 'tcp\n' >> "$1/order"
[[ $SCENARIO != tcp-fails ]] || exit 23
`
			identity := `set -euo pipefail
[[ $2 == absent && $4 == term-test ]] || exit 99
printf 'identity\n' >> "$1/order"
[[ $SCENARIO != pending-identity ]] || exit 75
[[ $SCENARIO != fatal-identity ]] || exit 65
`
			require.NoError(t, os.WriteFile(filepath.Join(dir, "observe-local-fault-label.sh"), []byte(identity), 0600))
			require.NoError(t, os.WriteFile(filepath.Join(dir, "observe-local-policy-state.sh"), []byte(policy), 0600))
			require.NoError(t, os.WriteFile(filepath.Join(dir, "observe-local-backend-tcp.sh"), []byte(tcp), 0600))
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			observationMode := "absent-unlabelled"
			if mode == "labelled-success" {
				observationMode = "absent"
			}
			cmd := exec.CommandContext(ctx, "bash", filepath.Join(dir, "observe-local-network-restored.sh"), dir, observationMode, "policy-uid", "kb-term-test", expected, targets)
			cmd.Env = append(os.Environ(), "SCENARIO="+mode, "OBSERVATION_MODE="+observationMode)
			output, err := cmd.CombinedOutput()
			require.NoError(t, ctx.Err())
			if mode == "success" || mode == "labelled-success" {
				require.NoError(t, err, string(output))
			} else {
				var exit *exec.ExitError
				require.ErrorAs(t, err, &exit, string(output))
				if strings.HasPrefix(mode, "pending-") {
					require.Equal(t, 75, exit.ExitCode())
				}
			}
			order, err := os.ReadFile(filepath.Join(dir, "order"))
			require.NoError(t, err)
			want := "policy-before\ntcp\npolicy-after\n"
			if mode == "pending-before" {
				want = "policy-before\n"
			}
			if mode == "tcp-fails" {
				want = "policy-before\ntcp\n"
			}
			if mode != "labelled-success" {
				want = "identity\n" + want
			}
			if mode == "pending-identity" || mode == "fatal-identity" {
				want = "identity\n"
			}
			require.Equal(t, want, string(order))
			observations, err := filepath.Glob(filepath.Join(dir, "network-observation.*"))
			require.NoError(t, err)
			require.Len(t, observations, 1)
			proof := filepath.Join(observations[0], "evidence.sha256")
			if mode == "success" || mode == "labelled-success" {
				check := exec.Command("sha256sum", "-c", proof)
				data, err := check.CombinedOutput()
				require.NoError(t, err, string(data))
			} else {
				_, err := os.Stat(proof)
				require.True(t, os.IsNotExist(err))
			}
		})
	}
}
