package compat

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// Execute the actual cleanup function without starting an apiserver or reaching
// a cluster. A lease appearing after preflight is not proof of ownership.
func TestAPIServerCleanupNeverRevokesUnownedLeases(t *testing.T) {
	for _, script := range []string{"apiserver-smoke.sh", "apiserver-watch-soak.sh", "incluster-apiserver-smoke.sh"} {
		for _, scenario := range []string{"unchanged", "foreign-added", "baseline-expired", "list-error", "natural-expiry"} {
			t.Run(script+"/"+scenario, func(t *testing.T) {
				data, err := os.ReadFile(filepath.Join("..", "dev", script))
				require.NoError(t, err)
				body := string(data)
				start := strings.Index(body, "cleanup() {\n")
				require.GreaterOrEqual(t, start, 0)
				end := strings.Index(body[start:], "\n}\n")
				require.Greater(t, end, 0)
				cleanup := body[start : start+end+3]
				log := filepath.Join(t.TempDir(), "commands")
				prelude := `set -euo pipefail
prefix_owned=true
process_owned=false
namespace_created=false
work_dir_created=false
bin_lock_owned=false
port_lock_owned=false
service_created=false
pod_created=false
secret_created=false
log_owned=false
baseline_lease_ids=aa
expired=false
ETCD_PREFIX=/registry-kubebrain-apiserver-test-owned
ETCDCTL=(fake_etcdctl)
fake_etcdctl() {
  printf '%s\n' "$*" >>"$COMMAND_LOG"
  case "$1" in
    del) echo 0 ;;
    get) echo '{"count":0,"kvs":[]}' ;;
    lease) return 0 ;;
    *) return 1 ;;
  esac
}
list_lease_ids() {
  case "$SCENARIO" in
    unchanged) echo aa ;;
    foreign-added) printf 'aa\nbb\n' ;;
    baseline-expired) : ;;
    list-error) return 1 ;;
    natural-expiry) if [[ "$expired" == true ]]; then echo aa; else printf 'aa\nbb\n'; fi ;;
  esac
}
# Skip real waiting, without weakening the production deadline.
sleep() { SECONDS=$((SECONDS+120)); expired=true; }
`
				helper := filepath.Join("..", "dev", "apiserver-lease-cleanup.sh")
				_, err = os.Stat(helper)
				require.NoError(t, err)
				prelude += "source " + helper + "\n"
				output, err := runCompatCommandContext(t, context.Background(), "bash", []string{"-c", prelude + cleanup + "\ncleanup\n"}, []string{"COMMAND_LOG=" + log, "SCENARIO=" + scenario})
				if scenario == "unchanged" || scenario == "natural-expiry" {
					require.NoError(t, err, "%s", output)
				} else {
					require.Error(t, err, "%s", output)
					var exitErr *exec.ExitError
					require.ErrorAs(t, err, &exitErr)
					require.Equal(t, 70, exitErr.ExitCode())
				}
				commands, readErr := os.ReadFile(log)
				require.NoError(t, readErr)
				require.Contains(t, string(commands), "del /registry-kubebrain-apiserver-test-owned --prefix")
				require.NotContains(t, string(commands), "lease revoke", "a global lease-list delta never grants ownership")
			})
		}
	}
}
