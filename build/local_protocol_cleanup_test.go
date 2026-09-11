package build_test

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

func TestLocalProtocolEntryRejectsMissingConsentAndExternalTargets(t *testing.T) {
	for _, args := range [][]string{nil, {"--endpoint", "127.0.0.1:2379"},
		{"--allow-local-containers", "--endpoint", "127.0.0.1:2379"}, {"--allow-local-containers=true"}} {
		t.Run(strings.Join(args, " "), func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			command := exec.CommandContext(ctx, "bash", append([]string{"../hack/backend-integration/run-real-local.sh"}, args...)...)
			output, err := command.CombinedOutput()
			var exitErr *exec.ExitError
			require.ErrorAs(t, err, &exitErr, string(output))
			require.Equal(t, 2, exitErr.ExitCode(), string(output))
			require.Contains(t, string(output), "Usage:")
			require.NotContains(t, string(output), "LOCAL_PROTOCOL_START")
		})
	}
}

// Execute the actual cleanup function with a recording Docker substitute. Do
// not source the entry: that could provision real resources during a unit test.
func TestLocalProtocolCleanupOwnershipAndFailures(t *testing.T) {
	source, err := os.ReadFile("../hack/backend-integration/run-real-local.sh")
	require.NoError(t, err)
	start := strings.Index(string(source), "cleanup() {\n")
	end := strings.Index(string(source), "\ntrap cleanup EXIT")
	require.GreaterOrEqual(t, start, 0)
	require.Greater(t, end, start)
	cleanup := string(source[start:end])
	for _, tc := range []struct {
		name       string
		initial    string
		wantExit   int
		removeTiKV bool
		removePD   bool
		removeNet  bool
	}{
		{"owned", "0", 0, true, true, true},
		{"owned", "23", 23, true, true, true},
		{"foreign-container", "0", 1, false, true, false},
		{"foreign-network", "0", 1, true, true, false},
		{"busy-network", "0", 1, true, true, false},
		{"daemon-down", "0", 1, false, false, false},
		{"absent", "0", 0, false, false, false},
		{"inspect-failed-present", "0", 1, false, false, false},
		{"remove-failed", "0", 1, true, true, false},
		{"logs-failed", "0", 1, true, true, true},
		{"network-remove-failed", "0", 1, true, true, true},
		{"missing-network-containers", "0", 1, true, true, false},
		{"symlink-binary", "0", 0, true, true, true},
	} {
		t.Run(tc.name+"/exit-"+tc.initial, func(t *testing.T) {
			dir := t.TempDir()
			binary := filepath.Join(dir, "protocol.test")
			unrelated := filepath.Join(dir, "preserve.txt")
			require.NoError(t, os.WriteFile(binary, []byte("owned executable"), 0o600))
			require.NoError(t, os.WriteFile(unrelated, []byte("preserve evidence"), 0o600))
			if tc.name == "symlink-binary" {
				require.NoError(t, os.Remove(binary))
				require.NoError(t, os.Symlink(unrelated, binary))
			}
			ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
			defer cancel()
			command := exec.CommandContext(ctx, "bash", "-c", localProtocolCleanupStub+"\n"+cleanup+"\ntrap cleanup EXIT\nexit \"$INITIAL\" > \"$evidence/id-output\"\n")
			command.Env = append(os.Environ(), "EVIDENCE="+dir, "CASE="+tc.name, "INITIAL="+tc.initial)
			output, runErr := command.CombinedOutput()
			require.Contains(t, string(output), "LOCAL_PROTOCOL_END", "report must escape interrupted command redirection")
			idOutput, readErr := os.ReadFile(filepath.Join(dir, "id-output"))
			require.NoError(t, readErr)
			require.Empty(t, idOutput, "cleanup must not corrupt the resource ID file")
			if tc.wantExit == 0 {
				require.NoError(t, runErr, string(output))
			} else {
				var exitErr *exec.ExitError
				require.ErrorAs(t, runErr, &exitErr, string(output))
				require.Equal(t, tc.wantExit, exitErr.ExitCode(), string(output))
			}
			trace, readErr := os.ReadFile(filepath.Join(dir, "trace"))
			require.NoError(t, readErr)
			for operation, want := range map[string]bool{
				"rm -f tikv-id": tc.removeTiKV, "rm -f pd-id": tc.removePD, "network rm network-id": tc.removeNet,
			} {
				require.Equal(t, want, strings.Contains(string(trace), operation+"\n"), "%s\n%s", operation, trace)
			}
			require.NotContains(t, string(trace), "rm -f foreign-id")
			require.NotContains(t, string(trace), "network rm foreign-network-id")
			if tc.name == "symlink-binary" {
				info, statErr := os.Lstat(binary)
				require.NoError(t, statErr)
				require.NotZero(t, info.Mode()&os.ModeSymlink)
			} else {
				require.NoFileExists(t, binary)
			}
			retained, readErr := os.ReadFile(unrelated)
			require.NoError(t, readErr)
			require.Equal(t, "preserve evidence", string(retained))
		})
	}
}

const localProtocolCleanupStub = `set -euo pipefail
exec {report_fd}>&1
evidence="$EVIDENCE"
owner=ours
label=io.kubebrain.local-protocol-owner
tikv_name=fixture-tikv
pd_name=fixture-pd
network=fixture-network
docker_local() {
  printf '%s\n' "$*" >> "$evidence/trace"
  [[ "$CASE" != daemon-down ]] || return 1
  case "$1 $2" in
    'container inspect')
      case "$CASE" in absent|inspect-failed-present) return 1 ;; esac
      id=pd-id
      actual_owner=ours
      if [[ "$3" == fixture-tikv ]]; then
        id=tikv-id
        if [[ "$CASE" == foreign-container ]]; then actual_owner=theirs; id=foreign-id; fi
      fi
      printf '[{"Id":"%s","Config":{"Labels":{"%s":"%s"}}}]' "$id" "$label" "$actual_owner"
      ;;
    'container ls')
      if [[ "$CASE" == inspect-failed-present ]]; then printf 'fixture-tikv\nfixture-pd\n'; fi
      ;;
    'network inspect')
      case "$CASE" in absent|inspect-failed-present) return 1 ;; esac
      actual_owner=ours
      id=network-id
      containers='{}'
      case "$CASE" in
        foreign-network) actual_owner=theirs; id=foreign-network-id ;;
        busy-network|remove-failed|foreign-container) containers='{"leftover":{}}' ;;
        missing-network-containers) containers=null ;;
      esac
      printf '[{"Id":"%s","Labels":{"%s":"%s"},"Containers":%s}]' "$id" "$label" "$actual_owner" "$containers"
      ;;
    'network ls')
      if [[ "$CASE" == inspect-failed-present ]]; then printf 'fixture-network\n'; fi
      ;;
    'network rm') [[ "$CASE" != network-remove-failed ]] || return 1 ;;
    'rm -f') [[ "$CASE" != remove-failed ]] || return 1 ;;
    logs*) [[ "$CASE" != logs-failed ]] || return 1 ;;
    *) echo "unexpected Docker operation: $*" >&2; return 1 ;;
  esac
}
`
