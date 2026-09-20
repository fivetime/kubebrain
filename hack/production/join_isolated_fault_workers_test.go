package production_test

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

func TestIsolatedFaultJoinRejectsHostInvocation(t *testing.T) {
	out, err := exec.Command("/bin/bash", "join-isolated-fault-workers.sh", t.TempDir()).CombinedOutput()
	require.Error(t, err)
	require.NotContains(t, string(out), "ISOLATED_PID_NAMESPACE_EMPTY")
}

// These are real private Linux PID namespaces, not a mock /proc. The namespace
// init exits after each case, which destroys only that case's test processes.
func TestIsolatedFaultJoinPIDNamespace(t *testing.T) {
	if _, err := exec.LookPath("unshare"); err != nil {
		t.Skip("unshare unavailable: isolated PID namespace tests not executed")
	}
	check := exec.Command("unshare", "--fork", "--pid", "--mount-proc", "/bin/true")
	if out, err := check.CombinedOutput(); err != nil {
		if strings.Contains(string(out), "Operation not permitted") {
			t.Skip("PID namespace creation denied: integration coverage unavailable")
		}
		require.NoError(t, err, string(out))
	}
	script, err := filepath.Abs("join-isolated-fault-workers.sh")
	require.NoError(t, err)
	for _, mode := range []string{"clean", "escaped-session", "wrong-namespace", "wrong-start", "wrong-owner", "wrong-boot", "public-identity", "extra-input", "nested-parent"} {
		t.Run(mode, func(t *testing.T) {
			owner := t.TempDir()
			require.NoError(t, os.Chmod(owner, 0700))
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			init := `set -euo pipefail
umask 077
script=$1; owner=$2; mode=$3
namespace=$(stat -Lc '%d:%i' /proc/1/ns/pid)
IFS= read -r boot </proc/sys/kernel/random/boot_id
IFS= read -r init_stat </proc/1/stat
read -r -a fields <<<"${init_stat##*) }"
started=${fields[19]}
directory=$(stat -c '%d:%i' "$owner")
case $mode in
wrong-namespace) namespace=0:0;;
wrong-start) started=$((started+1));;
wrong-owner) directory=0:0;;
wrong-boot) boot=00000000-0000-0000-0000-000000000000;;
esac
printf 'v1\t%s\t%s\t%s\t%s\n' "$namespace" "$boot" "$started" "$directory" > "$owner/join-namespace.tsv"
case $mode in
public-identity) chmod 0644 "$owner/join-namespace.tsv";;
extra-input) printf 'unexpected' >> "$owner/join-namespace.tsv";;
escaped-session)
  setsid /bin/sleep 30 >/dev/null 2>&1 &
  child=$!
  # Wait until setsid has actually moved the child into its own session.
  for ((i=0;i<100;i++)); do
    IFS= read -r child_stat <"/proc/$child/stat"
    read -r -a child_fields <<<"${child_stat##*) }"
    [[ ${child_fields[3]} == "$child" ]] && break
    /bin/sleep 0.01
  done
  [[ ${child_fields[3]} == "$child" ]] || exit 99
  ;;
nested-parent)
  /bin/bash -c '/bin/bash "$1" "$2"; exit $?' nested "$script" "$owner"
  exit $?
  ;;
esac
/bin/bash "$script" "$owner"
exit $?
`
			cmd := exec.CommandContext(ctx, "unshare", "--fork", "--pid", "--mount-proc", "--kill-child=KILL", "/bin/bash", "-c", init, "init", script, owner, mode)
			out, err := cmd.CombinedOutput()
			require.NoError(t, ctx.Err(), string(out))
			if mode == "clean" {
				require.NoError(t, err, string(out))
				require.Equal(t, "ISOLATED_PID_NAMESPACE_EMPTY_NOT_FAULT_ACCEPTANCE\n", string(out))
			} else {
				require.Error(t, err, string(out))
				require.NotContains(t, string(out), "ISOLATED_PID_NAMESPACE_EMPTY")
				if mode == "escaped-session" {
					require.Equal(t, 75, cmd.ProcessState.ExitCode(), string(out))
					require.Contains(t, string(out), "JOIN_REFUSED_PROCESS_REMAINS")
				}
			}
		})
	}
}
