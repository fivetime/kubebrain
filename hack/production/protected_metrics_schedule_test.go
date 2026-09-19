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

func TestProtectedMetricsSchedule(t *testing.T) {
	library, err := filepath.Abs("protected-stack-session.sh")
	require.NoError(t, err)
	for _, scenario := range []string{"success", "late-start", "expired", "future", "changed-origin", "repeat", "invalid-offset", "negative-offset", "capture-failed", "sleep-failed", "oversleep"} {
		t.Run(scenario, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			out, err := exec.CommandContext(ctx, "bash", "-c", metricsScheduleFixture, "test", library, t.TempDir(), scenario).CombinedOutput()
			require.NoError(t, ctx.Err(), string(out))
			require.NoError(t, err, string(out))
			require.Contains(t, string(out), "SCHEDULE_ASSERTIONS_PASSED")
		})
	}
}

const metricsScheduleFixture = `
set -euo pipefail
source "$1"
stack_owner=$2 scenario=$3
stack_session=$stack_owner
stack_pids=(1 2)
stack_bound_info_port=$stack_info_port
stack_bound_anonymous_port=$stack_anonymous_port
origin=1800000000000000000
test_clock_ns=$origin
offset=27000000000
calls=0 sleeps=0
date() { printf '%s\n' "$test_clock_ns"; }
stack_session_run() {
 stack_session_budget || return
 if [[ $1 == sleep ]]; then
  sleeps=$((sleeps+1))
  [[ $scenario != sleep-failed ]] || return 17
  [[ $2 == 1.000000000s ]]
  test_clock_ns=$((test_clock_ns+1000000000))
  if [[ $scenario == oversleep ]]; then test_clock_ns=$((origin+30000000000)); fi
 else "$@" || return
 fi
 stack_session_budget
}
stack_session_capture_metrics() {
 calls=$((calls+1))
 [[ $1 == "$origin" && $stack_fault_start == "$origin" && $test_clock_ns -ge $((origin+offset)) && $test_clock_ns -lt $((origin+30000000000)) ]]
 [[ $scenario != capture-failed ]] || return 19
 stack_capture=$stack_owner/capture
 mkdir "$stack_capture"
 printf 'synthetic capture receipt\n' > "$stack_capture/evidence.sha256"
}
case $scenario in
 late-start) test_clock_ns=$((origin+28000000000));;
 expired) test_clock_ns=$((origin+30000000000));;
 future) test_clock_ns=$((origin-1));;
 changed-origin) stack_fault_start=$((origin-1));;
 invalid-offset) offset=30000000000;;
 negative-offset) offset=-1;;
esac
rc=0
stack_session_capture_metrics_at "$origin" "$offset" || rc=$?
case $scenario in
 success|late-start|repeat)
  [[ $rc == 0 && $calls == 1 && -s $stack_schedule/COMPLETE ]]
  sha256sum -c "$stack_schedule/evidence.sha256" >/dev/null
  if [[ $scenario == late-start ]]; then [[ $sleeps == 0 ]]; else [[ $sleeps == 27 ]]; fi
  if [[ $scenario == repeat ]]; then
   rc=0; stack_session_capture_metrics_at "$origin" "$offset" || rc=$?
   [[ $rc == 2 && $calls == 1 ]]
  fi;;
 expired|oversleep) [[ $rc == 124 && $calls == 0 ]];;
 sleep-failed) [[ $rc == 17 && $calls == 0 ]];;
 capture-failed) [[ $rc == 19 && $calls == 1 ]];;
 *) [[ $rc == 2 && $calls == 0 ]];;
esac
if [[ $rc != 0 && $scenario != repeat ]]; then [[ -z $(find "$stack_owner" -name COMPLETE -print) ]]; fi
printf 'SCHEDULE_ASSERTIONS_PASSED\n'
`

func TestProtectedMetricsScheduleOuterCancellation(t *testing.T) {
	library, err := filepath.Abs("protected-stack-session.sh")
	require.NoError(t, err)
	owner := t.TempDir()
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	fixture := `
set -euo pipefail
source "$1"
stack_owner=$2
stack_session=$stack_owner
stack_pids=(1 2)
stack_bound_info_port=$stack_info_port
stack_bound_anonymous_port=$stack_anonymous_port
stack_session_run() {
 if [[ $1 == sleep ]]; then
  command timeout --foreground --kill-after=1s 25s bash -c 'printf "%s\n" "$BASHPID" >> "$2/wait-entered"; exec /usr/bin/sleep "$1"' worker "$2" "$stack_owner"
 else command timeout --foreground --kill-after=1s 25s "$@"; fi
}
stack_session_capture_metrics() { touch "$stack_owner/captured"; }
stack_session_capture_metrics_at "$(date -u +%s%N)" 27000000000
`
	out, err := exec.CommandContext(ctx, "timeout", "--kill-after=1s", "2s", "bash", "-c", fixture, "test", library, owner).CombinedOutput()
	require.NoError(t, ctx.Err(), string(out))
	var exit *exec.ExitError
	require.ErrorAs(t, err, &exit)
	require.Equal(t, 124, exit.ExitCode())
	_, err = os.Stat(filepath.Join(owner, "wait-entered"))
	require.NoError(t, err)
	pids, err := os.ReadFile(filepath.Join(owner, "wait-entered"))
	require.NoError(t, err)
	for _, pid := range strings.Fields(string(pids)) {
		require.Error(t, exec.Command("kill", "-0", pid).Run(), "scheduled wait child survived: %s", pid)
	}
	_, err = os.Stat(filepath.Join(owner, "captured"))
	require.True(t, os.IsNotExist(err))
	markers, err := filepath.Glob(filepath.Join(owner, "metrics-schedule.*", "COMPLETE"))
	require.NoError(t, err)
	require.Empty(t, markers)
}
