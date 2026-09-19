package metricsworker

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func faultLog(t *testing.T) *os.File {
	t.Helper()
	f, err := os.OpenFile(filepath.Join(t.TempDir(), "fault.log"), os.O_CREATE|os.O_EXCL|os.O_RDWR, 0600)
	require.NoError(t, err)
	t.Cleanup(func() { _ = f.Close() })
	return f
}

func TestFaultCommandClockEnvironmentAndExit(t *testing.T) {
	t.Setenv("KUBEBRAIN_FAULT_COMMAND_SECRET_TEST", "must-not-inherit")
	for _, fail := range []bool{false, true} {
		t.Run(fmt.Sprint(fail), func(t *testing.T) {
			log := faultLog(t)
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			origin := time.Now().Add(-time.Second)
			spec := Command{Executable: "/bin/bash", Args: []string{"-c", `printf '%s\n' "$#" "$2" "${KUBEBRAIN_FAULT_COMMAND_SECRET_TEST-absent}"; printf 'stderr\n' >&2; exit "$1"`, "fault"}, Stderr: log}
			// The configured arguments precede the appended clock, and are not
			// interpolated by the adapter. Adapt the fixture's positional slots.
			code := "0"
			if fail {
				code = "7"
			}
			spec.Args = append(spec.Args, code)
			originalArgs := append([]string{}, spec.Args...)
			err := RunFaultCommand(ctx, spec, origin)
			if fail {
				require.ErrorContains(t, err, "exit status 7")
			} else {
				require.NoError(t, err)
			}
			require.Equal(t, originalArgs, spec.Args)
			data, err := os.ReadFile(log.Name())
			require.NoError(t, err)
			require.Equal(t, fmt.Sprintf("2\n%d\nabsent\nstderr\n", origin.UnixNano()), string(data))
		})
	}
}

func TestFaultCommandRejectsInvalidBudgetBeforeStart(t *testing.T) {
	for _, mode := range []string{"nil", "no-deadline", "late-deadline", "expired", "future", "canceled", "no-log", "public-log"} {
		t.Run(mode, func(t *testing.T) {
			log := faultLog(t)
			origin := time.Now().Add(-time.Second)
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			spec := Command{Executable: "/bin/bash", Args: []string{"-c", "echo started", "fault"}, Stderr: log}
			switch mode {
			case "nil":
				ctx = nil
			case "no-deadline":
				ctx = context.Background()
			case "late-deadline":
				var lateCancel context.CancelFunc
				ctx, lateCancel = context.WithTimeout(context.Background(), time.Minute)
				defer lateCancel()
			case "expired":
				origin = origin.Add(-time.Minute)
			case "future":
				origin = time.Now().Add(time.Minute)
			case "canceled":
				cancel()
			case "no-log":
				spec.Stderr = nil
			case "public-log":
				require.NoError(t, log.Chmod(0644))
			}
			require.Error(t, RunFaultCommand(ctx, spec, origin))
			data, err := os.ReadFile(log.Name())
			require.NoError(t, err)
			require.Empty(t, data)
		})
	}
}

func TestFaultCommandDeadlineJoinsDirectChild(t *testing.T) {
	log := faultLog(t)
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	err := RunFaultCommand(ctx, Command{Executable: "/bin/bash", Args: []string{"-c", `printf '%s\n' "$$"; exec /bin/sleep 60`, "fault"}, Stderr: log}, time.Now())
	require.True(t, errors.Is(err, context.DeadlineExceeded), "%v", err)
	data, err := os.ReadFile(log.Name())
	require.NoError(t, err)
	pid := strings.TrimSpace(string(data))
	require.Regexp(t, `^[1-9][0-9]*$`, pid)
	_, err = os.Stat("/proc/" + pid)
	require.True(t, os.IsNotExist(err), "direct fault command was not joined")
}

func TestSupervisorExternalFaultCommand(t *testing.T) {
	for _, mode := range []string{"success", "failure", "deadline"} {
		t.Run(mode, func(t *testing.T) {
			owner := supervisorOwner(t)
			log := faultLog(t)
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			script := `printf '%s\n' "$1" "$$"`
			workerMode := "ok"
			if mode == "failure" {
				script += "; exit 7"
			}
			if mode == "deadline" {
				script += "; exec /bin/sleep 60"
				workerMode = "hang"
			}
			var origin time.Time
			completed := 0
			_, err := Run(ctx, owner, []Command{command(owner, "abcdefgh", workerMode)}, Hooks{
				Baseline: func(context.Context, int, Ready) error { return nil },
				Origin: func(context.Context) (time.Time, error) {
					origin = time.Now()
					if mode == "deadline" {
						origin = origin.Add(-29500 * time.Millisecond)
					}
					return origin, nil
				},
				Inject: func(ctx context.Context, origin time.Time) error {
					return RunFaultCommand(ctx, Command{Executable: "/bin/bash", Args: []string{"-c", script, "fault"}, Stderr: log}, origin)
				},
				Completed: func(context.Context, int, Result, time.Time) error { completed++; return nil },
			})
			if mode == "success" {
				require.NoError(t, err)
				require.Equal(t, 1, completed)
			} else {
				require.Error(t, err)
				require.Zero(t, completed)
			}
			require.NoError(t, ctx.Err(), "original clock must expire before outer context")
			data, readErr := os.ReadFile(log.Name())
			require.NoError(t, readErr)
			lines := strings.Split(strings.TrimSpace(string(data)), "\n")
			require.Len(t, lines, 2)
			require.Equal(t, fmt.Sprint(origin.UnixNano()), lines[0])
			require.Regexp(t, `^[1-9][0-9]*$`, lines[1])
			_, statErr := os.Stat("/proc/" + lines[1])
			require.True(t, os.IsNotExist(statErr), "fault command must exit before Run returns")
			assertJoined(t, owner, "abcdefgh")
		})
	}
}
