package metricsworker

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func preparedFixture(t *testing.T, mode string) (Command, string) {
	t.Helper()
	owner := t.TempDir()
	return Command{Executable: "/bin/bash", Stderr: faultLog(t), Args: []string{"-c", `
set -eu
owner=$1; mode=$2
printf '%s\n' "$$" > "$owner/pid"
printf '%s\n' "${KB_PREPARED_SECRET_TEST-absent}" > "$owner/environment"
if [[ $mode == prepare-hang ]]; then exec /bin/sleep 60; fi
if [[ $mode == bad-ready ]]; then printf 'FAULT_WRONG\n'; exit 0; fi
# This original child exists before READY and stays owned by THIS shell.
/bin/sleep 0.05 & probe=$!
printf 'FAULT_READY\n'
IFS= read -r origin
printf '%s\n' "$origin" > "$owner/origin"
if [[ $mode == fault-hang ]]; then wait "$probe"; exec /bin/sleep 60; fi
wait "$probe"
printf '0\n' > "$owner/probe-exit"
if [[ $mode == fault-fail ]]; then exit 7; fi
if [[ $mode == empty-done ]]; then exit 0; fi
if [[ $mode == oversize-done ]]; then printf 'x%.0s' {1..100}; exec /bin/sleep 60; fi
if [[ $mode == wrong-clock ]]; then origin=1; fi
printf 'FAULT_DONE\t%s\n' "$origin"
`, "fault", owner, mode}}, owner
}

func assertPreparedJoined(t *testing.T, owner string) {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(owner, "pid"))
	require.NoError(t, err)
	var pid int
	_, err = fmt.Sscanf(string(data), "%d", &pid)
	require.NoError(t, err)
	_, err = os.Stat(fmt.Sprintf("/proc/%d", pid))
	require.True(t, os.IsNotExist(err), "prepared script survived return")
}

func TestPreparedFaultKeepsOriginalProbeParent(t *testing.T) {
	t.Setenv("KB_PREPARED_SECRET_TEST", "do-not-inherit")
	spec, owner := preparedFixture(t, "success")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var origin time.Time
	err := WithPreparedFault(ctx, spec, func(ctx context.Context, inject func(context.Context, time.Time) error) error {
		origin = time.Now()
		return inject(ctx, origin)
	})
	require.NoError(t, err)
	for name, want := range map[string]string{"origin": fmt.Sprintln(origin.UnixNano()), "probe-exit": "0\n", "environment": "absent\n"} {
		data, err := os.ReadFile(filepath.Join(owner, name))
		require.NoError(t, err)
		require.Equal(t, want, string(data))
	}
	assertPreparedJoined(t, owner)
}

func TestPreparedFaultFailuresJoinBeforeRestore(t *testing.T) {
	for _, mode := range []string{"prepare-hang", "bad-ready", "fault-hang", "fault-fail", "empty-done", "oversize-done", "wrong-clock", "callback-fail", "no-inject", "twice", "bad-budget"} {
		t.Run(mode, func(t *testing.T) {
			spec, owner := preparedFixture(t, mode)
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			if mode == "prepare-hang" {
				cancel()
				ctx, cancel = context.WithTimeout(context.Background(), 300*time.Millisecond)
			}
			defer cancel()
			called := false
			err := WithPreparedFault(ctx, spec, func(ctx context.Context, inject func(context.Context, time.Time) error) error {
				called = true
				if mode == "callback-fail" {
					return errors.New("worker baseline rejected")
				}
				if mode == "no-inject" {
					return nil
				}
				if mode == "bad-budget" {
					_ = inject(context.Background(), time.Now())
					return nil
				}
				faultCtx := ctx
				if mode == "fault-hang" {
					var stop context.CancelFunc
					faultCtx, stop = context.WithTimeout(ctx, 300*time.Millisecond)
					defer stop()
				}
				err := inject(faultCtx, time.Now())
				if mode == "twice" {
					require.NoError(t, err)
					_ = inject(faultCtx, time.Now())
					return nil // Ignoring injection failure must not yield success.
				}
				return err
			})
			require.Error(t, err)
			require.Equal(t, mode != "prepare-hang" && mode != "bad-ready", called)
			// The external restore may start only here, after all direct children
			// are joined. This is a local lifecycle test, not cluster recovery.
			assertPreparedJoined(t, owner)
		})
	}
}

func TestPreparedFaultComposesWithWorkerCoordinator(t *testing.T) {
	for _, mode := range []string{"success", "baseline-reject", "fault-fail", "completion-reject"} {
		t.Run(mode, func(t *testing.T) {
			spec, faultOwner := preparedFixture(t, mode)
			workerOwner := supervisorOwner(t)
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			completed := false
			err := WithPreparedFault(ctx, spec, func(ctx context.Context, inject func(context.Context, time.Time) error) error {
				_, err := Run(ctx, workerOwner, []Command{command(workerOwner, "abcdefgh", "ok")}, Hooks{
					Baseline: func(context.Context, int, Ready) error {
						if mode == "baseline-reject" {
							return errors.New("baseline rejected")
						}
						return nil
					},
					Origin: func(context.Context) (time.Time, error) { return time.Now(), nil },
					Inject: inject,
					Completed: func(context.Context, int, Result, time.Time) error {
						completed = true
						if mode == "completion-reject" {
							return errors.New("completion rejected")
						}
						return nil
					},
				})
				return err
			})
			if mode == "success" {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
			}
			require.Equal(t, mode == "success" || mode == "completion-reject", completed)
			assertJoined(t, workerOwner, "abcdefgh")
			assertPreparedJoined(t, faultOwner)
			if mode == "baseline-reject" {
				_, err := os.Stat(filepath.Join(faultOwner, "origin"))
				require.True(t, os.IsNotExist(err), "rejected baseline dispatched clock")
			}
		})
	}
}
