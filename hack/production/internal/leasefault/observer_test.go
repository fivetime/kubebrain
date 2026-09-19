package leasefault

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/kubewharf/kubebrain/hack/production/internal/processgroup"
	"github.com/stretchr/testify/require"
)

func TestRunRecoveryObserver(t *testing.T) {
	bash, err := exec.LookPath("bash")
	require.NoError(t, err)
	for _, mode := range []string{"matched", "pending", "fatal", "environment", "output-limit", "cancelled", "deadline"} {
		t.Run(mode, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			body := "printf matched"
			var env []string
			switch mode {
			case "pending":
				body = "printf pending; exit 75"
			case "fatal":
				body = "printf invalid; exit 65"
			case "environment":
				t.Setenv("KB_OBSERVER_INHERITED", "must-not-leak")
				body = `printf '%s:%s' "${KB_OBSERVER_INHERITED-unset}" "$KB_OBSERVER_EXPLICIT"`
				env = []string{"KB_OBSERVER_EXPLICIT=kept"}
			case "output-limit":
				body = `printf '%*s' 1048577 ''; exit 75`
			case "cancelled":
				cancel()
			case "deadline":
				cancel()
				ctx, cancel = context.WithTimeout(context.Background(), 300*time.Millisecond)
				defer cancel()
				body = `printf '%s\n' "$BASHPID"; exec /bin/sleep 60`
			}
			out, err := RunRecoveryObserver(ctx, bash, []string{"-c", body}, env)
			switch mode {
			case "matched":
				require.NoError(t, err)
				require.Equal(t, "matched", string(out))
			case "environment":
				require.NoError(t, err)
				require.Equal(t, "unset:kept", string(out))
			case "pending":
				require.ErrorIs(t, err, ErrObservationPending)
			case "fatal":
				require.Error(t, err)
				require.NotErrorIs(t, err, ErrObservationPending)
			case "output-limit":
				require.Error(t, err)
				require.NotErrorIs(t, err, ErrObservationPending)
				require.Len(t, out, processgroup.DefaultOutputLimitBytes)
			case "cancelled":
				require.ErrorIs(t, err, context.Canceled)
				require.Empty(t, out)
			case "deadline":
				require.ErrorIs(t, err, context.DeadlineExceeded)
				pid, parseErr := strconv.Atoi(strings.TrimSpace(string(out)))
				require.NoError(t, parseErr)
				require.True(t, errors.Is(syscall.Kill(pid, 0), syscall.ESRCH), "direct child must be joined")
			}
		})
	}
	_, err = RunRecoveryObserver(context.Background(), bash, nil, nil)
	require.Error(t, err)
	_, err = RunRecoveryObserver(nil, bash, nil, nil)
	require.Error(t, err)
}

func TestWaitRecoveryObserver(t *testing.T) {
	bash, err := exec.LookPath("bash")
	require.NoError(t, err)
	for _, mode := range []string{"converges", "fatal", "admission-lost", "retention-fails", "matched-retention-fails", "cancel-on-retain", "deadline"} {
		t.Run(mode, func(t *testing.T) {
			gate := filepath.Join(t.TempDir(), "matched")
			body := `if [[ -f "$1" ]]; then printf matched; exit 0; fi; printf pending; exit 75`
			if mode == "fatal" {
				body = "printf invalid; exit 65"
			}
			if mode == "matched-retention-fails" {
				body = "printf matched"
			}
			budget := 3 * time.Second
			if mode == "deadline" {
				budget = 100 * time.Millisecond
			}
			ctx, cancel := context.WithTimeout(context.Background(), budget)
			defer cancel()
			deadline, _ := ctx.Deadline()
			admissions, retained := 0, 0
			var results []string
			err := WaitRecoveryObserver(ctx, bash, []string{"-c", body, "bash", gate}, nil, func(got context.Context) error {
				admissions++
				actual, ok := got.Deadline()
				require.True(t, ok)
				require.Equal(t, deadline, actual)
				if mode == "admission-lost" && admissions == 2 {
					return errors.New("ownership lost")
				}
				return nil
			}, func(output []byte, observed error) error {
				retained++
				results = append(results, string(output))
				if mode == "retention-fails" || mode == "matched-retention-fails" {
					return errors.New("cannot retain evidence")
				}
				if mode == "cancel-on-retain" {
					cancel()
				}
				if mode == "converges" && observed == ErrObservationPending {
					require.NoError(t, os.WriteFile(gate, nil, 0600))
				}
				return nil
			})
			if mode == "converges" {
				require.NoError(t, err)
				require.Equal(t, []string{"pending", "matched"}, results)
				require.Equal(t, 2, admissions)
			} else {
				require.Error(t, err)
				require.Equal(t, 1, retained)
				if mode == "deadline" {
					require.ErrorIs(t, err, context.DeadlineExceeded)
				}
				if mode == "cancel-on-retain" {
					require.ErrorIs(t, err, context.Canceled)
				}
				if mode == "admission-lost" {
					require.Equal(t, 2, admissions)
				} else {
					require.Equal(t, 1, admissions)
				}
			}
		})
	}
}
