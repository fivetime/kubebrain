package metricsworker

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestOwnerGuard(t *testing.T) {
	for _, change := range []string{"hold", "terminal", "dangling-hold", "replace-owner", "replace-claim", "missing-claim"} {
		t.Run(change, func(t *testing.T) {
			owner := supervisorOwner(t)
			guard, err := bindOwner(owner)
			require.NoError(t, err)
			switch change {
			case "hold":
				require.NoError(t, os.WriteFile(filepath.Join(owner, "HOLD"), nil, 0600))
			case "terminal":
				require.NoError(t, os.WriteFile(filepath.Join(owner, "final-exit-code"), nil, 0600))
			case "dangling-hold":
				require.NoError(t, os.Symlink("absent", filepath.Join(owner, "HOLD")))
			case "replace-owner":
				moved := filepath.Join(t.TempDir(), "old-owner")
				require.NoError(t, os.Rename(owner, moved))
				require.NoError(t, os.Mkdir(owner, 0700))
				require.NoError(t, os.Mkdir(filepath.Join(owner, "deployment-claimed"), 0700))
			case "replace-claim", "missing-claim":
				require.NoError(t, os.Rename(filepath.Join(owner, "deployment-claimed"), filepath.Join(owner, "old-claim")))
				if change == "replace-claim" {
					require.NoError(t, os.Mkdir(filepath.Join(owner, "deployment-claimed"), 0700))
				}
			}
			require.Error(t, guard.check())
		})
	}
}

func TestSupervisorOwnerTerminatesBeforeInjection(t *testing.T) {
	for _, marker := range []string{"HOLD", "final-exit-code"} {
		t.Run(marker, func(t *testing.T) {
			owner := supervisorOwner(t)
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			_, err := Run(ctx, owner, []Command{command(owner, "abcdefgh", "ok")}, Hooks{
				Baseline: func(context.Context, int, Ready) error { return os.WriteFile(filepath.Join(owner, marker), nil, 0600) },
				Inject: func(context.Context) (time.Time, error) {
					t.Error("inactive owner must not inject")
					return time.Now(), nil
				},
				Completed: func(context.Context, int, Result, time.Time) error {
					t.Error("inactive owner cannot complete")
					return nil
				},
			})
			require.Error(t, err)
			assertJoined(t, owner, "abcdefgh")
		})
	}
}

func TestSupervisorOwnerWatchCancelsBlockedHook(t *testing.T) {
	owner := supervisorOwner(t)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	_, err := Run(ctx, owner, []Command{command(owner, "abcdefgh", "ok")}, Hooks{
		Baseline: func(ctx context.Context, _ int, _ Ready) error {
			if err := os.WriteFile(filepath.Join(owner, "HOLD"), nil, 0600); err != nil {
				return err
			}
			<-ctx.Done()
			return ctx.Err()
		},
		Inject: func(context.Context) (time.Time, error) {
			t.Error("inactive owner must not inject")
			return time.Now(), nil
		},
		Completed: func(context.Context, int, Result, time.Time) error { return nil },
	})
	require.Error(t, err)
	require.NoError(t, ctx.Err(), "owner watcher must cancel before outer timeout")
	assertJoined(t, owner, "abcdefgh")
}

func TestSupervisorInactiveOwnerDoesNotStart(t *testing.T) {
	owner := supervisorOwner(t)
	require.NoError(t, os.Symlink("missing-target", filepath.Join(owner, "HOLD")))
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_, err := Run(ctx, owner, []Command{command(owner, "abcdefgh", "ok")}, Hooks{
		Baseline: func(context.Context, int, Ready) error { t.Error("inactive owner cannot reach baseline"); return nil },
		Inject: func(context.Context) (time.Time, error) {
			t.Error("inactive owner cannot inject")
			return time.Now(), nil
		},
		Completed: func(context.Context, int, Result, time.Time) error { return nil },
	})
	require.Error(t, err)
	_, err = os.Lstat(filepath.Join(owner, "pid-abcdefgh"))
	require.True(t, os.IsNotExist(err))
}

func TestSupervisorOwnerTerminatesDuringInjection(t *testing.T) {
	owner := supervisorOwner(t)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	_, err := Run(ctx, owner, []Command{command(owner, "abcdefgh", "ok")}, Hooks{
		Baseline: func(context.Context, int, Ready) error { return nil },
		Inject: func(context.Context) (time.Time, error) {
			return time.Now(), os.WriteFile(filepath.Join(owner, "HOLD"), nil, 0600)
		},
		Completed: func(context.Context, int, Result, time.Time) error {
			t.Error("terminated owner cannot complete")
			return nil
		},
	})
	require.Error(t, err)
	_, err = os.Lstat(filepath.Join(owner, "origin-abcdefgh"))
	require.True(t, os.IsNotExist(err), "must not deliver clock after owner termination")
	assertJoined(t, owner, "abcdefgh")
}
