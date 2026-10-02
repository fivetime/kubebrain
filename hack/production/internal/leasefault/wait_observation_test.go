package leasefault

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// Real supervised child, synthetic receipts: this tests the source boundary,
// not backend correctness or real fault acceptance.
func TestWaitObservationSourceBoundary(t *testing.T) {
	for _, mode := range []string{"success", "legacy", "source-before", "source-after", "live-after-child", "cancel-retain", "source-during", "receipt-changed", "retain-error"} {
		t.Run(mode, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			dir := t.TempDir()
			require.NoError(t, os.Chmod(dir, 0700))
			log, err := os.OpenFile(filepath.Join(dir, "stderr"), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
			require.NoError(t, err)
			defer log.Close()
			o, _ := nativeObservationFixture(t, FaultPreparation{Directory: dir, Protocol: ProtocolRecovery{LeaseID: 1, ClusterID: 2, AlarmMemberID: 3}}, log, func(string) {})
			w := o.Before
			w.Command.Args[1] = "printf '%s\\n' \"$$\" > \"$1/pid\"\n" + w.Command.Args[1]
			changed := errors.New("changed admission")
			sourceCalls, liveCalls := 0, 0
			sourceChanged := false
			tools := func(context.Context) error {
				sourceCalls++
				if sourceChanged || mode == "source-before" && sourceCalls == 1 || mode == "source-after" && sourceCalls == 2 {
					return changed
				}
				return nil
			}
			live := func(context.Context) error {
				liveCalls++
				if mode == "live-after-child" && liveCalls == 2 {
					return changed
				}
				return nil
			}
			w.Admit = func(context.Context) error { return nil } // preparation, outside the fault clock
			w.observeTools, w.observeAdmit = tools, live
			if mode == "legacy" {
				w.observeTools, w.observeAdmit = nil, nil
				w.Admit = func(ctx context.Context) error {
					if err := tools(ctx); err != nil {
						return err
					}
					if err := live(ctx); err != nil {
						return err
					}
					return tools(ctx)
				}
			}
			retained := false
			w.Retain = func(_ context.Context, _ WaitReceipt, validationErr error) error {
				retained = true
				switch mode {
				case "cancel-retain":
					cancel()
				case "source-during":
					sourceChanged = true
				case "retain-error":
					return changed
				}
				return validationErr
			}
			err = WithWaitObservation(ctx, w, func(runCtx context.Context, observe func(context.Context, time.Time) error) error {
				if mode == "legacy" {
					sourceCalls, liveCalls = 0, 0
				}
				if mode == "receipt-changed" {
					require.NoError(t, os.WriteFile(filepath.Join(dir, "stack.beforeaa", "goroutines.txt"), []byte("changed"), 0600))
				}
				origin := time.Now()
				faultCtx, stop := context.WithDeadline(runCtx, origin.Add(time.Second))
				defer stop()
				return observe(faultCtx, origin)
			})
			if mode == "success" || mode == "legacy" {
				require.NoError(t, err)
				require.True(t, retained)
				require.Equal(t, 4, liveCalls, "all live checks remain")
				want := 2
				if mode == "legacy" {
					want = 8
				}
				require.Equal(t, want, sourceCalls)
			} else {
				require.Error(t, err)
				if mode == "cancel-retain" {
					require.ErrorIs(t, err, context.Canceled)
				}
				if mode == "source-before" {
					require.Equal(t, 0, liveCalls)
				}
				if mode == "source-during" || strings.HasPrefix(mode, "source-") || mode == "retain-error" || mode == "live-after-child" {
					require.ErrorIs(t, err, changed)
				}
			}
			pidBytes, readErr := os.ReadFile(filepath.Join(w.Directory, "pid"))
			require.NoError(t, readErr)
			pid, err := strconv.Atoi(strings.TrimSpace(string(pidBytes)))
			require.NoError(t, err)
			require.ErrorIs(t, syscall.Kill(pid, 0), syscall.ESRCH, "child must be joined")
		})
	}
}
