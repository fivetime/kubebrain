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

	"github.com/kubewharf/kubebrain/hack/production/internal/metricsworker"
	"github.com/stretchr/testify/require"
)

func TestOriginalProbeProcess(t *testing.T) {
	for _, mode := range []string{"success", "early-response", "stack-fail", "capture-timeout", "exit-failure", "oversize", "finish-timeout", "missing-finish"} {
		t.Run(mode, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			dir := t.TempDir()
			log, err := os.OpenFile(filepath.Join(dir, "stderr"), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
			require.NoError(t, err)
			defer log.Close()
			b, events := fixture()
			// Synthetic child tests process supervision, not server blocking.
			prefix := string(encode(t, events[:2]))
			spec := metricsworker.Command{Executable: "/bin/bash", Stderr: log, Args: []string{"-c", `
set -eu
printf '%s\n' "$$" > "$2/pid"
if [[ $3 == oversize ]]; then printf '%13000s' x; exec /bin/sleep 60; fi
printf '%s' "$1"
while [[ ! -f "$2/release" ]]; do /bin/sleep 0.01; done
printf '%s\n' '{"phase":"response"}'
if [[ $3 == exit-failure ]]; then exit 7; fi
`, "probe", prefix, dir, mode}}
			err = WithOriginalProbe(ctx, spec, func(p *OriginalProbe) error {
				_, err := p.AwaitRequest(ctx, b)
				if err != nil {
					return err
				}
				if mode == "missing-finish" {
					return nil
				}
				b.Origin = time.Now()
				faultCtx, faultCancel := context.WithTimeout(ctx, time.Second)
				defer faultCancel()
				_, err = p.Pending(faultCtx, b, func(observeCtx context.Context) error {
					if mode == "stack-fail" {
						return errors.New("unapproved stack")
					}
					if mode == "capture-timeout" {
						faultCancel()
						return nil
					}
					if mode == "early-response" {
						require.NoError(t, os.WriteFile(filepath.Join(dir, "release"), nil, 0600))
						<-p.joined
					}
					return nil
				})
				if err != nil {
					return err
				}
				if mode != "finish-timeout" {
					require.NoError(t, os.WriteFile(filepath.Join(dir, "release"), nil, 0600))
				}
				data, err := p.Finish(faultCtx, b.Origin)
				if err == nil {
					require.Contains(t, string(data), `"phase":"response"`)
				}
				return err
			})
			if mode == "success" {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
			}
			pidBytes, readErr := os.ReadFile(filepath.Join(dir, "pid"))
			require.NoError(t, readErr)
			pid, readErr := strconv.Atoi(strings.TrimSpace(string(pidBytes)))
			require.NoError(t, readErr)
			require.ErrorIs(t, syscall.Kill(pid, 0), syscall.ESRCH, "direct child must be joined before recovery")
		})
	}
}
