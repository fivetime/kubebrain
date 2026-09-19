package production_test

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

	"github.com/kubewharf/kubebrain/hack/production/internal/leasefault"
	"github.com/kubewharf/kubebrain/hack/production/internal/metricsworker"
	"github.com/kubewharf/kubebrain/hack/production/internal/processgroup"
	"github.com/stretchr/testify/require"
)

func TestProtectedStackSessionWaitWorker(t *testing.T) {
	library, err := filepath.Abs("protected-stack-session.sh")
	require.NoError(t, err)
	for _, mode := range []string{"success", "wrong-count", "missing-binding", "probe-failed", "controller-reject", "retention-fail", "ignored-retention-error", "admission-lost-after-join", "repeat-observation"} {
		t.Run(mode, func(t *testing.T) {
			owner := t.TempDir()
			require.NoError(t, os.Mkdir(filepath.Join(owner, "bin"), 0700))
			receipt := filepath.Join(owner, "stack-worker.abcdefgh")
			require.NoError(t, os.Mkdir(receipt, 0700))
			frame := "goroutine 17 [select]:\ngithub.com/kubewharf/kubebrain/pkg/server/etcd.(*leaseManager).refreshLeaseHoldingLocks(0xc)\n\t/work/pkg/server/etcd/lease.go:1645 +0x1\n"
			probe := strings.Replace(stackProbeFixture, "synthetic stack\\n", frame, 1)
			require.NoError(t, os.WriteFile(filepath.Join(owner, "bin", "info-diagnostic-probe"), []byte(probe), 0700))
			setup, _, ok := strings.Cut(stackSessionFixture, "trap stack_session_close EXIT")
			require.True(t, ok)
			setup += `
if [[ $scenario != missing-binding ]]; then
 sha256sum "$stack_library_dir/protected-wait-worker.sh" "$stack_library_dir/expired-lease-wait-frames.jq" >> "$stack_owner/tools.sha256"
fi
declare -f kubectl timeout ss openssl
declare -p stack_owner scenario stack_library_dir stack_kubeconfig stack_context stack_namespace stack_namespace_uid stack_sts stack_sts_uid stack_tls stack_server_name
printf '%s\n' 'export stack_owner scenario stack_kubeconfig stack_context stack_namespace stack_namespace_uid stack_sts stack_sts_uid stack_tls stack_server_name' 'export -f kubectl timeout ss openssl'
`
			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()
			prepare := exec.CommandContext(ctx, "/bin/bash", "-c", setup, "test", library, owner, mode)
			processgroup.Configure(prepare)
			bootstrap, err := prepare.Output()
			require.NoError(t, err)
			worker := "set -euo pipefail\n" + string(bootstrap) + `
export stack_info_port=18588 stack_anonymous_port=18589
count=1
[[ $scenario != wrong-count ]] || count=0
exec bash "$stack_library_dir/protected-wait-worker.sh" brain-0 e3914449b57ab6e211ece94cb88fd3318acb2970 3c98f802359a5f185dc6e618691ad6098641a54afa528668c6dfcaf8091ccd88 "$count" "$1"
`
			stderr, err := os.CreateTemp(owner, "stderr-")
			require.NoError(t, err)
			defer stderr.Close()
			called := false
			var origin time.Time
			retained := false
			observation := leasefault.WaitObservation{
				Command:   metricsworker.Command{Executable: "/bin/bash", Args: []string{"-c", worker, "worker", receipt}, Env: os.Environ(), Stderr: stderr},
				Directory: receipt, Source: "e3914449b57ab6e211ece94cb88fd3318acb2970", SourceHash: "3c98f802359a5f185dc6e618691ad6098641a54afa528668c6dfcaf8091ccd88", Count: 1,
				Admit: func(context.Context) error {
					if mode == "admission-lost-after-join" {
						if _, err := os.Stat(filepath.Join(receipt, "exit-code")); err == nil {
							return errors.New("claim lost after capture")
						}
					}
					return nil
				},
				Retain: func(_ context.Context, got leasefault.WaitReceipt, validationErr error) error {
					retained = true
					require.NoError(t, validationErr)
					require.Equal(t, owner, filepath.Dir(got.Capture))
					code, err := os.ReadFile(filepath.Join(receipt, "exit-code"))
					require.NoError(t, err)
					require.Equal(t, "0\n", string(code))
					if mode == "retention-fail" || mode == "ignored-retention-error" {
						return errors.New("retention failed")
					}
					return nil
				},
			}
			err = leasefault.WithWaitObservation(ctx, observation, func(runCtx context.Context, capture func(context.Context, time.Time) error) error {
				called = true
				if mode == "controller-reject" {
					return errors.New("original probe not pending")
				}
				origin = time.Now()
				faultCtx, faultCancel := context.WithDeadline(runCtx, origin.Add(5*time.Second))
				defer faultCancel()
				captureErr := capture(faultCtx, origin)
				if mode == "ignored-retention-error" {
					return nil
				}
				if mode == "repeat-observation" {
					return capture(faultCtx, origin)
				}
				return captureErr
			})
			require.NoError(t, ctx.Err())
			require.Equal(t, mode != "missing-binding", called)
			if mode != "success" {
				require.Error(t, err)
				_, readErr := os.Stat(filepath.Join(receipt, "classification-path"))
				if mode == "retention-fail" || mode == "ignored-retention-error" || mode == "admission-lost-after-join" || mode == "repeat-observation" {
					require.NoError(t, readErr)
					require.Equal(t, mode != "admission-lost-after-join", retained)
				} else {
					require.ErrorIs(t, readErr, os.ErrNotExist)
				}
				return
			}
			require.NoError(t, err)
			require.True(t, retained)
			code, err := os.ReadFile(filepath.Join(receipt, "exit-code"))
			require.NoError(t, err)
			require.Equal(t, "0\n", string(code))
			pids, err := os.ReadFile(filepath.Join(owner, "pids"))
			require.NoError(t, err)
			require.NotEmpty(t, strings.Fields(string(pids)))
			for _, raw := range strings.Fields(string(pids)) {
				pid, err := strconv.Atoi(raw)
				require.NoError(t, err)
				require.ErrorIs(t, syscall.Kill(pid, 0), syscall.ESRCH, "owned tunnel must be reaped before returning")
			}
			path, err := os.ReadFile(filepath.Join(receipt, "classification-path"))
			require.NoError(t, err)
			classified := strings.TrimSpace(string(path))
			require.Equal(t, owner, filepath.Dir(classified))
			out, err := exec.Command("sha256sum", "-c", filepath.Join(classified, "evidence.sha256")).CombinedOutput()
			require.NoError(t, err, string(out))
			marker, err := os.ReadFile(filepath.Join(classified, "COMPLETE"))
			require.NoError(t, err)
			require.Equal(t, "SOURCE_BOUND_WAIT_CANDIDATES_NOT_LEASE_OR_FAULT_PROOF\n", string(marker))
			load := func(clock time.Time, count int) (leasefault.WaitReceipt, error) {
				return leasefault.LoadWaitReceipt(receipt, "e3914449b57ab6e211ece94cb88fd3318acb2970", "3c98f802359a5f185dc6e618691ad6098641a54afa528668c6dfcaf8091ccd88", count, clock)
			}
			verified, err := load(origin, 1)
			require.NoError(t, err)
			require.Equal(t, classified, verified.Classification)
			_, err = load(origin.Add(time.Nanosecond), 1)
			require.Error(t, err)
			_, err = load(origin, 0)
			require.Error(t, err)
			for _, item := range []struct{ dir, name string }{
				{receipt, "exit-code"}, {receipt, "capture-path"}, {receipt, "classification-path"},
				{verified.Capture, "goroutines.txt"}, {verified.Capture, "pod-after.json"},
				{verified.Classification, "input.json"}, {verified.Classification, "frames.json"},
				{verified.Classification, "evidence.sha256"}, {verified.Classification, "COMPLETE"},
			} {
				path := filepath.Join(item.dir, item.name)
				data, err := os.ReadFile(path)
				require.NoError(t, err)
				require.NoError(t, os.WriteFile(path, append(append([]byte{}, data...), 'x'), 0600))
				_, err = load(origin, 1)
				require.Error(t, err, item.name)
				require.NoError(t, os.WriteFile(path, data, 0600))
			}
			_, err = load(origin, 1)
			require.NoError(t, err)
		})
	}
}
