package leasefault

import (
	"context"
	"math"
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

// This exercises a real child lifecycle, not Kubernetes or recovery RPCs.
// The record belongs to the outer owner and survives all child outcomes.
func TestProtocolRecoverySurvivesPreparedChild(t *testing.T) {
	for _, mode := range []string{"prepare-failure", "cancel-ready", "fault-timeout", "success"} {
		t.Run(mode, func(t *testing.T) {
			dir := t.TempDir()
			require.NoError(t, os.Chmod(dir, 0700))
			plan := recoveryPlan()
			require.NoError(t, ArmProtocolRecovery(dir, plan))
			log, err := os.OpenFile(filepath.Join(dir, "child.stderr"), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
			require.NoError(t, err)
			defer log.Close()
			pidPath := filepath.Join(dir, "child.pid")
			spec := metricsworker.Command{Executable: "/bin/bash", Stderr: log, Args: []string{"-c", `
set -eu
[[ -s $2 ]]
printf '%s\n' "$BASHPID" > "$1"
[[ $3 != prepare-failure ]] || exit 23
printf 'FAULT_READY\n'
IFS= read -r origin
if [[ $3 == fault-timeout ]]; then
  while :; do /bin/sleep 1; done
fi
printf 'FAULT_DONE\t%s\n' "$origin"
`, "prepared-recovery-fixture", pidPath, filepath.Join(dir, protocolRecoveryFile), mode}}
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			called := false
			err = metricsworker.WithPreparedFault(ctx, spec, func(runCtx context.Context, inject func(context.Context, time.Time) error) error {
				called = true
				if mode == "cancel-ready" {
					cancel()
					<-runCtx.Done()
					return runCtx.Err()
				}
				origin := time.Now()
				budget := 2 * time.Second
				if mode == "fault-timeout" {
					budget = 100 * time.Millisecond
				}
				faultCtx, stop := context.WithDeadline(runCtx, origin.Add(budget))
				defer stop()
				return inject(faultCtx, origin)
			})
			if mode == "success" {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
			}
			require.Equal(t, mode != "prepare-failure", called)
			if mode == "fault-timeout" {
				require.ErrorIs(t, err, context.DeadlineExceeded)
			}
			if mode == "cancel-ready" {
				require.ErrorIs(t, err, context.Canceled)
			}
			// External recovery starts only after the lifecycle call returned.
			pidData, err := os.ReadFile(pidPath)
			require.NoError(t, err)
			pid, err := strconv.Atoi(strings.TrimSpace(string(pidData)))
			require.NoError(t, err)
			require.Positive(t, pid)
			require.ErrorIs(t, syscall.Kill(pid, 0), syscall.ESRCH)
			got, err := LoadProtocolRecovery(dir, plan)
			require.NoError(t, err)
			require.Equal(t, plan, got)
		})
	}
}

func recoveryPlan() ProtocolRecovery {
	return ProtocolRecovery{Owner: "fault-fresh.abcd1234", NamespaceUID: "namespace-uid", StatefulSetUID: "sts-uid",
		ClusterID: math.MaxUint64, AlarmMemberID: 9007199254740993, LeaseID: math.MinInt64, Key: "/acceptance/fresh-owned-key"}
}

func TestProtocolRecoveryCreateOnceAndReload(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, os.Chmod(dir, 0700))
	plan := recoveryPlan()
	require.NoError(t, ArmProtocolRecovery(dir, plan))
	got, err := LoadProtocolRecovery(dir, plan)
	require.NoError(t, err)
	require.Equal(t, plan, got)
	data, err := os.ReadFile(filepath.Join(dir, protocolRecoveryFile))
	require.NoError(t, err)
	require.Contains(t, string(data), `"cluster_id":"18446744073709551615"`)
	require.Error(t, ArmProtocolRecovery(dir, plan))
	after, err := os.ReadFile(filepath.Join(dir, protocolRecoveryFile))
	require.NoError(t, err)
	require.Equal(t, data, after)
	plan.LeaseID++
	got, err = LoadProtocolRecovery(dir, plan)
	require.Error(t, err)
	require.Zero(t, got)
}

func TestProtocolRecoveryRejectsUnsafeRecords(t *testing.T) {
	for _, mode := range []string{"directory-link", "public-directory", "file-link", "fifo", "public-file", "truncated", "duplicate", "unknown", "oversize", "missing"} {
		t.Run(mode, func(t *testing.T) {
			dir := t.TempDir()
			require.NoError(t, os.Chmod(dir, 0700))
			plan := recoveryPlan()
			require.NoError(t, ArmProtocolRecovery(dir, plan))
			path := filepath.Join(dir, protocolRecoveryFile)
			data, err := os.ReadFile(path)
			require.NoError(t, err)
			switch mode {
			case "directory-link":
				link := filepath.Join(t.TempDir(), "owner")
				require.NoError(t, os.Symlink(dir, link))
				dir = link
			case "public-directory":
				require.NoError(t, os.Chmod(dir, 0755))
			case "file-link":
				require.NoError(t, os.Rename(path, path+".original"))
				require.NoError(t, os.Symlink(path+".original", path))
			case "fifo":
				require.NoError(t, os.Remove(path))
				require.NoError(t, syscall.Mkfifo(path, 0600))
			case "public-file":
				require.NoError(t, os.Chmod(path, 0644))
			case "truncated":
				require.NoError(t, os.WriteFile(path, data[:len(data)/2], 0600))
			case "duplicate":
				require.NoError(t, os.WriteFile(path, []byte(strings.Replace(string(data), "{", `{"owner":"fault-fresh.abcd1234",`, 1)), 0600))
			case "unknown":
				require.NoError(t, os.WriteFile(path, []byte(strings.Replace(string(data), "{", `{"skip_recovery":true,`, 1)), 0600))
			case "oversize":
				require.NoError(t, os.WriteFile(path, []byte(strings.Repeat(" ", 4097)), 0600))
			case "missing":
				require.NoError(t, os.Remove(path))
			}
			got, err := LoadProtocolRecovery(dir, plan)
			require.Error(t, err)
			require.Zero(t, got)
		})
	}
}

func TestProtocolRecoveryInvalidPlanDoesNotArm(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, os.Chmod(dir, 0700))
	plan := recoveryPlan()
	plan.Key = "/business/key"
	require.Error(t, ArmProtocolRecovery(dir, plan))
	_, err := os.Stat(filepath.Join(dir, protocolRecoveryFile))
	require.True(t, os.IsNotExist(err))
}
