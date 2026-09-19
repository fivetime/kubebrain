package leasefault

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/kubewharf/kubebrain/hack/production/internal/metricsworker"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func testFaultLifecycle(t *testing.T, ctx context.Context, prep FaultPreparation, owner *FaultOwner, mode string) {
	t.Helper()
	parentCtx := ctx
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	dir := prep.Directory
	require.NoError(t, os.Mkdir(filepath.Join(dir, "deployment-claimed"), 0700))
	log := func(name string) *os.File {
		f, err := os.OpenFile(filepath.Join(dir, name), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, f.Close()) })
		return f
	}
	// Synthetic gate scripts exercise process and clock wiring only; they are
	// deliberately not deployable fault or metrics evidence generators.
	fault := metricsworker.Command{Executable: "/bin/bash", Stderr: log("fault.stderr"), Args: []string{"-c", `
set -eu
printf '%s\n' "$$" > "$1/fault.pid"
printf 'FAULT_READY\n'
IFS= read -r origin
printf '%s\n' "$origin" > "$1/fault.origin"
if [[ $2 == lifecycle-child-fail ]]; then exit 7; fi
if [[ $2 == lifecycle-deadline ]]; then exec /bin/sleep 60; fi
printf 'FAULT_DONE\t%s\n' "$origin"
`, "fault", dir, mode}}
	worker := metricsworker.Command{Executable: "/bin/bash", Stderr: log("worker.stderr"), Args: []string{"-c", `
set -eu
printf '%s\n' "$$" > "$1/worker.pid"
printf 'READY\t%s/metrics-worker.abcdefgh\t%s/metrics.abcdefgh\n' "$1" "$1"
IFS= read -r origin
printf '%s\n' "$origin" > "$1/worker.origin"
printf 'CAPTURED\t%s/metrics.ijklmnop\t%s/metrics-schedule.abcdefgh\n' "$1" "$1"
`, "worker", dir}}
	joined := false
	evidenceRead := false
	var origin time.Time
	result, err := RunFaultLifecycle(ctx, FaultLifecycle{
		Owner:       owner,
		Preparation: prep, Fault: fault, Workers: []metricsworker.Command{worker}, RecoveryConnection: prep.Connection, RecoveryTimeout: 5 * time.Second,
		OriginalEvidence: func(faultCtx context.Context) (Binding, []byte, error) {
			require.False(t, joined, "outcome must precede recovery")
			deadline, ok := faultCtx.Deadline()
			require.True(t, ok)
			require.False(t, deadline.After(origin.Add(30*time.Second)))
			data, err := os.ReadFile(filepath.Join(dir, "fault.pid"))
			require.NoError(t, err)
			pid, err := strconv.Atoi(strings.TrimSpace(string(data)))
			require.NoError(t, err)
			require.ErrorIs(t, syscall.Kill(pid, 0), syscall.ESRCH, "outcome must follow prepared child exit")
			evidenceRead = true
			if mode == "lifecycle-owner-lost" {
				require.NoError(t, prep.Client.Resource(ownerResource).Namespace(prep.Network.Namespace).Delete(faultCtx, faultOwnerName, metav1.DeleteOptions{}))
			}
			if mode == "lifecycle-outcome-timeout" {
				<-faultCtx.Done()
				return Binding{}, nil, faultCtx.Err()
			}
			b := Binding{LeaseID: prep.Protocol.LeaseID, ClusterID: prep.Protocol.ClusterID, InitialMemberID: prep.Protocol.AlarmMemberID, InitialTerm: 2, SuccessorTerm: 3, Origin: origin}
			// Synthetic original-stream and successor evidence for wiring only.
			events := []map[string]any{
				{"phase": "expired_preflight", "at": origin.Add(-2 * time.Second), "lease_id": b.LeaseID, "ttl": int64(-1), "member_id": b.InitialMemberID, "raft_term": b.InitialTerm, "cluster_id": b.ClusterID},
				{"phase": "request_sent", "at": origin.Add(-time.Second), "lease_id": b.LeaseID},
				{"phase": "response", "at": time.Now(), "lease_id": b.LeaseID, "ttl": int64(10), "header": map[string]any{"cluster_id": b.ClusterID, "member_id": b.InitialMemberID, "raft_term": b.SuccessorTerm, "revision": int64(1)}},
			}
			if mode == "lifecycle-evidence-fail" {
				return Binding{}, nil, errors.New("untrusted original evidence")
			}
			if mode == "lifecycle-clock-changed" {
				b.Origin = b.Origin.Add(time.Nanosecond)
			}
			if mode == "lifecycle-outcome-mismatch" {
				events[2]["ttl"] = int64(0)
			}
			return b, encode(t, events), nil
		},
		OutcomeAdmit: func(context.Context) error { require.False(t, joined); return nil },
		Metrics: metricsworker.Hooks{
			Baseline: func(context.Context, int, metricsworker.Ready) error {
				if mode == "lifecycle-baseline-fail" {
					return errors.New("baseline rejected")
				}
				return nil
			},
			Origin: func(context.Context) (time.Time, error) {
				origin = time.Now()
				if mode == "lifecycle-parent-cancel" {
					cancel()
				}
				if mode == "lifecycle-deadline" || mode == "lifecycle-outcome-timeout" {
					origin = origin.Add(-29500 * time.Millisecond)
				}
				return origin, nil
			},
			Completed: func(_ context.Context, _ int, _ metricsworker.Result, got time.Time) error {
				require.Equal(t, origin, got)
				require.True(t, evidenceRead)
				return nil
			},
		},
		Join: func(recoveryCtx context.Context) error {
			require.NoError(t, recoveryCtx.Err())
			for _, name := range []string{"fault.pid", "worker.pid"} {
				data, err := os.ReadFile(filepath.Join(dir, name))
				require.NoError(t, err)
				pid, err := strconv.Atoi(strings.TrimSpace(string(data)))
				require.NoError(t, err)
				require.ErrorIs(t, syscall.Kill(pid, 0), syscall.ESRCH, "child must join before recovery")
			}
			joined = true
			if mode == "lifecycle-join-fail" {
				return errors.New("external worker remains")
			}
			return nil
		},
		NetworkRestored:  func(context.Context) error { require.True(t, joined); return nil },
		IdentityRestored: func(context.Context) error { require.True(t, joined); return nil },
	})
	require.True(t, joined)
	if mode == "lifecycle-owner-lost" {
		require.Error(t, err)
		require.Error(t, result.ExecutionError)
		require.Error(t, result.RecoveryError)
		require.Error(t, VerifyProtocolRecovery(parentCtx, prep.Protocol, prep.Connection), "lost claim must not authorize recovery writes")
		return
	}
	require.NoError(t, owner.Check(parentCtx), "lifecycle must not automatically release the claim")
	if mode == "lifecycle-join-fail" {
		require.NoError(t, result.ExecutionError)
		require.ErrorContains(t, result.RecoveryError, "external worker remains")
		require.Error(t, err)
		require.Error(t, VerifyProtocolRecovery(parentCtx, prep.Protocol, prep.Connection))
		return
	}
	require.NoError(t, result.RecoveryError)
	require.NoError(t, VerifyProtocolRecovery(parentCtx, prep.Protocol, prep.Connection))
	require.NoError(t, owner.Release(parentCtx, func(ctx context.Context) error {
		require.True(t, joined)
		require.NoError(t, result.RecoveryError)
		return VerifyProtocolRecovery(ctx, prep.Protocol, prep.Connection)
	}))
	if mode == "lifecycle-success" {
		require.NoError(t, err)
		require.NoError(t, result.ExecutionError)
		require.Len(t, result.Metrics, 1)
		require.NotNil(t, result.Outcome)
		require.NotNil(t, result.Outcome.Key)
		for _, name := range []string{"fault.origin", "worker.origin"} {
			data, err := os.ReadFile(filepath.Join(dir, name))
			require.NoError(t, err)
			require.Equal(t, fmt.Sprintln(origin.UnixNano()), string(data))
		}
	} else {
		require.Error(t, err)
		require.Error(t, result.ExecutionError)
		if mode == "lifecycle-deadline" || mode == "lifecycle-outcome-timeout" {
			require.ErrorIs(t, err, context.DeadlineExceeded)
			require.NoError(t, ctx.Err())
		}
		if mode == "lifecycle-baseline-fail" {
			_, err := os.Stat(filepath.Join(dir, "fault.origin"))
			require.ErrorIs(t, err, os.ErrNotExist)
		}
		if mode == "lifecycle-child-fail" || mode == "lifecycle-baseline-fail" || mode == "lifecycle-deadline" || mode == "lifecycle-parent-cancel" {
			require.False(t, evidenceRead)
			require.Nil(t, result.Outcome)
		}
	}
}
