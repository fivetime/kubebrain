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
	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic/fake"
	ktesting "k8s.io/client-go/testing"
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
[[ -f "$1/activation-ack" ]]
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
	activationCalls := 0
	activationIdentityChecks := 0
	pendingChecks := 0
	pendingScopeActive := false
	pendingScopeCalls := 0
	identityPreparation := prep
	prep.identityCheck = func(checkCtx context.Context, phase NetworkLabelPhase) error {
		if !origin.IsZero() {
			activationIdentityChecks++
			require.Equal(t, NetworkLabelOwned, phase)
		}
		return identityPreparation.checkIdentity(checkCtx, phase)
	}
	client := prep.Client.(*fake.FakeDynamicClient)
	client.PrependReactor("patch", "ciliumnetworkpolicies", func(a ktesting.Action) (bool, runtime.Object, error) {
		require.False(t, pendingScopeActive, "activation must leave the read-only source scope")
		require.Equal(t, 1, pendingScopeCalls, "actual lifecycle must admit its pending phase once")
		activationCalls++
		if mode == "lifecycle-success" {
			require.Equal(t, 2, activationIdentityChecks, "activation must use both complete identity admission boundaries")
		}
		require.Equal(t, 1, pendingChecks, "activation requires fresh original probe admission")
		require.False(t, origin.IsZero(), "activation must follow original clock selection")
		_, err := os.Stat(filepath.Join(dir, "fault.origin"))
		require.ErrorIs(t, err, os.ErrNotExist, "child must still await activation acknowledgement")
		if mode == "lifecycle-activation-conflict" {
			return true, nil, errors.New("conditional PATCH conflict")
		}
		if mode == "lifecycle-activation-lost-response" {
			obj, err := client.Tracker().Get(a.GetResource(), a.GetNamespace(), prep.Network.PolicyName)
			require.NoError(t, err)
			u := obj.(*unstructured.Unstructured).DeepCopy()
			require.NoError(t, unstructured.SetNestedField(u.Object, prep.Network.Nonce, "spec", "endpointSelector", "matchLabels", "kubebrain.io/fault-owner"))
			require.NoError(t, client.Tracker().Update(a.GetResource(), u, a.GetNamespace()))
			return true, nil, errors.New("activation committed but response lost")
		}
		require.NoError(t, os.WriteFile(filepath.Join(dir, "activation-ack"), []byte("fixture\n"), 0600))
		return false, nil, nil
	})
	originalNonceCheck := prep.NoncesSafe
	prep.NoncesSafe = func(ctx context.Context) error {
		if !origin.IsZero() {
			deadline, ok := ctx.Deadline()
			require.True(t, ok)
			require.False(t, deadline.After(origin.Add(30*time.Second)))
			if mode == "lifecycle-activation-nonce-fail" {
				return errors.New("foreign endpoint before activation")
			}
		}
		return originalNonceCheck(ctx)
	}
	lifecycle := FaultLifecycle{
		Owner:       owner,
		Preparation: prep, Fault: fault, Workers: []metricsworker.Command{worker}, RecoveryConnection: prep.Connection, RecoveryTimeout: 5 * time.Second,
		OriginalPending: func(faultCtx context.Context, got time.Time) error {
			pendingChecks++
			require.Equal(t, origin, got)
			require.Zero(t, activationCalls)
			require.False(t, joined)
			deadline, ok := faultCtx.Deadline()
			require.True(t, ok)
			require.False(t, deadline.After(origin.Add(30*time.Second)))
			if mode == "lifecycle-activation-original-fail" {
				return errors.New("original probe already responded during baseline capture")
			}
			return nil
		},
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
	}
	lifecycle.beforeActivation = func(faultCtx context.Context, pending func(context.Context) error) error {
		pendingScopeCalls++
		require.False(t, pendingScopeActive)
		pendingScopeActive = true
		defer func() { pendingScopeActive = false }()
		deadline, ok := faultCtx.Deadline()
		require.True(t, ok)
		require.False(t, deadline.After(origin.Add(30*time.Second)))
		return pending(faultCtx)
	}
	var concrete *NetworkFaultRuntime
	if strings.HasPrefix(mode, "lifecycle-native-") {
		lifecycle.Fault = metricsworker.Command{}
		lifecycle.OriginalPending = nil
		lifecycle.OriginalEvidence = nil
		observation, release := nativeObservationFixture(t, prep, log("native.stderr"), func(stage string) {
			if stage == "pending" {
				pendingChecks++
				require.Zero(t, activationCalls)
			} else {
				evidenceRead = true
			}
		})
		lifecycle.Observation = &observation
		if mode == "lifecycle-native-identity-mismatch" {
			lifecycle.Observation.Initial.LeaseID++
		}
		lifecycle.ObserveFault = func(faultCtx context.Context, got time.Time) (uint64, error) {
			require.Equal(t, origin, got)
			require.Equal(t, 1, activationCalls)
			require.False(t, joined)
			if mode == "lifecycle-native-success" || mode == "lifecycle-native-gate-fail" {
				// Simulated network and independent Status, composed by the real
				// observer inside the native lifecycle; not live fault evidence.
				old := observation.Initial.InitialMemberID
				healthy := old + 1
				if healthy == 0 {
					healthy = 1
				}
				stages := []string{}
				conn := &successorConnection{read: func(context.Context, int) (*pb.StatusResponse, error) {
					stages = append(stages, "status")
					return &pb.StatusResponse{Header: &pb.ResponseHeader{ClusterId: observation.Initial.ClusterID, MemberId: healthy, RaftTerm: 3}, Leader: healthy}, nil
				}}
				observer := FaultObservation{
					Connection: conn,
					Successor:  SuccessorBinding{ClusterID: observation.Initial.ClusterID, ObserverMemberID: healthy, OldLeaderID: old, OldTerm: observation.Initial.InitialTerm},
					Active: func(_ context.Context, at time.Time) error {
						require.Equal(t, got, at)
						stages = append(stages, "active")
						return nil
					},
					Drops: func(_ context.Context, at time.Time) error {
						require.Equal(t, got, at)
						stages = append(stages, "drops")
						if mode == "lifecycle-native-gate-fail" {
							return errors.New("independent drop gate failed")
						}
						return nil
					},
					CheckIsolation: prep.Own,
					RetainStatus:   func(context.Context, SuccessorSample) error { stages = append(stages, "retain"); return nil },
				}
				term, err := observer.Observe(faultCtx, got)
				if mode == "lifecycle-native-gate-fail" {
					require.Error(t, err)
					require.Equal(t, []string{"active", "drops"}, stages)
					return term, err
				}
				require.NoError(t, err)
				require.Equal(t, uint64(3), term)
				require.Equal(t, []string{"active", "drops", "status", "retain", "active"}, stages)
			}
			require.NoError(t, os.WriteFile(filepath.Join(dir, "fault.origin"), []byte(fmt.Sprintln(got.UnixNano())), 0600))
			release()
			if mode == "lifecycle-native-stale-successor" {
				return 2, nil
			}
			return 3, nil
		}
		if strings.HasPrefix(mode, "lifecycle-native-runtime-") {
			r := nativeNetworkRuntimeFixture(t, lifecycle, release, func() bool { return joined }, mode)
			concrete = &r
		}
	}
	var result LifecycleResult
	var err error
	if concrete != nil {
		result, err = concrete.Run(ctx)
	} else {
		result, err = RunFaultLifecycle(ctx, lifecycle)
	}
	if mode == "lifecycle-native-identity-mismatch" {
		require.ErrorContains(t, err, "native observation protocol identity mismatch")
		require.False(t, joined)
		require.Zero(t, activationCalls)
		_, statErr := os.Stat(filepath.Join(dir, networkRecoveryFile))
		require.ErrorIs(t, statErr, os.ErrNotExist)
		return
	}
	require.True(t, joined)
	if mode == "lifecycle-baseline-fail" || mode == "lifecycle-parent-cancel" || mode == "lifecycle-activation-nonce-fail" || mode == "lifecycle-activation-original-fail" {
		require.Zero(t, activationCalls)
		require.False(t, result.ActivationAcknowledged)
	} else {
		require.Equal(t, 1, activationCalls, "activation must never retry")
		require.Equal(t, mode != "lifecycle-activation-conflict" && mode != "lifecycle-activation-lost-response", result.ActivationAcknowledged)
	}
	if strings.HasPrefix(mode, "lifecycle-activation-") {
		_, statErr := os.Stat(filepath.Join(dir, "fault.origin"))
		require.ErrorIs(t, statErr, os.ErrNotExist)
		require.False(t, evidenceRead)
		require.Nil(t, result.Outcome)
	}
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
	_, policyErr := prep.Client.Resource(schema.GroupVersionResource{Group: "cilium.io", Version: "v2", Resource: "ciliumnetworkpolicies"}).Namespace(prep.Network.Namespace).Get(parentCtx, prep.Network.PolicyName, metav1.GetOptions{})
	require.True(t, apierrors.IsNotFound(policyErr), "recovery must remove even activation committed with a lost response")
	pod, podErr := prep.Client.Resource(schema.GroupVersionResource{Version: "v1", Resource: "pods"}).Namespace(prep.Network.Namespace).Get(parentCtx, prep.Network.PodName, metav1.GetOptions{})
	require.NoError(t, podErr)
	require.Equal(t, map[string]string{"app": "brain"}, pod.GetLabels())
	require.NoError(t, owner.Release(parentCtx, func(ctx context.Context) error {
		require.True(t, joined)
		require.NoError(t, result.RecoveryError)
		return VerifyProtocolRecovery(ctx, prep.Protocol, prep.Connection)
	}))
	if mode == "lifecycle-success" || mode == "lifecycle-native-success" || mode == "lifecycle-native-runtime-success" {
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
