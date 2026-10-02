package leasefault

import (
	"context"
	"errors"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/dynamic/fake"
	ktesting "k8s.io/client-go/testing"
)

func TestNetworkAdmissionSourceBoundary(t *testing.T) {
	for _, mode := range []string{"success", "source-before", "source-after", "own", "network", "claim-changed", "cancelled", "identity-success", "identity-source-before", "identity-source-after", "identity-own", "identity-network", "identity-claim-changed", "identity-cancelled", "reservation-success", "reservation-source-before", "reservation-source-after", "reservation-own", "reservation-network", "reservation-claim-changed", "reservation-cancelled", "reservation-policy-replaced", "reservation-policy-active", "observation-success", "observation-source-before", "observation-source-after", "observation-own", "observation-network", "observation-claim-changed", "observation-cancelled", "observation-stage-error", "observation-stage-source", "observation-stage-claim", "observation-stage-cancel"} {
		t.Run(mode, func(t *testing.T) {
			reservationMode := strings.HasPrefix(mode, "reservation-")
			observationMode := strings.HasPrefix(mode, "observation-")
			identityMode := strings.HasPrefix(mode, "identity-") || reservationMode
			mode = strings.TrimPrefix(mode, "reservation-")
			mode = strings.TrimPrefix(mode, "identity-")
			mode = strings.TrimPrefix(mode, "observation-")
			ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
			defer cancel()
			binding := FaultOwnerBinding{Owner: "attempt-a", Namespace: "test", NamespaceUID: "namespace-uid", StatefulSetName: "brain", StatefulSetUID: "sts-uid"}
			dir := t.TempDir()
			require.NoError(t, os.Chmod(dir, 0700))
			ns := &unstructured.Unstructured{Object: map[string]interface{}{"apiVersion": "v1", "kind": "Namespace", "metadata": map[string]interface{}{"name": "test", "uid": "namespace-uid", "resourceVersion": "1"}}}
			sts := &unstructured.Unstructured{Object: map[string]interface{}{"apiVersion": "apps/v1", "kind": "StatefulSet", "metadata": map[string]interface{}{"name": "brain", "namespace": "test", "uid": "sts-uid", "resourceVersion": "1"}}}
			client := fake.NewSimpleDynamicClientWithCustomListKinds(runtime.NewScheme(), map[schema.GroupVersionResource]string{{Version: "v1", Resource: "pods"}: "PodList"}, ns, sts)
			client.PrependReactor("create", "configmaps", func(a ktesting.Action) (bool, runtime.Object, error) {
				claim := a.(ktesting.CreateAction).GetObject().(*unstructured.Unstructured).DeepCopy()
				claim.SetUID("claim-uid")
				claim.SetResourceVersion("1")
				return true, claim, client.Tracker().Create(ownerResource, claim, "test")
			})
			owner, err := AcquireFaultOwner(ctx, client, dir, binding)
			require.NoError(t, err)
			var calls []string
			changed := errors.New("admission changed")
			sourceCalls := 0
			stageVisited := false
			r := NetworkFaultRuntime{
				Lifecycle: FaultLifecycle{Owner: owner, Preparation: FaultPreparation{Own: func(context.Context) error {
					calls = append(calls, "own")
					if mode == "own" {
						return changed
					}
					return nil
				}}, Observation: &OriginalObservation{Initial: Binding{ClusterID: 1, InitialMemberID: 2, InitialTerm: 3}}, OutcomeAdmit: func(context.Context) error { return nil }},
				ScriptDirectory: "/admitted/scripts", TargetsSHA256: strings.Repeat("a", 64),
				AdmitNetwork: func(context.Context) error {
					calls = append(calls, "network")
					if mode == "network" {
						return changed
					}
					if mode == "claim-changed" {
						return client.Tracker().Delete(ownerResource, "test", faultOwnerName)
					}
					if mode == "cancelled" {
						cancel()
					}
					return nil
				},
				RetainNetwork:       func(string, []byte, error) error { return nil },
				SuccessorConnection: &successorConnection{}, Successor: SuccessorBinding{ClusterID: 1, ObserverMemberID: 4, OldLeaderID: 2, OldTerm: 3},
				AdmitSuccessor: func(context.Context) error { return nil }, RetainStatus: func(context.Context, SuccessorSample) error { return nil }, CaptureSeconds: 1,
				admitTools: func(context.Context) error {
					calls = append(calls, "source")
					sourceCalls++
					if (mode == "source-before" && sourceCalls == 1) || (mode == "source-after" && sourceCalls == 2) || (mode == "stage-source" && stageVisited) {
						return changed
					}
					return nil
				},
			}
			if identityMode {
				plan := networkPlan()
				plan.Namespace, plan.NamespaceUID = "test", "namespace-uid"
				pod := &unstructured.Unstructured{}
				require.NoError(t, pod.UnmarshalJSON(plan.PodBefore))
				pod.SetNamespace("test")
				controller := true
				pod.SetOwnerReferences([]metav1.OwnerReference{{APIVersion: "apps/v1", Kind: "StatefulSet", Name: "brain", UID: types.UID("sts-uid"), Controller: &controller}})
				plan.PodBefore, err = pod.MarshalJSON()
				require.NoError(t, err)
				policy := &unstructured.Unstructured{}
				require.NoError(t, policy.UnmarshalJSON(plan.ApprovedPolicy))
				policy.SetNamespace("test")
				plan.ApprovedPolicy, err = policy.MarshalJSON()
				require.NoError(t, err)
				if reservationMode {
					labels := pod.GetLabels()
					labels["kubebrain.io/fault-owner"] = plan.Nonce
					pod.SetLabels(labels)
					reserved := &unstructured.Unstructured{}
					require.NoError(t, reserved.UnmarshalJSON(reservationFixture()))
					reserved.SetNamespace("test")
					raw, err := reserved.MarshalJSON()
					require.NoError(t, err)
					require.NoError(t, ArmNetworkRecovery(dir, plan))
					require.NoError(t, SaveNetworkReservation(dir, plan, raw))
					if mode == "policy-replaced" {
						reserved.SetUID("replacement")
					}
					if mode == "policy-active" {
						require.NoError(t, unstructured.SetNestedField(reserved.Object, plan.Nonce, "spec", "endpointSelector", "matchLabels", "kubebrain.io/fault-owner"))
					}
					require.NoError(t, client.Tracker().Add(reserved))
				}
				require.NoError(t, client.Tracker().Add(pod))
				r.Lifecycle.Preparation.Directory = dir
				r.Lifecycle.Preparation.Network = plan
				r.Lifecycle.Preparation.Client = client
				r.Lifecycle.Preparation.StatefulSetName = "brain"
			}
			bound, err := r.bind()
			require.NoError(t, err)
			want := []string{"source", "own", "network", "source"}
			if observationMode {
				err = bound.admitObservation(ctx, func(context.Context) error {
					calls = append(calls, "stage")
					stageVisited = true
					switch mode {
					case "stage-error":
						return changed
					case "stage-claim":
						return client.Tracker().Delete(ownerResource, "test", faultOwnerName)
					case "stage-cancel":
						cancel()
					}
					return nil
				})
				want = []string{"source", "own", "network", "stage", "own", "network", "source"}
			} else if reservationMode {
				err = bound.Preparation.protocolReservationCheck(ctx)
				want = []string{"source", "own", "network", "own", "network", "own", "network", "source"}
			} else if identityMode {
				err = bound.Preparation.checkIdentity(ctx, NetworkUnlabelled)
				want = []string{"source", "own", "network", "own", "network", "source"}
			} else {
				err = bound.Preparation.Own(ctx)
			}
			if mode == "success" {
				require.NoError(t, err)
				require.Equal(t, want, calls)
				if reservationMode {
					require.Equal(t, 2, sourceCalls)
					// Compare the old nested boundary on the same live fixture:
					// only redundant source checks disappear, not ownership reads.
					calls, sourceCalls = nil, 0
					require.NoError(t, bound.Preparation.checkProtocolReservation(ctx, bound.Preparation.checkIdentity, bound.Preparation.Own))
					require.Equal(t, 4, sourceCalls)
					var liveCalls []string
					for _, call := range calls {
						if call != "source" {
							liveCalls = append(liveCalls, call)
						}
					}
					require.Equal(t, []string{"own", "network", "own", "network", "own", "network"}, liveCalls)
				}
				if observationMode {
					require.Equal(t, 2, sourceCalls)
					calls, sourceCalls = nil, 0
					// Prior runtime nested a source guard around each Own and
					// another around the process/member stage: six full checks.
					legacy := bound
					legacy.observationAdmission = nil
					require.NoError(t, legacy.admitObservation(ctx, func(ctx context.Context) error {
						if err := r.admitTools(ctx); err != nil {
							return err
						}
						calls = append(calls, "stage")
						return r.admitTools(ctx)
					}))
					require.Equal(t, 6, sourceCalls)
					var liveCalls []string
					for _, call := range calls {
						if call != "source" {
							liveCalls = append(liveCalls, call)
						}
					}
					require.Equal(t, []string{"own", "network", "stage", "own", "network"}, liveCalls)
				}
			} else {
				require.Error(t, err)
				if mode == "source-before" {
					require.Equal(t, []string{"source"}, calls)
				}
				if mode == "source-after" {
					require.ErrorIs(t, err, changed)
					require.Equal(t, want, calls)
				}
				if mode == "cancelled" || mode == "stage-cancel" {
					require.ErrorIs(t, err, context.Canceled)
				}
				if mode == "stage-error" || mode == "stage-source" {
					require.ErrorIs(t, err, changed)
				}
			}
		})
	}
}

func TestNetworkFaultRuntimeBinding(t *testing.T) {
	for _, mode := range []string{"bound", "cluster", "old-member", "old-term", "same-observer", "prefilled-clock", "duration", "conflicting-nonces", "conflicting-ready", "conflicting-fault", "conflicting-recovery", "missing-admission", "bad-script", "bad-digest"} {
		t.Run(mode, func(t *testing.T) {
			calls := 0
			admit := func(context.Context) error { calls++; return nil }
			r := NetworkFaultRuntime{
				Lifecycle:       FaultLifecycle{Owner: &FaultOwner{}, Preparation: FaultPreparation{Own: admit}, Observation: &OriginalObservation{Initial: Binding{ClusterID: 1, InitialMemberID: 2, InitialTerm: 3}}, OutcomeAdmit: admit},
				ScriptDirectory: "/admitted/scripts", TargetsSHA256: strings.Repeat("a", 64),
				AdmitNetwork: admit, RetainNetwork: func(string, []byte, error) error { calls++; return nil },
				SuccessorConnection: &successorConnection{}, Successor: SuccessorBinding{ClusterID: 1, ObserverMemberID: 4, OldLeaderID: 2, OldTerm: 3},
				AdmitSuccessor: admit, RetainStatus: func(context.Context, SuccessorSample) error { calls++; return nil }, CaptureSeconds: 1,
			}
			switch mode {
			case "cluster":
				r.Successor.ClusterID++
			case "old-member":
				r.Successor.OldLeaderID++
			case "old-term":
				r.Successor.OldTerm++
			case "same-observer":
				r.Successor.ObserverMemberID = r.Successor.OldLeaderID
			case "prefilled-clock":
				r.Successor.Origin = time.Now()
			case "duration":
				r.CaptureSeconds = 10
			case "conflicting-ready":
				r.Lifecycle.Preparation.ReservedReady = admit
			case "conflicting-nonces":
				r.Lifecycle.Preparation.NoncesSafe = admit
			case "conflicting-fault":
				r.Lifecycle.ObserveFault = func(context.Context, time.Time) (uint64, error) { calls++; return 4, nil }
			case "conflicting-recovery":
				r.Lifecycle.NetworkRestored = admit
			case "missing-admission":
				r.AdmitSuccessor = nil
			case "bad-script":
				r.ScriptDirectory = "relative"
			case "bad-digest":
				r.TargetsSHA256 = "wrong"
			}
			bound, err := r.bind()
			if mode != "bound" {
				require.Error(t, err)
				_, err = r.Run(context.Background())
				require.Error(t, err)
				require.Zero(t, calls, "reject local mismatches before any callback or mutation")
				return
			}
			require.NoError(t, err)
			require.NotNil(t, bound.Preparation.ReservedReady)
			require.NotNil(t, bound.Preparation.NoncesSafe)
			require.NotNil(t, bound.ObserveFault)
			require.NotNil(t, bound.NetworkRestored)
			require.NotNil(t, bound.IdentityRestored)
			require.Nil(t, r.Lifecycle.ObserveFault, "assembly must not modify caller configuration")
			require.Nil(t, r.Lifecycle.Preparation.ReservedReady)
			// Outcome cannot be checked before the actual fault supplies a clock.
			require.Error(t, bound.OutcomeAdmit(context.Background()))
			origin := time.Now()
			ctx, cancel := context.WithDeadline(context.Background(), origin.Add(time.Second))
			cancel()
			term, err := bound.ObserveFault(ctx, origin)
			require.Zero(t, term)
			require.ErrorIs(t, err, context.Canceled)
			_, err = bound.ObserveFault(ctx, origin)
			require.ErrorContains(t, err, "already invoked")
			require.Zero(t, calls)
		})
	}
}
