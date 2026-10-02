package leasefault

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"os"
	"path/filepath"
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

// Synthetic API and child output test the admission boundary, not real Cilium
// convergence or the product's original thirty-second fault acceptance gate.
func TestNetworkReadySourceBoundary(t *testing.T) {
	for _, mode := range []string{"success", "source-before", "source-after", "own", "network", "claim-after", "cancel-after", "source-during", "policy-replaced", "policy-active", "nonce-fail", "prepared-fail", "input-during", "retain-fail"} {
		t.Run(mode, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
			defer cancel()
			dir, scripts := t.TempDir(), t.TempDir()
			require.NoError(t, os.Chmod(dir, 0700))
			n := networkPlan()
			pod := &unstructured.Unstructured{}
			require.NoError(t, pod.UnmarshalJSON(n.PodBefore))
			controller := true
			pod.SetOwnerReferences([]metav1.OwnerReference{{APIVersion: "apps/v1", Kind: "StatefulSet", Name: "brain", UID: types.UID(n.StatefulSetUID), Controller: &controller}})
			var err error
			n.PodBefore, err = pod.MarshalJSON()
			require.NoError(t, err)
			pod.SetLabels(map[string]string{"app": "brain", "kubebrain.io/fault-owner": n.Nonce})
			ns := &unstructured.Unstructured{Object: map[string]interface{}{"apiVersion": "v1", "kind": "Namespace", "metadata": map[string]interface{}{"name": n.Namespace, "uid": n.NamespaceUID, "resourceVersion": "1"}}}
			sts := &unstructured.Unstructured{Object: map[string]interface{}{"apiVersion": "apps/v1", "kind": "StatefulSet", "metadata": map[string]interface{}{"name": "brain", "namespace": n.Namespace, "uid": n.StatefulSetUID, "resourceVersion": "1"}}}
			policy := &unstructured.Unstructured{}
			require.NoError(t, policy.UnmarshalJSON(reservationFixture()))
			require.NoError(t, ArmNetworkRecovery(dir, n))
			require.NoError(t, SaveNetworkReservation(dir, n, reservationFixture()))
			if mode == "policy-replaced" {
				policy.SetUID("foreign")
			}
			if mode == "policy-active" {
				require.NoError(t, unstructured.SetNestedField(policy.Object, n.Nonce, "spec", "endpointSelector", "matchLabels", "kubebrain.io/fault-owner"))
			}
			client := fake.NewSimpleDynamicClientWithCustomListKinds(runtime.NewScheme(), map[schema.GroupVersionResource]string{{Version: "v1", Resource: "pods"}: "PodList"}, ns, sts, pod, policy)
			client.PrependReactor("create", "configmaps", func(a ktesting.Action) (bool, runtime.Object, error) {
				claim := a.(ktesting.CreateAction).GetObject().(*unstructured.Unstructured).DeepCopy()
				claim.SetUID("claim-uid")
				claim.SetResourceVersion("1")
				return true, claim, client.Tracker().Create(ownerResource, claim, n.Namespace)
			})
			owner, err := AcquireFaultOwner(ctx, client, dir, FaultOwnerBinding{Owner: n.Owner, Namespace: n.Namespace, NamespaceUID: n.NamespaceUID, StatefulSetName: "brain", StatefulSetUID: n.StatefulSetUID})
			require.NoError(t, err)
			targets := []byte(`[{"name":"independently-admitted-fixture"}]`)
			digest := sha256.Sum256(targets)
			require.NoError(t, os.WriteFile(filepath.Join(dir, "observer-pod.json"), n.PodBefore, 0600))
			require.NoError(t, os.WriteFile(filepath.Join(dir, "observer-targets.json"), targets, 0600))
			require.NoError(t, os.WriteFile(filepath.Join(scripts, "observe-local-nonces.sh"), []byte("set -eu\n[[ $# == 5 ]]\nprintf 'nonce fixture\\n'\n[[ $MODE != nonce-fail ]]\n"), 0600))
			require.NoError(t, os.WriteFile(filepath.Join(scripts, "observe-local-network-restored.sh"), []byte("set -eu\n[[ $# == 6 && $2 == absent ]]\nprintf 'prepared fixture\\n'\n[[ $MODE != prepared-fail ]]\nif [[ $MODE == input-during ]]; then printf changed > \"$6\"; fi\n"), 0600))
			var calls []string
			sourceCalls := 0
			sourceChanged := false
			changed := errors.New("admission changed")
			r := NetworkFaultRuntime{
				Lifecycle: FaultLifecycle{Owner: owner, Preparation: FaultPreparation{Directory: dir, StatefulSetName: "brain", Network: n, Client: client, Own: func(context.Context) error {
					calls = append(calls, "own")
					if mode == "own" {
						return changed
					}
					return nil
				}}, Observation: &OriginalObservation{Initial: Binding{ClusterID: 1, InitialMemberID: 2, InitialTerm: 3}}, OutcomeAdmit: func(context.Context) error { return nil }},
				ScriptDirectory: scripts, TargetsSHA256: hex.EncodeToString(digest[:]), Env: []string{"MODE=" + mode, "PATH=/usr/bin:/bin"},
				AdmitNetwork: func(context.Context) error {
					calls = append(calls, "network")
					if mode == "network" {
						return changed
					}
					return nil
				},
				RetainNetwork: func(stage string, _ []byte, _ error) error {
					calls = append(calls, stage)
					if stage == "prepared" {
						switch mode {
						case "claim-after":
							return client.Tracker().Delete(ownerResource, n.Namespace, faultOwnerName)
						case "cancel-after":
							cancel()
						case "source-during":
							sourceChanged = true
						case "retain-fail":
							return changed
						}
					}
					return nil
				},
				SuccessorConnection: &successorConnection{}, Successor: SuccessorBinding{ClusterID: 1, ObserverMemberID: 4, OldLeaderID: 2, OldTerm: 3}, AdmitSuccessor: func(context.Context) error { return nil }, RetainStatus: func(context.Context, SuccessorSample) error { return nil }, CaptureSeconds: 1,
				admitTools: func(context.Context) error {
					calls = append(calls, "source")
					sourceCalls++
					if sourceChanged || (mode == "source-before" && sourceCalls == 1) || (mode == "source-after" && sourceCalls == 2) {
						return changed
					}
					return nil
				},
			}
			bound, err := r.bind()
			require.NoError(t, err)
			err = bound.Preparation.checkReady(ctx)
			if mode != "success" {
				require.Error(t, err)
				if mode == "source-before" {
					require.Equal(t, []string{"source"}, calls)
				}
				if mode == "cancel-after" {
					require.ErrorIs(t, err, context.Canceled)
				}
				if strings.HasPrefix(mode, "source-") || mode == "retain-fail" {
					require.ErrorIs(t, err, changed)
				}
				return
			}
			require.NoError(t, err)
			require.Equal(t, 2, sourceCalls, "one complete read-only readiness admission must have one source bracket")
			liveCalls := func() []string {
				var live []string
				for _, call := range calls {
					if call != "source" {
						live = append(live, call)
					}
				}
				return live
			}
			want := liveCalls()
			calls, sourceCalls = nil, 0
			legacy := bound.Preparation
			legacy.readinessCheck = nil
			require.NoError(t, legacy.checkReady(ctx))
			require.Equal(t, 12, sourceCalls)
			require.Equal(t, want, liveCalls(), "all live API admissions and both nonce scans remain")
		})
	}
}
