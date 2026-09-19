package leasefault

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	jsonpatch "gopkg.in/evanphx/json-patch.v4"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/dynamic/fake"
	ktesting "k8s.io/client-go/testing"
)

func TestRecoverFaultStages(t *testing.T) {
	for _, mode := range []string{"success", "join-error", "owner-mismatch", "withdrawal-error", "protocol-error", "identity-error"} {
		t.Run(mode, func(t *testing.T) {
			dir := t.TempDir()
			require.NoError(t, os.Chmod(dir, 0700))
			network, protocol := networkPlan(), recoveryPlan()
			protocol.Owner, protocol.NamespaceUID, protocol.StatefulSetUID = network.Owner, network.NamespaceUID, network.StatefulSetUID
			require.NoError(t, ArmNetworkRecovery(dir, network))
			require.NoError(t, SaveNetworkReservation(dir, network, reservationFixture()))
			require.NoError(t, ArmProtocolRecovery(dir, protocol))
			conn := &restoreConnection{recoveryConnection: recoveryConnection{t: t, plan: protocol}, alarm: true, lease: true}
			if mode == "protocol-error" {
				conn.mode = "alarm-error"
			}
			var pod, policy unstructured.Unstructured
			require.NoError(t, pod.UnmarshalJSON(network.PodBefore))
			require.NoError(t, policy.UnmarshalJSON(reservationFixture()))
			pod.SetLabels(map[string]string{"app": "brain", "kubebrain.io/fault-owner": network.Nonce})
			controller := true
			pod.SetOwnerReferences([]metav1.OwnerReference{{APIVersion: "apps/v1", Kind: "StatefulSet", Name: "brain", UID: types.UID(network.StatefulSetUID), Controller: &controller}})
			joined, removed, converged := false, false, false
			patches, deletes := 0, 0
			client := fake.NewSimpleDynamicClientWithCustomListKinds(runtime.NewScheme(), map[schema.GroupVersionResource]string{
				{Version: "v1", Resource: "pods"}:                                      "PodList",
				{Group: "cilium.io", Version: "v2", Resource: "ciliumnetworkpolicies"}: "CiliumNetworkPolicyList",
			})
			client.PrependReactor("get", "*", func(a ktesting.Action) (bool, runtime.Object, error) {
				require.True(t, joined)
				obj := &unstructured.Unstructured{Object: map[string]interface{}{}}
				obj.SetResourceVersion("1")
				switch a.GetResource().Resource {
				case "namespaces":
					obj.SetAPIVersion("v1")
					obj.SetKind("Namespace")
					obj.SetName(network.Namespace)
					obj.SetUID(types.UID(network.NamespaceUID))
				case "statefulsets":
					obj.SetAPIVersion("apps/v1")
					obj.SetKind("StatefulSet")
					obj.SetNamespace(network.Namespace)
					obj.SetName("brain")
					obj.SetUID(types.UID(network.StatefulSetUID))
				case "pods":
					return true, pod.DeepCopy(), nil
				case "ciliumnetworkpolicies":
					if removed {
						return true, nil, apierrors.NewNotFound(schema.GroupResource{Group: "cilium.io", Resource: "ciliumnetworkpolicies"}, network.PolicyName)
					}
					return true, policy.DeepCopy(), nil
				default:
					return true, nil, errors.New("unexpected GET")
				}
				return true, obj, nil
			})
			client.PrependReactor("list", "*", func(a ktesting.Action) (bool, runtime.Object, error) {
				require.True(t, joined)
				list := &unstructured.UnstructuredList{}
				if a.GetResource().Resource == "pods" {
					if _, ok := pod.GetLabels()["kubebrain.io/fault-owner"]; ok {
						list.Items = []unstructured.Unstructured{*pod.DeepCopy()}
					}
				} else if !removed {
					list.Items = []unstructured.Unstructured{*policy.DeepCopy()}
				}
				return true, list, nil
			})
			client.PrependReactor("delete", "ciliumnetworkpolicies", func(a ktesting.Action) (bool, runtime.Object, error) {
				require.True(t, joined)
				require.Empty(t, conn.writes)
				pre := a.(ktesting.DeleteAction).GetDeleteOptions().Preconditions
				require.NotNil(t, pre)
				require.Equal(t, policy.GetUID(), *pre.UID)
				require.Equal(t, policy.GetResourceVersion(), *pre.ResourceVersion)
				deletes++
				removed = true
				return true, nil, nil
			})
			client.PrependReactor("patch", "pods", func(a ktesting.Action) (bool, runtime.Object, error) {
				require.True(t, removed)
				require.False(t, conn.alarm)
				require.False(t, conn.lease)
				require.Equal(t, []string{"alarm", "lease"}, conn.writes)
				patch, err := jsonpatch.DecodePatch(a.(ktesting.PatchAction).GetPatch())
				require.NoError(t, err)
				before, err := pod.MarshalJSON()
				require.NoError(t, err)
				after, err := patch.Apply(before)
				require.NoError(t, err)
				require.NoError(t, pod.UnmarshalJSON(after))
				patches++
				return true, pod.DeepCopy(), nil
			})
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			r := FaultRecovery{Directory: dir, StatefulSetName: "brain", Network: network, Protocol: protocol, Client: client, Connection: conn,
				Join: func(context.Context) error {
					require.Empty(t, client.Actions())
					require.Zero(t, conn.calls)
					if mode == "join-error" {
						return errors.New("worker remains")
					}
					joined = true
					return nil
				},
				Own: func(context.Context) error { require.True(t, joined); return nil },
				NetworkRestored: func(context.Context) error {
					require.True(t, removed)
					if mode == "withdrawal-error" {
						return errors.New("Cilium still enforcing")
					}
					return nil
				},
				IdentityRestored: func(context.Context) error {
					require.NotContains(t, pod.GetLabels(), "kubebrain.io/fault-owner")
					if mode == "identity-error" {
						return errors.New("identity has not converged")
					}
					converged = true
					return nil
				},
			}
			if mode == "owner-mismatch" {
				r.Protocol.Owner = "foreign"
			}
			err := RecoverFault(ctx, r)
			if mode == "success" {
				require.NoError(t, err)
				require.True(t, converged)
			} else {
				require.Error(t, err)
				require.False(t, converged)
			}
			if mode == "join-error" || mode == "owner-mismatch" {
				require.Empty(t, client.Actions())
				require.Zero(t, conn.calls)
				require.Zero(t, deletes)
			} else {
				require.Equal(t, 1, deletes)
			}
			if mode == "success" || mode == "identity-error" {
				require.Equal(t, 1, patches)
			} else {
				require.Zero(t, patches)
			}
			if mode == "withdrawal-error" {
				require.Zero(t, conn.calls)
			}
			_, err = LoadProtocolRecovery(dir, protocol)
			require.NoError(t, err)
			_, err = LoadNetworkReservation(dir, network)
			require.NoError(t, err)
		})
	}
}
