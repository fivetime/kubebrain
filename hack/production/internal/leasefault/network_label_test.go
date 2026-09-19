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

func TestNetworkLabelLifecycle(t *testing.T) {
	for _, mode := range []string{"roundtrip", "no-label-map", "prepare-conflict", "active-reservation", "reservation-replaced", "prepare-no-policy", "restore-policy-remains", "restore-nonce-reference", "restore-pagination", "restore-blocked", "restore-conflict", "restore-foreign-label", "restore-already-absent"} {
		t.Run(mode, func(t *testing.T) {
			dir := t.TempDir()
			require.NoError(t, os.Chmod(dir, 0700))
			plan := networkPlan()
			require.NoError(t, ArmNetworkRecovery(dir, plan))
			require.NoError(t, SaveNetworkReservation(dir, plan, reservationFixture()))
			var pod, policy unstructured.Unstructured
			require.NoError(t, pod.UnmarshalJSON(plan.PodBefore))
			require.NoError(t, policy.UnmarshalJSON(reservationFixture()))
			controller := true
			pod.SetOwnerReferences([]metav1.OwnerReference{{APIVersion: "apps/v1", Kind: "StatefulSet", Name: "brain", UID: types.UID(plan.StatefulSetUID), Controller: &controller}})
			if mode == "no-label-map" {
				delete(pod.Object["metadata"].(map[string]interface{}), "labels")
			}
			if mode == "active-reservation" {
				require.NoError(t, unstructured.SetNestedField(policy.Object, plan.Nonce, "spec", "endpointSelector", "matchLabels", "kubebrain.io/fault-owner"))
			}
			if mode == "reservation-replaced" {
				policy.SetUID("replacement")
			}
			restoring := false
			patches := 0
			client := fake.NewSimpleDynamicClientWithCustomListKinds(runtime.NewScheme(), map[schema.GroupVersionResource]string{
				{Version: "v1", Resource: "pods"}:                                      "PodList",
				{Group: "cilium.io", Version: "v2", Resource: "ciliumnetworkpolicies"}: "CiliumNetworkPolicyList",
			})
			client.PrependReactor("get", "*", func(a ktesting.Action) (bool, runtime.Object, error) {
				obj := &unstructured.Unstructured{Object: map[string]interface{}{}}
				obj.SetResourceVersion("1")
				switch a.GetResource().Resource {
				case "namespaces":
					obj.SetAPIVersion("v1")
					obj.SetKind("Namespace")
					obj.SetName(plan.Namespace)
					obj.SetUID(types.UID(plan.NamespaceUID))
				case "statefulsets":
					obj.SetAPIVersion("apps/v1")
					obj.SetKind("StatefulSet")
					obj.SetNamespace(plan.Namespace)
					obj.SetName("brain")
					obj.SetUID(types.UID(plan.StatefulSetUID))
				case "pods":
					return true, pod.DeepCopy(), nil
				default:
					return true, nil, errors.New("unexpected GET")
				}
				return true, obj, nil
			})
			client.PrependReactor("list", "*", func(a ktesting.Action) (bool, runtime.Object, error) {
				list := &unstructured.UnstructuredList{}
				if a.GetResource().Resource == "pods" {
					if _, ok := pod.GetLabels()["kubebrain.io/fault-owner"]; ok {
						list.Items = []unstructured.Unstructured{*pod.DeepCopy()}
					}
				} else if !restoring || mode == "restore-policy-remains" {
					if mode != "prepare-no-policy" {
						list.Items = []unstructured.Unstructured{*policy.DeepCopy()}
					}
				} else if mode == "restore-nonce-reference" {
					other := policy.DeepCopy()
					other.SetName("other-policy")
					require.NoError(t, unstructured.SetNestedField(other.Object, plan.Nonce, "spec", "description"))
					list.Items = []unstructured.Unstructured{*other}
				} else if mode == "restore-pagination" {
					list.SetContinue("next")
				}
				return true, list, nil
			})
			client.PrependReactor("patch", "pods", func(a ktesting.Action) (bool, runtime.Object, error) {
				patches++
				if mode == "prepare-conflict" || (restoring && mode == "restore-conflict") {
					return true, nil, apierrors.NewConflict(schema.GroupResource{Resource: "pods"}, plan.PodName, errors.New("changed"))
				}
				patch, err := jsonpatch.DecodePatch(a.(ktesting.PatchAction).GetPatch())
				require.NoError(t, err)
				before, err := pod.MarshalJSON()
				require.NoError(t, err)
				after, err := patch.Apply(before)
				require.NoError(t, err)
				stale := pod.DeepCopy()
				stale.SetResourceVersion("concurrent")
				staleJSON, err := stale.MarshalJSON()
				require.NoError(t, err)
				_, err = patch.Apply(staleJSON)
				require.Error(t, err)
				require.NoError(t, pod.UnmarshalJSON(after))
				pod.SetResourceVersion(pod.GetResourceVersion() + "1")
				return true, pod.DeepCopy(), nil
			})
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			admit := func(context.Context) error {
				if restoring && mode == "restore-blocked" {
					return errors.New("protocol recovery unproven")
				}
				return nil
			}
			err := PrepareNetworkLabel(ctx, client, dir, plan, "brain", admit)
			switch mode {
			case "prepare-conflict", "active-reservation", "reservation-replaced", "prepare-no-policy":
				require.Error(t, err)
				if mode != "prepare-conflict" {
					require.Zero(t, patches)
				}
				return
			}
			require.NoError(t, err)
			require.Equal(t, plan.Nonce, pod.GetLabels()["kubebrain.io/fault-owner"])
			require.Equal(t, 1, patches)
			restoring = true
			if mode == "restore-foreign-label" {
				pod.SetLabels(map[string]string{"kubebrain.io/fault-owner": "foreign"})
			}
			if mode == "restore-already-absent" {
				pod.SetLabels(map[string]string{"app": "brain"})
			}
			err = RestoreNetworkLabel(ctx, client, dir, plan, "brain", admit)
			switch mode {
			case "roundtrip", "no-label-map", "restore-already-absent":
				require.NoError(t, err)
				require.NotContains(t, pod.GetLabels(), "kubebrain.io/fault-owner")
				if mode != "no-label-map" {
					require.Equal(t, "brain", pod.GetLabels()["app"])
				}
				if mode == "restore-already-absent" {
					require.Equal(t, 1, patches)
				} else {
					require.Equal(t, 2, patches)
				}
			default:
				require.Error(t, err)
				if mode == "restore-conflict" {
					require.Equal(t, 2, patches)
				} else {
					require.Equal(t, 1, patches)
				}
			}
		})
	}
}
