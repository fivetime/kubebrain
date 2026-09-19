package leasefault

import (
	"context"
	"errors"
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

func TestCheckNetworkIdentity(t *testing.T) {
	for _, mode := range []string{"unlabelled", "owned", "recovery-absent", "recovery-owned", "ns-replaced", "sts-replaced", "pod-replaced", "deleting", "foreign-controller", "no-controller", "foreign-label", "missing-label", "unexpected-label", "reserved-collision", "other-pod", "pagination", "changed-rv", "list-error", "ownership-lost", "cancelled"} {
		t.Run(mode, func(t *testing.T) {
			plan := networkPlan()
			object := func(api, kind, ns, name, uid string) *unstructured.Unstructured {
				obj := &unstructured.Unstructured{Object: map[string]interface{}{}}
				obj.SetAPIVersion(api)
				obj.SetKind(kind)
				obj.SetNamespace(ns)
				obj.SetName(name)
				obj.SetUID(types.UID(uid))
				obj.SetResourceVersion("18446744073709551615")
				return obj
			}
			ns := object("v1", "Namespace", "", plan.Namespace, plan.NamespaceUID)
			sts := object("apps/v1", "StatefulSet", plan.Namespace, "brain", plan.StatefulSetUID)
			pod := object("v1", "Pod", plan.Namespace, plan.PodName, plan.PodUID)
			controller := true
			pod.SetOwnerReferences([]metav1.OwnerReference{{APIVersion: "apps/v1", Kind: "StatefulSet", Name: "brain", UID: types.UID(plan.StatefulSetUID), Controller: &controller}})
			phase := NetworkLabelOwned
			pod.SetLabels(map[string]string{"kubebrain.io/fault-owner": plan.Nonce})
			switch mode {
			case "unlabelled", "unexpected-label":
				phase = NetworkUnlabelled
			case "recovery-absent", "recovery-owned":
				phase = NetworkLabelRecovery
			}
			if mode == "unlabelled" || mode == "recovery-absent" || mode == "missing-label" {
				pod.SetLabels(nil)
			}
			switch mode {
			case "ns-replaced":
				ns.SetUID("replacement")
			case "sts-replaced":
				sts.SetUID("replacement")
			case "pod-replaced":
				pod.SetUID("replacement")
			case "deleting":
				stamp := metav1.Now()
				pod.SetDeletionTimestamp(&stamp)
			case "foreign-controller":
				refs := pod.GetOwnerReferences()
				refs[0].UID = "replacement"
				pod.SetOwnerReferences(refs)
			case "no-controller":
				pod.SetOwnerReferences(nil)
			case "foreign-label":
				pod.SetLabels(map[string]string{"kubebrain.io/fault-owner": "foreign"})
			}
			client := fake.NewSimpleDynamicClientWithCustomListKinds(runtime.NewScheme(), map[schema.GroupVersionResource]string{
				{Version: "v1", Resource: "pods"}: "PodList",
			})
			client.PrependReactor("get", "*", func(a ktesting.Action) (bool, runtime.Object, error) {
				switch a.GetResource().Resource {
				case "namespaces":
					require.Empty(t, a.GetNamespace())
					return true, ns.DeepCopy(), nil
				case "statefulsets":
					require.Equal(t, plan.Namespace, a.GetNamespace())
					return true, sts.DeepCopy(), nil
				case "pods":
					require.Equal(t, plan.Namespace, a.GetNamespace())
					return true, pod.DeepCopy(), nil
				}
				return true, nil, errors.New("unexpected GET")
			})
			client.PrependReactor("list", "pods", func(a ktesting.Action) (bool, runtime.Object, error) {
				require.Equal(t, plan.Namespace, a.GetNamespace())
				require.Equal(t, "kubebrain.io/fault-owner in (active,reserved)", a.(ktesting.ListAction).GetListRestrictions().Labels.String())
				if mode == "list-error" {
					return true, nil, errors.New("forbidden")
				}
				list := &unstructured.UnstructuredList{}
				if _, found := pod.GetLabels()["kubebrain.io/fault-owner"]; found {
					list.Items = []unstructured.Unstructured{*pod.DeepCopy()}
				}
				switch mode {
				case "reserved-collision":
					list.Items[0].SetLabels(map[string]string{"kubebrain.io/fault-owner": plan.ReservedNonce})
				case "other-pod":
					list.Items[0].SetUID("other-pod")
				case "pagination":
					list.SetContinue("next")
				case "changed-rv":
					list.Items[0].SetResourceVersion("18446744073709551616")
				}
				return true, list, nil
			})
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			if mode == "cancelled" {
				cancel()
			}
			checks := 0
			err := CheckNetworkIdentity(ctx, client, plan, "brain", phase, func(context.Context) error {
				checks++
				if mode == "ownership-lost" && checks == 2 {
					return errors.New("lost exclusive ownership")
				}
				return nil
			})
			switch mode {
			case "unlabelled", "owned", "recovery-absent", "recovery-owned":
				require.NoError(t, err)
				require.Equal(t, 2, checks)
			default:
				require.Error(t, err)
			}
			for _, action := range client.Actions() {
				require.Contains(t, []string{"get", "list"}, action.GetVerb())
			}
			if mode == "cancelled" {
				require.Empty(t, client.Actions())
			}
		})
	}
}
