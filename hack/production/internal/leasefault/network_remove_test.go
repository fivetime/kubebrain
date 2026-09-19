package leasefault

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic/fake"
	ktesting "k8s.io/client-go/testing"
)

func TestRemoveNetworkPolicy(t *testing.T) {
	for _, mode := range []string{"inactive", "active", "absent", "replacement", "spec-changed", "conflict", "still-present", "delete-notfound", "ownership-lost", "receipt-lost", "missing-receipt", "cancelled"} {
		t.Run(mode, func(t *testing.T) {
			dir := t.TempDir()
			require.NoError(t, os.Chmod(dir, 0700))
			plan := networkPlan()
			require.NoError(t, ArmNetworkRecovery(dir, plan))
			if mode != "missing-receipt" {
				require.NoError(t, SaveNetworkReservation(dir, plan, reservationFixture()))
			}
			var current unstructured.Unstructured
			require.NoError(t, current.UnmarshalJSON(reservationFixture()))
			current.SetResourceVersion("9007199254740993")
			if mode == "active" {
				require.NoError(t, unstructured.SetNestedField(current.Object, plan.Nonce, "spec", "endpointSelector", "matchLabels", "kubebrain.io/fault-owner"))
			}
			if mode == "replacement" {
				current.SetUID("replacement")
			}
			if mode == "spec-changed" {
				require.NoError(t, unstructured.SetNestedField(current.Object, "changed", "spec", "description"))
			}
			resource := schema.GroupResource{Group: "cilium.io", Resource: "ciliumnetworkpolicies"}
			client := fake.NewSimpleDynamicClient(runtime.NewScheme())
			gets, deletes, admissions := 0, 0, 0
			client.PrependReactor("get", resource.Resource, func(action ktesting.Action) (bool, runtime.Object, error) {
				gets++
				require.Equal(t, plan.Namespace, action.GetNamespace())
				require.Equal(t, plan.PolicyName, action.(ktesting.GetAction).GetName())
				if mode == "absent" || (deletes > 0 && mode != "still-present") {
					return true, nil, apierrors.NewNotFound(resource, plan.PolicyName)
				}
				if mode == "receipt-lost" {
					require.NoError(t, os.Remove(dir+"/"+networkReceiptFile))
				}
				return true, current.DeepCopy(), nil
			})
			client.PrependReactor("delete", resource.Resource, func(action ktesting.Action) (bool, runtime.Object, error) {
				deletes++
				a := action.(ktesting.DeleteAction)
				require.Equal(t, plan.Namespace, a.GetNamespace())
				require.Equal(t, plan.PolicyName, a.GetName())
				pre := a.GetDeleteOptions().Preconditions
				require.NotNil(t, pre)
				require.Equal(t, current.GetUID(), *pre.UID)
				require.Equal(t, "9007199254740993", *pre.ResourceVersion)
				if mode == "conflict" {
					return true, nil, apierrors.NewConflict(resource, plan.PolicyName, errors.New("changed"))
				}
				if mode == "delete-notfound" {
					return true, nil, apierrors.NewNotFound(resource, plan.PolicyName)
				}
				return true, nil, nil
			})
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			if mode == "cancelled" {
				cancel()
			}
			err := RemoveNetworkPolicy(ctx, client, dir, plan, func(context.Context) error {
				admissions++
				if mode == "ownership-lost" && admissions == 2 {
					return errors.New("ownership lost")
				}
				return nil
			})
			switch mode {
			case "inactive", "active", "absent", "delete-notfound":
				require.NoError(t, err)
			default:
				require.Error(t, err)
			}
			switch mode {
			case "inactive", "active", "conflict", "still-present", "delete-notfound":
				require.Equal(t, 1, deletes)
			default:
				require.Zero(t, deletes)
			}
			if mode == "missing-receipt" || mode == "cancelled" {
				require.Zero(t, gets)
			}
			if mode == "inactive" || mode == "active" || mode == "delete-notfound" {
				require.Equal(t, 2, gets)
			}
		})
	}
}
