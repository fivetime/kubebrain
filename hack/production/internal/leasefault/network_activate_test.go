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
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic/fake"
	ktesting "k8s.io/client-go/testing"
)

func TestActivateNetworkPreconditions(t *testing.T) {
	for _, mode := range []string{"success", "replacement", "changed-spec", "conflict", "ownership-lost", "missing-receipt", "reset-clock", "bad-response"} {
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
			if mode == "replacement" {
				current.SetUID("replacement")
			}
			if mode == "changed-spec" {
				require.NoError(t, unstructured.SetNestedField(current.Object, "other", "spec", "endpointSelector", "matchLabels", "kubebrain.io/fault-owner"))
			}
			client := fake.NewSimpleDynamicClient(runtime.NewScheme())
			gets, patches := 0, 0
			client.PrependReactor("get", "ciliumnetworkpolicies", func(action ktesting.Action) (bool, runtime.Object, error) {
				gets++
				require.Equal(t, plan.Namespace, action.GetNamespace())
				return true, current.DeepCopy(), nil
			})
			client.PrependReactor("patch", "ciliumnetworkpolicies", func(action ktesting.Action) (bool, runtime.Object, error) {
				patches++
				patchAction := action.(ktesting.PatchAction)
				require.Equal(t, plan.PolicyName, patchAction.GetName())
				patch, err := jsonpatch.DecodePatch(patchAction.GetPatch())
				require.NoError(t, err)
				before, err := current.MarshalJSON()
				require.NoError(t, err)
				after, err := patch.Apply(before)
				require.NoError(t, err)
				var updated unstructured.Unstructured
				require.NoError(t, updated.UnmarshalJSON(after))
				label, _, err := unstructured.NestedString(updated.Object, "spec", "endpointSelector", "matchLabels", "kubebrain.io/fault-owner")
				require.NoError(t, err)
				require.Equal(t, plan.Nonce, label)
				for _, mutation := range []string{"uid", "rv", "spec"} {
					stale := current.DeepCopy()
					switch mutation {
					case "uid":
						stale.SetUID("replaced")
					case "rv":
						stale.SetResourceVersion("9007199254740994")
					case "spec":
						require.NoError(t, unstructured.SetNestedField(stale.Object, "x", "spec", "concurrent"))
					}
					data, err := stale.MarshalJSON()
					require.NoError(t, err)
					_, err = patch.Apply(data)
					require.Error(t, err)
				}
				if mode == "conflict" {
					return true, nil, apierrors.NewConflict(schema.GroupResource{Group: "cilium.io", Resource: "ciliumnetworkpolicies"}, plan.PolicyName, errors.New("changed"))
				}
				if mode == "bad-response" {
					updated.SetUID("unexpected")
				}
				return true, &updated, nil
			})
			origin := time.Now()
			budget := 5 * time.Second
			if mode == "reset-clock" {
				budget = time.Minute
			}
			ctx, cancel := context.WithTimeout(context.Background(), budget)
			defer cancel()
			admissions := 0
			admit := func(context.Context) error {
				admissions++
				if mode == "ownership-lost" && admissions == 2 {
					return errors.New("lost ownership")
				}
				return nil
			}
			err := ActivateNetwork(ctx, client, dir, plan, origin, admit)
			if mode == "success" {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
			}
			if mode == "success" || mode == "conflict" || mode == "bad-response" {
				require.Equal(t, 1, patches)
			} else {
				require.Zero(t, patches)
			}
			if mode == "missing-receipt" || mode == "reset-clock" {
				require.Zero(t, gets)
			}
		})
	}
}
