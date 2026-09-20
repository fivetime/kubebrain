package leasefault

import (
	"context"
	"errors"
	"os"
	"path/filepath"
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

func TestReleaseRecoveredRequiresFreshProof(t *testing.T) {
	for _, mode := range []string{"success", "join", "network", "identity", "admit", "policy", "policy-reappears", "protocol", "missing", "wrong-scope", "delete-error"} {
		t.Run(mode, func(t *testing.T) {
			n := networkPlan()
			b := FaultOwnerBinding{Owner: n.Owner, Namespace: n.Namespace, NamespaceUID: n.NamespaceUID, StatefulSetName: "brain", StatefulSetUID: n.StatefulSetUID}
			object := func(api, kind, ns, name, uid string) *unstructured.Unstructured {
				o := &unstructured.Unstructured{}
				o.SetAPIVersion(api)
				o.SetKind(kind)
				o.SetNamespace(ns)
				o.SetName(name)
				o.SetUID(types.UID(uid))
				o.SetResourceVersion("1")
				return o
			}
			pod := object("v1", "Pod", n.Namespace, n.PodName, n.PodUID)
			controller := true
			pod.SetOwnerReferences([]metav1.OwnerReference{{APIVersion: "apps/v1", Kind: "StatefulSet", Name: "brain", UID: types.UID(n.StatefulSetUID), Controller: &controller}})
			client := fake.NewSimpleDynamicClientWithCustomListKinds(runtime.NewScheme(), map[schema.GroupVersionResource]string{{Version: "v1", Resource: "pods"}: "PodList"}, object("v1", "Namespace", "", n.Namespace, n.NamespaceUID), object("apps/v1", "StatefulSet", n.Namespace, "brain", n.StatefulSetUID), pod)
			client.PrependReactor("create", "configmaps", func(a ktesting.Action) (bool, runtime.Object, error) {
				o := a.(ktesting.CreateAction).GetObject().(*unstructured.Unstructured).DeepCopy()
				o.SetUID("claim-uid")
				o.SetResourceVersion("1")
				require.NoError(t, client.Tracker().Create(ownerResource, o, n.Namespace))
				return true, o, nil
			})
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			dir := t.TempDir()
			require.NoError(t, os.Chmod(dir, 0700))
			owner, err := AcquireFaultOwner(ctx, client, dir, b)
			require.NoError(t, err)
			client.ClearActions()
			deletes := 0
			client.PrependReactor("delete", "configmaps", func(a ktesting.Action) (bool, runtime.Object, error) {
				deletes++
				pre := a.(ktesting.DeleteAction).GetDeleteOptions().Preconditions
				require.Equal(t, types.UID("claim-uid"), *pre.UID)
				require.Equal(t, "1", *pre.ResourceVersion)
				if mode == "delete-error" {
					return true, nil, errors.New("delete response lost")
				}
				return false, nil, nil
			})
			policy := func() {
				require.NoError(t, client.Tracker().Create(schema.GroupVersionResource{Group: "cilium.io", Version: "v2", Resource: "ciliumnetworkpolicies"}, object("cilium.io/v2", "CiliumNetworkPolicy", n.Namespace, n.PolicyName, "policy-uid"), n.Namespace))
			}
			if mode == "policy" {
				policy()
			}
			protocol := ProtocolRecovery{Owner: n.Owner, NamespaceUID: n.NamespaceUID, StatefulSetUID: n.StatefulSetUID, ClusterID: 1, AlarmMemberID: 2, LeaseID: 3, Key: "/acceptance/fixture"}
			conn := &recoveryConnection{t: t, plan: protocol}
			if mode == "protocol" {
				conn.mode = "key-remains"
			}
			stages := []string{}
			check := func(stage string) func(context.Context) error {
				return func(context.Context) error {
					stages = append(stages, stage)
					if mode == stage {
						return errors.New(stage + " refused")
					}
					if stage == "identity" && mode == "policy-reappears" {
						policy()
					}
					return nil
				}
			}
			proof := RecoveryReleaseProof{Network: n, Protocol: protocol, Connection: conn, Admit: check("admit"), Join: check("join"), NetworkRestored: check("network"), IdentityRestored: check("identity")}
			if mode == "missing" {
				proof.Join = nil
			}
			if mode == "wrong-scope" {
				proof.Network.Owner = "other"
			}
			err = owner.ReleaseRecovered(ctx, proof)
			if mode == "success" {
				require.NoError(t, err)
				require.Equal(t, 1, deletes)
				require.Equal(t, 3, conn.calls)
			} else {
				require.Error(t, err)
				if mode == "delete-error" {
					require.Equal(t, 1, deletes)
				} else {
					require.Zero(t, deletes)
				}
				require.NoError(t, owner.Check(ctx), "failure retains exact acquired claim")
			}
			for _, action := range client.Actions() {
				if action.GetVerb() != "get" && action.GetVerb() != "list" {
					require.Equal(t, "delete", action.GetVerb())
					require.Equal(t, "configmaps", action.GetResource().Resource)
				}
			}
			require.FileExists(t, filepath.Join(dir, ownerReceiptFile))
		})
	}
}
