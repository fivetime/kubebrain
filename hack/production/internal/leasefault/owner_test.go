package leasefault

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/dynamic/fake"
	ktesting "k8s.io/client-go/testing"
)

func TestFaultOwnerClaim(t *testing.T) {
	for _, mode := range []string{"success", "same-owner-busy", "other-owner-busy", "ambiguous-create", "replaced", "changed-holder", "mutable", "scope-replaced", "missing-receipt", "symlink-receipt", "recovery-failed", "delete-conflict"} {
		t.Run(mode, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			b := FaultOwnerBinding{Owner: "attempt-a", Namespace: "test", NamespaceUID: "namespace-uid", StatefulSetName: "brain", StatefulSetUID: "sts-uid"}
			dir := t.TempDir()
			require.NoError(t, os.Chmod(dir, 0700))
			ns := &unstructured.Unstructured{Object: map[string]interface{}{"apiVersion": "v1", "kind": "Namespace", "metadata": map[string]interface{}{"name": b.Namespace, "uid": b.NamespaceUID, "resourceVersion": "1"}}}
			sts := &unstructured.Unstructured{Object: map[string]interface{}{"apiVersion": "apps/v1", "kind": "StatefulSet", "metadata": map[string]interface{}{"name": b.StatefulSetName, "namespace": b.Namespace, "uid": b.StatefulSetUID, "resourceVersion": "1"}}}
			client := fake.NewSimpleDynamicClient(runtime.NewScheme(), ns, sts)
			creates, deletes := 0, 0
			client.PrependReactor("create", "configmaps", func(a ktesting.Action) (bool, runtime.Object, error) {
				creates++
				var intent FaultOwnerBinding
				require.NoError(t, readOwnerRecord(dir, ownerIntentFile, &intent))
				require.Equal(t, b, intent)
				u := a.(ktesting.CreateAction).GetObject().(*unstructured.Unstructured).DeepCopy()
				u.SetUID("claim-uid")
				u.SetResourceVersion("18446744073709551615")
				if err := client.Tracker().Create(ownerResource, u, b.Namespace); err != nil {
					return true, nil, err
				}
				if mode == "ambiguous-create" {
					return true, nil, errors.New("CREATE response lost")
				}
				return true, u, nil
			})
			client.PrependReactor("delete", "configmaps", func(a ktesting.Action) (bool, runtime.Object, error) {
				deletes++
				p := a.(ktesting.DeleteAction).GetDeleteOptions().Preconditions
				require.NotNil(t, p)
				require.NotNil(t, p.UID)
				require.NotNil(t, p.ResourceVersion)
				require.Equal(t, types.UID("claim-uid"), *p.UID)
				require.Equal(t, "18446744073709551615", *p.ResourceVersion)
				if mode == "delete-conflict" {
					return true, nil, apierrors.NewConflict(schema.GroupResource{Resource: "configmaps"}, faultOwnerName, errors.New("changed"))
				}
				return false, nil, nil
			})
			o, err := AcquireFaultOwner(ctx, client, dir, b)
			if mode == "ambiguous-create" {
				require.Error(t, err)
				require.Nil(t, o)
				_, err = client.Resource(ownerResource).Namespace(b.Namespace).Get(ctx, faultOwnerName, metav1.GetOptions{})
				require.NoError(t, err)
				_, err = LoadFaultOwner(client, dir, b)
				require.ErrorIs(t, err, os.ErrNotExist)
				_, err = AcquireFaultOwner(ctx, client, dir, b)
				require.Error(t, err)
				require.Equal(t, 1, creates)
				require.Zero(t, deletes)
				return
			}
			require.NoError(t, err)
			require.NoError(t, o.Check(ctx))
			if mode == "same-owner-busy" || mode == "other-owner-busy" {
				otherDir := t.TempDir()
				require.NoError(t, os.Chmod(otherDir, 0700))
				other := b
				if mode == "other-owner-busy" {
					other.Owner = "attempt-b"
				}
				_, err = AcquireFaultOwner(ctx, client, otherDir, other)
				require.True(t, apierrors.IsAlreadyExists(err))
				require.NoError(t, o.Check(ctx))
				require.Zero(t, deletes)
				return
			}
			switch mode {
			case "replaced", "changed-holder", "mutable":
				u, err := client.Resource(ownerResource).Namespace(b.Namespace).Get(ctx, faultOwnerName, metav1.GetOptions{})
				require.NoError(t, err)
				if mode == "replaced" {
					u.SetUID("replacement")
				}
				if mode == "changed-holder" {
					require.NoError(t, unstructured.SetNestedField(u.Object, "foreign", "data", "owner"))
				}
				if mode == "mutable" {
					u.Object["immutable"] = false
				}
				require.NoError(t, client.Tracker().Update(ownerResource, u, b.Namespace))
			case "scope-replaced":
				ns.SetUID("replacement")
				require.NoError(t, client.Tracker().Update(schema.GroupVersionResource{Version: "v1", Resource: "namespaces"}, ns, ""))
			case "missing-receipt":
				require.NoError(t, os.Remove(filepath.Join(dir, ownerReceiptFile)))
			case "symlink-receipt":
				require.NoError(t, os.Rename(filepath.Join(dir, ownerReceiptFile), filepath.Join(dir, "original")))
				require.NoError(t, os.Symlink("original", filepath.Join(dir, ownerReceiptFile)))
			}
			proofCalls := 0
			err = o.Release(ctx, func(context.Context) error {
				proofCalls++
				if mode == "recovery-failed" {
					return errors.New("workers remain")
				}
				return nil
			})
			if mode == "success" {
				require.NoError(t, err)
				require.Equal(t, 1, deletes)
				require.Error(t, o.Check(ctx))
				_, err = LoadFaultOwner(client, dir, b)
				require.NoError(t, err, "records remain after release")
			} else {
				require.Error(t, err)
			}
			if mode == "replaced" || mode == "changed-holder" || mode == "mutable" || mode == "scope-replaced" || mode == "missing-receipt" || mode == "symlink-receipt" {
				require.Zero(t, proofCalls)
				require.Zero(t, deletes)
			}
			if mode == "recovery-failed" {
				require.Zero(t, deletes)
				require.NoError(t, o.Check(ctx))
			}
			if mode == "delete-conflict" {
				require.True(t, apierrors.IsConflict(err))
				require.Equal(t, 1, deletes)
				require.NoError(t, o.Check(ctx))
			}
		})
	}
}
