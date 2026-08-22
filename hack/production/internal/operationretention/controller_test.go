package operationretention

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/kubewharf/kubebrain/hack/production/internal/dynamicpagination"
	"github.com/kubewharf/kubebrain/hack/production/internal/namespaceinventory"
	"github.com/kubewharf/kubebrain/hack/production/internal/operationqueue"
	"github.com/kubewharf/kubebrain/hack/production/operationaudit"
	"github.com/stretchr/testify/require"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/dynamic/fake"
	k8stesting "k8s.io/client-go/testing"
)

type recordingVerifier struct {
	names []string
	err   error
}

var retainedCandidateSink []*unstructured.Unstructured

func BenchmarkChargeAndDeepCopyNearByteBudgetCandidates(b *testing.B) {
	payload := strings.Repeat("x", 120<<10)
	items := make([]unstructured.Unstructured, 500)
	for i := range items {
		items[i] = unstructured.Unstructured{Object: map[string]any{
			"apiVersion": "dbaas.kubebrain.io/v1alpha1", "kind": "KubeBrainOperation",
			"metadata": map[string]any{"name": fmt.Sprintf("operation-%03d", i)},
			"payload":  fmt.Sprintf("%03d%s", i, payload),
		}}
	}
	b.ReportAllocs()
	b.ResetTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		charged := int64(0)
		candidates := make([]*unstructured.Unstructured, 0, len(items))
		for i := range items {
			var err error
			charged, err = dynamicpagination.Charge(charged, 64<<20, &items[i])
			if err != nil {
				b.Fatal(err)
			}
			candidates = append(candidates, items[i].DeepCopy())
		}
		retainedCandidateSink = candidates
		b.ReportMetric(float64(charged)/(1<<20), "charged-MiB/op")
	}
}

func (v *recordingVerifier) Process(_ context.Context, object *unstructured.Unstructured) error {
	v.names = append(v.names, object.GetNamespace()+"/"+object.GetName())
	return v.err
}

func TestControllerDeletesOnlyExpiredReleasedEvidenceAfterVerification(t *testing.T) {
	now := time.Unix(2_000_000, 0)
	client := retentionClient(t,
		retentionInventory("tenant-a"),
		retainedOperation("eligible", now.Add(-49*time.Hour).Unix(), "Succeeded", false, true),
		retainedOperation("young", now.Add(-23*time.Hour).Unix(), "Failed", false, true),
		retainedOperation("finalized", now.Add(-49*time.Hour).Unix(), "Succeeded", true, true),
		retainedOperation("no-evidence", now.Add(-49*time.Hour).Unix(), "Succeeded", false, false),
		retainedOperation("running", now.Add(-49*time.Hour).Unix(), "Running", false, true),
	)
	verifier := &recordingVerifier{}
	controller, err := New(client, verifier, "control", "inventory", "", 24*time.Hour, 10)
	require.NoError(t, err)
	controller.now = func() time.Time { return now }

	deleted, err := controller.Reconcile(context.Background())
	require.NoError(t, err)
	require.Equal(t, 1, deleted)
	require.Equal(t, []string{"tenant-a/eligible"}, verifier.names)
	_, err = client.Resource(operationqueue.Resource).Namespace("tenant-a").Get(context.Background(), "eligible", metav1.GetOptions{})
	require.True(t, apierrors.IsNotFound(err))
	for _, name := range []string{"young", "finalized", "no-evidence", "running"} {
		_, err = client.Resource(operationqueue.Resource).Namespace("tenant-a").Get(context.Background(), name, metav1.GetOptions{})
		require.NoError(t, err)
	}
	deleteAction := findDeleteAction(t, client.Actions())
	require.Equal(t, types.UID("uid-eligible"), *deleteAction.GetDeleteOptions().Preconditions.UID)
	require.Equal(t, "rv-eligible", *deleteAction.GetDeleteOptions().Preconditions.ResourceVersion)
}

func TestControllerDoesNotDeleteWhenRemoteVerificationFails(t *testing.T) {
	client := retentionClient(t, retentionInventory("tenant-a"), retainedOperation("eligible", 1, "Succeeded", false, true))
	controller, err := New(client, &recordingVerifier{err: errors.New("remote version mismatch")}, "control", "inventory", "", time.Second, 10)
	require.NoError(t, err)
	controller.now = func() time.Time { return time.Unix(100, 0) }
	deleted, err := controller.Reconcile(context.Background())
	require.Zero(t, deleted)
	require.ErrorContains(t, err, "remote version mismatch")
	for _, action := range client.Actions() {
		require.NotEqual(t, "delete", action.GetVerb())
	}
}

func TestControllerPreservesReplacementAfterAmbiguousDelete(t *testing.T) {
	original := retainedOperation("same-name", 1, "Succeeded", false, true)
	client := retentionClient(t, retentionInventory("tenant-a"), original)
	client.PrependReactor("delete", operationqueue.Resource.Resource, func(action k8stesting.Action) (bool, runtime.Object, error) {
		require.NoError(t, client.Tracker().Delete(operationqueue.Resource, "tenant-a", "same-name"))
		replacement := retainedOperation("same-name", 1, "Succeeded", false, true)
		replacement.SetUID("uid-replacement")
		replacement.SetResourceVersion("rv-replacement")
		require.NoError(t, client.Tracker().Create(operationqueue.Resource, replacement, "tenant-a"))
		return true, nil, apierrors.NewConflict(schema.GroupResource{Group: operationqueue.Resource.Group, Resource: operationqueue.Resource.Resource}, "same-name", errors.New("ambiguous"))
	})
	controller, err := New(client, &recordingVerifier{}, "control", "inventory", "", time.Second, 10)
	require.NoError(t, err)
	controller.now = func() time.Time { return time.Unix(100, 0) }
	deleted, err := controller.Reconcile(context.Background())
	require.NoError(t, err)
	require.Equal(t, 1, deleted)
	current, err := client.Resource(operationqueue.Resource).Namespace("tenant-a").Get(context.Background(), "same-name", metav1.GetOptions{})
	require.NoError(t, err)
	require.Equal(t, types.UID("uid-replacement"), current.GetUID())
}

func TestControllerFailsClosedWhenResourceVersionChangesBeforeDelete(t *testing.T) {
	original := retainedOperation("changed", 1, "Succeeded", false, true)
	client := retentionClient(t, retentionInventory("tenant-a"), original)
	client.PrependReactor("delete", operationqueue.Resource.Resource, func(action k8stesting.Action) (bool, runtime.Object, error) {
		current, err := client.Tracker().Get(operationqueue.Resource, "tenant-a", "changed")
		require.NoError(t, err)
		updated := current.(*unstructured.Unstructured).DeepCopy()
		updated.SetResourceVersion("rv-new")
		require.NoError(t, client.Tracker().Update(operationqueue.Resource, updated, "tenant-a"))
		return true, nil, apierrors.NewConflict(schema.GroupResource{Group: operationqueue.Resource.Group, Resource: operationqueue.Resource.Resource}, "changed", errors.New("resourceVersion precondition"))
	})
	controller, err := New(client, &recordingVerifier{}, "control", "inventory", "", time.Second, 10)
	require.NoError(t, err)
	controller.now = func() time.Time { return time.Unix(100, 0) }
	deleted, err := controller.Reconcile(context.Background())
	require.Zero(t, deleted)
	require.ErrorContains(t, err, "resourceVersion precondition")
	current, getErr := client.Resource(operationqueue.Resource).Namespace("tenant-a").Get(context.Background(), "changed", metav1.GetOptions{})
	require.NoError(t, getErr)
	require.Equal(t, types.UID("uid-changed"), current.GetUID())
	require.Equal(t, "rv-new", current.GetResourceVersion())
}

func TestControllerRejectsInvalidConfigurationAndBudget(t *testing.T) {
	client := retentionClient(t, retentionInventory("tenant-a"))
	_, err := New(client, nil, "control", "inventory", "", time.Hour, 1)
	require.ErrorContains(t, err, "configuration is incomplete")
	controller, err := New(client, &recordingVerifier{}, "control", "inventory", "", time.Hour, 1)
	require.NoError(t, err)
	require.Error(t, controller.SetScanBudget(0, 1))
	require.Error(t, controller.SetScanBudget(1, 0))
	require.NoError(t, controller.SetScanBudget(7, 11))
}

func retentionClient(t *testing.T, objects ...runtime.Object) *fake.FakeDynamicClient {
	t.Helper()
	return fake.NewSimpleDynamicClientWithCustomListKinds(runtime.NewScheme(), map[schema.GroupVersionResource]string{
		operationqueue.Resource: "KubeBrainOperationList", namespaceinventory.ConfigMapResource: "ConfigMapList",
	}, objects...)
}

func retentionInventory(namespaces ...string) *unstructured.Unstructured {
	raw := `[`
	for i, namespace := range namespaces {
		if i > 0 {
			raw += `,`
		}
		raw += `"` + namespace + `"`
	}
	raw += `]`
	return &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "v1", "kind": "ConfigMap",
		"metadata": map[string]any{"namespace": "control", "name": "inventory"},
		"data":     map[string]any{namespaceinventory.DefaultKey: raw},
	}}
}

func retainedOperation(name string, completed int64, phase string, finalizer, evidence bool) *unstructured.Unstructured {
	annotations := map[string]any{}
	if evidence {
		annotations[operationaudit.ReceiptSHAAnnotation] = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
		annotations[operationaudit.ArtifactSHAAnnotation] = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
		annotations[operationaudit.VersionAnnotation] = "version-1"
	}
	finalizers := []any{}
	if finalizer {
		finalizers = append(finalizers, operationaudit.Finalizer)
	}
	object := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "dbaas.kubebrain.io/v1alpha1", "kind": "KubeBrainOperation",
		"metadata": map[string]any{"namespace": "tenant-a", "name": name, "annotations": annotations, "finalizers": finalizers},
		"status":   map[string]any{"phase": phase, "completedAtUnix": completed},
	}}
	object.SetUID(types.UID("uid-" + name))
	object.SetResourceVersion("rv-" + name)
	return object
}

func findDeleteAction(t *testing.T, actions []k8stesting.Action) k8stesting.DeleteAction {
	t.Helper()
	for _, action := range actions {
		if action.GetVerb() == "delete" {
			return action.(k8stesting.DeleteAction)
		}
	}
	t.Fatal("delete action not found")
	return nil
}
