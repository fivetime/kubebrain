package operationarchiver

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/kubewharf/kubebrain/hack/production/internal/namespaceinventory"
	"github.com/kubewharf/kubebrain/hack/production/internal/operationqueue"
	"github.com/kubewharf/kubebrain/hack/production/operationaudit"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic/fake"
)

type recordingProcessor struct {
	names []string
	fail  map[string]error
}

type blockingFirstProcessor struct {
	names []string
}

func (p *blockingFirstProcessor) Process(ctx context.Context, object *unstructured.Unstructured) error {
	p.names = append(p.names, object.GetName())
	if object.GetName() == "blocked" {
		<-ctx.Done()
		return ctx.Err()
	}
	return nil
}

func (p *recordingProcessor) Process(_ context.Context, object *unstructured.Unstructured) error {
	key := object.GetNamespace() + "/" + object.GetName()
	p.names = append(p.names, key)
	return p.fail[key]
}

func TestControllerArchivesTerminalOperationsAcrossInventoryInStableOrder(t *testing.T) {
	client := fakeClient(t,
		inventory("tenant-b", "tenant-a"),
		operation("tenant-a", "later", "Succeeded", 20, true),
		operation("tenant-b", "first-b", "Failed", 10, true),
		operation("tenant-a", "first-a", "Succeeded", 10, true),
		operation("tenant-a", "running", "Running", 1, true),
		operation("tenant-b", "released", "Succeeded", 1, false),
	)
	processor := &recordingProcessor{fail: map[string]error{}}
	controller, err := New(client, processor, "control", "inventory", "namespaces.json", 10)
	require.NoError(t, err)

	processed, err := controller.Reconcile(context.Background())
	require.NoError(t, err)
	require.Equal(t, 3, processed)
	require.Equal(t, []string{
		"tenant-a/first-a", "tenant-b/first-b", "tenant-a/later",
	}, processor.names)
}

func TestControllerIsolatesNamespaceAndOperationFailures(t *testing.T) {
	client := fakeClient(t,
		inventory("tenant-a"),
		operation("tenant-a", "bad", "Succeeded", 1, true),
		operation("tenant-a", "good", "Failed", 2, true),
	)
	processor := &recordingProcessor{fail: map[string]error{
		"tenant-a/bad": errors.New("remote unavailable"),
	}}
	controller, err := New(client, processor, "control", "inventory", "", 10)
	require.NoError(t, err)

	processed, err := controller.Reconcile(context.Background())
	require.Error(t, err)
	require.Contains(t, err.Error(), "remote unavailable")
	require.Equal(t, 1, processed)
	require.Equal(t, []string{"tenant-a/bad", "tenant-a/good"}, processor.names)
}

func TestControllerContinuesAfterPerOperationArchiveTimeout(t *testing.T) {
	client := fakeClient(t,
		inventory("tenant-a"),
		operation("tenant-a", "blocked", "Succeeded", 1, true),
		operation("tenant-a", "following", "Succeeded", 2, true),
	)
	processor := &blockingFirstProcessor{}
	bounded, err := WithTimeout(processor, 10*time.Millisecond)
	require.NoError(t, err)
	controller, err := New(client, bounded, "control", "inventory", "", 10)
	require.NoError(t, err)

	start := time.Now()
	processed, err := controller.Reconcile(context.Background())
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.Equal(t, 1, processed)
	require.Equal(t, []string{"blocked", "following"}, processor.names)
	require.Less(t, time.Since(start), time.Second)
}

func TestArchiveTimeoutRejectsInvalidConfiguration(t *testing.T) {
	_, err := WithTimeout(nil, time.Second)
	require.ErrorContains(t, err, "processor is required")
	_, err = WithTimeout(&recordingProcessor{}, 0)
	require.ErrorContains(t, err, "timeout must be positive")
}

func TestControllerFailsClosedBeforeListingOnInvalidInventory(t *testing.T) {
	badInventory := inventory("tenant-a")
	require.NoError(t, unstructured.SetNestedField(
		badInventory.Object, "not-json", "data", namespaceinventory.DefaultKey,
	))
	client := fakeClient(t, badInventory, operation("tenant-a", "terminal", "Succeeded", 1, true))
	processor := &recordingProcessor{fail: map[string]error{}}
	controller, err := New(client, processor, "control", "inventory", "", 10)
	require.NoError(t, err)

	processed, err := controller.Reconcile(context.Background())
	require.Error(t, err)
	require.Zero(t, processed)
	require.Empty(t, processor.names)
}

func TestControllerHonorsGlobalBatchLimit(t *testing.T) {
	client := fakeClient(t,
		inventory("tenant-a"),
		operation("tenant-a", "one", "Succeeded", 1, true),
		operation("tenant-a", "two", "Succeeded", 2, true),
	)
	processor := &recordingProcessor{fail: map[string]error{}}
	controller, err := New(client, processor, "control", "inventory", "", 1)
	require.NoError(t, err)
	processed, err := controller.Reconcile(context.Background())
	require.NoError(t, err)
	require.Equal(t, 1, processed)
	require.Equal(t, []string{"tenant-a/one"}, processor.names)
	processed, err = controller.Reconcile(context.Background())
	require.NoError(t, err)
	require.Equal(t, 1, processed)
	require.Equal(t, []string{"tenant-a/one", "tenant-a/two"}, processor.names)
}

func fakeClient(t *testing.T, objects ...runtime.Object) *fake.FakeDynamicClient {
	t.Helper()
	scheme := runtime.NewScheme()
	client := fake.NewSimpleDynamicClientWithCustomListKinds(scheme, map[schema.GroupVersionResource]string{
		operationqueue.Resource:              "KubeBrainOperationList",
		namespaceinventory.ConfigMapResource: "ConfigMapList",
	}, objects...)
	return client
}

func inventory(namespaces ...string) *unstructured.Unstructured {
	raw := `["` + namespaces[0] + `"]`
	if len(namespaces) == 2 {
		raw = `["` + namespaces[0] + `","` + namespaces[1] + `"]`
	}
	return &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "v1", "kind": "ConfigMap",
		"metadata": map[string]any{"name": "inventory", "namespace": "control"},
		"data":     map[string]any{namespaceinventory.DefaultKey: raw},
	}}
}

func operation(namespace, name, phase string, completed int64, finalizer bool) *unstructured.Unstructured {
	finalizers := []string{}
	if finalizer {
		finalizers = append(finalizers, operationaudit.Finalizer)
	}
	finalizerValues := make([]any, 0, len(finalizers))
	for _, value := range finalizers {
		finalizerValues = append(finalizerValues, value)
	}
	return &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": operationqueue.Resource.Group + "/" + operationqueue.Resource.Version,
		"kind":       "KubeBrainOperation",
		"metadata": map[string]any{
			"name": name, "namespace": namespace, "finalizers": finalizerValues,
			"creationTimestamp": metav1.Now().Format("2006-01-02T15:04:05Z"),
		},
		"status": map[string]any{"phase": phase, "completedAtUnix": completed},
	}}
}
