package operationarchiveverifier

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/kubewharf/kubebrain/hack/production/internal/operationqueue"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic/fake"
)

type recordingVerifier struct {
	names []string
	fail  map[string]error
}

func (v *recordingVerifier) Process(_ context.Context, object *unstructured.Unstructured) error {
	v.names = append(v.names, object.GetName())
	return v.fail[object.GetName()]
}

func TestControllerFairlyVerifiesReleasedTerminalOperationsAndEvidenceGaps(t *testing.T) {
	client := verifierClient(t,
		verifierOperation("first", 10, false, true), verifierOperation("gap", 20, false, false),
		verifierOperation("pending", 30, true, false), verifierOperation("last", 40, false, true),
	)
	processor := &recordingVerifier{fail: map[string]error{"gap": errors.New("missing evidence")}}
	controller, err := NewController(client, processor, "control", "inventory", "namespaces.json", 2)
	require.NoError(t, err)
	controller.now = func() time.Time { return time.Unix(0, 0) }

	verified, err := controller.Reconcile(context.Background())
	require.ErrorContains(t, err, "verify operation archive tenant-a/gap: missing evidence")
	require.Equal(t, 1, verified)
	require.Equal(t, []string{"first", "gap"}, processor.names)
	verified, err = controller.Reconcile(context.Background())
	require.NoError(t, err)
	require.Equal(t, 2, verified)
	require.Equal(t, []string{"first", "gap", "last", "first"}, processor.names)
}

func TestFreshHourlyControllersRotateReadOnlyBatchWithoutPersistentCursor(t *testing.T) {
	client := verifierClient(t,
		verifierOperation("a", 10, false, true), verifierOperation("b", 20, false, true),
		verifierOperation("c", 30, false, true),
	)
	processor := &recordingVerifier{fail: map[string]error{}}
	for hour := int64(0); hour < 3; hour++ {
		controller, err := NewController(client, processor, "control", "inventory", "namespaces.json", 1)
		require.NoError(t, err)
		controller.now = func() time.Time { return time.Unix(hour*3600, 0) }
		verified, err := controller.Reconcile(context.Background())
		require.NoError(t, err)
		require.Equal(t, 1, verified)
	}
	require.Equal(t, []string{"a", "b", "c"}, processor.names)
}

func verifierClient(t *testing.T, operations ...*unstructured.Unstructured) *fake.FakeDynamicClient {
	t.Helper()
	scheme := runtime.NewScheme()
	require.NoError(t, corev1.AddToScheme(scheme))
	objects := []runtime.Object{&corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: "inventory", Namespace: "control"}, Data: map[string]string{"namespaces.json": `["tenant-a"]`}}}
	for _, operation := range operations {
		objects = append(objects, operation)
	}
	return fake.NewSimpleDynamicClientWithCustomListKinds(scheme, map[schema.GroupVersionResource]string{operationqueue.Resource: "KubeBrainOperationList"}, objects...)
}
