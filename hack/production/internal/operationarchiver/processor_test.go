package operationarchiver

import (
	"context"
	"encoding/json"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/kubewharf/kubebrain/hack/production/internal/operationqueue"
	"github.com/kubewharf/kubebrain/hack/production/operationaudit"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/dynamic/fake"
)

func TestArchiveProcessorUsesStableIdentityAndReleasesFinalizer(t *testing.T) {
	object := terminalOperation()
	client := fake.NewSimpleDynamicClientWithCustomListKinds(
		runtime.NewScheme(),
		map[schema.GroupVersionResource]string{operationqueue.Resource: "KubeBrainOperationList"},
		object,
	)
	processor, err := NewArchiveProcessor(
		client, "/executor", "store-a", "audit-bucket", "/audit/", "COMPLIANCE", 24*time.Hour,
	)
	require.NoError(t, err)
	processor.now = func() time.Time { return time.Unix(110, 0) }
	processor.run = func(_ context.Context, executable string, environment []string) error {
		require.Equal(t, "/executor", executable)
		values := envMap(environment)
		require.Equal(t, "audit/tenant-a/uid-a.json", values["S3_OBJECT_KEY"])
		require.Equal(t, strconv.FormatInt(100+int64((24*time.Hour)/time.Second), 10), values["RETAIN_UNTIL_UNIX"])
		status, err := operationaudit.Inspect(values["INPUT"])
		require.NoError(t, err)
		receipt := operationaudit.ArchiveReceipt{
			Format:      operationaudit.ArchiveReceiptFormat,
			OperationID: status.Artifact.OperationID, OperationUID: status.Artifact.UID,
			Instance: status.Artifact.Instance, OperationType: status.Artifact.Type,
			Phase: status.Artifact.Phase, ExecutionReceiptSHA256: status.Artifact.ReceiptSHA256,
			ObjectStoreID: "store-a", Bucket: "audit-bucket", ObjectKey: values["S3_OBJECT_KEY"],
			VersionID: "version-a", ArtifactSHA256: status.SHA256, ObjectBytes: status.Bytes,
			RetentionMode: "COMPLIANCE", RetainUntilUnix: 100 + int64((24*time.Hour)/time.Second),
			RemoteVerified: true, ArchivedAtUnix: 110,
		}
		data := mustCanonicalReceipt(t, receipt)
		return os.WriteFile(values["RECEIPT_OUTPUT"], data, 0o600)
	}

	require.NoError(t, processor.Process(context.Background(), object))
	updated, err := client.Resource(operationqueue.Resource).Namespace("tenant-a").
		Get(context.Background(), "operation-a", metav1.GetOptions{})
	require.NoError(t, err)
	require.NotContains(t, updated.GetFinalizers(), operationaudit.Finalizer)
	require.Equal(t, "version-a", updated.GetAnnotations()[operationaudit.VersionAnnotation])
}

func TestArchiveProcessorRejectsExpiredRetentionBeforeExecutor(t *testing.T) {
	object := terminalOperation()
	client := fake.NewSimpleDynamicClient(runtime.NewScheme(), object)
	processor, err := NewArchiveProcessor(
		client, "/executor", "store-a", "bucket", "audit", "GOVERNANCE", time.Second,
	)
	require.NoError(t, err)
	processor.now = func() time.Time { return time.Unix(102, 0) }
	called := false
	processor.run = func(context.Context, string, []string) error {
		called = true
		return nil
	}
	err = processor.Process(context.Background(), object)
	require.ErrorContains(t, err, "deadline is not in the future")
	require.False(t, called)
}

func TestMergeEnvironmentOverridesExistingValues(t *testing.T) {
	merged := mergeEnvironment(
		[]string{"ACTION=delete", "S3_BUCKET=wrong", "KEEP=value"},
		[]string{"ACTION=archive", "S3_BUCKET=audit"},
	)
	require.Equal(t, map[string]string{
		"ACTION": "archive", "S3_BUCKET": "audit", "KEEP": "value",
	}, envMap(merged))
	require.Len(t, merged, 3)
}

func terminalOperation() *unstructured.Unstructured {
	return &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "dbaas.kubebrain.io/v1alpha1", "kind": "KubeBrainOperation",
		"metadata": map[string]any{
			"name": "operation-a", "namespace": "tenant-a", "uid": string(types.UID("uid-a")),
			"generation": int64(1), "finalizers": []any{operationaudit.Finalizer},
		},
		"spec": map[string]any{
			"operationID": "op-a", "instance": "instance-a", "type": "Backup",
			"parametersSHA256": strings.Repeat("a", 64), "maxAttempts": int64(3),
		},
		"status": map[string]any{
			"phase": "Succeeded", "owner": "worker-a", "attempt": int64(1),
			"observedGeneration": int64(1), "startedAtUnix": int64(90),
			"completedAtUnix": int64(100), "receiptSHA256": strings.Repeat("b", 64),
			"message": "complete",
		},
	}}
}

func envMap(values []string) map[string]string {
	result := make(map[string]string, len(values))
	for _, value := range values {
		key, raw, _ := strings.Cut(value, "=")
		result[key] = raw
	}
	return result
}

func mustCanonicalReceipt(t *testing.T, receipt operationaudit.ArchiveReceipt) []byte {
	t.Helper()
	data, err := json.Marshal(receipt)
	require.NoError(t, err)
	return append(data, '\n')
}
