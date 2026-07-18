package operationauditrelease

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/kubewharf/kubebrain/hack/production/internal/operationauditbuilder"
	"github.com/kubewharf/kubebrain/hack/production/internal/operationqueue"
	"github.com/kubewharf/kubebrain/hack/production/operationaudit"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	dynamicfake "k8s.io/client-go/dynamic/fake"
)

func TestReleaseVerifiesArchiveAndRemovesOnlyAuditFinalizer(t *testing.T) {
	object := archivedOperation()
	client := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(
		runtime.NewScheme(), map[schema.GroupVersionResource]string{
			operationqueue.Resource: "KubeBrainOperationList",
		}, object,
	)
	artifactPath, receiptPath, receiptSHA := writeReleaseEvidence(t, object)
	result, err := Release(
		context.Background(), client, "operations", "backup-1", artifactPath, receiptPath,
	)
	require.NoError(t, err)
	require.Equal(t, []string{"example.com/other"}, result.GetFinalizers())
	require.Equal(t, receiptSHA, result.GetAnnotations()[operationaudit.ReceiptSHAAnnotation])
	require.NotEmpty(t, result.GetAnnotations()[operationaudit.ArtifactSHAAnnotation])
	require.Equal(t, "version-1", result.GetAnnotations()[operationaudit.VersionAnnotation])

	retried, err := Release(
		context.Background(), client, "operations", "backup-1", artifactPath, receiptPath,
	)
	require.NoError(t, err)
	require.Equal(t, result.GetResourceVersion(), retried.GetResourceVersion())
}

func TestReleaseRejectsReceiptAndCurrentOperationDrift(t *testing.T) {
	t.Run("receipt UID", func(t *testing.T) {
		object := archivedOperation()
		client := releaseClient(object)
		artifactPath, receiptPath, _ := writeReleaseEvidence(t, object)
		data, err := os.ReadFile(receiptPath)
		require.NoError(t, err)
		var receipt operationaudit.ArchiveReceipt
		require.NoError(t, json.Unmarshal(data, &receipt))
		receipt.OperationUID = "other"
		writeCanonicalJSON(t, receiptPath, receipt)
		_, err = Release(context.Background(), client, "operations", "backup-1", artifactPath, receiptPath)
		require.ErrorContains(t, err, "does not match")
	})
	t.Run("terminal operation", func(t *testing.T) {
		object := archivedOperation()
		artifactPath, receiptPath, _ := writeReleaseEvidence(t, object)
		require.NoError(t, unstructured.SetNestedField(object.Object, "changed", "status", "message"))
		_, err := Release(
			context.Background(), releaseClient(object), "operations", "backup-1",
			artifactPath, receiptPath,
		)
		require.ErrorContains(t, err, "current terminal operation does not match")
	})
}

func releaseClient(object *unstructured.Unstructured) *dynamicfake.FakeDynamicClient {
	return dynamicfake.NewSimpleDynamicClientWithCustomListKinds(
		runtime.NewScheme(), map[schema.GroupVersionResource]string{
			operationqueue.Resource: "KubeBrainOperationList",
		}, object,
	)
}

func archivedOperation() *unstructured.Unstructured {
	return &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "dbaas.kubebrain.io/v1alpha1", "kind": "KubeBrainOperation",
		"metadata": map[string]any{
			"name": "backup-1", "namespace": "operations", "uid": "uid-1",
			"generation": int64(1),
			"finalizers": []any{operationaudit.Finalizer, "example.com/other"},
		},
		"spec": map[string]any{
			"operationID": "backup-1", "instance": "instance-a", "type": "Backup",
			"parametersSHA256": strings.Repeat("a", 64), "maxAttempts": int64(3),
		},
		"status": map[string]any{
			"phase": "Succeeded", "owner": "worker-a", "attempt": int64(1),
			"observedGeneration": int64(1), "startedAtUnix": int64(100),
			"startedAtUnixNano": int64(100_000_000_001), "completedAtUnix": int64(101),
			"receiptSHA256": strings.Repeat("b", 64), "message": "done",
		},
	}}
}

func writeReleaseEvidence(
	t *testing.T,
	object *unstructured.Unstructured,
) (string, string, string) {
	t.Helper()
	dir := t.TempDir()
	artifact, err := operationauditbuilder.FromOperation(object)
	require.NoError(t, err)
	artifactPath := filepath.Join(dir, "artifact.json")
	require.NoError(t, operationaudit.WriteAtomic(artifactPath, artifact))
	status, err := operationaudit.Inspect(artifactPath)
	require.NoError(t, err)
	receiptPath := filepath.Join(dir, "receipt.json")
	writeCanonicalJSON(t, receiptPath, operationaudit.ArchiveReceipt{
		Format:      operationaudit.ArchiveReceiptFormat,
		OperationID: artifact.OperationID, OperationUID: artifact.UID,
		Instance: artifact.Instance, OperationType: artifact.Type, Phase: artifact.Phase,
		ExecutionReceiptSHA256: artifact.ReceiptSHA256, ObjectStoreID: "store-a",
		Bucket: "audits", ObjectKey: "instance-a/backup-1.json", VersionID: "version-1",
		ArtifactSHA256: status.SHA256, ObjectBytes: status.Bytes,
		RetentionMode: "COMPLIANCE", RetainUntilUnix: 200,
		RemoteVerified: true, ArchivedAtUnix: 150,
	})
	_, receiptSHA, err := operationaudit.InspectArchiveReceipt(receiptPath)
	require.NoError(t, err)
	return artifactPath, receiptPath, receiptSHA
}

func writeCanonicalJSON(t *testing.T, path string, value any) {
	t.Helper()
	data, err := json.Marshal(value)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(path, append(data, '\n'), 0o600))
}
