package operationauditrelease

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/kubewharf/kubebrain/hack/production/internal/operationauditbuilder"
	"github.com/kubewharf/kubebrain/hack/production/internal/operationqueue"
	"github.com/kubewharf/kubebrain/hack/production/operationaudit"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	clientgotesting "k8s.io/client-go/testing"
)

func TestReleaseVerifiesArchiveAndRemovesOnlyAuditFinalizer(t *testing.T) {
	object := archivedOperation()
	client := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(
		runtime.NewScheme(), map[schema.GroupVersionResource]string{
			operationqueue.Resource: "KubeBrainOperationList",
		}, object,
	)
	artifactPath, receiptPath, receiptSHA := writeReleaseEvidence(t, object)
	result, err := releaseWithExpected(
		context.Background(), client, "operations", "backup-1", artifactPath, receiptPath,
	)
	require.NoError(t, err)
	require.Equal(t, []string{"example.com/other"}, result.GetFinalizers())
	require.Equal(t, receiptSHA, result.GetAnnotations()[operationaudit.ReceiptSHAAnnotation])
	require.NotEmpty(t, result.GetAnnotations()[operationaudit.ArtifactSHAAnnotation])
	require.Equal(t, "version-1", result.GetAnnotations()[operationaudit.VersionAnnotation])

	retried, err := releaseWithExpected(
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
		_, err = releaseWithExpected(context.Background(), client, "operations", "backup-1", artifactPath, receiptPath)
		require.ErrorContains(t, err, "does not match")
	})
	t.Run("receipt archived before completion", func(t *testing.T) {
		object := archivedOperation()
		client := releaseClient(object)
		artifactPath, receiptPath, _ := writeReleaseEvidence(t, object)
		data, err := os.ReadFile(receiptPath)
		require.NoError(t, err)
		var receipt operationaudit.ArchiveReceipt
		require.NoError(t, json.Unmarshal(data, &receipt))
		receipt.ArchivedAtUnix = 100
		writeCanonicalJSON(t, receiptPath, receipt)
		_, err = releaseWithExpected(context.Background(), client, "operations", "backup-1", artifactPath, receiptPath)
		require.ErrorContains(t, err, "predates terminal operation completion")
		current, getErr := client.Resource(operationqueue.Resource).Namespace("operations").
			Get(context.Background(), "backup-1", metav1.GetOptions{})
		require.NoError(t, getErr)
		require.Contains(t, current.GetFinalizers(), operationaudit.Finalizer)
	})
	t.Run("terminal operation", func(t *testing.T) {
		object := archivedOperation()
		artifactPath, receiptPath, _ := writeReleaseEvidence(t, object)
		require.NoError(t, unstructured.SetNestedField(object.Object, "changed", "status", "message"))
		_, err := releaseWithExpected(
			context.Background(), releaseClient(object), "operations", "backup-1",
			artifactPath, receiptPath,
		)
		require.ErrorContains(t, err, "current terminal operation does not match")
	})
}

func TestReleaseWithExpectedReceiptRejectsScopeDrift(t *testing.T) {
	object := archivedOperation()
	client := releaseClient(object)
	artifactPath, receiptPath, _ := writeReleaseEvidence(t, object)

	_, err := ReleaseWithExpectedReceipt(
		context.Background(), client, "operations", "backup-1", artifactPath, receiptPath,
		ExpectedArchiveReceipt{},
	)
	require.ErrorContains(t, err, "scope is incomplete")

	_, err = ReleaseWithExpectedReceipt(
		context.Background(), client, "operations", "backup-1", artifactPath, receiptPath,
		ExpectedArchiveReceipt{
			ObjectStoreID: "store-a", Bucket: "audits", ObjectKey: "instance-a/other.json",
			RetentionMode: "COMPLIANCE", RetainUntilUnix: 200,
		},
	)
	require.ErrorContains(t, err, "expected object")

	result, err := ReleaseWithExpectedReceipt(
		context.Background(), client, "operations", "backup-1", artifactPath, receiptPath,
		ExpectedArchiveReceipt{
			ObjectStoreID: "store-a", Bucket: "audits", ObjectKey: "instance-a/backup-1.json",
			RetentionMode: "COMPLIANCE", RetainUntilUnix: 200,
		},
	)
	require.NoError(t, err)
	require.NotContains(t, result.GetFinalizers(), operationaudit.Finalizer)
}

func TestReleaseRejectsConflictingArchiveAnnotations(t *testing.T) {
	object := archivedOperation()
	object.SetAnnotations(map[string]string{
		operationaudit.ReceiptSHAAnnotation: strings.Repeat("0", 64),
	})
	client := releaseClient(object)
	artifactPath, receiptPath, _ := writeReleaseEvidence(t, object)
	updated := false
	client.PrependReactor("update", operationqueue.Resource.Resource, func(
		clientgotesting.Action,
	) (bool, runtime.Object, error) {
		updated = true
		return false, nil, nil
	})

	result, err := releaseWithExpected(
		context.Background(), client, "operations", "backup-1", artifactPath, receiptPath,
	)
	require.Nil(t, result)
	require.ErrorContains(t, err, "archive annotation")
	require.False(t, updated)
	current, getErr := client.Resource(operationqueue.Resource).Namespace("operations").
		Get(context.Background(), "backup-1", metav1.GetOptions{})
	require.NoError(t, getErr)
	require.Contains(t, current.GetFinalizers(), operationaudit.Finalizer)
	require.Equal(t, strings.Repeat("0", 64),
		current.GetAnnotations()[operationaudit.ReceiptSHAAnnotation])
}

func TestReleaseReconcilesCommittedUpdateAfterLostResponse(t *testing.T) {
	object := archivedOperation()
	client := releaseClient(object)
	artifactPath, receiptPath, receiptSHA := writeReleaseEvidence(t, object)
	client.PrependReactor("update", operationqueue.Resource.Resource, func(
		action clientgotesting.Action,
	) (bool, runtime.Object, error) {
		updated := action.(clientgotesting.UpdateAction).GetObject()
		require.NoError(t, client.Tracker().Update(
			operationqueue.Resource, updated, "operations",
		))
		return true, nil, errors.New("release response lost after commit")
	})

	result, err := releaseWithExpected(
		context.Background(), client, "operations", "backup-1", artifactPath, receiptPath,
	)
	require.NoError(t, err)
	require.NotContains(t, result.GetFinalizers(), operationaudit.Finalizer)
	require.Equal(t, receiptSHA,
		result.GetAnnotations()[operationaudit.ReceiptSHAAnnotation])
}

func TestReleaseReconciliationOutlivesCanceledParent(t *testing.T) {
	object := archivedOperation()
	client := releaseClient(object)
	artifactPath, receiptPath, _ := writeReleaseEvidence(t, object)
	ctx, cancel := context.WithCancel(context.Background())
	client.PrependReactor("update", operationqueue.Resource.Resource, func(
		action clientgotesting.Action,
	) (bool, runtime.Object, error) {
		updated := action.(clientgotesting.UpdateAction).GetObject()
		require.NoError(t, client.Tracker().Update(
			operationqueue.Resource, updated, "operations",
		))
		cancel()
		return true, nil, ctx.Err()
	})

	result, err := releaseWithExpected(
		ctx, client, "operations", "backup-1", artifactPath, receiptPath,
	)
	require.NoError(t, err)
	require.NotContains(t, result.GetFinalizers(), operationaudit.Finalizer)
}

func TestReleaseRejectsReplacementUIDAfterFailedUpdate(t *testing.T) {
	object := archivedOperation()
	client := releaseClient(object)
	artifactPath, receiptPath, _ := writeReleaseEvidence(t, object)
	client.PrependReactor("update", operationqueue.Resource.Resource, func(
		action clientgotesting.Action,
	) (bool, runtime.Object, error) {
		replacement := action.(clientgotesting.UpdateAction).GetObject().(*unstructured.Unstructured).DeepCopy()
		replacement.SetUID(types.UID("uid-replacement"))
		require.NoError(t, client.Tracker().Delete(
			operationqueue.Resource, "operations", "backup-1",
		))
		require.NoError(t, client.Tracker().Create(
			operationqueue.Resource, replacement, "operations",
		))
		return true, nil, errors.New("release response lost after replacement")
	})

	result, err := releaseWithExpected(
		context.Background(), client, "operations", "backup-1", artifactPath, receiptPath,
	)
	require.Nil(t, result)
	require.ErrorContains(t, err, "operation was replaced while releasing audit finalizer")
}

func TestReleaseReportsWriteAndInspectionFailures(t *testing.T) {
	object := archivedOperation()
	client := releaseClient(object)
	artifactPath, receiptPath, _ := writeReleaseEvidence(t, object)
	getCalls := 0
	client.PrependReactor("get", operationqueue.Resource.Resource, func(
		clientgotesting.Action,
	) (bool, runtime.Object, error) {
		getCalls++
		if getCalls > 1 {
			return true, nil, errors.New("release inspection unavailable")
		}
		return false, nil, nil
	})
	client.PrependReactor("update", operationqueue.Resource.Resource, func(
		clientgotesting.Action,
	) (bool, runtime.Object, error) {
		return true, nil, errors.New("release write unavailable")
	})

	result, err := releaseWithExpected(
		context.Background(), client, "operations", "backup-1", artifactPath, receiptPath,
	)
	require.Nil(t, result)
	require.ErrorContains(t, err, "release write unavailable")
	require.ErrorContains(t, err, "release inspection unavailable")
}

func releaseClient(object *unstructured.Unstructured) *dynamicfake.FakeDynamicClient {
	return dynamicfake.NewSimpleDynamicClientWithCustomListKinds(
		runtime.NewScheme(), map[schema.GroupVersionResource]string{
			operationqueue.Resource: "KubeBrainOperationList",
		}, object,
	)
}

func releaseWithExpected(
	ctx context.Context,
	client *dynamicfake.FakeDynamicClient,
	namespace, name, artifactPath, receiptPath string,
) (*unstructured.Unstructured, error) {
	return ReleaseWithExpectedReceipt(
		ctx, client, namespace, name, artifactPath, receiptPath,
		ExpectedArchiveReceipt{
			ObjectStoreID: "store-a", Bucket: "audits", ObjectKey: "instance-a/backup-1.json",
			RetentionMode: "COMPLIANCE", RetainUntilUnix: 200,
		},
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
