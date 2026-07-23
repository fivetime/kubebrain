package operationauditbuilder

import (
	"strings"
	"testing"

	"github.com/kubewharf/kubebrain/hack/production/operationaudit"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

func TestFromOperationBindsImmutableSpecAndTerminalStatus(t *testing.T) {
	object := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "dbaas.kubebrain.io/v1alpha1",
		"kind":       "KubeBrainOperation",
		"metadata": map[string]any{
			"name": "backup-1", "namespace": "operations",
			"uid": "uid-1", "generation": int64(1),
		},
		"spec": map[string]any{
			"operationID": "backup-1", "tenant": "tenant-a", "requestedBy": "user-123",
			"instance": "instance-a", "type": "Backup",
			"parametersSHA256": strings.Repeat("a", 64), "maxAttempts": int64(3),
		},
		"status": map[string]any{
			"phase": "Succeeded", "owner": "worker-a", "attempt": int64(1),
			"observedGeneration": int64(1), "startedAtUnix": int64(100),
			"startedAtUnixNano": int64(100_000_000_001), "completedAtUnix": int64(101),
			"receiptSHA256": strings.Repeat("b", 64), "message": "done",
		},
	}}
	artifact, err := FromOperation(object)
	require.NoError(t, err)
	require.Equal(t, "uid-1", artifact.UID)
	require.Equal(t, "tenant-a", artifact.Tenant)
	require.Equal(t, "user-123", artifact.RequestedBy)
	require.Equal(t, strings.Repeat("a", 64), artifact.ParametersSHA256)
	require.Equal(t, strings.Repeat("b", 64), artifact.ReceiptSHA256)

	require.NoError(t, unstructured.SetNestedField(object.Object, "Running", "status", "phase"))
	_, err = FromOperation(object)
	require.ErrorContains(t, err, "incomplete")
}

func TestFromOperationBindsHighRiskApproval(t *testing.T) {
	object := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "dbaas.kubebrain.io/v1alpha1",
		"kind":       "KubeBrainOperation",
		"metadata": map[string]any{
			"name": "destroy-1", "namespace": "operations", "uid": "uid-1",
			"generation": int64(1),
			"annotations": map[string]any{
				operationaudit.ApprovedByAnnotation: operationaudit.ApproverUsername,
				operationaudit.ApprovalIDAnnotation: "change-123",
			},
		},
		"spec": map[string]any{
			"operationID": "destroy-1", "instance": "instance-a", "type": "Destroy",
			"parametersSHA256": strings.Repeat("a", 64), "maxAttempts": int64(3),
		},
		"status": map[string]any{
			"phase": "Failed", "owner": "worker-a", "attempt": int64(1),
			"observedGeneration": int64(1), "startedAtUnix": int64(100),
			"completedAtUnix": int64(101), "receiptSHA256": "", "message": "failed",
		},
	}}
	artifact, err := FromOperation(object)
	require.NoError(t, err)
	require.Equal(t, operationaudit.ApproverUsername, artifact.ApprovedBy)
	require.Equal(t, "change-123", artifact.ApprovalID)

	annotations := object.GetAnnotations()
	delete(annotations, operationaudit.ApprovalIDAnnotation)
	object.SetAnnotations(annotations)
	_, err = FromOperation(object)
	require.ErrorContains(t, err, "approval evidence")
}

func TestFromOperationRejectsUnsafeIdentityMetadata(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mutate func(*unstructured.Unstructured)
	}{
		{
			name: "uid_slash",
			mutate: func(object *unstructured.Unstructured) {
				object.SetUID("uid/1")
			},
		},
		{
			name: "instance_slash",
			mutate: func(object *unstructured.Unstructured) {
				_ = unstructured.SetNestedField(object.Object, "instance/a", "spec", "instance")
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			object := operationForAuditBuilder()
			tc.mutate(object)

			_, err := FromOperation(object)
			require.ErrorContains(t, err, "incomplete")
		})
	}
}

func operationForAuditBuilder() *unstructured.Unstructured {
	return &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "dbaas.kubebrain.io/v1alpha1",
		"kind":       "KubeBrainOperation",
		"metadata": map[string]any{
			"name": "backup-1", "namespace": "operations",
			"uid": "uid-1", "generation": int64(1),
		},
		"spec": map[string]any{
			"operationID": "backup-1", "tenant": "tenant-a", "requestedBy": "user-123",
			"instance": "instance-a", "type": "Backup",
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
