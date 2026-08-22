package operationarchiveverifier

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/kubewharf/kubebrain/hack/production/operationaudit"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
)

func TestProcessorUsesOnlyVersionVerificationBinding(t *testing.T) {
	object := verifierOperation("verified", 100, false, true)
	processor, err := NewProcessor("/executor", "store-a", "audit-bucket", "/audit/", "COMPLIANCE", 24*time.Hour)
	require.NoError(t, err)
	processor.now = func() time.Time { return time.Unix(110, 0) }
	called := 0
	processor.run = func(_ context.Context, executable string, environment []string) error {
		called++
		require.Equal(t, "/executor", executable)
		values := environmentMap(environment)
		require.Equal(t, "audit-version-verify", values["ACTION"])
		require.Equal(t, "audit/tenant-a/uid-verified.json", values["S3_OBJECT_KEY"])
		require.Equal(t, strings.Repeat("c", 64), values["EXPECTED_RECEIPT_SHA256"])
		require.Equal(t, strings.Repeat("d", 64), values["EXPECTED_ARTIFACT_SHA256"])
		require.Equal(t, "version-verified", values["VERSION_ID"])
		require.NotEmpty(t, values["INPUT"])
		require.Empty(t, values["RECEIPT_INPUT"])
		require.Empty(t, values["RECEIPT_OUTPUT"])
		return nil
	}
	require.NoError(t, processor.Process(context.Background(), object))
	require.Equal(t, 1, called)
}

func TestProcessorRejectsEvidenceGapAndPendingArchiveBeforeExecutor(t *testing.T) {
	processor, err := NewProcessor("/executor", "store-a", "audit-bucket", "audit", "COMPLIANCE", 24*time.Hour)
	require.NoError(t, err)
	processor.now = func() time.Time { return time.Unix(110, 0) }
	called := false
	processor.run = func(context.Context, string, []string) error { called = true; return nil }

	missing := verifierOperation("missing", 100, false, false)
	err = processor.Process(context.Background(), missing)
	require.ErrorContains(t, err, "missing complete archive evidence annotations")
	pending := verifierOperation("pending", 100, true, true)
	err = processor.Process(context.Background(), pending)
	require.ErrorContains(t, err, "still has the audit finalizer")
	require.False(t, called)
}

func TestMergeEnvironmentCannotOverrideReadOnlyActionFromParent(t *testing.T) {
	merged := environmentMap(mergeEnvironment(
		[]string{"ACTION=archive", "VERSION_ID=wrong", "KEEP=value"},
		[]string{"ACTION=audit-version-verify", "VERSION_ID=bound"},
	))
	require.Equal(t, "audit-version-verify", merged["ACTION"])
	require.Equal(t, "bound", merged["VERSION_ID"])
	require.Equal(t, "value", merged["KEEP"])
}

func verifierOperation(name string, completed int64, finalizer, evidence bool) *unstructured.Unstructured {
	annotations := map[string]any{}
	if evidence {
		annotations[operationaudit.ReceiptSHAAnnotation] = strings.Repeat("c", 64)
		annotations[operationaudit.ArtifactSHAAnnotation] = strings.Repeat("d", 64)
		annotations[operationaudit.VersionAnnotation] = "version-" + name
	}
	finalizers := []any{}
	if finalizer {
		finalizers = append(finalizers, operationaudit.Finalizer)
	}
	return &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "dbaas.kubebrain.io/v1alpha1", "kind": "KubeBrainOperation",
		"metadata": map[string]any{"name": name, "namespace": "tenant-a", "uid": string(types.UID("uid-" + name)), "generation": int64(1), "annotations": annotations, "finalizers": finalizers},
		"spec":     map[string]any{"operationID": "op-" + name, "instance": "instance-a", "type": "Backup", "parametersSHA256": strings.Repeat("a", 64), "maxAttempts": int64(3)},
		"status":   map[string]any{"phase": "Succeeded", "owner": "worker-a", "attempt": int64(1), "observedGeneration": int64(1), "startedAtUnix": int64(90), "completedAtUnix": completed, "receiptSHA256": strings.Repeat("b", 64), "message": "done"},
	}}
}

func environmentMap(values []string) map[string]string {
	result := map[string]string{}
	for _, value := range values {
		key, item, ok := strings.Cut(value, "=")
		if ok {
			result[key] = item
		}
	}
	return result
}
