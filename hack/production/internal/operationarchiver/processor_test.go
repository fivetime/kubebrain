package operationarchiver

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

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
	var actions []string
	processor.run = func(_ context.Context, executable string, environment []string) error {
		require.Equal(t, "/executor", executable)
		values := envMap(environment)
		actions = append(actions, values["ACTION"])
		if values["ACTION"] == "audit-verify" {
			require.FileExists(t, values["RECEIPT_INPUT"])
			require.Empty(t, values["RECEIPT_OUTPUT"])
			return nil
		}
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
	require.Equal(t, []string{"archive", "audit-verify"}, actions)
}

func TestArchiveProcessorRetainsUnannotatedFinalizerUntilExecutorRecovery(t *testing.T) {
	object := terminalOperation()
	client := fake.NewSimpleDynamicClientWithCustomListKinds(
		runtime.NewScheme(),
		map[schema.GroupVersionResource]string{operationqueue.Resource: "KubeBrainOperationList"},
		object,
	)
	processor, err := NewArchiveProcessor(
		client, "/executor", "store-a", "audit-bucket", "audit", "COMPLIANCE", 24*time.Hour,
	)
	require.NoError(t, err)
	processor.now = func() time.Time { return time.Unix(110, 0) }
	archiveAttempts := 0
	verifyAttempts := 0
	processor.run = func(_ context.Context, _ string, environment []string) error {
		values := envMap(environment)
		if values["ACTION"] == "audit-verify" {
			verifyAttempts++
			if verifyAttempts == 1 {
				return errors.New("remote exact-version retention verification unavailable")
			}
			require.FileExists(t, values["RECEIPT_INPUT"])
			return nil
		}
		archiveAttempts++
		status, err := operationaudit.Inspect(values["INPUT"])
		if err != nil {
			return err
		}
		retainUntil, err := strconv.ParseInt(values["RETAIN_UNTIL_UNIX"], 10, 64)
		if err != nil {
			return err
		}
		receipt := operationaudit.ArchiveReceipt{
			Format: operationaudit.ArchiveReceiptFormat, OperationID: status.Artifact.OperationID,
			OperationUID: status.Artifact.UID, Instance: status.Artifact.Instance,
			OperationType: status.Artifact.Type, Phase: status.Artifact.Phase,
			ExecutionReceiptSHA256: status.Artifact.ReceiptSHA256,
			ObjectStoreID:          "store-a", Bucket: "audit-bucket", ObjectKey: values["S3_OBJECT_KEY"],
			VersionID: "recovered-version", ArtifactSHA256: status.SHA256, ObjectBytes: status.Bytes,
			RetentionMode: "COMPLIANCE", RetainUntilUnix: retainUntil,
			RemoteVerified: true, ArchivedAtUnix: 110,
		}
		return os.WriteFile(values["RECEIPT_OUTPUT"], mustCanonicalReceipt(t, receipt), 0o600)
	}

	err = processor.Process(context.Background(), object.DeepCopy())
	require.ErrorContains(t, err, "retention verification unavailable")
	failed, err := client.Resource(operationqueue.Resource).Namespace("tenant-a").
		Get(context.Background(), "operation-a", metav1.GetOptions{})
	require.NoError(t, err)
	require.Equal(t, []string{operationaudit.Finalizer}, failed.GetFinalizers())
	require.NotContains(t, failed.GetAnnotations(), operationaudit.ReceiptSHAAnnotation)
	require.NotContains(t, failed.GetAnnotations(), operationaudit.ArtifactSHAAnnotation)
	require.NotContains(t, failed.GetAnnotations(), operationaudit.VersionAnnotation)

	require.NoError(t, processor.Process(context.Background(), failed.DeepCopy()))
	recovered, err := client.Resource(operationqueue.Resource).Namespace("tenant-a").
		Get(context.Background(), "operation-a", metav1.GetOptions{})
	require.NoError(t, err)
	require.NotContains(t, recovered.GetFinalizers(), operationaudit.Finalizer)
	require.Equal(t, "recovered-version", recovered.GetAnnotations()[operationaudit.VersionAnnotation])
	require.Equal(t, 2, archiveAttempts)
	require.Equal(t, 2, verifyAttempts)
}

func TestConcurrentArchiveProcessorsConvergeWithOneCommittedFinalizerUpdate(t *testing.T) {
	object := terminalOperation()
	object.SetResourceVersion("1")
	client := fake.NewSimpleDynamicClientWithCustomListKinds(
		runtime.NewScheme(),
		map[schema.GroupVersionResource]string{operationqueue.Resource: "KubeBrainOperationList"},
		object,
	)
	var updateMu sync.Mutex
	updateAttempts := 0
	committedUpdates := 0
	client.PrependReactor("update", "kubebrainoperations", func(action k8stesting.Action) (bool, runtime.Object, error) {
		update := action.(k8stesting.UpdateAction)
		candidate := update.GetObject().(*unstructured.Unstructured).DeepCopy()
		updateMu.Lock()
		defer updateMu.Unlock()
		updateAttempts++
		if updateAttempts > 1 {
			return true, nil, apierrors.NewConflict(
				operationqueue.Resource.GroupResource(), candidate.GetName(), errors.New("concurrent release"),
			)
		}
		candidate.SetResourceVersion("2")
		if err := client.Tracker().Update(operationqueue.Resource, candidate, candidate.GetNamespace()); err != nil {
			return true, nil, err
		}
		committedUpdates++
		return true, candidate.DeepCopy(), nil
	})

	processors := make([]*ArchiveProcessor, 2)
	ready := make(chan struct{}, 2)
	release := make(chan struct{})
	for i := range processors {
		processor, err := NewArchiveProcessor(
			client, "/executor", "store-a", "audit-bucket", "audit", "COMPLIANCE", 24*time.Hour,
		)
		require.NoError(t, err)
		processor.now = func() time.Time { return time.Unix(110, 0) }
		processor.run = func(_ context.Context, _ string, environment []string) error {
			values := envMap(environment)
			if values["ACTION"] == "audit-verify" {
				require.FileExists(t, values["RECEIPT_INPUT"])
				return nil
			}
			status, err := operationaudit.Inspect(values["INPUT"])
			if err != nil {
				return err
			}
			retainUntil, err := strconv.ParseInt(values["RETAIN_UNTIL_UNIX"], 10, 64)
			if err != nil {
				return err
			}
			receipt := operationaudit.ArchiveReceipt{
				Format: operationaudit.ArchiveReceiptFormat, OperationID: status.Artifact.OperationID,
				OperationUID: status.Artifact.UID, Instance: status.Artifact.Instance,
				OperationType: status.Artifact.Type, Phase: status.Artifact.Phase,
				ExecutionReceiptSHA256: status.Artifact.ReceiptSHA256,
				ObjectStoreID:          "store-a", Bucket: "audit-bucket", ObjectKey: values["S3_OBJECT_KEY"],
				VersionID: "shared-version", ArtifactSHA256: status.SHA256, ObjectBytes: status.Bytes,
				RetentionMode: "COMPLIANCE", RetainUntilUnix: retainUntil,
				RemoteVerified: true, ArchivedAtUnix: 110,
			}
			if err := os.WriteFile(values["RECEIPT_OUTPUT"], mustCanonicalReceipt(t, receipt), 0o600); err != nil {
				return err
			}
			ready <- struct{}{}
			<-release
			return nil
		}
		processors[i] = processor
	}

	errs := make(chan error, 2)
	for _, processor := range processors {
		go func(processor *ArchiveProcessor) { errs <- processor.Process(context.Background(), object.DeepCopy()) }(processor)
	}
	<-ready
	<-ready
	close(release)
	require.NoError(t, <-errs)
	require.NoError(t, <-errs)
	require.Equal(t, 1, committedUpdates)
	require.GreaterOrEqual(t, updateAttempts, 1)
	require.LessOrEqual(t, updateAttempts, 2)
	updated, err := client.Resource(operationqueue.Resource).Namespace("tenant-a").Get(context.Background(), "operation-a", metav1.GetOptions{})
	require.NoError(t, err)
	require.NotContains(t, updated.GetFinalizers(), operationaudit.Finalizer)
	require.Equal(t, "shared-version", updated.GetAnnotations()[operationaudit.VersionAnnotation])
}

func TestArchiveProcessorRejectsUnsafeObjectPrefix(t *testing.T) {
	client := fake.NewSimpleDynamicClient(runtime.NewScheme())
	processor, err := NewArchiveProcessor(
		client, "/executor", "store-a", "bucket-a", "/audit/", "COMPLIANCE", time.Hour,
	)
	require.NoError(t, err)
	require.Equal(t, "audit", processor.prefix)

	for _, tc := range []struct {
		name   string
		prefix string
	}{
		{name: "dot", prefix: "."},
		{name: "dot_dot", prefix: ".."},
		{name: "parent_prefix", prefix: "../audit"},
		{name: "parent_segment", prefix: "audit/../other"},
		{name: "duplicate_separator", prefix: "audit//tenant"},
		{name: "ascii_space", prefix: "audit key"},
		{name: "ascii_tab", prefix: "audit\tkey"},
		{name: "unicode_space", prefix: "audit\u00a0key"},
		{name: "control_byte", prefix: "audit\x00key"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			processor, err := NewArchiveProcessor(
				client, "/executor", "store-a", "bucket-a", tc.prefix, "COMPLIANCE", time.Hour,
			)
			require.Nil(t, processor)
			require.ErrorContains(t, err, "normalized relative key prefix")
		})
	}
}

func TestArchiveProcessorRejectsUnsafeObjectScope(t *testing.T) {
	client := fake.NewSimpleDynamicClient(runtime.NewScheme())
	for _, tc := range []struct {
		name          string
		objectStoreID string
		bucket        string
	}{
		{name: "object_store_id_leading_space", objectStoreID: " store-a", bucket: "bucket-a"},
		{name: "object_store_id_control_byte", objectStoreID: "store-a\x00", bucket: "bucket-a"},
		{name: "bucket_internal_tab", objectStoreID: "store-a", bucket: "bucket\ta"},
		{name: "bucket_unicode_space", objectStoreID: "store-a", bucket: "bucket\u00a0a"},
		{name: "bucket_invalid_utf8", objectStoreID: "store-a", bucket: string([]byte{'b', 0xff})},
	} {
		t.Run(tc.name, func(t *testing.T) {
			processor, err := NewArchiveProcessor(
				client, "/executor", tc.objectStoreID, tc.bucket, "audit", "COMPLIANCE", time.Hour,
			)
			require.Nil(t, processor)
			require.ErrorContains(t, err, "object scope")
		})
	}
}

func TestArchiveProcessorRejectsUnsafeOperationIdentityBeforeExecutor(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mutate func(*unstructured.Unstructured)
	}{
		{
			name: "namespace_parent_segment",
			mutate: func(object *unstructured.Unstructured) {
				object.SetNamespace("../tenant-a")
			},
		},
		{
			name: "name_slash",
			mutate: func(object *unstructured.Unstructured) {
				object.SetName("operation/a")
			},
		},
		{
			name: "name_dot",
			mutate: func(object *unstructured.Unstructured) {
				object.SetName(".")
			},
		},
		{
			name: "uid_slash",
			mutate: func(object *unstructured.Unstructured) {
				object.SetUID(types.UID("uid/a"))
			},
		},
		{
			name: "uid_control_byte",
			mutate: func(object *unstructured.Unstructured) {
				object.SetUID(types.UID("uid-a\x00"))
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			object := terminalOperation()
			tc.mutate(object)
			processor, err := NewArchiveProcessor(
				fake.NewSimpleDynamicClient(runtime.NewScheme(), object),
				"/executor", "store-a", "bucket", "audit", "COMPLIANCE", 24*time.Hour,
			)
			require.NoError(t, err)
			processor.now = func() time.Time { return time.Unix(110, 0) }
			called := false
			processor.run = func(context.Context, string, []string) error {
				called = true
				return nil
			}

			err = processor.Process(context.Background(), object)
			require.ErrorContains(t, err, "incomplete")
			require.False(t, called)
		})
	}
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

func TestArchiveProcessorRejectsReceiptRetentionDriftBeforeRelease(t *testing.T) {
	object := terminalOperation()
	client := fake.NewSimpleDynamicClientWithCustomListKinds(
		runtime.NewScheme(),
		map[schema.GroupVersionResource]string{operationqueue.Resource: "KubeBrainOperationList"},
		object,
	)
	processor, err := NewArchiveProcessor(
		client, "/executor", "store-a", "audit-bucket", "audit", "COMPLIANCE", 24*time.Hour,
	)
	require.NoError(t, err)
	processor.now = func() time.Time { return time.Unix(110, 0) }
	processor.run = func(_ context.Context, _ string, environment []string) error {
		values := envMap(environment)
		if values["ACTION"] == "audit-verify" {
			return nil
		}
		status, err := operationaudit.Inspect(values["INPUT"])
		require.NoError(t, err)
		retainUntil, err := strconv.ParseInt(values["RETAIN_UNTIL_UNIX"], 10, 64)
		require.NoError(t, err)
		receipt := operationaudit.ArchiveReceipt{
			Format:      operationaudit.ArchiveReceiptFormat,
			OperationID: status.Artifact.OperationID, OperationUID: status.Artifact.UID,
			Instance: status.Artifact.Instance, OperationType: status.Artifact.Type,
			Phase: status.Artifact.Phase, ExecutionReceiptSHA256: status.Artifact.ReceiptSHA256,
			ObjectStoreID: "store-a", Bucket: "audit-bucket", ObjectKey: values["S3_OBJECT_KEY"],
			VersionID: "version-a", ArtifactSHA256: status.SHA256, ObjectBytes: status.Bytes,
			RetentionMode: "GOVERNANCE", RetainUntilUnix: retainUntil,
			RemoteVerified: true, ArchivedAtUnix: 110,
		}
		return os.WriteFile(values["RECEIPT_OUTPUT"], mustCanonicalReceipt(t, receipt), 0o600)
	}

	err = processor.Process(context.Background(), object)
	require.ErrorContains(t, err, "expected retention")
	updated, err := client.Resource(operationqueue.Resource).Namespace("tenant-a").
		Get(context.Background(), "operation-a", metav1.GetOptions{})
	require.NoError(t, err)
	require.Contains(t, updated.GetFinalizers(), operationaudit.Finalizer)
}

func TestArchiveProcessorRejectsReceiptObjectDriftBeforeRelease(t *testing.T) {
	object := terminalOperation()
	client := fake.NewSimpleDynamicClientWithCustomListKinds(
		runtime.NewScheme(),
		map[schema.GroupVersionResource]string{operationqueue.Resource: "KubeBrainOperationList"},
		object,
	)
	processor, err := NewArchiveProcessor(
		client, "/executor", "store-a", "audit-bucket", "audit", "COMPLIANCE", 24*time.Hour,
	)
	require.NoError(t, err)
	processor.now = func() time.Time { return time.Unix(110, 0) }
	processor.run = func(_ context.Context, _ string, environment []string) error {
		values := envMap(environment)
		if values["ACTION"] == "audit-verify" {
			return nil
		}
		status, err := operationaudit.Inspect(values["INPUT"])
		require.NoError(t, err)
		retainUntil, err := strconv.ParseInt(values["RETAIN_UNTIL_UNIX"], 10, 64)
		require.NoError(t, err)
		receipt := operationaudit.ArchiveReceipt{
			Format:      operationaudit.ArchiveReceiptFormat,
			OperationID: status.Artifact.OperationID, OperationUID: status.Artifact.UID,
			Instance: status.Artifact.Instance, OperationType: status.Artifact.Type,
			Phase: status.Artifact.Phase, ExecutionReceiptSHA256: status.Artifact.ReceiptSHA256,
			ObjectStoreID: "store-a", Bucket: "audit-bucket", ObjectKey: "audit/tenant-a/other.json",
			VersionID: "version-a", ArtifactSHA256: status.SHA256, ObjectBytes: status.Bytes,
			RetentionMode: "COMPLIANCE", RetainUntilUnix: retainUntil,
			RemoteVerified: true, ArchivedAtUnix: 110,
		}
		return os.WriteFile(values["RECEIPT_OUTPUT"], mustCanonicalReceipt(t, receipt), 0o600)
	}

	err = processor.Process(context.Background(), object)
	require.ErrorContains(t, err, "expected object")
	updated, err := client.Resource(operationqueue.Resource).Namespace("tenant-a").
		Get(context.Background(), "operation-a", metav1.GetOptions{})
	require.NoError(t, err)
	require.Contains(t, updated.GetFinalizers(), operationaudit.Finalizer)
}

func TestArchiveProcessorPropagatesReconcileCancellationToExecutor(t *testing.T) {
	object := terminalOperation()
	client := fake.NewSimpleDynamicClient(runtime.NewScheme(), object)
	processor, err := NewArchiveProcessor(
		client, "/executor", "store-a", "bucket", "audit", "COMPLIANCE", 24*time.Hour,
	)
	require.NoError(t, err)
	processor.now = func() time.Time { return time.Unix(110, 0) }
	processor.run = func(ctx context.Context, _ string, _ []string) error {
		<-ctx.Done()
		return ctx.Err()
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	err = processor.Process(ctx, object)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	updated, getErr := client.Resource(operationqueue.Resource).Namespace("tenant-a").
		Get(context.Background(), "operation-a", metav1.GetOptions{})
	require.NoError(t, getErr)
	require.Contains(t, updated.GetFinalizers(), operationaudit.Finalizer)
}

func TestArchiveCommandCancellationKillsDescendantProcessGroup(t *testing.T) {
	processor := &ArchiveProcessor{}
	executable := filepath.Join(t.TempDir(), "blocking-executor")
	require.NoError(t, os.WriteFile(
		executable, []byte("#!/bin/sh\nsleep 60\n"), 0o700,
	))
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	start := time.Now()
	err := processor.runCommand(ctx, executable, nil)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.Less(t, time.Since(start), 2*time.Second)
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
