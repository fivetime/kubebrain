package operationqueue

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/kubewharf/kubebrain/hack/production/operationaudit"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	dynamicfake "k8s.io/client-go/dynamic/fake"
)

func TestQueueLifecycleAndExpiredLeaseFencing(t *testing.T) {
	now := time.Unix(1_000, 0).UTC()
	queue := newFakeQueue().WithClock(func() time.Time { return now })
	ctx := context.Background()
	spec := validSpec()

	first, err := queue.Submit(ctx, "restore-1", spec)
	require.NoError(t, err)
	require.Equal(t, []string{operationaudit.Finalizer}, first.GetFinalizers())
	second, err := queue.Submit(ctx, "restore-1", spec)
	require.NoError(t, err)
	require.Equal(t, first.GetName(), second.GetName())

	claimA, err := queue.Claim(ctx, "worker-a", "PostRestoreAudit", 30*time.Second)
	require.NoError(t, err)
	require.Equal(t, int64(1), claimA.Attempt)
	require.Equal(t, "test", claimA.Namespace)
	require.Equal(t, "tenant-a", claimA.Tenant)
	require.Equal(t, "user-123", claimA.RequestedBy)
	require.Equal(t, int64(1_030), claimA.LeaseUntilUnix)

	now = now.Add(10 * time.Second)
	heartbeat, err := queue.Heartbeat(ctx, "restore-1", "worker-a", 1, 30*time.Second)
	require.NoError(t, err)
	require.Equal(t, int64(1_040), heartbeat.LeaseUntilUnix)

	now = time.Unix(1_041, 0).UTC()
	claimB, err := queue.Claim(ctx, "worker-b", "PostRestoreAudit", 30*time.Second)
	require.NoError(t, err)
	require.Equal(t, int64(2), claimB.Attempt)
	require.Equal(t, "worker-b", claimB.Owner)

	_, err = queue.Heartbeat(ctx, "restore-1", "worker-a", 1, 30*time.Second)
	require.ErrorIs(t, err, ErrFenced)
	_, err = queue.Finish(ctx, "restore-1", "worker-a", 1, true, strings.Repeat("a", 64), "")
	require.ErrorIs(t, err, ErrFenced)

	completed, err := queue.Finish(
		ctx, "restore-1", "worker-b", 2, true, strings.Repeat("b", 64), "verified",
	)
	require.NoError(t, err)
	phase, _, err := unstructured.NestedString(completed.Object, "status", "phase")
	require.NoError(t, err)
	require.Equal(t, PhaseSucceeded, phase)
	retried, err := queue.Finish(
		ctx, "restore-1", "worker-b", 2, true, strings.Repeat("b", 64), "verified",
	)
	require.NoError(t, err)
	require.Equal(t, completed.GetResourceVersion(), retried.GetResourceVersion())
	_, err = queue.Finish(ctx, "restore-1", "worker-b", 2, true, strings.Repeat("c", 64), "verified")
	require.ErrorIs(t, err, ErrTerminal)
	_, err = queue.Claim(ctx, "worker-c", "PostRestoreAudit", 30*time.Second)
	require.ErrorIs(t, err, ErrNoOperation)
}

func TestClaimAcrossNamespacesPrioritizesLeastRecentlyServedQueue(t *testing.T) {
	client := fakeQueueClient()
	ctx := context.Background()
	for _, namespace := range []string{"tenant-a", "tenant-b"} {
		_, err := New(client, namespace).Submit(ctx, "backup-1", validSpec())
		require.NoError(t, err)
	}

	first, err := ClaimAcrossNamespaces(
		ctx, client, []string{"tenant-b", "tenant-a"}, "worker-a", "PostRestoreAudit", time.Minute,
	)
	require.NoError(t, err)
	require.Equal(t, "tenant-a", first.Namespace,
		"equal service watermarks use canonical namespace order")

	second, err := ClaimAcrossNamespaces(
		ctx, client, []string{"tenant-b", "tenant-a"}, "worker-a", "PostRestoreAudit", time.Minute,
	)
	require.NoError(t, err)
	require.Equal(t, "tenant-b", second.Namespace,
		"an unserved namespace must win over a namespace that just started work")
}

func TestQueueRejectsSpecDriftAndInvalidCompletion(t *testing.T) {
	queue := newFakeQueue()
	ctx := context.Background()
	spec := validSpec()
	_, err := queue.Submit(ctx, "backup-1", spec)
	require.NoError(t, err)

	drift := spec
	drift.ParametersSHA256 = strings.Repeat("f", 64)
	_, err = queue.Submit(ctx, "backup-1", drift)
	require.ErrorContains(t, err, "different immutable spec")
	tenantDrift := spec
	tenantDrift.Tenant = "tenant-b"
	_, err = queue.Submit(ctx, "backup-1", tenantDrift)
	require.ErrorContains(t, err, "different immutable spec")
	requesterDrift := spec
	requesterDrift.RequestedBy = "user-456"
	_, err = queue.Submit(ctx, "backup-1", requesterDrift)
	require.ErrorContains(t, err, "different immutable spec")

	invalidTenant := spec
	invalidTenant.Tenant = "Invalid_Tenant"
	_, err = queue.Submit(ctx, "invalid-tenant", invalidTenant)
	require.ErrorContains(t, err, "invalid operation tenant")

	claim, err := queue.Claim(ctx, "worker-a", "", time.Minute)
	require.NoError(t, err)
	_, err = queue.Finish(ctx, claim.Name, claim.Owner, claim.Attempt, true, "", "")
	require.ErrorContains(t, err, "requires a receipt")
}

func TestQueueLoadsDigestBoundImmutableParameters(t *testing.T) {
	queue := newFakeQueue()
	ctx := context.Background()
	parameters := []byte("{\"backup_id\":\"backup-1\"}\n")
	spec := validSpec()
	spec.ParametersSHA256 = fmt.Sprintf("%x", sha256.Sum256(parameters))
	spec.ParametersSecret = "backup-1-parameters"
	spec.ParametersKey = "parameters.json"
	_, err := queue.secrets.Create(ctx, &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "v1", "kind": "Secret",
		"metadata":  map[string]any{"name": spec.ParametersSecret},
		"immutable": true,
		"data": map[string]any{
			spec.ParametersKey: base64.StdEncoding.EncodeToString(parameters),
		},
	}}, metav1.CreateOptions{})
	require.NoError(t, err)
	_, err = queue.Submit(ctx, "backup-1", spec)
	require.NoError(t, err)

	actual, err := queue.Parameters(ctx, "backup-1")
	require.NoError(t, err)
	require.Equal(t, parameters, actual)

	secret, err := queue.secrets.Get(ctx, spec.ParametersSecret, metav1.GetOptions{})
	require.NoError(t, err)
	secret.Object["immutable"] = false
	_, err = queue.secrets.Update(ctx, secret, metav1.UpdateOptions{})
	require.NoError(t, err)
	_, err = queue.Parameters(ctx, "backup-1")
	require.ErrorContains(t, err, "must be immutable")
}

func TestQueueOnlyLoadsParametersForCurrentTypeBoundWorker(t *testing.T) {
	now := time.Unix(1_000, 0).UTC()
	queue := newFakeQueue().WithClock(func() time.Time { return now })
	ctx := context.Background()
	parameters := []byte("{\"operation\":\"audit\"}\n")
	spec := validSpec()
	spec.ParametersSHA256 = fmt.Sprintf("%x", sha256.Sum256(parameters))
	spec.ParametersSecret = "audit-parameters"
	spec.ParametersKey = "parameters.json"
	_, err := queue.secrets.Create(ctx, &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "v1", "kind": "Secret",
		"metadata":  map[string]any{"name": spec.ParametersSecret},
		"immutable": true,
		"data": map[string]any{
			spec.ParametersKey: base64.StdEncoding.EncodeToString(parameters),
		},
	}}, metav1.CreateOptions{})
	require.NoError(t, err)
	_, err = queue.Submit(ctx, "audit-1", spec)
	require.NoError(t, err)
	claim, err := queue.Claim(ctx, "audit-worker", spec.Type, time.Minute)
	require.NoError(t, err)

	actual, err := queue.ParametersForWorker(
		ctx, claim.Name, spec.Type, claim.Owner, claim.Attempt,
	)
	require.NoError(t, err)
	require.Equal(t, parameters, actual)
	_, err = queue.ParametersForWorker(ctx, claim.Name, "Backup", claim.Owner, claim.Attempt)
	require.ErrorContains(t, err, "type does not match")
	_, err = queue.ParametersForWorker(ctx, claim.Name, spec.Type, "other-worker", claim.Attempt)
	require.ErrorIs(t, err, ErrFenced)
	_, err = queue.ParametersForWorker(ctx, claim.Name, spec.Type, claim.Owner, claim.Attempt+1)
	require.ErrorIs(t, err, ErrFenced)

	now = now.Add(61 * time.Second)
	_, err = queue.ParametersForWorker(ctx, claim.Name, spec.Type, claim.Owner, claim.Attempt)
	require.ErrorIs(t, err, ErrFenced)
}

func TestQueueStopsAfterMaximumAttempts(t *testing.T) {
	now := time.Unix(1_000, 0).UTC()
	queue := newFakeQueue().WithClock(func() time.Time { return now })
	ctx := context.Background()
	spec := validSpec()
	spec.MaxAttempts = 2
	_, err := queue.Submit(ctx, "destroy-1", spec)
	require.NoError(t, err)
	_, err = queue.Claim(ctx, "worker-a", "", time.Second)
	require.NoError(t, err)
	now = now.Add(2 * time.Second)
	_, err = queue.Claim(ctx, "worker-b", "", time.Second)
	require.NoError(t, err)
	now = now.Add(2 * time.Second)
	_, err = queue.Claim(ctx, "worker-c", "", time.Second)
	require.True(t, errors.Is(err, ErrNoOperation))
	exhausted, err := queue.Get(ctx, "destroy-1")
	require.NoError(t, err)
	phase, _, err := unstructured.NestedString(exhausted.Object, "status", "phase")
	require.NoError(t, err)
	require.Equal(t, PhaseFailed, phase)
}

func TestQueueRequeueConsumesAttemptAndFiltersType(t *testing.T) {
	queue := newFakeQueue()
	ctx := context.Background()
	backup := validSpec()
	backup.OperationID = "backup-1"
	backup.Type = "Backup"
	_, err := queue.Submit(ctx, "backup-1", backup)
	require.NoError(t, err)
	_, err = queue.Submit(ctx, "audit-1", validSpec())
	require.NoError(t, err)

	claim, err := queue.Claim(ctx, "worker-a", "PostRestoreAudit", time.Minute)
	require.NoError(t, err)
	require.Equal(t, "audit-1", claim.Name)
	_, err = queue.Requeue(ctx, claim.Name, claim.Owner, claim.Attempt, "transient")
	require.NoError(t, err)
	claim, err = queue.Claim(ctx, "worker-b", "PostRestoreAudit", time.Minute)
	require.NoError(t, err)
	require.Equal(t, int64(2), claim.Attempt)
	_, err = queue.Requeue(ctx, claim.Name, "worker-a", 1, "stale")
	require.ErrorIs(t, err, ErrFenced)
}

func TestQueueRequiresApprovalForHighRiskOperations(t *testing.T) {
	for _, operationType := range []string{
		"RestoreCutover", "CertificateRotation", "Destroy", "BackupDeletion",
	} {
		t.Run(operationType, func(t *testing.T) {
			queue := newFakeQueue()
			ctx := context.Background()
			spec := validSpec()
			spec.Type = operationType
			object, err := queue.Submit(ctx, "high-risk", spec)
			require.NoError(t, err)

			_, err = queue.Claim(ctx, "worker-a", operationType, time.Minute)
			require.ErrorIs(t, err, ErrNoOperation)
			pending, err := queue.Get(ctx, object.GetName())
			require.NoError(t, err)
			attempt, _, err := unstructured.NestedInt64(pending.Object, "status", "attempt")
			require.NoError(t, err)
			require.Zero(t, attempt)

			approved := pending.DeepCopy()
			approved.SetAnnotations(map[string]string{
				operationaudit.ApprovedByAnnotation: operationaudit.ApproverUsername,
				operationaudit.ApprovalIDAnnotation: "approval-1",
			})
			_, err = queue.resource.Update(ctx, approved, metav1.UpdateOptions{})
			require.NoError(t, err)

			claim, err := queue.Claim(ctx, "worker-a", operationType, time.Minute)
			require.NoError(t, err)
			require.Equal(t, operationType, claim.Type)
			require.Equal(t, int64(1), claim.Attempt)
		})
	}
}

func TestQueueDoesNotTreatForgedApprovalAsApproved(t *testing.T) {
	queue := newFakeQueue()
	ctx := context.Background()
	spec := validSpec()
	spec.Type = "Destroy"
	object, err := queue.Submit(ctx, "destroy", spec)
	require.NoError(t, err)
	object.SetAnnotations(map[string]string{
		operationaudit.ApprovedByAnnotation: "system:serviceaccount:test:approver",
		operationaudit.ApprovalIDAnnotation: "approval-1",
	})
	_, err = queue.resource.Update(ctx, object, metav1.UpdateOptions{})
	require.NoError(t, err)
	_, err = queue.Claim(ctx, "worker-a", "Destroy", time.Minute)
	require.ErrorIs(t, err, ErrNoOperation)
}

func TestQueueApprovalIsPendingOnlyAndIdempotent(t *testing.T) {
	queue := newFakeQueue()
	ctx := context.Background()
	spec := validSpec()
	spec.Type = "Destroy"
	_, err := queue.Submit(ctx, "destroy", spec)
	require.NoError(t, err)

	approved, err := queue.Approve(ctx, "destroy", operationaudit.ApproverUsername, "change-123")
	require.NoError(t, err)
	require.Equal(t, operationaudit.ApproverUsername,
		approved.GetAnnotations()[operationaudit.ApprovedByAnnotation])
	require.Equal(t, "change-123", approved.GetAnnotations()[operationaudit.ApprovalIDAnnotation])
	retried, err := queue.Approve(ctx, "destroy", operationaudit.ApproverUsername, "change-123")
	require.NoError(t, err)
	require.Equal(t, approved.GetResourceVersion(), retried.GetResourceVersion())
	_, err = queue.Approve(ctx, "destroy", operationaudit.ApproverUsername, "other")
	require.ErrorContains(t, err, "different immutable approval")

	claim, err := queue.Claim(ctx, "worker-a", "Destroy", time.Minute)
	require.NoError(t, err)
	_, err = queue.Approve(ctx, claim.Name, operationaudit.ApproverUsername, "change-123")
	require.ErrorContains(t, err, "pending")
}

func TestQueueRejectsApprovalForLowRiskOrInvalidDecision(t *testing.T) {
	queue := newFakeQueue()
	ctx := context.Background()
	_, err := queue.Submit(ctx, "audit", validSpec())
	require.NoError(t, err)
	_, err = queue.Approve(ctx, "audit", operationaudit.ApproverUsername, "change-123")
	require.ErrorContains(t, err, "does not require approval")

	spec := validSpec()
	spec.Type = "Destroy"
	_, err = queue.Submit(ctx, "destroy", spec)
	require.NoError(t, err)
	_, err = queue.Approve(ctx, "destroy", operationaudit.ApproverUsername, "INVALID_ID")
	require.ErrorContains(t, err, "DNS-compatible")
}

func TestQueueRotatesAcrossInstancesAndSerializesEachInstance(t *testing.T) {
	now := time.Unix(1_000, 0).UTC()
	queue := newFakeQueue().WithClock(func() time.Time { return now })
	ctx := context.Background()

	history := validSpec()
	history.OperationID = "a-history"
	history.Instance = "instance-a"
	historyObject, err := queue.Submit(ctx, "a-history", history)
	require.NoError(t, err)
	setOperationStatus(t, queue, historyObject, map[string]any{
		"phase": PhaseSucceeded, "startedAtUnix": int64(900),
		"startedAtUnixNano": int64(900_000_000_900),
		"completedAtUnix":   int64(901), "receiptSHA256": strings.Repeat("a", 64),
	})
	bHistory := validSpec()
	bHistory.OperationID = "b-history"
	bHistory.Instance = "instance-b"
	bHistoryObject, err := queue.Submit(ctx, "b-history", bHistory)
	require.NoError(t, err)
	setOperationStatus(t, queue, bHistoryObject, map[string]any{
		"phase": PhaseSucceeded, "startedAtUnix": int64(900),
		"startedAtUnixNano": int64(900_000_000_100),
		"completedAtUnix":   int64(901), "receiptSHA256": strings.Repeat("a", 64),
	})

	aFirst := validSpec()
	aFirst.OperationID = "a-first"
	aFirst.Instance = "instance-a"
	_, err = queue.Submit(ctx, "a-first", aFirst)
	require.NoError(t, err)
	aSecond := aFirst
	aSecond.OperationID = "a-second"
	_, err = queue.Submit(ctx, "a-second", aSecond)
	require.NoError(t, err)
	b := validSpec()
	b.OperationID = "b-first"
	b.Instance = "instance-b"
	_, err = queue.Submit(ctx, "b-first", b)
	require.NoError(t, err)

	claimB, err := queue.Claim(ctx, "worker-b", "", time.Minute)
	require.NoError(t, err)
	require.Equal(t, "b-first", claimB.Name)
	claimA, err := queue.Claim(ctx, "worker-a", "", time.Minute)
	require.NoError(t, err)
	require.Equal(t, "a-first", claimA.Name)

	_, err = queue.Claim(ctx, "worker-c", "", time.Minute)
	require.ErrorIs(t, err, ErrNoOperation)
	_, err = queue.Finish(ctx, claimA.Name, claimA.Owner, claimA.Attempt, true, strings.Repeat("b", 64), "done")
	require.NoError(t, err)
	nextA, err := queue.Claim(ctx, "worker-c", "", time.Minute)
	require.NoError(t, err)
	require.Equal(t, "a-second", nextA.Name)
}

func TestQueueFencesExpiredWorkerBeforeTakeover(t *testing.T) {
	now := time.Unix(1_000, 0).UTC()
	queue := newFakeQueue().WithClock(func() time.Time { return now })
	ctx := context.Background()
	_, err := queue.Submit(ctx, "operation-1", validSpec())
	require.NoError(t, err)
	claim, err := queue.Claim(ctx, "worker-a", "", 10*time.Second)
	require.NoError(t, err)

	now = now.Add(11 * time.Second)
	_, err = queue.Heartbeat(ctx, claim.Name, claim.Owner, claim.Attempt, time.Minute)
	require.ErrorIs(t, err, ErrFenced)
	_, err = queue.Requeue(ctx, claim.Name, claim.Owner, claim.Attempt, "late")
	require.ErrorIs(t, err, ErrFenced)
	_, err = queue.Finish(
		ctx, claim.Name, claim.Owner, claim.Attempt, true, strings.Repeat("a", 64), "late",
	)
	require.ErrorIs(t, err, ErrFenced)
}

func setOperationStatus(
	t *testing.T,
	queue *Queue,
	object *unstructured.Unstructured,
	status map[string]any,
) {
	t.Helper()
	updated := object.DeepCopy()
	updated.Object["status"] = status
	_, err := queue.resource.UpdateStatus(context.Background(), updated, metav1.UpdateOptions{})
	require.NoError(t, err)
}

func validSpec() Spec {
	return Spec{
		OperationID: "operation-1", Tenant: "tenant-a", RequestedBy: "user-123",
		Instance: "instance-a", Type: "PostRestoreAudit",
		ParametersSHA256: strings.Repeat("a", 64), MaxAttempts: 3,
	}
}

func newFakeQueue() *Queue {
	return New(fakeQueueClient(), "test")
}

func fakeQueueClient() *dynamicfake.FakeDynamicClient {
	return dynamicfake.NewSimpleDynamicClientWithCustomListKinds(
		runtime.NewScheme(), map[schema.GroupVersionResource]string{
			Resource:       "KubeBrainOperationList",
			LeaseResource:  "LeaseList",
			SecretResource: "SecretList",
		},
	)
}
