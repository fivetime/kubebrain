package operationqueue

import (
	"context"
	"errors"
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

	claim, err := queue.Claim(ctx, "worker-a", "", time.Minute)
	require.NoError(t, err)
	_, err = queue.Finish(ctx, claim.Name, claim.Owner, claim.Attempt, true, "", "")
	require.ErrorContains(t, err, "requires a receipt")
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
		OperationID: "operation-1", Instance: "instance-a", Type: "PostRestoreAudit",
		ParametersSHA256: strings.Repeat("a", 64), MaxAttempts: 3,
	}
}

func newFakeQueue() *Queue {
	client := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(
		runtime.NewScheme(), map[schema.GroupVersionResource]string{
			Resource:      "KubeBrainOperationList",
			LeaseResource: "LeaseList",
		},
	)
	return New(client, "test")
}
