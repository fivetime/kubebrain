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
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	clientgotesting "k8s.io/client-go/testing"
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

func TestClaimAcrossNamespacesReturnsIdleOnlyWhenAllQueuesAreEmpty(t *testing.T) {
	claim, err := ClaimAcrossNamespaces(
		context.Background(), fakeQueueClient(), []string{"tenant-a", "tenant-b"},
		"worker-a", "PostRestoreAudit", time.Minute,
	)
	require.Nil(t, claim)
	require.ErrorIs(t, err, ErrNoOperation)
}

func TestClaimAcrossNamespacesFailsClosedWhenQueueInspectionFails(t *testing.T) {
	client := fakeQueueClient()
	ctx := context.Background()
	_, err := New(client, "tenant-b").Submit(ctx, "backup-1", validSpec())
	require.NoError(t, err)
	client.PrependReactor("list", Resource.Resource, func(
		action clientgotesting.Action,
	) (bool, runtime.Object, error) {
		if action.GetNamespace() == "tenant-a" {
			return true, nil, errors.New("tenant-a API unavailable")
		}
		return false, nil, nil
	})

	claim, err := ClaimAcrossNamespaces(
		ctx, client, []string{"tenant-a", "tenant-b"},
		"worker-a", "PostRestoreAudit", time.Minute,
	)
	require.Nil(t, claim)
	require.ErrorContains(t, err, "tenant-a API unavailable")
	require.NotErrorIs(t, err, ErrNoOperation)
	operation, getErr := New(client, "tenant-b").Get(ctx, "backup-1")
	require.NoError(t, getErr)
	phase, _, _ := unstructured.NestedString(operation.Object, "status", "phase")
	require.Empty(t, phase)
}

func TestClaimAcrossNamespacesStopsInspectionWhenContextIsCanceled(t *testing.T) {
	client := fakeQueueClient()
	ctx, cancel := context.WithCancel(context.Background())
	inspected := []string{}
	client.PrependReactor("list", Resource.Resource, func(
		action clientgotesting.Action,
	) (bool, runtime.Object, error) {
		inspected = append(inspected, action.GetNamespace())
		cancel()
		return true, nil, ctx.Err()
	})

	claim, err := ClaimAcrossNamespaces(
		ctx, client, []string{"tenant-a", "tenant-b"},
		"worker-a", "PostRestoreAudit", time.Minute,
	)
	require.Nil(t, claim)
	require.ErrorIs(t, err, context.Canceled)
	require.NotErrorIs(t, err, ErrNoOperation)
	require.Equal(t, []string{"tenant-a"}, inspected)
}

func TestClaimAcrossNamespacesDoesNotSkipClaimFailure(t *testing.T) {
	client := fakeQueueClient()
	ctx := context.Background()
	for _, namespace := range []string{"tenant-a", "tenant-b"} {
		_, err := New(client, namespace).Submit(ctx, "backup-1", validSpec())
		require.NoError(t, err)
	}
	listCalls := map[string]int{}
	client.PrependReactor("list", Resource.Resource, func(
		action clientgotesting.Action,
	) (bool, runtime.Object, error) {
		namespace := action.GetNamespace()
		listCalls[namespace]++
		if namespace == "tenant-a" && listCalls[namespace] == 2 {
			return true, nil, errors.New("tenant-a claim unavailable")
		}
		return false, nil, nil
	})

	claim, err := ClaimAcrossNamespaces(
		ctx, client, []string{"tenant-a", "tenant-b"},
		"worker-a", "PostRestoreAudit", time.Minute,
	)
	require.Nil(t, claim)
	require.ErrorContains(t, err, "tenant-a claim unavailable")
	require.NotErrorIs(t, err, ErrNoOperation)
	require.Equal(t, 1, listCalls["tenant-b"],
		"the later queue must be inspected for fairness but not claimed after an earlier failure")
}

func TestClaimReleasesInstanceLeaseAfterCanceledStatusUpdate(t *testing.T) {
	client := fakeQueueClient()
	queue := New(client, "test")
	ctx, cancel := context.WithCancel(context.Background())
	_, err := queue.Submit(ctx, "backup-1", validSpec())
	require.NoError(t, err)
	client.PrependReactor("update", Resource.Resource, func(
		action clientgotesting.Action,
	) (bool, runtime.Object, error) {
		if action.GetSubresource() != "status" {
			return false, nil, nil
		}
		cancel()
		return true, nil, ctx.Err()
	})

	claim, err := queue.Claim(ctx, "worker-a", "PostRestoreAudit", time.Minute)
	require.Nil(t, claim)
	require.ErrorIs(t, err, context.Canceled)
	leases, listErr := client.Resource(LeaseResource).Namespace("test").
		List(context.Background(), metav1.ListOptions{})
	require.NoError(t, listErr)
	require.Empty(t, leases.Items)
}

func TestClaimReportsStatusAndLeaseCleanupFailures(t *testing.T) {
	client := fakeQueueClient()
	queue := New(client, "test")
	ctx := context.Background()
	_, err := queue.Submit(ctx, "backup-1", validSpec())
	require.NoError(t, err)
	client.PrependReactor("update", Resource.Resource, func(
		action clientgotesting.Action,
	) (bool, runtime.Object, error) {
		if action.GetSubresource() == "status" {
			return true, nil, errors.New("status unavailable")
		}
		return false, nil, nil
	})
	client.PrependReactor("delete", LeaseResource.Resource, func(
		clientgotesting.Action,
	) (bool, runtime.Object, error) {
		return true, nil, errors.New("lease cleanup unavailable")
	})

	claim, err := queue.Claim(ctx, "worker-a", "PostRestoreAudit", time.Minute)
	require.Nil(t, claim)
	require.ErrorContains(t, err, "status unavailable")
	require.ErrorContains(t, err, "lease cleanup unavailable")
	require.NotErrorIs(t, err, ErrNoOperation)
}

func TestFinishReportsLeaseCleanupFailureAfterTerminalStatusCommit(t *testing.T) {
	client := fakeQueueClient()
	queue := New(client, "test")
	ctx := context.Background()
	_, err := queue.Submit(ctx, "backup-1", validSpec())
	require.NoError(t, err)
	claim, err := queue.Claim(ctx, "worker-a", "PostRestoreAudit", time.Minute)
	require.NoError(t, err)
	deleteCalls := 0
	client.PrependReactor("delete", LeaseResource.Resource, func(
		clientgotesting.Action,
	) (bool, runtime.Object, error) {
		deleteCalls++
		if deleteCalls == 1 {
			return true, nil, errors.New("lease cleanup unavailable")
		}
		return false, nil, nil
	})

	result, err := queue.Finish(
		ctx, claim.Name, claim.Owner, claim.Attempt,
		true, strings.Repeat("a", 64), "complete",
	)
	require.NotNil(t, result)
	require.ErrorContains(t, err, "release instance lease after finish")
	phase, _, _ := unstructured.NestedString(result.Object, "status", "phase")
	require.Equal(t, PhaseSucceeded, phase)
	retried, err := queue.Finish(
		ctx, claim.Name, claim.Owner, claim.Attempt,
		true, strings.Repeat("a", 64), "complete",
	)
	require.NoError(t, err)
	require.Equal(t, result.GetResourceVersion(), retried.GetResourceVersion())
	leases, err := client.Resource(LeaseResource).Namespace("test").
		List(ctx, metav1.ListOptions{})
	require.NoError(t, err)
	require.Empty(t, leases.Items)
}

func TestRequeueRetriesLeaseCleanupAfterPendingStatusCommit(t *testing.T) {
	client := fakeQueueClient()
	queue := New(client, "test")
	ctx := context.Background()
	_, err := queue.Submit(ctx, "backup-1", validSpec())
	require.NoError(t, err)
	claim, err := queue.Claim(ctx, "worker-a", "PostRestoreAudit", time.Minute)
	require.NoError(t, err)
	deleteCalls := 0
	client.PrependReactor("delete", LeaseResource.Resource, func(
		clientgotesting.Action,
	) (bool, runtime.Object, error) {
		deleteCalls++
		if deleteCalls == 1 {
			return true, nil, errors.New("lease cleanup unavailable")
		}
		return false, nil, nil
	})

	result, err := queue.Requeue(
		ctx, claim.Name, claim.Owner, claim.Attempt, "transient",
	)
	require.NotNil(t, result)
	require.ErrorContains(t, err, "release instance lease after requeue")
	retried, err := queue.Requeue(
		ctx, claim.Name, claim.Owner, claim.Attempt, "transient",
	)
	require.NoError(t, err)
	require.Equal(t, result.GetResourceVersion(), retried.GetResourceVersion())
	leases, err := client.Resource(LeaseResource).Namespace("test").
		List(ctx, metav1.ListOptions{})
	require.NoError(t, err)
	require.Empty(t, leases.Items)

	_, err = queue.Requeue(
		ctx, claim.Name, claim.Owner, claim.Attempt, "different result",
	)
	require.ErrorIs(t, err, ErrFenced)
}

func TestFinishReconcilesCommittedStatusAfterLostResponse(t *testing.T) {
	client := fakeQueueClient()
	queue := New(client, "test")
	ctx := context.Background()
	_, err := queue.Submit(ctx, "backup-1", validSpec())
	require.NoError(t, err)
	claim, err := queue.Claim(ctx, "worker-a", "PostRestoreAudit", time.Minute)
	require.NoError(t, err)
	statusUpdates := 0
	client.PrependReactor("update", Resource.Resource, func(
		action clientgotesting.Action,
	) (bool, runtime.Object, error) {
		if action.GetSubresource() != "status" {
			return false, nil, nil
		}
		statusUpdates++
		updated := action.(clientgotesting.UpdateAction).GetObject()
		require.NoError(t, client.Tracker().Update(Resource, updated, "test"))
		return true, nil, errors.New("finish response lost after commit")
	})

	finished, err := queue.Finish(
		ctx, claim.Name, claim.Owner, claim.Attempt,
		true, strings.Repeat("a", 64), "complete",
	)
	require.NoError(t, err)
	require.Equal(t, 1, statusUpdates)
	phase, _, _ := unstructured.NestedString(finished.Object, "status", "phase")
	require.Equal(t, PhaseSucceeded, phase)
	leases, err := client.Resource(LeaseResource).Namespace("test").
		List(ctx, metav1.ListOptions{})
	require.NoError(t, err)
	require.Empty(t, leases.Items)
}

func TestRequeueReconcilesCommittedStatusAfterLostResponse(t *testing.T) {
	client := fakeQueueClient()
	queue := New(client, "test")
	ctx := context.Background()
	_, err := queue.Submit(ctx, "backup-1", validSpec())
	require.NoError(t, err)
	claim, err := queue.Claim(ctx, "worker-a", "PostRestoreAudit", time.Minute)
	require.NoError(t, err)
	statusUpdates := 0
	client.PrependReactor("update", Resource.Resource, func(
		action clientgotesting.Action,
	) (bool, runtime.Object, error) {
		if action.GetSubresource() != "status" {
			return false, nil, nil
		}
		statusUpdates++
		updated := action.(clientgotesting.UpdateAction).GetObject()
		require.NoError(t, client.Tracker().Update(Resource, updated, "test"))
		return true, nil, errors.New("requeue response lost after commit")
	})

	pending, err := queue.Requeue(
		ctx, claim.Name, claim.Owner, claim.Attempt, "transient",
	)
	require.NoError(t, err)
	require.Equal(t, 1, statusUpdates)
	phase, _, _ := unstructured.NestedString(pending.Object, "status", "phase")
	require.Equal(t, PhasePending, phase)
	leases, err := client.Resource(LeaseResource).Namespace("test").
		List(ctx, metav1.ListOptions{})
	require.NoError(t, err)
	require.Empty(t, leases.Items)
}

func TestFinishPreservesLeaseWhenStatusReconciliationIsUnavailable(t *testing.T) {
	client := fakeQueueClient()
	queue := New(client, "test")
	ctx := context.Background()
	_, err := queue.Submit(ctx, "backup-1", validSpec())
	require.NoError(t, err)
	claim, err := queue.Claim(ctx, "worker-a", "PostRestoreAudit", time.Minute)
	require.NoError(t, err)
	getCalls := 0
	client.PrependReactor("get", Resource.Resource, func(
		clientgotesting.Action,
	) (bool, runtime.Object, error) {
		getCalls++
		if getCalls > 1 {
			return true, nil, errors.New("finish reconciliation unavailable")
		}
		return false, nil, nil
	})
	client.PrependReactor("update", Resource.Resource, func(
		action clientgotesting.Action,
	) (bool, runtime.Object, error) {
		if action.GetSubresource() == "status" {
			return true, nil, errors.New("finish status unavailable")
		}
		return false, nil, nil
	})

	finished, err := queue.Finish(
		ctx, claim.Name, claim.Owner, claim.Attempt,
		true, strings.Repeat("a", 64), "complete",
	)
	require.Nil(t, finished)
	require.ErrorContains(t, err, "finish status unavailable")
	require.ErrorContains(t, err, "finish reconciliation unavailable")
	leases, err := client.Resource(LeaseResource).Namespace("test").
		List(ctx, metav1.ListOptions{})
	require.NoError(t, err)
	require.Len(t, leases.Items, 1)
}

func TestFinishStatusReconciliationOutlivesCanceledParent(t *testing.T) {
	client := fakeQueueClient()
	queue := New(client, "test")
	ctx, cancel := context.WithCancel(context.Background())
	_, err := queue.Submit(ctx, "backup-1", validSpec())
	require.NoError(t, err)
	claim, err := queue.Claim(ctx, "worker-a", "PostRestoreAudit", time.Minute)
	require.NoError(t, err)
	client.PrependReactor("update", Resource.Resource, func(
		action clientgotesting.Action,
	) (bool, runtime.Object, error) {
		if action.GetSubresource() != "status" {
			return false, nil, nil
		}
		updated := action.(clientgotesting.UpdateAction).GetObject()
		require.NoError(t, client.Tracker().Update(Resource, updated, "test"))
		cancel()
		return true, nil, ctx.Err()
	})

	finished, err := queue.Finish(
		ctx, claim.Name, claim.Owner, claim.Attempt,
		true, strings.Repeat("a", 64), "complete",
	)
	require.NoError(t, err)
	phase, _, _ := unstructured.NestedString(finished.Object, "status", "phase")
	require.Equal(t, PhaseSucceeded, phase)
	leases, listErr := client.Resource(LeaseResource).Namespace("test").
		List(context.Background(), metav1.ListOptions{})
	require.NoError(t, listErr)
	require.Empty(t, leases.Items)
}

func TestRequeueConflictCleansOldLeaseBeforeFencing(t *testing.T) {
	client := fakeQueueClient()
	queue := New(client, "test")
	ctx := context.Background()
	_, err := queue.Submit(ctx, "backup-1", validSpec())
	require.NoError(t, err)
	claim, err := queue.Claim(ctx, "worker-a", "PostRestoreAudit", time.Minute)
	require.NoError(t, err)
	client.PrependReactor("update", Resource.Resource, func(
		action clientgotesting.Action,
	) (bool, runtime.Object, error) {
		if action.GetSubresource() == "status" {
			return true, nil, apierrors.NewConflict(
				schema.GroupResource{Group: Resource.Group, Resource: Resource.Resource},
				claim.Name, errors.New("injected conflict"),
			)
		}
		return false, nil, nil
	})

	pending, err := queue.Requeue(
		ctx, claim.Name, claim.Owner, claim.Attempt, "transient",
	)
	require.Nil(t, pending)
	require.ErrorIs(t, err, ErrFenced)
	leases, err := client.Resource(LeaseResource).Namespace("test").
		List(ctx, metav1.ListOptions{})
	require.NoError(t, err)
	require.Empty(t, leases.Items)
}

func TestStatusTransitionRetriesRejectIncompleteCommittedState(t *testing.T) {
	queue := newFakeQueue()
	ctx := context.Background()
	object, err := queue.Submit(ctx, "backup-1", validSpec())
	require.NoError(t, err)
	setOperationStatus(t, queue, object, map[string]any{
		"phase": PhasePending, "owner": "", "attempt": int64(1),
		"leaseUntilUnix": int64(10), "message": "transient",
	})
	_, err = queue.Requeue(ctx, "backup-1", "worker-a", 1, "transient")
	require.ErrorIs(t, err, ErrFenced)

	object, err = queue.Get(ctx, "backup-1")
	require.NoError(t, err)
	setOperationStatus(t, queue, object, map[string]any{
		"phase": PhaseSucceeded, "owner": "worker-a", "attempt": int64(1),
		"leaseUntilUnix": int64(0), "completedAtUnix": int64(0),
		"receiptSHA256": strings.Repeat("a", 64), "message": "complete",
	})
	_, err = queue.Finish(
		ctx, "backup-1", "worker-a", 1,
		true, strings.Repeat("a", 64), "complete",
	)
	require.ErrorIs(t, err, ErrTerminal)
}

func TestLeaseCleanupContextOutlivesCanceledParentWithBoundedDeadline(t *testing.T) {
	type contextKey string
	parent, cancelParent := context.WithCancel(
		context.WithValue(context.Background(), contextKey("trace"), "trace-a"),
	)
	cancelParent()
	cleanup, cancelCleanup := leaseCleanupContext(parent)
	defer cancelCleanup()

	require.NoError(t, cleanup.Err())
	require.Equal(t, "trace-a", cleanup.Value(contextKey("trace")))
	deadline, found := cleanup.Deadline()
	require.True(t, found)
	require.WithinDuration(t, time.Now().Add(leaseCleanupTimeout), deadline, time.Second)
}

func TestHeartbeatReconcilesCommittedStatusAfterLostResponse(t *testing.T) {
	client := fakeQueueClient()
	now := time.Unix(1_000, 0).UTC()
	queue := New(client, "test").WithClock(func() time.Time { return now })
	ctx := context.Background()
	_, err := queue.Submit(ctx, "backup-1", validSpec())
	require.NoError(t, err)
	claim, err := queue.Claim(ctx, "worker-a", "PostRestoreAudit", time.Minute)
	require.NoError(t, err)
	client.PrependReactor("update", Resource.Resource, func(
		action clientgotesting.Action,
	) (bool, runtime.Object, error) {
		if action.GetSubresource() != "status" {
			return false, nil, nil
		}
		updated := action.(clientgotesting.UpdateAction).GetObject()
		require.NoError(t, client.Tracker().Update(Resource, updated, "test"))
		return true, nil, errors.New("response lost after commit")
	})

	reconciled, err := queue.Heartbeat(
		ctx, claim.Name, claim.Owner, claim.Attempt, 90*time.Second,
	)
	require.NoError(t, err)
	require.Equal(t, int64(1_090), reconciled.LeaseUntilUnix)
	leases, err := client.Resource(LeaseResource).Namespace("test").
		List(ctx, metav1.ListOptions{})
	require.NoError(t, err)
	require.Len(t, leases.Items, 1)
}

func TestHeartbeatReleasesRenewedLeaseAfterStatusConflict(t *testing.T) {
	client := fakeQueueClient()
	queue := New(client, "test")
	ctx := context.Background()
	_, err := queue.Submit(ctx, "backup-1", validSpec())
	require.NoError(t, err)
	claim, err := queue.Claim(ctx, "worker-a", "PostRestoreAudit", time.Minute)
	require.NoError(t, err)
	client.PrependReactor("update", Resource.Resource, func(
		action clientgotesting.Action,
	) (bool, runtime.Object, error) {
		if action.GetSubresource() == "status" {
			return true, nil, apierrors.NewConflict(
				schema.GroupResource{Group: Resource.Group, Resource: Resource.Resource},
				claim.Name, errors.New("injected conflict"),
			)
		}
		return false, nil, nil
	})

	reconciled, err := queue.Heartbeat(
		ctx, claim.Name, claim.Owner, claim.Attempt, 2*time.Minute,
	)
	require.Nil(t, reconciled)
	require.ErrorIs(t, err, ErrFenced)
	leases, err := client.Resource(LeaseResource).Namespace("test").
		List(ctx, metav1.ListOptions{})
	require.NoError(t, err)
	require.Empty(t, leases.Items)
}

func TestHeartbeatPreservesLeaseWhenReconciliationIsUnavailable(t *testing.T) {
	client := fakeQueueClient()
	queue := New(client, "test")
	ctx := context.Background()
	_, err := queue.Submit(ctx, "backup-1", validSpec())
	require.NoError(t, err)
	claim, err := queue.Claim(ctx, "worker-a", "PostRestoreAudit", time.Minute)
	require.NoError(t, err)
	getCalls := 0
	client.PrependReactor("get", Resource.Resource, func(
		clientgotesting.Action,
	) (bool, runtime.Object, error) {
		getCalls++
		if getCalls > 1 {
			return true, nil, errors.New("reconciliation unavailable")
		}
		return false, nil, nil
	})
	client.PrependReactor("update", Resource.Resource, func(
		action clientgotesting.Action,
	) (bool, runtime.Object, error) {
		if action.GetSubresource() == "status" {
			return true, nil, errors.New("status response unavailable")
		}
		return false, nil, nil
	})

	reconciled, err := queue.Heartbeat(
		ctx, claim.Name, claim.Owner, claim.Attempt, 2*time.Minute,
	)
	require.Nil(t, reconciled)
	require.ErrorContains(t, err, "status response unavailable")
	require.ErrorContains(t, err, "reconciliation unavailable")
	leases, err := client.Resource(LeaseResource).Namespace("test").
		List(ctx, metav1.ListOptions{})
	require.NoError(t, err)
	require.Len(t, leases.Items, 1)
}

func TestHeartbeatReportsStatusAndLeaseCleanupFailures(t *testing.T) {
	client := fakeQueueClient()
	queue := New(client, "test")
	ctx := context.Background()
	_, err := queue.Submit(ctx, "backup-1", validSpec())
	require.NoError(t, err)
	claim, err := queue.Claim(ctx, "worker-a", "PostRestoreAudit", time.Minute)
	require.NoError(t, err)
	client.PrependReactor("update", Resource.Resource, func(
		action clientgotesting.Action,
	) (bool, runtime.Object, error) {
		if action.GetSubresource() == "status" {
			return true, nil, errors.New("status unavailable")
		}
		return false, nil, nil
	})
	client.PrependReactor("delete", LeaseResource.Resource, func(
		clientgotesting.Action,
	) (bool, runtime.Object, error) {
		return true, nil, errors.New("lease cleanup unavailable")
	})

	reconciled, err := queue.Heartbeat(
		ctx, claim.Name, claim.Owner, claim.Attempt, 2*time.Minute,
	)
	require.Nil(t, reconciled)
	require.ErrorContains(t, err, "status unavailable")
	require.ErrorContains(t, err, "lease cleanup unavailable")
}

func TestHeartbeatCleanupOutlivesCanceledParent(t *testing.T) {
	client := fakeQueueClient()
	queue := New(client, "test")
	ctx, cancel := context.WithCancel(context.Background())
	_, err := queue.Submit(ctx, "backup-1", validSpec())
	require.NoError(t, err)
	claim, err := queue.Claim(ctx, "worker-a", "PostRestoreAudit", time.Minute)
	require.NoError(t, err)
	client.PrependReactor("update", Resource.Resource, func(
		action clientgotesting.Action,
	) (bool, runtime.Object, error) {
		if action.GetSubresource() != "status" {
			return false, nil, nil
		}
		cancel()
		return true, nil, ctx.Err()
	})

	reconciled, err := queue.Heartbeat(
		ctx, claim.Name, claim.Owner, claim.Attempt, 2*time.Minute,
	)
	require.Nil(t, reconciled)
	require.ErrorIs(t, err, context.Canceled)
	require.ErrorIs(t, err, ErrFenced)
	leases, listErr := client.Resource(LeaseResource).Namespace("test").
		List(context.Background(), metav1.ListOptions{})
	require.NoError(t, listErr)
	require.Empty(t, leases.Items)
}

func TestClaimReconcilesCommittedLeaseCreateAfterLostResponse(t *testing.T) {
	client := fakeQueueClient()
	queue := New(client, "test")
	ctx := context.Background()
	_, err := queue.Submit(ctx, "backup-1", validSpec())
	require.NoError(t, err)
	client.PrependReactor("create", LeaseResource.Resource, func(
		action clientgotesting.Action,
	) (bool, runtime.Object, error) {
		lease := action.(clientgotesting.CreateAction).GetObject()
		require.NoError(t, client.Tracker().Create(LeaseResource, lease, "test"))
		return true, nil, errors.New("lease create response lost")
	})

	claim, err := queue.Claim(ctx, "worker-a", "PostRestoreAudit", time.Minute)
	require.NoError(t, err)
	require.Equal(t, "backup-1", claim.Name)
	leases, err := client.Resource(LeaseResource).Namespace("test").
		List(ctx, metav1.ListOptions{})
	require.NoError(t, err)
	require.Len(t, leases.Items, 1)
}

func TestHeartbeatReconcilesCommittedLeaseUpdateAfterLostResponse(t *testing.T) {
	client := fakeQueueClient()
	now := time.Unix(1_000, 0).UTC()
	queue := New(client, "test").WithClock(func() time.Time { return now })
	ctx := context.Background()
	_, err := queue.Submit(ctx, "backup-1", validSpec())
	require.NoError(t, err)
	claim, err := queue.Claim(ctx, "worker-a", "PostRestoreAudit", time.Minute)
	require.NoError(t, err)
	now = now.Add(10 * time.Second)
	client.PrependReactor("update", LeaseResource.Resource, func(
		action clientgotesting.Action,
	) (bool, runtime.Object, error) {
		lease := action.(clientgotesting.UpdateAction).GetObject()
		require.NoError(t, client.Tracker().Update(LeaseResource, lease, "test"))
		return true, nil, errors.New("lease update response lost")
	})

	heartbeat, err := queue.Heartbeat(
		ctx, claim.Name, claim.Owner, claim.Attempt, 2*time.Minute,
	)
	require.NoError(t, err)
	require.Equal(t, int64(1_130), heartbeat.LeaseUntilUnix)
	leases, err := client.Resource(LeaseResource).Namespace("test").
		List(ctx, metav1.ListOptions{})
	require.NoError(t, err)
	require.Len(t, leases.Items, 1)
	renewed, _, _ := unstructured.NestedString(
		leases.Items[0].Object, "spec", "renewTime",
	)
	require.Equal(t, now.Format(microTimeFormat), renewed)
}

func TestClaimReportsLeaseWriteAndReconciliationFailures(t *testing.T) {
	client := fakeQueueClient()
	queue := New(client, "test")
	ctx := context.Background()
	_, err := queue.Submit(ctx, "backup-1", validSpec())
	require.NoError(t, err)
	client.PrependReactor("create", LeaseResource.Resource, func(
		clientgotesting.Action,
	) (bool, runtime.Object, error) {
		return true, nil, errors.New("lease write unavailable")
	})
	client.PrependReactor("get", LeaseResource.Resource, func(
		clientgotesting.Action,
	) (bool, runtime.Object, error) {
		return true, nil, errors.New("lease inspection unavailable")
	})

	claim, err := queue.Claim(ctx, "worker-a", "PostRestoreAudit", time.Minute)
	require.Nil(t, claim)
	require.ErrorContains(t, err, "lease write unavailable")
	require.ErrorContains(t, err, "lease inspection unavailable")
	operation, getErr := queue.Get(ctx, "backup-1")
	require.NoError(t, getErr)
	phase, _, _ := unstructured.NestedString(operation.Object, "status", "phase")
	require.Empty(t, phase)
}

func TestClaimRejectsMismatchedLeaseAfterFailedCreate(t *testing.T) {
	client := fakeQueueClient()
	queue := New(client, "test")
	ctx := context.Background()
	_, err := queue.Submit(ctx, "backup-1", validSpec())
	require.NoError(t, err)
	client.PrependReactor("create", LeaseResource.Resource, func(
		action clientgotesting.Action,
	) (bool, runtime.Object, error) {
		lease := action.(clientgotesting.CreateAction).GetObject().(*unstructured.Unstructured).DeepCopy()
		require.NoError(t, unstructured.SetNestedField(
			lease.Object, "2000-01-01T00:00:00.000000Z", "spec", "renewTime",
		))
		require.NoError(t, client.Tracker().Create(LeaseResource, lease, "test"))
		return true, nil, errors.New("lease create response lost")
	})

	claim, err := queue.Claim(ctx, "worker-a", "PostRestoreAudit", time.Minute)
	require.Nil(t, claim)
	require.ErrorContains(t, err, "lease create response lost")
	operation, getErr := queue.Get(ctx, "backup-1")
	require.NoError(t, getErr)
	phase, _, _ := unstructured.NestedString(operation.Object, "status", "phase")
	require.Empty(t, phase)
}

func TestClaimLeaseWriteReconciliationOutlivesCanceledParent(t *testing.T) {
	client := fakeQueueClient()
	queue := New(client, "test")
	ctx, cancel := context.WithCancel(context.Background())
	_, err := queue.Submit(ctx, "backup-1", validSpec())
	require.NoError(t, err)
	client.PrependReactor("create", LeaseResource.Resource, func(
		action clientgotesting.Action,
	) (bool, runtime.Object, error) {
		lease := action.(clientgotesting.CreateAction).GetObject()
		require.NoError(t, client.Tracker().Create(LeaseResource, lease, "test"))
		cancel()
		return true, nil, ctx.Err()
	})
	client.PrependReactor("update", Resource.Resource, func(
		action clientgotesting.Action,
	) (bool, runtime.Object, error) {
		if action.GetSubresource() == "status" {
			return true, nil, ctx.Err()
		}
		return false, nil, nil
	})

	claim, err := queue.Claim(ctx, "worker-a", "PostRestoreAudit", time.Minute)
	require.Nil(t, claim)
	require.ErrorIs(t, err, context.Canceled)
	leases, listErr := client.Resource(LeaseResource).Namespace("test").
		List(context.Background(), metav1.ListOptions{})
	require.NoError(t, listErr)
	require.Empty(t, leases.Items,
		"claim status cancellation must clean a reconciled Lease create")
}

func TestSubmitReconcilesCommittedCreateAfterLostResponse(t *testing.T) {
	client := fakeQueueClient()
	queue := New(client, "test")
	client.PrependReactor("create", Resource.Resource, func(
		action clientgotesting.Action,
	) (bool, runtime.Object, error) {
		object := action.(clientgotesting.CreateAction).GetObject()
		require.NoError(t, client.Tracker().Create(Resource, object, "test"))
		return true, nil, errors.New("submit response lost after commit")
	})

	created, err := queue.Submit(context.Background(), "backup-1", validSpec())
	require.NoError(t, err)
	require.Equal(t, "backup-1", created.GetName())
	require.Contains(t, created.GetFinalizers(), operationaudit.Finalizer)
}

func TestSubmitRejectsMismatchedCommittedObject(t *testing.T) {
	client := fakeQueueClient()
	queue := New(client, "test")
	client.PrependReactor("create", Resource.Resource, func(
		action clientgotesting.Action,
	) (bool, runtime.Object, error) {
		object := action.(clientgotesting.CreateAction).GetObject().(*unstructured.Unstructured).DeepCopy()
		require.NoError(t, unstructured.SetNestedField(
			object.Object, "replacement-instance", "spec", "instance",
		))
		require.NoError(t, client.Tracker().Create(Resource, object, "test"))
		return true, nil, errors.New("submit response lost after commit")
	})

	created, err := queue.Submit(context.Background(), "backup-1", validSpec())
	require.Nil(t, created)
	require.ErrorContains(t, err, "different immutable spec")
}

func TestSubmitRejectsCommittedObjectWithoutAuditFinalizer(t *testing.T) {
	client := fakeQueueClient()
	queue := New(client, "test")
	client.PrependReactor("create", Resource.Resource, func(
		action clientgotesting.Action,
	) (bool, runtime.Object, error) {
		object := action.(clientgotesting.CreateAction).GetObject().(*unstructured.Unstructured).DeepCopy()
		object.SetFinalizers(nil)
		require.NoError(t, client.Tracker().Create(Resource, object, "test"))
		return true, nil, errors.New("submit response lost after commit")
	})

	created, err := queue.Submit(context.Background(), "backup-1", validSpec())
	require.Nil(t, created)
	require.ErrorContains(t, err, "missing the audit finalizer")
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
	requesterControl := spec
	requesterControl.OperationID = "requester-control"
	requesterControl.RequestedBy = "user\n123"
	_, err = queue.Submit(ctx, "requester-control", requesterControl)
	require.ErrorIs(t, err, ErrInvalidSpec)
	require.ErrorContains(t, err, "operation requester")
	requesterInvalidUTF8 := spec
	requesterInvalidUTF8.OperationID = "requester-invalid-utf8"
	requesterInvalidUTF8.RequestedBy = string([]byte{'u', 0xff})
	_, err = queue.Submit(ctx, "requester-invalid-utf8", requesterInvalidUTF8)
	require.ErrorIs(t, err, ErrInvalidSpec)
	require.ErrorContains(t, err, "valid UTF-8")

	invalidOperationID := spec
	invalidOperationID.OperationID = "-invalid"
	_, err = queue.Submit(ctx, "invalid-operation-id", invalidOperationID)
	require.ErrorContains(t, err, "invalid operation ID")

	validCRDIdentifiers := spec
	validCRDIdentifiers.OperationID = "Operation_1.2"
	validCRDIdentifiers.Instance = "Instance_1.2"
	_, err = queue.Submit(ctx, "valid-identifiers", validCRDIdentifiers)
	require.NoError(t, err)

	invalidInstance := spec
	invalidInstance.Instance = ".invalid"
	_, err = queue.Submit(ctx, "invalid-instance", invalidInstance)
	require.ErrorContains(t, err, "invalid operation instance")

	invalidTenant := spec
	invalidTenant.Tenant = "Invalid_Tenant"
	_, err = queue.Submit(ctx, "invalid-tenant", invalidTenant)
	require.ErrorContains(t, err, "invalid operation tenant")

	invalidType := spec
	invalidType.OperationID = "invalid-type"
	invalidType.Type = "Unsupported"
	_, err = queue.Submit(ctx, "invalid-type", invalidType)
	require.ErrorIs(t, err, ErrInvalidSpec)
	require.ErrorContains(t, err, "unsupported operation type")

	invalidDigest := spec
	invalidDigest.OperationID = "invalid-digest"
	invalidDigest.ParametersSHA256 = strings.Repeat("g", 64)
	_, err = queue.Submit(ctx, "invalid-digest", invalidDigest)
	require.ErrorContains(t, err, "parameters digest")

	tooManyAttempts := spec
	tooManyAttempts.OperationID = "too-many-attempts"
	tooManyAttempts.MaxAttempts = 101
	_, err = queue.Submit(ctx, "too-many-attempts", tooManyAttempts)
	require.ErrorContains(t, err, "maxAttempts")

	invalidSecret := spec
	invalidSecret.OperationID = "invalid-secret"
	invalidSecret.ParametersSecret = "Invalid_Secret"
	invalidSecret.ParametersKey = "parameters.json"
	_, err = queue.Submit(ctx, "invalid-secret", invalidSecret)
	require.ErrorContains(t, err, "invalid parameter secret name")

	invalidSecretKey := spec
	invalidSecretKey.OperationID = "invalid-secret-key"
	invalidSecretKey.ParametersSecret = "valid-secret"
	invalidSecretKey.ParametersKey = "parameters/json"
	_, err = queue.Submit(ctx, "invalid-secret-key", invalidSecretKey)
	require.ErrorIs(t, err, ErrInvalidSpec)
	require.ErrorContains(t, err, "invalid parameter secret key")

	_, err = queue.Claim(ctx, strings.Repeat("w", maxStatusOwnerLength+1), "", time.Minute)
	require.ErrorContains(t, err, "status owner")
	_, err = queue.Claim(ctx, "worker\ncontrol", "", time.Minute)
	require.ErrorContains(t, err, "control characters")
	_, err = queue.Claim(ctx, string([]byte{'w', 0xff}), "", time.Minute)
	require.ErrorContains(t, err, "valid UTF-8")

	claim, err := queue.Claim(ctx, "worker-a", "", time.Minute)
	require.NoError(t, err)
	_, err = queue.Heartbeat(ctx, claim.Name, strings.Repeat("w", maxStatusOwnerLength+1), claim.Attempt, time.Minute)
	require.ErrorContains(t, err, "status owner")
	_, err = queue.Heartbeat(ctx, claim.Name, "worker\ncontrol", claim.Attempt, time.Minute)
	require.ErrorContains(t, err, "control characters")
	_, err = queue.Requeue(ctx, claim.Name, claim.Owner, claim.Attempt, strings.Repeat("m", maxStatusMessageLength+1))
	require.ErrorContains(t, err, "status message")
	_, err = queue.Requeue(ctx, claim.Name, claim.Owner, claim.Attempt, "retry\nlater")
	require.ErrorContains(t, err, "control characters")
	_, err = queue.Finish(
		ctx, claim.Name, claim.Owner, claim.Attempt,
		true, strings.Repeat("a", 64), strings.Repeat("m", maxStatusMessageLength+1),
	)
	require.ErrorContains(t, err, "status message")
	_, err = queue.Finish(
		ctx, claim.Name, claim.Owner, claim.Attempt,
		true, strings.Repeat("a", 64), string([]byte{'m', 0xff}),
	)
	require.ErrorContains(t, err, "valid UTF-8")
	_, err = queue.Finish(ctx, claim.Name, claim.Owner, claim.Attempt, false, strings.Repeat("a", 64), "")
	require.ErrorContains(t, err, "failed operation cannot carry")
	_, err = queue.Finish(ctx, claim.Name, claim.Owner, claim.Attempt, false, strings.Repeat("A", 64), "")
	require.ErrorContains(t, err, "failed operation cannot carry")
	_, err = queue.Finish(ctx, claim.Name, claim.Owner, claim.Attempt, true, "", "")
	require.ErrorContains(t, err, "requires a receipt")
	_, err = queue.Finish(ctx, claim.Name, claim.Owner, claim.Attempt, true, strings.Repeat("A", 64), "")
	require.ErrorContains(t, err, "receipt SHA-256 hex digest")
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

func TestApproveReconcilesCommittedUpdateAfterLostResponse(t *testing.T) {
	client := fakeQueueClient()
	queue := New(client, "test")
	ctx := context.Background()
	spec := validSpec()
	spec.Type = "Destroy"
	object, err := queue.Submit(ctx, "destroy", spec)
	require.NoError(t, err)
	object.SetUID(types.UID("uid-destroy"))
	_, err = queue.resource.Update(ctx, object, metav1.UpdateOptions{})
	require.NoError(t, err)
	client.PrependReactor("update", Resource.Resource, func(
		action clientgotesting.Action,
	) (bool, runtime.Object, error) {
		updated := action.(clientgotesting.UpdateAction).GetObject()
		require.NoError(t, client.Tracker().Update(Resource, updated, "test"))
		return true, nil, errors.New("approval response lost after commit")
	})

	approved, err := queue.Approve(
		ctx, "destroy", operationaudit.ApproverUsername, "change-123",
	)
	require.NoError(t, err)
	require.Equal(t, types.UID("uid-destroy"), approved.GetUID())
	require.Equal(t, "change-123",
		approved.GetAnnotations()[operationaudit.ApprovalIDAnnotation])
}

func TestApproveReportsWriteAndReconciliationFailures(t *testing.T) {
	client := fakeQueueClient()
	queue := New(client, "test")
	ctx := context.Background()
	spec := validSpec()
	spec.Type = "Destroy"
	_, err := queue.Submit(ctx, "destroy", spec)
	require.NoError(t, err)
	getCalls := 0
	client.PrependReactor("get", Resource.Resource, func(
		clientgotesting.Action,
	) (bool, runtime.Object, error) {
		getCalls++
		if getCalls > 1 {
			return true, nil, errors.New("approval reconciliation unavailable")
		}
		return false, nil, nil
	})
	client.PrependReactor("update", Resource.Resource, func(
		clientgotesting.Action,
	) (bool, runtime.Object, error) {
		return true, nil, errors.New("approval write unavailable")
	})

	approved, err := queue.Approve(
		ctx, "destroy", operationaudit.ApproverUsername, "change-123",
	)
	require.Nil(t, approved)
	require.ErrorContains(t, err, "approval write unavailable")
	require.ErrorContains(t, err, "approval reconciliation unavailable")
}

func TestApproveRejectsReplacementUIDAfterFailedUpdate(t *testing.T) {
	client := fakeQueueClient()
	queue := New(client, "test")
	ctx := context.Background()
	spec := validSpec()
	spec.Type = "Destroy"
	object, err := queue.Submit(ctx, "destroy", spec)
	require.NoError(t, err)
	object.SetUID(types.UID("uid-original"))
	_, err = queue.resource.Update(ctx, object, metav1.UpdateOptions{})
	require.NoError(t, err)
	client.PrependReactor("update", Resource.Resource, func(
		action clientgotesting.Action,
	) (bool, runtime.Object, error) {
		replacement := action.(clientgotesting.UpdateAction).GetObject().(*unstructured.Unstructured).DeepCopy()
		replacement.SetUID(types.UID("uid-replacement"))
		require.NoError(t, client.Tracker().Delete(Resource, "test", "destroy"))
		require.NoError(t, client.Tracker().Create(Resource, replacement, "test"))
		return true, nil, errors.New("approval response lost after replacement")
	})

	approved, err := queue.Approve(
		ctx, "destroy", operationaudit.ApproverUsername, "change-123",
	)
	require.Nil(t, approved)
	require.ErrorContains(t, err, "operation was replaced while approving")
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
	_, err = queue.Approve(ctx, "destroy", "system:serviceaccount:test:approver", "change-123")
	require.ErrorContains(t, err, "dedicated approver")
}

func TestDeleteReconcilesCommittedUIDFencedDelete(t *testing.T) {
	client := fakeQueueClient()
	queue := New(client, "test")
	ctx := context.Background()
	createOperationForDelete(t, queue, "delete-me", types.UID("uid-original"))
	client.PrependReactor("delete", Resource.Resource, func(
		clientgotesting.Action,
	) (bool, runtime.Object, error) {
		require.NoError(t, client.Tracker().Delete(Resource, "test", "delete-me"))
		return true, nil, errors.New("delete response lost after commit")
	})

	require.NoError(t, queue.Delete(ctx, "delete-me", types.UID("uid-original")))
	_, err := queue.Get(ctx, "delete-me")
	require.True(t, apierrors.IsNotFound(err))
}

func TestDeletePreservesReplacementAfterFailedDelete(t *testing.T) {
	client := fakeQueueClient()
	queue := New(client, "test")
	ctx := context.Background()
	createOperationForDelete(t, queue, "delete-me", types.UID("uid-original"))
	client.PrependReactor("delete", Resource.Resource, func(
		clientgotesting.Action,
	) (bool, runtime.Object, error) {
		require.NoError(t, client.Tracker().Delete(Resource, "test", "delete-me"))
		replacement := operationForDelete("delete-me", types.UID("uid-replacement"))
		require.NoError(t, client.Tracker().Create(Resource, replacement, "test"))
		return true, nil, errors.New("delete response lost after replacement")
	})

	require.NoError(t, queue.Delete(ctx, "delete-me", types.UID("uid-original")))
	replacement, err := queue.Get(ctx, "delete-me")
	require.NoError(t, err)
	require.Equal(t, types.UID("uid-replacement"), replacement.GetUID())
}

func TestDeletePreservesErrorWhileOriginalTargetExists(t *testing.T) {
	client := fakeQueueClient()
	queue := New(client, "test")
	ctx := context.Background()
	createOperationForDelete(t, queue, "delete-me", types.UID("uid-original"))
	client.PrependReactor("delete", Resource.Resource, func(
		clientgotesting.Action,
	) (bool, runtime.Object, error) {
		return true, nil, errors.New("delete unavailable")
	})

	err := queue.Delete(ctx, "delete-me", types.UID("uid-original"))
	require.ErrorContains(t, err, "delete unavailable")
	remaining, getErr := queue.Get(ctx, "delete-me")
	require.NoError(t, getErr)
	require.Equal(t, types.UID("uid-original"), remaining.GetUID())
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

func createOperationForDelete(t *testing.T, queue *Queue, name string, uid types.UID) {
	t.Helper()
	_, err := queue.resource.Create(
		context.Background(), operationForDelete(name, uid), metav1.CreateOptions{},
	)
	require.NoError(t, err)
}

func operationForDelete(name string, uid types.UID) *unstructured.Unstructured {
	object := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": Resource.Group + "/" + Resource.Version,
		"kind":       "KubeBrainOperation",
		"metadata":   map[string]any{"name": name},
	}}
	object.SetUID(uid)
	return object
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
