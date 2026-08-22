package operationretention

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/kubewharf/kubebrain/hack/production/internal/contextsort"
	"github.com/kubewharf/kubebrain/hack/production/internal/dynamicpagination"
	"github.com/kubewharf/kubebrain/hack/production/internal/namespaceinventory"
	"github.com/kubewharf/kubebrain/hack/production/internal/operationqueue"
	"github.com/kubewharf/kubebrain/hack/production/operationaudit"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/dynamic"
)

type Verifier interface {
	Process(context.Context, *unstructured.Unstructured) error
}

type Controller struct {
	client                                          dynamic.Interface
	verifier                                        Verifier
	inventoryNamespace, inventoryName, inventoryKey string
	deleteAfter                                     time.Duration
	maxBatch                                        int
	scanBudget                                      dynamicpagination.Budget
	now                                             func() time.Time
	cursorMu                                        sync.Mutex
	cursor                                          candidateCursor
}

type candidateCursor struct {
	completedAt     int64
	namespace, name string
	valid           bool
}

func New(client dynamic.Interface, verifier Verifier, inventoryNamespace, inventoryName, inventoryKey string, deleteAfter time.Duration, maxBatch int) (*Controller, error) {
	if client == nil || verifier == nil || inventoryName == "" || deleteAfter <= 0 || maxBatch <= 0 {
		return nil, errors.New("operation retention controller configuration is incomplete")
	}
	var err error
	if inventoryKey, err = namespaceinventory.ValidateSource(inventoryNamespace, inventoryName, inventoryKey); err != nil {
		return nil, err
	}
	return &Controller{
		client: client, verifier: verifier, inventoryNamespace: inventoryNamespace,
		inventoryName: inventoryName, inventoryKey: inventoryKey, deleteAfter: deleteAfter,
		maxBatch: maxBatch, scanBudget: dynamicpagination.DefaultBudget(), now: time.Now,
	}, nil
}

func (c *Controller) SetScanBudget(maxItems, maxBytes int64) error {
	budget := dynamicpagination.Budget{PageLimit: dynamicpagination.DefaultPageLimit, MaxItems: maxItems, MaxBytes: maxBytes}
	if err := budget.Validate(); err != nil {
		return fmt.Errorf("operation retention scan budget: %w", err)
	}
	c.scanBudget = budget
	return nil
}

func (c *Controller) Reconcile(ctx context.Context) (int, error) {
	namespaces, err := namespaceinventory.Load(ctx, c.client, c.inventoryNamespace, c.inventoryName, c.inventoryKey)
	if err != nil {
		return 0, err
	}
	cutoff := c.now().UTC().Add(-c.deleteAfter).Unix()
	var candidates []*unstructured.Unstructured
	var errs []error
	var candidateBytes int64
	for _, namespace := range namespaces {
		if err := ctx.Err(); err != nil {
			return 0, errors.Join(append(errs, err)...)
		}
		items, err := dynamicpagination.All(ctx, c.client.Resource(operationqueue.Resource).Namespace(namespace), metav1.ListOptions{}, c.scanBudget.PageLimit, c.scanBudget.MaxItems, c.scanBudget.MaxBytes)
		if err != nil {
			errs = append(errs, fmt.Errorf("list operations in namespace %s: %w", namespace, err))
			if ctx.Err() != nil {
				return 0, errors.Join(errs...)
			}
			continue
		}
		for i := range items {
			if err := ctx.Err(); err != nil {
				return 0, errors.Join(append(errs, err)...)
			}
			if !eligible(&items[i], cutoff) {
				continue
			}
			if int64(len(candidates)) >= c.scanBudget.MaxItems {
				return 0, errors.Join(append(errs, fmt.Errorf("operation retention candidates exceed the %d-item aggregate limit", c.scanBudget.MaxItems))...)
			}
			candidateBytes, err = dynamicpagination.Charge(candidateBytes, c.scanBudget.MaxBytes, &items[i])
			if err != nil {
				return 0, errors.Join(append(errs, fmt.Errorf("charge operation retention candidate: %w", err))...)
			}
			candidates = append(candidates, items[i].DeepCopy())
		}
	}
	if err := contextsort.Slice(ctx, candidates, candidateLess); err != nil {
		return 0, errors.Join(append(errs, err)...)
	}
	candidates = c.nextBatch(candidates)
	deleted := 0
	for _, candidate := range candidates {
		if err := ctx.Err(); err != nil {
			errs = append(errs, err)
			break
		}
		if err := c.verifier.Process(ctx, candidate); err != nil {
			errs = append(errs, fmt.Errorf("verify retained operation archive %s/%s: %w", candidate.GetNamespace(), candidate.GetName(), err))
			c.markAttempted(candidate)
			continue
		}
		if err := c.deleteExact(ctx, candidate); err != nil {
			errs = append(errs, fmt.Errorf("delete retained operation %s/%s: %w", candidate.GetNamespace(), candidate.GetName(), err))
			c.markAttempted(candidate)
			continue
		}
		c.markAttempted(candidate)
		deleted++
	}
	return deleted, errors.Join(errs...)
}

func eligible(object *unstructured.Unstructured, cutoff int64) bool {
	if object == nil || object.GetDeletionTimestamp() != nil || len(object.GetFinalizers()) != 0 || object.GetUID() == "" || object.GetResourceVersion() == "" {
		return false
	}
	phase, _, _ := unstructured.NestedString(object.Object, "status", "phase")
	completed, found, _ := unstructured.NestedInt64(object.Object, "status", "completedAtUnix")
	if (phase != operationqueue.PhaseSucceeded && phase != operationqueue.PhaseFailed) || !found || completed <= 0 || completed > cutoff {
		return false
	}
	return operationaudit.ValidateArchiveEvidenceAnnotations(object.GetAnnotations()) == nil
}

func (c *Controller) deleteExact(ctx context.Context, object *unstructured.Unstructured) error {
	uid, resourceVersion := object.GetUID(), object.GetResourceVersion()
	if uid == "" || resourceVersion == "" {
		return errors.New("operation UID and resourceVersion preconditions are required")
	}
	resource := c.client.Resource(operationqueue.Resource).Namespace(object.GetNamespace())
	policy := metav1.DeletePropagationForeground
	err := resource.Delete(ctx, object.GetName(), metav1.DeleteOptions{
		Preconditions:     &metav1.Preconditions{UID: &uid, ResourceVersion: &resourceVersion},
		PropagationPolicy: &policy,
	})
	if err == nil || apierrors.IsNotFound(err) {
		return nil
	}
	reconcileCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	current, getErr := resource.Get(reconcileCtx, object.GetName(), metav1.GetOptions{})
	if apierrors.IsNotFound(getErr) {
		return nil
	}
	if getErr != nil {
		return errors.Join(err, fmt.Errorf("inspect operation after failed retention delete: %w", getErr))
	}
	if current.GetUID() != types.UID(uid) {
		return nil
	}
	return err
}

func candidateLess(left, right *unstructured.Unstructured) bool {
	lc, _, _ := unstructured.NestedInt64(left.Object, "status", "completedAtUnix")
	rc, _, _ := unstructured.NestedInt64(right.Object, "status", "completedAtUnix")
	if lc != rc {
		return lc < rc
	}
	if left.GetNamespace() != right.GetNamespace() {
		return left.GetNamespace() < right.GetNamespace()
	}
	return left.GetName() < right.GetName()
}

func (c *Controller) nextBatch(items []*unstructured.Unstructured) []*unstructured.Unstructured {
	if len(items) == 0 {
		return items
	}
	c.cursorMu.Lock()
	cursor := c.cursor
	c.cursorMu.Unlock()
	start := 0
	if cursor.valid {
		start = sort.Search(len(items), func(i int) bool {
			completed, _, _ := unstructured.NestedInt64(items[i].Object, "status", "completedAtUnix")
			if completed != cursor.completedAt {
				return completed > cursor.completedAt
			}
			if items[i].GetNamespace() != cursor.namespace {
				return items[i].GetNamespace() > cursor.namespace
			}
			return items[i].GetName() > cursor.name
		})
		if start == len(items) {
			start = 0
		}
	}
	batch := make([]*unstructured.Unstructured, 0, min(c.maxBatch, len(items)))
	for i := 0; i < min(c.maxBatch, len(items)); i++ {
		batch = append(batch, items[(start+i)%len(items)])
	}
	return batch
}

func (c *Controller) markAttempted(item *unstructured.Unstructured) {
	completed, _, _ := unstructured.NestedInt64(item.Object, "status", "completedAtUnix")
	c.cursorMu.Lock()
	c.cursor = candidateCursor{completedAt: completed, namespace: item.GetNamespace(), name: item.GetName(), valid: true}
	c.cursorMu.Unlock()
}
