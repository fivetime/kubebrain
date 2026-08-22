package operationarchiver

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"

	"github.com/kubewharf/kubebrain/hack/production/internal/contextsort"
	"github.com/kubewharf/kubebrain/hack/production/internal/dynamicpagination"
	"github.com/kubewharf/kubebrain/hack/production/internal/namespaceinventory"
	"github.com/kubewharf/kubebrain/hack/production/internal/operationqueue"
	"github.com/kubewharf/kubebrain/hack/production/operationaudit"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/client-go/dynamic"
)

type Processor interface {
	Process(context.Context, *unstructured.Unstructured) error
}

type Controller struct {
	client             dynamic.Interface
	processor          Processor
	inventoryNamespace string
	inventoryName      string
	inventoryKey       string
	maxBatch           int
	cursorMu           sync.Mutex
	cursor             candidateCursor
}

type candidateCursor struct {
	completedAt int64
	namespace   string
	name        string
	valid       bool
}

func New(
	client dynamic.Interface,
	processor Processor,
	inventoryNamespace, inventoryName, inventoryKey string,
	maxBatch int,
) (*Controller, error) {
	if client == nil || processor == nil || inventoryNamespace == "" || inventoryName == "" {
		return nil, errors.New("operation archiver configuration is incomplete")
	}
	var err error
	if inventoryKey, err = namespaceinventory.ValidateSource(
		inventoryNamespace, inventoryName, inventoryKey,
	); err != nil {
		return nil, err
	}
	if maxBatch <= 0 {
		return nil, errors.New("operation archiver max batch must be positive")
	}
	return &Controller{
		client: client, processor: processor,
		inventoryNamespace: inventoryNamespace, inventoryName: inventoryName,
		inventoryKey: inventoryKey, maxBatch: maxBatch,
	}, nil
}

func (c *Controller) Reconcile(ctx context.Context) (int, error) {
	namespaces, err := namespaceinventory.Load(
		ctx, c.client, c.inventoryNamespace, c.inventoryName, c.inventoryKey,
	)
	if err != nil {
		return 0, err
	}
	var candidates []*unstructured.Unstructured
	var errs []error
	candidateBytes := int64(0)
	for _, namespace := range namespaces {
		if err := ctx.Err(); err != nil {
			return 0, errors.Join(append(errs, err)...)
		}
		items, err := dynamicpagination.All(
			ctx, c.client.Resource(operationqueue.Resource).Namespace(namespace),
			metav1.ListOptions{},
			dynamicpagination.DefaultPageLimit, dynamicpagination.DefaultMaxItems,
			dynamicpagination.DefaultMaxBytes,
		)
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
			if needsArchive(&items[i]) {
				if int64(len(candidates)) >= dynamicpagination.DefaultMaxItems {
					return 0, errors.Join(append(errs, fmt.Errorf(
						"operation archive candidates exceed the %d-item aggregate limit",
						dynamicpagination.DefaultMaxItems,
					))...)
				}
				candidateBytes, err = dynamicpagination.Charge(
					candidateBytes, dynamicpagination.DefaultMaxBytes, &items[i],
				)
				if err != nil {
					return 0, errors.Join(append(errs,
						fmt.Errorf("charge operation archive candidate: %w", err),
					)...)
				}
				candidates = append(candidates, items[i].DeepCopy())
			}
		}
	}
	if err := contextsort.Slice(ctx, candidates, func(left, right *unstructured.Unstructured) bool {
		leftCompleted, _, _ := unstructured.NestedInt64(left.Object, "status", "completedAtUnix")
		rightCompleted, _, _ := unstructured.NestedInt64(right.Object, "status", "completedAtUnix")
		if leftCompleted != rightCompleted {
			return leftCompleted < rightCompleted
		}
		if left.GetNamespace() != right.GetNamespace() {
			return left.GetNamespace() < right.GetNamespace()
		}
		return left.GetName() < right.GetName()
	}); err != nil {
		return 0, errors.Join(append(errs, err)...)
	}
	candidates = c.nextBatch(candidates)
	processed := 0
	for _, candidate := range candidates {
		if err := ctx.Err(); err != nil {
			errs = append(errs, err)
			break
		}
		if err := c.processor.Process(ctx, candidate); err != nil {
			errs = append(errs, fmt.Errorf(
				"archive operation %s/%s: %w",
				candidate.GetNamespace(), candidate.GetName(), err,
			))
			c.markAttempted(candidate)
			continue
		}
		c.markAttempted(candidate)
		processed++
	}
	return processed, errors.Join(errs...)
}

func (c *Controller) nextBatch(candidates []*unstructured.Unstructured) []*unstructured.Unstructured {
	if len(candidates) == 0 {
		return candidates
	}
	c.cursorMu.Lock()
	cursor := c.cursor
	c.cursorMu.Unlock()
	start := 0
	if cursor.valid {
		start = sort.Search(len(candidates), func(i int) bool {
			return cursorLess(cursor, candidates[i])
		})
		if start == len(candidates) {
			start = 0
		}
	}
	batchSize := min(c.maxBatch, len(candidates))
	batch := make([]*unstructured.Unstructured, 0, batchSize)
	for offset := 0; offset < batchSize; offset++ {
		batch = append(batch, candidates[(start+offset)%len(candidates)])
	}
	return batch
}

func (c *Controller) markAttempted(candidate *unstructured.Unstructured) {
	completedAt, _, _ := unstructured.NestedInt64(candidate.Object, "status", "completedAtUnix")
	c.cursorMu.Lock()
	c.cursor = candidateCursor{
		completedAt: completedAt,
		namespace:   candidate.GetNamespace(),
		name:        candidate.GetName(),
		valid:       true,
	}
	c.cursorMu.Unlock()
}

func cursorLess(cursor candidateCursor, candidate *unstructured.Unstructured) bool {
	completedAt, _, _ := unstructured.NestedInt64(candidate.Object, "status", "completedAtUnix")
	if cursor.completedAt != completedAt {
		return cursor.completedAt < completedAt
	}
	if cursor.namespace != candidate.GetNamespace() {
		return cursor.namespace < candidate.GetNamespace()
	}
	return cursor.name < candidate.GetName()
}

func needsArchive(object *unstructured.Unstructured) bool {
	if object == nil || !contains(object.GetFinalizers(), operationaudit.Finalizer) {
		return false
	}
	phase, _, _ := unstructured.NestedString(object.Object, "status", "phase")
	return phase == "Succeeded" || phase == "Failed"
}

func contains(values []string, wanted string) bool {
	for _, value := range values {
		if value == wanted {
			return true
		}
	}
	return false
}
