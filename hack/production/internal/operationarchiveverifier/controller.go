package operationarchiveverifier

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
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/client-go/dynamic"
)

type ObjectProcessor interface {
	Process(context.Context, *unstructured.Unstructured) error
}

type Controller struct {
	client                                          dynamic.Interface
	processor                                       ObjectProcessor
	inventoryNamespace, inventoryName, inventoryKey string
	maxBatch                                        int
	now                                             func() time.Time
	cursorMu                                        sync.Mutex
	cursor                                          candidateCursor
}

type candidateCursor struct {
	completedAt     int64
	namespace, name string
	valid           bool
}

func NewController(client dynamic.Interface, processor ObjectProcessor, inventoryNamespace, inventoryName, inventoryKey string, maxBatch int) (*Controller, error) {
	if client == nil || processor == nil || inventoryName == "" || maxBatch <= 0 {
		return nil, errors.New("operation archive verifier controller configuration is incomplete")
	}
	var err error
	if inventoryKey, err = namespaceinventory.ValidateSource(inventoryNamespace, inventoryName, inventoryKey); err != nil {
		return nil, err
	}
	return &Controller{client: client, processor: processor, inventoryNamespace: inventoryNamespace, inventoryName: inventoryName, inventoryKey: inventoryKey, maxBatch: maxBatch, now: time.Now}, nil
}

func (c *Controller) Reconcile(ctx context.Context) (int, error) {
	namespaces, err := namespaceinventory.Load(ctx, c.client, c.inventoryNamespace, c.inventoryName, c.inventoryKey)
	if err != nil {
		return 0, err
	}
	var candidates []*unstructured.Unstructured
	var errs []error
	for _, namespace := range namespaces {
		if err := ctx.Err(); err != nil {
			return 0, errors.Join(append(errs, err)...)
		}
		items, err := dynamicpagination.All(
			ctx, c.client.Resource(operationqueue.Resource).Namespace(namespace),
			metav1.ListOptions{},
			dynamicpagination.DefaultPageLimit, dynamicpagination.DefaultMaxItems,
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
			if needsVerification(&items[i]) {
				if int64(len(candidates)) >= dynamicpagination.DefaultMaxItems {
					return 0, errors.Join(append(errs, fmt.Errorf(
						"operation archive verification candidates exceed the %d-item aggregate limit",
						dynamicpagination.DefaultMaxItems,
					))...)
				}
				candidates = append(candidates, items[i].DeepCopy())
			}
		}
	}
	if err := contextsort.Slice(ctx, candidates, candidateLess); err != nil {
		return 0, errors.Join(append(errs, err)...)
	}
	candidates = c.nextBatch(candidates)
	verified := 0
	for _, candidate := range candidates {
		if err := ctx.Err(); err != nil {
			errs = append(errs, err)
			break
		}
		err := c.processor.Process(ctx, candidate)
		c.markAttempted(candidate)
		if err != nil {
			errs = append(errs, fmt.Errorf("verify operation archive %s/%s: %w", candidate.GetNamespace(), candidate.GetName(), err))
			continue
		}
		verified++
	}
	return verified, errors.Join(errs...)
}

func needsVerification(object *unstructured.Unstructured) bool {
	phase, _, _ := unstructured.NestedString(object.Object, "status", "phase")
	completed, found, _ := unstructured.NestedInt64(object.Object, "status", "completedAtUnix")
	return (phase == "Succeeded" || phase == "Failed") && found && completed > 0 && !contains(object.GetFinalizers(), operationaudit.Finalizer)
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
	} else {
		hour := c.now().UTC().Unix() / int64(time.Hour/time.Second)
		if hour < 0 {
			hour = -hour
		}
		start = int(hour % int64(len(items)))
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
