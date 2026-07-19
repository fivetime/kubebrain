package operationarchiver

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"

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
	cursor             int
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
	for _, namespace := range namespaces {
		list, err := c.client.Resource(operationqueue.Resource).Namespace(namespace).
			List(ctx, metav1.ListOptions{})
		if err != nil {
			errs = append(errs, fmt.Errorf("list operations in namespace %s: %w", namespace, err))
			continue
		}
		for i := range list.Items {
			if needsArchive(&list.Items[i]) {
				candidates = append(candidates, list.Items[i].DeepCopy())
			}
		}
	}
	sort.Slice(candidates, func(i, j int) bool {
		left, right := candidates[i], candidates[j]
		leftCompleted, _, _ := unstructured.NestedInt64(left.Object, "status", "completedAtUnix")
		rightCompleted, _, _ := unstructured.NestedInt64(right.Object, "status", "completedAtUnix")
		if leftCompleted != rightCompleted {
			return leftCompleted < rightCompleted
		}
		if left.GetNamespace() != right.GetNamespace() {
			return left.GetNamespace() < right.GetNamespace()
		}
		return left.GetName() < right.GetName()
	})
	candidates = c.nextBatch(candidates)
	processed := 0
	for _, candidate := range candidates {
		if err := c.processor.Process(ctx, candidate); err != nil {
			errs = append(errs, fmt.Errorf(
				"archive operation %s/%s: %w",
				candidate.GetNamespace(), candidate.GetName(), err,
			))
			continue
		}
		processed++
	}
	return processed, errors.Join(errs...)
}

func (c *Controller) nextBatch(candidates []*unstructured.Unstructured) []*unstructured.Unstructured {
	if len(candidates) <= c.maxBatch {
		return candidates
	}
	c.cursorMu.Lock()
	defer c.cursorMu.Unlock()
	start := c.cursor % len(candidates)
	batch := make([]*unstructured.Unstructured, 0, c.maxBatch)
	for offset := 0; offset < c.maxBatch; offset++ {
		batch = append(batch, candidates[(start+offset)%len(candidates)])
	}
	c.cursor = (start + c.maxBatch) % len(candidates)
	return batch
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
