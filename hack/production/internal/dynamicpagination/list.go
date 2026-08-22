package dynamicpagination

import (
	"context"
	"errors"
	"fmt"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

const (
	DefaultPageLimit int64 = 500
	DefaultMaxItems  int64 = 10_000
	DefaultMaxBytes  int64 = 64 << 20
)

type ResourceLister interface {
	List(context.Context, metav1.ListOptions) (*unstructured.UnstructuredList, error)
}

// All follows Kubernetes continue tokens and rejects token loops, a changing
// list resourceVersion, oversized pages, or an oversized collection. A caller
// aggregating multiple collections must enforce its own global item and byte
// limits.
func All(ctx context.Context, resource ResourceLister, options metav1.ListOptions, pageLimit, maxItems, maxBytes int64) ([]unstructured.Unstructured, error) {
	if ctx == nil || resource == nil {
		return nil, errors.New("list context and resource are required")
	}
	if pageLimit <= 0 || maxItems <= 0 || maxBytes <= 0 || options.Limit != 0 || options.Continue != "" {
		return nil, errors.New("page, item, and byte limits must be positive and list options must not predefine limit or continue")
	}
	var items []unstructured.Unstructured
	seenTokens := map[string]struct{}{}
	resourceVersion := ""
	pageNumber := 0
	continueToken := ""
	aggregateBytes := int64(0)
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		pageNumber++
		pageOptions := options
		pageOptions.Limit = pageLimit
		pageOptions.Continue = continueToken
		if continueToken != "" {
			// The continue token already binds the snapshot resourceVersion.
			// Kubernetes rejects a continuation request that also specifies one.
			pageOptions.ResourceVersion = ""
			pageOptions.ResourceVersionMatch = ""
		}
		page, err := resource.List(ctx, pageOptions)
		if err != nil {
			return nil, fmt.Errorf("list page %d: %w", pageNumber, err)
		}
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if page == nil {
			return nil, fmt.Errorf("list page %d returned a nil response", pageNumber)
		}
		pageItems := int64(len(page.Items))
		if pageItems > pageLimit {
			return nil, fmt.Errorf("list page %d returned %d items above the %d-item page limit", pageNumber, pageItems, pageLimit)
		}
		if int64(len(items)) > maxItems-pageItems {
			return nil, fmt.Errorf("list exceeds the %d-item aggregate limit", maxItems)
		}
		if pageNumber == 1 {
			resourceVersion = page.GetResourceVersion()
		} else if page.GetResourceVersion() != resourceVersion {
			return nil, fmt.Errorf("list resourceVersion changed between pages %d and %d", pageNumber-1, pageNumber)
		}
		for i := range page.Items {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			aggregateBytes, err = Charge(aggregateBytes, maxBytes, &page.Items[i])
			if err != nil {
				return nil, fmt.Errorf("charge list page %d item %d: %w", pageNumber, i, err)
			}
		}
		items = append(items, page.Items...)
		next := page.GetContinue()
		if next == "" {
			return items, nil
		}
		if _, duplicate := seenTokens[next]; duplicate {
			return nil, fmt.Errorf("list page %d repeated a continue token", pageNumber)
		}
		seenTokens[next] = struct{}{}
		continueToken = next
	}
}

// Charge adds one object's JSON representation to a bounded aggregate.
func Charge(current, maxBytes int64, item *unstructured.Unstructured) (int64, error) {
	if current < 0 || maxBytes <= 0 || item == nil {
		return 0, errors.New("byte charge, limit, and object are invalid")
	}
	encoded, err := item.MarshalJSON()
	if err != nil {
		return 0, fmt.Errorf("serialize object for byte charge: %w", err)
	}
	size := int64(len(encoded))
	if current > maxBytes-size {
		return 0, fmt.Errorf("objects exceed the %d-byte aggregate limit", maxBytes)
	}
	return current + size, nil
}
