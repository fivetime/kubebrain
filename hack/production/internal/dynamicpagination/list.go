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
)

type ResourceLister interface {
	List(context.Context, metav1.ListOptions) (*unstructured.UnstructuredList, error)
}

// All follows Kubernetes continue tokens and rejects token loops, a changing
// list resourceVersion, oversized pages, or an oversized collection. A caller
// aggregating multiple collections must enforce its own global item limit.
func All(ctx context.Context, resource ResourceLister, options metav1.ListOptions, pageLimit, maxItems int64) ([]unstructured.Unstructured, error) {
	if ctx == nil || resource == nil {
		return nil, errors.New("list context and resource are required")
	}
	if pageLimit <= 0 || maxItems <= 0 || options.Limit != 0 || options.Continue != "" {
		return nil, errors.New("page and item limits must be positive and list options must not predefine limit or continue")
	}
	var items []unstructured.Unstructured
	seenTokens := map[string]struct{}{}
	resourceVersion := ""
	pageNumber := 0
	continueToken := ""
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
