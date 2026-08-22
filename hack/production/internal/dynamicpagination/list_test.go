package dynamicpagination

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

type recordingLister struct {
	options []metav1.ListOptions
	pages   []*unstructured.UnstructuredList
	err     error
	onList  func(int)
}

func (l *recordingLister) List(_ context.Context, options metav1.ListOptions) (*unstructured.UnstructuredList, error) {
	l.options = append(l.options, options)
	if l.onList != nil {
		l.onList(len(l.options))
	}
	if l.err != nil {
		return nil, l.err
	}
	page := l.pages[0]
	l.pages = l.pages[1:]
	return page, nil
}

func TestAllFollowsConsistentPages(t *testing.T) {
	lister := &recordingLister{pages: []*unstructured.UnstructuredList{
		listPage("7", "next", "a", "b"), listPage("7", "", "c"),
	}}
	items, err := All(context.Background(), lister, metav1.ListOptions{
		LabelSelector:        "managed=true",
		ResourceVersion:      "7",
		ResourceVersionMatch: metav1.ResourceVersionMatchExact,
	}, 2, 3, 1<<20)
	require.NoError(t, err)
	require.Equal(t, []string{"a", "b", "c"}, itemNames(items))
	require.Equal(t, []metav1.ListOptions{
		{LabelSelector: "managed=true", ResourceVersion: "7", ResourceVersionMatch: metav1.ResourceVersionMatchExact, Limit: 2},
		{LabelSelector: "managed=true", Limit: 2, Continue: "next"},
	}, lister.options)
}

func TestAllFailsClosedOnPaginationDriftAndLoops(t *testing.T) {
	for _, tc := range []struct {
		name  string
		pages []*unstructured.UnstructuredList
		want  string
	}{
		{name: "resource version", pages: []*unstructured.UnstructuredList{listPage("7", "next"), listPage("8", "")}, want: "resourceVersion changed"},
		{name: "continue loop", pages: []*unstructured.UnstructuredList{listPage("7", "next"), listPage("7", "next")}, want: "repeated a continue token"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := All(context.Background(), &recordingLister{pages: tc.pages}, metav1.ListOptions{}, 2, 10, 1<<20)
			require.ErrorContains(t, err, tc.want)
		})
	}
}

func TestAllPropagatesCancellationAndListErrors(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := All(ctx, &recordingLister{}, metav1.ListOptions{}, 2, 10, 1<<20)
	require.ErrorIs(t, err, context.Canceled)
	_, err = All(context.Background(), &recordingLister{err: errors.New("unavailable")}, metav1.ListOptions{}, 2, 10, 1<<20)
	require.ErrorContains(t, err, "list page 1: unavailable")
}

func TestAllRejectsAPageReturnedAfterCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	lister := &recordingLister{
		pages: []*unstructured.UnstructuredList{listPage("7", "", "a")},
		onList: func(call int) {
			if call == 1 {
				cancel()
			}
		},
	}
	items, err := All(ctx, lister, metav1.ListOptions{}, 1, 10, 1<<20)
	require.ErrorIs(t, err, context.Canceled)
	require.Nil(t, items)
	require.Len(t, lister.options, 1)
}

func TestAllRejectsInvalidInputs(t *testing.T) {
	_, err := All(nil, &recordingLister{}, metav1.ListOptions{}, 2, 10, 1<<20)
	require.Error(t, err)
	_, err = All(context.Background(), nil, metav1.ListOptions{}, 2, 10, 1<<20)
	require.Error(t, err)
	_, err = All(context.Background(), &recordingLister{}, metav1.ListOptions{Limit: 1}, 2, 10, 1<<20)
	require.Error(t, err)
	_, err = All(context.Background(), &recordingLister{}, metav1.ListOptions{}, 0, 10, 1<<20)
	require.Error(t, err)
	_, err = All(context.Background(), &recordingLister{}, metav1.ListOptions{}, 2, 0, 1<<20)
	require.Error(t, err)
	_, err = All(context.Background(), &recordingLister{}, metav1.ListOptions{}, 2, 10, 0)
	require.Error(t, err)
}

func TestAllRejectsPageAndAggregateItemLimitViolations(t *testing.T) {
	_, err := All(context.Background(), &recordingLister{pages: []*unstructured.UnstructuredList{
		listPage("7", "", "a", "b", "c"),
	}}, metav1.ListOptions{}, 2, 10, 1<<20)
	require.ErrorContains(t, err, "above the 2-item page limit")

	_, err = All(context.Background(), &recordingLister{pages: []*unstructured.UnstructuredList{
		listPage("7", "next", "a", "b"), listPage("7", "", "c", "d"),
	}}, metav1.ListOptions{}, 2, 3, 1<<20)
	require.ErrorContains(t, err, "exceeds the 3-item aggregate limit")
}

func TestAllRejectsAggregateByteLimitAndSerializationFailures(t *testing.T) {
	_, err := All(context.Background(), &recordingLister{pages: []*unstructured.UnstructuredList{
		listPage("7", "", "a"),
	}}, metav1.ListOptions{}, 2, 10, 1)
	require.ErrorContains(t, err, "exceed the 1-byte aggregate limit")

	invalid := &unstructured.UnstructuredList{Items: []unstructured.Unstructured{{
		Object: map[string]any{"invalid": func() {}},
	}}}
	_, err = All(context.Background(), &recordingLister{pages: []*unstructured.UnstructuredList{invalid}}, metav1.ListOptions{}, 2, 10, 1<<20)
	require.ErrorContains(t, err, "serialize object for byte charge")
}

func TestChargeAccumulatesExactSerializedBytes(t *testing.T) {
	item := &unstructured.Unstructured{Object: map[string]any{"kind": "Example", "value": "payload"}}
	encoded, err := item.MarshalJSON()
	require.NoError(t, err)
	size := int64(len(encoded))
	charged, err := Charge(0, 2*size, item)
	require.NoError(t, err)
	require.Equal(t, size, charged)
	charged, err = Charge(charged, 2*size, item)
	require.NoError(t, err)
	require.Equal(t, 2*size, charged)
	_, err = Charge(charged, 2*size, item)
	require.ErrorContains(t, err, "byte aggregate limit")
	_, err = Charge(-1, 2*size, item)
	require.Error(t, err)
	_, err = Charge(0, 2*size, nil)
	require.Error(t, err)
}

func TestBudgetDefaultsAndValidation(t *testing.T) {
	budget := DefaultBudget()
	require.Equal(t, int64(500), budget.PageLimit)
	require.Equal(t, int64(10_000), budget.MaxItems)
	require.Equal(t, int64(64<<20), budget.MaxBytes)
	require.NoError(t, budget.Validate())
	for _, invalid := range []Budget{
		{MaxItems: 1, MaxBytes: 1},
		{PageLimit: 1, MaxBytes: 1},
		{PageLimit: 1, MaxItems: 1},
	} {
		require.ErrorContains(t, invalid.Validate(), "must be positive")
	}
}

func listPage(resourceVersion, continueToken string, names ...string) *unstructured.UnstructuredList {
	page := &unstructured.UnstructuredList{}
	page.SetResourceVersion(resourceVersion)
	page.SetContinue(continueToken)
	for _, name := range names {
		item := unstructured.Unstructured{}
		item.SetName(name)
		page.Items = append(page.Items, item)
	}
	return page
}

func itemNames(items []unstructured.Unstructured) []string {
	names := make([]string, len(items))
	for i := range items {
		names[i] = items[i].GetName()
	}
	return names
}
