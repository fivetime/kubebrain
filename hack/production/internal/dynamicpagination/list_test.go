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
	}, 2, 3)
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
			_, err := All(context.Background(), &recordingLister{pages: tc.pages}, metav1.ListOptions{}, 2, 10)
			require.ErrorContains(t, err, tc.want)
		})
	}
}

func TestAllPropagatesCancellationAndListErrors(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := All(ctx, &recordingLister{}, metav1.ListOptions{}, 2, 10)
	require.ErrorIs(t, err, context.Canceled)
	_, err = All(context.Background(), &recordingLister{err: errors.New("unavailable")}, metav1.ListOptions{}, 2, 10)
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
	items, err := All(ctx, lister, metav1.ListOptions{}, 1, 10)
	require.ErrorIs(t, err, context.Canceled)
	require.Nil(t, items)
	require.Len(t, lister.options, 1)
}

func TestAllRejectsInvalidInputs(t *testing.T) {
	_, err := All(nil, &recordingLister{}, metav1.ListOptions{}, 2, 10)
	require.Error(t, err)
	_, err = All(context.Background(), nil, metav1.ListOptions{}, 2, 10)
	require.Error(t, err)
	_, err = All(context.Background(), &recordingLister{}, metav1.ListOptions{Limit: 1}, 2, 10)
	require.Error(t, err)
	_, err = All(context.Background(), &recordingLister{}, metav1.ListOptions{}, 0, 10)
	require.Error(t, err)
	_, err = All(context.Background(), &recordingLister{}, metav1.ListOptions{}, 2, 0)
	require.Error(t, err)
}

func TestAllRejectsPageAndAggregateItemLimitViolations(t *testing.T) {
	_, err := All(context.Background(), &recordingLister{pages: []*unstructured.UnstructuredList{
		listPage("7", "", "a", "b", "c"),
	}}, metav1.ListOptions{}, 2, 10)
	require.ErrorContains(t, err, "above the 2-item page limit")

	_, err = All(context.Background(), &recordingLister{pages: []*unstructured.UnstructuredList{
		listPage("7", "next", "a", "b"), listPage("7", "", "c", "d"),
	}}, metav1.ListOptions{}, 2, 3)
	require.ErrorContains(t, err, "exceeds the 3-item aggregate limit")
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
