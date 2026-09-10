package main

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/watch"
)

func TestFullListFollowsContinuation(t *testing.T) {
	calls := 0
	rv, err := listRevision(context.Background(), func(_ context.Context, o metav1.ListOptions) (page, error) {
		calls++
		require.EqualValues(t, 5000, o.Limit)
		if calls == 1 {
			require.Empty(t, o.Continue)
			return page{"42", "next"}, nil
		}
		require.Equal(t, "next", o.Continue)
		return page{"42", ""}, nil
	}, true)
	require.NoError(t, err)
	require.Equal(t, "42", rv)
	require.Equal(t, 2, calls)
}

func TestLightListStopsAfterFirstPage(t *testing.T) {
	rv, err := listRevision(context.Background(), func(_ context.Context, o metav1.ListOptions) (page, error) {
		require.EqualValues(t, 1, o.Limit)
		return page{"42", "next"}, nil
	}, false)
	require.NoError(t, err)
	require.Equal(t, "42", rv)
}

func TestListRejectsBrokenPagination(t *testing.T) {
	for _, second := range []page{{"43", ""}, {"42", "next"}, {"", ""}} {
		calls := 0
		_, err := listRevision(context.Background(), func(context.Context, metav1.ListOptions) (page, error) {
			calls++
			if calls == 1 {
				return page{"42", "next"}, nil
			}
			return second, nil
		}, true)
		require.Error(t, err)
	}
}

func TestWatchErrorsAreNotCountedAsData(t *testing.T) {
	stream := watch.NewRaceFreeFake()
	stream.Add(&metav1.PartialObjectMetadata{})
	stream.Action(watch.Bookmark, &metav1.PartialObjectMetadata{})
	stream.Error(&metav1.Status{Code: 410})
	var c counters
	require.ErrorContains(t, drain(context.Background(), stream, &c), "watch error")
	require.EqualValues(t, 1, c.events.Load())
	require.EqualValues(t, 1, c.bookmarks.Load())
	require.True(t, stream.IsStopped())
}

func TestLoopCancellationJoinsWatch(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	stream := watch.NewRaceFreeFake()
	opened := make(chan struct{})
	done := make(chan struct{})
	var c counters
	go func() {
		defer close(done)
		watchLoop(ctx, func(context.Context, metav1.ListOptions) (page, error) { return page{"1", ""}, nil },
			func(context.Context, metav1.ListOptions) (watch.Interface, error) { close(opened); return stream, nil }, false, &c)
	}()
	select {
	case <-opened:
	case <-time.After(time.Second):
		t.Fatal("watch did not open")
	}
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("watch worker did not join")
	}
	require.True(t, stream.IsStopped())
	require.Zero(t, c.failures.Load())
}

func TestListFailureRecordedAndRetryCancelable(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	var c counters
	watchLoop(ctx, func(context.Context, metav1.ListOptions) (page, error) { return page{}, errors.New("denied") },
		func(context.Context, metav1.ListOptions) (watch.Interface, error) {
			t.Fatal("must not open watch")
			return nil, nil
		}, false, &c)
	require.EqualValues(t, 1, c.failures.Load())
	require.Zero(t, c.lists.Load())
}
