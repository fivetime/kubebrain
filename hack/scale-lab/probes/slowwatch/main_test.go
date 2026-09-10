package main

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
)

func batch(revisions ...int64) clientv3.WatchResponse {
	r := clientv3.WatchResponse{}
	for _, rev := range revisions {
		r.Events = append(r.Events, &clientv3.Event{Kv: &mvccpb.KeyValue{ModRevision: rev}})
	}
	return r
}

func TestObservedOrderingAllowsTransactionRevisionAndUnrelatedGaps(t *testing.T) {
	var s observations
	require.NoError(t, s.record(batch(4, 4, 9)))
	require.EqualValues(t, 3, s.events)
	require.EqualValues(t, 1, s.batches)
	require.ErrorContains(t, s.record(batch(8)), "regression")
}

func TestRejectMalformedAndCanceledResponses(t *testing.T) {
	for _, response := range []clientv3.WatchResponse{batch(0), {Canceled: true}, {CompactRevision: 3, Canceled: true}, {Events: []*clientv3.Event{nil}}} {
		var s observations
		require.Error(t, s.record(response))
	}
}

func TestClosedWatchIsNotSuccess(t *testing.T) {
	ch := make(chan clientv3.WatchResponse)
	close(ch)
	require.ErrorContains(t, consume(context.Background(), ch, 0, &observations{}), "closed before deadline")
}

func TestDelayIsCanceledPromptly(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	ch := make(chan clientv3.WatchResponse, 1)
	ch <- batch(1)
	started := time.Now()
	require.ErrorIs(t, consume(ctx, ch, time.Hour, &observations{}), context.DeadlineExceeded)
	require.Less(t, time.Since(started), time.Second)
}
