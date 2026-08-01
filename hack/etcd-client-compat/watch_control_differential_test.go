package compat

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type watchControlOutcome struct {
	WatchID           int64
	Created           bool
	Canceled          bool
	CancelReason      string
	HeaderMatchesSeed bool
}

func TestWatchControlDifferentialAgainstReferenceEtcd(t *testing.T) {
	reference := os.Getenv("REFERENCE_ETCD_ENDPOINT")
	if reference == "" {
		t.Skip("set REFERENCE_ETCD_ENDPOINT to run differential compatibility tests")
	}
	require.Equal(t,
		runWatchControlScenario(t, reference),
		runWatchControlScenario(t, compatEndpoint()),
	)
	referenceProgress := runSingleWatchProgressScenario(t, reference, "reference")
	require.Equal(t, watchProgressOutcome{
		Created: true, PutRevisionGap: 1, EventHeaderGap: 1, EventModRevisionGap: 1,
		EventValue: "1", ProgressWatchID: -1, ProgressHeaderGap: 1, ProgressEmpty: true,
	}, referenceProgress)
	require.Equal(t, referenceProgress, runSingleWatchProgressScenario(t, compatEndpoint(), "kubebrain"))
	require.Equal(t,
		runWatchFilterEnumScenario(t, reference, "reference"),
		runWatchFilterEnumScenario(t, compatEndpoint(), "kubebrain"),
	)
	referenceInvalid := runWatchInvalidControlScenario(t, reference, "reference")
	require.Equal(t, []watchControlOutcome{
		{WatchID: 0, Created: true, HeaderMatchesSeed: true},
		{WatchID: 0, Canceled: true, HeaderMatchesSeed: true},
		{WatchID: 404, Created: true, HeaderMatchesSeed: true},
	}, referenceInvalid)
	require.Equal(t, referenceInvalid, runWatchInvalidControlScenario(t, compatEndpoint(), "kubebrain"))
	referenceFragments := runWatchFragmentScenario(t, reference, "reference")
	require.Zero(t, referenceFragments.CreatedHeaderGap)
	require.Equal(t, int64(1), referenceFragments.DeleteRevisionGap)
	require.True(t, referenceFragments.FragmentHeadersAtDelete)
	require.True(t, referenceFragments.EventModsAtDelete)
	kubebrainFragments := runWatchFragmentScenario(t, compatEndpoint(), "kubebrain")
	require.Equal(t, referenceFragments, kubebrainFragments)
}

type watchFilterEnumOutcome struct {
	UnknownTypes   []int32
	DuplicateTypes []int32
}

func runWatchInvalidControlScenario(t *testing.T, endpoint, instance string) []watchControlOutcome {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	conn, err := grpc.NewClient(grpcTarget(endpoint), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, conn.Close()) })
	prefix := fmt.Sprintf("/dbaas-watch-invalid-control/%s/%d", instance, time.Now().UnixNano())
	seedKey := []byte(prefix + "/seed")
	key := []byte(prefix + "/live")
	kv := etcdserverpb.NewKVClient(conn)
	seed, err := kv.Put(ctx, &etcdserverpb.PutRequest{Key: seedKey, Value: []byte("seed")})
	require.NoError(t, err)
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		_, _ = kv.DeleteRange(cleanupCtx, &etcdserverpb.DeleteRangeRequest{
			Key: []byte(prefix), RangeEnd: []byte(clientv3.GetPrefixRangeEnd(prefix)),
		})
	})

	stream, err := etcdserverpb.NewWatchClient(conn).Watch(ctx)
	require.NoError(t, err)
	for _, request := range []*etcdserverpb.WatchRequest{
		{},
		{RequestUnion: &etcdserverpb.WatchRequest_CreateRequest{}},
		{RequestUnion: &etcdserverpb.WatchRequest_CancelRequest{}},
		{RequestUnion: &etcdserverpb.WatchRequest_ProgressRequest{}},
		{RequestUnion: &etcdserverpb.WatchRequest_CreateRequest{
			CreateRequest: &etcdserverpb.WatchCreateRequest{Key: key, WatchId: 404},
		}},
	} {
		require.NoError(t, stream.Send(request))
	}

	outcomes := make([]watchControlOutcome, 0, 3)
	for len(outcomes) < 3 {
		resp, recvErr := stream.Recv()
		require.NoError(t, recvErr)
		outcomes = append(outcomes, watchControlOutcome{
			WatchID: resp.WatchId, Created: resp.Created,
			Canceled: resp.Canceled, CancelReason: resp.CancelReason,
			HeaderMatchesSeed: resp.Header != nil && resp.Header.Revision == seed.Header.Revision,
		})
	}
	type receiveResult struct {
		response *etcdserverpb.WatchResponse
		err      error
	}
	pending := make(chan receiveResult, 1)
	go func() {
		response, recvErr := stream.Recv()
		pending <- receiveResult{response: response, err: recvErr}
	}()
	select {
	case extra := <-pending:
		require.Failf(t, "unexpected extra watch response", "response=%v error=%v", extra.response, extra.err)
	case <-time.After(150 * time.Millisecond):
	}
	cancel()
	select {
	case result := <-pending:
		require.Error(t, result.err)
	case <-time.After(time.Second):
		require.Fail(t, "watch receive did not stop after context cancellation")
	}
	_ = stream.CloseSend()
	return outcomes
}

func runWatchFilterEnumScenario(t *testing.T, endpoint, instance string) watchFilterEnumOutcome {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	conn, err := grpc.NewClient(grpcTarget(endpoint), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, conn.Close()) })
	kv := etcdserverpb.NewKVClient(conn)
	prefix := fmt.Sprintf("/dbaas-watch-filter-enum/%s/%d/", instance, time.Now().UnixNano())
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		_, _ = kv.DeleteRange(cleanupCtx, &etcdserverpb.DeleteRangeRequest{
			Key: []byte(prefix), RangeEnd: []byte(clientv3.GetPrefixRangeEnd(prefix)),
		})
	})

	run := func(suffix string, filters []etcdserverpb.WatchCreateRequest_FilterType, wantEvents int) []int32 {
		key := []byte(prefix + suffix)
		put, putErr := kv.Put(ctx, &etcdserverpb.PutRequest{Key: key, Value: []byte("value")})
		require.NoError(t, putErr)
		_, deleteErr := kv.DeleteRange(ctx, &etcdserverpb.DeleteRangeRequest{Key: key})
		require.NoError(t, deleteErr)

		stream, watchErr := etcdserverpb.NewWatchClient(conn).Watch(ctx)
		require.NoError(t, watchErr)
		require.NoError(t, stream.Send(&etcdserverpb.WatchRequest{
			RequestUnion: &etcdserverpb.WatchRequest_CreateRequest{CreateRequest: &etcdserverpb.WatchCreateRequest{
				Key: key, StartRevision: put.Header.Revision, Filters: filters,
			}},
		}))
		created, recvErr := stream.Recv()
		require.NoError(t, recvErr)
		require.True(t, created.Created)

		types := make([]int32, 0, wantEvents)
		for len(types) < wantEvents {
			response, eventErr := stream.Recv()
			require.NoError(t, eventErr)
			for _, event := range response.Events {
				types = append(types, int32(event.Type))
			}
		}
		require.Len(t, types, wantEvents)
		require.NoError(t, stream.CloseSend())
		return types
	}

	return watchFilterEnumOutcome{
		UnknownTypes: run("unknown",
			[]etcdserverpb.WatchCreateRequest_FilterType{etcdserverpb.WatchCreateRequest_FilterType(99)}, 2),
		DuplicateTypes: run("duplicate",
			[]etcdserverpb.WatchCreateRequest_FilterType{
				etcdserverpb.WatchCreateRequest_NOPUT,
				etcdserverpb.WatchCreateRequest_NOPUT,
			}, 1),
	}
}

type watchFragmentOutcome struct {
	CreatedHeaderGap        int64
	DeleteRevisionGap       int64
	FragmentHeadersAtDelete bool
	EventModsAtDelete       bool
	EventCounts             []int
	FragmentFlags           []bool
	PrevValueBytes          []int
}

type watchProgressOutcome struct {
	Created             bool
	CreatedHeaderGap    int64
	PutRevisionGap      int64
	EventHeaderGap      int64
	EventModRevisionGap int64
	EventValue          string
	ProgressWatchID     int64
	ProgressHeaderGap   int64
	ProgressEmpty       bool
}

func runWatchFragmentScenario(t *testing.T, endpoint, instance string) watchFragmentOutcome {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	conn, err := grpc.NewClient(grpcTarget(endpoint), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, conn.Close()) })

	prefix := fmt.Sprintf("/dbaas-watch-fragment/%s/%d/", instance, time.Now().UnixNano())
	keys := []string{prefix + "a", prefix + "b"}
	value := make([]byte, 800*1024)
	kv := etcdserverpb.NewKVClient(conn)
	var revision int64
	for _, key := range keys {
		resp, putErr := kv.Put(ctx, &etcdserverpb.PutRequest{Key: []byte(key), Value: value})
		require.NoError(t, putErr)
		revision = resp.Header.Revision
	}
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		_, _ = kv.DeleteRange(cleanupCtx, &etcdserverpb.DeleteRangeRequest{
			Key: []byte(prefix), RangeEnd: []byte(clientv3.GetPrefixRangeEnd(prefix)),
		})
	})

	stream, err := etcdserverpb.NewWatchClient(conn).Watch(ctx)
	require.NoError(t, err)
	require.NoError(t, stream.Send(&etcdserverpb.WatchRequest{
		RequestUnion: &etcdserverpb.WatchRequest_CreateRequest{CreateRequest: &etcdserverpb.WatchCreateRequest{
			Key: []byte(prefix), RangeEnd: []byte(clientv3.GetPrefixRangeEnd(prefix)),
			StartRevision: revision + 1, PrevKv: true, Fragment: true,
		}},
	}))
	created, err := stream.Recv()
	require.NoError(t, err)
	require.True(t, created.Created)
	require.NotNil(t, created.Header)

	deleted, err := kv.DeleteRange(ctx, &etcdserverpb.DeleteRangeRequest{
		Key: []byte(prefix), RangeEnd: []byte(clientv3.GetPrefixRangeEnd(prefix)), PrevKv: true,
	})
	require.NoError(t, err)
	require.NotNil(t, deleted.Header)

	outcome := watchFragmentOutcome{
		CreatedHeaderGap:        created.Header.Revision - revision,
		DeleteRevisionGap:       deleted.Header.Revision - revision,
		FragmentHeadersAtDelete: true,
		EventModsAtDelete:       true,
	}
	for {
		resp, recvErr := stream.Recv()
		require.NoError(t, recvErr)
		require.NotEmpty(t, resp.Events)
		outcome.EventCounts = append(outcome.EventCounts, len(resp.Events))
		outcome.FragmentFlags = append(outcome.FragmentFlags, resp.Fragment)
		outcome.FragmentHeadersAtDelete = outcome.FragmentHeadersAtDelete &&
			resp.Header != nil && resp.Header.Revision == deleted.Header.Revision
		for _, event := range resp.Events {
			outcome.PrevValueBytes = append(outcome.PrevValueBytes, len(event.PrevKv.GetValue()))
			outcome.EventModsAtDelete = outcome.EventModsAtDelete &&
				event.Kv != nil && event.Kv.ModRevision == deleted.Header.Revision
		}
		if !resp.Fragment {
			break
		}
	}
	require.NoError(t, stream.CloseSend())
	return outcome
}

func runSingleWatchProgressScenario(t *testing.T, endpoint, instance string) watchProgressOutcome {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	conn, err := grpc.NewClient(grpcTarget(endpoint), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, conn.Close()) })
	kv := etcdserverpb.NewKVClient(conn)
	watchKey := []byte(fmt.Sprintf("/dbaas-watch-control/progress/%s/%d", instance, time.Now().UnixNano()))
	seed, err := kv.Put(ctx, &etcdserverpb.PutRequest{Key: watchKey, Value: []byte("seed")})
	require.NoError(t, err)
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		_, _ = kv.DeleteRange(cleanupCtx, &etcdserverpb.DeleteRangeRequest{Key: watchKey})
	})
	stream, err := etcdserverpb.NewWatchClient(conn).Watch(ctx)
	require.NoError(t, err)
	require.NoError(t, stream.Send(&etcdserverpb.WatchRequest{
		RequestUnion: &etcdserverpb.WatchRequest_CreateRequest{CreateRequest: &etcdserverpb.WatchCreateRequest{
			Key: watchKey, WatchId: 51,
		}},
	}))
	created, err := stream.Recv()
	require.NoError(t, err)
	require.True(t, created.Created)
	put, err := kv.Put(ctx, &etcdserverpb.PutRequest{Key: watchKey, Value: []byte("1")})
	require.NoError(t, err)
	events, err := stream.Recv()
	require.NoError(t, err)
	require.Len(t, events.Events, 1)
	require.NoError(t, stream.Send(&etcdserverpb.WatchRequest{
		RequestUnion: &etcdserverpb.WatchRequest_ProgressRequest{ProgressRequest: &etcdserverpb.WatchProgressRequest{}},
	}))
	progress, err := stream.Recv()
	require.NoError(t, err)
	require.NoError(t, stream.CloseSend())
	for name, header := range map[string]*etcdserverpb.ResponseHeader{
		"seed": seed.Header, "created": created.Header, "put": put.Header,
		"event": events.Header, "progress": progress.Header,
	} {
		require.NotNil(t, header, name)
	}
	baseRevision := seed.Header.Revision
	return watchProgressOutcome{
		Created:             created.Created && created.WatchId == 51 && !created.Canceled,
		CreatedHeaderGap:    created.Header.Revision - baseRevision,
		PutRevisionGap:      put.Header.Revision - baseRevision,
		EventHeaderGap:      events.Header.Revision - baseRevision,
		EventModRevisionGap: events.Events[0].Kv.ModRevision - baseRevision,
		EventValue:          string(events.Events[0].Kv.Value),
		ProgressWatchID:     progress.WatchId,
		ProgressHeaderGap:   progress.Header.Revision - baseRevision,
		ProgressEmpty:       !progress.Created && !progress.Canceled && len(progress.Events) == 0 && progress.CancelReason == "",
	}
}

func runWatchControlScenario(t *testing.T, endpoint string) []watchControlOutcome {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	conn, err := grpc.NewClient(grpcTarget(endpoint), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, conn.Close()) })
	seedKey := []byte("/dbaas-watch-control/seed")
	seed, err := etcdserverpb.NewKVClient(conn).Put(ctx, &etcdserverpb.PutRequest{Key: seedKey, Value: []byte("1")})
	require.NoError(t, err)
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		_, _ = etcdserverpb.NewKVClient(conn).DeleteRange(cleanupCtx, &etcdserverpb.DeleteRangeRequest{Key: seedKey})
	})
	stream, err := etcdserverpb.NewWatchClient(conn).Watch(ctx)
	require.NoError(t, err)

	create := func(key string, id, revision int64) {
		require.NoError(t, stream.Send(&etcdserverpb.WatchRequest{
			RequestUnion: &etcdserverpb.WatchRequest_CreateRequest{CreateRequest: &etcdserverpb.WatchCreateRequest{
				Key: []byte(key), WatchId: id, StartRevision: revision,
			}},
		}))
	}
	recv := func() watchControlOutcome {
		resp, err := stream.Recv()
		require.NoError(t, err)
		return watchControlOutcome{
			WatchID: resp.WatchId, Created: resp.Created,
			Canceled: resp.Canceled, CancelReason: resp.CancelReason,
			HeaderMatchesSeed: resp.Header != nil && resp.Header.Revision == seed.Header.Revision,
		}
	}

	create("/dbaas-watch-control/a", 42, 0)
	outcomes := []watchControlOutcome{recv()}
	create("/dbaas-watch-control/duplicate", 42, 0)
	outcomes = append(outcomes, recv())
	require.NoError(t, stream.Send(&etcdserverpb.WatchRequest{
		RequestUnion: &etcdserverpb.WatchRequest_CancelRequest{CancelRequest: &etcdserverpb.WatchCancelRequest{WatchId: 999}},
	}))
	create("/dbaas-watch-control/b", 43, 0)
	outcomes = append(outcomes, recv())
	create("/dbaas-watch-control/negative", 44, -1)
	outcomes = append(outcomes, recv())
	create("/dbaas-watch-control/c", 45, 0)
	outcomes = append(outcomes, recv())
	require.NoError(t, stream.Send(&etcdserverpb.WatchRequest{
		RequestUnion: &etcdserverpb.WatchRequest_CancelRequest{CancelRequest: &etcdserverpb.WatchCancelRequest{WatchId: 42}},
	}))
	outcomes = append(outcomes, recv())

	create("/dbaas-watch-control/auto-first", 0, 0)
	outcomes = append(outcomes, recv())
	require.NoError(t, stream.Send(&etcdserverpb.WatchRequest{
		RequestUnion: &etcdserverpb.WatchRequest_CancelRequest{CancelRequest: &etcdserverpb.WatchCancelRequest{WatchId: 0}},
	}))
	outcomes = append(outcomes, recv())
	create("/dbaas-watch-control/auto-second", 0, 0)
	outcomes = append(outcomes, recv())

	require.NoError(t, stream.CloseSend())
	return outcomes
}
