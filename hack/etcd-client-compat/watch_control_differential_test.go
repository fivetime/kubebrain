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
	WatchID      int64
	Created      bool
	Canceled     bool
	CancelReason string
	HeaderZero   bool
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
	require.Equal(t,
		runSingleWatchProgressScenario(t, reference),
		runSingleWatchProgressScenario(t, compatEndpoint()),
	)
	require.Equal(t,
		runWatchFilterEnumScenario(t, reference, "reference"),
		runWatchFilterEnumScenario(t, compatEndpoint(), "kubebrain"),
	)
	referenceFragments := runWatchFragmentScenario(t, reference, "reference")
	kubebrainFragments := runWatchFragmentScenario(t, compatEndpoint(), "kubebrain")
	require.Equal(t, referenceFragments, kubebrainFragments)
}

type watchFilterEnumOutcome struct {
	UnknownTypes   []int32
	DuplicateTypes []int32
}

func runWatchFilterEnumScenario(t *testing.T, endpoint, instance string) watchFilterEnumOutcome {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	conn, err := grpc.NewClient(endpoint, grpc.WithTransportCredentials(insecure.NewCredentials()))
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
	EventCounts    []int
	FragmentFlags  []bool
	PrevValueBytes []int
}

func runWatchFragmentScenario(t *testing.T, endpoint, instance string) watchFragmentOutcome {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	conn, err := grpc.NewClient(endpoint, grpc.WithTransportCredentials(insecure.NewCredentials()))
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

	_, err = kv.DeleteRange(ctx, &etcdserverpb.DeleteRangeRequest{
		Key: []byte(prefix), RangeEnd: []byte(clientv3.GetPrefixRangeEnd(prefix)), PrevKv: true,
	})
	require.NoError(t, err)

	var outcome watchFragmentOutcome
	for {
		resp, recvErr := stream.Recv()
		require.NoError(t, recvErr)
		require.NotEmpty(t, resp.Events)
		outcome.EventCounts = append(outcome.EventCounts, len(resp.Events))
		outcome.FragmentFlags = append(outcome.FragmentFlags, resp.Fragment)
		for _, event := range resp.Events {
			outcome.PrevValueBytes = append(outcome.PrevValueBytes, len(event.PrevKv.GetValue()))
		}
		if !resp.Fragment {
			break
		}
	}
	require.NoError(t, stream.CloseSend())
	return outcome
}

func runSingleWatchProgressScenario(t *testing.T, endpoint string) watchControlOutcome {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	conn, err := grpc.NewClient(endpoint, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, conn.Close()) })
	stream, err := etcdserverpb.NewWatchClient(conn).Watch(ctx)
	require.NoError(t, err)
	watchKey := []byte("/dbaas-watch-control/progress")
	require.NoError(t, stream.Send(&etcdserverpb.WatchRequest{
		RequestUnion: &etcdserverpb.WatchRequest_CreateRequest{CreateRequest: &etcdserverpb.WatchCreateRequest{
			Key: watchKey, WatchId: 51,
		}},
	}))
	created, err := stream.Recv()
	require.NoError(t, err)
	require.True(t, created.Created)
	_, err = etcdserverpb.NewKVClient(conn).Put(ctx, &etcdserverpb.PutRequest{Key: watchKey, Value: []byte("1")})
	require.NoError(t, err)
	events, err := stream.Recv()
	require.NoError(t, err)
	require.Len(t, events.Events, 1)
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		_, _ = etcdserverpb.NewKVClient(conn).DeleteRange(cleanupCtx, &etcdserverpb.DeleteRangeRequest{Key: watchKey})
	})
	require.NoError(t, stream.Send(&etcdserverpb.WatchRequest{
		RequestUnion: &etcdserverpb.WatchRequest_ProgressRequest{ProgressRequest: &etcdserverpb.WatchProgressRequest{}},
	}))
	progress, err := stream.Recv()
	require.NoError(t, err)
	require.NoError(t, stream.CloseSend())
	return watchControlOutcome{
		WatchID: progress.WatchId, Created: progress.Created,
		Canceled: progress.Canceled, CancelReason: progress.CancelReason,
		HeaderZero: progress.Header == nil || progress.Header.Revision == 0,
	}
}

func runWatchControlScenario(t *testing.T, endpoint string) []watchControlOutcome {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	conn, err := grpc.NewClient(endpoint, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, conn.Close()) })
	seedKey := []byte("/dbaas-watch-control/seed")
	_, err = etcdserverpb.NewKVClient(conn).Put(ctx, &etcdserverpb.PutRequest{Key: seedKey, Value: []byte("1")})
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
			HeaderZero: resp.Header == nil || resp.Header.Revision == 0,
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
