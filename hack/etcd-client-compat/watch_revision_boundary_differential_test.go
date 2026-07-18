package compat

import (
	"context"
	"fmt"
	"math"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type watchRevisionBoundaryOutcome struct {
	Name                     string
	Created                  bool
	CreatedHeaderAtBase      bool
	EventValues              []string
	EventAtWriteRevision     bool
	Canceled                 bool
	CancelReason             string
	CancelHeaderAtOrAfterPut bool
}

func TestWatchRevisionBoundaryDifferentialAgainstReferenceEtcd(t *testing.T) {
	reference := os.Getenv("REFERENCE_ETCD_ENDPOINT")
	if reference == "" {
		t.Skip("set REFERENCE_ETCD_ENDPOINT to run differential compatibility tests")
	}

	want := []watchRevisionBoundaryOutcome{
		{
			Name:                 "latest-zero",
			Created:              true,
			CreatedHeaderAtBase:  true,
			EventValues:          []string{"after-create"},
			EventAtWriteRevision: true,
		},
		{
			Name:                 "historical-current",
			Created:              true,
			CreatedHeaderAtBase:  true,
			EventValues:          []string{"seed"},
			EventAtWriteRevision: true,
		},
		{
			Name:                 "future-next",
			Created:              true,
			CreatedHeaderAtBase:  true,
			EventValues:          []string{"future"},
			EventAtWriteRevision: true,
		},
		{
			Name:                     "maximum",
			Created:                  true,
			CreatedHeaderAtBase:      true,
			EventValues:              []string{},
			Canceled:                 true,
			CancelHeaderAtOrAfterPut: true,
		},
	}
	referenceOutcomes := runWatchRevisionBoundaryScenario(t, reference, "etcd")
	require.Equal(t, want, referenceOutcomes)
	require.Equal(t, referenceOutcomes, runWatchRevisionBoundaryScenario(t, compatEndpoint(), "kubebrain"))
}

func runWatchRevisionBoundaryScenario(
	t *testing.T,
	endpoint string,
	instance string,
) []watchRevisionBoundaryOutcome {
	t.Helper()
	endpoint = strings.TrimPrefix(strings.TrimPrefix(endpoint, "http://"), "https://")
	conn, err := grpc.NewClient(endpoint, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, conn.Close()) })
	kv := etcdserverpb.NewKVClient(conn)
	watch := etcdserverpb.NewWatchClient(conn)
	prefix := fmt.Sprintf("/dbaas-watch-revision/%s/%d/", instance, time.Now().UnixNano())
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		_, _ = kv.DeleteRange(cleanupCtx, &etcdserverpb.DeleteRangeRequest{
			Key: []byte(prefix), RangeEnd: []byte(prefix + "0"),
		})
	})

	var outcomes []watchRevisionBoundaryOutcome
	t.Run(instance+"/latest-zero", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		key := []byte(prefix + "latest")
		base, rangeErr := kv.Range(ctx, &etcdserverpb.RangeRequest{Key: key})
		require.NoError(t, rangeErr)
		stream, streamErr := watch.Watch(ctx)
		require.NoError(t, streamErr)
		sendWatchCreate(t, stream, key, 301, 0)
		created := recvWatchResponse(t, stream)
		put, putErr := kv.Put(ctx, &etcdserverpb.PutRequest{Key: key, Value: []byte("after-create")})
		require.NoError(t, putErr)
		event := recvWatchResponse(t, stream)
		outcomes = append(outcomes, normalizeWatchRevisionOutcome("latest-zero", created, event, base.Header.Revision, put.Header.Revision))
		require.NoError(t, stream.CloseSend())
	})

	t.Run(instance+"/historical-current", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		key := []byte(prefix + "historical")
		put, putErr := kv.Put(ctx, &etcdserverpb.PutRequest{Key: key, Value: []byte("seed")})
		require.NoError(t, putErr)
		stream, streamErr := watch.Watch(ctx)
		require.NoError(t, streamErr)
		sendWatchCreate(t, stream, key, 302, put.Header.Revision)
		created := recvWatchResponse(t, stream)
		event := recvWatchResponse(t, stream)
		outcomes = append(outcomes, normalizeWatchRevisionOutcome(
			"historical-current", created, event, put.Header.Revision, put.Header.Revision,
		))
		require.NoError(t, stream.CloseSend())
	})

	t.Run(instance+"/future-next", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		key := []byte(prefix + "future")
		base, rangeErr := kv.Range(ctx, &etcdserverpb.RangeRequest{Key: key})
		require.NoError(t, rangeErr)
		stream, streamErr := watch.Watch(ctx)
		require.NoError(t, streamErr)
		sendWatchCreate(t, stream, key, 303, base.Header.Revision+1)
		created := recvWatchResponse(t, stream)
		put, putErr := kv.Put(ctx, &etcdserverpb.PutRequest{Key: key, Value: []byte("future")})
		require.NoError(t, putErr)
		event := recvWatchResponse(t, stream)
		outcomes = append(outcomes, normalizeWatchRevisionOutcome(
			"future-next", created, event, base.Header.Revision, put.Header.Revision,
		))
		require.NoError(t, stream.CloseSend())
	})

	t.Run(instance+"/maximum", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		key := []byte(prefix + "maximum")
		base, rangeErr := kv.Range(ctx, &etcdserverpb.RangeRequest{Key: key})
		require.NoError(t, rangeErr)
		stream, streamErr := watch.Watch(ctx)
		require.NoError(t, streamErr)
		sendWatchCreate(t, stream, key, 304, math.MaxInt64)
		created := recvWatchResponse(t, stream)
		put, putErr := kv.Put(ctx, &etcdserverpb.PutRequest{Key: key, Value: []byte("below-maximum")})
		require.NoError(t, putErr)
		require.NoError(t, stream.Send(&etcdserverpb.WatchRequest{
			RequestUnion: &etcdserverpb.WatchRequest_CancelRequest{
				CancelRequest: &etcdserverpb.WatchCancelRequest{WatchId: 304},
			},
		}))
		canceled := recvWatchResponse(t, stream)
		outcomes = append(outcomes, watchRevisionBoundaryOutcome{
			Name:                     "maximum",
			Created:                  created.Created,
			CreatedHeaderAtBase:      created.Header.Revision >= base.Header.Revision,
			EventValues:              []string{},
			Canceled:                 canceled.Canceled,
			CancelReason:             canceled.CancelReason,
			CancelHeaderAtOrAfterPut: canceled.Header.Revision >= put.Header.Revision,
		})
		require.NoError(t, stream.CloseSend())
	})
	return outcomes
}

func sendWatchCreate(
	t *testing.T,
	stream etcdserverpb.Watch_WatchClient,
	key []byte,
	id int64,
	revision int64,
) {
	t.Helper()
	require.NoError(t, stream.Send(&etcdserverpb.WatchRequest{
		RequestUnion: &etcdserverpb.WatchRequest_CreateRequest{
			CreateRequest: &etcdserverpb.WatchCreateRequest{
				Key: key, WatchId: id, StartRevision: revision,
			},
		},
	}))
}

func recvWatchResponse(t *testing.T, stream etcdserverpb.Watch_WatchClient) *etcdserverpb.WatchResponse {
	t.Helper()
	resp, err := stream.Recv()
	require.NoError(t, err)
	return resp
}

func normalizeWatchRevisionOutcome(
	name string,
	created *etcdserverpb.WatchResponse,
	event *etcdserverpb.WatchResponse,
	baseRevision int64,
	writeRevision int64,
) watchRevisionBoundaryOutcome {
	values := make([]string, 0, len(event.Events))
	eventAtWriteRevision := len(event.Events) > 0
	for _, item := range event.Events {
		values = append(values, string(item.Kv.Value))
		eventAtWriteRevision = eventAtWriteRevision && item.Kv.ModRevision == writeRevision
	}
	return watchRevisionBoundaryOutcome{
		Name:                 name,
		Created:              created.Created,
		CreatedHeaderAtBase:  created.Header.Revision >= baseRevision,
		EventValues:          values,
		EventAtWriteRevision: eventAtWriteRevision,
	}
}
