package compat

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type watchPeriodicMultiOutcome struct {
	SyncedCreatedCanonical bool
	FutureCreatedCanonical bool
	CreatedHeaderGaps      []int64
	ProgressWatchID        int64
	ProgressHeaderGap      int64
	ProgressCanonical      bool
	FutureResponseAbsent   bool
}

func TestWatchPeriodicProgressIgnoresFutureSiblingDifferentialAgainstReferenceEtcd(t *testing.T) {
	reference := os.Getenv("REFERENCE_ETCD_ENDPOINT")
	if reference == "" {
		t.Skip("set REFERENCE_ETCD_ENDPOINT to run differential compatibility tests")
	}

	want := watchPeriodicMultiOutcome{
		SyncedCreatedCanonical: true, FutureCreatedCanonical: true, CreatedHeaderGaps: []int64{0, 0},
		ProgressWatchID: 909, ProgressCanonical: true, FutureResponseAbsent: true,
	}
	referenceOutcome := runWatchPeriodicMultiScenario(t, reference, "reference")
	require.Equal(t, want, referenceOutcome)
	require.Equal(t, referenceOutcome, runWatchPeriodicMultiScenario(t, compatEndpoint(t), "kubebrain"))
}

func runWatchPeriodicMultiScenario(t *testing.T, endpoint, instance string) watchPeriodicMultiOutcome {
	t.Helper()
	endpoint = strings.TrimPrefix(strings.TrimPrefix(endpoint, "http://"), "https://")
	conn, err := grpc.NewClient(endpoint, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, conn.Close()) })
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	t.Cleanup(cancel)
	kv := etcdserverpb.NewKVClient(conn)
	prefix := fmt.Sprintf("/dbaas-watch-periodic-multi/%s/%d/", instance, time.Now().UnixNano())
	base, err := kv.Range(ctx, &etcdserverpb.RangeRequest{Key: []byte(prefix)})
	require.NoError(t, err)
	require.NotNil(t, base.Header)

	stream, err := etcdserverpb.NewWatchClient(conn).Watch(ctx)
	require.NoError(t, err)
	t.Cleanup(func() { _ = stream.CloseSend() })
	create := func(request *etcdserverpb.WatchCreateRequest) *etcdserverpb.WatchResponse {
		require.NoError(t, stream.Send(&etcdserverpb.WatchRequest{
			RequestUnion: &etcdserverpb.WatchRequest_CreateRequest{CreateRequest: request},
		}))
		return recvWatchResponse(t, stream)
	}
	syncedCreated := create(&etcdserverpb.WatchCreateRequest{
		Key: []byte(prefix + "synced"), WatchId: 909, ProgressNotify: true,
	})
	futureCreated := create(&etcdserverpb.WatchCreateRequest{
		Key: []byte(prefix + "future"), WatchId: 910, StartRevision: base.Header.Revision + 2, ProgressNotify: true,
	})
	progress := recvWatchResponse(t, stream)

	type receiveResult struct {
		response *etcdserverpb.WatchResponse
		err      error
	}
	pending := make(chan receiveResult, 1)
	go func() {
		response, recvErr := stream.Recv()
		pending <- receiveResult{response: response, err: recvErr}
	}()
	futureAbsent := false
	select {
	case extra := <-pending:
		require.Failf(t, "future sibling emitted periodic progress", "response=%v error=%v", extra.response, extra.err)
	case <-time.After(300 * time.Millisecond):
		futureAbsent = true
	}
	baseRevision := base.Header.Revision
	return watchPeriodicMultiOutcome{
		SyncedCreatedCanonical: canonicalWatchControlResponse(syncedCreated, true, 909),
		FutureCreatedCanonical: canonicalWatchControlResponse(futureCreated, true, 910),
		CreatedHeaderGaps: []int64{
			syncedCreated.Header.Revision - baseRevision, futureCreated.Header.Revision - baseRevision,
		},
		ProgressWatchID: progress.WatchId, ProgressHeaderGap: progress.Header.Revision - baseRevision,
		ProgressCanonical:    canonicalWatchControlResponse(progress, false, 909),
		FutureResponseAbsent: futureAbsent,
	}
}
