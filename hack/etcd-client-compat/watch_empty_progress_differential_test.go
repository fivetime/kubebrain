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

type watchEmptyProgressOutcome struct {
	EmptyProgressSilent bool
	Created             bool
	CreatedHeaderGap    int64
	ProgressWatchID     int64
	ProgressHeaderGap   int64
	ProgressEmpty       bool
}

func TestWatchEmptyProgressDifferentialAgainstReferenceEtcd(t *testing.T) {
	reference := os.Getenv("REFERENCE_ETCD_ENDPOINT")
	if reference == "" {
		t.Skip("set REFERENCE_ETCD_ENDPOINT to run differential compatibility tests")
	}

	want := watchEmptyProgressOutcome{
		EmptyProgressSilent: true,
		Created:             true,
		ProgressWatchID:     -1,
		ProgressEmpty:       true,
	}
	referenceOutcome := runWatchEmptyProgressScenario(t, reference, "reference")
	require.Equal(t, want, referenceOutcome)
	require.Equal(t, referenceOutcome, runWatchEmptyProgressScenario(t, compatEndpoint(t), "kubebrain"))
}

func runWatchEmptyProgressScenario(t *testing.T, endpoint, instance string) watchEmptyProgressOutcome {
	t.Helper()
	endpoint = strings.TrimPrefix(strings.TrimPrefix(endpoint, "http://"), "https://")
	conn, err := grpc.NewClient(endpoint, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, conn.Close()) })
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	t.Cleanup(cancel)
	kv := etcdserverpb.NewKVClient(conn)
	key := []byte(fmt.Sprintf("/dbaas-watch-empty-progress/%s/%d", instance, time.Now().UnixNano()))
	seed, err := kv.Put(ctx, &etcdserverpb.PutRequest{Key: key, Value: []byte("seed")})
	require.NoError(t, err)
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		_, _ = kv.DeleteRange(cleanupCtx, &etcdserverpb.DeleteRangeRequest{Key: key})
	})

	stream, err := etcdserverpb.NewWatchClient(conn).Watch(ctx)
	require.NoError(t, err)
	t.Cleanup(func() { _ = stream.CloseSend() })
	require.NoError(t, stream.Send(progressWatchRequest()))
	type receiveResult struct {
		response *etcdserverpb.WatchResponse
		err      error
	}
	pending := make(chan receiveResult, 1)
	go func() {
		response, recvErr := stream.Recv()
		pending <- receiveResult{response: response, err: recvErr}
	}()
	silent := false
	select {
	case early := <-pending:
		require.Failf(t, "empty stream emitted progress", "response=%v error=%v", early.response, early.err)
	case <-time.After(150 * time.Millisecond):
		silent = true
	}

	require.NoError(t, stream.Send(&etcdserverpb.WatchRequest{
		RequestUnion: &etcdserverpb.WatchRequest_CreateRequest{CreateRequest: &etcdserverpb.WatchCreateRequest{
			Key: key, WatchId: 902,
		}},
	}))
	createdResult := <-pending
	require.NoError(t, createdResult.err)
	created := createdResult.response
	require.NoError(t, stream.Send(progressWatchRequest()))
	progress := recvWatchResponse(t, stream)
	for name, header := range map[string]*etcdserverpb.ResponseHeader{
		"seed": seed.Header, "created": created.Header, "progress": progress.Header,
	} {
		require.NotNil(t, header, name)
	}
	baseRevision := seed.Header.Revision
	return watchEmptyProgressOutcome{
		EmptyProgressSilent: silent,
		Created:             created.Created && !created.Canceled && created.WatchId == 902,
		CreatedHeaderGap:    created.Header.Revision - baseRevision,
		ProgressWatchID:     progress.WatchId,
		ProgressHeaderGap:   progress.Header.Revision - baseRevision,
		ProgressEmpty:       !progress.Created && !progress.Canceled && len(progress.Events) == 0,
	}
}

func progressWatchRequest() *etcdserverpb.WatchRequest {
	return &etcdserverpb.WatchRequest{
		RequestUnion: &etcdserverpb.WatchRequest_ProgressRequest{ProgressRequest: &etcdserverpb.WatchProgressRequest{}},
	}
}
