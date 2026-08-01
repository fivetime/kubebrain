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

type watchFilterProgressOutcome struct {
	Created             bool
	CreatedHeaderGap    int64
	PutRevisionGap      int64
	SuppressedBeforeAck bool
	ProgressWatchID     int64
	ProgressHeaderGap   int64
	ProgressEmpty       bool
}

func TestWatchFilterProgressDifferentialAgainstReferenceEtcd(t *testing.T) {
	reference := os.Getenv("REFERENCE_ETCD_ENDPOINT")
	if reference == "" {
		t.Skip("set REFERENCE_ETCD_ENDPOINT to run differential compatibility tests")
	}

	want := watchFilterProgressOutcome{
		Created: true, PutRevisionGap: 1, SuppressedBeforeAck: true,
		ProgressWatchID: -1, ProgressHeaderGap: 1, ProgressEmpty: true,
	}
	referenceOutcome := runWatchFilterProgressScenario(t, reference, "reference")
	require.Equal(t, want, referenceOutcome)
	require.Equal(t, referenceOutcome, runWatchFilterProgressScenario(t, compatEndpoint(), "kubebrain"))
}

func runWatchFilterProgressScenario(t *testing.T, endpoint, instance string) watchFilterProgressOutcome {
	t.Helper()
	endpoint = strings.TrimPrefix(strings.TrimPrefix(endpoint, "http://"), "https://")
	conn, err := grpc.NewClient(endpoint, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, conn.Close()) })
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	t.Cleanup(cancel)
	kv := etcdserverpb.NewKVClient(conn)
	key := []byte(fmt.Sprintf("/dbaas-watch-filter-progress/%s/%d", instance, time.Now().UnixNano()))
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
	require.NoError(t, stream.Send(&etcdserverpb.WatchRequest{
		RequestUnion: &etcdserverpb.WatchRequest_CreateRequest{CreateRequest: &etcdserverpb.WatchCreateRequest{
			Key: key, WatchId: 901, Filters: []etcdserverpb.WatchCreateRequest_FilterType{etcdserverpb.WatchCreateRequest_NOPUT},
		}},
	}))
	created := recvWatchResponse(t, stream)
	put, err := kv.Put(ctx, &etcdserverpb.PutRequest{Key: key, Value: []byte("filtered")})
	require.NoError(t, err)

	type receiveResult struct {
		response *etcdserverpb.WatchResponse
		err      error
	}
	pending := make(chan receiveResult, 1)
	go func() {
		response, recvErr := stream.Recv()
		pending <- receiveResult{response: response, err: recvErr}
	}()
	suppressed := false
	select {
	case early := <-pending:
		require.Failf(t, "filtered PUT emitted a response", "response=%v error=%v", early.response, early.err)
	case <-time.After(150 * time.Millisecond):
		suppressed = true
	}
	require.NoError(t, stream.Send(&etcdserverpb.WatchRequest{
		RequestUnion: &etcdserverpb.WatchRequest_ProgressRequest{ProgressRequest: &etcdserverpb.WatchProgressRequest{}},
	}))
	progressResult := <-pending
	require.NoError(t, progressResult.err)
	progress := progressResult.response
	for name, header := range map[string]*etcdserverpb.ResponseHeader{
		"seed": seed.Header, "created": created.Header, "put": put.Header, "progress": progress.Header,
	} {
		require.NotNil(t, header, name)
	}
	baseRevision := seed.Header.Revision
	return watchFilterProgressOutcome{
		Created:             created.Created && !created.Canceled && created.WatchId == 901,
		CreatedHeaderGap:    created.Header.Revision - baseRevision,
		PutRevisionGap:      put.Header.Revision - baseRevision,
		SuppressedBeforeAck: suppressed,
		ProgressWatchID:     progress.WatchId,
		ProgressHeaderGap:   progress.Header.Revision - baseRevision,
		ProgressEmpty:       !progress.Created && !progress.Canceled && len(progress.Events) == 0,
	}
}
