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

type watchEmptyKeyOutcome struct {
	PointCreated        bool
	FromKeyCreated      bool
	EventWatchID        int64
	EventKeyMatches     bool
	EventValue          string
	EventHeaderGap      int64
	EventModRevisionGap int64
	PointCanceled       bool
	FromKeyCanceled     bool
	HeadersCanonical    bool
}

func TestWatchEmptyKeyDifferentialAgainstReferenceEtcd(t *testing.T) {
	reference := os.Getenv("REFERENCE_ETCD_ENDPOINT")
	if reference == "" {
		t.Skip("set REFERENCE_ETCD_ENDPOINT to run differential compatibility tests")
	}

	want := watchEmptyKeyOutcome{
		PointCreated: true, FromKeyCreated: true, EventWatchID: 802,
		EventKeyMatches: true, EventValue: "value", EventHeaderGap: 1, EventModRevisionGap: 1,
		PointCanceled: true, FromKeyCanceled: true, HeadersCanonical: true,
	}
	referenceOutcome := runWatchEmptyKeyScenario(t, reference, "reference")
	require.Equal(t, want, referenceOutcome)
	require.Equal(t, referenceOutcome, runWatchEmptyKeyScenario(t, compatEndpoint(t), "kubebrain"))
}

func runWatchEmptyKeyScenario(t *testing.T, endpoint, instance string) watchEmptyKeyOutcome {
	t.Helper()
	endpoint = strings.TrimPrefix(strings.TrimPrefix(endpoint, "http://"), "https://")
	conn, err := grpc.NewClient(endpoint, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, conn.Close()) })

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	kv := etcdserverpb.NewKVClient(conn)
	seedKey := []byte(fmt.Sprintf("/dbaas-watch-empty-key/%s/seed/%d", instance, time.Now().UnixNano()))
	eventKey := []byte(fmt.Sprintf("/dbaas-watch-empty-key/%s/event/%d", instance, time.Now().UnixNano()))
	seed, err := kv.Put(ctx, &etcdserverpb.PutRequest{Key: seedKey, Value: []byte("seed")})
	require.NoError(t, err)
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		_, _ = kv.DeleteRange(cleanupCtx, &etcdserverpb.DeleteRangeRequest{Key: seedKey})
		_, _ = kv.DeleteRange(cleanupCtx, &etcdserverpb.DeleteRangeRequest{Key: eventKey})
	})

	stream, err := etcdserverpb.NewWatchClient(conn).Watch(ctx)
	require.NoError(t, err)
	t.Cleanup(func() { _ = stream.CloseSend() })
	recv := func() *etcdserverpb.WatchResponse {
		response, recvErr := stream.Recv()
		require.NoError(t, recvErr)
		return response
	}
	create := func(id int64, rangeEnd []byte) *etcdserverpb.WatchResponse {
		require.NoError(t, stream.Send(&etcdserverpb.WatchRequest{RequestUnion: &etcdserverpb.WatchRequest_CreateRequest{
			CreateRequest: &etcdserverpb.WatchCreateRequest{WatchId: id, RangeEnd: rangeEnd},
		}}))
		return recv()
	}
	cancelWatch := func(id int64) *etcdserverpb.WatchResponse {
		require.NoError(t, stream.Send(&etcdserverpb.WatchRequest{RequestUnion: &etcdserverpb.WatchRequest_CancelRequest{
			CancelRequest: &etcdserverpb.WatchCancelRequest{WatchId: id},
		}}))
		return recv()
	}

	pointCreated := create(801, nil)
	fromKeyCreated := create(802, []byte{0})
	put, err := kv.Put(ctx, &etcdserverpb.PutRequest{Key: eventKey, Value: []byte("value")})
	require.NoError(t, err)
	eventResponse := recv()
	require.Len(t, eventResponse.Events, 1)
	event := eventResponse.Events[0]
	pointCanceled := cancelWatch(801)
	fromKeyCanceled := cancelWatch(802)

	headers := []*etcdserverpb.ResponseHeader{
		pointCreated.Header, fromKeyCreated.Header, eventResponse.Header, pointCanceled.Header, fromKeyCanceled.Header,
	}
	headersCanonical := true
	for _, header := range headers {
		headersCanonical = headersCanonical && header != nil && header.ClusterId == seed.Header.ClusterId &&
			header.MemberId == seed.Header.MemberId && header.RaftTerm > 0
	}
	return watchEmptyKeyOutcome{
		PointCreated:   pointCreated.Created && !pointCreated.Canceled && pointCreated.WatchId == 801,
		FromKeyCreated: fromKeyCreated.Created && !fromKeyCreated.Canceled && fromKeyCreated.WatchId == 802,
		EventWatchID:   eventResponse.WatchId, EventKeyMatches: string(event.GetKv().GetKey()) == string(eventKey),
		EventValue: string(event.GetKv().GetValue()), EventHeaderGap: eventResponse.GetHeader().GetRevision() - seed.Header.Revision,
		EventModRevisionGap: event.GetKv().GetModRevision() - seed.Header.Revision,
		PointCanceled:       pointCanceled.Canceled && !pointCanceled.Created && pointCanceled.WatchId == 801,
		FromKeyCanceled:     fromKeyCanceled.Canceled && !fromKeyCanceled.Created && fromKeyCanceled.WatchId == 802,
		HeadersCanonical:    headersCanonical && put.Header.Revision == seed.Header.Revision+1,
	}
}
