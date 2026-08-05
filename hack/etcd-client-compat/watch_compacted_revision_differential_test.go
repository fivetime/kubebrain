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

type watchCompactedRevisionOutcome struct {
	FirstPutGap          int64
	SecondPutGap         int64
	CompactionHeaderGap  int64
	PostCompactPutGap    int64
	OldCreatedHeaderGap  int64
	OldCreatedEnvelope   watchControlOutcome
	CancelHeaderZero     bool
	CanceledEnvelope     watchControlOutcome
	NextCreatedHeaderGap int64
	NextCreatedEnvelope  watchControlOutcome
	FinalPutGap          int64
	EventHeaderGap       int64
	EventModRevisionGap  int64
	EventValue           string
}

func TestWatchCompactedRevisionDifferentialAgainstReferenceEtcd(t *testing.T) {
	reference := os.Getenv("REFERENCE_ETCD_ENDPOINT")
	if reference == "" {
		t.Skip("set REFERENCE_ETCD_ENDPOINT to run differential compatibility tests")
	}

	want := watchCompactedRevisionOutcome{
		FirstPutGap: 1, SecondPutGap: 2, CompactionHeaderGap: 2, PostCompactPutGap: 3,
		OldCreatedHeaderGap:  3,
		OldCreatedEnvelope:   expectedCompactedWatchEnvelope(707, true, false, false, 0),
		CancelHeaderZero:     true,
		CanceledEnvelope:     expectedCompactedWatchEnvelope(707, false, true, true, 2),
		NextCreatedHeaderGap: 3,
		NextCreatedEnvelope:  expectedCompactedWatchEnvelope(708, true, false, false, 0),
		FinalPutGap:          4, EventHeaderGap: 4, EventModRevisionGap: 4, EventValue: "v4",
	}
	referenceOutcome := runWatchCompactedRevisionScenario(t, reference, "reference")
	require.Equal(t, want, referenceOutcome)
	require.Equal(t, referenceOutcome, runWatchCompactedRevisionScenario(t, compatEndpoint(t), "kubebrain"))
}

func expectedCompactedWatchEnvelope(
	id int64,
	created bool,
	canceled bool,
	compactRevisionSet bool,
	compactRevisionGap int64,
) watchControlOutcome {
	return watchControlOutcome{
		WatchID: id, Created: created, Canceled: canceled,
		HeaderIdentitySet: true, HeaderClusterMatch: true, HeaderMemberMatch: true,
		HeaderTermPositive: true, CompactRevisionSet: compactRevisionSet,
		CompactRevisionGap: compactRevisionGap, EnvelopeObserved: true,
	}
}

func runWatchCompactedRevisionScenario(t *testing.T, endpoint, instance string) watchCompactedRevisionOutcome {
	t.Helper()
	endpoint = grpcTarget(endpoint)
	conn, err := grpc.NewClient(endpoint, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, conn.Close()) })
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	t.Cleanup(cancel)
	kv := etcdserverpb.NewKVClient(conn)
	prefix := fmt.Sprintf("/dbaas-watch-compacted/%s/%d/", instance, time.Now().UnixNano())
	seedKey := []byte(prefix + "seed")
	key := []byte(prefix + "key")
	seed, err := kv.Put(ctx, &etcdserverpb.PutRequest{Key: seedKey, Value: []byte("seed")})
	require.NoError(t, err)
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		_, _ = kv.DeleteRange(cleanupCtx, &etcdserverpb.DeleteRangeRequest{
			Key: []byte(prefix), RangeEnd: []byte(clientv3.GetPrefixRangeEnd(prefix)),
		})
	})
	put1, err := kv.Put(ctx, &etcdserverpb.PutRequest{Key: key, Value: []byte("v1")})
	require.NoError(t, err)
	put2, err := kv.Put(ctx, &etcdserverpb.PutRequest{Key: key, Value: []byte("v2")})
	require.NoError(t, err)
	compacted, err := kv.Compact(ctx, &etcdserverpb.CompactionRequest{Revision: put2.Header.Revision})
	require.NoError(t, err)
	put3, err := kv.Put(ctx, &etcdserverpb.PutRequest{Key: key, Value: []byte("v3")})
	require.NoError(t, err)

	stream, err := etcdserverpb.NewWatchClient(conn).Watch(ctx)
	require.NoError(t, err)
	t.Cleanup(func() { _ = stream.CloseSend() })
	sendWatchCreate(t, stream, key, 707, put1.Header.Revision)
	oldCreated := recvWatchResponse(t, stream)
	canceled := recvWatchResponse(t, stream)
	sendWatchCreate(t, stream, key, 708, put3.Header.Revision+1)
	created := recvWatchResponse(t, stream)
	put4, err := kv.Put(ctx, &etcdserverpb.PutRequest{Key: key, Value: []byte("v4")})
	require.NoError(t, err)
	event := recvWatchResponse(t, stream)
	require.Len(t, event.Events, 1)
	for name, header := range map[string]*etcdserverpb.ResponseHeader{
		"seed": seed.Header, "put1": put1.Header, "put2": put2.Header,
		"compact": compacted.Header, "put3": put3.Header, "old-created": oldCreated.Header,
		"canceled": canceled.Header,
		"created":  created.Header, "put4": put4.Header, "event": event.Header,
	} {
		require.NotNil(t, header, name)
	}
	require.NotNil(t, event.Events[0].Kv)
	baseRevision := seed.Header.Revision
	return watchCompactedRevisionOutcome{
		FirstPutGap:          put1.Header.Revision - baseRevision,
		SecondPutGap:         put2.Header.Revision - baseRevision,
		CompactionHeaderGap:  compacted.Header.Revision - baseRevision,
		PostCompactPutGap:    put3.Header.Revision - baseRevision,
		OldCreatedHeaderGap:  oldCreated.Header.Revision - baseRevision,
		OldCreatedEnvelope:   observeWatchControlResponse(oldCreated, seed.Header),
		CancelHeaderZero:     canceled.Header.Revision == 0,
		CanceledEnvelope:     observeWatchControlResponse(canceled, seed.Header),
		NextCreatedHeaderGap: created.Header.Revision - baseRevision,
		NextCreatedEnvelope:  observeWatchControlResponse(created, seed.Header),
		FinalPutGap:          put4.Header.Revision - baseRevision,
		EventHeaderGap:       event.Header.Revision - baseRevision,
		EventModRevisionGap:  event.Events[0].Kv.ModRevision - baseRevision,
		EventValue:           string(event.Events[0].Kv.Value),
	}
}
