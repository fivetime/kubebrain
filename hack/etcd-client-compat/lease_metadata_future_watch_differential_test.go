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
	"go.etcd.io/etcd/api/v3/mvccpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type leaseMetadataFutureWatchOutcome struct {
	CreatedCanonical         bool
	CreatedHeaderGap         int64
	LeaseHeaderGaps          []int64
	FutureProgressSuppressed bool
	EventHeaderGap           int64
	EventCanonical           bool
	ProgressCanonical        bool
	ProgressHeaderGap        int64
}

func TestLeaseMetadataDoesNotAdvanceFutureWatchDifferentialAgainstReferenceEtcd(t *testing.T) {
	reference := os.Getenv("REFERENCE_ETCD_ENDPOINT")
	if reference == "" {
		t.Skip("set REFERENCE_ETCD_ENDPOINT to run differential compatibility tests")
	}
	want := leaseMetadataFutureWatchOutcome{
		CreatedCanonical:         true,
		LeaseHeaderGaps:          []int64{0, 0, 0, 0},
		FutureProgressSuppressed: true,
		EventHeaderGap:           1,
		EventCanonical:           true,
		ProgressCanonical:        true,
		ProgressHeaderGap:        1,
	}
	referenceOutcome := runLeaseMetadataFutureWatchScenario(t, reference, "etcd")
	require.Equal(t, want, referenceOutcome)
	require.Equal(t, referenceOutcome, runLeaseMetadataFutureWatchScenario(t, compatEndpoint(t), "kubebrain"))
}

func runLeaseMetadataFutureWatchScenario(t *testing.T, endpoint, instance string) leaseMetadataFutureWatchOutcome {
	t.Helper()
	endpoint = strings.TrimPrefix(strings.TrimPrefix(endpoint, "http://"), "https://")
	conn, err := grpc.NewClient(endpoint, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, conn.Close()) })
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	t.Cleanup(cancel)
	kv := etcdserverpb.NewKVClient(conn)
	lease := etcdserverpb.NewLeaseClient(conn)
	key := []byte(fmt.Sprintf("/dbaas-lease-metadata-future-watch/%s/%d", instance, time.Now().UnixNano()))
	base, err := kv.Range(ctx, &etcdserverpb.RangeRequest{Key: key})
	require.NoError(t, err)
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		_, _ = kv.DeleteRange(cleanupCtx, &etcdserverpb.DeleteRangeRequest{Key: key})
	})

	stream, err := etcdserverpb.NewWatchClient(conn).Watch(ctx)
	require.NoError(t, err)
	t.Cleanup(func() { _ = stream.CloseSend() })
	const watchID int64 = 3698
	sendWatchCreate(t, stream, key, watchID, base.Header.Revision+1)
	created := recvWatchResponse(t, stream)
	require.True(t, created.Created)
	require.Empty(t, created.Events)

	grant, err := lease.LeaseGrant(ctx, &etcdserverpb.LeaseGrantRequest{TTL: 300})
	require.NoError(t, err)
	revoked := false
	t.Cleanup(func() {
		if revoked {
			return
		}
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		_, _ = lease.LeaseRevoke(cleanupCtx, &etcdserverpb.LeaseRevokeRequest{ID: grant.ID})
	})
	ttl, err := lease.LeaseTimeToLive(ctx, &etcdserverpb.LeaseTimeToLiveRequest{ID: grant.ID, Keys: true})
	require.NoError(t, err)
	list, err := lease.LeaseLeases(ctx, &etcdserverpb.LeaseLeasesRequest{})
	require.NoError(t, err)
	revoke, err := lease.LeaseRevoke(ctx, &etcdserverpb.LeaseRevokeRequest{ID: grant.ID})
	require.NoError(t, err)
	revoked = true

	require.NoError(t, stream.Send(progressWatchRequest()))
	type receiveResult struct {
		response *etcdserverpb.WatchResponse
		err      error
	}
	pending := make(chan receiveResult, 1)
	go func() {
		response, receiveErr := stream.Recv()
		pending <- receiveResult{response: response, err: receiveErr}
	}()
	var received receiveResult
	receivedEarly := false
	progressSuppressed := false
	select {
	case received = <-pending:
		receivedEarly = true
	case <-time.After(150 * time.Millisecond):
		progressSuppressed = true
	}
	put, err := kv.Put(ctx, &etcdserverpb.PutRequest{Key: key, Value: []byte("user-write")})
	require.NoError(t, err)
	if !receivedEarly {
		received = <-pending
	}
	require.NoError(t, received.err)
	event := received.response
	require.NotNil(t, event)
	require.NoError(t, stream.Send(progressWatchRequest()))
	progress := recvWatchResponse(t, stream)
	require.NotNil(t, progress)
	for name, header := range map[string]*etcdserverpb.ResponseHeader{
		"base": base.Header, "created": created.Header, "grant": grant.Header, "ttl": ttl.Header,
		"list": list.Header, "revoke": revoke.Header, "put": put.Header, "event": event.Header,
		"progress": progress.Header,
	} {
		require.NotNil(t, header, name)
	}

	eventCanonical := event != nil && event.Header != nil && event.WatchId == watchID &&
		!event.Created && !event.Canceled && len(event.Events) == 1 && event.Events[0] != nil &&
		event.Events[0].Type == mvccpb.PUT && event.Events[0].Kv != nil &&
		string(event.Events[0].Kv.Key) == string(key) && string(event.Events[0].Kv.Value) == "user-write" &&
		event.Events[0].Kv.CreateRevision == put.Header.Revision &&
		event.Events[0].Kv.ModRevision == put.Header.Revision && event.Events[0].Kv.Version == 1 &&
		event.Events[0].Kv.Lease == 0 && event.Events[0].PrevKv == nil
	return leaseMetadataFutureWatchOutcome{
		CreatedCanonical:         canonicalWatchControlResponse(created, true, watchID),
		CreatedHeaderGap:         created.Header.Revision - base.Header.Revision,
		LeaseHeaderGaps:          []int64{grant.Header.Revision - base.Header.Revision, ttl.Header.Revision - base.Header.Revision, list.Header.Revision - base.Header.Revision, revoke.Header.Revision - base.Header.Revision},
		FutureProgressSuppressed: progressSuppressed,
		EventHeaderGap:           event.Header.Revision - base.Header.Revision,
		EventCanonical:           eventCanonical,
		ProgressCanonical:        canonicalWatchControlResponse(progress, false, -1),
		ProgressHeaderGap:        progress.Header.Revision - base.Header.Revision,
	}
}
