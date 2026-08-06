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

type leaseKeepAliveFutureWatchOutcome struct {
	GrantHeaderAfterBase     int64
	SeedHeaderAfterBase      int64
	CreatedCanonical         bool
	CreatedHeaderAfterSeed   int64
	KeepAliveHeaderAfterSeed []int64
	KeepAliveTTLs            []int64
	TTLHeaderAfterSeed       int64
	TTLKeyAttached           bool
	FutureProgressSuppressed bool
	UpdateHeaderAfterSeed    int64
	EventHeaderAfterSeed     int64
	EventCanonical           bool
	FinalProgressCanonical   bool
	FinalProgressAfterSeed   int64
}

func TestLeaseKeepAliveDoesNotAdvanceFutureWatchDifferentialAgainstReferenceEtcd(t *testing.T) {
	reference := os.Getenv("REFERENCE_ETCD_ENDPOINT")
	if reference == "" {
		t.Skip("set REFERENCE_ETCD_ENDPOINT to run differential compatibility tests")
	}
	want := leaseKeepAliveFutureWatchOutcome{
		SeedHeaderAfterBase:      1,
		CreatedCanonical:         true,
		KeepAliveHeaderAfterSeed: []int64{0, 0},
		KeepAliveTTLs:            []int64{300, 300},
		TTLKeyAttached:           true,
		FutureProgressSuppressed: true,
		UpdateHeaderAfterSeed:    1,
		EventHeaderAfterSeed:     1,
		EventCanonical:           true,
		FinalProgressCanonical:   true,
		FinalProgressAfterSeed:   1,
	}
	referenceOutcome := runLeaseKeepAliveFutureWatchScenario(t, reference, "etcd")
	require.Equal(t, want, referenceOutcome)
	require.Equal(t, referenceOutcome, runLeaseKeepAliveFutureWatchScenario(t, compatEndpoint(t), "kubebrain"))
}

func runLeaseKeepAliveFutureWatchScenario(t *testing.T, endpoint, instance string) leaseKeepAliveFutureWatchOutcome {
	t.Helper()
	endpoint = strings.TrimPrefix(strings.TrimPrefix(endpoint, "http://"), "https://")
	conn, err := grpc.NewClient(endpoint, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, conn.Close()) })
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	t.Cleanup(cancel)
	kv := etcdserverpb.NewKVClient(conn)
	lease := etcdserverpb.NewLeaseClient(conn)
	key := []byte(fmt.Sprintf("/dbaas-lease-keepalive-future-watch/%s/%d", instance, time.Now().UnixNano()))
	base, err := kv.Range(ctx, &etcdserverpb.RangeRequest{Key: key})
	require.NoError(t, err)
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
	seed, err := kv.Put(ctx, &etcdserverpb.PutRequest{Key: key, Value: []byte("seed"), Lease: grant.ID})
	require.NoError(t, err)

	watch, err := etcdserverpb.NewWatchClient(conn).Watch(ctx)
	require.NoError(t, err)
	t.Cleanup(func() { _ = watch.CloseSend() })
	const watchID int64 = 3699
	require.NoError(t, watch.Send(&etcdserverpb.WatchRequest{
		RequestUnion: &etcdserverpb.WatchRequest_CreateRequest{CreateRequest: &etcdserverpb.WatchCreateRequest{
			Key: key, WatchId: watchID, StartRevision: seed.Header.Revision + 1, PrevKv: true,
		}},
	}))
	created := recvWatchResponse(t, watch)
	require.True(t, created.Created)
	require.Empty(t, created.Events)

	keepAlive, err := lease.LeaseKeepAlive(ctx)
	require.NoError(t, err)
	keepAliveResponses := make([]*etcdserverpb.LeaseKeepAliveResponse, 0, 2)
	for range 2 {
		require.NoError(t, keepAlive.Send(&etcdserverpb.LeaseKeepAliveRequest{ID: grant.ID}))
		response, receiveErr := keepAlive.Recv()
		require.NoError(t, receiveErr)
		keepAliveResponses = append(keepAliveResponses, response)
	}
	require.NoError(t, keepAlive.CloseSend())
	ttl, err := lease.LeaseTimeToLive(ctx, &etcdserverpb.LeaseTimeToLiveRequest{ID: grant.ID, Keys: true})
	require.NoError(t, err)

	require.NoError(t, watch.Send(progressWatchRequest()))
	type receiveResult struct {
		response *etcdserverpb.WatchResponse
		err      error
	}
	pending := make(chan receiveResult, 1)
	go func() {
		response, receiveErr := watch.Recv()
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
	update, err := kv.Put(ctx, &etcdserverpb.PutRequest{Key: key, Value: []byte("updated"), Lease: grant.ID})
	require.NoError(t, err)
	if !receivedEarly {
		received = <-pending
	}
	require.NoError(t, received.err)
	event := received.response
	require.NotNil(t, event)
	require.NoError(t, watch.Send(progressWatchRequest()))
	progress := recvWatchResponse(t, watch)
	require.NotNil(t, progress)

	for name, header := range map[string]*etcdserverpb.ResponseHeader{
		"base": base.Header, "grant": grant.Header, "seed": seed.Header, "created": created.Header,
		"keepalive-0": keepAliveResponses[0].Header, "keepalive-1": keepAliveResponses[1].Header,
		"ttl": ttl.Header, "update": update.Header, "event": event.Header, "progress": progress.Header,
	} {
		require.NotNil(t, header, name)
	}
	ttlKeyAttached := len(ttl.Keys) == 1 && string(ttl.Keys[0]) == string(key)
	eventCanonical := event.WatchId == watchID && !event.Created && !event.Canceled &&
		len(event.Events) == 1 && event.Events[0] != nil && event.Events[0].Type == mvccpb.PUT &&
		event.Events[0].Kv != nil && string(event.Events[0].Kv.Key) == string(key) &&
		string(event.Events[0].Kv.Value) == "updated" && event.Events[0].Kv.CreateRevision == seed.Header.Revision &&
		event.Events[0].Kv.ModRevision == update.Header.Revision && event.Events[0].Kv.Version == 2 &&
		event.Events[0].Kv.Lease == grant.ID && event.Events[0].PrevKv != nil &&
		string(event.Events[0].PrevKv.Key) == string(key) && string(event.Events[0].PrevKv.Value) == "seed" &&
		event.Events[0].PrevKv.CreateRevision == seed.Header.Revision &&
		event.Events[0].PrevKv.ModRevision == seed.Header.Revision && event.Events[0].PrevKv.Version == 1 &&
		event.Events[0].PrevKv.Lease == grant.ID

	_, err = lease.LeaseRevoke(ctx, &etcdserverpb.LeaseRevokeRequest{ID: grant.ID})
	require.NoError(t, err)
	revoked = true
	return leaseKeepAliveFutureWatchOutcome{
		GrantHeaderAfterBase:     grant.Header.Revision - base.Header.Revision,
		SeedHeaderAfterBase:      seed.Header.Revision - base.Header.Revision,
		CreatedCanonical:         canonicalWatchControlResponse(created, true, watchID),
		CreatedHeaderAfterSeed:   created.Header.Revision - seed.Header.Revision,
		KeepAliveHeaderAfterSeed: []int64{keepAliveResponses[0].Header.Revision - seed.Header.Revision, keepAliveResponses[1].Header.Revision - seed.Header.Revision},
		KeepAliveTTLs:            []int64{keepAliveResponses[0].TTL, keepAliveResponses[1].TTL},
		TTLHeaderAfterSeed:       ttl.Header.Revision - seed.Header.Revision,
		TTLKeyAttached:           ttlKeyAttached,
		FutureProgressSuppressed: progressSuppressed,
		UpdateHeaderAfterSeed:    update.Header.Revision - seed.Header.Revision,
		EventHeaderAfterSeed:     event.Header.Revision - seed.Header.Revision,
		EventCanonical:           eventCanonical,
		FinalProgressCanonical:   canonicalWatchControlResponse(progress, false, -1),
		FinalProgressAfterSeed:   progress.Header.Revision - seed.Header.Revision,
	}
}
