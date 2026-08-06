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

type emptyLeaseExpiryFutureWatchOutcome struct {
	GrantedTTL                int64
	GrantHeaderAfterBase      int64
	CreatedCanonical          bool
	CreatedHeaderAfterBase    int64
	ExpiredTTL                int64
	ExpiredTTLHeaderAfterBase int64
	LeaseAbsentFromList       bool
	ListHeaderAfterBase       int64
	FutureProgressSuppressed  bool
	PutHeaderAfterBase        int64
	EventHeaderAfterBase      int64
	EventCanonical            bool
	FinalProgressCanonical    bool
	FinalProgressAfterBase    int64
}

func TestEmptyLeaseNaturalExpiryDoesNotAdvanceFutureWatchDifferentialAgainstReferenceEtcd(t *testing.T) {
	reference := os.Getenv("REFERENCE_ETCD_ENDPOINT")
	if reference == "" {
		t.Skip("set REFERENCE_ETCD_ENDPOINT to run differential compatibility tests")
	}
	want := emptyLeaseExpiryFutureWatchOutcome{
		GrantedTTL:               2,
		CreatedCanonical:         true,
		ExpiredTTL:               -1,
		LeaseAbsentFromList:      true,
		FutureProgressSuppressed: true,
		PutHeaderAfterBase:       1,
		EventHeaderAfterBase:     1,
		EventCanonical:           true,
		FinalProgressCanonical:   true,
		FinalProgressAfterBase:   1,
	}
	referenceOutcome := runEmptyLeaseExpiryFutureWatchScenario(t, reference, "etcd")
	require.Equal(t, want, referenceOutcome)
	require.Equal(t, referenceOutcome, runEmptyLeaseExpiryFutureWatchScenario(t, compatEndpoint(t), "kubebrain"))
}

func runEmptyLeaseExpiryFutureWatchScenario(t *testing.T, endpoint, instance string) emptyLeaseExpiryFutureWatchOutcome {
	t.Helper()
	endpoint = strings.TrimPrefix(strings.TrimPrefix(endpoint, "http://"), "https://")
	conn, err := grpc.NewClient(endpoint, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, conn.Close()) })
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	t.Cleanup(cancel)
	kv := etcdserverpb.NewKVClient(conn)
	lease := etcdserverpb.NewLeaseClient(conn)
	key := []byte(fmt.Sprintf("/dbaas-empty-lease-expiry-future-watch/%s/%d", instance, time.Now().UnixNano()))
	base, err := kv.Range(ctx, &etcdserverpb.RangeRequest{Key: key})
	require.NoError(t, err)
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		_, _ = kv.DeleteRange(cleanupCtx, &etcdserverpb.DeleteRangeRequest{Key: key})
	})
	grant, err := lease.LeaseGrant(ctx, &etcdserverpb.LeaseGrantRequest{TTL: 1})
	require.NoError(t, err)

	watch, err := etcdserverpb.NewWatchClient(conn).Watch(ctx)
	require.NoError(t, err)
	t.Cleanup(func() { _ = watch.CloseSend() })
	const watchID int64 = 3700
	sendWatchCreate(t, watch, key, watchID, base.Header.Revision+1)
	created := recvWatchResponse(t, watch)
	require.True(t, created.Created)
	require.Empty(t, created.Events)

	var expired *etcdserverpb.LeaseTimeToLiveResponse
	require.Eventually(t, func() bool {
		response, ttlErr := lease.LeaseTimeToLive(ctx, &etcdserverpb.LeaseTimeToLiveRequest{ID: grant.ID, Keys: true})
		if ttlErr != nil {
			return false
		}
		expired = response
		return response.TTL == -1
	}, 10*time.Second, 100*time.Millisecond)
	list, err := lease.LeaseLeases(ctx, &etcdserverpb.LeaseLeasesRequest{})
	require.NoError(t, err)
	absent := true
	for _, status := range list.Leases {
		absent = absent && status.ID != grant.ID
	}

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
	put, err := kv.Put(ctx, &etcdserverpb.PutRequest{Key: key, Value: []byte("after-empty-expiry")})
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
		"base": base.Header, "grant": grant.Header, "created": created.Header, "expired": expired.Header,
		"list": list.Header, "put": put.Header, "event": event.Header, "progress": progress.Header,
	} {
		require.NotNil(t, header, name)
	}
	eventCanonical := event.WatchId == watchID && !event.Created && !event.Canceled &&
		len(event.Events) == 1 && event.Events[0] != nil && event.Events[0].Type == mvccpb.PUT &&
		event.Events[0].Kv != nil && string(event.Events[0].Kv.Key) == string(key) &&
		string(event.Events[0].Kv.Value) == "after-empty-expiry" &&
		event.Events[0].Kv.CreateRevision == put.Header.Revision &&
		event.Events[0].Kv.ModRevision == put.Header.Revision && event.Events[0].Kv.Version == 1 &&
		event.Events[0].Kv.Lease == 0 && event.Events[0].PrevKv == nil
	return emptyLeaseExpiryFutureWatchOutcome{
		GrantedTTL:                grant.TTL,
		GrantHeaderAfterBase:      grant.Header.Revision - base.Header.Revision,
		CreatedCanonical:          canonicalWatchControlResponse(created, true, watchID),
		CreatedHeaderAfterBase:    created.Header.Revision - base.Header.Revision,
		ExpiredTTL:                expired.TTL,
		ExpiredTTLHeaderAfterBase: expired.Header.Revision - base.Header.Revision,
		LeaseAbsentFromList:       absent,
		ListHeaderAfterBase:       list.Header.Revision - base.Header.Revision,
		FutureProgressSuppressed:  progressSuppressed,
		PutHeaderAfterBase:        put.Header.Revision - base.Header.Revision,
		EventHeaderAfterBase:      event.Header.Revision - base.Header.Revision,
		EventCanonical:            eventCanonical,
		FinalProgressCanonical:    canonicalWatchControlResponse(progress, false, -1),
		FinalProgressAfterBase:    progress.Header.Revision - base.Header.Revision,
	}
}
