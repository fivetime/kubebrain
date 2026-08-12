package compat

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"
)

type leaseGrantResponseLossReplayOutcome struct {
	ResponseDiscarded    bool
	SecondReplicaDialed  bool
	GrantReturnedSuccess bool
	NewLeaseCount        int
	ReturnedLeasePresent bool
	OrphanLeaseCount     int
	AllNewLeasesLive     bool
	RevisionDelta        int64
}

// TestLeaseGrantResponseLossReplayAcrossReplicasDifferential records the
// upstream automatic-ID ambiguity. LeaseGrant is repeatable, but a replayed
// ID=0 request allocates another lease; only the replay response reaches the
// caller, leaving the first committed lease without a returned ID.
func TestLeaseGrantResponseLossReplayAcrossReplicasDifferential(t *testing.T) {
	referenceEndpoints := splitRequiredDirectEndpoints(t, "REFERENCE_ETCD_DIRECT_ENDPOINTS")
	kubeBrainEndpoints := splitRequiredDirectEndpoints(t, "KUBEBRAIN_DIRECT_ENDPOINTS")
	requireDistinctDirectReplicaTopology(t, referenceEndpoints)
	requireDistinctDirectReplicaTopology(t, kubeBrainEndpoints)

	want := leaseGrantResponseLossReplayOutcome{
		ResponseDiscarded:    true,
		SecondReplicaDialed:  true,
		GrantReturnedSuccess: true,
		NewLeaseCount:        2,
		ReturnedLeasePresent: true,
		OrphanLeaseCount:     1,
		AllNewLeasesLive:     true,
		RevisionDelta:        0,
	}
	reference := runLeaseGrantResponseLossReplayScenario(t, referenceEndpoints, "etcd")
	require.Equal(t, want, reference)
	require.Equal(t, reference, runLeaseGrantResponseLossReplayScenario(t, kubeBrainEndpoints, "kubebrain"))
}

func runLeaseGrantResponseLossReplayScenario(
	t *testing.T,
	endpoints []string,
	instance string,
) leaseGrantResponseLossReplayOutcome {
	t.Helper()
	bridge := newTCPBridge(t, endpoints[0])
	throughBridge, err := clientv3.New(clientv3.Config{
		Endpoints: []string{bridge.Endpoint()}, DialTimeout: 3 * time.Second,
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, throughBridge.Close()) })
	observer, err := clientv3.New(clientv3.Config{
		Endpoints: []string{endpoints[2]}, DialTimeout: 3 * time.Second,
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, observer.Close()) })

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	t.Cleanup(cancel)
	baseline, err := observer.Leases(ctx)
	require.NoError(t, err)
	baselineIDs := leaseIDSet(baseline)
	// Establish the HTTP/2 connection before response blackholing; otherwise
	// dropping the server SETTINGS frame can prevent the Grant from being sent.
	warm, err := throughBridge.Leases(ctx)
	require.NoError(t, err)
	require.Equal(t, baselineIDs, leaseIDSet(warm))
	require.True(t, bridge.DialedTarget(endpoints[0]))
	probeKey := fmt.Sprintf("/dbaas-lease-grant-response-loss/%s/%d", instance, time.Now().UnixNano())
	before, err := observer.Get(ctx, probeKey)
	require.NoError(t, err)

	droppedBefore := bridge.DroppedBytes()
	bridge.BlackholeResponses()
	grantDone := make(chan *clientv3.LeaseGrantResponse, 1)
	grantErrDone := make(chan error, 1)
	go func() {
		response, grantErr := throughBridge.Grant(ctx, 60)
		grantDone <- response
		grantErrDone <- grantErr
	}()

	// The first allocation must be durable before the connection is moved; this
	// distinguishes committed-response-loss from a request that never arrived.
	require.Eventually(t, func() bool {
		current, listErr := observer.Leases(ctx)
		return listErr == nil && countNewLeaseIDs(current, baselineIDs) == 1
	}, 5*time.Second, 10*time.Millisecond)
	require.Eventually(t, func() bool { return bridge.DroppedBytes() > droppedBefore }, 2*time.Second, 10*time.Millisecond)
	bridge.SetTarget(endpoints[1])
	bridge.DropConnections()
	bridge.Resume()

	var grant *clientv3.LeaseGrantResponse
	var grantErr error
	select {
	case grant = <-grantDone:
		grantErr = <-grantErrDone
	case <-ctx.Done():
		require.NoError(t, ctx.Err())
	}
	require.Eventually(t, func() bool { return bridge.DialedTarget(endpoints[1]) }, 5*time.Second, 10*time.Millisecond)
	var final *clientv3.LeaseLeasesResponse
	require.Eventually(t, func() bool {
		var listErr error
		final, listErr = observer.Leases(ctx)
		return listErr == nil && countNewLeaseIDs(final, baselineIDs) == 2
	}, 5*time.Second, 10*time.Millisecond)
	newIDs := newLeaseIDs(final, baselineIDs)
	returnedPresent := false
	allLive := len(newIDs) == 2
	for _, id := range newIDs {
		if grant != nil && id == grant.ID {
			returnedPresent = true
		}
		ttl, ttlErr := observer.TimeToLive(ctx, id)
		allLive = allLive && ttlErr == nil && ttl.TTL > 0
	}
	after, err := observer.Get(ctx, probeKey)
	require.NoError(t, err)

	// Both leases are known to the test even though one is unknown to a real
	// caller. Revoke them so a reusable differential environment stays clean.
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		for _, id := range newIDs {
			_, _ = observer.Revoke(cleanupCtx, id)
		}
	})
	orphans := len(newIDs)
	if returnedPresent {
		orphans--
	}
	return leaseGrantResponseLossReplayOutcome{
		ResponseDiscarded:    bridge.DroppedBytes() > droppedBefore,
		SecondReplicaDialed:  bridge.DialedTarget(endpoints[1]),
		GrantReturnedSuccess: grantErr == nil && grant != nil,
		NewLeaseCount:        len(newIDs),
		ReturnedLeasePresent: returnedPresent,
		OrphanLeaseCount:     orphans,
		AllNewLeasesLive:     allLive,
		RevisionDelta:        after.Header.Revision - before.Header.Revision,
	}
}

func leaseIDSet(response *clientv3.LeaseLeasesResponse) map[clientv3.LeaseID]struct{} {
	ids := make(map[clientv3.LeaseID]struct{}, len(response.Leases))
	for _, lease := range response.Leases {
		ids[lease.ID] = struct{}{}
	}
	return ids
}

func countNewLeaseIDs(response *clientv3.LeaseLeasesResponse, baseline map[clientv3.LeaseID]struct{}) int {
	return len(newLeaseIDs(response, baseline))
}

func newLeaseIDs(response *clientv3.LeaseLeasesResponse, baseline map[clientv3.LeaseID]struct{}) []clientv3.LeaseID {
	ids := make([]clientv3.LeaseID, 0, len(response.Leases))
	for _, lease := range response.Leases {
		if _, existed := baseline[lease.ID]; !existed {
			ids = append(ids, lease.ID)
		}
	}
	return ids
}
