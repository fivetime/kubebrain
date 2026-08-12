package compat

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
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

type explicitLeaseGrantResponseLossOutcome struct {
	ResponseDiscarded    bool
	FirstUnavailable     bool
	SecondReplicaDialed  bool
	RetryLeaseExists     bool
	NewLeaseCount        int
	ExplicitLeasePresent bool
	ExplicitLeaseLive    bool
	RevisionDelta        int64
}

type orphanLeaseExpiryAfterGrantLossOutcome struct {
	TwoLeasesCreated     bool
	ReturnedLeaseRevoked bool
	SingleOrphanObserved bool
	OrphanExpired        bool
	LeaseSetRestored     bool
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

// TestExplicitLeaseGrantResponseLossRetryAcrossReplicasDifferential proves the
// reconciliation path available to platforms that allocate an ID before the
// RPC. A raw non-repeatable call exposes response loss as Unavailable; an
// explicit retry on another replica returns LeaseExist and cannot allocate an
// orphan generation.
func TestExplicitLeaseGrantResponseLossRetryAcrossReplicasDifferential(t *testing.T) {
	referenceEndpoints := splitRequiredDirectEndpoints(t, "REFERENCE_ETCD_DIRECT_ENDPOINTS")
	kubeBrainEndpoints := splitRequiredDirectEndpoints(t, "KUBEBRAIN_DIRECT_ENDPOINTS")
	requireDistinctDirectReplicaTopology(t, referenceEndpoints)
	requireDistinctDirectReplicaTopology(t, kubeBrainEndpoints)

	want := explicitLeaseGrantResponseLossOutcome{
		ResponseDiscarded:    true,
		FirstUnavailable:     true,
		SecondReplicaDialed:  true,
		RetryLeaseExists:     true,
		NewLeaseCount:        1,
		ExplicitLeasePresent: true,
		ExplicitLeaseLive:    true,
		RevisionDelta:        0,
	}
	reference := runExplicitLeaseGrantResponseLossScenario(t, referenceEndpoints, "etcd")
	require.Equal(t, want, reference)
	require.Equal(t, reference, runExplicitLeaseGrantResponseLossScenario(t, kubeBrainEndpoints, "kubebrain"))
}

// TestOrphanLeaseExpiresAfterGrantResponseLossDifferential proves that the
// extra automatic-ID lease exposed by response loss is bounded by its TTL. The
// caller revokes only the returned ID; the unknown empty lease must disappear
// through normal expiry without advancing the user KV revision.
func TestOrphanLeaseExpiresAfterGrantResponseLossDifferential(t *testing.T) {
	referenceEndpoints := splitRequiredDirectEndpoints(t, "REFERENCE_ETCD_DIRECT_ENDPOINTS")
	kubeBrainEndpoints := splitRequiredDirectEndpoints(t, "KUBEBRAIN_DIRECT_ENDPOINTS")
	requireDistinctDirectReplicaTopology(t, referenceEndpoints)
	requireDistinctDirectReplicaTopology(t, kubeBrainEndpoints)

	want := orphanLeaseExpiryAfterGrantLossOutcome{
		TwoLeasesCreated:     true,
		ReturnedLeaseRevoked: true,
		SingleOrphanObserved: true,
		OrphanExpired:        true,
		LeaseSetRestored:     true,
		RevisionDelta:        0,
	}
	reference := runOrphanLeaseExpiryAfterGrantLossScenario(t, referenceEndpoints, "etcd")
	require.Equal(t, want, reference)
	require.Equal(t, reference, runOrphanLeaseExpiryAfterGrantLossScenario(t, kubeBrainEndpoints, "kubebrain"))
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

func runExplicitLeaseGrantResponseLossScenario(
	t *testing.T,
	endpoints []string,
	instance string,
) explicitLeaseGrantResponseLossOutcome {
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
	// Ask the cluster for a collision-free ID, remove that temporary lease, then
	// use the ID through the raw explicit-ID API under test.
	reserved, err := observer.Grant(ctx, 60)
	require.NoError(t, err)
	_, err = observer.Revoke(ctx, reserved.ID)
	require.NoError(t, err)
	baseline, err := observer.Leases(ctx)
	require.NoError(t, err)
	baselineIDs := leaseIDSet(baseline)
	warm, err := throughBridge.Leases(ctx)
	require.NoError(t, err)
	require.Equal(t, baselineIDs, leaseIDSet(warm))
	require.True(t, bridge.DialedTarget(endpoints[0]))
	probeKey := fmt.Sprintf("/dbaas-explicit-lease-grant-response-loss/%s/%d", instance, time.Now().UnixNano())
	before, err := observer.Get(ctx, probeKey)
	require.NoError(t, err)

	rawLease := etcdserverpb.NewLeaseClient(throughBridge.ActiveConnection())
	request := &etcdserverpb.LeaseGrantRequest{ID: int64(reserved.ID), TTL: 60}
	droppedBefore := bridge.DroppedBytes()
	bridge.BlackholeResponses()
	firstDone := make(chan error, 1)
	go func() {
		_, firstErr := rawLease.LeaseGrant(ctx, request)
		firstDone <- firstErr
	}()
	require.Eventually(t, func() bool {
		ttl, ttlErr := observer.TimeToLive(ctx, reserved.ID)
		return ttlErr == nil && ttl.TTL > 0
	}, 5*time.Second, 10*time.Millisecond)
	require.Eventually(t, func() bool { return bridge.DroppedBytes() > droppedBefore }, 2*time.Second, 10*time.Millisecond)
	bridge.SetTarget(endpoints[1])
	bridge.DropConnections()
	bridge.Resume()

	var firstErr error
	select {
	case firstErr = <-firstDone:
	case <-ctx.Done():
		require.NoError(t, ctx.Err())
	}
	// Re-establish the switched HTTP/2 connection with an immutable read before
	// issuing the application's one explicit retry.
	_, err = throughBridge.Leases(ctx)
	require.NoError(t, err)
	require.True(t, bridge.DialedTarget(endpoints[1]))
	_, retryErr := rawLease.LeaseGrant(ctx, request)
	final, err := observer.Leases(ctx)
	require.NoError(t, err)
	newIDs := newLeaseIDs(final, baselineIDs)
	present := false
	for _, id := range newIDs {
		present = present || id == reserved.ID
	}
	ttl, err := observer.TimeToLive(ctx, reserved.ID)
	require.NoError(t, err)
	after, err := observer.Get(ctx, probeKey)
	require.NoError(t, err)
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		_, _ = observer.Revoke(cleanupCtx, reserved.ID)
	})

	return explicitLeaseGrantResponseLossOutcome{
		ResponseDiscarded:   bridge.DroppedBytes() > droppedBefore,
		FirstUnavailable:    status.Code(firstErr) == codes.Unavailable,
		SecondReplicaDialed: bridge.DialedTarget(endpoints[1]),
		RetryLeaseExists: status.Code(retryErr) == codes.FailedPrecondition &&
			status.Convert(retryErr).Message() == "etcdserver: lease already exists",
		NewLeaseCount:        len(newIDs),
		ExplicitLeasePresent: present,
		ExplicitLeaseLive:    ttl.TTL > 0,
		RevisionDelta:        after.Header.Revision - before.Header.Revision,
	}
}

func runOrphanLeaseExpiryAfterGrantLossScenario(
	t *testing.T,
	endpoints []string,
	instance string,
) orphanLeaseExpiryAfterGrantLossOutcome {
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

	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
	t.Cleanup(cancel)
	baseline, err := observer.Leases(ctx)
	require.NoError(t, err)
	baselineIDs := leaseIDSet(baseline)
	warm, err := throughBridge.Leases(ctx)
	require.NoError(t, err)
	require.Equal(t, baselineIDs, leaseIDSet(warm))
	probeKey := fmt.Sprintf("/dbaas-orphan-lease-expiry-after-grant-loss/%s/%d", instance, time.Now().UnixNano())
	before, err := observer.Get(ctx, probeKey)
	require.NoError(t, err)

	droppedBefore := bridge.DroppedBytes()
	bridge.BlackholeResponses()
	grantDone := make(chan *clientv3.LeaseGrantResponse, 1)
	grantErrDone := make(chan error, 1)
	go func() {
		response, grantErr := throughBridge.Grant(ctx, 5)
		grantDone <- response
		grantErrDone <- grantErr
	}()
	require.Eventually(t, func() bool {
		current, listErr := observer.Leases(ctx)
		return listErr == nil && countNewLeaseIDs(current, baselineIDs) == 1
	}, 5*time.Second, 10*time.Millisecond)
	require.Eventually(t, func() bool { return bridge.DroppedBytes() > droppedBefore }, 2*time.Second, 10*time.Millisecond)
	bridge.SetTarget(endpoints[1])
	bridge.DropConnections()
	bridge.Resume()

	var grant *clientv3.LeaseGrantResponse
	select {
	case grant = <-grantDone:
		require.NoError(t, <-grantErrDone)
	case <-ctx.Done():
		require.NoError(t, ctx.Err())
	}
	var created *clientv3.LeaseLeasesResponse
	require.Eventually(t, func() bool {
		var listErr error
		created, listErr = observer.Leases(ctx)
		return listErr == nil && countNewLeaseIDs(created, baselineIDs) == 2
	}, 5*time.Second, 10*time.Millisecond)
	newIDs := newLeaseIDs(created, baselineIDs)
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		for _, id := range newIDs {
			_, _ = observer.Revoke(cleanupCtx, id)
		}
	})
	require.NotNil(t, grant)
	orphanID := clientv3.LeaseID(0)
	for _, id := range newIDs {
		if id != grant.ID {
			orphanID = id
		}
	}
	twoCreated := len(newIDs) == 2
	singleOrphan := orphanID != 0
	_, revokeErr := observer.Revoke(ctx, grant.ID)
	returnedRevoked := revokeErr == nil

	orphanExpired := false
	leaseSetRestored := false
	require.Eventually(t, func() bool {
		ttl, ttlErr := observer.TimeToLive(ctx, orphanID)
		if ttlErr != nil || ttl.TTL != -1 {
			return false
		}
		orphanExpired = true
		current, listErr := observer.Leases(ctx)
		if listErr != nil || countNewLeaseIDs(current, baselineIDs) != 0 {
			return false
		}
		leaseSetRestored = true
		return true
	}, 15*time.Second, 50*time.Millisecond)
	after, err := observer.Get(ctx, probeKey)
	require.NoError(t, err)

	return orphanLeaseExpiryAfterGrantLossOutcome{
		TwoLeasesCreated:     twoCreated,
		ReturnedLeaseRevoked: returnedRevoked,
		SingleOrphanObserved: singleOrphan,
		OrphanExpired:        orphanExpired,
		LeaseSetRestored:     leaseSetRestored,
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
