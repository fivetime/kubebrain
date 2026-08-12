package compat

import (
	"context"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type leaseRevokeResponseLossOutcome struct {
	ResponseDiscarded bool
	RevokeTimedOut    bool
	KeyDeleted        bool
	LeaseMissing      bool
	LeaseUnlisted     bool
	KeepAliveClosed   bool
	RevisionDelta     int64
}

type leaseRevokeResponseLossReplayOutcome struct {
	ResponseDiscarded bool
	ConnectionDropped bool
	LeaseNotFound     bool
	KeyDeleted        bool
	LeaseMissing      bool
	RevisionDelta     int64
}

type leaseRevokeCrossReplicaReplayOutcome struct {
	ResponseDiscarded   bool
	ConnectionDropped   bool
	SecondReplicaDialed bool
	LeaseNotFound       bool
	KeyDeleted          bool
	LeaseMissing        bool
	LeaseUnlisted       bool
	RevisionDelta       int64
}

type leaseRevokeRegrantReplayOutcome struct {
	ResponseDiscarded       bool
	SecondReplicaDialed     bool
	ReplayReturnedSuccess   bool
	OriginalKeyDeleted      bool
	ReplacementKeyDeleted   bool
	ReplacementLeaseMissing bool
	RevisionDelta           int64
}

// TestLeaseRevokeResponseLossDifferentialAgainstReferenceEtcd fixes the
// observable contract when a Revoke commits but its response is lost. The RPC
// remains ambiguous to the caller, while its atomic key/lease effects must be
// durable and an already-open KeepAlive channel must converge closed.
func TestLeaseRevokeResponseLossDifferentialAgainstReferenceEtcd(t *testing.T) {
	reference := os.Getenv("REFERENCE_ETCD_ENDPOINT")
	if reference == "" {
		t.Skip("set REFERENCE_ETCD_ENDPOINT to run LeaseRevoke response-loss differential tests")
	}

	want := leaseRevokeResponseLossOutcome{
		ResponseDiscarded: true,
		RevokeTimedOut:    true,
		KeyDeleted:        true,
		LeaseMissing:      true,
		LeaseUnlisted:     true,
		KeepAliveClosed:   true,
		RevisionDelta:     1,
	}
	require.Equal(t, want, runLeaseRevokeResponseLossScenario(t, reference, "etcd"))
	require.Equal(t, want, runLeaseRevokeResponseLossScenario(t, compatEndpoint(t), "kubebrain"))

	wantReplay := leaseRevokeResponseLossReplayOutcome{
		ResponseDiscarded: true,
		ConnectionDropped: true,
		LeaseNotFound:     true,
		KeyDeleted:        true,
		LeaseMissing:      true,
		RevisionDelta:     1,
	}
	require.Equal(t, wantReplay, runLeaseRevokeResponseLossReplayScenario(t, reference, "etcd"))
	require.Equal(t, wantReplay, runLeaseRevokeResponseLossReplayScenario(t, compatEndpoint(t), "kubebrain"))
}

// TestLeaseRevokeResponseLossReplayAcrossReplicasDifferential proves that the
// repeatable client policy preserves the same observable result when the first
// Revoke commits on one member but its retry is served by another member. A
// switching TCP bridge makes the replica transition explicit instead of
// relying on load-balancer scheduling.
func TestLeaseRevokeResponseLossReplayAcrossReplicasDifferential(t *testing.T) {
	referenceEndpoints := splitRequiredDirectEndpoints(t, "REFERENCE_ETCD_DIRECT_ENDPOINTS")
	kubeBrainEndpoints := splitRequiredDirectEndpoints(t, "KUBEBRAIN_DIRECT_ENDPOINTS")
	requireDistinctDirectReplicaTopology(t, referenceEndpoints)
	requireDistinctDirectReplicaTopology(t, kubeBrainEndpoints)

	want := leaseRevokeCrossReplicaReplayOutcome{
		ResponseDiscarded:   true,
		ConnectionDropped:   true,
		SecondReplicaDialed: true,
		LeaseNotFound:       true,
		KeyDeleted:          true,
		LeaseMissing:        true,
		LeaseUnlisted:       true,
		RevisionDelta:       1,
	}
	reference := runLeaseRevokeCrossReplicaReplayScenario(t, referenceEndpoints, "etcd")
	require.Equal(t, want, reference)
	require.Equal(t, reference, runLeaseRevokeCrossReplicaReplayScenario(t, kubeBrainEndpoints, "kubebrain"))
}

// TestLeaseRevokeReplayAfterSameIDRegrantDifferential records an important
// upstream limitation: LeaseRevoke carries only an ID, not a generation. If an
// ambiguous old request is replayed after the application regrants that ID, the
// replay successfully revokes the replacement generation. KubeBrain must not
// silently invent stronger fencing than the public etcd protocol provides.
func TestLeaseRevokeReplayAfterSameIDRegrantDifferential(t *testing.T) {
	referenceEndpoints := splitRequiredDirectEndpoints(t, "REFERENCE_ETCD_DIRECT_ENDPOINTS")
	kubeBrainEndpoints := splitRequiredDirectEndpoints(t, "KUBEBRAIN_DIRECT_ENDPOINTS")
	requireDistinctDirectReplicaTopology(t, referenceEndpoints)
	requireDistinctDirectReplicaTopology(t, kubeBrainEndpoints)

	want := leaseRevokeRegrantReplayOutcome{
		ResponseDiscarded:       true,
		SecondReplicaDialed:     true,
		ReplayReturnedSuccess:   true,
		OriginalKeyDeleted:      true,
		ReplacementKeyDeleted:   true,
		ReplacementLeaseMissing: true,
		RevisionDelta:           3,
	}
	reference := runLeaseRevokeRegrantReplayScenario(t, referenceEndpoints, "etcd")
	require.Equal(t, want, reference)
	require.Equal(t, reference, runLeaseRevokeRegrantReplayScenario(t, kubeBrainEndpoints, "kubebrain"))
}

func runLeaseRevokeResponseLossScenario(t *testing.T, endpoint, instance string) leaseRevokeResponseLossOutcome {
	t.Helper()
	bridge := newTCPBridge(t, endpoint)
	throughBridge, err := clientv3.New(clientv3.Config{
		Endpoints: []string{bridge.Endpoint()}, DialTimeout: 3 * time.Second,
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, throughBridge.Close()) })
	direct, err := clientv3.New(clientv3.Config{
		Endpoints: []string{endpoint}, DialTimeout: 3 * time.Second,
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, direct.Close()) })

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	t.Cleanup(cancel)
	prefix := fmt.Sprintf("/dbaas-lease-revoke-response-loss/%s/%d/", instance, time.Now().UnixNano())
	key := prefix + "key"
	grant, err := throughBridge.Grant(ctx, 60)
	require.NoError(t, err)
	_, err = throughBridge.Put(ctx, key, "value", clientv3.WithLease(grant.ID))
	require.NoError(t, err)
	before, err := direct.Get(ctx, key)
	require.NoError(t, err)
	require.Len(t, before.Kvs, 1)
	keepAlive, err := throughBridge.KeepAlive(ctx, grant.ID)
	require.NoError(t, err)
	initial := receivePositiveKeepAlive(t, ctx, keepAlive, grant.ID)
	require.NotNil(t, initial.ResponseHeader)

	droppedBefore := bridge.DroppedBytes()
	bridge.BlackholeResponses()
	revokeCtx, revokeCancel := context.WithTimeout(ctx, 750*time.Millisecond)
	_, revokeErr := throughBridge.Revoke(revokeCtx, grant.ID)
	revokeCancel()
	revokeTimedOut := errors.Is(revokeErr, context.DeadlineExceeded) || status.Code(revokeErr) == codes.DeadlineExceeded
	require.Truef(t, revokeTimedOut, "unexpected ambiguous Revoke error: %v", revokeErr)
	require.Eventually(t, func() bool {
		return bridge.DroppedBytes() > droppedBefore
	}, 2*time.Second, 10*time.Millisecond)
	responseDiscarded := bridge.DroppedBytes() > droppedBefore
	bridge.Unblackhole()

	after, err := direct.Get(ctx, key)
	require.NoError(t, err)
	keyDeleted := len(after.Kvs) == 0
	ttl, err := direct.TimeToLive(ctx, grant.ID)
	require.NoError(t, err)
	leaseMissing := ttl.TTL == -1
	listed, err := direct.Leases(ctx)
	require.NoError(t, err)
	leaseUnlisted := true
	for _, lease := range listed.Leases {
		if lease.ID == grant.ID {
			leaseUnlisted = false
		}
	}
	streamErr := waitForKeepAliveChannelClose(ctx, keepAlive, grant.ID, time.Duration(initial.TTL)*time.Second)

	return leaseRevokeResponseLossOutcome{
		ResponseDiscarded: responseDiscarded,
		RevokeTimedOut:    revokeTimedOut,
		KeyDeleted:        keyDeleted,
		LeaseMissing:      leaseMissing,
		LeaseUnlisted:     leaseUnlisted,
		KeepAliveClosed:   streamErr == nil,
		RevisionDelta:     after.Header.Revision - before.Header.Revision,
	}
}

func runLeaseRevokeResponseLossReplayScenario(
	t *testing.T,
	endpoint, instance string,
) leaseRevokeResponseLossReplayOutcome {
	t.Helper()
	bridge := newTCPBridge(t, endpoint)
	throughBridge, err := clientv3.New(clientv3.Config{
		Endpoints: []string{bridge.Endpoint()}, DialTimeout: 3 * time.Second,
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, throughBridge.Close()) })
	direct, err := clientv3.New(clientv3.Config{
		Endpoints: []string{endpoint}, DialTimeout: 3 * time.Second,
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, direct.Close()) })

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	t.Cleanup(cancel)
	prefix := fmt.Sprintf("/dbaas-lease-revoke-response-replay/%s/%d/", instance, time.Now().UnixNano())
	key := prefix + "key"
	grant, err := throughBridge.Grant(ctx, 60)
	require.NoError(t, err)
	_, err = throughBridge.Put(ctx, key, "value", clientv3.WithLease(grant.ID))
	require.NoError(t, err)
	before, err := direct.Get(ctx, key)
	require.NoError(t, err)
	require.Len(t, before.Kvs, 1)

	droppedBytesBefore := bridge.DroppedBytes()
	droppedConnectionsBefore := bridge.DroppedConnections()
	bridge.BlackholeResponses()
	revokeDone := make(chan error, 1)
	go func() {
		_, revokeErr := throughBridge.Revoke(ctx, grant.ID)
		revokeDone <- revokeErr
	}()
	require.Eventually(t, func() bool {
		return bridge.DroppedBytes() > droppedBytesBefore
	}, 5*time.Second, 10*time.Millisecond)
	responseDiscarded := bridge.DroppedBytes() > droppedBytesBefore
	bridge.DropConnections()
	bridge.Resume()

	var revokeErr error
	select {
	case revokeErr = <-revokeDone:
	case <-ctx.Done():
		require.NoError(t, ctx.Err())
	}
	after, err := direct.Get(ctx, key)
	require.NoError(t, err)
	ttl, err := direct.TimeToLive(ctx, grant.ID)
	require.NoError(t, err)

	return leaseRevokeResponseLossReplayOutcome{
		ResponseDiscarded: responseDiscarded,
		ConnectionDropped: bridge.DroppedConnections() > droppedConnectionsBefore,
		LeaseNotFound:     errors.Is(revokeErr, rpctypes.ErrLeaseNotFound),
		KeyDeleted:        len(after.Kvs) == 0,
		LeaseMissing:      ttl.TTL == -1,
		RevisionDelta:     after.Header.Revision - before.Header.Revision,
	}
}

func runLeaseRevokeCrossReplicaReplayScenario(
	t *testing.T,
	endpoints []string,
	instance string,
) leaseRevokeCrossReplicaReplayOutcome {
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
	prefix := fmt.Sprintf("/dbaas-lease-revoke-cross-replica-replay/%s/%d/", instance, time.Now().UnixNano())
	key := prefix + "key"
	grant, err := throughBridge.Grant(ctx, 60)
	require.NoError(t, err)
	_, err = throughBridge.Put(ctx, key, "value", clientv3.WithLease(grant.ID))
	require.NoError(t, err)
	before, err := observer.Get(ctx, key)
	require.NoError(t, err)
	require.Len(t, before.Kvs, 1)
	require.True(t, bridge.DialedTarget(endpoints[0]))

	droppedBytesBefore := bridge.DroppedBytes()
	droppedConnectionsBefore := bridge.DroppedConnections()
	bridge.BlackholeResponses()
	revokeDone := make(chan error, 1)
	go func() {
		_, revokeErr := throughBridge.Revoke(ctx, grant.ID)
		revokeDone <- revokeErr
	}()

	// Observe the committed effect through a third replica before forcing the
	// client connection from the first replica to the second one.
	require.Eventually(t, func() bool {
		response, getErr := observer.TimeToLive(ctx, grant.ID)
		return getErr == nil && response.TTL == -1
	}, 5*time.Second, 10*time.Millisecond)
	require.Eventually(t, func() bool {
		return bridge.DroppedBytes() > droppedBytesBefore
	}, 2*time.Second, 10*time.Millisecond)
	responseDiscarded := bridge.DroppedBytes() > droppedBytesBefore
	bridge.SetTarget(endpoints[1])
	bridge.DropConnections()
	bridge.Resume()

	var revokeErr error
	select {
	case revokeErr = <-revokeDone:
	case <-ctx.Done():
		require.NoError(t, ctx.Err())
	}
	require.Eventually(t, func() bool {
		return bridge.DialedTarget(endpoints[1])
	}, 5*time.Second, 10*time.Millisecond)
	after, err := observer.Get(ctx, key)
	require.NoError(t, err)
	ttl, err := observer.TimeToLive(ctx, grant.ID)
	require.NoError(t, err)
	listed, err := observer.Leases(ctx)
	require.NoError(t, err)
	leaseUnlisted := true
	for _, lease := range listed.Leases {
		if lease.ID == grant.ID {
			leaseUnlisted = false
		}
	}

	return leaseRevokeCrossReplicaReplayOutcome{
		ResponseDiscarded:   responseDiscarded,
		ConnectionDropped:   bridge.DroppedConnections() > droppedConnectionsBefore,
		SecondReplicaDialed: bridge.DialedTarget(endpoints[1]),
		LeaseNotFound:       errors.Is(revokeErr, rpctypes.ErrLeaseNotFound),
		KeyDeleted:          len(after.Kvs) == 0,
		LeaseMissing:        ttl.TTL == -1,
		LeaseUnlisted:       leaseUnlisted,
		RevisionDelta:       after.Header.Revision - before.Header.Revision,
	}
}

func runLeaseRevokeRegrantReplayScenario(
	t *testing.T,
	endpoints []string,
	instance string,
) leaseRevokeRegrantReplayOutcome {
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
	prefix := fmt.Sprintf("/dbaas-lease-revoke-regrant-replay/%s/%d/", instance, time.Now().UnixNano())
	originalKey := prefix + "original"
	replacementKey := prefix + "replacement"
	grant, err := throughBridge.Grant(ctx, 60)
	require.NoError(t, err)
	_, err = throughBridge.Put(ctx, originalKey, "old", clientv3.WithLease(grant.ID))
	require.NoError(t, err)
	before, err := observer.Get(ctx, prefix, clientv3.WithPrefix())
	require.NoError(t, err)
	require.Len(t, before.Kvs, 1)

	droppedBefore := bridge.DroppedBytes()
	bridge.BlackholeResponses()
	revokeDone := make(chan error, 1)
	go func() {
		_, revokeErr := throughBridge.Revoke(ctx, grant.ID)
		revokeDone <- revokeErr
	}()
	require.Eventually(t, func() bool {
		response, ttlErr := observer.TimeToLive(ctx, grant.ID)
		return ttlErr == nil && response.TTL == -1
	}, 5*time.Second, 10*time.Millisecond)
	require.Eventually(t, func() bool {
		return bridge.DroppedBytes() > droppedBefore
	}, 2*time.Second, 10*time.Millisecond)

	// Reuse the exact ID while the old call is still waiting behind the response
	// blackhole, then attach a key that only belongs to the replacement.
	replacement, err := etcdserverpb.NewLeaseClient(observer.ActiveConnection()).LeaseGrant(ctx,
		&etcdserverpb.LeaseGrantRequest{ID: int64(grant.ID), TTL: 60})
	require.NoError(t, err)
	require.Equal(t, int64(grant.ID), replacement.ID)
	_, err = observer.Put(ctx, replacementKey, "new", clientv3.WithLease(grant.ID))
	require.NoError(t, err)
	bridge.SetTarget(endpoints[1])
	bridge.DropConnections()
	bridge.Resume()

	var revokeErr error
	select {
	case revokeErr = <-revokeDone:
	case <-ctx.Done():
		require.NoError(t, ctx.Err())
	}
	require.Eventually(t, func() bool { return bridge.DialedTarget(endpoints[1]) }, 5*time.Second, 10*time.Millisecond)
	after, err := observer.Get(ctx, prefix, clientv3.WithPrefix())
	require.NoError(t, err)
	ttl, err := observer.TimeToLive(ctx, grant.ID)
	require.NoError(t, err)

	return leaseRevokeRegrantReplayOutcome{
		ResponseDiscarded:       bridge.DroppedBytes() > droppedBefore,
		SecondReplicaDialed:     bridge.DialedTarget(endpoints[1]),
		ReplayReturnedSuccess:   revokeErr == nil,
		OriginalKeyDeleted:      !containsLeaseReplayKey(after, originalKey),
		ReplacementKeyDeleted:   !containsLeaseReplayKey(after, replacementKey),
		ReplacementLeaseMissing: ttl.TTL == -1,
		RevisionDelta:           after.Header.Revision - before.Header.Revision,
	}
}

func containsLeaseReplayKey(response *clientv3.GetResponse, key string) bool {
	for _, kv := range response.Kvs {
		if string(kv.Key) == key {
			return true
		}
	}
	return false
}
