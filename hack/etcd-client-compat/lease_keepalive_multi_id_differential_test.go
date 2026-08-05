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
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type leaseKeepAliveMultiIDOutcome struct {
	GrantHeaderGaps        []int64
	PutHeaderGaps          []int64
	InitialIDsMatch        bool
	InitialTTLsPositive    bool
	InitialHeaderGaps      []int64
	RevokeHeaderGap        int64
	RevokedIDMatches       bool
	RevokedTTLZero         bool
	RevokedHeaderGap       int64
	SurvivorIDMatches      bool
	SurvivorTTLPositive    bool
	SurvivorHeaderGap      int64
	RevokedKeyDeleted      bool
	SurvivorKeyPreserved   bool
	RevokedLeaseMissing    bool
	SurvivorLeaseStillLive bool
}

func TestLeaseKeepAliveMultiIDRevokeIsolationDifferentialAgainstReferenceEtcd(t *testing.T) {
	reference := os.Getenv("REFERENCE_ETCD_ENDPOINT")
	if reference == "" {
		t.Skip("set REFERENCE_ETCD_ENDPOINT to run differential compatibility tests")
	}

	want := leaseKeepAliveMultiIDOutcome{
		GrantHeaderGaps: []int64{0, 0}, PutHeaderGaps: []int64{1, 2},
		InitialIDsMatch: true, InitialTTLsPositive: true, InitialHeaderGaps: []int64{2, 2},
		RevokeHeaderGap: 3, RevokedIDMatches: true, RevokedTTLZero: true, RevokedHeaderGap: 3,
		SurvivorIDMatches: true, SurvivorTTLPositive: true, SurvivorHeaderGap: 3,
		RevokedKeyDeleted: true, SurvivorKeyPreserved: true,
		RevokedLeaseMissing: true, SurvivorLeaseStillLive: true,
	}
	referenceOutcome := runLeaseKeepAliveMultiIDScenario(t, reference, "reference")
	require.Equal(t, want, referenceOutcome)
	require.Equal(t, referenceOutcome, runLeaseKeepAliveMultiIDScenario(t, compatEndpoint(t), "kubebrain"))
}

func runLeaseKeepAliveMultiIDScenario(t *testing.T, endpoint, instance string) leaseKeepAliveMultiIDOutcome {
	t.Helper()
	endpoint = strings.TrimPrefix(strings.TrimPrefix(endpoint, "http://"), "https://")
	conn, err := grpc.NewClient(endpoint, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, conn.Close()) })
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	t.Cleanup(cancel)
	kv := etcdserverpb.NewKVClient(conn)
	lease := etcdserverpb.NewLeaseClient(conn)
	prefix := fmt.Sprintf("/dbaas-lease-keepalive-multi/%s/%d/", instance, time.Now().UnixNano())
	keyA := []byte(prefix + "a")
	keyB := []byte(prefix + "b")
	seed, err := kv.Put(ctx, &etcdserverpb.PutRequest{Key: []byte(prefix + "seed"), Value: []byte("seed")})
	require.NoError(t, err)
	require.NotNil(t, seed.Header)
	grantA, err := lease.LeaseGrant(ctx, &etcdserverpb.LeaseGrantRequest{TTL: 30})
	require.NoError(t, err)
	grantB, err := lease.LeaseGrant(ctx, &etcdserverpb.LeaseGrantRequest{TTL: 30})
	require.NoError(t, err)
	revokedA := false
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		if !revokedA {
			_, _ = lease.LeaseRevoke(cleanupCtx, &etcdserverpb.LeaseRevokeRequest{ID: grantA.ID})
		}
		_, _ = lease.LeaseRevoke(cleanupCtx, &etcdserverpb.LeaseRevokeRequest{ID: grantB.ID})
		_, _ = kv.DeleteRange(cleanupCtx, &etcdserverpb.DeleteRangeRequest{
			Key: []byte(prefix), RangeEnd: []byte(clientv3.GetPrefixRangeEnd(prefix)),
		})
	})
	putA, err := kv.Put(ctx, &etcdserverpb.PutRequest{Key: keyA, Value: []byte("a"), Lease: grantA.ID})
	require.NoError(t, err)
	putB, err := kv.Put(ctx, &etcdserverpb.PutRequest{Key: keyB, Value: []byte("b"), Lease: grantB.ID})
	require.NoError(t, err)

	stream, err := lease.LeaseKeepAlive(ctx)
	require.NoError(t, err)
	t.Cleanup(func() { _ = stream.CloseSend() })
	keepAlive := func(id int64) *etcdserverpb.LeaseKeepAliveResponse {
		require.NoError(t, stream.Send(&etcdserverpb.LeaseKeepAliveRequest{ID: id}))
		response, recvErr := stream.Recv()
		require.NoError(t, recvErr)
		require.NotNil(t, response.Header)
		return response
	}
	initialA := keepAlive(grantA.ID)
	initialB := keepAlive(grantB.ID)
	revoke, err := lease.LeaseRevoke(ctx, &etcdserverpb.LeaseRevokeRequest{ID: grantA.ID})
	require.NoError(t, err)
	require.NotNil(t, revoke.Header)
	revokedA = true
	missingA := keepAlive(grantA.ID)
	survivorB := keepAlive(grantB.ID)

	rangeA, err := kv.Range(ctx, &etcdserverpb.RangeRequest{Key: keyA})
	require.NoError(t, err)
	rangeB, err := kv.Range(ctx, &etcdserverpb.RangeRequest{Key: keyB})
	require.NoError(t, err)
	ttlA, err := lease.LeaseTimeToLive(ctx, &etcdserverpb.LeaseTimeToLiveRequest{ID: grantA.ID, Keys: true})
	require.NoError(t, err)
	ttlB, err := lease.LeaseTimeToLive(ctx, &etcdserverpb.LeaseTimeToLiveRequest{ID: grantB.ID, Keys: true})
	require.NoError(t, err)
	baseRevision := seed.Header.Revision
	return leaseKeepAliveMultiIDOutcome{
		GrantHeaderGaps:     []int64{grantA.Header.Revision - baseRevision, grantB.Header.Revision - baseRevision},
		PutHeaderGaps:       []int64{putA.Header.Revision - baseRevision, putB.Header.Revision - baseRevision},
		InitialIDsMatch:     initialA.ID == grantA.ID && initialB.ID == grantB.ID,
		InitialTTLsPositive: initialA.TTL > 0 && initialB.TTL > 0,
		InitialHeaderGaps:   []int64{initialA.Header.Revision - baseRevision, initialB.Header.Revision - baseRevision},
		RevokeHeaderGap:     revoke.Header.Revision - baseRevision,
		RevokedIDMatches:    missingA.ID == grantA.ID, RevokedTTLZero: missingA.TTL == 0,
		RevokedHeaderGap:  missingA.Header.Revision - baseRevision,
		SurvivorIDMatches: survivorB.ID == grantB.ID, SurvivorTTLPositive: survivorB.TTL > 0,
		SurvivorHeaderGap:      survivorB.Header.Revision - baseRevision,
		RevokedKeyDeleted:      len(rangeA.Kvs) == 0,
		SurvivorKeyPreserved:   len(rangeB.Kvs) == 1 && string(rangeB.Kvs[0].Value) == "b" && rangeB.Kvs[0].Lease == grantB.ID,
		RevokedLeaseMissing:    ttlA.TTL == -1 && len(ttlA.Keys) == 0,
		SurvivorLeaseStillLive: ttlB.TTL > 0 && len(ttlB.Keys) == 1 && string(ttlB.Keys[0]) == string(keyB),
	}
}
