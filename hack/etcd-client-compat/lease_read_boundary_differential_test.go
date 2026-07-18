package compat

import (
	"context"
	"fmt"
	"os"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type leaseReadBoundaryOutcome struct {
	ZeroID                int64
	ZeroTTL               int64
	ZeroGrantedTTL        int64
	ZeroKeysEmpty         bool
	ZeroHeadersWellFormed bool
	ZeroResponsesMatch    bool
	LiveIDMatches         bool
	LiveTTLWithinGrant    bool
	LiveGrantedTTLMatches bool
	LiveKeysOmitted       bool
	LiveAttachedKeysMatch bool
	ListContainsLive      bool
	ListIDsNonZero        bool
	ListHeaderWellFormed  bool
}

func TestLeaseReadBoundaryDifferentialAgainstReferenceEtcd(t *testing.T) {
	reference := os.Getenv("REFERENCE_ETCD_ENDPOINT")
	if reference == "" {
		t.Skip("set REFERENCE_ETCD_ENDPOINT to run differential compatibility tests")
	}

	want := leaseReadBoundaryOutcome{
		ZeroTTL: -1, ZeroKeysEmpty: true, ZeroHeadersWellFormed: true, ZeroResponsesMatch: true,
		LiveIDMatches: true, LiveTTLWithinGrant: true, LiveGrantedTTLMatches: true,
		LiveKeysOmitted: true, LiveAttachedKeysMatch: true,
		ListContainsLive: true, ListIDsNonZero: true, ListHeaderWellFormed: true,
	}
	referenceOutcome := runLeaseReadBoundaryScenario(t, reference, "etcd")
	require.Equal(t, want, referenceOutcome)
	require.Equal(t, referenceOutcome, runLeaseReadBoundaryScenario(t, compatEndpoint(), "kubebrain"))
}

func runLeaseReadBoundaryScenario(t *testing.T, endpoint, instance string) leaseReadBoundaryOutcome {
	t.Helper()
	endpoint = strings.TrimPrefix(strings.TrimPrefix(endpoint, "http://"), "https://")
	conn, err := grpc.NewClient(endpoint, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, conn.Close()) })
	kv := etcdserverpb.NewKVClient(conn)
	lease := etcdserverpb.NewLeaseClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	t.Cleanup(cancel)

	zeroWithoutKeys, err := lease.LeaseTimeToLive(ctx, &etcdserverpb.LeaseTimeToLiveRequest{})
	require.NoError(t, err)
	zeroWithKeys, err := lease.LeaseTimeToLive(ctx, &etcdserverpb.LeaseTimeToLiveRequest{Keys: true})
	require.NoError(t, err)

	id := time.Now().UnixNano() & ((1 << 62) - 1)
	grant, err := lease.LeaseGrant(ctx, &etcdserverpb.LeaseGrantRequest{ID: id, TTL: 300})
	require.NoError(t, err)
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		_, _ = lease.LeaseRevoke(cleanupCtx, &etcdserverpb.LeaseRevokeRequest{ID: id})
	})
	prefix := fmt.Sprintf("/dbaas-lease-read-boundary/%s/%d/", instance, time.Now().UnixNano())
	keys := []string{prefix + "z", prefix + "a"}
	for _, key := range keys {
		_, err = kv.Put(ctx, &etcdserverpb.PutRequest{Key: []byte(key), Value: []byte("value"), Lease: id})
		require.NoError(t, err)
	}

	liveWithoutKeys, err := lease.LeaseTimeToLive(ctx, &etcdserverpb.LeaseTimeToLiveRequest{ID: id})
	require.NoError(t, err)
	liveWithKeys, err := lease.LeaseTimeToLive(ctx, &etcdserverpb.LeaseTimeToLiveRequest{ID: id, Keys: true})
	require.NoError(t, err)
	list, err := lease.LeaseLeases(ctx, &etcdserverpb.LeaseLeasesRequest{})
	require.NoError(t, err)

	gotKeys := make([]string, 0, len(liveWithKeys.Keys))
	for _, key := range liveWithKeys.Keys {
		gotKeys = append(gotKeys, string(key))
	}
	slices.Sort(keys)
	slices.Sort(gotKeys)
	containsLive := false
	idsNonZero := true
	for _, status := range list.Leases {
		containsLive = containsLive || status.ID == id
		idsNonZero = idsNonZero && status.ID != 0
	}
	return leaseReadBoundaryOutcome{
		ZeroID:                zeroWithoutKeys.ID,
		ZeroTTL:               zeroWithoutKeys.TTL,
		ZeroGrantedTTL:        zeroWithoutKeys.GrantedTTL,
		ZeroKeysEmpty:         len(zeroWithoutKeys.Keys) == 0 && len(zeroWithKeys.Keys) == 0,
		ZeroHeadersWellFormed: leaseReadHeaderWellFormed(zeroWithoutKeys.Header) && leaseReadHeaderWellFormed(zeroWithKeys.Header),
		ZeroResponsesMatch: zeroWithKeys.ID == zeroWithoutKeys.ID &&
			zeroWithKeys.TTL == zeroWithoutKeys.TTL &&
			zeroWithKeys.GrantedTTL == zeroWithoutKeys.GrantedTTL,
		LiveIDMatches:         liveWithoutKeys.ID == id && liveWithKeys.ID == id && grant.ID == id,
		LiveTTLWithinGrant:    liveWithoutKeys.TTL > 0 && liveWithoutKeys.TTL <= 300 && liveWithKeys.TTL > 0 && liveWithKeys.TTL <= 300,
		LiveGrantedTTLMatches: liveWithoutKeys.GrantedTTL == 300 && liveWithKeys.GrantedTTL == 300,
		LiveKeysOmitted:       len(liveWithoutKeys.Keys) == 0,
		LiveAttachedKeysMatch: slices.Equal(gotKeys, keys),
		ListContainsLive:      containsLive,
		ListIDsNonZero:        idsNonZero,
		ListHeaderWellFormed:  leaseReadHeaderWellFormed(list.Header),
	}
}

func leaseReadHeaderWellFormed(header *etcdserverpb.ResponseHeader) bool {
	return header != nil && header.ClusterId != 0 && header.MemberId != 0 &&
		header.Revision > 0 && header.RaftTerm > 0
}
