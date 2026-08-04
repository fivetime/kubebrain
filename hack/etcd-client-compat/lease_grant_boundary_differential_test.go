package compat

import (
	"context"
	"fmt"
	"math"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

const referenceMaxLeaseTTL = int64(9_000_000_000)

type leaseGrantBoundaryOutcome struct {
	Name                string
	Code                string
	Message             string
	IDMatches           bool
	IDNonZero           bool
	TTL                 int64
	RevisionGap         int64
	SeedValue           string
	LeaseStatePreserved bool
}

func TestLeaseGrantBoundaryDifferentialAgainstReferenceEtcd(t *testing.T) {
	reference := os.Getenv("REFERENCE_ETCD_ENDPOINT")
	if reference == "" {
		t.Skip("set REFERENCE_ETCD_ENDPOINT to run differential compatibility tests")
	}

	want := []leaseGrantBoundaryOutcome{
		successfulFixedLeaseGrant("minimum-int64", 2),
		successfulFixedLeaseGrant("negative-one", 2),
		successfulFixedLeaseGrant("zero", 2),
		successfulFixedLeaseGrant("one", 2),
		successfulFixedLeaseGrant("minimum", 2),
		successfulFixedLeaseGrant("maximum", referenceMaxLeaseTTL),
		leaseGrantError("above-maximum", "OutOfRange", "etcdserver: too large lease TTL"),
		leaseGrantError("maximum-int64", "OutOfRange", "etcdserver: too large lease TTL"),
		{Name: "automatic-id", Code: "OK", IDNonZero: true, TTL: 10, SeedValue: "seed"},
		leaseGrantError("duplicate-id", "FailedPrecondition", "etcdserver: lease already exists"),
		leaseGrantError("duplicate-id-above-maximum", "OutOfRange", "etcdserver: too large lease TTL"),
	}
	referenceOutcomes := runLeaseGrantBoundaryScenario(t, reference)
	require.Equal(t, want, referenceOutcomes)
	require.Equal(t, referenceOutcomes, runLeaseGrantBoundaryScenario(t, compatEndpoint(t)))
}

func successfulFixedLeaseGrant(name string, ttl int64) leaseGrantBoundaryOutcome {
	return leaseGrantBoundaryOutcome{Name: name, Code: "OK", IDMatches: true, IDNonZero: true, TTL: ttl, SeedValue: "seed"}
}

func leaseGrantError(name, code, message string) leaseGrantBoundaryOutcome {
	return leaseGrantBoundaryOutcome{
		Name: name, Code: code, Message: message, SeedValue: "seed", LeaseStatePreserved: true,
	}
}

func runLeaseGrantBoundaryScenario(t *testing.T, endpoint string) []leaseGrantBoundaryOutcome {
	t.Helper()
	endpoint = strings.TrimPrefix(strings.TrimPrefix(endpoint, "http://"), "https://")
	conn, err := grpc.NewClient(endpoint, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, conn.Close()) })
	lease := etcdserverpb.NewLeaseClient(conn)
	kv := etcdserverpb.NewKVClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	seedKey := []byte(fmt.Sprintf("/dbaas-lease-grant-boundary/%d/seed", time.Now().UnixNano()))
	seed, err := kv.Put(ctx, &etcdserverpb.PutRequest{Key: seedKey, Value: []byte("seed")})
	require.NoError(t, err)
	require.NotNil(t, seed.Header)
	seedState := func(name string) (int64, string) {
		t.Helper()
		resp, rangeErr := kv.Range(ctx, &etcdserverpb.RangeRequest{Key: seedKey})
		require.NoError(t, rangeErr, name)
		require.NotNil(t, resp.Header, name)
		require.Len(t, resp.Kvs, 1, name)
		return resp.Header.Revision - seed.Header.Revision, string(resp.Kvs[0].Value)
	}
	grants := []struct {
		name string
		id   int64
		ttl  int64
	}{
		{name: "minimum-int64", id: 13_301, ttl: math.MinInt64},
		{name: "negative-one", id: 13_302, ttl: -1},
		{name: "zero", id: 13_303, ttl: 0},
		{name: "one", id: 13_304, ttl: 1},
		{name: "minimum", id: 13_305, ttl: 2},
		{name: "maximum", id: 13_306, ttl: referenceMaxLeaseTTL},
		{name: "above-maximum", id: 13_307, ttl: referenceMaxLeaseTTL + 1},
		{name: "maximum-int64", id: 13_308, ttl: math.MaxInt64},
		{name: "automatic-id", id: 0, ttl: 10},
	}
	outcomes := make([]leaseGrantBoundaryOutcome, 0, len(grants)+1)
	grantedIDs := make([]int64, 0, len(grants))
	defer func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cleanupCancel()
		for _, id := range grantedIDs {
			_, revokeErr := lease.LeaseRevoke(cleanupCtx, &etcdserverpb.LeaseRevokeRequest{ID: id})
			require.NoError(t, revokeErr)
		}
		cleanupGap, cleanupSeed := seedState("cleanup-revoke")
		require.Zero(t, cleanupGap, "cleanup-revoke")
		require.Equal(t, "seed", cleanupSeed, "cleanup-revoke")
	}()
	for _, test := range grants {
		resp, callErr := lease.LeaseGrant(ctx, &etcdserverpb.LeaseGrantRequest{ID: test.id, TTL: test.ttl})
		outcome := leaseGrantBoundaryOutcome{
			Name: test.name, Code: status.Code(callErr).String(), Message: status.Convert(callErr).Message(),
		}
		if callErr == nil {
			outcome.IDMatches = test.id != 0 && resp.ID == test.id
			outcome.IDNonZero = resp.ID != 0
			outcome.TTL = resp.TTL
			grantedIDs = append(grantedIDs, resp.ID)
		}
		outcome.RevisionGap, outcome.SeedValue = seedState(test.name)
		if callErr != nil {
			ttl, ttlErr := lease.LeaseTimeToLive(ctx, &etcdserverpb.LeaseTimeToLiveRequest{ID: test.id})
			require.NoError(t, ttlErr, test.name)
			outcome.LeaseStatePreserved = ttl.TTL == -1 && ttl.GrantedTTL == 0 && len(ttl.Keys) == 0
		}
		require.Zero(t, outcome.RevisionGap, test.name)
		require.Equal(t, "seed", outcome.SeedValue, test.name)
		outcomes = append(outcomes, outcome)
	}

	const duplicateID = int64(13_309)
	first, err := lease.LeaseGrant(ctx, &etcdserverpb.LeaseGrantRequest{ID: duplicateID, TTL: 10})
	require.NoError(t, err)
	grantedIDs = append(grantedIDs, first.ID)
	_, duplicateErr := lease.LeaseGrant(ctx, &etcdserverpb.LeaseGrantRequest{ID: duplicateID, TTL: 20})
	duplicateGap, duplicateSeed := seedState("duplicate-id")
	require.Zero(t, duplicateGap, "duplicate-id")
	require.Equal(t, "seed", duplicateSeed, "duplicate-id")
	outcomes = append(outcomes, leaseGrantBoundaryOutcome{
		Name: "duplicate-id", Code: status.Code(duplicateErr).String(),
		Message: status.Convert(duplicateErr).Message(), RevisionGap: duplicateGap, SeedValue: duplicateSeed,
		LeaseStatePreserved: leaseGrantStateMatches(t, ctx, lease, duplicateID, 10),
	})
	_, duplicateTTLTooLargeErr := lease.LeaseGrant(ctx, &etcdserverpb.LeaseGrantRequest{
		ID: duplicateID, TTL: referenceMaxLeaseTTL + 1,
	})
	duplicateTTLTooLargeGap, duplicateTTLTooLargeSeed := seedState("duplicate-id-above-maximum")
	require.Zero(t, duplicateTTLTooLargeGap, "duplicate-id-above-maximum")
	require.Equal(t, "seed", duplicateTTLTooLargeSeed, "duplicate-id-above-maximum")
	outcomes = append(outcomes, leaseGrantBoundaryOutcome{
		Name: "duplicate-id-above-maximum", Code: status.Code(duplicateTTLTooLargeErr).String(),
		Message: status.Convert(duplicateTTLTooLargeErr).Message(), RevisionGap: duplicateTTLTooLargeGap,
		SeedValue:           duplicateTTLTooLargeSeed,
		LeaseStatePreserved: leaseGrantStateMatches(t, ctx, lease, duplicateID, 10),
	})
	return outcomes
}

func leaseGrantStateMatches(t *testing.T, ctx context.Context, lease etcdserverpb.LeaseClient, id, grantedTTL int64) bool {
	t.Helper()
	ttl, err := lease.LeaseTimeToLive(ctx, &etcdserverpb.LeaseTimeToLiveRequest{ID: id})
	require.NoError(t, err)
	return ttl.ID == id && ttl.TTL > 0 && ttl.TTL <= grantedTTL && ttl.GrantedTTL == grantedTTL && len(ttl.Keys) == 0
}
