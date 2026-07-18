package compat

import (
	"context"
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
	Name      string
	Code      string
	Message   string
	IDMatches bool
	IDNonZero bool
	TTL       int64
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
		{Name: "automatic-id", Code: "OK", IDNonZero: true, TTL: 10},
		leaseGrantError("duplicate-id", "FailedPrecondition", "etcdserver: lease already exists"),
	}
	referenceOutcomes := runLeaseGrantBoundaryScenario(t, reference)
	require.Equal(t, want, referenceOutcomes)
	require.Equal(t, referenceOutcomes, runLeaseGrantBoundaryScenario(t, compatEndpoint()))
}

func successfulFixedLeaseGrant(name string, ttl int64) leaseGrantBoundaryOutcome {
	return leaseGrantBoundaryOutcome{Name: name, Code: "OK", IDMatches: true, IDNonZero: true, TTL: ttl}
}

func leaseGrantError(name, code, message string) leaseGrantBoundaryOutcome {
	return leaseGrantBoundaryOutcome{Name: name, Code: code, Message: message}
}

func runLeaseGrantBoundaryScenario(t *testing.T, endpoint string) []leaseGrantBoundaryOutcome {
	t.Helper()
	endpoint = strings.TrimPrefix(strings.TrimPrefix(endpoint, "http://"), "https://")
	conn, err := grpc.NewClient(endpoint, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, conn.Close()) })
	lease := etcdserverpb.NewLeaseClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
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
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cleanupCancel()
		for _, id := range grantedIDs {
			_, _ = lease.LeaseRevoke(cleanupCtx, &etcdserverpb.LeaseRevokeRequest{ID: id})
		}
	})
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
		outcomes = append(outcomes, outcome)
	}

	const duplicateID = int64(13_309)
	first, err := lease.LeaseGrant(ctx, &etcdserverpb.LeaseGrantRequest{ID: duplicateID, TTL: 10})
	require.NoError(t, err)
	grantedIDs = append(grantedIDs, first.ID)
	_, duplicateErr := lease.LeaseGrant(ctx, &etcdserverpb.LeaseGrantRequest{ID: duplicateID, TTL: 20})
	outcomes = append(outcomes, leaseGrantBoundaryOutcome{
		Name: "duplicate-id", Code: status.Code(duplicateErr).String(),
		Message: status.Convert(duplicateErr).Message(),
	})
	return outcomes
}
