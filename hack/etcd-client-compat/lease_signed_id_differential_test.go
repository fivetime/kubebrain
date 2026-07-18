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
)

type leaseSignedIDOutcome struct {
	Name                    string
	GrantIDMatches          bool
	GrantHeaderUnchanged    bool
	TTLIDMatches            bool
	TTLWithinGrant          bool
	GrantedTTLMatches       bool
	KeysOmitted             bool
	AttachedKeysMatch       bool
	ListContainsLease       bool
	UnknownIDMatches        bool
	UnknownTTL              int64
	UnknownGrantedTTL       int64
	UnknownKeysEmpty        bool
	HeaderAtCurrentRevision bool
}

func TestLeaseSignedIDDifferentialAgainstReferenceEtcd(t *testing.T) {
	reference := os.Getenv("REFERENCE_ETCD_ENDPOINT")
	if reference == "" {
		t.Skip("set REFERENCE_ETCD_ENDPOINT to run differential compatibility tests")
	}

	want := []leaseSignedIDOutcome{
		signedLeaseExpectedOutcome("negative-one"),
		signedLeaseExpectedOutcome("minimum"),
		signedLeaseExpectedOutcome("maximum"),
	}
	referenceOutcomes := runLeaseSignedIDScenario(t, reference, "etcd")
	require.Equal(t, want, referenceOutcomes)
	require.Equal(t, referenceOutcomes, runLeaseSignedIDScenario(t, compatEndpoint(), "kubebrain"))
}

func signedLeaseExpectedOutcome(name string) leaseSignedIDOutcome {
	return leaseSignedIDOutcome{
		Name:                    name,
		GrantIDMatches:          true,
		GrantHeaderUnchanged:    true,
		TTLIDMatches:            true,
		TTLWithinGrant:          true,
		GrantedTTLMatches:       true,
		KeysOmitted:             true,
		AttachedKeysMatch:       true,
		ListContainsLease:       true,
		UnknownIDMatches:        true,
		UnknownTTL:              -1,
		UnknownKeysEmpty:        true,
		HeaderAtCurrentRevision: true,
	}
}

func runLeaseSignedIDScenario(t *testing.T, endpoint, instance string) []leaseSignedIDOutcome {
	t.Helper()
	endpoint = strings.TrimPrefix(strings.TrimPrefix(endpoint, "http://"), "https://")
	conn, err := grpc.NewClient(endpoint, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, conn.Close()) })
	kv := etcdserverpb.NewKVClient(conn)
	lease := etcdserverpb.NewLeaseClient(conn)

	tests := []struct {
		name string
		id   int64
	}{
		{name: "negative-one", id: -1},
		{name: "minimum", id: math.MinInt64},
		{name: "maximum", id: math.MaxInt64},
	}
	outcomes := make([]leaseSignedIDOutcome, 0, len(tests))
	for i, test := range tests {
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		key := fmt.Sprintf("/dbaas-lease-signed-id/%s/%d/%d", instance, time.Now().UnixNano(), i)
		before, beforeErr := kv.Range(ctx, &etcdserverpb.RangeRequest{Key: []byte(key)})
		require.NoError(t, beforeErr)
		grant, grantErr := lease.LeaseGrant(ctx, &etcdserverpb.LeaseGrantRequest{ID: test.id, TTL: 300})
		require.NoError(t, grantErr)
		put, putErr := kv.Put(ctx, &etcdserverpb.PutRequest{
			Key: []byte(key), Value: []byte("value"), Lease: test.id,
		})
		require.NoError(t, putErr)
		withoutKeys, ttlErr := lease.LeaseTimeToLive(ctx, &etcdserverpb.LeaseTimeToLiveRequest{ID: test.id})
		require.NoError(t, ttlErr)
		withKeys, keysErr := lease.LeaseTimeToLive(ctx, &etcdserverpb.LeaseTimeToLiveRequest{
			ID: test.id, Keys: true,
		})
		require.NoError(t, keysErr)
		list, listErr := lease.LeaseLeases(ctx, &etcdserverpb.LeaseLeasesRequest{})
		require.NoError(t, listErr)
		contains := false
		for _, item := range list.Leases {
			contains = contains || item.ID == test.id
		}
		_, revokeErr := lease.LeaseRevoke(ctx, &etcdserverpb.LeaseRevokeRequest{ID: test.id})
		require.NoError(t, revokeErr)
		unknown, unknownErr := lease.LeaseTimeToLive(ctx, &etcdserverpb.LeaseTimeToLiveRequest{
			ID: test.id, Keys: true,
		})
		require.NoError(t, unknownErr)

		outcomes = append(outcomes, leaseSignedIDOutcome{
			Name:                    test.name,
			GrantIDMatches:          grant.ID == test.id,
			GrantHeaderUnchanged:    grant.Header.Revision == before.Header.Revision,
			TTLIDMatches:            withoutKeys.ID == test.id && withKeys.ID == test.id,
			TTLWithinGrant:          withoutKeys.TTL > 0 && withoutKeys.TTL <= 300 && withKeys.TTL > 0 && withKeys.TTL <= 300,
			GrantedTTLMatches:       withoutKeys.GrantedTTL == 300 && withKeys.GrantedTTL == 300,
			KeysOmitted:             len(withoutKeys.Keys) == 0,
			AttachedKeysMatch:       len(withKeys.Keys) == 1 && string(withKeys.Keys[0]) == key,
			ListContainsLease:       contains,
			UnknownIDMatches:        unknown.ID == test.id,
			UnknownTTL:              unknown.TTL,
			UnknownGrantedTTL:       unknown.GrantedTTL,
			UnknownKeysEmpty:        len(unknown.Keys) == 0,
			HeaderAtCurrentRevision: unknown.Header.Revision >= put.Header.Revision,
		})
		cancel()
	}
	return outcomes
}
