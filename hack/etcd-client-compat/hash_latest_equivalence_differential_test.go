package compat

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type hashLatestEquivalenceOutcome struct {
	StableRevisionWindow  bool
	HashStable            bool
	HashKVAtSameRevision  bool
	HashValuesEqual       bool
	HeadersPresent        bool
	HeaderEnvelopesEqual  bool
	HashKVRevisionMatches bool
	LeaseKeepsRevision    bool
	LeaseChangesHash      bool
	LeaseKeepsHashKV      bool
}

func TestHashLatestEquivalenceDifferentialAgainstReferenceEtcd(t *testing.T) {
	reference := os.Getenv("REFERENCE_ETCD_ENDPOINT")
	if reference == "" {
		t.Skip("set REFERENCE_ETCD_ENDPOINT to run differential compatibility tests")
	}
	want := hashLatestEquivalenceOutcome{
		StableRevisionWindow: true, HashStable: true, HashKVAtSameRevision: true,
		HashValuesEqual: false, HeadersPresent: true, HeaderEnvelopesEqual: true,
		HashKVRevisionMatches: true, LeaseKeepsRevision: true, LeaseChangesHash: true,
		LeaseKeepsHashKV: true,
	}
	referenceOutcome := readStableHashLatestEquivalence(t, reference)
	require.Equal(t, want, referenceOutcome)
	require.Equal(t, referenceOutcome, readStableHashLatestEquivalence(t, compatEndpoint(t)))
}

func readStableHashLatestEquivalence(t *testing.T, endpoint string) hashLatestEquivalenceOutcome {
	t.Helper()
	conn, err := grpc.NewClient(grpcTarget(endpoint), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, conn.Close()) })
	client := etcdserverpb.NewMaintenanceClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	beforeOutcome, beforeHash, beforeHashKV := readStableHashPair(t, ctx, client)
	lease := etcdserverpb.NewLeaseClient(conn)
	leaseID := time.Now().UnixNano() & ((1 << 62) - 1)
	grant, err := lease.LeaseGrant(ctx, &etcdserverpb.LeaseGrantRequest{ID: leaseID, TTL: 60})
	require.NoError(t, err)
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		_, _ = lease.LeaseRevoke(cleanupCtx, &etcdserverpb.LeaseRevokeRequest{ID: leaseID})
	})
	afterOutcome, afterHash, afterHashKV := readStableHashPair(t, ctx, client)
	_, err = lease.LeaseRevoke(ctx, &etcdserverpb.LeaseRevokeRequest{ID: leaseID})
	require.NoError(t, err)
	require.Equal(t, beforeOutcome, afterOutcome,
		"granting a detached lease must not change the stable Hash/HashKV structural invariants")

	beforeOutcome.LeaseKeepsRevision = grant.GetHeader().GetRevision() == beforeHash.GetHeader().GetRevision() &&
		afterHash.GetHeader().GetRevision() == beforeHash.GetHeader().GetRevision()
	beforeOutcome.LeaseChangesHash = afterHash.GetHash() != beforeHash.GetHash()
	beforeOutcome.LeaseKeepsHashKV = afterHashKV.GetHash() == beforeHashKV.GetHash() &&
		afterHashKV.GetHashRevision() == beforeHashKV.GetHashRevision()
	return beforeOutcome
}

func readStableHashPair(t *testing.T, ctx context.Context, client etcdserverpb.MaintenanceClient) (
	hashLatestEquivalenceOutcome, *etcdserverpb.HashResponse, *etcdserverpb.HashKVResponse,
) {
	t.Helper()

	for attempt := 0; attempt < 10; attempt++ {
		before, err := client.Hash(ctx, &etcdserverpb.HashRequest{})
		require.NoError(t, err)
		latest, err := client.HashKV(ctx, &etcdserverpb.HashKVRequest{})
		require.NoError(t, err)
		after, err := client.Hash(ctx, &etcdserverpb.HashRequest{})
		require.NoError(t, err)
		if before.GetHeader().GetRevision() != after.GetHeader().GetRevision() {
			continue
		}
		beforeHeader, latestHeader, afterHeader := before.GetHeader(), latest.GetHeader(), after.GetHeader()
		return hashLatestEquivalenceOutcome{
			StableRevisionWindow: true,
			HashStable:           before.GetHash() == after.GetHash(),
			HashKVAtSameRevision: latestHeader.GetRevision() == beforeHeader.GetRevision(),
			HashValuesEqual:      before.GetHash() == latest.GetHash() && latest.GetHash() == after.GetHash(),
			HeadersPresent:       beforeHeader != nil && latestHeader != nil && afterHeader != nil,
			HeaderEnvelopesEqual: sameMaintenanceHeaderEnvelope(beforeHeader, latestHeader) &&
				sameMaintenanceHeaderEnvelope(latestHeader, afterHeader),
			HashKVRevisionMatches: latest.GetHashRevision() == latestHeader.GetRevision(),
		}, before, latest
	}
	return hashLatestEquivalenceOutcome{}, nil, nil
}

func sameMaintenanceHeaderEnvelope(a, b *etcdserverpb.ResponseHeader) bool {
	return a != nil && b != nil &&
		a.GetClusterId() == b.GetClusterId() && a.GetMemberId() == b.GetMemberId() &&
		a.GetRevision() == b.GetRevision() && a.GetRaftTerm() == b.GetRaftTerm()
}
