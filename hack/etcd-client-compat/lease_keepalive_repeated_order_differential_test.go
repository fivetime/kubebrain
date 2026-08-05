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

type leaseKeepAliveRepeatedOrderOutcome struct {
	ResponseOrder []string
	InitialOrder  []string
	AfterAOrder   []string
	AfterBOrder   []string
	TTLsPositive  bool
	RevisionGaps  []int64
}

func TestLeaseKeepAliveRepeatedOrderDifferentialAgainstReferenceEtcd(t *testing.T) {
	reference := os.Getenv("REFERENCE_ETCD_ENDPOINT")
	if reference == "" {
		t.Skip("set REFERENCE_ETCD_ENDPOINT to run differential compatibility tests")
	}

	want := leaseKeepAliveRepeatedOrderOutcome{
		ResponseOrder: []string{"a", "b", "a", "b"},
		InitialOrder:  []string{"a", "b"},
		AfterAOrder:   []string{"b", "a"},
		AfterBOrder:   []string{"a", "b"},
		TTLsPositive:  true,
		RevisionGaps:  []int64{0, 0, 0, 0},
	}
	referenceOutcome := runLeaseKeepAliveRepeatedOrderScenario(t, reference)
	require.Equal(t, want, referenceOutcome)
	require.Equal(t, referenceOutcome, runLeaseKeepAliveRepeatedOrderScenario(t, compatEndpoint(t)))
}

func runLeaseKeepAliveRepeatedOrderScenario(t *testing.T, endpoint string) leaseKeepAliveRepeatedOrderOutcome {
	t.Helper()
	conn, err := grpc.NewClient(endpoint, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, conn.Close()) })
	lease := etcdserverpb.NewLeaseClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	t.Cleanup(cancel)

	grantA, err := lease.LeaseGrant(ctx, &etcdserverpb.LeaseGrantRequest{TTL: 300})
	require.NoError(t, err)
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		_, _ = lease.LeaseRevoke(cleanupCtx, &etcdserverpb.LeaseRevokeRequest{ID: grantA.ID})
	})
	time.Sleep(20 * time.Millisecond)
	grantB, err := lease.LeaseGrant(ctx, &etcdserverpb.LeaseGrantRequest{TTL: 300})
	require.NoError(t, err)
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		_, _ = lease.LeaseRevoke(cleanupCtx, &etcdserverpb.LeaseRevokeRequest{ID: grantB.ID})
	})

	names := map[int64]string{grantA.ID: "a", grantB.ID: "b"}
	listOrder := func() []string {
		listed, listErr := lease.LeaseLeases(ctx, &etcdserverpb.LeaseLeasesRequest{})
		require.NoError(t, listErr)
		order := make([]string, 0, 2)
		for _, status := range listed.Leases {
			if name, ok := names[status.ID]; ok {
				order = append(order, name)
			}
		}
		return order
	}

	outcome := leaseKeepAliveRepeatedOrderOutcome{InitialOrder: listOrder(), TTLsPositive: true}
	stream, err := lease.LeaseKeepAlive(ctx)
	require.NoError(t, err)
	request := func(id int64) {
		require.NoError(t, stream.Send(&etcdserverpb.LeaseKeepAliveRequest{ID: id}))
		response, recvErr := stream.Recv()
		require.NoError(t, recvErr)
		require.NotNil(t, response.Header)
		outcome.ResponseOrder = append(outcome.ResponseOrder, names[response.ID])
		outcome.TTLsPositive = outcome.TTLsPositive && response.TTL > 0
		outcome.RevisionGaps = append(outcome.RevisionGaps, response.Header.Revision-grantA.Header.Revision)
	}

	request(grantA.ID)
	request(grantB.ID)
	time.Sleep(20 * time.Millisecond)
	request(grantA.ID)
	outcome.AfterAOrder = listOrder()
	time.Sleep(20 * time.Millisecond)
	request(grantB.ID)
	outcome.AfterBOrder = listOrder()
	require.NoError(t, stream.CloseSend())
	return outcome
}
