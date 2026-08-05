package compat

import (
	"context"
	"io"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type leaseKeepAliveHalfCloseOutcome struct {
	ResponseKinds []string
	TTLKinds      []string
	HeaderGaps    []int64
	FinalOrder    []string
	EndsWithEOF   bool
}

func TestLeaseKeepAliveHalfCloseDifferentialAgainstReferenceEtcd(t *testing.T) {
	reference := os.Getenv("REFERENCE_ETCD_ENDPOINT")
	if reference == "" {
		t.Skip("set REFERENCE_ETCD_ENDPOINT to run differential compatibility tests")
	}

	want := leaseKeepAliveHalfCloseOutcome{
		ResponseKinds: []string{"a", "zero", "b", "unknown", "a", "b"},
		TTLKinds:      []string{"live", "missing", "live", "missing", "live", "live"},
		HeaderGaps:    []int64{0, 0, 0, 0, 0, 0},
		FinalOrder:    []string{"a", "b"},
		EndsWithEOF:   true,
	}
	referenceOutcome := runLeaseKeepAliveHalfCloseScenario(t, reference)
	require.Equal(t, want, referenceOutcome)
	require.Equal(t, referenceOutcome, runLeaseKeepAliveHalfCloseScenario(t, compatEndpoint(t)))
}

func runLeaseKeepAliveHalfCloseScenario(t *testing.T, endpoint string) leaseKeepAliveHalfCloseOutcome {
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

	const unknownID int64 = 13_477
	names := map[int64]string{grantA.ID: "a", 0: "zero", grantB.ID: "b", unknownID: "unknown"}
	requests := []int64{grantA.ID, 0, grantB.ID, unknownID, grantA.ID, grantB.ID}
	stream, err := lease.LeaseKeepAlive(ctx)
	require.NoError(t, err)
	for _, id := range requests {
		require.NoError(t, stream.Send(&etcdserverpb.LeaseKeepAliveRequest{ID: id}))
	}
	require.NoError(t, stream.CloseSend())

	outcome := leaseKeepAliveHalfCloseOutcome{}
	for range requests {
		response, recvErr := stream.Recv()
		require.NoError(t, recvErr)
		require.NotNil(t, response.Header)
		outcome.ResponseKinds = append(outcome.ResponseKinds, names[response.ID])
		if response.TTL > 0 {
			outcome.TTLKinds = append(outcome.TTLKinds, "live")
		} else {
			outcome.TTLKinds = append(outcome.TTLKinds, "missing")
		}
		outcome.HeaderGaps = append(outcome.HeaderGaps, response.Header.Revision-grantA.Header.Revision)
	}
	_, err = stream.Recv()
	outcome.EndsWithEOF = err == io.EOF

	listed, err := lease.LeaseLeases(ctx, &etcdserverpb.LeaseLeasesRequest{})
	require.NoError(t, err)
	for _, status := range listed.Leases {
		if name, ok := names[status.ID]; ok && (name == "a" || name == "b") {
			outcome.FinalOrder = append(outcome.FinalOrder, name)
		}
	}
	return outcome
}
