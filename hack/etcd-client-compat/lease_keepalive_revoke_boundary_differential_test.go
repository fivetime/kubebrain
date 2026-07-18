package compat

import (
	"context"
	"io"
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

type leaseKeepAliveBoundaryOutcome struct {
	Name           string
	Code           string
	Message        string
	IDMatches      bool
	TTLZero        bool
	TTLWithinGrant bool
	HeaderPositive bool
}

func TestLeaseKeepAliveRevokeBoundaryDifferentialAgainstReferenceEtcd(t *testing.T) {
	reference := os.Getenv("REFERENCE_ETCD_ENDPOINT")
	if reference == "" {
		t.Skip("set REFERENCE_ETCD_ENDPOINT to run differential compatibility tests")
	}

	want := []leaseKeepAliveBoundaryOutcome{
		keepAliveMissingOutcome("keepalive-zero"),
		keepAliveMissingOutcome("keepalive-unknown"),
		keepAliveLiveOutcome("keepalive-negative-one"),
		keepAliveLiveOutcome("keepalive-minimum"),
		keepAliveLiveOutcome("keepalive-maximum"),
		{Name: "revoke-negative-one", Code: "OK", HeaderPositive: true},
		leaseRevokeMissingOutcome("revoke-negative-one-again"),
		{Name: "revoke-minimum", Code: "OK", HeaderPositive: true},
		leaseRevokeMissingOutcome("revoke-minimum-again"),
		{Name: "revoke-maximum", Code: "OK", HeaderPositive: true},
		leaseRevokeMissingOutcome("revoke-maximum-again"),
		leaseRevokeMissingOutcome("revoke-zero"),
		leaseRevokeMissingOutcome("revoke-unknown"),
	}
	referenceOutcomes := runLeaseKeepAliveRevokeBoundaryScenario(t, reference)
	require.Equal(t, want, referenceOutcomes)
	require.Equal(t, referenceOutcomes, runLeaseKeepAliveRevokeBoundaryScenario(t, compatEndpoint()))
}

func keepAliveMissingOutcome(name string) leaseKeepAliveBoundaryOutcome {
	return leaseKeepAliveBoundaryOutcome{
		Name: name, Code: "OK", IDMatches: true, TTLZero: true, HeaderPositive: true,
	}
}

func keepAliveLiveOutcome(name string) leaseKeepAliveBoundaryOutcome {
	return leaseKeepAliveBoundaryOutcome{
		Name: name, Code: "OK", IDMatches: true, TTLWithinGrant: true, HeaderPositive: true,
	}
}

func leaseRevokeMissingOutcome(name string) leaseKeepAliveBoundaryOutcome {
	return leaseKeepAliveBoundaryOutcome{
		Name: name, Code: "NotFound", Message: "etcdserver: requested lease not found",
	}
}

func runLeaseKeepAliveRevokeBoundaryScenario(t *testing.T, endpoint string) []leaseKeepAliveBoundaryOutcome {
	t.Helper()
	endpoint = strings.TrimPrefix(strings.TrimPrefix(endpoint, "http://"), "https://")
	conn, err := grpc.NewClient(endpoint, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, conn.Close()) })
	lease := etcdserverpb.NewLeaseClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	live := []struct {
		name string
		id   int64
	}{
		{name: "negative-one", id: -1},
		{name: "minimum", id: math.MinInt64},
		{name: "maximum", id: math.MaxInt64},
	}
	granted := make([]int64, 0, len(live))
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cleanupCancel()
		for _, id := range granted {
			_, _ = lease.LeaseRevoke(cleanupCtx, &etcdserverpb.LeaseRevokeRequest{ID: id})
		}
	})
	for _, test := range live {
		resp, grantErr := lease.LeaseGrant(ctx, &etcdserverpb.LeaseGrantRequest{ID: test.id, TTL: 30})
		require.NoError(t, grantErr)
		require.Equal(t, test.id, resp.ID)
		granted = append(granted, test.id)
	}

	stream, err := lease.LeaseKeepAlive(ctx)
	require.NoError(t, err)
	outcomes := make([]leaseKeepAliveBoundaryOutcome, 0, 13)
	keepAliveCases := []struct {
		name string
		id   int64
		live bool
	}{
		{name: "zero", id: 0},
		{name: "unknown", id: 13_400},
		{name: "negative-one", id: -1, live: true},
		{name: "minimum", id: math.MinInt64, live: true},
		{name: "maximum", id: math.MaxInt64, live: true},
	}
	for _, test := range keepAliveCases {
		require.NoError(t, stream.Send(&etcdserverpb.LeaseKeepAliveRequest{ID: test.id}))
		resp, recvErr := stream.Recv()
		outcome := leaseKeepAliveBoundaryOutcome{
			Name: "keepalive-" + test.name, Code: status.Code(recvErr).String(),
			Message: status.Convert(recvErr).Message(),
		}
		if recvErr == nil {
			outcome.IDMatches = resp.ID == test.id
			outcome.TTLZero = !test.live && resp.TTL == 0
			outcome.TTLWithinGrant = test.live && resp.TTL > 0 && resp.TTL <= 30
			outcome.HeaderPositive = resp.Header != nil && resp.Header.Revision > 0
		}
		outcomes = append(outcomes, outcome)
	}
	require.NoError(t, stream.CloseSend())
	_, err = stream.Recv()
	require.ErrorIs(t, err, io.EOF)

	for _, test := range live {
		for attempt := 0; attempt < 2; attempt++ {
			resp, revokeErr := lease.LeaseRevoke(ctx, &etcdserverpb.LeaseRevokeRequest{ID: test.id})
			outcome := leaseKeepAliveBoundaryOutcome{
				Name: "revoke-" + test.name, Code: status.Code(revokeErr).String(),
				Message: status.Convert(revokeErr).Message(),
			}
			if attempt == 1 {
				outcome.Name += "-again"
			}
			if revokeErr == nil {
				outcome.HeaderPositive = resp.Header != nil && resp.Header.Revision > 0
			}
			outcomes = append(outcomes, outcome)
		}
	}
	for _, test := range []struct {
		name string
		id   int64
	}{{name: "zero", id: 0}, {name: "unknown", id: 13_400}} {
		_, revokeErr := lease.LeaseRevoke(ctx, &etcdserverpb.LeaseRevokeRequest{ID: test.id})
		outcomes = append(outcomes, leaseKeepAliveBoundaryOutcome{
			Name: "revoke-" + test.name, Code: status.Code(revokeErr).String(),
			Message: status.Convert(revokeErr).Message(),
		})
	}
	return outcomes
}
