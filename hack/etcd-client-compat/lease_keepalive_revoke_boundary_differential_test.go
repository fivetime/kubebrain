package compat

import (
	"context"
	"fmt"
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
	Name              string
	Code              string
	Message           string
	IDMatches         bool
	TTLZero           bool
	TTLWithinGrant    bool
	HeaderPositive    bool
	HeaderMatchesSeed bool
	RevisionGap       int64
	SeedValue         string
	LeaseMissing      bool
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
		{Name: "revoke-negative-one", Code: "OK", HeaderPositive: true, HeaderMatchesSeed: true, SeedValue: "seed", LeaseMissing: true},
		leaseRevokeMissingOutcome("revoke-negative-one-again"),
		{Name: "revoke-minimum", Code: "OK", HeaderPositive: true, HeaderMatchesSeed: true, SeedValue: "seed", LeaseMissing: true},
		leaseRevokeMissingOutcome("revoke-minimum-again"),
		{Name: "revoke-maximum", Code: "OK", HeaderPositive: true, HeaderMatchesSeed: true, SeedValue: "seed", LeaseMissing: true},
		leaseRevokeMissingOutcome("revoke-maximum-again"),
		leaseRevokeMissingOutcome("revoke-zero"),
		leaseRevokeMissingOutcome("revoke-unknown"),
	}
	referenceOutcomes := runLeaseKeepAliveRevokeBoundaryScenario(t, reference)
	require.Equal(t, want, referenceOutcomes)
	require.Equal(t, referenceOutcomes, runLeaseKeepAliveRevokeBoundaryScenario(t, compatEndpoint(t)))
}

func keepAliveMissingOutcome(name string) leaseKeepAliveBoundaryOutcome {
	return leaseKeepAliveBoundaryOutcome{
		Name: name, Code: "OK", IDMatches: true, TTLZero: true, HeaderPositive: true,
		HeaderMatchesSeed: true, SeedValue: "seed",
	}
}

func keepAliveLiveOutcome(name string) leaseKeepAliveBoundaryOutcome {
	return leaseKeepAliveBoundaryOutcome{
		Name: name, Code: "OK", IDMatches: true, TTLWithinGrant: true, HeaderPositive: true,
		HeaderMatchesSeed: true, SeedValue: "seed",
	}
}

func leaseRevokeMissingOutcome(name string) leaseKeepAliveBoundaryOutcome {
	return leaseKeepAliveBoundaryOutcome{
		Name: name, Code: "NotFound", Message: "etcdserver: requested lease not found", SeedValue: "seed",
		LeaseMissing: true,
	}
}

func runLeaseKeepAliveRevokeBoundaryScenario(t *testing.T, endpoint string) []leaseKeepAliveBoundaryOutcome {
	t.Helper()
	endpoint = strings.TrimPrefix(strings.TrimPrefix(endpoint, "http://"), "https://")
	conn, err := grpc.NewClient(endpoint, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, conn.Close()) })
	lease := etcdserverpb.NewLeaseClient(conn)
	kv := etcdserverpb.NewKVClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	seedKey := []byte(fmt.Sprintf("/dbaas-lease-keepalive-revoke-boundary/%d/seed", time.Now().UnixNano()))
	seed, err := kv.Put(ctx, &etcdserverpb.PutRequest{Key: seedKey, Value: []byte("seed")})
	require.NoError(t, err)
	require.NotNil(t, seed.Header)
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		_, deleteErr := kv.DeleteRange(cleanupCtx, &etcdserverpb.DeleteRangeRequest{Key: seedKey})
		require.NoError(t, deleteErr)
		deleted, rangeErr := kv.Range(cleanupCtx, &etcdserverpb.RangeRequest{Key: seedKey})
		require.NoError(t, rangeErr)
		require.Empty(t, deleted.Kvs)
	})
	seedState := func(name string) (int64, string) {
		t.Helper()
		resp, rangeErr := kv.Range(ctx, &etcdserverpb.RangeRequest{Key: seedKey})
		require.NoError(t, rangeErr, name)
		require.NotNil(t, resp.Header, name)
		require.Len(t, resp.Kvs, 1, name)
		return resp.Header.Revision - seed.Header.Revision, string(resp.Kvs[0].Value)
	}
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
	grantGap, grantSeed := seedState("grants")
	require.Zero(t, grantGap, "grants")
	require.Equal(t, "seed", grantSeed, "grants")

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
			outcome.HeaderMatchesSeed = resp.Header != nil && resp.Header.Revision == seed.Header.Revision
		}
		outcome.RevisionGap, outcome.SeedValue = seedState(outcome.Name)
		require.Zero(t, outcome.RevisionGap, outcome.Name)
		require.Equal(t, "seed", outcome.SeedValue, outcome.Name)
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
				outcome.HeaderMatchesSeed = resp.Header != nil && resp.Header.Revision == seed.Header.Revision
			}
			outcome.LeaseMissing = leaseIsMissing(t, ctx, lease, test.id)
			outcome.RevisionGap, outcome.SeedValue = seedState(outcome.Name)
			require.Zero(t, outcome.RevisionGap, outcome.Name)
			require.Equal(t, "seed", outcome.SeedValue, outcome.Name)
			outcomes = append(outcomes, outcome)
		}
	}
	for _, test := range []struct {
		name string
		id   int64
	}{{name: "zero", id: 0}, {name: "unknown", id: 13_400}} {
		_, revokeErr := lease.LeaseRevoke(ctx, &etcdserverpb.LeaseRevokeRequest{ID: test.id})
		outcome := leaseKeepAliveBoundaryOutcome{
			Name: "revoke-" + test.name, Code: status.Code(revokeErr).String(),
			Message: status.Convert(revokeErr).Message(),
		}
		outcome.LeaseMissing = leaseIsMissing(t, ctx, lease, test.id)
		outcome.RevisionGap, outcome.SeedValue = seedState(outcome.Name)
		require.Zero(t, outcome.RevisionGap, outcome.Name)
		require.Equal(t, "seed", outcome.SeedValue, outcome.Name)
		outcomes = append(outcomes, outcome)
	}
	return outcomes
}

func leaseIsMissing(t *testing.T, ctx context.Context, lease etcdserverpb.LeaseClient, id int64) bool {
	t.Helper()
	ttl, err := lease.LeaseTimeToLive(ctx, &etcdserverpb.LeaseTimeToLiveRequest{ID: id, Keys: true})
	require.NoError(t, err)
	return ttl.ID == id && ttl.TTL == -1 && ttl.GrantedTTL == 0 && len(ttl.Keys) == 0
}
