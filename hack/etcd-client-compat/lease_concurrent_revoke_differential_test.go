package compat

import (
	"context"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

type leaseConcurrentRevokeOutcome struct {
	Successes           int
	LeaseNotFound       int
	OtherErrors         int
	SuccessfulHeaderGap int64
	FinalRevisionGap    int64
	LeasedKeysDeleted   bool
	SeedPreserved       bool
	LeaseMissing        bool
	LeaseAbsentFromList bool
}

func TestLeaseConcurrentRevokeDifferentialAgainstReferenceEtcd(t *testing.T) {
	reference := os.Getenv("REFERENCE_ETCD_ENDPOINT")
	if reference == "" {
		t.Skip("set REFERENCE_ETCD_ENDPOINT to run differential compatibility tests")
	}

	want := leaseConcurrentRevokeOutcome{
		Successes: 1, LeaseNotFound: 15,
		SuccessfulHeaderGap: 3, FinalRevisionGap: 3,
		LeasedKeysDeleted: true, SeedPreserved: true, LeaseMissing: true, LeaseAbsentFromList: true,
	}
	referenceOutcome := runLeaseConcurrentRevokeScenario(t, reference)
	require.Equal(t, want, referenceOutcome)
	require.Equal(t, referenceOutcome, runLeaseConcurrentRevokeScenario(t, compatEndpoint(t)))
}

func runLeaseConcurrentRevokeScenario(t *testing.T, endpoint string) leaseConcurrentRevokeOutcome {
	t.Helper()
	conn, err := grpc.NewClient(endpoint, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, conn.Close()) })
	kv := etcdserverpb.NewKVClient(conn)
	lease := etcdserverpb.NewLeaseClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	t.Cleanup(cancel)

	prefix := []byte(testPrefix(t) + "/lease-concurrent-revoke/")
	seedKey := append(append([]byte(nil), prefix...), "seed"...)
	seed, err := kv.Put(ctx, &etcdserverpb.PutRequest{Key: seedKey, Value: []byte("seed")})
	require.NoError(t, err)
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		_, _ = kv.DeleteRange(cleanupCtx, &etcdserverpb.DeleteRangeRequest{
			Key: prefix, RangeEnd: []byte(clientPrefixRangeEnd(string(prefix))),
		})
	})

	grant, err := lease.LeaseGrant(ctx, &etcdserverpb.LeaseGrantRequest{TTL: 300})
	require.NoError(t, err)
	for _, suffix := range []string{"a", "b"} {
		key := append(append([]byte(nil), prefix...), suffix...)
		_, err = kv.Put(ctx, &etcdserverpb.PutRequest{Key: key, Value: []byte("leased"), Lease: grant.ID})
		require.NoError(t, err)
	}

	const contenders = 16
	type result struct {
		response *etcdserverpb.LeaseRevokeResponse
		err      error
	}
	results := make(chan result, contenders)
	start := make(chan struct{})
	var ready sync.WaitGroup
	ready.Add(contenders)
	for range contenders {
		go func() {
			ready.Done()
			<-start
			response, callErr := lease.LeaseRevoke(ctx, &etcdserverpb.LeaseRevokeRequest{ID: grant.ID})
			results <- result{response: response, err: callErr}
		}()
	}
	ready.Wait()
	close(start)

	outcome := leaseConcurrentRevokeOutcome{}
	for range contenders {
		result := <-results
		switch {
		case result.err == nil:
			outcome.Successes++
			require.NotNil(t, result.response.Header)
			outcome.SuccessfulHeaderGap = result.response.Header.Revision - seed.Header.Revision
		case status.Code(result.err) == codes.NotFound && status.Convert(result.err).Message() == "etcdserver: requested lease not found":
			outcome.LeaseNotFound++
		default:
			outcome.OtherErrors++
		}
	}

	ranged, err := kv.Range(ctx, &etcdserverpb.RangeRequest{
		Key: prefix, RangeEnd: []byte(clientPrefixRangeEnd(string(prefix))),
	})
	require.NoError(t, err)
	outcome.FinalRevisionGap = ranged.Header.Revision - seed.Header.Revision
	outcome.LeasedKeysDeleted = true
	for _, item := range ranged.Kvs {
		if string(item.Key) != string(seedKey) {
			outcome.LeasedKeysDeleted = false
		}
	}
	outcome.SeedPreserved = len(ranged.Kvs) == 1 && string(ranged.Kvs[0].Key) == string(seedKey) && string(ranged.Kvs[0].Value) == "seed"

	ttl, err := lease.LeaseTimeToLive(ctx, &etcdserverpb.LeaseTimeToLiveRequest{ID: grant.ID, Keys: true})
	require.NoError(t, err)
	outcome.LeaseMissing = ttl.ID == grant.ID && ttl.TTL == -1 && ttl.GrantedTTL == 0 && len(ttl.Keys) == 0
	listed, err := lease.LeaseLeases(ctx, &etcdserverpb.LeaseLeasesRequest{})
	require.NoError(t, err)
	outcome.LeaseAbsentFromList = true
	for _, item := range listed.Leases {
		if item.ID == grant.ID {
			outcome.LeaseAbsentFromList = false
		}
	}
	return outcome
}
