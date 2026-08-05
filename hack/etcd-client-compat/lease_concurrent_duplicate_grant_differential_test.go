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

type leaseConcurrentDuplicateGrantOutcome struct {
	Successes              int
	LeaseExists            int
	OtherErrors            int
	WinnerStateValid       bool
	ListOccurrences        int
	GrantRevisionGap       int64
	RevokeRevisionGap      int64
	RegrantSucceeded       bool
	RegrantStateValid      bool
	RegrantRevisionGap     int64
	FinalRevokeRevisionGap int64
}

func TestLeaseConcurrentDuplicateGrantDifferentialAgainstReferenceEtcd(t *testing.T) {
	reference := os.Getenv("REFERENCE_ETCD_ENDPOINT")
	if reference == "" {
		t.Skip("set REFERENCE_ETCD_ENDPOINT to run differential compatibility tests")
	}
	want := leaseConcurrentDuplicateGrantOutcome{
		Successes: 1, LeaseExists: 15, WinnerStateValid: true, ListOccurrences: 1,
		RegrantSucceeded: true, RegrantStateValid: true,
	}
	referenceOutcome := runLeaseConcurrentDuplicateGrantScenario(t, reference)
	require.Equal(t, want, referenceOutcome)
	require.Equal(t, referenceOutcome, runLeaseConcurrentDuplicateGrantScenario(t, compatEndpoint(t)))
}

func runLeaseConcurrentDuplicateGrantScenario(t *testing.T, endpoint string) leaseConcurrentDuplicateGrantOutcome {
	t.Helper()
	conn, err := grpc.NewClient(endpoint, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, conn.Close()) })
	kv := etcdserverpb.NewKVClient(conn)
	lease := etcdserverpb.NewLeaseClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	t.Cleanup(cancel)

	seedKey := []byte(testPrefix(t) + "/lease-concurrent-duplicate-grant/seed")
	seed, err := kv.Put(ctx, &etcdserverpb.PutRequest{Key: seedKey, Value: []byte("seed")})
	require.NoError(t, err)
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		_, _ = kv.DeleteRange(cleanupCtx, &etcdserverpb.DeleteRangeRequest{Key: seedKey})
	})

	id := time.Now().UnixNano() & ((1 << 62) - 1)
	const contenders = 16
	type result struct {
		response *etcdserverpb.LeaseGrantResponse
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
			response, callErr := lease.LeaseGrant(ctx, &etcdserverpb.LeaseGrantRequest{ID: id, TTL: 300})
			results <- result{response: response, err: callErr}
		}()
	}
	ready.Wait()
	close(start)

	outcome := leaseConcurrentDuplicateGrantOutcome{}
	for range contenders {
		result := <-results
		switch {
		case result.err == nil:
			outcome.Successes++
			require.NotNil(t, result.response.Header)
			outcome.GrantRevisionGap = result.response.Header.Revision - seed.Header.Revision
		case status.Code(result.err) == codes.FailedPrecondition && status.Convert(result.err).Message() == "etcdserver: lease already exists":
			outcome.LeaseExists++
		default:
			outcome.OtherErrors++
		}
	}

	ttl, err := lease.LeaseTimeToLive(ctx, &etcdserverpb.LeaseTimeToLiveRequest{ID: id, Keys: true})
	require.NoError(t, err)
	outcome.WinnerStateValid = ttl.ID == id && ttl.TTL > 0 && ttl.TTL <= 300 && ttl.GrantedTTL == 300 && len(ttl.Keys) == 0
	listed, err := lease.LeaseLeases(ctx, &etcdserverpb.LeaseLeasesRequest{})
	require.NoError(t, err)
	for _, item := range listed.Leases {
		if item.ID == id {
			outcome.ListOccurrences++
		}
	}

	revoke, err := lease.LeaseRevoke(ctx, &etcdserverpb.LeaseRevokeRequest{ID: id})
	require.NoError(t, err)
	outcome.RevokeRevisionGap = revoke.Header.Revision - seed.Header.Revision
	regrant, err := lease.LeaseGrant(ctx, &etcdserverpb.LeaseGrantRequest{ID: id, TTL: 301})
	outcome.RegrantSucceeded = err == nil
	if err == nil {
		outcome.RegrantRevisionGap = regrant.Header.Revision - seed.Header.Revision
		regrantedTTL, ttlErr := lease.LeaseTimeToLive(ctx, &etcdserverpb.LeaseTimeToLiveRequest{ID: id})
		require.NoError(t, ttlErr)
		outcome.RegrantStateValid = regrantedTTL.ID == id && regrantedTTL.TTL > 0 && regrantedTTL.TTL <= 301 && regrantedTTL.GrantedTTL == 301
		finalRevoke, revokeErr := lease.LeaseRevoke(ctx, &etcdserverpb.LeaseRevokeRequest{ID: id})
		require.NoError(t, revokeErr)
		outcome.FinalRevokeRevisionGap = finalRevoke.Header.Revision - seed.Header.Revision
	}
	return outcome
}
