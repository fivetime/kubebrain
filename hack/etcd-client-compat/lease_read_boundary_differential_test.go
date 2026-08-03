package compat

import (
	"context"
	"fmt"
	"os"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type leaseReadBoundaryOutcome struct {
	ZeroID                int64
	ZeroTTL               int64
	ZeroGrantedTTL        int64
	ZeroKeysEmpty         bool
	ZeroHeadersWellFormed bool
	ZeroWithoutKeysGap    int64
	ZeroWithKeysGap       int64
	ZeroResponsesMatch    bool
	GrantRevisionGap      int64
	FirstPutRevisionGap   int64
	SecondPutRevisionGap  int64
	LiveIDMatches         bool
	LiveTTLWithinGrant    bool
	LiveGrantedTTLMatches bool
	LiveKeysOmitted       bool
	LiveAttachedKeysMatch bool
	LiveWithoutKeysGap    int64
	LiveWithKeysGap       int64
	ListContainsLive      bool
	ListIDsNonZero        bool
	ListHeaderWellFormed  bool
	ListRevisionGap       int64
}

func TestLeaseReadBoundaryDifferentialAgainstReferenceEtcd(t *testing.T) {
	reference := os.Getenv("REFERENCE_ETCD_ENDPOINT")
	if reference == "" {
		t.Skip("set REFERENCE_ETCD_ENDPOINT to run differential compatibility tests")
	}

	want := leaseReadBoundaryOutcome{
		ZeroTTL: -1, ZeroKeysEmpty: true, ZeroHeadersWellFormed: true, ZeroResponsesMatch: true,
		FirstPutRevisionGap: 1, SecondPutRevisionGap: 2,
		LiveIDMatches: true, LiveTTLWithinGrant: true, LiveGrantedTTLMatches: true,
		LiveKeysOmitted: true, LiveAttachedKeysMatch: true, LiveWithoutKeysGap: 2, LiveWithKeysGap: 2,
		ListContainsLive: true, ListIDsNonZero: true, ListHeaderWellFormed: true,
		ListRevisionGap: 2,
	}
	referenceOutcome := runLeaseReadBoundaryScenario(t, reference, "etcd")
	require.Equal(t, want, referenceOutcome)
	require.Equal(t, referenceOutcome, runLeaseReadBoundaryScenario(t, compatEndpoint(t), "kubebrain"))
}

func runLeaseReadBoundaryScenario(t *testing.T, endpoint, instance string) leaseReadBoundaryOutcome {
	t.Helper()
	endpoint = strings.TrimPrefix(strings.TrimPrefix(endpoint, "http://"), "https://")
	conn, err := grpc.NewClient(endpoint, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, conn.Close()) })
	kv := etcdserverpb.NewKVClient(conn)
	lease := etcdserverpb.NewLeaseClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	t.Cleanup(cancel)
	prefix := fmt.Sprintf("/dbaas-lease-read-boundary/%s/%d/", instance, time.Now().UnixNano())
	seedKey := prefix + "seed"
	seed, err := kv.Put(ctx, &etcdserverpb.PutRequest{Key: []byte(seedKey), Value: []byte("seed")})
	require.NoError(t, err)
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		_, _ = kv.DeleteRange(cleanupCtx, &etcdserverpb.DeleteRangeRequest{
			Key:      []byte(prefix),
			RangeEnd: []byte(clientPrefixRangeEnd(prefix)),
		})
	})

	zeroWithoutKeys, err := lease.LeaseTimeToLive(ctx, &etcdserverpb.LeaseTimeToLiveRequest{})
	require.NoError(t, err)
	zeroWithKeys, err := lease.LeaseTimeToLive(ctx, &etcdserverpb.LeaseTimeToLiveRequest{Keys: true})
	require.NoError(t, err)

	id := time.Now().UnixNano() & ((1 << 62) - 1)
	grant, err := lease.LeaseGrant(ctx, &etcdserverpb.LeaseGrantRequest{ID: id, TTL: 300})
	require.NoError(t, err)
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		_, _ = lease.LeaseRevoke(cleanupCtx, &etcdserverpb.LeaseRevokeRequest{ID: id})
	})
	keys := []string{prefix + "z", prefix + "a"}
	putRevisions := make([]int64, 0, len(keys))
	for _, key := range keys {
		put, putErr := kv.Put(ctx, &etcdserverpb.PutRequest{Key: []byte(key), Value: []byte("value"), Lease: id})
		require.NoError(t, putErr)
		putRevisions = append(putRevisions, put.Header.Revision)
	}

	liveWithoutKeys, err := lease.LeaseTimeToLive(ctx, &etcdserverpb.LeaseTimeToLiveRequest{ID: id})
	require.NoError(t, err)
	liveWithKeys, err := lease.LeaseTimeToLive(ctx, &etcdserverpb.LeaseTimeToLiveRequest{ID: id, Keys: true})
	require.NoError(t, err)
	list, err := lease.LeaseLeases(ctx, &etcdserverpb.LeaseLeasesRequest{})
	require.NoError(t, err)

	gotKeys := make([]string, 0, len(liveWithKeys.Keys))
	for _, key := range liveWithKeys.Keys {
		gotKeys = append(gotKeys, string(key))
	}
	slices.Sort(keys)
	slices.Sort(gotKeys)
	containsLive := false
	idsNonZero := true
	for _, status := range list.Leases {
		containsLive = containsLive || status.ID == id
		idsNonZero = idsNonZero && status.ID != 0
	}
	return leaseReadBoundaryOutcome{
		ZeroID:                zeroWithoutKeys.ID,
		ZeroTTL:               zeroWithoutKeys.TTL,
		ZeroGrantedTTL:        zeroWithoutKeys.GrantedTTL,
		ZeroKeysEmpty:         len(zeroWithoutKeys.Keys) == 0 && len(zeroWithKeys.Keys) == 0,
		ZeroHeadersWellFormed: leaseReadHeaderWellFormed(zeroWithoutKeys.Header) && leaseReadHeaderWellFormed(zeroWithKeys.Header),
		ZeroWithoutKeysGap:    zeroWithoutKeys.Header.Revision - seed.Header.Revision,
		ZeroWithKeysGap:       zeroWithKeys.Header.Revision - seed.Header.Revision,
		ZeroResponsesMatch: zeroWithKeys.ID == zeroWithoutKeys.ID &&
			zeroWithKeys.TTL == zeroWithoutKeys.TTL &&
			zeroWithKeys.GrantedTTL == zeroWithoutKeys.GrantedTTL,
		GrantRevisionGap:      grant.Header.Revision - seed.Header.Revision,
		FirstPutRevisionGap:   putRevisions[0] - seed.Header.Revision,
		SecondPutRevisionGap:  putRevisions[1] - seed.Header.Revision,
		LiveIDMatches:         liveWithoutKeys.ID == id && liveWithKeys.ID == id && grant.ID == id,
		LiveTTLWithinGrant:    liveWithoutKeys.TTL > 0 && liveWithoutKeys.TTL <= 300 && liveWithKeys.TTL > 0 && liveWithKeys.TTL <= 300,
		LiveGrantedTTLMatches: liveWithoutKeys.GrantedTTL == 300 && liveWithKeys.GrantedTTL == 300,
		LiveKeysOmitted:       len(liveWithoutKeys.Keys) == 0,
		LiveAttachedKeysMatch: slices.Equal(gotKeys, keys),
		LiveWithoutKeysGap:    liveWithoutKeys.Header.Revision - seed.Header.Revision,
		LiveWithKeysGap:       liveWithKeys.Header.Revision - seed.Header.Revision,
		ListContainsLive:      containsLive,
		ListIDsNonZero:        idsNonZero,
		ListHeaderWellFormed:  leaseReadHeaderWellFormed(list.Header),
		ListRevisionGap:       list.Header.Revision - seed.Header.Revision,
	}
}

func clientPrefixRangeEnd(prefix string) string {
	end := []byte(prefix)
	for i := len(end) - 1; i >= 0; i-- {
		if end[i] < 0xff {
			end[i]++
			return string(end[:i+1])
		}
	}
	return "\x00"
}

func leaseReadHeaderWellFormed(header *etcdserverpb.ResponseHeader) bool {
	return header != nil && header.ClusterId != 0 && header.MemberId != 0 &&
		header.Revision > 0 && header.RaftTerm > 0
}
