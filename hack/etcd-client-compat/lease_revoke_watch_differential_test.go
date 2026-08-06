package compat

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
)

type leaseRevokeWatchOutcome struct {
	GrantTTL               int64
	FirstPutAfterBase      int64
	SecondPutAfterBase     int64
	CreatedCanonical       bool
	CreatedHeaderAfterBase int64
	RevokeHeaderAfterBase  int64
	WatchFrameEventCounts  []int
	WatchFrameHeaderGaps   []int64
	Events                 []normalizedLeaseExpiryEvent
	RangeAfterRevoke       int
	RangeRevisionAfterPut  int64
	UnknownTTL             int64
	UnknownTTLRevision     int64
	LeaseStillListed       bool
}

func TestLeaseRevokeMultiKeyWatchDifferentialAgainstReferenceEtcd(t *testing.T) {
	reference := os.Getenv("REFERENCE_ETCD_ENDPOINT")
	if reference == "" {
		t.Skip("set REFERENCE_ETCD_ENDPOINT to run differential compatibility tests")
	}
	want := leaseRevokeWatchOutcome{
		GrantTTL:               300,
		FirstPutAfterBase:      1,
		SecondPutAfterBase:     2,
		CreatedCanonical:       true,
		CreatedHeaderAfterBase: 2,
		RevokeHeaderAfterBase:  3,
		WatchFrameEventCounts:  []int{2},
		WatchFrameHeaderGaps:   []int64{3},
		Events: []normalizedLeaseExpiryEvent{
			{
				Key: "a", ModAfterLastPut: 1, PrevValue: "value-a", PrevModAfterBase: 2,
				PrevCreateAfterBase: 2, PrevVersion: 1, PrevLeaseWasAttached: true,
			},
			{
				Key: "b", ModAfterLastPut: 1, PrevValue: "value-b", PrevModAfterBase: 1,
				PrevCreateAfterBase: 1, PrevVersion: 1, PrevLeaseWasAttached: true,
			},
		},
		RangeRevisionAfterPut: 1,
		UnknownTTL:            -1,
		UnknownTTLRevision:    3,
	}
	referenceOutcome := runLeaseRevokeWatchScenario(t, reference, "etcd")
	require.Equal(t, want, referenceOutcome)
	require.Equal(t, referenceOutcome, runLeaseRevokeWatchScenario(t, compatEndpoint(t), "kubebrain"))
}

func runLeaseRevokeWatchScenario(t *testing.T, endpoint, instance string) leaseRevokeWatchOutcome {
	t.Helper()
	cli, err := clientv3.New(clientv3.Config{Endpoints: []string{endpoint}, DialTimeout: 3 * time.Second})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, cli.Close()) })
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	t.Cleanup(cancel)
	prefix := fmt.Sprintf("/dbaas-lease-revoke-watch/%s/%d/", instance, time.Now().UnixNano())
	base, err := cli.Get(ctx, prefix, clientv3.WithPrefix())
	require.NoError(t, err)
	grant, err := cli.Grant(ctx, 300)
	require.NoError(t, err)
	revoked := false
	t.Cleanup(func() {
		if revoked {
			return
		}
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		_, _ = cli.Revoke(cleanupCtx, grant.ID)
	})
	putB, err := cli.Put(ctx, prefix+"b", "value-b", clientv3.WithLease(grant.ID))
	require.NoError(t, err)
	putA, err := cli.Put(ctx, prefix+"a", "value-a", clientv3.WithLease(grant.ID))
	require.NoError(t, err)

	watchCtx, watchCancel := context.WithCancel(ctx)
	defer watchCancel()
	watch := cli.Watch(watchCtx, prefix,
		clientv3.WithPrefix(), clientv3.WithRev(putA.Header.Revision+1), clientv3.WithPrevKV(), clientv3.WithCreatedNotify())
	created := receiveTxnLeaseLiveWatchResponse(t, ctx, watch)
	require.True(t, created.Created)
	require.Empty(t, created.Events)
	revoke, err := cli.Revoke(ctx, grant.ID)
	require.NoError(t, err)
	revoked = true
	frame := receiveTxnLeaseLiveWatchResponse(t, ctx, watch)
	require.Len(t, frame.Events, 2)
	watchCancel()

	events := make([]normalizedLeaseExpiryEvent, 0, 2)
	for _, event := range frame.Events {
		require.Equal(t, mvccpb.DELETE, event.Type)
		require.NotNil(t, event.PrevKv)
		events = append(events, normalizedLeaseExpiryEvent{
			Key:                  string(event.Kv.Key[len(prefix):]),
			ModAfterLastPut:      event.Kv.ModRevision - putA.Header.Revision,
			CreateRevision:       event.Kv.CreateRevision,
			Version:              event.Kv.Version,
			LeaseAttached:        event.Kv.Lease != 0,
			PrevValue:            string(event.PrevKv.Value),
			PrevModAfterBase:     event.PrevKv.ModRevision - base.Header.Revision,
			PrevCreateAfterBase:  event.PrevKv.CreateRevision - base.Header.Revision,
			PrevVersion:          event.PrevKv.Version,
			PrevLeaseWasAttached: event.PrevKv.Lease != 0,
		})
	}
	after, err := cli.Get(ctx, prefix, clientv3.WithPrefix())
	require.NoError(t, err)
	unknown, err := cli.TimeToLive(ctx, grant.ID, clientv3.WithAttachedKeys())
	require.NoError(t, err)
	leases, err := cli.Leases(ctx)
	require.NoError(t, err)
	listed := false
	for _, lease := range leases.Leases {
		listed = listed || lease.ID == grant.ID
	}

	return leaseRevokeWatchOutcome{
		GrantTTL:               grant.TTL,
		FirstPutAfterBase:      putB.Header.Revision - base.Header.Revision,
		SecondPutAfterBase:     putA.Header.Revision - base.Header.Revision,
		CreatedCanonical:       created.Created && !created.Canceled && created.CompactRevision == 0 && len(created.Events) == 0,
		CreatedHeaderAfterBase: created.Header.Revision - base.Header.Revision,
		RevokeHeaderAfterBase:  revoke.Header.Revision - base.Header.Revision,
		WatchFrameEventCounts:  []int{len(frame.Events)},
		WatchFrameHeaderGaps:   []int64{frame.Header.Revision - base.Header.Revision},
		Events:                 events,
		RangeAfterRevoke:       len(after.Kvs),
		RangeRevisionAfterPut:  after.Header.Revision - putA.Header.Revision,
		UnknownTTL:             unknown.TTL,
		UnknownTTLRevision:     unknown.ResponseHeader.Revision - base.Header.Revision,
		LeaseStillListed:       listed,
	}
}
