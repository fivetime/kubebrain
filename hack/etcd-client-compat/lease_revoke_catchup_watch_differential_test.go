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

type leaseRevokeCatchupWatchOutcome struct {
	CreatedCanonical       bool
	CreatedHeaderAfterBase int64
	FirstPutAfterBase      int64
	SecondPutAfterBase     int64
	RevokeHeaderAfterBase  int64
	FrameEventCounts       []int
	FrameHeaderAfterBase   []int64
	Events                 []normalizedLeaseExpiryEvent
}

func TestLeaseRevokeMultiKeyCatchupWatchDifferentialAgainstReferenceEtcd(t *testing.T) {
	reference := os.Getenv("REFERENCE_ETCD_ENDPOINT")
	if reference == "" {
		t.Skip("set REFERENCE_ETCD_ENDPOINT to run differential compatibility tests")
	}
	want := leaseRevokeCatchupWatchOutcome{
		CreatedCanonical:       true,
		CreatedHeaderAfterBase: 3,
		FirstPutAfterBase:      1,
		SecondPutAfterBase:     2,
		RevokeHeaderAfterBase:  3,
		FrameEventCounts:       []int{2},
		FrameHeaderAfterBase:   []int64{3},
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
	}
	referenceOutcome := runLeaseRevokeCatchupWatchScenario(t, reference, "etcd")
	require.Equal(t, want, referenceOutcome)
	require.Equal(t, referenceOutcome, runLeaseRevokeCatchupWatchScenario(t, compatEndpoint(t), "kubebrain"))
}

func runLeaseRevokeCatchupWatchScenario(t *testing.T, endpoint, instance string) leaseRevokeCatchupWatchOutcome {
	t.Helper()
	cli, err := clientv3.New(clientv3.Config{Endpoints: []string{endpoint}, DialTimeout: 3 * time.Second})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, cli.Close()) })
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	t.Cleanup(cancel)
	prefix := fmt.Sprintf("/dbaas-lease-revoke-catchup-watch/%s/%d/", instance, time.Now().UnixNano())
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
	revoke, err := cli.Revoke(ctx, grant.ID)
	require.NoError(t, err)
	revoked = true

	watchCtx, watchCancel := context.WithCancel(ctx)
	defer watchCancel()
	watch := cli.Watch(watchCtx, prefix,
		clientv3.WithPrefix(), clientv3.WithRev(revoke.Header.Revision), clientv3.WithPrevKV(), clientv3.WithCreatedNotify())
	created := receiveTxnLeaseLiveWatchResponse(t, ctx, watch)
	require.True(t, created.Created)
	require.Empty(t, created.Events)
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

	return leaseRevokeCatchupWatchOutcome{
		CreatedCanonical:       created.Created && !created.Canceled && created.CompactRevision == 0 && len(created.Events) == 0,
		CreatedHeaderAfterBase: created.Header.Revision - base.Header.Revision,
		FirstPutAfterBase:      putB.Header.Revision - base.Header.Revision,
		SecondPutAfterBase:     putA.Header.Revision - base.Header.Revision,
		RevokeHeaderAfterBase:  revoke.Header.Revision - base.Header.Revision,
		FrameEventCounts:       []int{len(frame.Events)},
		FrameHeaderAfterBase:   []int64{frame.Header.Revision - base.Header.Revision},
		Events:                 events,
	}
}
