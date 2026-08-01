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

type normalizedLeaseExpiryEvent struct {
	Key                  string
	ModAfterLastPut      int64
	CreateRevision       int64
	Version              int64
	LeaseAttached        bool
	PrevValue            string
	PrevModAfterBase     int64
	PrevCreateAfterBase  int64
	PrevVersion          int64
	PrevLeaseWasAttached bool
}

type leaseExpiryDifferentialResult struct {
	GrantTTL              int64
	FirstPutAfterBase     int64
	SecondPutAfterBase    int64
	WatchHeaderAfterBase  int64
	Events                []normalizedLeaseExpiryEvent
	RangeAfterExpiry      int
	RangeRevisionAfterPut int64
	UnknownTTL            int64
	UnknownTTLRevision    int64
	LeaseStillListed      bool
}

func TestLeaseNaturalExpiryDifferentialAgainstReferenceEtcd(t *testing.T) {
	reference := os.Getenv("REFERENCE_ETCD_ENDPOINT")
	kubebrain := os.Getenv("KUBEBRAIN_ETCD_ENDPOINT")
	if reference == "" {
		t.Skip("set REFERENCE_ETCD_ENDPOINT to run lease expiry differential tests")
	}
	if kubebrain == "" {
		t.Fatal("set KUBEBRAIN_ETCD_ENDPOINT explicitly for differential tests")
	}
	referenceResult := runLeaseExpiryScenario(t, reference, "etcd")
	want := leaseExpiryDifferentialResult{
		GrantTTL:              2,
		FirstPutAfterBase:     1,
		SecondPutAfterBase:    2,
		WatchHeaderAfterBase:  3,
		RangeRevisionAfterPut: 1,
		UnknownTTL:            -1,
		UnknownTTLRevision:    3,
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
	require.Equal(t, want, referenceResult)
	kubebrainResult := runLeaseExpiryScenario(t, kubebrain, "kubebrain")
	require.Equal(t, referenceResult, kubebrainResult)
}

func runLeaseExpiryScenario(t *testing.T, endpoint, instance string) leaseExpiryDifferentialResult {
	t.Helper()
	cli, err := clientv3.New(clientv3.Config{Endpoints: []string{endpoint}, DialTimeout: 3 * time.Second})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, cli.Close()) })
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	t.Cleanup(cancel)

	prefix := fmt.Sprintf("/dbaas-lease-expiry/%s/%d/", instance, time.Now().UnixNano())
	base, err := cli.Get(ctx, prefix, clientv3.WithPrefix())
	require.NoError(t, err)
	grant, err := cli.Grant(ctx, 2)
	require.NoError(t, err)
	// Insert in reverse key order. etcd sorts lease keys before the atomic revoke;
	// the watch sequence must therefore still be a then b.
	putB, err := cli.Put(ctx, prefix+"b", "value-b", clientv3.WithLease(grant.ID))
	require.NoError(t, err)
	putA, err := cli.Put(ctx, prefix+"a", "value-a", clientv3.WithLease(grant.ID))
	require.NoError(t, err)

	watchCtx, watchCancel := context.WithCancel(ctx)
	defer watchCancel()
	watch := cli.Watch(watchCtx, prefix, clientv3.WithPrefix(), clientv3.WithRev(putA.Header.Revision+1), clientv3.WithPrevKV())
	var (
		events    []normalizedLeaseExpiryEvent
		watchHead int64
	)
	for len(events) < 2 {
		select {
		case response, ok := <-watch:
			require.True(t, ok, "watch closed before lease expiry on %s", endpoint)
			require.NoError(t, response.Err())
			watchHead = response.Header.Revision
			for _, event := range response.Events {
				require.Equal(t, mvccpb.DELETE, event.Type)
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
		case <-ctx.Done():
			t.Fatalf("lease did not expire on %s: %v", endpoint, ctx.Err())
		}
	}
	watchCancel()

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

	return leaseExpiryDifferentialResult{
		GrantTTL:              grant.TTL,
		FirstPutAfterBase:     putB.Header.Revision - base.Header.Revision,
		SecondPutAfterBase:    putA.Header.Revision - base.Header.Revision,
		WatchHeaderAfterBase:  watchHead - base.Header.Revision,
		Events:                events,
		RangeAfterExpiry:      len(after.Kvs),
		RangeRevisionAfterPut: after.Header.Revision - putA.Header.Revision,
		UnknownTTL:            unknown.TTL,
		UnknownTTLRevision:    unknown.ResponseHeader.Revision - base.Header.Revision,
		LeaseStillListed:      listed,
	}
}
