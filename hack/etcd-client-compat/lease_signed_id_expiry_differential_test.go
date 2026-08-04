package compat

import (
	"context"
	"fmt"
	"math"
	"os"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	clientv3 "go.etcd.io/etcd/client/v3"
)

type signedLeaseExpiryOutcome struct {
	Name                string
	DeleteEvent         bool
	DeleteLeaseZero     bool
	PrevKVLeaseMatches  bool
	PrevKVValueMatches  bool
	TTLIDMatches        bool
	TTLExpired          bool
	GrantedTTLZero      bool
	TTLKeysEmpty        bool
	KeyDeleted          bool
	LeaseAbsentFromList bool
}

func TestLeaseSignedIDNaturalExpiryDifferentialAgainstReferenceEtcd(t *testing.T) {
	reference := os.Getenv("REFERENCE_ETCD_ENDPOINT")
	if reference == "" {
		t.Skip("set REFERENCE_ETCD_ENDPOINT to run differential compatibility tests")
	}
	want := []signedLeaseExpiryOutcome{
		signedLeaseExpiryExpected("negative-one"),
		signedLeaseExpiryExpected("minimum"),
		signedLeaseExpiryExpected("maximum"),
	}
	referenceOutcome := runSignedLeaseExpiryScenario(t, reference, "etcd")
	require.Equal(t, want, referenceOutcome)
	require.Equal(t, referenceOutcome, runSignedLeaseExpiryScenario(t, compatEndpoint(t), "kubebrain"))
}

func signedLeaseExpiryExpected(name string) signedLeaseExpiryOutcome {
	return signedLeaseExpiryOutcome{
		Name: name, DeleteEvent: true, DeleteLeaseZero: true, PrevKVLeaseMatches: true,
		PrevKVValueMatches: true, TTLIDMatches: true, TTLExpired: true, GrantedTTLZero: true,
		TTLKeysEmpty: true, KeyDeleted: true, LeaseAbsentFromList: true,
	}
}

func runSignedLeaseExpiryScenario(t *testing.T, endpoint, instance string) []signedLeaseExpiryOutcome {
	t.Helper()
	endpoint = strings.TrimPrefix(strings.TrimPrefix(endpoint, "http://"), "https://")
	cli, err := clientv3.New(clientv3.Config{Endpoints: []string{endpoint}, DialTimeout: 3 * time.Second})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, cli.Close()) })
	rawLease := etcdserverpb.NewLeaseClient(cli.ActiveConnection())
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	t.Cleanup(cancel)

	tests := []struct {
		name string
		id   int64
	}{
		{name: "negative-one", id: -1},
		{name: "minimum", id: math.MinInt64},
		{name: "maximum", id: math.MaxInt64},
	}
	type activeLease struct {
		name  string
		id    int64
		key   string
		value string
		watch clientv3.WatchChan
	}
	active := make([]activeLease, 0, len(tests))
	prefix := fmt.Sprintf("/dbaas-signed-lease-expiry/%s/%d/", instance, time.Now().UnixNano())
	for _, test := range tests {
		grant, grantErr := rawLease.LeaseGrant(ctx, &etcdserverpb.LeaseGrantRequest{ID: test.id, TTL: 2})
		require.NoError(t, grantErr)
		require.Equal(t, test.id, grant.ID)
		key := prefix + test.name
		value := "value-" + test.name
		put, putErr := cli.Put(ctx, key, value, clientv3.WithLease(clientv3.LeaseID(test.id)))
		require.NoError(t, putErr)
		active = append(active, activeLease{
			name: test.name, id: test.id, key: key, value: value,
			watch: cli.Watch(ctx, key, clientv3.WithRev(put.Header.Revision+1), clientv3.WithPrevKV()),
		})
	}
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		_, _ = cli.Delete(cleanupCtx, prefix, clientv3.WithPrefix())
	})

	outcomes := make([]signedLeaseExpiryOutcome, 0, len(active))
	for _, lease := range active {
		var event *clientv3.Event
		for event == nil {
			select {
			case response, ok := <-lease.watch:
				require.True(t, ok, "watch closed before expiry for %s", lease.name)
				require.NoError(t, response.Err())
				if len(response.Events) > 0 {
					event = response.Events[0]
				}
			case <-ctx.Done():
				require.FailNow(t, "timed out waiting for signed lease expiry", lease.name)
			}
		}
		ttl, ttlErr := cli.TimeToLive(ctx, clientv3.LeaseID(lease.id), clientv3.WithAttachedKeys())
		require.NoError(t, ttlErr)
		got, getErr := cli.Get(ctx, lease.key)
		require.NoError(t, getErr)
		list, listErr := cli.Leases(ctx)
		require.NoError(t, listErr)
		ids := make([]int64, 0, len(list.Leases))
		for _, item := range list.Leases {
			ids = append(ids, int64(item.ID))
		}
		outcomes = append(outcomes, signedLeaseExpiryOutcome{
			Name:                lease.name,
			DeleteEvent:         event.Type == clientv3.EventTypeDelete,
			DeleteLeaseZero:     event.Kv != nil && event.Kv.Lease == 0,
			PrevKVLeaseMatches:  event.PrevKv != nil && event.PrevKv.Lease == lease.id,
			PrevKVValueMatches:  event.PrevKv != nil && string(event.PrevKv.Value) == lease.value,
			TTLIDMatches:        int64(ttl.ID) == lease.id,
			TTLExpired:          ttl.TTL == -1,
			GrantedTTLZero:      ttl.GrantedTTL == 0,
			TTLKeysEmpty:        len(ttl.Keys) == 0,
			KeyDeleted:          len(got.Kvs) == 0,
			LeaseAbsentFromList: !slices.Contains(ids, lease.id),
		})
	}
	return outcomes
}
