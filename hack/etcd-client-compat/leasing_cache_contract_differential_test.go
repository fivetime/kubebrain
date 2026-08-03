package compat

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/leasing"
)

type leasingCacheContractOutcome struct {
	ResponseIsolated       bool
	OfflineValue           string
	OfflineVersion         int64
	OfflineAtPutRevision   bool
	KeysOnlyValueEmpty     bool
	CountOnlyCount         int64
	LimitPreserved         bool
	SortedValue            string
	MinModFiltered         bool
	MaxModFiltered         bool
	MinCreateFiltered      bool
	MaxCreateFiltered      bool
	SerializableValue      string
	TTLReadRequiresBackend bool
	TrafficDiscarded       bool
}

func TestLeasingCacheContractDifferentialAgainstReferenceEtcd(t *testing.T) {
	reference := os.Getenv("REFERENCE_ETCD_ENDPOINT")
	if reference == "" {
		t.Skip("set REFERENCE_ETCD_ENDPOINT to run leasing cache contract differential tests")
	}

	require.Equal(t,
		runLeasingCacheContractScenario(t, reference, "etcd"),
		runLeasingCacheContractScenario(t, compatEndpoint(t), "kubebrain"),
	)
}

func runLeasingCacheContractScenario(t *testing.T, endpoint, instance string) leasingCacheContractOutcome {
	t.Helper()
	bridge := newTCPBridge(t, endpoint)
	bridged, err := clientv3.New(clientv3.Config{
		Endpoints: []string{bridge.Endpoint()}, DialTimeout: 3 * time.Second,
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, bridged.Close()) })
	direct, err := clientv3.New(clientv3.Config{
		Endpoints: []string{endpoint}, DialTimeout: 3 * time.Second,
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, direct.Close()) })

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	t.Cleanup(cancel)
	prefix := fmt.Sprintf("/dbaas-leasing-cache-contract/%s/%d/", instance, time.Now().UnixNano())
	key := prefix + "cached"
	ttlKey := prefix + "ttl"
	ownerPrefix := prefix + "owners/"
	leased, closeLeased, err := leasing.NewKV(bridged, ownerPrefix)
	require.NoError(t, err)
	t.Cleanup(closeLeased)
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		_, _ = direct.Delete(cleanupCtx, prefix, clientv3.WithPrefix())
	})

	_, err = direct.Put(ctx, key, "initial")
	require.NoError(t, err)
	first, err := leased.Get(ctx, key)
	require.NoError(t, err)
	require.Len(t, first.Kvs, 1)
	first.Kvs[0].Key[0] ^= 0xff
	first.Kvs[0].Value[0] ^= 0xff
	isolated, err := leased.Get(ctx, key)
	require.NoError(t, err)
	require.Len(t, isolated.Kvs, 1)
	require.Equal(t, key, string(isolated.Kvs[0].Key))
	require.Equal(t, "initial", string(isolated.Kvs[0].Value))

	put, err := leased.Put(ctx, key, "offline")
	require.NoError(t, err)
	require.NotNil(t, put)

	grant, err := direct.Grant(ctx, 60)
	require.NoError(t, err)
	_, err = direct.Put(ctx, ttlKey, "ephemeral", clientv3.WithLease(grant.ID))
	require.NoError(t, err)
	ttlOnline, err := leased.Get(ctx, ttlKey)
	require.NoError(t, err)
	require.Len(t, ttlOnline.Kvs, 1)

	droppedBefore := bridge.DroppedBytes()
	bridge.Blackhole()

	offlineCtx, offlineCancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer offlineCancel()
	offline, err := leased.Get(offlineCtx, key)
	require.NoError(t, err)
	require.Len(t, offline.Kvs, 1)
	keysOnly, err := leased.Get(offlineCtx, key, clientv3.WithKeysOnly())
	require.NoError(t, err)
	countOnly, err := leased.Get(offlineCtx, key, clientv3.WithCountOnly())
	require.NoError(t, err)
	limited, err := leased.Get(offlineCtx, key, clientv3.WithLimit(1))
	require.NoError(t, err)
	sorted, err := leased.Get(offlineCtx, key, clientv3.WithSort(clientv3.SortByKey, clientv3.SortAscend))
	require.NoError(t, err)
	minModFiltered, err := leased.Get(offlineCtx, key, clientv3.WithMinModRev(put.Header.Revision+1))
	require.NoError(t, err)
	maxModFiltered, err := leased.Get(offlineCtx, key, clientv3.WithMaxModRev(put.Header.Revision-1))
	require.NoError(t, err)
	minCreateFiltered, err := leased.Get(offlineCtx, key, clientv3.WithMinCreateRev(offline.Kvs[0].CreateRevision+1))
	require.NoError(t, err)
	maxCreateFiltered, err := leased.Get(offlineCtx, key, clientv3.WithMaxCreateRev(offline.Kvs[0].CreateRevision-1))
	require.NoError(t, err)
	serializable, err := leased.Get(offlineCtx, key, clientv3.WithSerializable())
	require.NoError(t, err)

	ttlCtx, ttlCancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	_, ttlErr := leased.Get(ttlCtx, ttlKey)
	ttlCancel()
	require.ErrorIs(t, ttlErr, context.DeadlineExceeded)
	require.Eventually(t, func() bool {
		return bridge.DroppedBytes() > droppedBefore
	}, 2*time.Second, 10*time.Millisecond)
	trafficDiscarded := bridge.DroppedBytes() > droppedBefore
	bridge.Unblackhole()

	require.Len(t, keysOnly.Kvs, 1)
	require.Empty(t, keysOnly.Kvs[0].Value)
	require.Empty(t, countOnly.Kvs)
	require.Equal(t, int64(1), countOnly.Count)
	require.Len(t, limited.Kvs, 1)
	require.Len(t, sorted.Kvs, 1)
	require.Empty(t, minModFiltered.Kvs)
	require.Empty(t, maxModFiltered.Kvs)
	require.Empty(t, minCreateFiltered.Kvs)
	require.Empty(t, maxCreateFiltered.Kvs)
	require.Len(t, serializable.Kvs, 1)

	return leasingCacheContractOutcome{
		ResponseIsolated:       string(isolated.Kvs[0].Key) == key && string(isolated.Kvs[0].Value) == "initial",
		OfflineValue:           string(offline.Kvs[0].Value),
		OfflineVersion:         offline.Kvs[0].Version,
		OfflineAtPutRevision:   offline.Kvs[0].ModRevision == put.Header.Revision,
		KeysOnlyValueEmpty:     len(keysOnly.Kvs[0].Value) == 0,
		CountOnlyCount:         countOnly.Count,
		LimitPreserved:         len(limited.Kvs) == 1,
		SortedValue:            string(sorted.Kvs[0].Value),
		MinModFiltered:         len(minModFiltered.Kvs) == 0,
		MaxModFiltered:         len(maxModFiltered.Kvs) == 0,
		MinCreateFiltered:      len(minCreateFiltered.Kvs) == 0,
		MaxCreateFiltered:      len(maxCreateFiltered.Kvs) == 0,
		SerializableValue:      string(serializable.Kvs[0].Value),
		TTLReadRequiresBackend: ttlErr != nil,
		TrafficDiscarded:       trafficDiscarded,
	}
}
