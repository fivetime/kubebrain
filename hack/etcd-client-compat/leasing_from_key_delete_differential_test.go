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

type leasingFromKeyDeleteOutcome struct {
	Deleted                int64
	ResponseTyped          bool
	DeleteEvents           int
	DeleteSingleRevision   bool
	UnaffectedValue        string
	AffectedCachesEmpty    bool
	AffectedDirectEmpty    bool
	UnaffectedOwnerPresent bool
	AffectedOwnerCount     int
}

func TestLeasingFromKeyDeleteDifferentialAgainstReferenceEtcd(t *testing.T) {
	reference := os.Getenv("REFERENCE_ETCD_ENDPOINT")
	if reference == "" {
		t.Skip("set REFERENCE_ETCD_ENDPOINT to run leasing from-key delete differential tests")
	}

	require.Equal(t,
		runLeasingFromKeyDeleteScenario(t, reference, "etcd"),
		runLeasingFromKeyDeleteScenario(t, compatEndpoint(t), "kubebrain"),
	)
}

func runLeasingFromKeyDeleteScenario(t *testing.T, endpoint, instance string) leasingFromKeyDeleteOutcome {
	t.Helper()
	client, err := clientv3.New(clientv3.Config{
		Endpoints: []string{endpoint}, DialTimeout: 3 * time.Second,
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, client.Close()) })

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	t.Cleanup(cancel)
	base := fmt.Sprintf("\xff\xff/dbaas-leasing-from-key/%s/%d/", instance, time.Now().UnixNano())
	ownerPrefix := base + "0owners/"
	dataPrefix := base + "data/"
	keys := []string{
		dataPrefix + "a",
		dataPrefix + "m",
		dataPrefix + "n",
		dataPrefix + "z",
	}
	fromKey := keys[1]
	leased, closeLeased, err := leasing.NewKV(client, ownerPrefix)
	require.NoError(t, err)
	t.Cleanup(closeLeased)
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		_, _ = client.Delete(cleanupCtx, base, clientv3.WithFromKey())
	})

	for index, key := range keys {
		_, err = client.Put(ctx, key, fmt.Sprintf("value-%d", index))
		require.NoError(t, err)
		cached, getErr := leased.Get(ctx, key)
		require.NoError(t, getErr)
		require.Len(t, cached.Kvs, 1)
	}

	watchCtx, watchCancel := context.WithTimeout(ctx, 5*time.Second)
	defer watchCancel()
	watch := client.Watch(watchCtx, dataPrefix, clientv3.WithPrefix())
	opResponse, err := leased.Do(ctx, clientv3.OpDelete(fromKey, clientv3.WithFromKey()))
	require.NoError(t, err)
	deleted := opResponse.Del()
	require.NotNil(t, deleted)
	require.Equal(t, int64(3), deleted.Deleted)

	deleteEvents := 0
	deleteSingleRevision := true
	for deleteEvents < 3 {
		select {
		case response := <-watch:
			require.NoError(t, response.Err())
			for _, event := range response.Events {
				if event.Type != clientv3.EventTypeDelete {
					continue
				}
				deleteEvents++
				if event.Kv.ModRevision != deleted.Header.Revision {
					deleteSingleRevision = false
				}
			}
		case <-watchCtx.Done():
			t.Fatalf("timed out after %d/3 delete events at revision %d", deleteEvents, deleted.Header.Revision)
		}
	}

	unaffected, err := leased.Get(ctx, keys[0])
	require.NoError(t, err)
	require.Len(t, unaffected.Kvs, 1)
	affectedCachesEmpty := true
	affectedDirectEmpty := true
	for _, key := range keys[1:] {
		cached, getErr := leased.Get(ctx, key)
		require.NoError(t, getErr)
		direct, getErr := client.Get(ctx, key)
		require.NoError(t, getErr)
		affectedCachesEmpty = affectedCachesEmpty && len(cached.Kvs) == 0
		affectedDirectEmpty = affectedDirectEmpty && len(direct.Kvs) == 0
	}

	unaffectedOwner, err := client.Get(ctx, ownerPrefix+keys[0])
	require.NoError(t, err)
	require.Len(t, unaffectedOwner.Kvs, 1)
	affectedOwnerCount := 0
	for _, key := range keys[1:] {
		owner, getErr := client.Get(ctx, ownerPrefix+key)
		require.NoError(t, getErr)
		affectedOwnerCount += len(owner.Kvs)
	}

	return leasingFromKeyDeleteOutcome{
		Deleted:                deleted.Deleted,
		ResponseTyped:          deleted != nil,
		DeleteEvents:           deleteEvents,
		DeleteSingleRevision:   deleteSingleRevision,
		UnaffectedValue:        string(unaffected.Kvs[0].Value),
		AffectedCachesEmpty:    affectedCachesEmpty,
		AffectedDirectEmpty:    affectedDirectEmpty,
		UnaffectedOwnerPresent: len(unaffectedOwner.Kvs) == 1,
		AffectedOwnerCount:     affectedOwnerCount,
	}
}
