package compat

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"
	"go.etcd.io/etcd/client/v3/leasing"
)

type leasingSessionExpiryOutcome struct {
	InitialValue    string
	OwnerLeaseFound bool
	OwnerRemoved    bool
	RefreshedValue  string
	NewOwnerLease   bool
}

func TestLeasingSessionExpiryDifferentialAgainstReferenceEtcd(t *testing.T) {
	reference := os.Getenv("REFERENCE_ETCD_ENDPOINT")
	if reference == "" {
		t.Skip("set REFERENCE_ETCD_ENDPOINT to run leasing session expiry differential tests")
	}

	require.Equal(t,
		runLeasingSessionExpiryScenario(t, reference, "etcd"),
		runLeasingSessionExpiryScenario(t, compatEndpoint(), "kubebrain"),
	)
}

func runLeasingSessionExpiryScenario(t *testing.T, endpoint, instance string) leasingSessionExpiryOutcome {
	t.Helper()
	first, err := clientv3.New(clientv3.Config{
		Endpoints: []string{endpoint}, DialTimeout: 3 * time.Second,
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, first.Close()) })
	second, err := clientv3.New(clientv3.Config{
		Endpoints: []string{endpoint}, DialTimeout: 3 * time.Second,
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, second.Close()) })

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	t.Cleanup(cancel)
	prefix := fmt.Sprintf("/dbaas-leasing-session/%s/%d/", instance, time.Now().UnixNano())
	key := prefix + "data"
	ownerPrefix := prefix + "owners/"
	firstKV, closeFirst, err := leasing.NewKV(first, ownerPrefix, concurrency.WithTTL(2))
	require.NoError(t, err)
	t.Cleanup(closeFirst)
	secondKV, closeSecond, err := leasing.NewKV(second, ownerPrefix, concurrency.WithTTL(2))
	require.NoError(t, err)
	t.Cleanup(closeSecond)
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		_, _ = first.Delete(cleanupCtx, prefix, clientv3.WithPrefix())
	})

	_, err = first.Put(ctx, key, "old")
	require.NoError(t, err)
	initial, err := firstKV.Get(ctx, key)
	require.NoError(t, err)
	require.Len(t, initial.Kvs, 1)
	owners, err := first.Get(ctx, ownerPrefix, clientv3.WithPrefix())
	require.NoError(t, err)
	require.Len(t, owners.Kvs, 1)
	oldLease := clientv3.LeaseID(owners.Kvs[0].Lease)
	require.NotZero(t, oldLease)

	_, err = second.Revoke(ctx, oldLease)
	require.NoError(t, err)
	ownerRemoved := false
	require.Eventually(t, func() bool {
		response, getErr := first.Get(ctx, ownerPrefix, clientv3.WithPrefix())
		ownerRemoved = getErr == nil && len(response.Kvs) == 0
		return ownerRemoved
	}, 5*time.Second, 20*time.Millisecond)
	_, err = secondKV.Put(ctx, key, "new")
	require.NoError(t, err)

	var refreshed *clientv3.GetResponse
	require.Eventually(t, func() bool {
		response, getErr := firstKV.Get(ctx, key)
		if getErr != nil || len(response.Kvs) != 1 || string(response.Kvs[0].Value) != "new" {
			return false
		}
		refreshed = response
		return true
	}, 10*time.Second, 20*time.Millisecond)
	newOwnerLease := false
	require.Eventually(t, func() bool {
		_, getErr := firstKV.Get(ctx, key)
		if getErr != nil {
			return false
		}
		newOwners, getErr := first.Get(ctx, ownerPrefix, clientv3.WithPrefix())
		if getErr != nil {
			return false
		}
		for _, owner := range newOwners.Kvs {
			if clientv3.LeaseID(owner.Lease) != 0 && clientv3.LeaseID(owner.Lease) != oldLease {
				newOwnerLease = true
				return true
			}
		}
		return false
	}, 5*time.Second, 20*time.Millisecond)

	return leasingSessionExpiryOutcome{
		InitialValue:    string(initial.Kvs[0].Value),
		OwnerLeaseFound: oldLease != 0,
		OwnerRemoved:    ownerRemoved,
		RefreshedValue:  string(refreshed.Kvs[0].Value),
		NewOwnerLease:   newOwnerLease,
	}
}
