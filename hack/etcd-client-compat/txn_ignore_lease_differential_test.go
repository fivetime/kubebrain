package compat

import (
	"context"
	"fmt"
	"math"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc/status"
)

type txnIgnoreLeaseOutcome struct {
	IgnoreValueSucceeded  bool
	IgnoreValuePrevValue  string
	IgnoreValuePrevLeaseA bool
	StagedValue           string
	StagedLeaseB          bool
	IgnoreLeaseSucceeded  bool
	IgnoreLeasePrevValue  string
	IgnoreLeasePrevLeaseB bool
	FinalValue            string
	FinalLeaseB           bool
	ExistsAfterRevokeA    bool
	ExistsAfterRevokeB    bool
	UnselectedBadLeaseOK  bool
	SelectedBadLeaseCode  string
	SelectedBadLeaseError string
	BadLeaseKeyExists     bool
}

func TestTxnIgnoreLeaseDifferentialAgainstReferenceEtcd(t *testing.T) {
	reference := os.Getenv("REFERENCE_ETCD_ENDPOINT")
	if reference == "" {
		t.Skip("set REFERENCE_ETCD_ENDPOINT to run differential compatibility tests")
	}

	require.Equal(t,
		runTxnIgnoreLeaseScenario(t, reference, "etcd"),
		runTxnIgnoreLeaseScenario(t, compatEndpoint(), "kubebrain"),
	)
}

func runTxnIgnoreLeaseScenario(t *testing.T, endpoint, instance string) txnIgnoreLeaseOutcome {
	t.Helper()
	client, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{endpoint},
		DialTimeout: 3 * time.Second,
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, client.Close()) })
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	key := fmt.Sprintf("/dbaas-txn-ignore-lease/%s/%d", instance, time.Now().UnixNano())
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		_, _ = client.Delete(cleanupCtx, key, clientv3.WithPrefix())
	})
	leaseA, err := client.Grant(ctx, 300)
	require.NoError(t, err)
	leaseB, err := client.Grant(ctx, 300)
	require.NoError(t, err)
	revokedA, revokedB := false, false
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		if !revokedA {
			_, _ = client.Revoke(cleanupCtx, leaseA.ID)
		}
		if !revokedB {
			_, _ = client.Revoke(cleanupCtx, leaseB.ID)
		}
	})

	_, err = client.Put(ctx, key, "old", clientv3.WithLease(leaseA.ID))
	require.NoError(t, err)
	ignoreValue, err := client.Txn(ctx).Then(
		clientv3.OpPut(key, "", clientv3.WithIgnoreValue(), clientv3.WithLease(leaseB.ID), clientv3.WithPrevKV()),
		clientv3.OpGet(key),
	).Commit()
	require.NoError(t, err)
	require.Len(t, ignoreValue.Responses, 2)
	ignoreValuePrev := ignoreValue.Responses[0].GetResponsePut().PrevKv
	staged := ignoreValue.Responses[1].GetResponseRange().Kvs
	require.NotNil(t, ignoreValuePrev)
	require.Len(t, staged, 1)

	ignoreLease, err := client.Txn(ctx).Then(
		clientv3.OpPut(key, "new", clientv3.WithIgnoreLease(), clientv3.WithPrevKV()),
	).Commit()
	require.NoError(t, err)
	require.Len(t, ignoreLease.Responses, 1)
	ignoreLeasePrev := ignoreLease.Responses[0].GetResponsePut().PrevKv
	require.NotNil(t, ignoreLeasePrev)
	final, err := client.Get(ctx, key)
	require.NoError(t, err)
	require.Len(t, final.Kvs, 1)

	_, err = client.Revoke(ctx, leaseA.ID)
	require.NoError(t, err)
	revokedA = true
	afterA, err := client.Get(ctx, key)
	require.NoError(t, err)
	_, err = client.Revoke(ctx, leaseB.ID)
	require.NoError(t, err)
	revokedB = true
	afterB, err := client.Get(ctx, key)
	require.NoError(t, err)

	branchKey := key + "/branch"
	unselectedBadLease, err := client.Txn(ctx).
		If(clientv3.Compare(clientv3.Version(branchKey), "=", 0)).
		Then(clientv3.OpPut(branchKey, "valid")).
		Else(clientv3.OpPut(branchKey, "invalid", clientv3.WithLease(clientv3.LeaseID(math.MaxInt64)))).
		Commit()
	require.NoError(t, err)
	_, err = client.Delete(ctx, branchKey)
	require.NoError(t, err)
	_, selectedBadLeaseErr := client.Txn(ctx).
		If(clientv3.Compare(clientv3.Version(branchKey), ">", 0)).
		Then(clientv3.OpPut(branchKey, "valid")).
		Else(clientv3.OpPut(branchKey, "invalid", clientv3.WithLease(clientv3.LeaseID(math.MaxInt64)))).
		Commit()
	require.Error(t, selectedBadLeaseErr)
	afterBadLease, err := client.Get(ctx, branchKey)
	require.NoError(t, err)

	return txnIgnoreLeaseOutcome{
		IgnoreValueSucceeded:  ignoreValue.Succeeded,
		IgnoreValuePrevValue:  string(ignoreValuePrev.Value),
		IgnoreValuePrevLeaseA: ignoreValuePrev.Lease == int64(leaseA.ID),
		StagedValue:           string(staged[0].Value),
		StagedLeaseB:          staged[0].Lease == int64(leaseB.ID),
		IgnoreLeaseSucceeded:  ignoreLease.Succeeded,
		IgnoreLeasePrevValue:  string(ignoreLeasePrev.Value),
		IgnoreLeasePrevLeaseB: ignoreLeasePrev.Lease == int64(leaseB.ID),
		FinalValue:            string(final.Kvs[0].Value),
		FinalLeaseB:           final.Kvs[0].Lease == int64(leaseB.ID),
		ExistsAfterRevokeA:    len(afterA.Kvs) == 1,
		ExistsAfterRevokeB:    len(afterB.Kvs) == 1,
		UnselectedBadLeaseOK:  unselectedBadLease.Succeeded,
		SelectedBadLeaseCode:  status.Code(selectedBadLeaseErr).String(),
		SelectedBadLeaseError: status.Convert(selectedBadLeaseErr).Message(),
		BadLeaseKeyExists:     len(afterBadLease.Kvs) == 1,
	}
}
