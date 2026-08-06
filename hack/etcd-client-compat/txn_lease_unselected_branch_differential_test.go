package compat

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"
)

type txnLeaseUnselectedBranchOutcome struct {
	Succeeded            bool
	TxnRevisionGap       int64
	ResponseCount        int
	SelectedRangeValue   string
	SelectedRangeLeaseA  bool
	LeaseAKeys           []string
	LeaseBKeys           []string
	LeaseATTLRevisionGap int64
	LeaseBTTLRevisionGap int64
	Values               map[string]string
	Leases               map[string]string
	AddedKeyAbsent       bool
}

func TestTxnUnselectedLeaseMutationBranchDifferentialAgainstReferenceEtcd(t *testing.T) {
	reference := os.Getenv("REFERENCE_ETCD_ENDPOINT")
	if reference == "" {
		t.Skip("set REFERENCE_ETCD_ENDPOINT to run differential compatibility tests")
	}
	referenceOutcome := runTxnLeaseUnselectedBranchScenario(t, reference, "etcd")
	want := txnLeaseUnselectedBranchOutcome{
		Succeeded:            false,
		TxnRevisionGap:       0,
		ResponseCount:        1,
		SelectedRangeValue:   "old-x",
		SelectedRangeLeaseA:  true,
		LeaseAKeys:           []string{"x"},
		LeaseBKeys:           []string{"y"},
		LeaseATTLRevisionGap: 0,
		LeaseBTTLRevisionGap: 0,
		Values:               map[string]string{"x": "old-x", "y": "old-y"},
		Leases:               map[string]string{"x": "A", "y": "B"},
		AddedKeyAbsent:       true,
	}
	require.Equal(t, want, referenceOutcome)
	require.Equal(t, referenceOutcome, runTxnLeaseUnselectedBranchScenario(t, compatEndpoint(t), "kubebrain"))
}

func runTxnLeaseUnselectedBranchScenario(t *testing.T, endpoint, instance string) txnLeaseUnselectedBranchOutcome {
	t.Helper()
	cli, err := clientv3.New(clientv3.Config{Endpoints: []string{endpoint}, DialTimeout: 3 * time.Second})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, cli.Close()) })
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	t.Cleanup(cancel)
	prefix := fmt.Sprintf("/dbaas-txn-lease-unselected/%s/%d/", instance, time.Now().UnixNano())
	leaseA, err := cli.Grant(ctx, 300)
	require.NoError(t, err)
	leaseB, err := cli.Grant(ctx, 300)
	require.NoError(t, err)
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		_, _ = cli.Revoke(cleanupCtx, leaseA.ID)
		_, _ = cli.Revoke(cleanupCtx, leaseB.ID)
	})
	_, err = cli.Put(ctx, prefix+"x", "old-x", clientv3.WithLease(leaseA.ID))
	require.NoError(t, err)
	seed, err := cli.Put(ctx, prefix+"y", "old-y", clientv3.WithLease(leaseB.ID))
	require.NoError(t, err)
	baseRevision := seed.Header.Revision

	txn, err := cli.Txn(ctx).
		If(clientv3.Compare(clientv3.Version(prefix+"missing"), ">", 0)).
		Then(
			clientv3.OpPut(prefix+"x", "new-x", clientv3.WithLease(leaseB.ID)),
			clientv3.OpDelete(prefix+"y"),
			clientv3.OpPut(prefix+"w", "new-w", clientv3.WithLease(leaseA.ID)),
		).
		Else(clientv3.OpGet(prefix + "x")).
		Commit()
	require.NoError(t, err)
	require.Len(t, txn.Responses, 1)
	selected := txn.Responses[0].GetResponseRange()
	require.NotNil(t, selected)
	require.Len(t, selected.Kvs, 1)

	ttlA, err := cli.TimeToLive(ctx, leaseA.ID, clientv3.WithAttachedKeys())
	require.NoError(t, err)
	ttlB, err := cli.TimeToLive(ctx, leaseB.ID, clientv3.WithAttachedKeys())
	require.NoError(t, err)
	current, err := cli.Get(ctx, prefix, clientv3.WithPrefix())
	require.NoError(t, err)
	values := make(map[string]string, len(current.Kvs))
	leases := make(map[string]string, len(current.Kvs))
	for _, kv := range current.Kvs {
		key := string(kv.Key[len(prefix):])
		values[key] = string(kv.Value)
		switch kv.Lease {
		case int64(leaseA.ID):
			leases[key] = "A"
		case int64(leaseB.ID):
			leases[key] = "B"
		default:
			leases[key] = "other"
		}
	}
	_, addedKeyExists := values["w"]

	return txnLeaseUnselectedBranchOutcome{
		Succeeded:            txn.Succeeded,
		TxnRevisionGap:       txn.Header.Revision - baseRevision,
		ResponseCount:        len(txn.Responses),
		SelectedRangeValue:   string(selected.Kvs[0].Value),
		SelectedRangeLeaseA:  selected.Kvs[0].Lease == int64(leaseA.ID),
		LeaseAKeys:           relativeLeaseKeys(ttlA.Keys, prefix),
		LeaseBKeys:           relativeLeaseKeys(ttlB.Keys, prefix),
		LeaseATTLRevisionGap: ttlA.ResponseHeader.Revision - baseRevision,
		LeaseBTTLRevisionGap: ttlB.ResponseHeader.Revision - baseRevision,
		Values:               values,
		Leases:               leases,
		AddedKeyAbsent:       !addedKeyExists,
	}
}
