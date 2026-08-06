package compat

import (
	"context"
	"fmt"
	"os"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"
)

type txnLeaseMultiOpOutcome struct {
	Succeeded            bool
	TxnRevisionGap       int64
	ResponseCount        int
	DeleteCount          int64
	DeletePrevValue      string
	DeletePrevLeaseIsA   bool
	LeaseAKeys           []string
	LeaseBKeys           []string
	LeaseATTLRevisionGap int64
	LeaseBTTLRevisionGap int64
	TransferredValue     string
	TransferredLeaseIsB  bool
	DeletedKeyAbsent     bool
	AddedValue           string
	AddedLeaseIsA        bool
}

func TestTxnLeaseMultiOperationDifferentialAgainstReferenceEtcd(t *testing.T) {
	reference := os.Getenv("REFERENCE_ETCD_ENDPOINT")
	if reference == "" {
		t.Skip("set REFERENCE_ETCD_ENDPOINT to run differential compatibility tests")
	}
	referenceOutcome := runTxnLeaseMultiOperationScenario(t, reference, "etcd")
	want := txnLeaseMultiOpOutcome{
		Succeeded:            true,
		TxnRevisionGap:       4,
		ResponseCount:        3,
		DeleteCount:          1,
		DeletePrevValue:      "old-y",
		DeletePrevLeaseIsA:   true,
		LeaseAKeys:           []string{"w", "z"},
		LeaseBKeys:           []string{"x"},
		LeaseATTLRevisionGap: 4,
		LeaseBTTLRevisionGap: 4,
		TransferredValue:     "new-x",
		TransferredLeaseIsB:  true,
		DeletedKeyAbsent:     true,
		AddedValue:           "new-w",
		AddedLeaseIsA:        true,
	}
	require.Equal(t, want, referenceOutcome)
	require.Equal(t, referenceOutcome, runTxnLeaseMultiOperationScenario(t, compatEndpoint(t), "kubebrain"))
}

func runTxnLeaseMultiOperationScenario(t *testing.T, endpoint, instance string) txnLeaseMultiOpOutcome {
	t.Helper()
	cli, err := clientv3.New(clientv3.Config{Endpoints: []string{endpoint}, DialTimeout: 3 * time.Second})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, cli.Close()) })
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	t.Cleanup(cancel)
	prefix := fmt.Sprintf("/dbaas-txn-lease-multi/%s/%d/", instance, time.Now().UnixNano())
	base, err := cli.Get(ctx, prefix, clientv3.WithPrefix())
	require.NoError(t, err)
	baseRevision := base.Header.Revision
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
	for key, value := range map[string]string{"x": "old-x", "y": "old-y", "z": "old-z"} {
		_, err = cli.Put(ctx, prefix+key, value, clientv3.WithLease(leaseA.ID))
		require.NoError(t, err)
	}

	txn, err := cli.Txn(ctx).
		If(clientv3.Compare(clientv3.Version(prefix+"x"), ">", 0)).
		Then(
			clientv3.OpPut(prefix+"x", "new-x", clientv3.WithLease(leaseB.ID)),
			clientv3.OpDelete(prefix+"y", clientv3.WithPrevKV()),
			clientv3.OpPut(prefix+"w", "new-w", clientv3.WithLease(leaseA.ID)),
		).Commit()
	require.NoError(t, err)
	require.Len(t, txn.Responses, 3)
	deleted := txn.Responses[1].GetResponseDeleteRange()
	require.NotNil(t, deleted)
	require.Len(t, deleted.PrevKvs, 1)

	ttlA, err := cli.TimeToLive(ctx, leaseA.ID, clientv3.WithAttachedKeys())
	require.NoError(t, err)
	ttlB, err := cli.TimeToLive(ctx, leaseB.ID, clientv3.WithAttachedKeys())
	require.NoError(t, err)
	current, err := cli.Get(ctx, prefix, clientv3.WithPrefix())
	require.NoError(t, err)
	values := make(map[string]string, len(current.Kvs))
	leases := make(map[string]int64, len(current.Kvs))
	for _, kv := range current.Kvs {
		key := strings.TrimPrefix(string(kv.Key), prefix)
		values[key] = string(kv.Value)
		leases[key] = kv.Lease
	}
	_, deletedKeyExists := values["y"]

	return txnLeaseMultiOpOutcome{
		Succeeded:            txn.Succeeded,
		TxnRevisionGap:       txn.Header.Revision - baseRevision,
		ResponseCount:        len(txn.Responses),
		DeleteCount:          deleted.Deleted,
		DeletePrevValue:      string(deleted.PrevKvs[0].Value),
		DeletePrevLeaseIsA:   deleted.PrevKvs[0].Lease == int64(leaseA.ID),
		LeaseAKeys:           relativeLeaseKeys(ttlA.Keys, prefix),
		LeaseBKeys:           relativeLeaseKeys(ttlB.Keys, prefix),
		LeaseATTLRevisionGap: ttlA.ResponseHeader.Revision - baseRevision,
		LeaseBTTLRevisionGap: ttlB.ResponseHeader.Revision - baseRevision,
		TransferredValue:     values["x"],
		TransferredLeaseIsB:  leases["x"] == int64(leaseB.ID),
		DeletedKeyAbsent:     !deletedKeyExists,
		AddedValue:           values["w"],
		AddedLeaseIsA:        leases["w"] == int64(leaseA.ID),
	}
}

func relativeLeaseKeys(keys [][]byte, prefix string) []string {
	result := make([]string, len(keys))
	for index, key := range keys {
		result[index] = strings.TrimPrefix(string(key), prefix)
	}
	sort.Strings(result)
	return result
}
