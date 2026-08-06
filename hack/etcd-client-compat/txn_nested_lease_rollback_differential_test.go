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

type txnNestedLeaseRollbackOutcome struct {
	ErrorCode            string
	ErrorMessage         string
	HasResponse          bool
	RevisionGap          int64
	LeaseAKeys           []string
	LeaseBKeys           []string
	LeaseATTLRevisionGap int64
	LeaseBTTLRevisionGap int64
	Values               map[string]string
	Leases               map[string]string
	NewKeysAbsent        bool
}

func TestTxnNestedInvalidLeaseRollbackDifferentialAgainstReferenceEtcd(t *testing.T) {
	reference := os.Getenv("REFERENCE_ETCD_ENDPOINT")
	if reference == "" {
		t.Skip("set REFERENCE_ETCD_ENDPOINT to run differential compatibility tests")
	}
	referenceOutcome := runTxnNestedLeaseRollbackScenario(t, reference, "etcd")
	want := txnNestedLeaseRollbackOutcome{
		ErrorCode:            "Unknown",
		ErrorMessage:         "etcdserver: requested lease not found",
		RevisionGap:          0,
		LeaseAKeys:           []string{"x"},
		LeaseBKeys:           []string{},
		LeaseATTLRevisionGap: 0,
		LeaseBTTLRevisionGap: 0,
		Values:               map[string]string{"x": "old-x"},
		Leases:               map[string]string{"x": "A"},
		NewKeysAbsent:        true,
	}
	require.Equal(t, want, referenceOutcome)
	require.Equal(t, referenceOutcome, runTxnNestedLeaseRollbackScenario(t, compatEndpoint(t), "kubebrain"))
}

func runTxnNestedLeaseRollbackScenario(t *testing.T, endpoint, instance string) txnNestedLeaseRollbackOutcome {
	t.Helper()
	cli, err := clientv3.New(clientv3.Config{Endpoints: []string{endpoint}, DialTimeout: 3 * time.Second})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, cli.Close()) })
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	t.Cleanup(cancel)
	prefix := fmt.Sprintf("/dbaas-txn-nested-lease-rollback/%s/%d/", instance, time.Now().UnixNano())
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
	seed, err := cli.Put(ctx, prefix+"x", "old-x", clientv3.WithLease(leaseA.ID))
	require.NoError(t, err)
	baseRevision := seed.Header.Revision

	invalidInner := clientv3.OpTxn(
		[]clientv3.Cmp{clientv3.Compare(clientv3.Version(prefix+"x"), ">", 0)},
		[]clientv3.Op{
			clientv3.OpPut(prefix+"bad", "must-not-write", clientv3.WithLease(clientv3.LeaseID(math.MaxInt64))),
		},
		nil,
	)
	response, txnErr := cli.Txn(ctx).Then(
		clientv3.OpPut(prefix+"x", "new-x", clientv3.WithLease(leaseB.ID)),
		invalidInner,
		clientv3.OpPut(prefix+"w", "new-w", clientv3.WithLease(leaseA.ID)),
	).Commit()
	require.Error(t, txnErr)

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
	_, badExists := values["bad"]
	_, wExists := values["w"]

	return txnNestedLeaseRollbackOutcome{
		ErrorCode:            status.Code(txnErr).String(),
		ErrorMessage:         status.Convert(txnErr).Message(),
		HasResponse:          response != nil,
		RevisionGap:          current.Header.Revision - baseRevision,
		LeaseAKeys:           relativeLeaseKeys(ttlA.Keys, prefix),
		LeaseBKeys:           relativeLeaseKeys(ttlB.Keys, prefix),
		LeaseATTLRevisionGap: ttlA.ResponseHeader.Revision - baseRevision,
		LeaseBTTLRevisionGap: ttlB.ResponseHeader.Revision - baseRevision,
		Values:               values,
		Leases:               leases,
		NewKeysAbsent:        !badExists && !wExists,
	}
}
