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
)

type txnNestedLeaseOutcome struct {
	OuterSucceeded          bool
	InnerSucceeded          bool
	OuterResponseCount      int
	InnerResponseCount      int
	RevisionGap             int64
	InnerHeaderRevisionZero bool
	InnerPutHeaderGap       int64
	InnerDeleteHeaderGap    int64
	OuterPutHeaderGap       int64
	DeleteCount             int64
	DeletePrevLeaseIsB      bool
	LeaseAKeys              []string
	LeaseBKeys              []string
	LeaseATTLRevisionGap    int64
	LeaseBTTLRevisionGap    int64
	Values                  map[string]string
	Leases                  map[string]string
	UnselectedValueIntact   bool
}

func TestTxnNestedLeaseMutationDifferentialAgainstReferenceEtcd(t *testing.T) {
	reference := os.Getenv("REFERENCE_ETCD_ENDPOINT")
	if reference == "" {
		t.Skip("set REFERENCE_ETCD_ENDPOINT to run differential compatibility tests")
	}
	thenOutcome := txnNestedLeaseOutcome{
		OuterSucceeded:          true,
		InnerSucceeded:          true,
		OuterResponseCount:      2,
		InnerResponseCount:      2,
		RevisionGap:             1,
		InnerHeaderRevisionZero: true,
		InnerPutHeaderGap:       1,
		InnerDeleteHeaderGap:    1,
		OuterPutHeaderGap:       1,
		DeleteCount:             1,
		DeletePrevLeaseIsB:      true,
		LeaseAKeys:              []string{"w", "z"},
		LeaseBKeys:              []string{"x"},
		LeaseATTLRevisionGap:    1,
		LeaseBTTLRevisionGap:    1,
		Values:                  map[string]string{"w": "new-w", "x": "new-x", "z": "old-z"},
		Leases:                  map[string]string{"w": "A", "x": "B", "z": "A"},
		UnselectedValueIntact:   true,
	}
	elseOutcome := thenOutcome
	elseOutcome.InnerSucceeded = false
	for _, test := range []struct {
		name            string
		selectInnerThen bool
		want            txnNestedLeaseOutcome
	}{
		{name: "then", selectInnerThen: true, want: thenOutcome},
		{name: "else-with-unselected-invalid-lease", selectInnerThen: false, want: elseOutcome},
	} {
		t.Run(test.name, func(t *testing.T) {
			referenceOutcome := runTxnNestedLeaseScenario(t, reference, "etcd-"+test.name, test.selectInnerThen)
			require.Equal(t, test.want, referenceOutcome)
			require.Equal(t, referenceOutcome, runTxnNestedLeaseScenario(t, compatEndpoint(t), "kubebrain-"+test.name, test.selectInnerThen))
		})
	}
}

func runTxnNestedLeaseScenario(t *testing.T, endpoint, instance string, selectInnerThen bool) txnNestedLeaseOutcome {
	t.Helper()
	cli, err := clientv3.New(clientv3.Config{Endpoints: []string{endpoint}, DialTimeout: 3 * time.Second})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, cli.Close()) })
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	t.Cleanup(cancel)
	prefix := fmt.Sprintf("/dbaas-txn-nested-lease/%s/%d/", instance, time.Now().UnixNano())
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
	_, err = cli.Put(ctx, prefix+"y", "old-y", clientv3.WithLease(leaseB.ID))
	require.NoError(t, err)
	seed, err := cli.Put(ctx, prefix+"z", "old-z", clientv3.WithLease(leaseA.ID))
	require.NoError(t, err)
	baseRevision := seed.Header.Revision

	innerCompare := clientv3.Compare(clientv3.Version(prefix+"x"), ">", 0)
	innerThen := []clientv3.Op{
		clientv3.OpPut(prefix+"x", "new-x", clientv3.WithLease(leaseB.ID)),
		clientv3.OpDelete(prefix+"y", clientv3.WithPrevKV()),
	}
	innerElse := []clientv3.Op{
		clientv3.OpPut(prefix+"z", "must-not-write", clientv3.WithLease(leaseB.ID)),
	}
	if !selectInnerThen {
		innerCompare = clientv3.Compare(clientv3.Version(prefix+"missing"), ">", 0)
		innerThen = []clientv3.Op{
			clientv3.OpPut(prefix+"z", "must-not-write", clientv3.WithLease(clientv3.LeaseID(math.MaxInt64))),
		}
		innerElse = []clientv3.Op{
			clientv3.OpPut(prefix+"x", "new-x", clientv3.WithLease(leaseB.ID)),
			clientv3.OpDelete(prefix+"y", clientv3.WithPrevKV()),
		}
	}
	inner := clientv3.OpTxn([]clientv3.Cmp{innerCompare}, innerThen, innerElse)
	outer, err := cli.Txn(ctx).
		If(clientv3.Compare(clientv3.Version(prefix+"x"), ">", 0)).
		Then(inner, clientv3.OpPut(prefix+"w", "new-w", clientv3.WithLease(leaseA.ID))).
		Commit()
	require.NoError(t, err)
	require.Len(t, outer.Responses, 2)
	innerResponse := outer.Responses[0].GetResponseTxn()
	require.NotNil(t, innerResponse)
	require.Len(t, innerResponse.Responses, 2)
	innerPut := innerResponse.Responses[0].GetResponsePut()
	require.NotNil(t, innerPut)
	deleteResponse := innerResponse.Responses[1].GetResponseDeleteRange()
	require.NotNil(t, deleteResponse)
	require.Len(t, deleteResponse.PrevKvs, 1)
	outerPut := outer.Responses[1].GetResponsePut()
	require.NotNil(t, outerPut)
	txnRevision := outer.Header.Revision
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

	return txnNestedLeaseOutcome{
		OuterSucceeded:          outer.Succeeded,
		InnerSucceeded:          innerResponse.Succeeded,
		OuterResponseCount:      len(outer.Responses),
		InnerResponseCount:      len(innerResponse.Responses),
		RevisionGap:             txnRevision - baseRevision,
		InnerHeaderRevisionZero: innerResponse.Header.Revision == 0,
		InnerPutHeaderGap:       innerPut.Header.Revision - baseRevision,
		InnerDeleteHeaderGap:    deleteResponse.Header.Revision - baseRevision,
		OuterPutHeaderGap:       outerPut.Header.Revision - baseRevision,
		DeleteCount:             deleteResponse.Deleted,
		DeletePrevLeaseIsB:      deleteResponse.PrevKvs[0].Lease == int64(leaseB.ID),
		LeaseAKeys:              relativeLeaseKeys(ttlA.Keys, prefix),
		LeaseBKeys:              relativeLeaseKeys(ttlB.Keys, prefix),
		LeaseATTLRevisionGap:    ttlA.ResponseHeader.Revision - baseRevision,
		LeaseBTTLRevisionGap:    ttlB.ResponseHeader.Revision - baseRevision,
		Values:                  values,
		Leases:                  leases,
		UnselectedValueIntact:   values["z"] == "old-z" && leases["z"] == "A",
	}
}
