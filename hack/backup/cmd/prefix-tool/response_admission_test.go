package main

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	clientv3 "go.etcd.io/etcd/client/v3"
)

func prefixAdmissionHeader(clusterID, memberID uint64, revision int64) *etcdserverpb.ResponseHeader {
	return &etcdserverpb.ResponseHeader{ClusterId: clusterID, MemberId: memberID, Revision: revision}
}

func TestPrefixResponseAdmissionLifecycle(t *testing.T) {
	a := &prefixResponseAdmission{}
	require.NoError(t, a.admitGrant(&clientv3.LeaseGrantResponse{ResponseHeader: prefixAdmissionHeader(7, 9, 10)}))
	require.NoError(t, a.admitLeasePut(&clientv3.TxnResponse{Header: prefixAdmissionHeader(7, 11, 11)}))
	require.NoError(t, a.admitRevoke(&clientv3.LeaseRevokeResponse{Header: prefixAdmissionHeader(7, 9, 12)}))
}

func TestPrefixResponseAdmissionStandaloneActions(t *testing.T) {
	a := &prefixResponseAdmission{}
	require.NoError(t, a.admitCount(&clientv3.GetResponse{Header: prefixAdmissionHeader(7, 9, 10)}))
	require.NoError(t, a.admitPut(&clientv3.PutResponse{Header: prefixAdmissionHeader(7, 11, 11)}))
	require.NoError(t, a.admitDelete(&clientv3.DeleteResponse{Header: prefixAdmissionHeader(7, 9, 11)}))
	require.NoError(t, a.admitDelete(&clientv3.DeleteResponse{Header: prefixAdmissionHeader(7, 9, 12), Deleted: 1}))
}

func TestPrefixResponseAdmissionRejectsMalformedIdentityAndTimeline(t *testing.T) {
	for name, header := range map[string]*etcdserverpb.ResponseHeader{
		"nil": nil, "zero cluster": prefixAdmissionHeader(0, 9, 10),
		"zero member": prefixAdmissionHeader(7, 0, 10), "zero revision": prefixAdmissionHeader(7, 9, 0),
	} {
		t.Run(name, func(t *testing.T) {
			require.Error(t, (&prefixResponseAdmission{}).admitCount(&clientv3.GetResponse{Header: header}))
		})
	}
	a := &prefixResponseAdmission{}
	require.NoError(t, a.admitGrant(&clientv3.LeaseGrantResponse{ResponseHeader: prefixAdmissionHeader(7, 9, 10)}))
	require.ErrorContains(t, a.admitLeasePut(&clientv3.TxnResponse{Header: prefixAdmissionHeader(8, 9, 11)}), "cluster ID changed")
	require.ErrorContains(t, a.admitLeasePut(&clientv3.TxnResponse{Header: prefixAdmissionHeader(7, 9, 9)}), "regressed")
	require.ErrorContains(t, a.admitLeasePut(&clientv3.TxnResponse{Header: prefixAdmissionHeader(7, 9, 10)}), "did not advance")
	require.ErrorContains(t, a.admitRevoke(&clientv3.LeaseRevokeResponse{Header: prefixAdmissionHeader(7, 9, 9)}), "regressed")
}

func TestPrefixResponseAdmissionReferenceLifecycle(t *testing.T) {
	endpoint := os.Getenv("REFERENCE_PREFIX_ADMISSION_ENDPOINT")
	if endpoint == "" {
		t.Skip("set REFERENCE_PREFIX_ADMISSION_ENDPOINT to a disposable reference etcd")
	}
	cli, err := clientv3.New(clientv3.Config{Endpoints: []string{endpoint}, DialTimeout: 5 * time.Second})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, cli.Close()) })
	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
	defer cancel()
	prefix := "/prefix-admission-reference/"
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		_, _ = cli.Delete(cleanupCtx, prefix, clientv3.WithPrefix())
	})
	a := &prefixResponseAdmission{}
	grant, err := cli.Grant(ctx, 60)
	require.NoError(t, err)
	require.NoError(t, a.admitGrant(grant))
	txn, err := cli.Txn(ctx).Then(
		clientv3.OpPut(prefix+"a", "value", clientv3.WithLease(grant.ID)),
		clientv3.OpPut(prefix+"b", "value", clientv3.WithLease(grant.ID)),
	).Commit()
	require.NoError(t, err)
	require.NoError(t, a.admitLeasePut(txn))
	require.NoError(t, validateLeasePutTxnResponse(txn, 2))
	revoke, err := cli.Revoke(ctx, grant.ID)
	require.NoError(t, err)
	require.NoError(t, a.admitRevoke(revoke))
}
