package targetverify

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	clientv3 "go.etcd.io/etcd/client/v3"
)

func targetAdmissionHeader(clusterID, memberID uint64, revision int64) *etcdserverpb.ResponseHeader {
	return &etcdserverpb.ResponseHeader{ClusterId: clusterID, MemberId: memberID, Revision: revision}
}

func TestResponseAdmissionSemanticLifecycle(t *testing.T) {
	a := &ResponseAdmission{}
	require.NoError(t, a.AdmitRange(&clientv3.GetResponse{Header: targetAdmissionHeader(7, 9, 10)}))
	require.NoError(t, a.AdmitLeaseTTL(&clientv3.LeaseTimeToLiveResponse{ResponseHeader: targetAdmissionHeader(7, 11, 10)}))
	require.NoError(t, a.AdmitWatch(clientv3.WatchResponse{Header: targetAdmissionHeader(7, 9, 10)}))
	require.NoError(t, a.AdmitGrant(&clientv3.LeaseGrantResponse{ResponseHeader: targetAdmissionHeader(7, 11, 10)}))
	require.NoError(t, a.AdmitTxn(&clientv3.TxnResponse{Header: targetAdmissionHeader(7, 9, 11)}, true))
	require.NoError(t, a.AdmitRange(&clientv3.GetResponse{Header: targetAdmissionHeader(7, 11, 11)}))
	require.NoError(t, a.AdmitTxn(&clientv3.TxnResponse{Header: targetAdmissionHeader(7, 9, 12)}, true))
	require.NoError(t, a.AdmitRevoke(&clientv3.LeaseRevokeResponse{Header: targetAdmissionHeader(7, 11, 12)}))
}

func TestResponseAdmissionRejectsIdentityAndTimelineDrift(t *testing.T) {
	for name, header := range map[string]*etcdserverpb.ResponseHeader{
		"nil": nil, "zero cluster": targetAdmissionHeader(0, 9, 10),
		"zero member": targetAdmissionHeader(7, 0, 10), "zero revision": targetAdmissionHeader(7, 9, 0),
	} {
		t.Run(name, func(t *testing.T) {
			require.Error(t, (&ResponseAdmission{}).AdmitRange(&clientv3.GetResponse{Header: header}))
		})
	}
	a := &ResponseAdmission{}
	require.NoError(t, a.AdmitRange(&clientv3.GetResponse{Header: targetAdmissionHeader(7, 9, 10)}))
	require.ErrorContains(t, a.AdmitLeaseTTL(&clientv3.LeaseTimeToLiveResponse{ResponseHeader: targetAdmissionHeader(8, 9, 11)}), "cluster ID changed")
	require.ErrorContains(t, a.AdmitGrant(&clientv3.LeaseGrantResponse{ResponseHeader: targetAdmissionHeader(7, 9, 9)}), "regressed")
	require.ErrorContains(t, a.AdmitTxn(&clientv3.TxnResponse{Header: targetAdmissionHeader(7, 9, 10)}, true), "did not advance")
	require.Error(t, (&ResponseAdmission{}).AdmitLeaseTTL(nil))
	require.Error(t, (&ResponseAdmission{}).AdmitGrant(nil))
	require.Error(t, (&ResponseAdmission{}).AdmitTxn(nil, true))
	require.Error(t, (&ResponseAdmission{}).AdmitRevoke(nil))
	require.Error(t, (&ResponseAdmission{}).AdmitWatch(clientv3.WatchResponse{}))
}

func TestResponseAdmissionReferenceSemanticLifecycle(t *testing.T) {
	endpoint := os.Getenv("REFERENCE_TARGET_ADMISSION_ENDPOINT")
	if endpoint == "" {
		t.Skip("set REFERENCE_TARGET_ADMISSION_ENDPOINT to a disposable reference etcd")
	}
	cli, err := clientv3.New(clientv3.Config{Endpoints: []string{endpoint}, DialTimeout: 5 * time.Second})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, cli.Close()) })
	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
	defer cancel()
	prefix := "/target-admission-reference/"
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		_, _ = cli.Delete(cleanupCtx, prefix, clientv3.WithPrefix())
	})
	seed, err := cli.Put(ctx, prefix+"seed", "seed")
	require.NoError(t, err)
	a := &ResponseAdmission{}
	historical, err := cli.Get(ctx, prefix, clientv3.WithPrefix(), clientv3.WithRev(seed.Header.Revision))
	require.NoError(t, err)
	require.NoError(t, a.AdmitRange(historical))
	current, err := cli.Get(ctx, prefix, clientv3.WithPrefix())
	require.NoError(t, err)
	require.NoError(t, a.AdmitRange(current))

	watch := cli.Watch(ctx, prefix+"probe", clientv3.WithCreatedNotify())
	created := <-watch
	require.True(t, created.Created)
	require.NoError(t, a.AdmitWatch(created))
	grant, err := cli.Grant(ctx, 60)
	require.NoError(t, err)
	require.NoError(t, a.AdmitGrant(grant))
	ttl, err := cli.TimeToLive(ctx, grant.ID, clientv3.WithAttachedKeys())
	require.NoError(t, err)
	require.NoError(t, a.AdmitLeaseTTL(ttl))
	put, err := cli.Txn(ctx).If(clientv3.Compare(clientv3.CreateRevision(prefix+"probe"), "=", 0)).Then(
		clientv3.OpPut(prefix+"probe", "value", clientv3.WithLease(grant.ID)),
	).Commit()
	require.NoError(t, err)
	require.NoError(t, a.AdmitTxn(put, true))
	putWatch := <-watch
	require.NoError(t, a.AdmitWatch(putWatch))
	read, err := cli.Get(ctx, prefix+"probe")
	require.NoError(t, err)
	require.NoError(t, a.AdmitRange(read))
	deleted, err := cli.Txn(ctx).Then(clientv3.OpDelete(prefix + "probe")).Commit()
	require.NoError(t, err)
	require.NoError(t, a.AdmitTxn(deleted, true))
	deleteWatch := <-watch
	require.NoError(t, a.AdmitWatch(deleteWatch))
	revoke, err := cli.Revoke(ctx, grant.ID)
	require.NoError(t, err)
	require.NoError(t, a.AdmitRevoke(revoke))
}
