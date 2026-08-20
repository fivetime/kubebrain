package admissionfence

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
)

func admissionHeader(cluster, member uint64, revision int64) *etcdserverpb.ResponseHeader {
	return &etcdserverpb.ResponseHeader{ClusterId: cluster, MemberId: member, Revision: revision}
}

func putOperation(header *etcdserverpb.ResponseHeader) *etcdserverpb.ResponseOp {
	return &etcdserverpb.ResponseOp{Response: &etcdserverpb.ResponseOp_ResponsePut{ResponsePut: &etcdserverpb.PutResponse{Header: header}}}
}

func TestResponseAdmissionHeaderIdentityAndRevision(t *testing.T) {
	a := newResponseAdmission(0)
	_, err := a.admitHeader(admissionHeader(7, 9, 10), false)
	require.NoError(t, err)
	_, err = a.admitHeader(admissionHeader(7, 11, 10), false)
	require.NoError(t, err)
	_, err = a.admitHeader(admissionHeader(7, 9, 11), true)
	require.NoError(t, err)

	for name, header := range map[string]*etcdserverpb.ResponseHeader{
		"nil":           nil,
		"zero cluster":  admissionHeader(0, 9, 12),
		"zero member":   admissionHeader(7, 0, 12),
		"zero revision": admissionHeader(7, 9, 0),
		"wrong cluster": admissionHeader(8, 9, 12),
		"stale":         admissionHeader(7, 9, 10),
	} {
		t.Run(name, func(t *testing.T) {
			_, err := a.admitHeader(header, false)
			require.Error(t, err)
		})
	}
	_, err = a.admitHeader(admissionHeader(7, 9, 11), true)
	require.ErrorContains(t, err, "did not advance")
}

func TestResponseAdmissionPutTxn(t *testing.T) {
	outer := admissionHeader(7, 9, 10)
	require.True(t, mustAdmitPutTxn(t, newResponseAdmission(0), &clientv3.TxnResponse{
		Header: outer, Succeeded: true, Responses: []*etcdserverpb.ResponseOp{putOperation(nil), putOperation(admissionHeader(0, 0, 10))},
	}, 2))
	require.False(t, mustAdmitPutTxn(t, newResponseAdmission(0), &clientv3.TxnResponse{Header: outer}, 1))

	bad := []*clientv3.TxnResponse{
		nil,
		{Succeeded: true, Responses: []*etcdserverpb.ResponseOp{putOperation(nil)}},
		{Header: outer, Succeeded: true},
		{Header: outer, Succeeded: false, Responses: []*etcdserverpb.ResponseOp{putOperation(nil)}},
		{Header: outer, Succeeded: true, Responses: []*etcdserverpb.ResponseOp{nil}},
		{Header: outer, Succeeded: true, Responses: []*etcdserverpb.ResponseOp{{Response: &etcdserverpb.ResponseOp_ResponseRange{ResponseRange: &etcdserverpb.RangeResponse{}}}}},
		{Header: outer, Succeeded: true, Responses: []*etcdserverpb.ResponseOp{putOperation(admissionHeader(7, 9, 9))}},
		{Header: outer, Succeeded: true, Responses: []*etcdserverpb.ResponseOp{putOperation(admissionHeader(8, 9, 10))}},
	}
	prev := putOperation(nil)
	prev.GetResponsePut().PrevKv = &mvccpb.KeyValue{Key: []byte("hidden")}
	bad = append(bad, &clientv3.TxnResponse{Header: outer, Succeeded: true, Responses: []*etcdserverpb.ResponseOp{prev}})
	for i, response := range bad {
		_, err := newResponseAdmission(0).admitPutTxn(response, 1, "test transaction")
		require.Error(t, err, i)
	}
}

func mustAdmitPutTxn(t *testing.T, admission *responseAdmission, response *clientv3.TxnResponse, puts int) bool {
	t.Helper()
	succeeded, err := admission.admitPutTxn(response, puts, "test transaction")
	require.NoError(t, err)
	return succeeded
}

func TestResponseAdmissionRangePayloads(t *testing.T) {
	header := admissionHeader(7, 9, 10)
	kv := &mvccpb.KeyValue{Key: []byte("gate"), Value: []byte("open"), CreateRevision: 4, ModRevision: 8, Version: 2}
	require.NoError(t, newResponseAdmission(0).admitExactGet(&clientv3.GetResponse{Header: header, Count: 1, Kvs: []*mvccpb.KeyValue{kv}}, "gate", "open", "gate read"))
	require.NoError(t, newResponseAdmission(0).admitEmptyGet(&clientv3.GetResponse{Header: header}, "session read"))

	badExact := []*clientv3.GetResponse{
		nil,
		{Header: header},
		{Header: header, Count: 1, More: true, Kvs: []*mvccpb.KeyValue{kv}},
		{Header: header, Count: 1, Kvs: []*mvccpb.KeyValue{nil}},
		{Header: header, Count: 1, Kvs: []*mvccpb.KeyValue{{Key: []byte("wrong"), Value: []byte("open"), CreateRevision: 4, ModRevision: 8, Version: 2}}},
		{Header: header, Count: 1, Kvs: []*mvccpb.KeyValue{{Key: []byte("gate"), Value: []byte("open"), CreateRevision: 4, ModRevision: 11, Version: 2}}},
		{Header: header, Count: 1, Kvs: []*mvccpb.KeyValue{{Key: []byte("gate"), Value: []byte("open"), CreateRevision: 4, ModRevision: 8, Version: 2, Lease: 3}}},
		{Header: header, Count: 1, Kvs: []*mvccpb.KeyValue{{Key: []byte("gate"), Value: []byte("open"), CreateRevision: 8, ModRevision: 8, Version: 2}}},
	}
	for i, response := range badExact {
		require.Error(t, newResponseAdmission(0).admitExactGet(response, "gate", "open", "gate read"), i)
	}
	for i, response := range []*clientv3.GetResponse{
		nil,
		{Header: header, Count: -1},
		{Header: header, Count: 1},
		{Header: header, Kvs: []*mvccpb.KeyValue{kv}},
		{Header: header, More: true},
	} {
		require.Error(t, newResponseAdmission(0).admitEmptyGet(response, "session read"), i)
	}
}

func TestResponseAdmissionLeasePayloads(t *testing.T) {
	header := admissionHeader(7, 9, 10)
	a := newResponseAdmission(0)
	require.NoError(t, a.admitGrant(&clientv3.LeaseGrantResponse{ResponseHeader: header, ID: 12, TTL: 15}, 15))
	require.NoError(t, a.admitKeepAlive(&clientv3.LeaseKeepAliveResponse{ResponseHeader: admissionHeader(7, 11, 10), ID: 12, TTL: 15}, 12))
	require.NoError(t, a.admitRevoke(&clientv3.LeaseRevokeResponse{Header: admissionHeader(7, 9, 11)}, "session close"))

	for i, response := range []*clientv3.LeaseGrantResponse{
		nil,
		{ResponseHeader: header, TTL: 15},
		{ResponseHeader: header, ID: -1, TTL: 15},
		{ResponseHeader: header, ID: 12, TTL: 14},
		{ResponseHeader: header, ID: 12, TTL: 15, Error: "legacy"},
		{ResponseHeader: header, ID: 12, TTL: clientv3.MaxLeaseTTL + 1},
	} {
		require.Error(t, newResponseAdmission(0).admitGrant(response, 15), i)
	}
	for i, response := range []*clientv3.LeaseKeepAliveResponse{
		nil,
		{ResponseHeader: header, ID: 13, TTL: 15},
		{ResponseHeader: header, ID: 12, TTL: 0},
		{ResponseHeader: header, ID: 12, TTL: 14},
		{ResponseHeader: header, ID: 12, TTL: 16},
		{ResponseHeader: header, ID: 12, TTL: clientv3.MaxLeaseTTL + 1},
	} {
		require.Error(t, newResponseAdmission(0).admitKeepAlive(response, 12), i)
	}
	require.Error(t, newResponseAdmission(0).admitRevoke(nil, "session close"))
}
