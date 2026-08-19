package main

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	clientv3 "go.etcd.io/etcd/client/v3"
)

func admittedHeader(revision int64) *etcdserverpb.ResponseHeader {
	return &etcdserverpb.ResponseHeader{ClusterId: 7, MemberId: 9, Revision: revision}
}

func admittedPut(revision int64, nested *etcdserverpb.ResponseHeader) *clientv3.TxnResponse {
	return &clientv3.TxnResponse{Header: admittedHeader(revision), Succeeded: true, Responses: []*etcdserverpb.ResponseOp{{
		Response: &etcdserverpb.ResponseOp_ResponsePut{ResponsePut: &etcdserverpb.PutResponse{Header: nested}},
	}}}
}

func admittedDelete(revision int64, nested *etcdserverpb.ResponseHeader) *clientv3.TxnResponse {
	return &clientv3.TxnResponse{Header: admittedHeader(revision), Succeeded: true, Responses: []*etcdserverpb.ResponseOp{{
		Response: &etcdserverpb.ResponseOp_ResponseDeleteRange{ResponseDeleteRange: &etcdserverpb.DeleteRangeResponse{Header: nested, Deleted: 1}},
	}}}
}

func TestRestoreResponseAdmissionAcceptsUpstreamNestedHeaders(t *testing.T) {
	a := &restoreResponseAdmission{}
	require.NoError(t, a.admitGrant(&clientv3.LeaseGrantResponse{ResponseHeader: admittedHeader(10), ID: 1, TTL: 60}))
	preflight := &clientv3.TxnResponse{Header: admittedHeader(10), Succeeded: true, Responses: []*etcdserverpb.ResponseOp{{
		Response: &etcdserverpb.ResponseOp_ResponseRange{ResponseRange: &etcdserverpb.RangeResponse{Header: &etcdserverpb.ResponseHeader{Revision: 10}}},
	}}}
	require.NoError(t, a.admitPreflight(preflight))
	revision, err := a.admitPut(admittedPut(11, nil))
	require.NoError(t, err)
	require.Equal(t, int64(11), revision)
	require.NoError(t, a.admitRevoke(&clientv3.LeaseRevokeResponse{Header: admittedHeader(11)}))
	require.NoError(t, a.admitRollback(admittedDelete(12, &etcdserverpb.ResponseHeader{Revision: 12})))
	require.Equal(t, uint64(7), a.clusterID)
	require.Equal(t, int64(12), a.revision)
}

func TestRestoreResponseAdmissionRejectsInvalidTopLevelIdentity(t *testing.T) {
	for name, mutate := range map[string]func(*etcdserverpb.ResponseHeader){
		"zero cluster":  func(header *etcdserverpb.ResponseHeader) { header.ClusterId = 0 },
		"zero member":   func(header *etcdserverpb.ResponseHeader) { header.MemberId = 0 },
		"zero revision": func(header *etcdserverpb.ResponseHeader) { header.Revision = 0 },
	} {
		t.Run(name, func(t *testing.T) {
			header := admittedHeader(10)
			mutate(header)
			a := &restoreResponseAdmission{}
			require.Error(t, a.admitGrant(&clientv3.LeaseGrantResponse{ResponseHeader: header, ID: 1, TTL: 60}))
		})
	}
	a := &restoreResponseAdmission{}
	require.NoError(t, a.admitGrant(&clientv3.LeaseGrantResponse{ResponseHeader: admittedHeader(10), ID: 1, TTL: 60}))
	wrongCluster := admittedPut(11, nil)
	wrongCluster.Header.ClusterId = 8
	_, err := a.admitPut(wrongCluster)
	require.ErrorContains(t, err, "cluster ID changed")
	_, err = a.admitPut(admittedPut(10, nil))
	require.ErrorContains(t, err, "did not advance")
	require.Error(t, a.admitRevoke(&clientv3.LeaseRevokeResponse{Header: admittedHeader(9)}))
}

func TestRestoreResponseAdmissionRejectsInvalidNestedIdentity(t *testing.T) {
	for name, nested := range map[string]*etcdserverpb.ResponseHeader{
		"wrong revision":   {Revision: 12},
		"partial identity": {ClusterId: 7, Revision: 11},
		"wrong cluster":    {ClusterId: 8, MemberId: 9, Revision: 11},
		"wrong member":     {ClusterId: 7, MemberId: 8, Revision: 11},
	} {
		t.Run(name, func(t *testing.T) {
			a := &restoreResponseAdmission{}
			require.NoError(t, a.admitGrant(&clientv3.LeaseGrantResponse{ResponseHeader: admittedHeader(10), ID: 1, TTL: 60}))
			_, err := a.admitPut(admittedPut(11, nested))
			require.Error(t, err)
		})
	}
}

func TestMalformedCommittedPutIsRecordedBeforeAdmissionFailure(t *testing.T) {
	a := &restoreResponseAdmission{}
	committed := []committedBatch{{keys: []string{"older"}, revision: 10}}
	response := admittedPut(11, &etcdserverpb.ResponseHeader{ClusterId: 7, Revision: 11})
	_, err := validateAndAdmitRestorePut(response, 1, []string{"current"}, true, &committed, a)
	require.ErrorContains(t, err, "nested response identity")
	require.Equal(t, []committedBatch{
		{keys: []string{"older"}, revision: 10},
		{keys: []string{"current"}, revision: 11},
	}, committed)

	committed = nil
	_, err = validateAndAdmitRestorePut(nil, 1, []string{"never"}, true, &committed, &restoreResponseAdmission{})
	require.Error(t, err)
	require.Empty(t, committed)
}
