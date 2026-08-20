package main

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
)

func verifyHeader(clusterID, memberID uint64, revision int64) *etcdserverpb.ResponseHeader {
	return &etcdserverpb.ResponseHeader{ClusterId: clusterID, MemberId: memberID, Revision: revision}
}

func TestVerifyResponseAdmissionAcceptsSameClusterAcrossMembers(t *testing.T) {
	a := &verifyResponseAdmission{}
	require.NoError(t, a.admitGet(&clientv3.GetResponse{Header: verifyHeader(7, 9, 10)}, []byte("a")))
	require.NoError(t, a.admitGet(&clientv3.GetResponse{Header: verifyHeader(7, 11, 11)}, []byte("b")))
	require.NoError(t, a.admitLease(&clientv3.LeaseTimeToLiveResponse{ResponseHeader: verifyHeader(7, 9, 11)}, 42))
	require.Equal(t, uint64(7), a.clusterID)
	require.Equal(t, uint64(7), a.ClusterID())
	require.Equal(t, int64(11), a.revision)
}

func TestVerifyResponseAdmissionRejectsMalformedOrDriftingHeaders(t *testing.T) {
	for name, response := range map[string]*clientv3.GetResponse{
		"nil response":  nil,
		"nil header":    {},
		"zero cluster":  {Header: verifyHeader(0, 9, 10)},
		"zero member":   {Header: verifyHeader(7, 0, 10)},
		"zero revision": {Header: verifyHeader(7, 9, 0)},
	} {
		t.Run(name, func(t *testing.T) {
			require.Error(t, (&verifyResponseAdmission{}).admitGet(response, []byte("key")))
		})
	}
	a := &verifyResponseAdmission{}
	require.NoError(t, a.admitGet(&clientv3.GetResponse{Header: verifyHeader(7, 9, 10)}, []byte("a")))
	require.ErrorContains(t, a.admitGet(&clientv3.GetResponse{Header: verifyHeader(8, 9, 11)}, []byte("b")), "cluster ID changed")
	require.ErrorContains(t, a.admitGet(&clientv3.GetResponse{Header: verifyHeader(7, 9, 9)}, []byte("b")), "stale header")
	require.ErrorContains(t, a.admitLease(&clientv3.LeaseTimeToLiveResponse{ResponseHeader: verifyHeader(8, 9, 11)}, 42), "cluster ID changed")
	require.Error(t, a.admitLease(nil, 42))
}

func TestValidateTargetPrefixCount(t *testing.T) {
	require.NoError(t, validateTargetPrefixCount(&clientv3.GetResponse{Header: verifyHeader(7, 9, 10), Count: 2}, "/target/", 2))
	for name, response := range map[string]*clientv3.GetResponse{
		"nil":             nil,
		"nil header":      {Count: 2},
		"negative count":  {Header: verifyHeader(7, 9, 10), Count: -1},
		"missing key":     {Header: verifyHeader(7, 9, 10), Count: 1},
		"extra key":       {Header: verifyHeader(7, 9, 10), Count: 3},
		"hidden payload":  {Header: verifyHeader(7, 9, 10), Count: 2, Kvs: []*mvccpb.KeyValue{{Key: []byte("hidden")}}},
		"unexpected more": {Header: verifyHeader(7, 9, 10), Count: 2, More: true},
	} {
		t.Run(name, func(t *testing.T) {
			require.Error(t, validateTargetPrefixCount(response, "/target/", 2))
		})
	}
}
