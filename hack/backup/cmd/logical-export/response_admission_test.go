package main

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	clientv3 "go.etcd.io/etcd/client/v3"
)

func exportHeader(clusterID, memberID uint64, revision int64) *etcdserverpb.ResponseHeader {
	return &etcdserverpb.ResponseHeader{ClusterId: clusterID, MemberId: memberID, Revision: revision}
}

func TestExportResponseAdmissionAcceptsSameClusterAndAdvancingRevision(t *testing.T) {
	a := &exportResponseAdmission{}
	require.NoError(t, a.admitRange(&clientv3.GetResponse{Header: exportHeader(7, 9, 10)}))
	require.NoError(t, a.admitLease(&clientv3.LeaseTimeToLiveResponse{ResponseHeader: exportHeader(7, 9, 10)}, 42))
	// A load-balanced request may be served by another member, while cluster
	// identity and the global observed revision remain stable.
	require.NoError(t, a.admitRange(&clientv3.GetResponse{Header: exportHeader(7, 11, 12)}))
	require.Equal(t, uint64(7), a.clusterID)
	require.Equal(t, int64(12), a.revision)
}

func TestExportResponseAdmissionRejectsMalformedOrDriftingHeaders(t *testing.T) {
	for name, response := range map[string]*clientv3.GetResponse{
		"nil response":  nil,
		"nil header":    {},
		"zero cluster":  {Header: exportHeader(0, 9, 10)},
		"zero member":   {Header: exportHeader(7, 0, 10)},
		"zero revision": {Header: exportHeader(7, 9, 0)},
	} {
		t.Run(name, func(t *testing.T) {
			require.Error(t, (&exportResponseAdmission{}).admitRange(response))
		})
	}
	a := &exportResponseAdmission{}
	require.NoError(t, a.admitRange(&clientv3.GetResponse{Header: exportHeader(7, 9, 10)}))
	require.ErrorContains(t, a.admitRange(&clientv3.GetResponse{Header: exportHeader(8, 9, 11)}), "cluster ID changed")
	require.ErrorContains(t, a.admitRange(&clientv3.GetResponse{Header: exportHeader(7, 9, 9)}), "stale header")
	require.ErrorContains(t, a.admitLease(&clientv3.LeaseTimeToLiveResponse{ResponseHeader: exportHeader(8, 9, 11)}, 42), "cluster ID changed")
	require.Error(t, a.admitLease(nil, 42))
}
