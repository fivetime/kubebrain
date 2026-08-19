package main

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
)

func auditHeader(revision int64) *etcdserverpb.ResponseHeader {
	return &etcdserverpb.ResponseHeader{ClusterId: 7, MemberId: 9, Revision: revision}
}

func auditPutResponse(revision int64) *clientv3.TxnResponse {
	return &clientv3.TxnResponse{
		Header: auditHeader(revision), Succeeded: true,
		Responses: []*etcdserverpb.ResponseOp{{Response: &etcdserverpb.ResponseOp_ResponsePut{ResponsePut: &etcdserverpb.PutResponse{Header: auditHeader(revision)}}}},
	}
}

func auditDeleteResponse(revision int64) *clientv3.TxnResponse {
	return &clientv3.TxnResponse{
		Header: auditHeader(revision), Succeeded: true,
		Responses: []*etcdserverpb.ResponseOp{{Response: &etcdserverpb.ResponseOp_ResponseDeleteRange{ResponseDeleteRange: &etcdserverpb.DeleteRangeResponse{Header: auditHeader(revision), Deleted: 1}}}},
	}
}

func TestValidateAuditProbeResponseChain(t *testing.T) {
	clusterID, leaseID, ttl, revision, err := validateAuditGrant(&clientv3.LeaseGrantResponse{ResponseHeader: auditHeader(10), ID: 42, TTL: 60}, 60)
	require.NoError(t, err)
	require.Equal(t, uint64(7), clusterID)
	require.Equal(t, clientv3.LeaseID(42), leaseID)
	require.Equal(t, int64(60), ttl)

	revision, err = validateAuditPut(auditPutResponse(11), clusterID, revision)
	require.NoError(t, err)
	read := &clientv3.GetResponse{Header: auditHeader(11), Count: 1, Kvs: []*mvccpb.KeyValue{{Key: []byte("key"), Value: []byte("value"), CreateRevision: 11, ModRevision: 11, Version: 1, Lease: 42}}}
	revision, err = validateAuditRead(read, clusterID, revision, []byte("key"), []byte("value"), leaseID)
	require.NoError(t, err)
	revision, err = validateAuditDelete(auditDeleteResponse(12), clusterID, revision)
	require.NoError(t, err)
	revision, err = validateAuditAbsent(&clientv3.GetResponse{Header: auditHeader(12)}, clusterID, revision)
	require.NoError(t, err)
	require.NoError(t, validateAuditRevoke(&clientv3.LeaseRevokeResponse{Header: auditHeader(12)}, clusterID, revision))
	require.NoError(t, func() error {
		response := auditPutResponse(13)
		response.Responses[0].GetResponsePut().Header = nil
		_, err := validateAuditPut(response, clusterID, 12)
		return err
	}())
	require.NoError(t, func() error {
		response := auditDeleteResponse(13)
		response.Responses[0].GetResponseDeleteRange().Header = nil
		_, err := validateAuditDelete(response, clusterID, 12)
		return err
	}())
	require.NoError(t, func() error {
		response := auditPutResponse(13)
		response.Responses[0].GetResponsePut().Header = &etcdserverpb.ResponseHeader{Revision: 13}
		_, err := validateAuditPut(response, clusterID, 12)
		return err
	}())
}

func TestValidateAuditGrantRejectsMalformedSuccess(t *testing.T) {
	for name, response := range map[string]*clientv3.LeaseGrantResponse{
		"nil":          nil,
		"nil header":   {ID: 1, TTL: 60},
		"zero cluster": {ResponseHeader: &etcdserverpb.ResponseHeader{MemberId: 9, Revision: 1}, ID: 1, TTL: 60},
		"zero member":  {ResponseHeader: &etcdserverpb.ResponseHeader{ClusterId: 7, Revision: 1}, ID: 1, TTL: 60},
		"zero lease":   {ResponseHeader: auditHeader(1), TTL: 60},
		"short ttl":    {ResponseHeader: auditHeader(1), ID: 1, TTL: 59},
		"legacy error": {ResponseHeader: auditHeader(1), ID: 1, TTL: 60, Error: "denied"},
	} {
		t.Run(name, func(t *testing.T) {
			_, _, _, _, err := validateAuditGrant(response, 60)
			require.Error(t, err)
		})
	}
}

func TestValidateAuditPutRejectsMalformedSuccess(t *testing.T) {
	wrongCluster := auditPutResponse(11)
	wrongCluster.Header.ClusterId = 8
	stale := auditPutResponse(10)
	compareFalse := auditPutResponse(11)
	compareFalse.Succeeded = false
	missingOperation := auditPutResponse(11)
	missingOperation.Responses = nil
	nestedRevision := auditPutResponse(11)
	nestedRevision.Responses[0].GetResponsePut().Header.Revision = 12
	previous := auditPutResponse(11)
	previous.Responses[0].GetResponsePut().PrevKv = &mvccpb.KeyValue{Key: []byte("hidden")}
	partialIdentity := auditPutResponse(11)
	partialIdentity.Responses[0].GetResponsePut().Header = &etcdserverpb.ResponseHeader{ClusterId: 7, Revision: 11}
	for name, response := range map[string]*clientv3.TxnResponse{
		"nil":               nil,
		"wrong cluster":     wrongCluster,
		"stale revision":    stale,
		"compare false":     compareFalse,
		"missing operation": missingOperation,
		"nested revision":   nestedRevision,
		"previous value":    previous,
		"partial identity":  partialIdentity,
	} {
		t.Run(name, func(t *testing.T) {
			_, err := validateAuditPut(response, 7, 10)
			require.Error(t, err)
		})
	}
}

func TestValidateAuditReadRejectsMalformedSuccess(t *testing.T) {
	validKV := &mvccpb.KeyValue{Key: []byte("key"), Value: []byte("value"), CreateRevision: 11, ModRevision: 11, Version: 1, Lease: 42}
	for name, response := range map[string]*clientv3.GetResponse{
		"nil":           nil,
		"hidden kv":     {Header: auditHeader(11), Kvs: []*mvccpb.KeyValue{validKV}},
		"more":          {Header: auditHeader(11), Count: 1, More: true, Kvs: []*mvccpb.KeyValue{validKV}},
		"wrong key":     {Header: auditHeader(11), Count: 1, Kvs: []*mvccpb.KeyValue{{Key: []byte("other"), Value: []byte("value"), CreateRevision: 11, ModRevision: 11, Version: 1, Lease: 42}}},
		"wrong lease":   {Header: auditHeader(11), Count: 1, Kvs: []*mvccpb.KeyValue{{Key: []byte("key"), Value: []byte("value"), CreateRevision: 11, ModRevision: 11, Version: 1}}},
		"wrong version": {Header: auditHeader(11), Count: 1, Kvs: []*mvccpb.KeyValue{{Key: []byte("key"), Value: []byte("value"), CreateRevision: 11, ModRevision: 11, Version: 2, Lease: 42}}},
	} {
		t.Run(name, func(t *testing.T) {
			_, err := validateAuditRead(response, 7, 11, []byte("key"), []byte("value"), 42)
			require.Error(t, err)
		})
	}
}

func TestValidateAuditDeleteRejectsMalformedSuccess(t *testing.T) {
	compareFalse := auditDeleteResponse(12)
	compareFalse.Succeeded = false
	missingOperation := auditDeleteResponse(12)
	missingOperation.Responses = nil
	wrongCount := auditDeleteResponse(12)
	wrongCount.Responses[0].GetResponseDeleteRange().Deleted = 0
	previous := auditDeleteResponse(12)
	previous.Responses[0].GetResponseDeleteRange().PrevKvs = []*mvccpb.KeyValue{{Key: []byte("hidden")}}
	for name, response := range map[string]*clientv3.TxnResponse{
		"nil":               nil,
		"stale revision":    auditDeleteResponse(11),
		"compare false":     compareFalse,
		"missing operation": missingOperation,
		"wrong count":       wrongCount,
		"previous value":    previous,
	} {
		t.Run(name, func(t *testing.T) {
			_, err := validateAuditDelete(response, 7, 11)
			require.Error(t, err)
		})
	}
}

func TestValidateAuditAbsentAndRevokeRejectMalformedSuccess(t *testing.T) {
	for name, response := range map[string]*clientv3.GetResponse{
		"nil":         nil,
		"wrong count": {Header: auditHeader(12), Count: 1},
		"hidden kv":   {Header: auditHeader(12), Kvs: []*mvccpb.KeyValue{{Key: []byte("hidden")}}},
		"more":        {Header: auditHeader(12), More: true},
	} {
		t.Run(name, func(t *testing.T) {
			_, err := validateAuditAbsent(response, 7, 12)
			require.Error(t, err)
		})
	}
	require.Error(t, validateAuditRevoke(nil, 7, 12))
	require.Error(t, validateAuditRevoke(&clientv3.LeaseRevokeResponse{Header: &etcdserverpb.ResponseHeader{ClusterId: 8, MemberId: 9, Revision: 12}}, 7, 12))
}
