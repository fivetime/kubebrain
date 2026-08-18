package nativepitr

import (
	"testing"

	"github.com/stretchr/testify/require"
	etcdserverpb "go.etcd.io/etcd/api/v3/etcdserverpb"
	mvccpb "go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
)

func metadataHeader(revision int64) *etcdserverpb.ResponseHeader {
	return &etcdserverpb.ResponseHeader{Revision: revision}
}

func metadataPut(revision int64) *etcdserverpb.ResponseOp {
	return &etcdserverpb.ResponseOp{Response: &etcdserverpb.ResponseOp_ResponsePut{ResponsePut: &etcdserverpb.PutResponse{Header: metadataHeader(revision)}}}
}

func metadataRange(revision int64, keys ...string) *etcdserverpb.ResponseOp {
	kvs := make([]*mvccpb.KeyValue, 0, len(keys))
	for _, key := range keys {
		kvs = append(kvs, &mvccpb.KeyValue{Key: []byte(key), Value: []byte("value")})
	}
	return &etcdserverpb.ResponseOp{Response: &etcdserverpb.ResponseOp_ResponseRange{ResponseRange: &etcdserverpb.RangeResponse{Header: metadataHeader(revision), Kvs: kvs, Count: int64(len(kvs))}}}
}

func metadataDelete(revision int64) *etcdserverpb.ResponseOp {
	return &etcdserverpb.ResponseOp{Response: &etcdserverpb.ResponseOp_ResponseDeleteRange{ResponseDeleteRange: &etcdserverpb.DeleteRangeResponse{Header: metadataHeader(revision)}}}
}

func TestValidateMetadataCreateResponse(t *testing.T) {
	valid := &clientv3.TxnResponse{Header: metadataHeader(9), Succeeded: true, Responses: []*etcdserverpb.ResponseOp{metadataPut(9), metadataPut(9)}}
	require.NoError(t, validateMetadataCreateResponse(valid, 2))
	require.NoError(t, validateMetadataCreateResponse(&clientv3.TxnResponse{Header: metadataHeader(9)}, 2))

	tests := []*clientv3.TxnResponse{
		nil,
		{Header: metadataHeader(0), Succeeded: true, Responses: valid.Responses},
		{Header: metadataHeader(9), Succeeded: true, Responses: valid.Responses[:1]},
		{Header: metadataHeader(9), Responses: []*etcdserverpb.ResponseOp{metadataPut(9)}},
		{Header: metadataHeader(9), Succeeded: true, Responses: []*etcdserverpb.ResponseOp{nil, metadataPut(9)}},
		{Header: metadataHeader(9), Succeeded: true, Responses: []*etcdserverpb.ResponseOp{metadataRange(9), metadataPut(9)}},
		{Header: metadataHeader(9), Succeeded: true, Responses: []*etcdserverpb.ResponseOp{metadataPut(8), metadataPut(9)}},
		{Header: metadataHeader(9), Succeeded: true, Responses: []*etcdserverpb.ResponseOp{{Response: &etcdserverpb.ResponseOp_ResponsePut{ResponsePut: &etcdserverpb.PutResponse{Header: metadataHeader(9), PrevKv: &mvccpb.KeyValue{Key: []byte("old")}}}}, metadataPut(9)}},
	}
	for i, response := range tests {
		require.Error(t, validateMetadataCreateResponse(response, 2), i)
	}
}

func TestValidateMetadataStatusResponse(t *testing.T) {
	expected := []metadataRangeExpectation{{key: []byte("owner")}, {key: []byte("ranges/"), prefix: true}}
	valid := &clientv3.TxnResponse{Header: metadataHeader(11), Succeeded: true, Responses: []*etcdserverpb.ResponseOp{metadataRange(11, "owner"), metadataRange(11, "ranges/a", "ranges/b")}}
	require.NoError(t, validateMetadataStatusResponse(valid, expected))

	wrongType := metadataPut(11)
	badHeader := metadataRange(10, "owner")
	badCount := metadataRange(11, "owner")
	badCount.GetResponseRange().Count = 2
	more := metadataRange(11, "owner")
	more.GetResponseRange().More = true
	nilKV := metadataRange(11, "owner")
	nilKV.GetResponseRange().Kvs[0] = nil
	tests := []*clientv3.TxnResponse{
		nil,
		{Header: metadataHeader(11), Responses: nil},
		{Header: metadataHeader(11), Succeeded: true, Responses: valid.Responses[:1]},
		{Header: metadataHeader(11), Succeeded: true, Responses: []*etcdserverpb.ResponseOp{nil, valid.Responses[1]}},
		{Header: metadataHeader(11), Succeeded: true, Responses: []*etcdserverpb.ResponseOp{wrongType, valid.Responses[1]}},
		{Header: metadataHeader(11), Succeeded: true, Responses: []*etcdserverpb.ResponseOp{badHeader, valid.Responses[1]}},
		{Header: metadataHeader(11), Succeeded: true, Responses: []*etcdserverpb.ResponseOp{badCount, valid.Responses[1]}},
		{Header: metadataHeader(11), Succeeded: true, Responses: []*etcdserverpb.ResponseOp{more, valid.Responses[1]}},
		{Header: metadataHeader(11), Succeeded: true, Responses: []*etcdserverpb.ResponseOp{nilKV, valid.Responses[1]}},
		{Header: metadataHeader(11), Succeeded: true, Responses: []*etcdserverpb.ResponseOp{metadataRange(11, "other"), valid.Responses[1]}},
		{Header: metadataHeader(11), Succeeded: true, Responses: []*etcdserverpb.ResponseOp{metadataRange(11, "owner"), metadataRange(11, "ranges/b", "ranges/a")}},
		{Header: metadataHeader(11), Succeeded: true, Responses: []*etcdserverpb.ResponseOp{metadataRange(11, "owner", "owner"), valid.Responses[1]}},
	}
	for i, response := range tests {
		require.Error(t, validateMetadataStatusResponse(response, expected), i)
	}
}

func TestValidateMetadataDeleteResponse(t *testing.T) {
	valid := &clientv3.TxnResponse{Header: metadataHeader(13), Succeeded: true, Responses: []*etcdserverpb.ResponseOp{metadataDelete(13), metadataDelete(13)}}
	require.NoError(t, validateMetadataDeleteResponse(valid, 2))
	require.NoError(t, validateMetadataDeleteResponse(&clientv3.TxnResponse{Header: metadataHeader(13)}, 2))

	negative := metadataDelete(13)
	negative.GetResponseDeleteRange().Deleted = -1
	prev := metadataDelete(13)
	prev.GetResponseDeleteRange().PrevKvs = []*mvccpb.KeyValue{{Key: []byte("old")}}
	tests := []*clientv3.TxnResponse{
		nil,
		{Header: metadataHeader(13), Succeeded: true, Responses: valid.Responses[:1]},
		{Header: metadataHeader(13), Responses: []*etcdserverpb.ResponseOp{metadataDelete(13)}},
		{Header: metadataHeader(13), Succeeded: true, Responses: []*etcdserverpb.ResponseOp{nil, metadataDelete(13)}},
		{Header: metadataHeader(13), Succeeded: true, Responses: []*etcdserverpb.ResponseOp{metadataPut(13), metadataDelete(13)}},
		{Header: metadataHeader(13), Succeeded: true, Responses: []*etcdserverpb.ResponseOp{metadataDelete(12), metadataDelete(13)}},
		{Header: metadataHeader(13), Succeeded: true, Responses: []*etcdserverpb.ResponseOp{negative, metadataDelete(13)}},
		{Header: metadataHeader(13), Succeeded: true, Responses: []*etcdserverpb.ResponseOp{prev, metadataDelete(13)}},
	}
	for i, response := range tests {
		require.Error(t, validateMetadataDeleteResponse(response, 2), i)
	}
}
