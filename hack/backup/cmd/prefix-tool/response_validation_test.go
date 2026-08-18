package main

import (
	"testing"

	"github.com/stretchr/testify/require"
	etcdserverpb "go.etcd.io/etcd/api/v3/etcdserverpb"
	mvccpb "go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
)

func prefixHeader(revision int64) *etcdserverpb.ResponseHeader {
	return &etcdserverpb.ResponseHeader{Revision: revision}
}

func prefixPutOp(revision int64) *etcdserverpb.ResponseOp {
	return &etcdserverpb.ResponseOp{Response: &etcdserverpb.ResponseOp_ResponsePut{ResponsePut: &etcdserverpb.PutResponse{Header: prefixHeader(revision)}}}
}

func TestValidateCountResponse(t *testing.T) {
	require.NoError(t, validateCountResponse(&clientv3.GetResponse{Header: prefixHeader(5), Count: 3}))
	for i, response := range []*clientv3.GetResponse{
		nil,
		{},
		{Header: prefixHeader(0)},
		{Header: prefixHeader(5), Count: -1},
		{Header: prefixHeader(5), Kvs: []*mvccpb.KeyValue{{Key: []byte("hidden")}}},
		{Header: prefixHeader(5), More: true},
	} {
		require.Error(t, validateCountResponse(response), i)
	}
}

func TestValidateDeleteResponse(t *testing.T) {
	require.NoError(t, validateDeleteResponse(&clientv3.DeleteResponse{Header: prefixHeader(6), Deleted: 2}))
	for i, response := range []*clientv3.DeleteResponse{
		nil,
		{},
		{Header: prefixHeader(0)},
		{Header: prefixHeader(6), Deleted: -1},
		{Header: prefixHeader(6), PrevKvs: []*mvccpb.KeyValue{{Key: []byte("old")}}},
	} {
		require.Error(t, validateDeleteResponse(response), i)
	}
}

func TestValidatePutResponse(t *testing.T) {
	require.NoError(t, validatePutResponse(&clientv3.PutResponse{Header: prefixHeader(7)}))
	for i, response := range []*clientv3.PutResponse{
		nil,
		{},
		{Header: prefixHeader(0)},
		{Header: prefixHeader(7), PrevKv: &mvccpb.KeyValue{Key: []byte("old")}},
	} {
		require.Error(t, validatePutResponse(response), i)
	}
}

func TestValidateLeasePutTxnResponse(t *testing.T) {
	valid := &clientv3.TxnResponse{Header: prefixHeader(8), Succeeded: true, Responses: []*etcdserverpb.ResponseOp{prefixPutOp(8), prefixPutOp(8)}}
	require.NoError(t, validateLeasePutTxnResponse(valid, 2))
	wrongType := &etcdserverpb.ResponseOp{Response: &etcdserverpb.ResponseOp_ResponseRange{ResponseRange: &etcdserverpb.RangeResponse{Header: prefixHeader(8)}}}
	prev := prefixPutOp(8)
	prev.GetResponsePut().PrevKv = &mvccpb.KeyValue{Key: []byte("old")}
	for i, response := range []*clientv3.TxnResponse{
		nil,
		{},
		{Header: prefixHeader(0), Succeeded: true, Responses: valid.Responses},
		{Header: prefixHeader(8), Responses: valid.Responses},
		{Header: prefixHeader(8), Succeeded: true, Responses: valid.Responses[:1]},
		{Header: prefixHeader(8), Succeeded: true, Responses: []*etcdserverpb.ResponseOp{nil, prefixPutOp(8)}},
		{Header: prefixHeader(8), Succeeded: true, Responses: []*etcdserverpb.ResponseOp{wrongType, prefixPutOp(8)}},
		{Header: prefixHeader(8), Succeeded: true, Responses: []*etcdserverpb.ResponseOp{prefixPutOp(7), prefixPutOp(8)}},
		{Header: prefixHeader(8), Succeeded: true, Responses: []*etcdserverpb.ResponseOp{prev, prefixPutOp(8)}},
	} {
		require.Error(t, validateLeasePutTxnResponse(response, 2), i)
	}
}
