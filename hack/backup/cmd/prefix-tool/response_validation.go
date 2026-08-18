package main

import (
	"fmt"

	etcdserverpb "go.etcd.io/etcd/api/v3/etcdserverpb"
	clientv3 "go.etcd.io/etcd/client/v3"
)

func validateCountResponse(response *clientv3.GetResponse) error {
	if response == nil || response.Header == nil || response.Header.Revision <= 0 {
		return fmt.Errorf("count returned an invalid response header")
	}
	if response.Count < 0 || len(response.Kvs) != 0 || response.More {
		return fmt.Errorf("count returned an invalid count-only payload")
	}
	return nil
}

func validateDeleteResponse(response *clientv3.DeleteResponse) error {
	if response == nil || response.Header == nil || response.Header.Revision <= 0 {
		return fmt.Errorf("delete returned an invalid response header")
	}
	if response.Deleted < 0 || len(response.PrevKvs) != 0 {
		return fmt.Errorf("delete returned an invalid payload")
	}
	return nil
}

func validatePutResponse(response *clientv3.PutResponse) error {
	if response == nil || response.Header == nil || response.Header.Revision <= 0 {
		return fmt.Errorf("put returned an invalid response header")
	}
	if response.PrevKv != nil {
		return fmt.Errorf("put returned an unrequested previous value")
	}
	return nil
}

func validateLeasePutTxnResponse(response *clientv3.TxnResponse, puts int) error {
	if response == nil || response.Header == nil || response.Header.Revision <= 0 || !response.Succeeded {
		return fmt.Errorf("lease-put transaction returned an invalid response envelope")
	}
	if len(response.Responses) != puts {
		return fmt.Errorf("lease-put transaction returned %d operations, want %d", len(response.Responses), puts)
	}
	for i, op := range response.Responses {
		if op == nil {
			return fmt.Errorf("lease-put transaction operation %d is nil", i)
		}
		put, ok := op.Response.(*etcdserverpb.ResponseOp_ResponsePut)
		if !ok || put.ResponsePut == nil || put.ResponsePut.Header == nil || put.ResponsePut.Header.Revision != response.Header.Revision || put.ResponsePut.PrevKv != nil {
			return fmt.Errorf("lease-put transaction operation %d is not a valid put response", i)
		}
	}
	return nil
}
