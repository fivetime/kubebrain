package nativepitr

import (
	"bytes"
	"errors"
	"fmt"

	etcdserverpb "go.etcd.io/etcd/api/v3/etcdserverpb"
	clientv3 "go.etcd.io/etcd/client/v3"
)

func validateMetadataTxn(response *clientv3.TxnResponse, succeededOps int, kind string) (int64, error) {
	if response == nil || response.Header == nil || response.Header.Revision <= 0 {
		return 0, fmt.Errorf("%s metadata transaction returned an invalid response header", kind)
	}
	want := 0
	if response.Succeeded {
		want = succeededOps
	}
	if len(response.Responses) != want {
		return 0, fmt.Errorf("%s metadata transaction returned %d operations, want %d", kind, len(response.Responses), want)
	}
	return response.Header.Revision, nil
}

func validateMetadataCreateResponse(response *clientv3.TxnResponse, puts int) error {
	revision, err := validateMetadataTxn(response, puts, "create")
	if err != nil {
		return err
	}
	for i, op := range response.Responses {
		if op == nil {
			return fmt.Errorf("create metadata transaction operation %d is nil", i)
		}
		put, ok := op.Response.(*etcdserverpb.ResponseOp_ResponsePut)
		if !ok || put.ResponsePut == nil || put.ResponsePut.Header == nil || put.ResponsePut.Header.Revision != revision || put.ResponsePut.PrevKv != nil {
			return fmt.Errorf("create metadata transaction operation %d is not a valid put response", i)
		}
	}
	return nil
}

type metadataRangeExpectation struct {
	key    []byte
	prefix bool
}

func validateMetadataStatusResponse(response *clientv3.TxnResponse, expected []metadataRangeExpectation) error {
	revision, err := validateMetadataTxn(response, len(expected), "status")
	if err != nil {
		return err
	}
	if !response.Succeeded {
		return errors.New("status metadata transaction unexpectedly selected its failure branch")
	}
	for i, op := range response.Responses {
		if op == nil {
			return fmt.Errorf("status metadata transaction operation %d is nil", i)
		}
		rangeOp, ok := op.Response.(*etcdserverpb.ResponseOp_ResponseRange)
		if !ok || rangeOp.ResponseRange == nil {
			return fmt.Errorf("status metadata transaction operation %d is not a range response", i)
		}
		r := rangeOp.ResponseRange
		if r.Header == nil || r.Header.Revision != revision || r.Count < 0 || r.Count != int64(len(r.Kvs)) || r.More {
			return fmt.Errorf("status metadata range operation %d has an invalid envelope", i)
		}
		for j, kv := range r.Kvs {
			if kv == nil || (!expected[i].prefix && !bytes.Equal(kv.Key, expected[i].key)) || (expected[i].prefix && !bytes.HasPrefix(kv.Key, expected[i].key)) {
				return fmt.Errorf("status metadata range operation %d returned an unexpected key", i)
			}
			if j > 0 && bytes.Compare(r.Kvs[j-1].Key, kv.Key) >= 0 {
				return fmt.Errorf("status metadata range operation %d returned unordered or duplicate keys", i)
			}
		}
		if !expected[i].prefix && len(r.Kvs) > 1 {
			return fmt.Errorf("status metadata range operation %d returned multiple exact-key values", i)
		}
	}
	return nil
}

func validateMetadataDeleteResponse(response *clientv3.TxnResponse, deletes int) error {
	revision, err := validateMetadataTxn(response, deletes, "delete")
	if err != nil {
		return err
	}
	for i, op := range response.Responses {
		if op == nil {
			return fmt.Errorf("delete metadata transaction operation %d is nil", i)
		}
		deleted, ok := op.Response.(*etcdserverpb.ResponseOp_ResponseDeleteRange)
		if !ok || deleted.ResponseDeleteRange == nil || deleted.ResponseDeleteRange.Header == nil || deleted.ResponseDeleteRange.Header.Revision != revision || deleted.ResponseDeleteRange.Deleted < 0 || len(deleted.ResponseDeleteRange.PrevKvs) != 0 {
			return fmt.Errorf("delete metadata transaction operation %d is not a valid delete response", i)
		}
	}
	return nil
}
