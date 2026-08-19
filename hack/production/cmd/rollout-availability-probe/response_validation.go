package main

import (
	"bytes"
	"errors"
	"fmt"

	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
)

func validateResponseHeader(header *etcdserverpb.ResponseHeader, clusterID uint64, minRevision int64) (uint64, int64, error) {
	if header == nil || header.ClusterId == 0 || header.MemberId == 0 || header.Revision <= 0 || header.Revision < minRevision {
		return 0, 0, errors.New("invalid response header")
	}
	if clusterID != 0 && header.ClusterId != clusterID {
		return 0, 0, fmt.Errorf("response cluster ID changed from %d to %d", clusterID, header.ClusterId)
	}
	return header.ClusterId, header.Revision, nil
}

func validateDeleteResponse(response *clientv3.DeleteResponse, clusterID uint64, minRevision int64) (uint64, int64, error) {
	if response == nil {
		return 0, 0, errors.New("delete returned an empty response")
	}
	validatedCluster, revision, err := validateResponseHeader(response.Header, clusterID, minRevision)
	if err != nil {
		return 0, 0, fmt.Errorf("delete: %w", err)
	}
	if response.Deleted < 0 || len(response.PrevKvs) != 0 {
		return 0, 0, errors.New("delete returned inconsistent count or unrequested previous values")
	}
	return validatedCluster, revision, nil
}

func validateGrantResponse(response *clientv3.LeaseGrantResponse, clusterID uint64, minRevision, requestedTTL int64) (clientv3.LeaseID, int64, error) {
	if response == nil {
		return clientv3.NoLease, 0, errors.New("lease grant returned an empty response")
	}
	_, revision, err := validateResponseHeader(response.ResponseHeader, clusterID, minRevision)
	if err != nil {
		return clientv3.NoLease, 0, fmt.Errorf("lease grant: %w", err)
	}
	if response.ID == clientv3.NoLease || response.Error != "" || response.TTL < requestedTTL || response.TTL > clientv3.MaxLeaseTTL {
		return clientv3.NoLease, 0, fmt.Errorf("lease grant returned invalid ID, TTL, or legacy error: id=%d ttl=%d error=%q", response.ID, response.TTL, response.Error)
	}
	return response.ID, revision, nil
}

func validateKeepAliveResponse(response *clientv3.LeaseKeepAliveResponse, clusterID uint64, minRevision int64, leaseID clientv3.LeaseID, grantedTTL int64) (int64, error) {
	if response == nil {
		return 0, errors.New("lease keepalive returned an empty response")
	}
	_, revision, err := validateResponseHeader(response.ResponseHeader, clusterID, minRevision)
	if err != nil {
		return 0, fmt.Errorf("lease keepalive: %w", err)
	}
	if response.ID != leaseID || response.TTL <= 0 || response.TTL > grantedTTL {
		return 0, fmt.Errorf("lease keepalive returned invalid identity or TTL: id=%d ttl=%d", response.ID, response.TTL)
	}
	return revision, nil
}

func validatePutResponse(response *clientv3.PutResponse, clusterID uint64, minRevision int64) (int64, error) {
	if response == nil {
		return 0, errors.New("put returned an empty response")
	}
	_, revision, err := validateResponseHeader(response.Header, clusterID, minRevision)
	if err != nil {
		return 0, fmt.Errorf("put: %w", err)
	}
	if response.PrevKv != nil {
		return 0, errors.New("put returned an unrequested previous value")
	}
	if revision <= minRevision {
		return 0, errors.New("put response revision did not advance")
	}
	return revision, nil
}

func validateObservedPut(response *clientv3.GetResponse, clusterID uint64, minRevision int64, key, value string) (int64, error) {
	if response == nil {
		return 0, errors.New("put reconciliation returned an empty response")
	}
	_, _, err := validateResponseHeader(response.Header, clusterID, minRevision)
	if err != nil {
		return 0, fmt.Errorf("put reconciliation: %w", err)
	}
	if response.More || response.Count != 1 || len(response.Kvs) != 1 {
		return 0, errors.New("put reconciliation returned inconsistent count or pagination metadata")
	}
	kv := response.Kvs[0]
	if kv == nil || !bytes.Equal(kv.Key, []byte(key)) || !bytes.Equal(kv.Value, []byte(value)) || kv.Lease != 0 || kv.CreateRevision <= 0 || kv.ModRevision <= minRevision || kv.ModRevision > response.Header.Revision || kv.Version <= 0 {
		return 0, errors.New("put reconciliation returned invalid key/value or MVCC metadata")
	}
	return kv.ModRevision, nil
}

func validateTimeToLiveResponse(response *clientv3.LeaseTimeToLiveResponse, clusterID uint64, minRevision int64, leaseID clientv3.LeaseID, grantedTTL int64, key string) (int64, error) {
	if response == nil {
		return 0, errors.New("lease verification returned an empty response")
	}
	_, revision, err := validateResponseHeader(response.ResponseHeader, clusterID, minRevision)
	if err != nil {
		return 0, fmt.Errorf("lease verification: %w", err)
	}
	if response.ID != leaseID || response.TTL <= 0 || response.TTL > grantedTTL || response.GrantedTTL != grantedTTL || len(response.Keys) != 1 || !bytes.Equal(response.Keys[0], []byte(key)) {
		return 0, errors.New("lease verification returned invalid identity, TTL, or attached keys")
	}
	return revision, nil
}

func validateAbsentRange(response *clientv3.GetResponse, clusterID uint64, minRevision int64) error {
	if response == nil {
		return errors.New("cleanup range returned an empty response")
	}
	if _, _, err := validateResponseHeader(response.Header, clusterID, minRevision); err != nil {
		return fmt.Errorf("cleanup range: %w", err)
	}
	if response.More || response.Count != 0 || len(response.Kvs) != 0 {
		return errors.New("cleanup range returned keys or inconsistent count metadata")
	}
	return nil
}

func validateCreatedWatch(response clientv3.WatchResponse, clusterID uint64, minRevision int64) (int64, error) {
	if response.Err() != nil || !response.Created || response.Canceled || response.CompactRevision != 0 || len(response.Events) != 0 {
		return 0, errors.New("watch creation returned an invalid envelope")
	}
	_, revision, err := validateResponseHeader(response.Header, clusterID, minRevision)
	if err != nil {
		return 0, fmt.Errorf("watch creation: %w", err)
	}
	return revision, nil
}

func validatePutWatch(response clientv3.WatchResponse, clusterID uint64, putRevision int64, key, value string) (int64, error) {
	if response.Err() != nil || response.Canceled || response.Created || response.CompactRevision != 0 || len(response.Events) != 1 {
		return 0, errors.New("put watch returned an invalid envelope")
	}
	_, revision, err := validateResponseHeader(response.Header, clusterID, putRevision)
	if err != nil {
		return 0, fmt.Errorf("put watch: %w", err)
	}
	event := response.Events[0]
	if event == nil || event.Type != mvccpb.PUT || event.Kv == nil || event.PrevKv != nil || !bytes.Equal(event.Kv.Key, []byte(key)) || !bytes.Equal(event.Kv.Value, []byte(value)) || event.Kv.ModRevision != putRevision || event.Kv.CreateRevision <= 0 || event.Kv.Version <= 0 || event.Kv.Lease != 0 {
		return 0, errors.New("put watch returned invalid event identity or MVCC metadata")
	}
	return revision, nil
}
