package main

import (
	"bytes"
	"errors"
	"fmt"

	"go.etcd.io/etcd/api/v3/etcdserverpb"
	clientv3 "go.etcd.io/etcd/client/v3"
)

func validateAuditHeader(header *etcdserverpb.ResponseHeader, clusterID uint64, minRevision int64) (uint64, int64, error) {
	if header == nil || header.ClusterId == 0 || header.MemberId == 0 || header.Revision <= 0 || header.Revision < minRevision {
		return 0, 0, errors.New("invalid response header")
	}
	if clusterID != 0 && header.ClusterId != clusterID {
		return 0, 0, fmt.Errorf("cluster ID changed from %d to %d", clusterID, header.ClusterId)
	}
	return header.ClusterId, header.Revision, nil
}

func validateAuditNestedHeader(header *etcdserverpb.ResponseHeader, clusterID uint64, revision int64) error {
	if header == nil {
		return nil
	}
	if header.Revision != revision {
		return errors.New("nested response revision differs from transaction header")
	}
	if header.ClusterId == 0 && header.MemberId == 0 {
		return nil
	}
	if header.ClusterId != clusterID || header.MemberId == 0 {
		return errors.New("nested response identity differs from transaction header")
	}
	return nil
}

func validateAuditGrant(response *clientv3.LeaseGrantResponse, requestedTTL int64) (uint64, clientv3.LeaseID, int64, int64, error) {
	if response == nil {
		return 0, clientv3.NoLease, 0, 0, errors.New("lease grant returned an empty response")
	}
	clusterID, revision, err := validateAuditHeader(response.ResponseHeader, 0, 1)
	if err != nil {
		return 0, clientv3.NoLease, 0, 0, fmt.Errorf("lease grant: %w", err)
	}
	if response.ID == clientv3.NoLease || response.Error != "" || response.TTL < requestedTTL || response.TTL > clientv3.MaxLeaseTTL {
		return 0, clientv3.NoLease, 0, 0, fmt.Errorf("lease grant returned invalid ID, TTL, or legacy error: id=%d ttl=%d error=%q", response.ID, response.TTL, response.Error)
	}
	return clusterID, response.ID, response.TTL, revision, nil
}

func validateAuditPut(response *clientv3.TxnResponse, clusterID uint64, minRevision int64) (int64, error) {
	if response == nil {
		return 0, errors.New("audit put returned an empty transaction response")
	}
	_, revision, err := validateAuditHeader(response.Header, clusterID, minRevision)
	if err != nil {
		return 0, fmt.Errorf("audit put: %w", err)
	}
	if !response.Succeeded || revision <= minRevision || len(response.Responses) != 1 || response.Responses[0] == nil || response.Responses[0].GetResponsePut() == nil {
		return 0, errors.New("audit put returned an invalid transaction envelope")
	}
	put := response.Responses[0].GetResponsePut()
	if put.PrevKv != nil {
		return 0, errors.New("audit put returned an invalid nested response")
	}
	if err := validateAuditNestedHeader(put.Header, clusterID, revision); err != nil {
		return 0, fmt.Errorf("audit put: %w", err)
	}
	return revision, nil
}

func validateAuditRead(response *clientv3.GetResponse, clusterID uint64, minRevision int64, key, value []byte, leaseID clientv3.LeaseID) (int64, error) {
	if response == nil {
		return 0, errors.New("audit read returned an empty response")
	}
	_, revision, err := validateAuditHeader(response.Header, clusterID, minRevision)
	if err != nil {
		return 0, fmt.Errorf("audit read: %w", err)
	}
	if response.More || response.Count != 1 || len(response.Kvs) != 1 {
		return 0, errors.New("audit read returned inconsistent count or pagination metadata")
	}
	kv := response.Kvs[0]
	if kv == nil || !bytes.Equal(kv.Key, key) || !bytes.Equal(kv.Value, value) || kv.Lease != int64(leaseID) || kv.CreateRevision != minRevision || kv.ModRevision != minRevision || kv.Version != 1 {
		return 0, errors.New("audit read returned invalid key/value or MVCC metadata")
	}
	return revision, nil
}

func validateAuditDelete(response *clientv3.TxnResponse, clusterID uint64, minRevision int64) (int64, error) {
	if response == nil {
		return 0, errors.New("audit delete returned an empty transaction response")
	}
	_, revision, err := validateAuditHeader(response.Header, clusterID, minRevision)
	if err != nil {
		return 0, fmt.Errorf("audit delete: %w", err)
	}
	if !response.Succeeded || revision <= minRevision || len(response.Responses) != 1 || response.Responses[0] == nil || response.Responses[0].GetResponseDeleteRange() == nil {
		return 0, errors.New("audit delete returned an invalid transaction envelope")
	}
	deleted := response.Responses[0].GetResponseDeleteRange()
	if deleted.Deleted != 1 || len(deleted.PrevKvs) != 0 {
		return 0, errors.New("audit delete returned an invalid nested response")
	}
	if err := validateAuditNestedHeader(deleted.Header, clusterID, revision); err != nil {
		return 0, fmt.Errorf("audit delete: %w", err)
	}
	return revision, nil
}

func validateAuditAbsent(response *clientv3.GetResponse, clusterID uint64, minRevision int64) (int64, error) {
	if response == nil {
		return 0, errors.New("audit absence read returned an empty response")
	}
	_, revision, err := validateAuditHeader(response.Header, clusterID, minRevision)
	if err != nil {
		return 0, fmt.Errorf("audit absence read: %w", err)
	}
	if response.More || response.Count != 0 || len(response.Kvs) != 0 {
		return 0, errors.New("audit absence read returned keys or inconsistent count metadata")
	}
	return revision, nil
}

func validateAuditRevoke(response *clientv3.LeaseRevokeResponse, clusterID uint64, minRevision int64) error {
	if response == nil {
		return errors.New("audit lease revoke returned an empty response")
	}
	if _, _, err := validateAuditHeader(response.Header, clusterID, minRevision); err != nil {
		return fmt.Errorf("audit lease revoke: %w", err)
	}
	return nil
}
