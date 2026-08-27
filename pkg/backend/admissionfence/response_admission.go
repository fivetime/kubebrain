package admissionfence

import (
	"errors"
	"fmt"

	"go.etcd.io/etcd/api/v3/etcdserverpb"
	clientv3 "go.etcd.io/etcd/client/v3"
)

type responseAdmission struct {
	clusterID uint64
	revision  int64
	grantTTL  int64
}

func newResponseAdmission(expectedClusterID uint64) *responseAdmission {
	return &responseAdmission{clusterID: expectedClusterID}
}

func (a *responseAdmission) admitHeader(header *etcdserverpb.ResponseHeader, mutation bool) (int64, error) {
	if header == nil || header.ClusterId == 0 || header.MemberId == 0 || header.Revision <= 0 {
		return 0, errors.New("response returned an invalid header")
	}
	if a.clusterID != 0 && header.ClusterId != a.clusterID {
		return 0, fmt.Errorf("response cluster ID changed from %d to %d", a.clusterID, header.ClusterId)
	}
	if header.Revision < a.revision {
		return 0, fmt.Errorf("response revision regressed from %d to %d", a.revision, header.Revision)
	}
	if mutation && header.Revision <= a.revision {
		return 0, errors.New("mutation response revision did not advance")
	}
	if a.clusterID == 0 {
		a.clusterID = header.ClusterId
	}
	a.revision = header.Revision
	return header.Revision, nil
}

func (a *responseAdmission) admitNestedHeader(header *etcdserverpb.ResponseHeader, outer *etcdserverpb.ResponseHeader) error {
	if header == nil {
		return nil
	}
	if header.Revision != outer.Revision {
		return errors.New("nested response revision differs from transaction header")
	}
	if header.ClusterId == 0 && header.MemberId == 0 {
		return nil
	}
	if header.ClusterId != outer.ClusterId || header.MemberId != outer.MemberId {
		return errors.New("nested response identity differs from transaction header")
	}
	return nil
}

func (a *responseAdmission) admitPutTxn(response *clientv3.TxnResponse, puts int, kind string) (bool, error) {
	if response == nil {
		return false, fmt.Errorf("%s returned an empty transaction response", kind)
	}
	expected := 0
	if response.Succeeded {
		expected = puts
	}
	if len(response.Responses) != expected {
		return false, fmt.Errorf("%s returned %d operations, expected %d", kind, len(response.Responses), expected)
	}
	if _, err := a.admitHeader(response.Header, response.Succeeded); err != nil {
		return false, fmt.Errorf("%s response admission: %w", kind, err)
	}
	for i, operation := range response.Responses {
		if operation == nil {
			return false, fmt.Errorf("%s operation %d is not a canonical put response", kind, i)
		}
		put := operation.GetResponsePut()
		if put == nil || put.PrevKv != nil {
			return false, fmt.Errorf("%s operation %d is not a canonical put response", kind, i)
		}
		if err := a.admitNestedHeader(put.Header, response.Header); err != nil {
			return false, fmt.Errorf("%s put operation %d: %w", kind, i, err)
		}
	}
	return response.Succeeded, nil
}

func (a *responseAdmission) admitExactGet(response *clientv3.GetResponse, key, value string, kind string) error {
	if response == nil {
		return fmt.Errorf("%s returned an empty range response", kind)
	}
	revision, err := a.admitHeader(response.Header, false)
	if err != nil {
		return fmt.Errorf("%s response admission: %w", kind, err)
	}
	if response.Count != 1 || len(response.Kvs) != 1 || response.More || response.Kvs[0] == nil {
		return fmt.Errorf("%s did not return exactly one key", kind)
	}
	kv := response.Kvs[0]
	if string(kv.Key) != key || string(kv.Value) != value || kv.Lease != 0 || kv.CreateRevision <= 0 || kv.ModRevision < kv.CreateRevision || kv.ModRevision > revision || kv.Version <= 0 || kv.Version > kv.ModRevision-kv.CreateRevision+1 {
		return fmt.Errorf("%s returned an invalid key/value record", kind)
	}
	return nil
}

func (a *responseAdmission) admitEmptyGet(response *clientv3.GetResponse, kind string) error {
	if response == nil {
		return fmt.Errorf("%s returned an empty range response", kind)
	}
	if _, err := a.admitHeader(response.Header, false); err != nil {
		return fmt.Errorf("%s response admission: %w", kind, err)
	}
	if response.Count != 0 || len(response.Kvs) != 0 || response.More {
		return fmt.Errorf("%s did not return a canonical empty range", kind)
	}
	return nil
}

func (a *responseAdmission) admitExactLeasedGet(response *clientv3.GetResponse, key, value, kind string) error {
	if response == nil {
		return fmt.Errorf("%s returned an empty range response", kind)
	}
	revision, err := a.admitHeader(response.Header, false)
	if err != nil {
		return fmt.Errorf("%s response admission: %w", kind, err)
	}
	if response.Count != 1 || len(response.Kvs) != 1 || response.More || response.Kvs[0] == nil {
		return fmt.Errorf("%s did not return exactly one key", kind)
	}
	kv := response.Kvs[0]
	if string(kv.Key) != key || string(kv.Value) != value || kv.Lease <= 0 || kv.CreateRevision <= 0 ||
		kv.ModRevision != kv.CreateRevision || kv.ModRevision > revision || kv.Version != 1 {
		return fmt.Errorf("%s returned an invalid leased key/value record", kind)
	}
	return nil
}

func (a *responseAdmission) admitGrant(response *clientv3.LeaseGrantResponse, requestedTTL int64) error {
	if response == nil {
		return errors.New("session lease grant returned an empty response")
	}
	if _, err := a.admitHeader(response.ResponseHeader, false); err != nil {
		return fmt.Errorf("session lease grant response admission: %w", err)
	}
	if response.ID <= clientv3.NoLease || response.Error != "" || response.TTL < requestedTTL || response.TTL > clientv3.MaxLeaseTTL {
		return errors.New("session lease grant returned an invalid payload")
	}
	a.grantTTL = response.TTL
	return nil
}

func (a *responseAdmission) admitKeepAlive(response *clientv3.LeaseKeepAliveResponse, leaseID clientv3.LeaseID) error {
	if response == nil {
		return errors.New("session keepalive returned an empty response")
	}
	if _, err := a.admitHeader(response.ResponseHeader, false); err != nil {
		return fmt.Errorf("session keepalive response admission: %w", err)
	}
	if response.ID != leaseID || response.TTL <= 0 || response.TTL != a.grantTTL {
		return errors.New("session keepalive returned an invalid payload")
	}
	return nil
}

func (a *responseAdmission) admitRevoke(response *clientv3.LeaseRevokeResponse, kind string) error {
	if response == nil {
		return fmt.Errorf("%s returned an empty revoke response", kind)
	}
	if _, err := a.admitHeader(response.Header, false); err != nil {
		return fmt.Errorf("%s response admission: %w", kind, err)
	}
	return nil
}
