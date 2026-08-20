package main

import (
	"errors"
	"fmt"

	"go.etcd.io/etcd/api/v3/etcdserverpb"
	clientv3 "go.etcd.io/etcd/client/v3"
)

// prefixResponseAdmission binds every successful response produced by one
// prefix-tool action to one etcd cluster and a non-regressing MVCC timeline.
// Member IDs may change because the client can load-balance between calls.
type prefixResponseAdmission struct {
	clusterID uint64
	revision  int64
}

func (a *prefixResponseAdmission) admitHeader(header *etcdserverpb.ResponseHeader, mutation bool) error {
	if header == nil || header.ClusterId == 0 || header.MemberId == 0 || header.Revision <= 0 {
		return errors.New("prefix operation returned an invalid response identity")
	}
	if a.clusterID != 0 && header.ClusterId != a.clusterID {
		return fmt.Errorf("prefix operation cluster ID changed from %d to %d", a.clusterID, header.ClusterId)
	}
	if header.Revision < a.revision {
		return fmt.Errorf("prefix operation revision regressed from %d to %d", a.revision, header.Revision)
	}
	if mutation && a.revision != 0 && header.Revision <= a.revision {
		return errors.New("prefix mutation response revision did not advance")
	}
	if a.clusterID == 0 {
		a.clusterID = header.ClusterId
	}
	a.revision = header.Revision
	return nil
}

func (a *prefixResponseAdmission) admitCount(response *clientv3.GetResponse) error {
	if response == nil {
		return errors.New("prefix count returned an empty response")
	}
	return a.admitHeader(response.Header, false)
}

func (a *prefixResponseAdmission) admitDelete(response *clientv3.DeleteResponse) error {
	if response == nil {
		return errors.New("prefix delete returned an empty response")
	}
	return a.admitHeader(response.Header, response.Deleted > 0)
}

func (a *prefixResponseAdmission) admitPut(response *clientv3.PutResponse) error {
	if response == nil {
		return errors.New("prefix put returned an empty response")
	}
	return a.admitHeader(response.Header, true)
}

func (a *prefixResponseAdmission) admitGrant(response *clientv3.LeaseGrantResponse) error {
	if response == nil {
		return errors.New("prefix lease grant returned an empty response")
	}
	return a.admitHeader(response.ResponseHeader, false)
}

func (a *prefixResponseAdmission) admitLeasePut(response *clientv3.TxnResponse) error {
	if response == nil {
		return errors.New("prefix lease-put returned an empty response")
	}
	return a.admitHeader(response.Header, true)
}

func (a *prefixResponseAdmission) admitRevoke(response *clientv3.LeaseRevokeResponse) error {
	if response == nil {
		return errors.New("prefix lease revoke returned an empty response")
	}
	// Revoking an unattached lease need not advance the KV revision.
	return a.admitHeader(response.Header, false)
}
