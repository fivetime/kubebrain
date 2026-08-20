package targetverify

import (
	"errors"
	"fmt"

	"go.etcd.io/etcd/api/v3/etcdserverpb"
	clientv3 "go.etcd.io/etcd/client/v3"
)

// ResponseAdmission binds one semantic verification run to one etcd cluster
// and a non-regressing MVCC timeline. Requests may be load-balanced, so the
// member ID is required but is not fixed.
type ResponseAdmission struct {
	clusterID uint64
	revision  int64
}

func (a *ResponseAdmission) admitHeader(header *etcdserverpb.ResponseHeader, mutation bool) error {
	if header == nil || header.ClusterId == 0 || header.MemberId == 0 || header.Revision <= 0 {
		return errors.New("target response returned an invalid identity header")
	}
	if a.clusterID != 0 && header.ClusterId != a.clusterID {
		return fmt.Errorf("target response cluster ID changed from %d to %d", a.clusterID, header.ClusterId)
	}
	if header.Revision < a.revision {
		return fmt.Errorf("target response revision regressed from %d to %d", a.revision, header.Revision)
	}
	if mutation && a.revision != 0 && header.Revision <= a.revision {
		return errors.New("target mutation response revision did not advance")
	}
	if a.clusterID == 0 {
		a.clusterID = header.ClusterId
	}
	a.revision = header.Revision
	return nil
}

func (a *ResponseAdmission) AdmitRange(response *clientv3.GetResponse) error {
	if response == nil {
		return errors.New("target range returned an empty response")
	}
	return a.admitHeader(response.Header, false)
}

func (a *ResponseAdmission) AdmitLeaseTTL(response *clientv3.LeaseTimeToLiveResponse) error {
	if response == nil {
		return errors.New("target lease TTL returned an empty response")
	}
	return a.admitHeader(response.ResponseHeader, false)
}

func (a *ResponseAdmission) AdmitGrant(response *clientv3.LeaseGrantResponse) error {
	if response == nil {
		return errors.New("target lease grant returned an empty response")
	}
	return a.admitHeader(response.ResponseHeader, false)
}

func (a *ResponseAdmission) AdmitTxn(response *clientv3.TxnResponse, mutation bool) error {
	if response == nil {
		return errors.New("target transaction returned an empty response")
	}
	return a.admitHeader(response.Header, mutation)
}

func (a *ResponseAdmission) AdmitRevoke(response *clientv3.LeaseRevokeResponse) error {
	if response == nil {
		return errors.New("target lease revoke returned an empty response")
	}
	// Revoking an already detached lease does not necessarily change KV state.
	return a.admitHeader(response.Header, false)
}

func (a *ResponseAdmission) AdmitWatch(response clientv3.WatchResponse) error {
	return a.admitHeader(response.Header, false)
}
