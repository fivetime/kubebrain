package main

import (
	"errors"
	"fmt"

	"go.etcd.io/etcd/api/v3/etcdserverpb"
	clientv3 "go.etcd.io/etcd/client/v3"
)

type exportResponseAdmission struct {
	clusterID uint64
	revision  int64
}

func (a *exportResponseAdmission) admitHeader(header *etcdserverpb.ResponseHeader) error {
	if header == nil || header.ClusterId == 0 || header.MemberId == 0 || header.Revision <= 0 || header.Revision < a.revision {
		return errors.New("response returned an invalid or stale header")
	}
	if a.clusterID != 0 && header.ClusterId != a.clusterID {
		return fmt.Errorf("response cluster ID changed from %d to %d", a.clusterID, header.ClusterId)
	}
	if a.clusterID == 0 {
		a.clusterID = header.ClusterId
	}
	a.revision = header.Revision
	return nil
}

func (a *exportResponseAdmission) admitRange(response *clientv3.GetResponse) error {
	if response == nil {
		return errors.New("range returned an empty response")
	}
	if err := a.admitHeader(response.Header); err != nil {
		return fmt.Errorf("range response admission: %w", err)
	}
	return nil
}

func (a *exportResponseAdmission) admitLease(response *clientv3.LeaseTimeToLiveResponse, id int64) error {
	if response == nil {
		return fmt.Errorf("lease %d returned an empty TTL response", id)
	}
	if err := a.admitHeader(response.ResponseHeader); err != nil {
		return fmt.Errorf("lease %d TTL response admission: %w", id, err)
	}
	return nil
}
