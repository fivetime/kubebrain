package main

import (
	"errors"
	"fmt"

	"go.etcd.io/etcd/api/v3/etcdserverpb"
	clientv3 "go.etcd.io/etcd/client/v3"
)

type verifyResponseAdmission struct {
	clusterID uint64
	revision  int64
}

func (a *verifyResponseAdmission) admitHeader(header *etcdserverpb.ResponseHeader) error {
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

func (a *verifyResponseAdmission) admitGet(response *clientv3.GetResponse, key []byte) error {
	if response == nil {
		return fmt.Errorf("target key %q returned an empty range response", key)
	}
	if err := a.admitHeader(response.Header); err != nil {
		return fmt.Errorf("target key %q response admission: %w", key, err)
	}
	return nil
}

func (a *verifyResponseAdmission) admitLease(response *clientv3.LeaseTimeToLiveResponse, sourceID int64) error {
	if response == nil {
		return fmt.Errorf("restored lease for source %d returned an empty TTL response", sourceID)
	}
	if err := a.admitHeader(response.ResponseHeader); err != nil {
		return fmt.Errorf("restored lease for source %d response admission: %w", sourceID, err)
	}
	return nil
}

func validateTargetPrefixCount(response *clientv3.GetResponse, prefix string, expected int64) error {
	if response == nil {
		return fmt.Errorf("target prefix %q returned an empty count response", prefix)
	}
	if response.Header == nil || response.Header.Revision <= 0 {
		return fmt.Errorf("target prefix %q returned no valid count revision", prefix)
	}
	if response.Count < 0 || response.Count != expected || len(response.Kvs) != 0 || response.More {
		return fmt.Errorf("target prefix %q contains %d keys, expected exactly %d with an empty count-only payload", prefix, response.Count, expected)
	}
	return nil
}
