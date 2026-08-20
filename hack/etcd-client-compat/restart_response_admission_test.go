package compat

import (
	"bytes"
	"errors"
	"fmt"

	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
)

type restartResponseAdmission struct {
	clusterID uint64
	revision  int64
}

func (a *restartResponseAdmission) admitHeader(header *etcdserverpb.ResponseHeader, mutation bool) (int64, error) {
	if header == nil || header.ClusterId == 0 || header.MemberId == 0 || header.Revision <= 0 {
		return 0, errors.New("restart response returned an invalid header")
	}
	if a.clusterID != 0 && header.ClusterId != a.clusterID {
		return 0, fmt.Errorf("restart response cluster ID changed from %d to %d", a.clusterID, header.ClusterId)
	}
	if header.Revision < a.revision {
		return 0, fmt.Errorf("restart response revision regressed from %d to %d", a.revision, header.Revision)
	}
	if mutation && header.Revision <= a.revision {
		return 0, errors.New("restart mutation response revision did not advance")
	}
	if a.clusterID == 0 {
		a.clusterID = header.ClusterId
	}
	a.revision = header.Revision
	return header.Revision, nil
}

func (a *restartResponseAdmission) admitPut(response *clientv3.PutResponse) (int64, error) {
	if response == nil || response.PrevKv != nil {
		return 0, errors.New("restart put returned an empty or non-canonical response")
	}
	return a.admitHeader(response.Header, true)
}

func (a *restartResponseAdmission) admitDelete(response *clientv3.DeleteResponse, expected int64) (int64, error) {
	if response == nil || response.Deleted != expected || len(response.PrevKvs) != 0 {
		return 0, errors.New("restart delete returned an empty or non-canonical response")
	}
	return a.admitHeader(response.Header, expected > 0)
}

func (a *restartResponseAdmission) admitGrant(response *clientv3.LeaseGrantResponse, requestedTTL int64) error {
	if response == nil {
		return errors.New("restart lease grant returned an empty response")
	}
	if _, err := a.admitHeader(response.ResponseHeader, false); err != nil {
		return err
	}
	if response.ID <= clientv3.NoLease || response.Error != "" || response.TTL < requestedTTL || response.TTL > clientv3.MaxLeaseTTL {
		return errors.New("restart lease grant returned an invalid payload")
	}
	return nil
}

type restartRangeExpectation struct {
	key, value       []byte
	present          bool
	leaseID          clientv3.LeaseID
	maxKVRevision    int64
	exactCreate      int64
	exactModRevision int64
}

func (a *restartResponseAdmission) admitRange(response *clientv3.GetResponse, expected restartRangeExpectation) (*mvccpb.KeyValue, error) {
	if response == nil {
		return nil, errors.New("restart range returned an empty response")
	}
	headerRevision, err := a.admitHeader(response.Header, false)
	if err != nil {
		return nil, err
	}
	wantCount := int64(0)
	if expected.present {
		wantCount = 1
	}
	if response.Count != wantCount || int64(len(response.Kvs)) != wantCount || response.More {
		return nil, errors.New("restart range returned an invalid point-range envelope")
	}
	if !expected.present {
		return nil, nil
	}
	kv := response.Kvs[0]
	maxRevision := expected.maxKVRevision
	if maxRevision == 0 {
		maxRevision = headerRevision
	}
	if kv == nil || !bytes.Equal(kv.Key, expected.key) || !bytes.Equal(kv.Value, expected.value) || kv.Lease != int64(expected.leaseID) || kv.CreateRevision <= 0 || kv.ModRevision < kv.CreateRevision || kv.ModRevision > maxRevision || kv.Version <= 0 || kv.Version > kv.ModRevision-kv.CreateRevision+1 {
		return nil, errors.New("restart range returned an invalid key-value record")
	}
	if expected.exactCreate != 0 && kv.CreateRevision != expected.exactCreate {
		return nil, errors.New("restart range returned an unexpected create revision")
	}
	if expected.exactModRevision != 0 && kv.ModRevision != expected.exactModRevision {
		return nil, errors.New("restart range returned an unexpected modification revision")
	}
	return kv, nil
}

func (a *restartResponseAdmission) admitTTL(response *clientv3.LeaseTimeToLiveResponse, leaseID clientv3.LeaseID, grantedTTL int64, keys [][]byte) error {
	if response == nil {
		return errors.New("restart lease TTL returned an empty response")
	}
	if _, err := a.admitHeader(response.ResponseHeader, false); err != nil {
		return err
	}
	if response.ID != leaseID || response.TTL <= 0 || response.TTL > grantedTTL || response.GrantedTTL != grantedTTL || len(response.Keys) != len(keys) {
		return errors.New("restart lease TTL returned an invalid payload")
	}
	for i := range keys {
		if !bytes.Equal(response.Keys[i], keys[i]) {
			return errors.New("restart lease TTL returned unexpected attached keys")
		}
	}
	return nil
}

func (a *restartResponseAdmission) admitWatch(response clientv3.WatchResponse) error {
	if response.Err() != nil || response.Canceled || response.Created || response.CompactRevision != 0 || len(response.Events) == 0 {
		return errors.New("restart watch returned an invalid envelope")
	}
	headerRevision, err := a.admitHeader(response.Header, false)
	if err != nil {
		return err
	}
	for _, event := range response.Events {
		if event == nil || event.Type != mvccpb.PUT || event.Kv == nil || event.PrevKv != nil || event.Kv.CreateRevision <= 0 || event.Kv.ModRevision < event.Kv.CreateRevision || event.Kv.ModRevision > headerRevision || event.Kv.Version <= 0 || event.Kv.Version > event.Kv.ModRevision-event.Kv.CreateRevision+1 {
			return errors.New("restart watch returned an invalid event")
		}
	}
	return nil
}

func (a *restartResponseAdmission) admitStatus(response *etcdserverpb.StatusResponse) error {
	if response == nil || response.Leader == 0 || len(response.Errors) != 0 || response.DbSize < 0 || response.DbSizeInUse < 0 {
		return errors.New("restart status returned an invalid payload")
	}
	_, err := a.admitHeader(response.Header, false)
	return err
}

func (a *restartResponseAdmission) admitAlarm(response *etcdserverpb.AlarmResponse) error {
	if response == nil {
		return errors.New("restart alarm returned an empty response")
	}
	if _, err := a.admitHeader(response.Header, false); err != nil {
		return err
	}
	seen := make(map[uint64]struct{}, len(response.Alarms))
	for _, alarm := range response.Alarms {
		if alarm == nil || alarm.MemberID == 0 || alarm.Alarm == etcdserverpb.AlarmType_NONE {
			return errors.New("restart alarm returned an invalid member")
		}
		if _, duplicate := seen[alarm.MemberID]; duplicate {
			return errors.New("restart alarm returned a duplicate member")
		}
		seen[alarm.MemberID] = struct{}{}
	}
	return nil
}

func (a *restartResponseAdmission) admitRevoke(response *clientv3.LeaseRevokeResponse) error {
	if response == nil {
		return errors.New("restart lease revoke returned an empty response")
	}
	_, err := a.admitHeader(response.Header, false)
	return err
}
