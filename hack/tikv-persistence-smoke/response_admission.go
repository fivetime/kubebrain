package main

import (
	"errors"
	"fmt"

	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
)

type smokeResponseAdmission struct {
	clusterID uint64
	revision  int64
}

func (a *smokeResponseAdmission) admitHeader(header *etcdserverpb.ResponseHeader, mutation bool) (int64, error) {
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

func (a *smokeResponseAdmission) admitPut(response *clientv3.PutResponse) error {
	if response == nil {
		return errors.New("etcd put returned an empty response")
	}
	if response.PrevKv != nil {
		return errors.New("etcd put returned an unrequested previous key-value")
	}
	if _, err := a.admitHeader(response.Header, true); err != nil {
		return fmt.Errorf("etcd put response admission: %w", err)
	}
	return nil
}

func (a *smokeResponseAdmission) admitPointGet(response *clientv3.GetResponse, key string) (*mvccpb.KeyValue, error) {
	if response == nil {
		return nil, errors.New("etcd get returned an empty response")
	}
	revision, err := a.admitHeader(response.Header, false)
	if err != nil {
		return nil, fmt.Errorf("etcd get response admission: %w", err)
	}
	if response.Count < 0 || response.Count != int64(len(response.Kvs)) || response.Count > 1 || response.More {
		return nil, errors.New("etcd get returned an invalid point-range envelope")
	}
	if len(response.Kvs) == 0 {
		return nil, nil
	}
	kv := response.Kvs[0]
	if kv == nil || string(kv.Key) != key || kv.CreateRevision <= 0 || kv.ModRevision < kv.CreateRevision || kv.ModRevision > revision || kv.Version <= 0 || kv.Version > kv.ModRevision-kv.CreateRevision+1 {
		return nil, errors.New("etcd get returned an invalid key-value record")
	}
	return kv, nil
}
