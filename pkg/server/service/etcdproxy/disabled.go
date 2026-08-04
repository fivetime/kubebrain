// Copyright 2022 ByteDance and/or its affiliates
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package etcdproxy

import (
	"context"

	"github.com/pkg/errors"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
)

type disabledEtcdProxy struct{}

func NewDisabledEtcdProxy() EtcdProxy {
	return disabledEtcdProxy{}
}

var errDisabled = errors.New("etcd proxy is disabled")

func (d disabledEtcdProxy) EtcdProxyEnabled() bool {
	return false
}

func (d disabledEtcdProxy) Ready() error {
	return errDisabled
}

func (d disabledEtcdProxy) Txn(ctx context.Context, txn *etcdserverpb.TxnRequest) (*etcdserverpb.TxnResponse, error) {
	return nil, errDisabled
}

func (d disabledEtcdProxy) Range(ctx context.Context, req *etcdserverpb.RangeRequest) (*etcdserverpb.RangeResponse, error) {
	return nil, errDisabled
}

func (d disabledEtcdProxy) Put(ctx context.Context, req *etcdserverpb.PutRequest) (*etcdserverpb.PutResponse, error) {
	return nil, errDisabled
}

func (d disabledEtcdProxy) DeleteRange(ctx context.Context, req *etcdserverpb.DeleteRangeRequest) (*etcdserverpb.DeleteRangeResponse, error) {
	return nil, errDisabled
}

func (d disabledEtcdProxy) Compact(ctx context.Context, req *etcdserverpb.CompactionRequest) (*etcdserverpb.CompactionResponse, error) {
	return nil, errDisabled
}

func (d disabledEtcdProxy) Watch(ctx context.Context, key, rangeEnd []byte, revision uint64) (<-chan WatchResult, error) {
	return nil, errDisabled
}

func (d disabledEtcdProxy) LeaseGrant(ctx context.Context, req *etcdserverpb.LeaseGrantRequest) (*etcdserverpb.LeaseGrantResponse, error) {
	return nil, errDisabled
}

func (d disabledEtcdProxy) LeaseRevoke(ctx context.Context, req *etcdserverpb.LeaseRevokeRequest) (*etcdserverpb.LeaseRevokeResponse, error) {
	return nil, errDisabled
}

func (d disabledEtcdProxy) LeaseKeepAlive(ctx context.Context, req *etcdserverpb.LeaseKeepAliveRequest) (*etcdserverpb.LeaseKeepAliveResponse, error) {
	return nil, errDisabled
}

func (d disabledEtcdProxy) LeaseTimeToLive(ctx context.Context, req *etcdserverpb.LeaseTimeToLiveRequest) (*etcdserverpb.LeaseTimeToLiveResponse, error) {
	return nil, errDisabled
}

func (d disabledEtcdProxy) LeaseLeases(ctx context.Context, req *etcdserverpb.LeaseLeasesRequest) (*etcdserverpb.LeaseLeasesResponse, error) {
	return nil, errDisabled
}

func (d disabledEtcdProxy) Snapshot(context.Context, *etcdserverpb.SnapshotRequest) (<-chan SnapshotResult, error) {
	return nil, errDisabled
}
