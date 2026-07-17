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

	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/mvccpb"
)

// EtcdProxy forward etcd-style rpc request to leader for compatibility of k8s apiserver community edition
type EtcdProxy interface {
	// EtcdProxyEnabled returns if the etcd proxy is enabled
	EtcdProxyEnabled() bool

	// Ready returns nil when the proxy can forward requests to the current leader.
	Ready() error

	// Txn forward txn unary request to leader
	Txn(ctx context.Context, txn *etcdserverpb.TxnRequest) (*etcdserverpb.TxnResponse, error)

	// Range forwards historical range unary request to leader.
	Range(ctx context.Context, req *etcdserverpb.RangeRequest) (*etcdserverpb.RangeResponse, error)

	// Put forwards put unary request to leader.
	Put(ctx context.Context, req *etcdserverpb.PutRequest) (*etcdserverpb.PutResponse, error)

	// DeleteRange forwards delete range unary request to leader.
	DeleteRange(ctx context.Context, req *etcdserverpb.DeleteRangeRequest) (*etcdserverpb.DeleteRangeResponse, error)

	// Compact forwards compaction request to leader.
	Compact(ctx context.Context, req *etcdserverpb.CompactionRequest) (*etcdserverpb.CompactionResponse, error)

	// Watch forward watch stream request to leader.
	Watch(ctx context.Context, key, rangeEnd []byte, revision uint64) (<-chan WatchResult, error)

	// LeaseGrant forwards lease grant request to leader.
	LeaseGrant(ctx context.Context, req *etcdserverpb.LeaseGrantRequest) (*etcdserverpb.LeaseGrantResponse, error)

	// LeaseRevoke forwards lease revoke request to leader.
	LeaseRevoke(ctx context.Context, req *etcdserverpb.LeaseRevokeRequest) (*etcdserverpb.LeaseRevokeResponse, error)

	// LeaseKeepAlive forwards one lease keepalive message to leader.
	LeaseKeepAlive(ctx context.Context, req *etcdserverpb.LeaseKeepAliveRequest) (*etcdserverpb.LeaseKeepAliveResponse, error)

	// LeaseTimeToLive forwards lease ttl request to leader.
	LeaseTimeToLive(ctx context.Context, req *etcdserverpb.LeaseTimeToLiveRequest) (*etcdserverpb.LeaseTimeToLiveResponse, error)

	// LeaseLeases forwards lease list request to leader.
	LeaseLeases(ctx context.Context, req *etcdserverpb.LeaseLeasesRequest) (*etcdserverpb.LeaseLeasesResponse, error)
}

// WatchResult is exactly one of: an event batch (Events set, Revision is the
// store revision covered by the batch), an error (Err set), or a progress
// notification (ProgressRevision > 0) — never a mix. Revision is independent of
// visible events because server-side watch filters may remove some or all events
// while the watch still advances through the batch.
type WatchResult struct {
	Events           []*mvccpb.Event
	Err              error
	Revision         uint64
	ProgressRevision uint64
}
