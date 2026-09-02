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

// AuthorizedWatchProxyMetadataKey marks an internal follower-to-leader Watch
// generation. The ingress replica has already authenticated and authorized the
// logical Watch; a successor must not reinterpret a resume after an auth-store
// revision change as a brand-new client authorization. The receiver accepts
// this marker only on its peer listener.
const AuthorizedWatchProxyMetadataKey = "kubebrain-authorized-watch-proxy"

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

	// RangeStream forwards a server-streaming range request to leader.
	RangeStream(ctx context.Context, req *etcdserverpb.RangeRequest) (<-chan RangeStreamResult, error)

	// MemberList forwards a cluster membership lookup to leader.
	MemberList(ctx context.Context, req *etcdserverpb.MemberListRequest) (*etcdserverpb.MemberListResponse, error)

	// Put forwards put unary request to leader.
	Put(ctx context.Context, req *etcdserverpb.PutRequest) (*etcdserverpb.PutResponse, error)

	// DeleteRange forwards delete range unary request to leader.
	DeleteRange(ctx context.Context, req *etcdserverpb.DeleteRangeRequest) (*etcdserverpb.DeleteRangeResponse, error)

	// Compact forwards compaction request to leader.
	Compact(ctx context.Context, req *etcdserverpb.CompactionRequest) (*etcdserverpb.CompactionResponse, error)

	// Alarm forwards maintenance alarm requests to leader.
	Alarm(ctx context.Context, req *etcdserverpb.AlarmRequest) (*etcdserverpb.AlarmResponse, error)

	// Defragment forwards a maintenance defragment request to leader.
	Defragment(ctx context.Context, req *etcdserverpb.DefragmentRequest) (*etcdserverpb.DefragmentResponse, error)

	// Status forwards a maintenance status request to leader.
	Status(ctx context.Context, req *etcdserverpb.StatusRequest) (*etcdserverpb.StatusResponse, error)

	// Hash forwards a maintenance backend hash request to leader.
	Hash(ctx context.Context, req *etcdserverpb.HashRequest) (*etcdserverpb.HashResponse, error)

	// HashKV forwards a revision-bounded MVCC hash request to leader.
	HashKV(ctx context.Context, req *etcdserverpb.HashKVRequest) (*etcdserverpb.HashKVResponse, error)

	// Downgrade forwards a cluster downgrade request to leader.
	Downgrade(ctx context.Context, req *etcdserverpb.DowngradeRequest) (*etcdserverpb.DowngradeResponse, error)

	// AuthStatus forwards an authentication status request to leader.
	AuthStatus(ctx context.Context, req *etcdserverpb.AuthStatusRequest) (*etcdserverpb.AuthStatusResponse, error)

	// Authenticate forwards a username/password authentication request to leader.
	Authenticate(ctx context.Context, req *etcdserverpb.AuthenticateRequest) (*etcdserverpb.AuthenticateResponse, error)
	AuthEnable(ctx context.Context, req *etcdserverpb.AuthEnableRequest) (*etcdserverpb.AuthEnableResponse, error)
	AuthDisable(ctx context.Context, req *etcdserverpb.AuthDisableRequest) (*etcdserverpb.AuthDisableResponse, error)
	UserAdd(ctx context.Context, req *etcdserverpb.AuthUserAddRequest) (*etcdserverpb.AuthUserAddResponse, error)
	UserDelete(ctx context.Context, req *etcdserverpb.AuthUserDeleteRequest) (*etcdserverpb.AuthUserDeleteResponse, error)
	UserChangePassword(ctx context.Context, req *etcdserverpb.AuthUserChangePasswordRequest) (*etcdserverpb.AuthUserChangePasswordResponse, error)
	UserGrantRole(ctx context.Context, req *etcdserverpb.AuthUserGrantRoleRequest) (*etcdserverpb.AuthUserGrantRoleResponse, error)
	UserRevokeRole(ctx context.Context, req *etcdserverpb.AuthUserRevokeRoleRequest) (*etcdserverpb.AuthUserRevokeRoleResponse, error)
	RoleAdd(ctx context.Context, req *etcdserverpb.AuthRoleAddRequest) (*etcdserverpb.AuthRoleAddResponse, error)
	RoleDelete(ctx context.Context, req *etcdserverpb.AuthRoleDeleteRequest) (*etcdserverpb.AuthRoleDeleteResponse, error)
	RoleGrantPermission(ctx context.Context, req *etcdserverpb.AuthRoleGrantPermissionRequest) (*etcdserverpb.AuthRoleGrantPermissionResponse, error)
	RoleRevokePermission(ctx context.Context, req *etcdserverpb.AuthRoleRevokePermissionRequest) (*etcdserverpb.AuthRoleRevokePermissionResponse, error)

	// UserGet forwards an authentication user lookup to leader.
	UserGet(ctx context.Context, req *etcdserverpb.AuthUserGetRequest) (*etcdserverpb.AuthUserGetResponse, error)

	// UserList forwards an authentication user list request to leader.
	UserList(ctx context.Context, req *etcdserverpb.AuthUserListRequest) (*etcdserverpb.AuthUserListResponse, error)

	// RoleGet forwards an authentication role lookup to leader.
	RoleGet(ctx context.Context, req *etcdserverpb.AuthRoleGetRequest) (*etcdserverpb.AuthRoleGetResponse, error)

	// RoleList forwards an authentication role list request to leader.
	RoleList(ctx context.Context, req *etcdserverpb.AuthRoleListRequest) (*etcdserverpb.AuthRoleListResponse, error)

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

	// Snapshot forwards a maintenance snapshot stream to the current leader.
	Snapshot(ctx context.Context, req *etcdserverpb.SnapshotRequest) (<-chan SnapshotResult, error)
}

type SnapshotResult struct {
	Response *etcdserverpb.SnapshotResponse
	Err      error
}

type RangeStreamResult struct {
	Response *etcdserverpb.RangeStreamResponse
	Err      error
}

// WatchResult is exactly one of: a successful create acknowledgement (Created
// set, Revision is the leader's response revision), an event batch (Events set,
// Revision is the store revision covered by the batch), an authoritative error
// (Err set, Revision is the leader's response revision, which etcd may leave
// zero), or a progress notification (ProgressRevision > 0) — never a mix. An
// error has a positive CompactRevision exactly when it reports compaction; that
// watermark is at least the requested revision and at most a nonzero response
// revision. Revision is
// independent of visible events because server-side watch filters may remove
// some or all events while the watch still advances through the batch. Header
// preserves the serving leader's response identity for the ingress replica to
// validate before it publishes or advances any client-visible watermark.
type WatchResult struct {
	Header           *etcdserverpb.ResponseHeader
	Events           []*mvccpb.Event
	Err              error
	Created          bool
	Revision         uint64
	ProgressRevision uint64
	CompactRevision  int64
}
