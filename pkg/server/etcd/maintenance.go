// Copyright 2026 ByteDance and/or its affiliates
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

package etcd

import (
	"context"
	"errors"
	"hash/crc32"

	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/kubewharf/kubebrain/pkg/backend"
	"github.com/kubewharf/kubebrain/pkg/backend/election"
	"github.com/kubewharf/kubebrain/pkg/metrics"
)

// Version is the single source of truth for the etcd version KubeBrain claims,
// reported both by the Maintenance.Status gRPC (Status.Version, read by the
// kube-apiserver) and by the HTTP /version endpoint (read by kubeadm's
// ExternalEtcdVersion preflight and other etcd tooling).
//
// It must be semver-parseable (a non-semver string like "kubebrain" fails
// version.ParseSemantic and permanently disables the version-gated features)
// and satisfy the apiserver's RequestWatchProgress gate — >= 3.5.13 (or in
// [3.4.31, 3.5.0)) — which backs consistent-list-from-cache / WatchList.
//
// We advertise 3.7.0. Two things make a version *above* the 3.5.13 floor safe:
//   - RequestWatchProgress only has a lower bound, so 3.7.0 keeps it enabled;
//     KubeBrain's watch pipeline delivers events without silent gaps and
//     progress notifications never run ahead of delivered events.
//   - RangeStream (the streaming-list RPC introduced in etcd 3.7) is now
//     implemented (kv.go RangeStream, backed by the partition-parallel scanner):
//     the apiserver's EtcdRangeStream feature gate (k8s 1.37) streams large
//     initial LISTs through it, bounding watch-cache init memory. Advertising
//     3.7.0 matches the RPC we serve. Callers on older apiservers that never call
//     RangeStream are unaffected.
//
// See k8s.io/apiserver/pkg/storage/feature/feature_support_checker.go and
// k8s.io/apiserver/pkg/storage/etcd3/watcher.go (sync()).
const Version = "3.7.0"

// ClusterVersion is etcd's major.minor protocol version label. Upstream keeps
// this separate from the full server binary version so rolling-upgrade alerts
// can distinguish binary patch skew from a cluster protocol transition.
const ClusterVersion = "3.7"

func emitVersionMetrics(metricCli metrics.Metrics) {
	_ = metricCli.EmitGauge(
		"etcd.server.version", 1, metrics.Tag("server_version", Version),
	)
	_ = metricCli.EmitGauge(
		"etcd.cluster.version", 1, metrics.Tag("cluster_version", ClusterVersion),
	)
}

// etcd substitutes this value when --quota-backend-bytes is unset. KubeBrain's
// actual capacity belongs to TiKV/PD, but Status must still return a nonzero
// protocol-compatible value for etcdctl and other 3.6+ clients.
const defaultEtcdBackendQuota int64 = 2 * 1024 * 1024 * 1024

func (s *RPCServer) Alarm(ctx context.Context, req *etcdserverpb.AlarmRequest) (*etcdserverpb.AlarmResponse, error) {
	s.metricCli.EmitCounter("maintenance.alarm", 1)
	if req.GetAction() == etcdserverpb.AlarmRequest_GET {
		if err := s.requireAuthenticated(ctx, false); err != nil {
			return nil, err
		}
		if err := s.peers.SyncReadRevision(ctx); err != nil {
			return nil, readBarrierStatusErr(err)
		}
	} else {
		if err := s.requireAuthenticated(ctx, true); err != nil {
			return nil, err
		}
		return nil, status.Error(codes.Unimplemented, alarmMutationUnsupportedMessage)
	}
	return &etcdserverpb.AlarmResponse{
		Header: s.maintenanceHeader(),
		Alarms: nil,
	}, nil
}

func (s *RPCServer) Status(ctx context.Context, _ *etcdserverpb.StatusRequest) (*etcdserverpb.StatusResponse, error) {
	s.metricCli.EmitCounter("maintenance.status", 1)
	if err := s.requireAuthenticated(ctx, false); err != nil {
		return nil, err
	}
	revision := s.backend.GetCurrentRevision()
	leader := s.memberIDFromAddress(s.peers.GetLeaderInfo())
	term, err := s.responseRaftTerm(ctx)
	if err != nil {
		return nil, err
	}
	resp := &etcdserverpb.StatusResponse{
		Header:           s.maintenanceHeader(),
		Version:          Version,
		StorageVersion:   Version,
		Leader:           leader,
		RaftIndex:        revision,
		RaftAppliedIndex: revision,
		RaftTerm:         term,
		// DbSize uses a 1-byte compatibility sentinel: it exists in etcd to warn before the hard
		// --quota-backend-bytes NOSPACE cliff (and to drive defrag). The TiKV
		// backend has no per-logical-DB quota (it scales horizontally), so that
		// semantics does not apply and a synthesized number would only invite
		// etcd-style false quota alarms. Zero is not usable either: etcdctl 3.7's
		// endpoint-status table divides DbSizeInUse by DbSize and panics on zero.
		// Equal 1-byte sentinels report 0% fragmentation without pretending to
		// measure TiKV capacity. Real capacity is observed out of band:
		// TiKV/PD's own metrics (store disk, region count) for bytes, and
		// KubeBrain's count_index.keys gauge for object count. See
		// docs/observability_cn.md.
		DbSize:        1,
		DbSizeInUse:   1,
		DbSizeQuota:   defaultEtcdBackendQuota,
		Errors:        nil,
		IsLearner:     false,
		DowngradeInfo: &etcdserverpb.DowngradeInfo{Enabled: false},
	}
	if leader == 0 {
		resp.Errors = append(resp.Errors, rpctypes.ErrNoLeader.Error())
	}
	return resp, nil
}

func (s *RPCServer) Defragment(ctx context.Context, _ *etcdserverpb.DefragmentRequest) (*etcdserverpb.DefragmentResponse, error) {
	s.metricCli.EmitCounter("maintenance.defragment", 1)
	if err := s.requireAuthenticated(ctx, true); err != nil {
		return nil, err
	}
	return &etcdserverpb.DefragmentResponse{}, nil
}

func (s *RPCServer) Hash(ctx context.Context, _ *etcdserverpb.HashRequest) (*etcdserverpb.HashResponse, error) {
	s.metricCli.EmitCounter("maintenance.hash", 1)
	if err := s.requireAuthenticated(ctx, true); err != nil {
		return nil, err
	}
	// KubeBrain's committed revision is cached per replica even though the MVCC
	// data is shared in TiKV. Refresh it when possible, but preserve etcd's
	// member-local diagnostic behavior when the leader is unavailable.
	_ = s.peers.SyncReadRevision(ctx)
	hashResult, err := s.backend.HashKV(ctx, 0)
	if err != nil {
		return nil, err
	}
	return &etcdserverpb.HashResponse{
		Header: txnHeader(hashResult.CurrentRevision),
		Hash:   hashResult.Hash,
	}, nil
}

func (s *RPCServer) HashKV(ctx context.Context, req *etcdserverpb.HashKVRequest) (*etcdserverpb.HashKVResponse, error) {
	s.metricCli.EmitCounter("maintenance.hashkv", 1)
	if err := s.requireAuthenticated(ctx, true); err != nil {
		return nil, err
	}
	// A successful refresh pins normal-operation hashes to the latest committed
	// revision. A failed refresh must not make this local diagnostic unavailable.
	_ = s.peers.SyncReadRevision(ctx)
	revision := req.GetRevision()
	if req.GetRevision() > 0 {
		// Use the request context so a cancelled/expired HashKV call aborts the
		// revision and compaction lookups instead of running under a detached
		// context.Background() (#59).
		if err := s.checkRequestedRevision(ctx, req.GetRevision()); err != nil {
			return nil, err
		}
	}
	hashResult, err := s.backend.HashKV(ctx, revision)
	if err != nil {
		if errors.Is(err, backend.ErrHashKVCompacted) {
			return nil, compactedRevisionError()
		}
		if errors.Is(err, backend.ErrHashKVFuture) {
			return nil, futureRevisionError()
		}
		return nil, err
	}
	return &etcdserverpb.HashKVResponse{
		Header:          txnHeader(hashResult.CurrentRevision),
		Hash:            hashResult.Hash,
		CompactRevision: hashResult.CompactRevision,
		HashRevision:    hashResult.HashRevision,
	}, nil
}

func (s *RPCServer) Snapshot(_ *etcdserverpb.SnapshotRequest, stream etcdserverpb.Maintenance_SnapshotServer) error {
	s.metricCli.EmitCounter("maintenance.snapshot", 1)
	if err := s.requireAuthenticated(stream.Context(), true); err != nil {
		return err
	}
	return status.Error(codes.Unimplemented, snapshotUnsupportedMessage)
}

func (s *RPCServer) MoveLeader(ctx context.Context, _ *etcdserverpb.MoveLeaderRequest) (*etcdserverpb.MoveLeaderResponse, error) {
	s.metricCli.EmitCounter("maintenance.moveleader", 1)
	if err := s.requireAuthenticated(ctx, true); err != nil {
		return nil, err
	}
	return nil, status.Error(codes.Unimplemented, moveLeaderUnsupportedMessage)
}

func (s *RPCServer) Downgrade(ctx context.Context, _ *etcdserverpb.DowngradeRequest) (*etcdserverpb.DowngradeResponse, error) {
	s.metricCli.EmitCounter("maintenance.downgrade", 1)
	if err := s.requireAuthenticated(ctx, true); err != nil {
		return nil, err
	}
	return nil, status.Error(codes.Unimplemented, downgradeUnsupportedMessage)
}

func (s *RPCServer) requireAuthenticated(ctx context.Context, root bool) error {
	caller, err := s.authCallerFromContext(ctx)
	if err != nil {
		return err
	}
	if root && !caller.isRoot() {
		return rpctypes.ErrPermissionDenied
	}
	return nil
}

func (s *RPCServer) maintenanceHeader() *etcdserverpb.ResponseHeader {
	// ClusterId/MemberId are stamped on EVERY response header by the
	// ClientServerOptions interceptor (#79), so they need not be set here;
	// Revision is method-specific. (MemberId there uses the same local-identity
	// derivation as StatusResponse.Leader, so "am I the leader" comparisons —
	// Leader == MemberId — behave like etcd's.)
	return &etcdserverpb.ResponseHeader{
		Revision: int64(s.backend.GetCurrentRevision()),
	}
}

func (s *RPCServer) memberIDFromAddress(address string) uint64 {
	if !election.IsLeaderKnown(address) {
		return 0
	}
	return uint64(crc32.ChecksumIEEE([]byte(address)))
}
