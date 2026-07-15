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
	"encoding/binary"
	"hash/crc32"

	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/kubewharf/kubebrain/pkg/backend/election"
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

func (s *RPCServer) Alarm(context.Context, *etcdserverpb.AlarmRequest) (*etcdserverpb.AlarmResponse, error) {
	s.metricCli.EmitCounter("maintenance.alarm", 1)
	return &etcdserverpb.AlarmResponse{
		Header: s.maintenanceHeader(),
		Alarms: nil,
	}, nil
}

func (s *RPCServer) Status(ctx context.Context, _ *etcdserverpb.StatusRequest) (*etcdserverpb.StatusResponse, error) {
	s.metricCli.EmitCounter("maintenance.status", 1)
	if err := s.peers.SyncReadRevision(ctx); err != nil {
		return nil, err
	}
	revision := s.backend.GetCurrentRevision()
	return &etcdserverpb.StatusResponse{
		Header:           s.maintenanceHeader(),
		Version:          Version,
		Leader:           s.memberIDFromAddress(s.peers.GetLeaderInfo()),
		RaftIndex:        revision,
		RaftAppliedIndex: revision,
		// DbSize is intentionally 0: it exists in etcd to warn before the hard
		// --quota-backend-bytes NOSPACE cliff (and to drive defrag). The TiKV
		// backend has no per-logical-DB quota (it scales horizontally), so that
		// semantics does not apply and a synthesized number would only invite
		// etcd-style false quota alarms. Real capacity is observed out of band:
		// TiKV/PD's own metrics (store disk, region count) for bytes, and
		// KubeBrain's count_index.keys gauge for object count. See
		// docs/observability_cn.md.
		DbSize:      0,
		DbSizeInUse: 0,
		Errors:      nil,
		IsLearner:   false,
	}, nil
}

func (s *RPCServer) Defragment(context.Context, *etcdserverpb.DefragmentRequest) (*etcdserverpb.DefragmentResponse, error) {
	s.metricCli.EmitCounter("maintenance.defragment", 1)
	return &etcdserverpb.DefragmentResponse{
		Header: s.maintenanceHeader(),
	}, nil
}

func (s *RPCServer) Hash(ctx context.Context, _ *etcdserverpb.HashRequest) (*etcdserverpb.HashResponse, error) {
	s.metricCli.EmitCounter("maintenance.hash", 1)
	if err := s.peers.SyncReadRevision(ctx); err != nil {
		return nil, err
	}
	revision := s.backend.GetCurrentRevision()
	return &etcdserverpb.HashResponse{
		Header: s.maintenanceHeader(),
		Hash:   revisionHash(revision),
	}, nil
}

func (s *RPCServer) HashKV(ctx context.Context, req *etcdserverpb.HashKVRequest) (*etcdserverpb.HashKVResponse, error) {
	s.metricCli.EmitCounter("maintenance.hashkv", 1)
	if err := s.peers.SyncReadRevision(ctx); err != nil {
		return nil, err
	}
	revision := s.backend.GetCurrentRevision()
	if req.GetRevision() > 0 {
		// Use the request context so a cancelled/expired HashKV call aborts the
		// revision and compaction lookups instead of running under a detached
		// context.Background() (#59).
		if err := s.checkRequestedRevision(ctx, req.GetRevision()); err != nil {
			return nil, err
		}
		revision = uint64(req.GetRevision())
	}
	compactRevision, err := s.backend.GetCompactRevision(ctx)
	if err != nil {
		return nil, err
	}
	return &etcdserverpb.HashKVResponse{
		Header:          s.maintenanceHeader(),
		Hash:            revisionHash(revision),
		CompactRevision: int64(compactRevision),
	}, nil
}

func (s *RPCServer) Snapshot(*etcdserverpb.SnapshotRequest, etcdserverpb.Maintenance_SnapshotServer) error {
	s.metricCli.EmitCounter("maintenance.snapshot", 1)
	return status.Errorf(codes.Unimplemented, "snapshot is not supported")
}

func (s *RPCServer) MoveLeader(context.Context, *etcdserverpb.MoveLeaderRequest) (*etcdserverpb.MoveLeaderResponse, error) {
	s.metricCli.EmitCounter("maintenance.moveleader", 1)
	return nil, status.Errorf(codes.Unimplemented, "move leader is not supported")
}

func (s *RPCServer) Downgrade(context.Context, *etcdserverpb.DowngradeRequest) (*etcdserverpb.DowngradeResponse, error) {
	s.metricCli.EmitCounter("maintenance.downgrade", 1)
	return nil, status.Errorf(codes.Unimplemented, "downgrade is not supported")
}

func (s *RPCServer) maintenanceHeader() *etcdserverpb.ResponseHeader {
	// ClusterId/MemberId are stamped on EVERY response header by the
	// HeaderStampServerOptions interceptor (#79), so they need not be set here;
	// Revision is method-specific. (MemberId there uses the same local-identity
	// derivation as StatusResponse.Leader, so "am I the leader" comparisons —
	// Leader == MemberId — behave like etcd's.)
	return &etcdserverpb.ResponseHeader{
		Revision: int64(s.backend.GetCurrentRevision()),
	}
}

func (s *RPCServer) memberIDFromAddress(address string) uint64 {
	if !election.IsLeaderKnown(address) {
		address = s.backend.GetResourceLock().Identity()
	}
	return uint64(crc32.ChecksumIEEE([]byte(address)))
}

func revisionHash(revision uint64) uint32 {
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], revision)
	return crc32.ChecksumIEEE(buf[:])
}
