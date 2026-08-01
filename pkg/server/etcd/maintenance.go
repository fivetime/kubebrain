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
	switch req.GetAction() {
	case etcdserverpb.AlarmRequest_GET:
		if err := s.requireAuthenticated(ctx, false); err != nil {
			return nil, err
		}
		if err := s.peers.SyncReadRevision(ctx); err != nil {
			return nil, readBarrierStatusErr(err)
		}
	case etcdserverpb.AlarmRequest_DEACTIVATE:
		if err := s.requireAuthenticated(ctx, true); err != nil {
			return nil, err
		}
		response := &etcdserverpb.AlarmResponse{Header: s.maintenanceHeader()}
		if req.GetAlarm() == etcdserverpb.AlarmType_NONE {
			return response, nil
		}
		if req.GetAlarm() == etcdserverpb.AlarmType_CORRUPT {
			removed, err := s.backend.DisarmCorrupt(ctx, req.GetMemberID())
			if err != nil {
				return nil, mapFenceErr(err)
			}
			if removed {
				response.Alarms = []*etcdserverpb.AlarmMember{{
					MemberID: req.GetMemberID(), Alarm: etcdserverpb.AlarmType_CORRUPT,
				}}
			}
			return response, nil
		}
		if req.GetAlarm() != etcdserverpb.AlarmType_NOSPACE {
			removed, err := s.mutateGenericAlarm(ctx, req.GetAlarm(), req.GetMemberID(), false)
			if err != nil {
				return nil, mapFenceErr(err)
			}
			if removed {
				response.Alarms = []*etcdserverpb.AlarmMember{{
					MemberID: req.GetMemberID(), Alarm: req.GetAlarm(),
				}}
			}
			return response, nil
		}
		removed, err := s.backend.DisarmNoSpace(ctx, req.GetMemberID())
		if err != nil {
			return nil, mapFenceErr(err)
		}
		if removed {
			response.Alarms = []*etcdserverpb.AlarmMember{{
				MemberID: req.GetMemberID(),
				Alarm:    etcdserverpb.AlarmType_NOSPACE,
			}}
		}
		return response, nil
	case etcdserverpb.AlarmRequest_ACTIVATE:
		if err := s.requireAuthenticated(ctx, true); err != nil {
			return nil, err
		}
		response := &etcdserverpb.AlarmResponse{Header: s.maintenanceHeader()}
		if req.GetAlarm() == etcdserverpb.AlarmType_NONE {
			return response, nil
		}
		if req.GetAlarm() == etcdserverpb.AlarmType_CORRUPT {
			if err := s.backend.ArmCorrupt(ctx, req.GetMemberID()); err != nil {
				return nil, mapFenceErr(err)
			}
			response.Alarms = []*etcdserverpb.AlarmMember{{
				MemberID: req.GetMemberID(), Alarm: etcdserverpb.AlarmType_CORRUPT,
			}}
			return response, nil
		}
		if req.GetAlarm() != etcdserverpb.AlarmType_NOSPACE {
			if _, err := s.mutateGenericAlarm(ctx, req.GetAlarm(), req.GetMemberID(), true); err != nil {
				return nil, mapFenceErr(err)
			}
			response.Alarms = []*etcdserverpb.AlarmMember{{
				MemberID: req.GetMemberID(), Alarm: req.GetAlarm(),
			}}
			return response, nil
		}
		memberID, err := s.backend.ArmNoSpace(ctx, req.GetMemberID())
		if err != nil {
			return nil, mapFenceErr(err)
		}
		response.Alarms = []*etcdserverpb.AlarmMember{{
			MemberID: memberID,
			Alarm:    etcdserverpb.AlarmType_NOSPACE,
		}}
		return response, nil
	default:
		if err := s.requireAuthenticated(ctx, true); err != nil {
			return nil, err
		}
		return nil, status.Error(codes.Unimplemented, alarmMutationUnsupportedMessage)
	}
	response := &etcdserverpb.AlarmResponse{Header: s.maintenanceHeader()}
	filter := req.GetAlarm()
	if filter != etcdserverpb.AlarmType_NONE &&
		filter != etcdserverpb.AlarmType_NOSPACE &&
		filter != etcdserverpb.AlarmType_CORRUPT {
		alarms, err := s.genericAlarms(ctx, filter)
		if err != nil {
			return nil, err
		}
		response.Alarms = append(response.Alarms, alarms...)
		return response, nil
	}
	if filter == etcdserverpb.AlarmType_NONE || filter == etcdserverpb.AlarmType_NOSPACE {
		_, _, noSpace, err := s.backend.QuotaStatus(ctx)
		if err != nil {
			return nil, err
		}
		if noSpace {
			memberIDs, alarmErr := s.backend.NoSpaceAlarms(ctx)
			if alarmErr != nil {
				return nil, alarmErr
			}
			for _, memberID := range memberIDs {
				response.Alarms = append(response.Alarms, &etcdserverpb.AlarmMember{
					MemberID: memberID,
					Alarm:    etcdserverpb.AlarmType_NOSPACE,
				})
			}
		}
	}
	if filter == etcdserverpb.AlarmType_NONE || filter == etcdserverpb.AlarmType_CORRUPT {
		memberIDs, alarmErr := s.backend.CorruptAlarms(ctx)
		if alarmErr != nil {
			return nil, alarmErr
		}
		for _, memberID := range memberIDs {
			response.Alarms = append(response.Alarms, &etcdserverpb.AlarmMember{
				MemberID: memberID, Alarm: etcdserverpb.AlarmType_CORRUPT,
			})
		}
	}
	if filter == etcdserverpb.AlarmType_NONE {
		alarms, alarmErr := s.genericAlarms(ctx, filter)
		if alarmErr != nil {
			return nil, alarmErr
		}
		response.Alarms = append(response.Alarms, alarms...)
	}
	return response, nil
}

func (s *RPCServer) Status(ctx context.Context, _ *etcdserverpb.StatusRequest) (*etcdserverpb.StatusResponse, error) {
	s.metricCli.EmitCounter("maintenance.status", 1)
	if err := s.requireAuthenticated(ctx, false); err != nil {
		return nil, err
	}
	revision := s.backend.GetCurrentRevision()
	usage, quota, noSpace, quotaErr := s.backend.QuotaStatus(ctx)
	if quotaErr != nil {
		return nil, quotaErr
	}
	var noSpaceAlarms []*etcdserverpb.AlarmMember
	if noSpace {
		memberIDs, alarmErr := s.backend.NoSpaceAlarms(ctx)
		if alarmErr != nil {
			return nil, alarmErr
		}
		for _, memberID := range memberIDs {
			noSpaceAlarms = append(noSpaceAlarms, &etcdserverpb.AlarmMember{
				MemberID: memberID,
				Alarm:    etcdserverpb.AlarmType_NOSPACE,
			})
		}
	}
	dbSize := usage
	if dbSize == 0 {
		dbSize = 1
	}
	if quota == 0 {
		quota = defaultEtcdBackendQuota
	}
	leader := s.memberIDForPeerIdentity(s.peers.GetLeaderInfo())
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
		// With a configured quota these fields report the tenant's latest logical
		// key+value bytes. Without one they retain the nonzero sentinel required
		// by etcdctl's fragmentation calculation; TiKV physical capacity remains
		// observable through PD/TiKV metrics.
		DbSize:        dbSize,
		DbSizeInUse:   dbSize,
		DbSizeQuota:   quota,
		Errors:        nil,
		IsLearner:     false,
		DowngradeInfo: &etcdserverpb.DowngradeInfo{Enabled: false},
	}
	if leader == 0 {
		resp.Errors = append(resp.Errors, rpctypes.ErrNoLeader.Error())
	}
	for _, alarm := range noSpaceAlarms {
		resp.Errors = append(resp.Errors, alarm.String())
	}
	corruptAlarms, err := s.backend.CorruptAlarms(ctx)
	if err != nil {
		return nil, err
	}
	for _, memberID := range corruptAlarms {
		resp.Errors = append(resp.Errors, (&etcdserverpb.AlarmMember{
			MemberID: memberID, Alarm: etcdserverpb.AlarmType_CORRUPT,
		}).String())
	}
	genericAlarms, err := s.genericAlarms(ctx, etcdserverpb.AlarmType_NONE)
	if err != nil {
		return nil, err
	}
	for _, alarm := range genericAlarms {
		resp.Errors = append(resp.Errors, alarm.String())
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
	if root && caller != nil {
		return caller.adminError()
	}
	return nil
}

func (s *RPCServer) rejectCorrupt(ctx context.Context) error {
	alarms, err := s.backend.CorruptAlarms(ctx)
	if err != nil {
		return mapFenceErr(err)
	}
	if len(alarms) != 0 {
		return rpctypes.ErrGRPCCorrupt
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
