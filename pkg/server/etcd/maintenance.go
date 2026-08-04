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
	"fmt"
	"hash/crc32"
	"strings"

	"github.com/coreos/go-semver/semver"
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
		header, err := s.maintenanceHeader(ctx)
		if err != nil {
			return nil, err
		}
		response := &etcdserverpb.AlarmResponse{Header: header}
		if req.GetAlarm() == etcdserverpb.AlarmType_NONE {
			return response, nil
		}
		if req.GetAlarm() == etcdserverpb.AlarmType_CORRUPT {
			removed, err := s.backend.DisarmCorrupt(ctx, req.GetMemberID())
			if err != nil {
				return nil, mapFenceErr(err)
			}
			if removed {
				alarm := &etcdserverpb.AlarmMember{
					MemberID: req.GetMemberID(), Alarm: etcdserverpb.AlarmType_CORRUPT,
				}
				response.Alarms = []*etcdserverpb.AlarmMember{alarm}
				s.recordAlarmDeactivated(alarm)
			}
			return response, nil
		}
		if req.GetAlarm() != etcdserverpb.AlarmType_NOSPACE {
			removed, err := s.mutateGenericAlarm(ctx, req.GetAlarm(), req.GetMemberID(), false)
			if err != nil {
				return nil, mapFenceErr(err)
			}
			if removed {
				alarm := &etcdserverpb.AlarmMember{
					MemberID: req.GetMemberID(), Alarm: req.GetAlarm(),
				}
				response.Alarms = []*etcdserverpb.AlarmMember{alarm}
				s.recordAlarmDeactivated(alarm)
			}
			return response, nil
		}
		removed, err := s.backend.DisarmNoSpace(ctx, req.GetMemberID())
		if err != nil {
			return nil, mapFenceErr(err)
		}
		if removed {
			alarm := &etcdserverpb.AlarmMember{
				MemberID: req.GetMemberID(),
				Alarm:    etcdserverpb.AlarmType_NOSPACE,
			}
			response.Alarms = []*etcdserverpb.AlarmMember{alarm}
			s.recordAlarmDeactivated(alarm)
		}
		return response, nil
	case etcdserverpb.AlarmRequest_ACTIVATE:
		if err := s.requireAuthenticated(ctx, true); err != nil {
			return nil, err
		}
		header, err := s.maintenanceHeader(ctx)
		if err != nil {
			return nil, err
		}
		response := &etcdserverpb.AlarmResponse{Header: header}
		if req.GetAlarm() == etcdserverpb.AlarmType_NONE {
			return response, nil
		}
		if req.GetAlarm() == etcdserverpb.AlarmType_CORRUPT {
			if err := s.backend.ArmCorrupt(ctx, req.GetMemberID()); err != nil {
				return nil, mapFenceErr(err)
			}
			alarm := &etcdserverpb.AlarmMember{
				MemberID: req.GetMemberID(), Alarm: etcdserverpb.AlarmType_CORRUPT,
			}
			response.Alarms = []*etcdserverpb.AlarmMember{alarm}
			s.recordAlarmActivated(alarm)
			return response, nil
		}
		if req.GetAlarm() != etcdserverpb.AlarmType_NOSPACE {
			if _, err := s.mutateGenericAlarm(ctx, req.GetAlarm(), req.GetMemberID(), true); err != nil {
				return nil, mapFenceErr(err)
			}
			alarm := &etcdserverpb.AlarmMember{
				MemberID: req.GetMemberID(), Alarm: req.GetAlarm(),
			}
			response.Alarms = []*etcdserverpb.AlarmMember{alarm}
			s.recordAlarmActivated(alarm)
			return response, nil
		}
		memberID, err := s.backend.ArmNoSpace(ctx, req.GetMemberID())
		if err != nil {
			return nil, mapFenceErr(err)
		}
		alarm := &etcdserverpb.AlarmMember{
			MemberID: memberID,
			Alarm:    etcdserverpb.AlarmType_NOSPACE,
		}
		response.Alarms = []*etcdserverpb.AlarmMember{alarm}
		s.recordAlarmActivated(alarm)
		return response, nil
	default:
		if err := s.requireAuthenticated(ctx, true); err != nil {
			return nil, err
		}
		// Upstream's applier currently returns a nil response for an unknown enum,
		// which the maintenance wrapper dereferences and crashes. Unknown protobuf
		// enum values are valid on the wire, so reject them explicitly instead of
		// classifying malformed input as a TiKV platform boundary.
		return nil, status.Error(codes.InvalidArgument, "etcdserver: invalid alarm action")
	}
	header, err := s.maintenanceHeader(ctx)
	if err != nil {
		return nil, err
	}
	response := &etcdserverpb.AlarmResponse{Header: header}
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
	// Status is intentionally member-local and does not require a leader read
	// barrier, but a newly started replica must restore its persisted user
	// revision before exposing the synthetic Raft indexes. Sampling the raw local
	// cache here used to capture zero; QuotaStatus recovered the cache later, so
	// the same response had a durable Header.Revision but zero Raft{,Applied}Index.
	revision, err := safeBackendRevision(ctx, s.backend)
	if err != nil {
		return nil, err
	}
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
		Header:           txnHeader(int64(revision)),
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
		IsLearner:     s.localMemberIsLearner(),
		DowngradeInfo: &etcdserverpb.DowngradeInfo{Enabled: false},
	}
	if leader == 0 {
		resp.Errors = append(resp.Errors, rpctypes.ErrNoLeader.Error())
	}
	for _, alarm := range noSpaceAlarms {
		resp.Errors = append(resp.Errors, alarmStatusError(alarm))
	}
	corruptAlarms, err := s.backend.CorruptAlarms(ctx)
	if err != nil {
		return nil, err
	}
	for _, memberID := range corruptAlarms {
		resp.Errors = append(resp.Errors, alarmStatusError(&etcdserverpb.AlarmMember{
			MemberID: memberID, Alarm: etcdserverpb.AlarmType_CORRUPT,
		}))
	}
	genericAlarms, err := s.genericAlarms(ctx, etcdserverpb.AlarmType_NONE)
	if err != nil {
		return nil, err
	}
	for _, alarm := range genericAlarms {
		resp.Errors = append(resp.Errors, alarmStatusError(alarm))
	}
	return resp, nil
}

func alarmStatusError(alarm *etcdserverpb.AlarmMember) string {
	fields := make([]string, 0, 2)
	if alarm.GetMemberID() != 0 {
		fields = append(fields, fmt.Sprintf("memberID:%d", alarm.GetMemberID()))
	}
	if alarm.GetAlarm() != etcdserverpb.AlarmType_NONE {
		fields = append(fields, fmt.Sprintf("alarm:%s", alarm.GetAlarm().String()))
	}
	return strings.Join(fields, "  ")
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

func (s *RPCServer) Snapshot(request *etcdserverpb.SnapshotRequest, stream etcdserverpb.Maintenance_SnapshotServer) error {
	s.metricCli.EmitCounter("maintenance.snapshot", 1)
	caller, err := s.authCallerFromContext(stream.Context())
	if err != nil {
		return err
	}
	if caller != nil {
		if err = caller.adminError(); err != nil {
			return err
		}
	}
	if !s.peers.IsLeader() && s.peers.EtcdProxyEnabled() {
		proxyCtx, err := s.forwardAuthToken(stream.Context(), caller)
		if err != nil {
			return err
		}
		responses, err := s.peers.Snapshot(proxyCtx, request)
		if err != nil {
			return err
		}
		for result := range responses {
			if result.Err != nil {
				return result.Err
			}
			if result.Response == nil {
				return fmt.Errorf("leader snapshot proxy returned an empty response")
			}
			if err = stream.Send(result.Response); err != nil {
				return err
			}
		}
		return nil
	}
	return s.sendSnapshot(stream)
}

func (s *RPCServer) MoveLeader(ctx context.Context, request *etcdserverpb.MoveLeaderRequest) (*etcdserverpb.MoveLeaderResponse, error) {
	s.metricCli.EmitCounter("maintenance.moveleader", 1)
	if err := s.requireAuthenticated(ctx, true); err != nil {
		return nil, err
	}
	localID := s.memberIDForPeerIdentity(s.backend.GetResourceLock().Identity())
	leaderID := s.memberIDForPeerIdentity(s.peers.GetLeaderInfo())
	// Match maintenanceServer.MoveLeader: the serving member must reject the
	// request before validating the transferee when it is not the current
	// leader. This is member-local state, so a load balancer must not turn a
	// request that landed on a follower into an idempotent success merely because
	// TargetID names the actual leader.
	if localID == 0 || localID != leaderID {
		return nil, rpctypes.ErrGRPCNotLeader
	}
	member := s.memberByID(request.GetTargetID())
	if member == nil || member.GetIsLearner() {
		return nil, rpctypes.ErrGRPCBadLeaderTransferee
	}
	if request.GetTargetID() == leaderID {
		return &etcdserverpb.MoveLeaderResponse{}, nil
	}
	return nil, moveLeaderPlatformManagedError()
}

func (s *RPCServer) Downgrade(ctx context.Context, request *etcdserverpb.DowngradeRequest) (*etcdserverpb.DowngradeResponse, error) {
	s.metricCli.EmitCounter("maintenance.downgrade", 1)
	if err := s.requireAuthenticated(ctx, true); err != nil {
		return nil, err
	}
	switch request.GetAction() {
	case etcdserverpb.DowngradeRequest_VALIDATE, etcdserverpb.DowngradeRequest_ENABLE:
		targetVersion, err := parseDowngradeVersion(request.GetVersion())
		if err != nil {
			return nil, rpctypes.ErrGRPCWrongDowngradeVersionFormat
		}
		if err := s.peers.SyncReadRevision(ctx); err != nil {
			return nil, readBarrierStatusErr(err)
		}
		if !validDowngradeTargetVersion(targetVersion) {
			return nil, rpctypes.ErrGRPCInvalidDowngradeTargetVersion
		}
		if request.GetAction() == etcdserverpb.DowngradeRequest_VALIDATE {
			header, err := s.maintenanceHeader(ctx)
			if err != nil {
				return nil, err
			}
			return &etcdserverpb.DowngradeResponse{Header: header, Version: ClusterVersion}, nil
		}
	case etcdserverpb.DowngradeRequest_CANCEL:
		// The version field is ignored for CANCEL by etcd.
		// Upstream DowngradeCancel performs a linearizable read before checking
		// downgrade state, but EtcdServer.downgradeCancel deliberately ignores
		// that internal error and still returns a successful response. Refresh the
		// revision on the same best-effort basis, then attach the normal
		// maintenance response header filled by the public wrapper.
		_ = s.peers.SyncReadRevision(ctx)
		header, err := s.maintenanceHeader(ctx)
		if err != nil {
			return nil, err
		}
		current := semver.Must(semver.NewVersion(Version))
		return &etcdserverpb.DowngradeResponse{
			Header:  header,
			Version: fmt.Sprintf("%d.%d", current.Major, current.Minor),
		}, nil
	default:
		return nil, status.Error(codes.Unknown, "etcdserver: unknown method")
	}
	return nil, downgradePlatformManagedError()
}

func parseDowngradeVersion(value string) (*semver.Version, error) {
	if version, err := semver.NewVersion(value); err == nil {
		return version, nil
	}
	return semver.NewVersion(value + ".0")
}

func validDowngradeTargetVersion(target *semver.Version) bool {
	current := semver.Must(semver.NewVersion(Version))
	return target.Major == current.Major && target.Minor == current.Minor-1
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

func (s *RPCServer) maintenanceHeader(ctx context.Context) (*etcdserverpb.ResponseHeader, error) {
	// ClusterId/MemberId are stamped on EVERY response header by the
	// ClientServerOptions interceptor (#79), so they need not be set here;
	// Revision is method-specific. (MemberId there uses the same local-identity
	// derivation as StatusResponse.Leader, so "am I the leader" comparisons —
	// Leader == MemberId — behave like etcd's.)
	revision, err := safeBackendRevision(ctx, s.backend)
	if err != nil {
		return nil, err
	}
	return txnHeader(int64(revision)), nil
}

func (s *RPCServer) memberIDFromAddress(address string) uint64 {
	if !election.IsLeaderKnown(address) {
		return 0
	}
	return uint64(crc32.ChecksumIEEE([]byte(address)))
}
