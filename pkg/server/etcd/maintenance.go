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
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"hash/crc32"
	"runtime"
	"strconv"
	"time"

	"github.com/Masterminds/semver/v3"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"k8s.io/klog/v2"

	"github.com/kubewharf/kubebrain/pkg/backend"
	"github.com/kubewharf/kubebrain/pkg/backend/election"
	"github.com/kubewharf/kubebrain/pkg/etcdsnapshot"
	"github.com/kubewharf/kubebrain/pkg/metrics"
	"github.com/kubewharf/kubebrain/pkg/server/proxyprotocol"
	"github.com/kubewharf/kubebrain/pkg/storage"
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
	_ = metricCli.EmitGauge(
		"etcd.server.go_version", 1, metrics.Tag("server_go_version", runtime.Version()),
	)
}

func emitServerIDMetric(metricCli metrics.Metrics, memberID uint64) {
	if memberID == 0 {
		return
	}
	_ = metricCli.EmitGauge(
		"etcd.server.id", 1, metrics.Tag("server_id", strconv.FormatUint(memberID, 16)),
	)
}

func emitKnownPeersMetric(metricCli metrics.Metrics, localID uint64, members []*etcdserverpb.Member) {
	if localID == 0 {
		return
	}
	local := strconv.FormatUint(localID, 16)
	for _, member := range members {
		if member.GetID() == 0 {
			continue
		}
		_ = metricCli.EmitGauge(
			"etcd.network.known_peers", 1,
			metrics.Tag("Local", local),
			metrics.Tag("Remote", strconv.FormatUint(member.GetID(), 16)),
		)
	}
}

// etcd substitutes this value when --quota-backend-bytes is unset. KubeBrain's
// actual capacity belongs to TiKV/PD, but Status must still return a nonzero
// protocol-compatible value for etcdctl and other 3.6+ clients.
const defaultEtcdBackendQuota int64 = 2 * 1024 * 1024 * 1024

func (s *RPCServer) Alarm(ctx context.Context, req *etcdserverpb.AlarmRequest) (_ *etcdserverpb.AlarmResponse, retErr error) {
	s.metricCli.EmitCounter("maintenance.alarm", 1)
	_, leadingFresh := s.peers.EpochAndLeadingFresh()
	if !leadingFresh && s.peers.EtcdProxyEnabled() {
		// A DBaaS ingress replica can remain available to its peers while losing
		// every TiKV/PD path. Forward the untouched Alarm operation and logical
		// client identity; the trusted leader runs this complete handler, including
		// GET authentication/read fencing and mutation admin checks.
		proxyCtx, err := s.forwardWriteAuthContext(ctx)
		if err != nil {
			return nil, err
		}
		response, err := s.peers.Alarm(proxyCtx, req)
		response, err = validateMaintenanceProxyResult(s.metricCli, s.expectedProxyResponseIdentity(), maintenanceProxyRPCAlarm, response, err)
		response, err = validateAlarmProxyPayload(s.metricCli, req, response, err)
		s.observeForwardedRevision(response.GetHeader(), err)
		return response, err
	}
	switch req.GetAction() {
	case etcdserverpb.AlarmRequest_GET:
		if err := s.requireAuthenticated(ctx, false); err != nil {
			return nil, err
		}
		if err := s.peers.SyncReadRevision(ctx); err != nil {
			return nil, readBarrierStatusErr(err)
		}
		defer beginEtcdApply(s.metricCli, "Alarm", &retErr)()
	case etcdserverpb.AlarmRequest_DEACTIVATE:
		if err := s.requireAuthenticated(ctx, true); err != nil {
			return nil, err
		}
		header, err := s.maintenanceHeader(ctx)
		if err != nil {
			return nil, err
		}
		defer beginEtcdApply(s.metricCli, "Alarm", &retErr)()
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
		defer beginEtcdApply(s.metricCli, "Alarm", &retErr)()
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

func (s *RPCServer) Status(ctx context.Context, req *etcdserverpb.StatusRequest) (*etcdserverpb.StatusResponse, error) {
	s.metricCli.EmitCounter("maintenance.status", 1)
	if checkpoint, checkpointErr := s.backend.GetSerializableCheckpoint(); checkpointErr == nil {
		liveCtx, cancel := context.WithTimeout(ctx, serializableLiveReadBudget)
		response, err := s.statusOnce(liveCtx, req)
		cancel()
		if err == nil || ctx.Err() != nil || !isSerializableLiveReadFallbackError(err) {
			return response, err
		}
		s.metricCli.EmitCounter("maintenance.status.checkpoint_fallback", 1)
		return s.localMaintenanceStatus(backend.WithSerializableCheckpoint(ctx, checkpoint))
	}
	return s.statusOnce(ctx, req)
}

func (s *RPCServer) statusOnce(ctx context.Context, req *etcdserverpb.StatusRequest) (*etcdserverpb.StatusResponse, error) {
	_, leadingFresh := s.peers.EpochAndLeadingFresh()
	if !leadingFresh && s.peers.EtcdProxyEnabled() {
		// Upstream Status reads only the serving member's already-applied BoltDB,
		// Raft and alarm state, while KubeBrain's logical state lives in shared
		// TiKV. Race both paths so a storage-isolated ingress can use the leader,
		// while a member with healthy storage retains upstream's member-local
		// diagnostic availability when only its leader-peer path is unavailable.
		return s.hedgedMaintenanceStatus(ctx, req)
	}
	return s.localMaintenanceStatus(ctx)
}

func (s *RPCServer) localMaintenanceStatus(ctx context.Context) (*etcdserverpb.StatusResponse, error) {
	if err := s.requireAuthenticated(ctx, false); err != nil {
		return nil, err
	}
	// Status is intentionally member-local and does not require a leader read
	// barrier, but a newly started replica must restore its persisted user
	// revision before exposing the synthetic Raft indexes. Sampling the raw local
	// cache here used to capture zero; QuotaStatus recovered the cache later, so
	// the same response had a durable Header.Revision but zero Raft{,Applied}Index.
	revision, err := s.freshMaintenanceRevision(ctx)
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
		StorageVersion:   etcdsnapshot.StorageVersion,
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

func (s *RPCServer) hedgedMaintenanceStatus(
	ctx context.Context, req *etcdserverpb.StatusRequest,
) (*etcdserverpb.StatusResponse, error) {
	return hedgeMaintenanceResult(
		ctx,
		func(ctx context.Context) (*etcdserverpb.StatusResponse, error) {
			return s.localMaintenanceStatus(ctx)
		},
		func(ctx context.Context) (*etcdserverpb.StatusResponse, error) {
			proxyCtx, err := s.forwardWriteAuthContext(ctx)
			if err != nil {
				return nil, err
			}
			identity := s.expectedProxyResponseIdentity()
			response, err := s.peers.Status(proxyCtx, req)
			response, err = validateMaintenanceProxyResult(s.metricCli, identity, maintenanceProxyRPCStatus, response, err)
			response, err = validateStatusProxyPayload(s.metricCli, identity, response, err)
			if response != nil {
				// IsLearner is a property of the serving member, not the leader that
				// supplied the shared TiKV-backed status payload.
				response.IsLearner = s.localMemberIsLearner()
			}
			return response, err
		},
		func(response *etcdserverpb.StatusResponse, err error) {
			s.observeForwardedRevision(response.GetHeader(), err)
		},
		terminalMaintenanceResultError,
	)
}

// freshMaintenanceRevision reports the shared TiKV commit watermark rather
// than a follower's process-local cache. Unlike a linearizable KV read this
// does not require leader routing: every user mutation advances the durable
// revision in the same TiKV transaction. Status is commonly followed by a
// maintenance call that does establish a read barrier (for example downgrade
// validation); sampling durable state here keeps their response headers from
// describing two revisions solely because the first request hit a follower.
func (s *RPCServer) freshMaintenanceRevision(ctx context.Context) (uint64, error) {
	if checkpoint, protected := backend.SerializableCheckpointFromContext(ctx); protected {
		// The checkpoint revision is the durable watermark captured at the same
		// protected engine timestamp. Re-reading its internal row would add no
		// freshness and can reintroduce an avoidable TiKV route dependency while
		// serving the member-local degraded Status path.
		return checkpoint.Revision, nil
	}
	revision, err := safeBackendRevision(ctx, s.backend)
	if err != nil {
		return 0, err
	}
	durableRevision, err := s.backend.GetDurableRevision(ctx)
	if errors.Is(err, storage.ErrKeyNotFound) {
		return revision, nil
	}
	if err != nil {
		return 0, err
	}
	if durableRevision > revision {
		revision = durableRevision
		s.backend.SetCurrentRevision(revision)
	}
	return revision, nil
}

func alarmStatusError(alarm *etcdserverpb.AlarmMember) string {
	// Upstream Status appends AlarmMember.String() verbatim. In particular,
	// protobuf deliberately varies the compact renderer's separator between
	// builds. Emit one of the two reachable forms deterministically while
	// preserving protobuf's omission of zero-valued fields.
	if alarm.GetMemberID() == 0 && alarm.GetAlarm() == etcdserverpb.AlarmType_NONE {
		return ""
	}
	if alarm.GetMemberID() == 0 {
		return fmt.Sprintf("alarm:%s", alarm.GetAlarm().String())
	}
	if alarm.GetAlarm() == etcdserverpb.AlarmType_NONE {
		return fmt.Sprintf("memberID:%d", alarm.GetMemberID())
	}
	return fmt.Sprintf("memberID:%d  alarm:%s", alarm.GetMemberID(), alarm.GetAlarm().String())
}

func matchesAlarmStatusError(alarm *etcdserverpb.AlarmMember, value string) bool {
	if alarm.GetMemberID() == 0 || alarm.GetAlarm() == etcdserverpb.AlarmType_NONE {
		return value == alarmStatusError(alarm)
	}
	return value == alarmStatusError(alarm) ||
		value == fmt.Sprintf("memberID:%d alarm:%s", alarm.GetMemberID(), alarm.GetAlarm().String())
}

func (s *RPCServer) Defragment(ctx context.Context, req *etcdserverpb.DefragmentRequest) (*etcdserverpb.DefragmentResponse, error) {
	s.metricCli.EmitCounter("maintenance.defragment", 1)
	if checkpoint, checkpointErr := s.backend.GetSerializableCheckpoint(); checkpointErr == nil {
		liveCtx, cancel := context.WithTimeout(ctx, serializableLiveReadBudget)
		response, err := s.defragmentOnce(liveCtx, req)
		cancel()
		if err == nil || ctx.Err() != nil || !isSerializableLiveReadFallbackError(err) {
			return response, err
		}
		s.metricCli.EmitCounter("maintenance.defragment.checkpoint_fallback", 1)
		return s.localMaintenanceDefragment(backend.WithSerializableCheckpoint(ctx, checkpoint))
	}
	return s.defragmentOnce(ctx, req)
}

func (s *RPCServer) defragmentOnce(ctx context.Context, req *etcdserverpb.DefragmentRequest) (*etcdserverpb.DefragmentResponse, error) {
	_, leadingFresh := s.peers.EpochAndLeadingFresh()
	if !leadingFresh && s.peers.EtcdProxyEnabled() {
		// TiKV owns physical compaction, so this is a compatibility no-op after
		// admin auth. Race that local path with the trusted leader: upstream
		// defragments the serving member even when it cannot reach the leader,
		// while a storage-isolated KubeBrain ingress still needs the leader path.
		return s.hedgedMaintenanceDefragment(ctx, req)
	}
	return s.localMaintenanceDefragment(ctx)
}

func (s *RPCServer) localMaintenanceDefragment(ctx context.Context) (*etcdserverpb.DefragmentResponse, error) {
	if err := s.requireProtectedMaintenanceAuthenticated(ctx, true); err != nil {
		return nil, err
	}
	return &etcdserverpb.DefragmentResponse{}, nil
}

func (s *RPCServer) requireProtectedMaintenanceAuthenticated(ctx context.Context, root bool) error {
	if _, pinned := backend.SerializableCheckpointFromContext(ctx); pinned {
		if caller, err, complete := s.authCallerFromCachedContext(ctx); complete {
			if err != nil {
				return err
			}
			if root && caller != nil {
				return caller.adminError()
			}
			return nil
		}
	}
	return s.requireAuthenticated(ctx, root)
}

func (s *RPCServer) hedgedMaintenanceDefragment(
	ctx context.Context, req *etcdserverpb.DefragmentRequest,
) (*etcdserverpb.DefragmentResponse, error) {
	return hedgeMaintenanceResult(
		ctx,
		func(ctx context.Context) (*etcdserverpb.DefragmentResponse, error) {
			return s.localMaintenanceDefragment(ctx)
		},
		func(ctx context.Context) (*etcdserverpb.DefragmentResponse, error) {
			proxyCtx, err := s.forwardWriteAuthContext(ctx)
			if err != nil {
				return nil, err
			}
			response, err := s.peers.Defragment(proxyCtx, req)
			return validateMaintenanceProxyResult(s.metricCli, s.expectedProxyResponseIdentity(), maintenanceProxyRPCDefragment, response, err)
		},
		func(response *etcdserverpb.DefragmentResponse, err error) {
			s.observeForwardedRevision(response.GetHeader(), err)
		},
		terminalMaintenanceResultError,
	)
}

func authorizedPeerHashKVProxy(ctx context.Context) bool {
	if !isPeerRequest(ctx) {
		return false
	}
	values := metadata.ValueFromIncomingContext(ctx, authorizedPeerHashKVProxyMetadataKey)
	return len(values) == 1 && values[0] == "1"
}

func (s *RPCServer) Hash(ctx context.Context, req *etcdserverpb.HashRequest) (*etcdserverpb.HashResponse, error) {
	s.metricCli.EmitCounter("maintenance.hash", 1)
	// Backend Hash covers revision-neutral internal state in addition to user
	// MVCC. A serializable checkpoint is identified only by the user revision,
	// so substituting an older checkpoint after a slow live scan can return a
	// different checksum with the same response-header revision. Upstream Hash
	// is a current backend diagnostic rather than a revision-addressed read:
	// preserve that identity and fail when the live backend cannot be read.
	return s.hashOnce(ctx, req)
}

func (s *RPCServer) hashOnce(ctx context.Context, req *etcdserverpb.HashRequest) (*etcdserverpb.HashResponse, error) {
	_, leadingFresh := s.peers.EpochAndLeadingFresh()
	if !leadingFresh && s.peers.EtcdProxyEnabled() {
		// Race the local shared TiKV history with the leader peer. This preserves
		// member-local etcd availability when either the ingress storage path or
		// its leader-peer path (but not both) is unavailable.
		return s.hedgedMaintenanceHash(ctx, req)
	}
	return s.localMaintenanceHash(ctx, true)
}

func (s *RPCServer) localMaintenanceHash(ctx context.Context, refresh bool) (*etcdserverpb.HashResponse, error) {
	if err := s.requireProtectedMaintenanceAuthenticated(ctx, true); err != nil {
		return nil, err
	}
	// KubeBrain's committed revision is cached per replica even though the MVCC
	// data is shared in TiKV. Refresh it when possible, but preserve etcd's
	// member-local diagnostic behavior when the leader is unavailable.
	if refresh {
		_ = s.peers.SyncReadRevision(ctx)
	}
	start := time.Now()
	hashResult, err := s.backend.Hash(ctx)
	if err != nil {
		return nil, err
	}
	emitEtcdMVCCHashDuration(s.metricCli, time.Since(start))
	return &etcdserverpb.HashResponse{
		Header: txnHeader(hashResult.CurrentRevision), Hash: hashResult.Hash,
	}, nil
}

func (s *RPCServer) HashKV(ctx context.Context, req *etcdserverpb.HashKVRequest) (*etcdserverpb.HashKVResponse, error) {
	s.metricCli.EmitCounter("maintenance.hashkv", 1)
	s.emitMaintenanceHashKVStage("entered")
	_, leadingFresh := s.peers.EpochAndLeadingFresh()
	// Upstream hashes an explicit historical revision from the serving member's
	// already-applied local MVCC backend, so losing quorum does not require a new
	// read index. KubeBrain has no per-member bbolt file; use the same
	// GC-protected, Region-warmed TiKV checkpoint that backs degraded
	// serializable reads. Filtering encoded object versions still happens at the
	// requested logical revision, while the checkpoint supplies a known engine
	// timestamp and the member's latest safely applied response-header revision.
	// Select it independently of the sampled leading epoch: PD quorum can vanish
	// after that sample, whereas an explicit historical hash must remain a local
	// operation. Never use a checkpoint that predates the request, and retain the
	// ordinary path for latest/negative revisions.
	if requestedRevision := req.GetRevision(); requestedRevision > 0 {
		if checkpoint, err := s.backend.GetSerializableCheckpoint(); err == nil &&
			uint64(requestedRevision) <= checkpoint.Revision {
			ctx = backend.WithSerializableCheckpoint(ctx, checkpoint)
			s.emitMaintenanceHashKVStage("checkpoint_attached")
		} else {
			s.emitMaintenanceHashKVStage("checkpoint_unavailable")
		}
	}
	if !leadingFresh && s.peers.EtcdProxyEnabled() {
		s.emitMaintenanceHashKVStage("hedged")
		return s.hedgedMaintenanceHashKV(ctx, req)
	}
	s.emitMaintenanceHashKVStage("local")
	return s.localMaintenanceHashKV(ctx, req, true)
}

// emitMaintenanceHashKVStage exposes a bounded state machine for a diagnostic
// RPC that is expected to remain usable during backend quorum faults. The
// values are compile-time constants at call sites: never add revisions, keys,
// tenants, member addresses, or error strings to this label.
func (s *RPCServer) emitMaintenanceHashKVStage(stage string) {
	_ = s.metricCli.EmitCounter("maintenance.hashkv.stage", 1, metrics.Tag("stage", stage))
}

func (s *RPCServer) localMaintenanceHashKV(
	ctx context.Context, req *etcdserverpb.HashKVRequest, refresh bool,
) (*etcdserverpb.HashKVResponse, error) {
	s.emitMaintenanceHashKVStage("local_entered")
	if !authorizedPeerHashKVProxy(ctx) {
		if _, pinned := backend.SerializableCheckpointFromContext(ctx); pinned {
			// An upstream member authorizes historical HashKV against its already-
			// applied local auth store. The protected checkpoint is useful during
			// exactly the interval in which refreshing that store from TiKV would
			// require a new PD TSO. Reuse only a complete applied snapshot; a cold
			// member still falls through to the authoritative path and fails closed.
			if caller, err, complete := s.authCallerFromCachedContext(ctx); complete {
				if err != nil {
					return nil, err
				}
				if caller != nil {
					if err := caller.adminError(); err != nil {
						return nil, err
					}
				}
			} else if err := s.requireAuthenticated(ctx, true); err != nil {
				return nil, err
			}
		} else if err := s.requireAuthenticated(ctx, true); err != nil {
			return nil, err
		}
	}
	s.emitMaintenanceHashKVStage("auth_complete")
	// A successful refresh pins normal-operation hashes to the latest committed
	// revision. A failed refresh must not make this local diagnostic unavailable.
	if refresh {
		if _, pinned := backend.SerializableCheckpointFromContext(ctx); pinned {
			// An explicit historical hash is already bound to this member's
			// protected applied snapshot. Upstream does not put a read-index barrier
			// in front of HashKV(revision), and doing so would consume the entire RPC
			// deadline while PD quorum is unavailable.
			refresh = false
		}
	}
	if refresh {
		_ = s.peers.SyncReadRevision(ctx)
	}
	s.emitMaintenanceHashKVStage("refresh_complete")
	revision := req.GetRevision()
	if req.GetRevision() > 0 {
		if checkpoint, pinned := backend.SerializableCheckpointFromContext(ctx); pinned {
			if uint64(revision) < checkpoint.CompactRevision {
				return nil, compactedRevisionError()
			}
			if uint64(revision) > checkpoint.Revision {
				return nil, futureRevisionError()
			}
		} else {
			// Use the request context so a cancelled/expired HashKV call aborts the
			// revision and compaction lookups instead of running under a detached
			// context.Background() (#59).
			if err := s.checkRequestedRevision(ctx, revision); err != nil {
				return nil, err
			}
		}
	}
	s.emitMaintenanceHashKVStage("revision_validated")
	start := time.Now()
	s.emitMaintenanceHashKVStage("backend_started")
	hashResult, err := s.backend.HashKV(ctx, revision)
	if err != nil {
		s.emitMaintenanceHashKVStage("backend_failed")
		if errors.Is(err, backend.ErrHashKVCompacted) {
			return nil, compactedRevisionError()
		}
		if errors.Is(err, backend.ErrHashKVFuture) {
			return nil, futureRevisionError()
		}
		return nil, err
	}
	s.emitMaintenanceHashKVStage("backend_complete")
	emitEtcdMVCCHashRevDuration(s.metricCli, time.Since(start))
	return &etcdserverpb.HashKVResponse{
		Header:          txnHeader(hashResult.CurrentRevision),
		Hash:            hashResult.Hash,
		CompactRevision: hashResult.CompactRevision,
		HashRevision:    hashResult.HashRevision,
	}, nil
}

func (s *RPCServer) hedgedMaintenanceHash(ctx context.Context, req *etcdserverpb.HashRequest) (*etcdserverpb.HashResponse, error) {
	return hedgeMaintenanceResult(
		ctx,
		func(ctx context.Context) (*etcdserverpb.HashResponse, error) {
			return s.localMaintenanceHash(ctx, false)
		},
		func(ctx context.Context) (*etcdserverpb.HashResponse, error) {
			proxyCtx, err := s.forwardWriteAuthContext(ctx)
			if err != nil {
				return nil, err
			}
			response, err := s.peers.Hash(proxyCtx, req)
			response, err = validateMaintenanceProxyResult(s.metricCli, s.expectedProxyResponseIdentity(), maintenanceProxyRPCHash, response, err)
			return validateHashProxyPayload(s.metricCli, response, err)
		},
		func(response *etcdserverpb.HashResponse, err error) {
			s.observeForwardedRevision(response.GetHeader(), err)
		},
		terminalMaintenanceResultError,
	)
}

func (s *RPCServer) hedgedMaintenanceHashKV(
	ctx context.Context, req *etcdserverpb.HashKVRequest,
) (*etcdserverpb.HashKVResponse, error) {
	return hedgeMaintenanceResult(
		ctx,
		func(ctx context.Context) (*etcdserverpb.HashKVResponse, error) {
			// Non-positive revisions still report the serving member's current revision
			// in the response header. A follower's local result is therefore a valid
			// hedge only after it has crossed the leader's read revision barrier;
			// otherwise a fast stale header can beat the authoritative peer response
			// immediately after an acknowledged Put. Revision zero hashes the latest
			// state, while negative revisions retain upstream's empty historical hash.
			// Explicit positive revisions remain member-local diagnostics.
			if req.GetRevision() <= 0 {
				if err := s.peers.SyncReadRevision(ctx); err != nil {
					return nil, err
				}
			}
			return s.localMaintenanceHashKV(ctx, req, false)
		},
		func(ctx context.Context) (*etcdserverpb.HashKVResponse, error) {
			proxyCtx, err := s.forwardWriteAuthContext(ctx)
			if err != nil {
				return nil, err
			}
			response, err := s.peers.HashKV(proxyCtx, req)
			response, err = validateMaintenanceProxyResult(s.metricCli, s.expectedProxyResponseIdentity(), maintenanceProxyRPCHashKV, response, err)
			return validateHashKVProxyPayload(s.metricCli, req, response, err)
		},
		func(response *etcdserverpb.HashKVResponse, err error) {
			s.observeForwardedRevision(response.GetHeader(), err)
		},
		terminalMaintenanceResultError,
	)
}

type maintenanceResult[T any] struct {
	response T
	err      error
	remote   bool
}

func hedgeMaintenanceResult[T any](
	ctx context.Context,
	local func(context.Context) (T, error),
	remote func(context.Context) (T, error),
	observeRemote func(T, error),
	terminalError func(error) bool,
) (T, error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	results := make(chan maintenanceResult[T], 2)
	go func() {
		response, err := local(ctx)
		results <- maintenanceResult[T]{response: response, err: err}
	}()
	go func() {
		response, err := remote(ctx)
		results <- maintenanceResult[T]{response: response, err: err, remote: true}
	}()

	var zero T
	var firstErr error
	for range 2 {
		select {
		case <-ctx.Done():
			return zero, ctx.Err()
		case result := <-results:
			if result.remote {
				observeRemote(result.response, result.err)
			}
			if result.err == nil {
				return result.response, nil
			}
			if terminalError(result.err) {
				return zero, result.err
			}
			if firstErr == nil {
				firstErr = result.err
			}
		}
	}
	return zero, firstErr
}

func terminalMaintenanceResultError(err error) bool {
	// HashKV revision errors are not terminal while hedging. A follower's
	// process-local revision watermark can lag the shared TiKV history, so its
	// fast future-revision result must not cancel a leader response that can
	// serve the requested revision. Waiting for the other result also preserves
	// the correct compacted/future outcome when both paths reject the request.
	if errors.Is(err, rpctypes.ErrUserEmpty) || errors.Is(err, rpctypes.ErrUserNotFound) ||
		errors.Is(err, rpctypes.ErrAuthFailed) || errors.Is(err, rpctypes.ErrPermissionDenied) ||
		errors.Is(err, rpctypes.ErrInvalidAuthToken) || errors.Is(err, rpctypes.ErrAuthOldRevision) {
		return true
	}
	switch status.Code(err) {
	case codes.InvalidArgument, codes.Unauthenticated, codes.PermissionDenied, codes.NotFound,
		codes.FailedPrecondition, codes.DataLoss:
		return true
	default:
		return false
	}
}

func (s *RPCServer) Snapshot(request *etcdserverpb.SnapshotRequest, stream etcdserverpb.Maintenance_SnapshotServer) error {
	s.metricCli.EmitCounter("maintenance.snapshot", 1)
	ctx := stream.Context()
	_, leadingFresh := s.peers.EpochAndLeadingFresh()
	// Healthy members keep the existing leader-captured semantics: a follower
	// proxies to the leader instead of returning an arbitrarily old local
	// checkpoint. A member whose data plane is unavailable exports its last fully
	// protected applied checkpoint, matching etcd's member-local Snapshot
	// availability without pretending that the artifact contains newer writes.
	checkpoint, checkpointErr := s.backend.GetSerializableCheckpoint()
	protected := false
	if checkpointErr == nil {
		// A backend partition begins before the local leader lease expires. Probe
		// the live read barrier with a small slice of the Snapshot deadline so the
		// request can select the protected artifact before any stream frame is
		// emitted. This also prevents a healthy follower from serving a stale local
		// checkpoint instead of proxying the current leader. The actual live capture
		// repeats the barrier under its write fence; this probe is availability
		// selection, not linearization.
		probeCtx, cancel := context.WithTimeout(ctx, 750*time.Millisecond)
		probeErr := s.probeSnapshotDataPlane(probeCtx)
		cancel()
		protected = probeErr != nil
	}
	if protected {
		ctx = backend.WithSerializableCheckpoint(ctx, checkpoint)
		stream = &contextMaintenanceSnapshotServer{Maintenance_SnapshotServer: stream, ctx: ctx}
		s.metricCli.EmitCounter("maintenance.snapshot.checkpoint", 1)
		klog.InfoS(
			"serving maintenance Snapshot from protected checkpoint",
			"revision", checkpoint.Revision,
			"timestamp", checkpoint.Timestamp,
		)
	}
	if !protected && !leadingFresh && s.peers.EtcdProxyEnabled() {
		// A follower can remain reachable through the peer network after losing
		// every TiKV/PD path. Do not read its auth snapshot before deciding to
		// proxy: unlike raft etcd, that process-local cache is neither guaranteed
		// current nor sufficient to authorize a new request. The leader owns both
		// initial authentication and the fixed-revision snapshot transaction.
		proxyCtx, err := s.forwardWriteAuthContext(stream.Context())
		if err != nil {
			return err
		}
		return s.forwardSnapshot(proxyCtx, request, stream)
	}
	caller, err := s.authCallerFromContext(ctx)
	if err != nil {
		if errors.Is(err, errInvalidAuthMetadata) {
			return status.Error(codes.FailedPrecondition, fmt.Sprintf("%s: %v", etcdsnapshot.ErrInvalidSnapshotMetadata, err))
		}
		return err
	}
	if caller != nil {
		if err = caller.adminError(); err != nil {
			return err
		}
	}
	if !protected && !s.peers.IsLeader() {
		if !s.peers.EtcdProxyEnabled() {
			// BeginRangeTxn is a process-local barrier. A follower cannot use it
			// to freeze the leader's metadata mutations while taking the several
			// TiKV reads that form one portable snapshot.
			return rpctypes.ErrGRPCNotLeader
		}
		proxyCtx, err := s.forwardAuthToken(stream.Context(), caller)
		if err != nil {
			return err
		}
		return s.forwardSnapshot(proxyCtx, request, stream)
	}
	if !s.snapshotActive.CompareAndSwap(false, true) {
		emitSnapshotAdmissionRejected(s.metricCli)
		emitClientAdmissionRejection(s.metricCli, clientAdmissionGuardConcurrency)
		return rpctypes.ErrGRPCRequestTooManyRequests
	}
	emitSnapshotActive(s.metricCli, true)
	defer func() {
		s.snapshotActive.Store(false)
		emitSnapshotActive(s.metricCli, false)
	}()
	err = s.sendSnapshot(stream)
	if errors.Is(err, errSnapshotHistoryStreamProtocol) {
		emitSnapshotFailure(s.metricCli, snapshotFailureProtocol)
		// The local capture stream has already crossed its fixed-revision
		// integrity boundary. Missing, duplicate, or out-of-order termination is
		// the leader-side equivalent of a malformed proxied Snapshot stream.
		return status.Error(codes.DataLoss, err.Error())
	}
	if err != nil && ctx.Err() == nil {
		if errors.Is(err, errSnapshotSend) {
			if shouldCountServerStreamFailure(stream.Context(), err) {
				emitSnapshotFailure(s.metricCli, snapshotFailureSend)
			}
		} else {
			emitSnapshotFailure(s.metricCli, snapshotFailureSource)
		}
	}
	if errors.Is(err, errSnapshotHistoricalLeaseUnknown) || errors.Is(err, etcdsnapshot.ErrInvalidRetainedHistory) ||
		errors.Is(err, etcdsnapshot.ErrInvalidSnapshotMetadata) || errors.Is(err, backend.ErrInvalidMVCCMetadata) {
		// This is durable source-data provenance, not an opaque server fault:
		// retrying the same history or metadata cannot succeed until the source is
		// compacted, migrated, or repaired.
		// Preserve the key/revision diagnostic while giving DBaaS automation a
		// stable non-transient class instead of grpc-go's fallback Unknown.
		return status.Error(codes.FailedPrecondition, err.Error())
	}
	return err
}

var snapshotDataPlaneProbeKey = []byte("\x00kubebrain/snapshot-data-plane-probe")

// probeSnapshotDataPlane verifies both halves needed by a live capture. A TSO
// alone only proves PD availability; the point read forces an actual TiKV
// request through the tenant object/revision keyspace. Snapshot construction
// remains the authoritative full-range check, so this admission probe stays
// constant-cost instead of scanning the complete database twice.
func (s *RPCServer) probeSnapshotDataPlane(ctx context.Context) error {
	if _, err := s.backend.GetFollowerSnapshotTimestamp(ctx); err != nil {
		return err
	}
	_, err := s.backend.Get(ctx, &etcdserverpb.RangeRequest{Key: snapshotDataPlaneProbeKey})
	return err
}

type contextMaintenanceSnapshotServer struct {
	etcdserverpb.Maintenance_SnapshotServer
	ctx context.Context
}

func (s *contextMaintenanceSnapshotServer) Context() context.Context { return s.ctx }

func (s *RPCServer) forwardSnapshot(
	ctx context.Context,
	request *etcdserverpb.SnapshotRequest,
	stream etcdserverpb.Maintenance_SnapshotServer,
) error {
	proxyCtx, cancelProxy := context.WithCancel(ctx)
	defer cancelProxy()
	sentAny := false
	for attempt := 0; attempt < preResponseStreamProxyAttempts; attempt++ {
		responses, callErr := s.peers.Snapshot(proxyCtx, request)
		if callErr != nil {
			mappedErr := snapshotForwardError(ctx, callErr)
			if !sentAny && attempt+1 < preResponseStreamProxyAttempts && errors.Is(mappedErr, rpctypes.ErrGRPCLeaderChanged) {
				s.metricCli.EmitCounter("maintenance.snapshot.proxy_retry", 1)
				continue
			}
			if ctx.Err() == nil {
				emitSnapshotFailure(s.metricCli, snapshotFailureProxy)
			}
			return mappedErr
		}
		if responses == nil {
			emitSnapshotFailure(s.metricCli, snapshotFailureProtocol)
			return status.Error(codes.DataLoss, "leader snapshot proxy returned a nil result channel")
		}
		awaitingChecksum := false
		complete := false
		retry := false
		hash := sha256.New()
		var remaining uint64
		var snapshotVersion string
		haveData := false
		for {
			result, ok, receiveErr := receiveProxyStreamResult(
				ctx, responses, complete, s.proxyStreamTrailingStatusTimeout,
			)
			if receiveErr != nil {
				if receiveErr == errProxyStreamTrailingStatusTimeout {
					emitSnapshotFailure(s.metricCli, snapshotFailureProxy)
				}
				return receiveErr
			}
			if !ok {
				break
			}
			if result.Err != nil && result.Response != nil {
				emitSnapshotFailure(s.metricCli, snapshotFailureProtocol)
				return status.Error(codes.DataLoss, "leader snapshot proxy returned a mixed response and error")
			}
			if result.Err != nil {
				mappedErr := snapshotForwardError(ctx, result.Err)
				if !sentAny && attempt+1 < preResponseStreamProxyAttempts && errors.Is(mappedErr, rpctypes.ErrGRPCLeaderChanged) {
					s.metricCli.EmitCounter("maintenance.snapshot.proxy_retry", 1)
					retry = true
					break
				}
				if ctx.Err() == nil {
					emitSnapshotFailure(s.metricCli, snapshotFailureProxy)
				}
				return mappedErr
			}
			if result.Response == nil {
				emitSnapshotFailure(s.metricCli, snapshotFailureProtocol)
				return status.Error(codes.DataLoss, "leader snapshot proxy returned an empty response")
			}
			if complete {
				emitSnapshotFailure(s.metricCli, snapshotFailureProtocol)
				return status.Error(codes.DataLoss, "leader snapshot proxy returned data after checksum")
			}
			if awaitingChecksum {
				if result.Response.GetRemainingBytes() != 0 || len(result.Response.GetBlob()) != sha256.Size {
					emitSnapshotFailure(s.metricCli, snapshotFailureProtocol)
					return status.Error(codes.DataLoss, "leader snapshot proxy returned an invalid checksum frame")
				}
				if result.Response.GetVersion() != snapshotVersion {
					emitSnapshotFailure(s.metricCli, snapshotFailureProtocol)
					return status.Error(codes.DataLoss, "leader snapshot proxy changed storage version")
				}
				if !bytes.Equal(result.Response.GetBlob(), hash.Sum(nil)) {
					emitSnapshotFailure(s.metricCli, snapshotFailureProtocol)
					return status.Error(codes.DataLoss, "leader snapshot proxy checksum mismatch")
				}
				complete = true
			} else {
				blob := result.Response.GetBlob()
				if len(blob) == 0 {
					emitSnapshotFailure(s.metricCli, snapshotFailureProtocol)
					return status.Error(codes.DataLoss, "leader snapshot proxy returned an empty data frame")
				}
				if haveData {
					if uint64(len(blob)) > remaining || result.Response.GetRemainingBytes() != remaining-uint64(len(blob)) {
						emitSnapshotFailure(s.metricCli, snapshotFailureProtocol)
						return status.Error(codes.DataLoss, "leader snapshot proxy returned discontinuous remaining bytes")
					}
					if result.Response.GetVersion() != snapshotVersion {
						emitSnapshotFailure(s.metricCli, snapshotFailureProtocol)
						return status.Error(codes.DataLoss, "leader snapshot proxy changed storage version")
					}
				} else {
					snapshotVersion = result.Response.GetVersion()
					haveData = true
				}
				_, _ = hash.Write(blob)
				remaining = result.Response.GetRemainingBytes()
				awaitingChecksum = remaining == 0
			}
			if sendErr := stream.Send(result.Response); sendErr != nil {
				if shouldCountServerStreamFailure(stream.Context(), sendErr) {
					emitSnapshotFailure(s.metricCli, snapshotFailureSend)
				}
				return sendErr
			}
			sentAny = true
		}
		if retry {
			continue
		}
		if !complete {
			if ctx.Err() == nil {
				emitSnapshotFailure(s.metricCli, snapshotFailureProtocol)
			}
			return status.Error(codes.DataLoss, "leader snapshot proxy stream ended before checksum")
		}
		return nil
	}
	return status.Error(codes.Internal, "kubebrain: exhausted pre-response snapshot proxy retry")
}

func snapshotForwardError(ctx context.Context, err error) error {
	if err == nil || ctx.Err() != nil {
		return err
	}
	// Closing the proxy's shared gRPC client after a leader connection failure
	// can surface as Canceled even though the downstream caller is still live.
	// A Snapshot stream cannot resume after any response frame was delivered, so
	// report the topology change as Unavailable and let the client restart the
	// complete checksum-protected download. Preserve genuine caller cancellation.
	if errors.Is(err, context.Canceled) || status.Code(err) == codes.Canceled ||
		proxyprotocol.IsPeerStreamDrained(err) {
		return rpctypes.ErrGRPCLeaderChanged
	}
	return err
}

func (s *RPCServer) MoveLeader(ctx context.Context, request *etcdserverpb.MoveLeaderRequest) (*etcdserverpb.MoveLeaderResponse, error) {
	s.metricCli.EmitCounter("maintenance.moveleader", 1)
	localID := s.memberIDForPeerIdentity(s.backend.GetResourceLock().Identity())
	leaderID := s.memberIDForPeerIdentity(s.peers.GetLeaderInfo())
	// Match maintenanceServer.MoveLeader: the serving member must reject the
	// request before validating the transferee when it is not the current
	// leader. This is member-local state, so a load balancer must not turn a
	// request that landed on a follower into an idempotent success merely because
	// TargetID names the actual leader.
	if localID == 0 || localID != leaderID {
		// Upstream's authMaintenanceServer performs its admin check before the
		// member-local not-leader check. A follower already has the applied auth
		// store needed for that decision; consulting TiKV here would make an etcd
		// member-local error depend on quorum storage availability. Use the complete
		// local snapshot when possible, while retaining the authoritative fallback
		// for cold/incomplete auth state.
		if caller, err, complete := s.authCallerFromCachedContext(ctx); complete {
			if err != nil {
				return nil, err
			}
			if caller != nil {
				if err := caller.adminError(); err != nil {
					return nil, err
				}
			}
		} else if err := s.requireAuthenticated(ctx, true); err != nil {
			return nil, err
		}
		return nil, rpctypes.ErrGRPCNotLeader
	}
	// A leader can act on the request, so its authorization decision must use
	// authoritative storage rather than a potentially stale applied snapshot.
	if err := s.requireAuthenticated(ctx, true); err != nil {
		return nil, err
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
	_, leadingFresh := s.peers.EpochAndLeadingFresh()
	if !leadingFresh && s.peers.EtcdProxyEnabled() {
		// Downgrade is cluster-wide in upstream etcd and its gRPC proxy forwards
		// every action. Preserve that boundary here: the trusted leader performs
		// admin auth, version validation and the platform-managed ENABLE decision
		// without requiring this ingress to reach its local TiKV auth path.
		proxyCtx, err := s.forwardWriteAuthContext(ctx)
		if err != nil {
			return nil, err
		}
		response, err := s.peers.Downgrade(proxyCtx, request)
		response, err = validateMaintenanceProxyResult(s.metricCli, s.expectedProxyResponseIdentity(), maintenanceProxyRPCDowngrade, response, err)
		response, err = validateDowngradeProxyPayload(s.metricCli, request, response, err)
		s.observeForwardedRevision(response.GetHeader(), err)
		return response, err
	}
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
		current := semver.MustParse(Version)
		return &etcdserverpb.DowngradeResponse{
			Header:  header,
			Version: fmt.Sprintf("%d.%d", current.Major(), current.Minor()),
		}, nil
	default:
		return nil, status.Error(codes.Unknown, "etcdserver: unknown method")
	}
	return nil, downgradePlatformManagedError()
}

func parseDowngradeVersion(value string) (*semver.Version, error) {
	if version, err := semver.NewVersion(value); err == nil {
		return semver.New(version.Major(), version.Minor(), 0, "", ""), nil
	}
	version, err := semver.NewVersion(value + ".0")
	if err != nil {
		return nil, err
	}
	return semver.New(version.Major(), version.Minor(), 0, "", ""), nil
}

func validDowngradeTargetVersion(target *semver.Version) bool {
	current := semver.MustParse(Version)
	return target.Equal(semver.New(current.Major(), current.Minor()-1, 0, "", ""))
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
