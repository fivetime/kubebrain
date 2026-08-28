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
	"strings"
	"time"
	"unicode/utf8"

	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	"go.etcd.io/etcd/server/v3/etcdserver/api/v3election/v3electionpb"
	"go.etcd.io/etcd/server/v3/etcdserver/api/v3lock/v3lockpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	"github.com/kubewharf/kubebrain/pkg/backend"
	"github.com/kubewharf/kubebrain/pkg/backend/election"
	"github.com/kubewharf/kubebrain/pkg/metrics"
	"github.com/kubewharf/kubebrain/pkg/server/proxyprotocol"
	"github.com/kubewharf/kubebrain/pkg/storage"
)

// ClientServerOptions returns client-facing gRPC admission and response options.
// The interceptors stamp ClusterId and
// MemberId onto EVERY outgoing etcd response header (unary and stream). etcd
// puts these on every response, and Cilium's clustermesh wraps each remote-etcd
// connection with an interceptor that pins the FIRST ClusterId it sees and
// disconnects the cluster on any later mismatch (pkg/clustermesh/common/
// interceptor.go). Stamping only some headers (e.g. Status but not Range) would
// make ClusterId flip between a real value and 0 across responses and trip that
// guard. Doing it in one interceptor — rather than at each of the ~48 header
// construction sites — also makes it impossible for a new call site to
// regress (#79). The serving member is always THIS node, so MemberId is the
// local identity's id and ClusterId the storage cluster's id.
func (s *RPCServer) ClientServerOptions() []grpc.ServerOption {
	return []grpc.ServerOption{
		grpc.StatsHandler(newEtcdClientGRPCBytesStatsHandler(s.metricCli)),
		grpc.ChainUnaryInterceptor(s.rejectNativeClientAuthBypassUnary, s.admitUnary, s.stampUnary),
		grpc.ChainStreamInterceptor(s.rejectNativeClientAuthBypassStream, s.admitStream, s.stampStream),
		grpc.MaxRecvMsgSize(int(s.maxRequestBytes + grpcOverheadBytes)),
	}
}

// The legacy kubebrain-client services share the public listener with etcd.
// They predate etcd authentication and do not carry etcd users, roles, or key
// permissions, so serving them while auth is enabled would provide an
// unauthenticated path to the same data. Keep the backward-compatible surface
// while auth is disabled, but fail closed as soon as AuthEnable commits. This
// interceptor is deliberately client-only; peer RPCs use PeerServerOptions and
// remain available for replica coordination.
func (s *RPCServer) rejectNativeClientAuthBypassUnary(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
	if !isNativeBrainMethod(info.FullMethod) {
		return handler(ctx, req)
	}
	s.nativeAuthBoundary.RLock()
	defer s.nativeAuthBoundary.RUnlock()
	if err := s.rejectNativeClientAuthBypass(ctx); err != nil {
		return nil, err
	}
	return handler(ctx, req)
}

func (s *RPCServer) rejectNativeClientAuthBypassStream(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
	if !isNativeBrainMethod(info.FullMethod) {
		return handler(srv, ss)
	}
	if err := s.rejectNativeClientAuthBypass(ss.Context()); err != nil {
		return err
	}

	ctx, cancel := context.WithCancelCause(ss.Context())
	defer cancel(nil)
	done := make(chan struct{})
	go func() {
		ticker := time.NewTicker(nativeAuthBoundaryPollInterval)
		defer ticker.Stop()
		for {
			select {
			case <-done:
				return
			case <-ctx.Done():
				return
			case <-ticker.C:
				if err := s.rejectNativeClientAuthBypass(ctx); err != nil {
					cancel(err)
					return
				}
			}
		}
	}()
	err := handler(srv, &serverStreamWithContext{ServerStream: ss, ctx: ctx})
	close(done)
	if cause := context.Cause(ctx); cause != nil && !errors.Is(cause, context.Canceled) {
		return cause
	}
	return err
}

const nativeAuthBoundaryPollInterval = 100 * time.Millisecond

func (s *RPCServer) rejectNativeClientAuthBypass(ctx context.Context) error {
	snapshot, err := s.tokens.snapshots.current(ctx)
	if err != nil {
		return authGRPCError(err)
	}
	if snapshot.Config.Enabled {
		return rpctypes.ErrGRPCPermissionDenied
	}
	return nil
}

func isNativeBrainMethod(method string) bool {
	return strings.HasPrefix(method, "/Read/") ||
		strings.HasPrefix(method, "/Write/") ||
		strings.HasPrefix(method, "/Watch/")
}

// PeerServerOptions keeps response identity and request-size behavior identical
// on the peer listener, but reserves it from public-client overload admission.
func (s *RPCServer) PeerServerOptions() []grpc.ServerOption {
	return []grpc.ServerOption{
		grpc.ChainUnaryInterceptor(s.requireLeaderUnary, s.stampUnary),
		grpc.ChainStreamInterceptor(s.requireLeaderStream, s.stampStream),
		grpc.MaxRecvMsgSize(int(s.maxRequestBytes + grpcOverheadBytes)),
	}
}

func (s *RPCServer) hasKnownLeader() bool {
	if availability, ok := s.peers.(interface{ HasLeader() bool }); ok {
		return availability.HasLeader()
	}
	return s.peers.IsLeader() || election.IsLeaderKnown(s.peers.GetLeaderInfo())
}

func requireLeader(ctx context.Context) bool {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return false
	}
	values := md.Get(rpctypes.MetadataRequireLeaderKey)
	return len(values) > 0 && values[0] == rpctypes.MetadataHasLeader
}

func clientAPIVersionFromContext(ctx context.Context) (string, error) {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return "unknown", nil
	}
	values := md.Get(rpctypes.MetadataClientAPIVersionKey)
	if len(values) > 0 && !utf8.ValidString(values[0]) {
		return "", rpctypes.ErrGRPCInvalidClientAPIVersion
	}
	if len(values) > 0 {
		return values[0], nil
	}
	return "unknown", nil
}

func validateClientAPIVersion(ctx context.Context) error {
	_, err := clientAPIVersionFromContext(ctx)
	return err
}

func (s *RPCServer) observeClientRequest(ctx context.Context, requestType, fullMethod string) error {
	clientAPIVersion, err := clientAPIVersionFromContext(ctx)
	if err != nil {
		return err
	}
	if !isNativeBrainMethod(fullMethod) {
		emitEtcdClientRequestCounter(s.metricCli, requestType, clientAPIVersion, 1)
	}
	return nil
}

func (s *RPCServer) requireLeaderUnary(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
	if s.peerLeadershipDrained.Load() {
		return nil, proxyprotocol.ErrPeerDrainedBeforeAdmission
	}
	unlock, err := s.tryLeadershipUnaryAdmission(proxyprotocol.ErrPeerDrainedBeforeAdmission)
	if err != nil {
		return nil, err
	}
	defer unlock()
	if s.peerLeadershipDrained.Load() {
		return nil, proxyprotocol.ErrPeerDrainedBeforeAdmission
	}
	ctx = context.WithValue(ctx, peerRequestContextKey{}, true)
	if err = validateClientAPIVersion(ctx); err != nil {
		return nil, err
	}
	if requireLeader(ctx) && !s.hasKnownLeader() {
		return nil, rpctypes.ErrGRPCNoLeader
	}
	return handler(ctx, req)
}

func (s *RPCServer) requireLeaderStream(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
	ss = &serverStreamWithContext{
		ServerStream: ss,
		ctx:          context.WithValue(ss.Context(), peerRequestContextKey{}, true),
	}
	return s.monitorLeadershipStreamDrain(srv, ss, proxyprotocol.ErrPeerStreamDrained,
		func(srv any, draining grpc.ServerStream) error {
			if err := validateClientAPIVersion(draining.Context()); err != nil {
				return err
			}
			if requireLeader(draining.Context()) && !s.hasKnownLeader() {
				return rpctypes.ErrGRPCNoLeader
			}
			if requireLeader(draining.Context()) {
				return s.monitorRequiredLeaderStream(srv, draining, info, handler)
			}
			return handler(srv, draining)
		})
}

// peerRequestContextKey is injected only by PeerServerOptions. Metadata alone
// is client-controlled, so internal continuation markers must always be paired
// with this listener provenance before they can affect authorization.
type peerRequestContextKey struct{}

func isPeerRequest(ctx context.Context) bool {
	peerRequest, _ := ctx.Value(peerRequestContextKey{}).(bool)
	return peerRequest
}

const requireLeaderPollInterval = 100 * time.Millisecond

type serverStreamWithContext struct {
	grpc.ServerStream
	ctx context.Context
}

func (s *serverStreamWithContext) Context() context.Context {
	return s.ctx
}

// SendMsg and RecvMsg on grpc.ServerStream wait on the transport context
// captured before interceptors wrap the stream. Canceling only Context()
// therefore does not wake a handler blocked on flow control or its next client
// message. Race the underlying I/O with the monitored context so
// require-leader streams can return ErrNoLeader even when either side is idle.
func (s *serverStreamWithContext) SendMsg(message any) error {
	sent := make(chan error, 1)
	go func() {
		sent <- s.ServerStream.SendMsg(message)
	}()
	select {
	case err := <-sent:
		return err
	case <-s.ctx.Done():
		return context.Cause(s.ctx)
	}
}

func (s *serverStreamWithContext) RecvMsg(message any) error {
	received := make(chan error, 1)
	go func() {
		received <- s.ServerStream.RecvMsg(message)
	}()
	select {
	case err := <-received:
		return err
	case <-s.ctx.Done():
		return context.Cause(s.ctx)
	}
}

func (s *RPCServer) monitorRequiredLeaderStream(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
	ctx, cancel := context.WithCancelCause(ss.Context())
	defer cancel(nil)
	done := make(chan struct{})
	go func() {
		ticker := time.NewTicker(requireLeaderPollInterval)
		defer ticker.Stop()
		for {
			select {
			case <-done:
				return
			case <-ctx.Done():
				return
			case <-ticker.C:
				if !s.hasKnownLeader() {
					cancel(rpctypes.ErrGRPCNoLeader)
					return
				}
			}
		}
	}()
	err := handler(srv, &serverStreamWithContext{ServerStream: ss, ctx: ctx})
	close(done)
	if errors.Is(context.Cause(ctx), rpctypes.ErrGRPCNoLeader) {
		return rpctypes.ErrGRPCNoLeader
	}
	return err
}

func (s *RPCServer) acquireRequest(method, kind string) bool {
	limit := int64(s.maxRequestsInFlight)
	if limit == 0 {
		return true
	}
	admissionLimit := limit
	if isPriorityAdmissionMethod(method) {
		admissionLimit += int64(priorityAdmissionReserve(s.maxRequestsInFlight))
	}
	s.admissionMu.Lock()
	defer s.admissionMu.Unlock()
	if s.requestsInFlight >= admissionLimit {
		s.metricCli.EmitCounter("grpc.server.admission.rejected", 1,
			metrics.Tag("method", method), metrics.Tag("kind", kind))
		emitClientAdmissionRejection(s.metricCli, clientAdmissionGuardConcurrency)
		return false
	}
	if s.requestsInFlight >= limit {
		s.metricCli.EmitCounter("grpc.server.admission.priority_admitted", 1,
			metrics.Tag("method", method), metrics.Tag("kind", kind))
	}
	s.requestsInFlight++
	s.metricCli.EmitGauge("grpc.server.admission.inflight", s.requestsInFlight)
	return true
}

func (s *RPCServer) releaseRequest() {
	s.admissionMu.Lock()
	defer s.admissionMu.Unlock()
	s.requestsInFlight--
	s.metricCli.EmitGauge("grpc.server.admission.inflight", s.requestsInFlight)
}

func (s *RPCServer) allowRequestRate(method, kind string) bool {
	if s.requestRateLimiter == nil {
		return true
	}
	if s.requestRateLimiter.Allow() {
		return true
	}
	if isPriorityAdmissionMethod(method) &&
		s.priorityRateLimiter != nil &&
		s.priorityRateLimiter.Allow() {
		s.metricCli.EmitCounter("grpc.server.rate_limit.priority_admitted", 1,
			metrics.Tag("method", method), metrics.Tag("kind", kind))
		return true
	}
	s.metricCli.EmitCounter("grpc.server.rate_limit.rejected", 1,
		metrics.Tag("method", method), metrics.Tag("kind", kind))
	emitClientAdmissionRejection(s.metricCli, clientAdmissionGuardRate)
	return false
}

func isPriorityAdmissionMethod(method string) bool {
	return method == etcdserverpb.Lease_LeaseRevoke_FullMethodName
}

// priorityAdmissionReserve is a bounded ten-percent reserve that ordinary
// traffic cannot consume. At least one revoke remains admissible for any
// enabled limit, while revokes still fail closed once their reserve is full.
func priorityAdmissionReserve(limit uint32) uint32 {
	reserve := limit / 10
	if limit%10 != 0 {
		reserve++
	}
	if reserve == 0 && limit != 0 {
		return 1
	}
	return reserve
}

func (s *RPCServer) admitUnary(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
	unlock, err := s.tryLeadershipUnaryAdmission(proxyprotocol.ErrClientDrainedBeforeAdmission)
	if err != nil {
		return nil, err
	}
	defer unlock()
	if err := s.observeClientRequest(ctx, "unary", info.FullMethod); err != nil {
		return nil, err
	}
	if requireLeader(ctx) && !s.hasKnownLeader() {
		return nil, rpctypes.ErrGRPCNoLeader
	}
	if !s.allowRequestRate(info.FullMethod, "unary") {
		return nil, rpctypes.ErrGRPCRequestTooManyRequests
	}
	if !s.acquireRequest(info.FullMethod, "unary") {
		return nil, rpctypes.ErrGRPCRequestTooManyRequests
	}
	if s.maxRequestsInFlight != 0 {
		defer s.releaseRequest()
	}
	return handler(ctx, req)
}

// tryLeadershipUnaryAdmission acquires the read side only when doing so cannot
// wait behind a leadership drain. The pre-check avoids normal drain traffic;
// TryRLock closes the race with a writer that is already pending; and the
// post-check closes the race where the drain fence was published immediately
// after TryRLock succeeded. Returning the listener-specific before-admission
// status preserves mutable write-at-most-once semantics while letting clientv3
// retry on another public endpoint without spending the handoff duration here.
func (s *RPCServer) tryLeadershipUnaryAdmission(drainErr error) (func(), error) {
	if s.leadershipDrainInProgress.Load() {
		return nil, drainErr
	}
	if !s.leadershipDrainBoundary.TryRLock() {
		return nil, drainErr
	}
	if s.leadershipDrainInProgress.Load() {
		s.leadershipDrainBoundary.RUnlock()
		return nil, drainErr
	}
	return s.leadershipDrainBoundary.RUnlock, nil
}

func (s *RPCServer) admitStream(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
	if err := s.observeClientRequest(ss.Context(), "stream", info.FullMethod); err != nil {
		return err
	}
	if requireLeader(ss.Context()) && !s.hasKnownLeader() {
		return rpctypes.ErrGRPCNoLeader
	}
	if !s.acquireRequest(info.FullMethod, "stream") {
		return rpctypes.ErrGRPCRequestTooManyRequests
	}
	if s.maxRequestsInFlight != 0 {
		defer s.releaseRequest()
	}
	return s.monitorLeadershipStreamDrain(srv, ss, rpctypes.ErrGRPCStopped, func(srv any, draining grpc.ServerStream) error {
		if requireLeader(draining.Context()) {
			return s.monitorRequiredLeaderStream(srv, draining, info, func(srv any, monitored grpc.ServerStream) error {
				return handler(srv, &rateLimitedServerStream{
					ServerStream: monitored,
					server:       s,
					method:       info.FullMethod,
				})
			})
		}
		return handler(srv, &rateLimitedServerStream{
			ServerStream: draining,
			server:       s,
			method:       info.FullMethod,
		})
	})
}

func (s *RPCServer) monitorLeadershipStreamDrain(srv any, ss grpc.ServerStream, drainErr error, handler grpc.StreamHandler) error {
	drain, drained := s.leadershipStreamDrainState()
	if drained {
		return drainErr
	}

	ctx, cancel := context.WithCancelCause(ss.Context())
	defer cancel(nil)
	done := make(chan struct{})
	go func() {
		select {
		case <-done:
		case <-ctx.Done():
		case <-drain:
			cancel(drainErr)
		}
	}()
	err := handler(srv, &serverStreamWithContext{ServerStream: ss, ctx: ctx})
	close(done)
	if errors.Is(context.Cause(ctx), drainErr) {
		return drainErr
	}
	return err
}

type rateLimitedServerStream struct {
	grpc.ServerStream
	server *RPCServer
	method string
}

func (s *rateLimitedServerStream) RecvMsg(message any) error {
	if !s.server.allowRequestRate(s.method, "stream_message") {
		return rpctypes.ErrGRPCRequestTooManyRequests
	}
	return s.ServerStream.RecvMsg(message)
}

func (s *RPCServer) localMemberID() uint64 {
	return s.memberIDForPeerIdentity(s.backend.GetResourceLock().Identity())
}

func stampHeader(reply any, clusterID, memberID, raftTerm uint64) {
	var header *etcdserverpb.ResponseHeader
	switch response := reply.(type) {
	case *etcdserverpb.RangeStreamResponse:
		if response.GetRangeResponse() != nil {
			header = response.GetRangeResponse().GetHeader()
		}
	case interface {
		GetHeader() *etcdserverpb.ResponseHeader
	}:
		header = response.GetHeader()
	}
	// Most etcd responses construct a non-nil Header. Defragment intentionally
	// returns an empty response with nil Header, matching the reference server;
	// do not synthesize metadata for response types that omit it.
	if header != nil {
		header.ClusterId = clusterID
		header.MemberId = memberID
		header.RaftTerm = raftTerm
	}
}

func (s *RPCServer) responseRaftTerm(ctx context.Context) (uint64, error) {
	if term := s.peers.CurrentLeadershipTerm(); term != 0 {
		return term, nil
	}
	term, err := s.peers.LeadershipTerm(ctx)
	if err != nil {
		return 0, retryableCoordinationStatusErr(err)
	}
	return term, nil
}

func (s *RPCServer) stampUnary(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (resp any, retErr error) {
	if message, ok := req.(proto.Message); ok && uint(proto.Size(message)) > s.maxRequestBytes {
		return nil, rpctypes.ErrGRPCRequestTooLarge
	}
	handlerCtx := ctx
	if shouldApplyUnaryServerAttemptTimeout(info.FullMethod, req) {
		var cancel context.CancelFunc
		handlerCtx, cancel = withUnaryRequestTimeout(ctx)
		defer cancel()
	}
	if requestType, ok := unaryRequestDurationType(info.FullMethod, req); ok {
		started := time.Now()
		defer func() { emitEtcdRequestDuration(s.metricCli, requestType, time.Since(started), retErr) }()
	}
	resp, retErr = handler(handlerCtx, req)
	if retErr == nil {
		var term uint64
		if statusResponse, ok := resp.(*etcdserverpb.StatusResponse); ok {
			// Status already snapshots the Raft term for its response body. Reuse
			// that value so a concurrent leadership change cannot make the body
			// and response header describe different terms.
			term = statusResponse.RaftTerm
		} else {
			var termErr error
			term, termErr = s.responseRaftTerm(ctx)
			if termErr != nil {
				retErr = termErr
				return nil, retErr
			}
		}
		stampHeader(resp, s.backend.ClusterID(), s.localMemberID(), term)
	}
	if isDedicatedConcurrencyMethod(info.FullMethod) {
		return resp, retErr
	}
	retErr = authGRPCError(retErr)
	return resp, retErr
}

func unaryRequestDurationType(fullMethod string, request any) (string, bool) {
	switch fullMethod {
	case etcdserverpb.KV_Range_FullMethodName:
		return "Range", true
	case etcdserverpb.KV_Put_FullMethodName:
		return "Put", true
	case etcdserverpb.KV_DeleteRange_FullMethodName:
		return "DeleteRange", true
	case etcdserverpb.KV_Txn_FullMethodName:
		if txn, ok := request.(*etcdserverpb.TxnRequest); ok && txnIsReadonly(txn) {
			return "ReadonlyTxn", true
		}
		return "Txn", true
	case etcdserverpb.KV_Compact_FullMethodName:
		return "Compaction", true
	case etcdserverpb.Lease_LeaseGrant_FullMethodName:
		return "LeaseGrant", true
	case etcdserverpb.Lease_LeaseRevoke_FullMethodName:
		return "LeaseRevoke", true
	case etcdserverpb.Maintenance_Alarm_FullMethodName:
		return "Alarm", true
	case etcdserverpb.Auth_Authenticate_FullMethodName:
		return "Authenticate", true
	case etcdserverpb.Auth_AuthEnable_FullMethodName:
		return "AuthEnable", true
	case etcdserverpb.Auth_AuthDisable_FullMethodName:
		return "AuthDisable", true
	case etcdserverpb.Auth_AuthStatus_FullMethodName:
		return "AuthStatus", true
	case etcdserverpb.Auth_UserAdd_FullMethodName:
		return "AuthUserAdd", true
	case etcdserverpb.Auth_UserGet_FullMethodName:
		return "AuthUserGet", true
	case etcdserverpb.Auth_UserList_FullMethodName:
		return "AuthUserList", true
	case etcdserverpb.Auth_UserDelete_FullMethodName:
		return "AuthUserDelete", true
	case etcdserverpb.Auth_UserChangePassword_FullMethodName:
		return "AuthUserChangePassword", true
	case etcdserverpb.Auth_UserGrantRole_FullMethodName:
		return "AuthUserGrantRole", true
	case etcdserverpb.Auth_UserRevokeRole_FullMethodName:
		return "AuthUserRevokeRole", true
	case etcdserverpb.Auth_RoleAdd_FullMethodName:
		return "AuthRoleAdd", true
	case etcdserverpb.Auth_RoleGet_FullMethodName:
		return "AuthRoleGet", true
	case etcdserverpb.Auth_RoleList_FullMethodName:
		return "AuthRoleList", true
	case etcdserverpb.Auth_RoleDelete_FullMethodName:
		return "AuthRoleDelete", true
	case etcdserverpb.Auth_RoleGrantPermission_FullMethodName:
		return "AuthRoleGrantPermission", true
	case etcdserverpb.Auth_RoleRevokePermission_FullMethodName:
		return "AuthRoleRevokePermission", true
	default:
		return "", false
	}
}

func (s *RPCServer) stampStream(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
	err := handler(srv, &stampedServerStream{ServerStream: ss, s: s})
	if isDedicatedConcurrencyMethod(info.FullMethod) {
		return err
	}
	return authGRPCError(err)
}

var dedicatedConcurrencyMethods = grpcServiceFullMethods(
	v3lockpb.Lock_ServiceDesc,
	v3electionpb.Election_ServiceDesc,
)

var unaryServerAttemptTimeoutMethods = func() map[string]struct{} {
	// Every upstream Auth RPC is dispatched through raftRequest. Alarm is the
	// corresponding Maintenance RPC. KV, Compact, and Lease mutations already
	// establish the same attempt budget in their handlers; keep ordinary Range,
	// Status, and other non-Raft reads on the caller's context.
	methods := grpcServiceFullMethods(etcdserverpb.Auth_ServiceDesc)
	methods[etcdserverpb.Maintenance_Alarm_FullMethodName] = struct{}{}
	return methods
}()

func isDedicatedConcurrencyMethod(method string) bool {
	_, ok := dedicatedConcurrencyMethods[method]
	return ok
}

func isUnaryServerAttemptTimeoutMethod(method string) bool {
	_, ok := unaryServerAttemptTimeoutMethods[method]
	return ok
}

func shouldApplyUnaryServerAttemptTimeout(method string, request any) bool {
	if !isUnaryServerAttemptTimeoutMethod(method) {
		return false
	}
	// KubeBrain validates every uncompacted durable transaction witness before
	// reopening writes after a CORRUPT alarm. That work is intentionally stronger
	// than upstream's constant-time alarm mutation and can exceed one ordinary
	// raft-request attempt as history grows. Clipping it to unaryRpcTimeout makes
	// every client retry restart the scan from the beginning, so a healthy cluster
	// can become permanently impossible to disarm. Preserve the caller's deadline
	// for this one operator recovery operation; all other Alarm/Auth attempts keep
	// the normal server-side bound.
	if method == etcdserverpb.Maintenance_Alarm_FullMethodName {
		alarm, ok := request.(*etcdserverpb.AlarmRequest)
		return !ok || alarm.GetAction() != etcdserverpb.AlarmRequest_DEACTIVATE ||
			alarm.GetAlarm() != etcdserverpb.AlarmType_CORRUPT
	}
	return true
}

func grpcServiceFullMethods(services ...grpc.ServiceDesc) map[string]struct{} {
	methods := make(map[string]struct{})
	for _, service := range services {
		for _, method := range service.Methods {
			methods["/"+service.ServiceName+"/"+method.MethodName] = struct{}{}
		}
		for _, stream := range service.Streams {
			methods["/"+service.ServiceName+"/"+stream.StreamName] = struct{}{}
		}
	}
	return methods
}

func authGRPCError(err error) error {
	switch {
	case err == nil:
		return nil
	case errors.Is(err, storage.ErrUncertainResult):
		// The storage commit may already be durable. Return etcd's timeout
		// contract so clients treat it as a retryable, outcome-unknown write;
		// the backend resolves the event-log markers asynchronously.
		return rpctypes.ErrGRPCTimeout
	case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
		// Storage and client-go may wrap the caller context while adding Region or
		// transaction diagnostics. Preserve the gRPC cancellation/deadline contract
		// before classifying retryable backend failures such as ErrUnavailable.
		return status.FromContextError(err).Err()
	case errors.Is(err, rpctypes.ErrRootUserNotExist):
		return rpctypes.ErrGRPCRootUserNotExist
	case errors.Is(err, rpctypes.ErrRootRoleNotExist):
		return rpctypes.ErrGRPCRootRoleNotExist
	case errors.Is(err, rpctypes.ErrUserAlreadyExist):
		return rpctypes.ErrGRPCUserAlreadyExist
	case errors.Is(err, rpctypes.ErrUserEmpty):
		return rpctypes.ErrGRPCUserEmpty
	case errors.Is(err, rpctypes.ErrUserNotFound):
		return rpctypes.ErrGRPCUserNotFound
	case errors.Is(err, rpctypes.ErrRoleAlreadyExist):
		return rpctypes.ErrGRPCRoleAlreadyExist
	case errors.Is(err, rpctypes.ErrRoleNotFound):
		return rpctypes.ErrGRPCRoleNotFound
	case errors.Is(err, rpctypes.ErrRoleEmpty):
		return rpctypes.ErrGRPCRoleEmpty
	case errors.Is(err, rpctypes.ErrAuthFailed):
		return rpctypes.ErrGRPCAuthFailed
	case errors.Is(err, rpctypes.ErrPermissionDenied):
		return rpctypes.ErrGRPCPermissionDenied
	case errors.Is(err, rpctypes.ErrRoleNotGranted):
		return rpctypes.ErrGRPCRoleNotGranted
	case errors.Is(err, rpctypes.ErrPermissionNotGranted):
		return rpctypes.ErrGRPCPermissionNotGranted
	case errors.Is(err, rpctypes.ErrAuthNotEnabled):
		return rpctypes.ErrGRPCAuthNotEnabled
	case errors.Is(err, rpctypes.ErrInvalidAuthToken):
		return rpctypes.ErrGRPCInvalidAuthToken
	case errors.Is(err, rpctypes.ErrInvalidAuthMgmt):
		return rpctypes.ErrGRPCInvalidAuthMgmt
	case errors.Is(err, rpctypes.ErrAuthOldRevision):
		return rpctypes.ErrGRPCAuthOldRevision
	case errors.Is(err, errInvalidAuthMetadata), errors.Is(err, errInvalidLeaseMetadata),
		errors.Is(err, backend.ErrInvalidAlarmMetadata), errors.Is(err, backend.ErrInvalidQuotaMetadata),
		errors.Is(err, backend.ErrInvalidMVCCMetadata):
		// Upstream refuses to recover malformed auth/lease/alarm/MVCC backend
		// state; a clean but undecodable quota checkpoint has the same durable
		// shape.
		// KubeBrain can encounter the same durable corruption during live TiKV
		// reads, so expose an integrity failure rather than grpc-go's fallback
		// Unknown. Snapshot converts these to its narrower FailedPrecondition
		// contract before reaching this shared interceptor.
		return status.Error(codes.DataLoss, err.Error())
	case errors.Is(err, backend.ErrQuotaUninitialized):
		return status.Error(codes.Unavailable, err.Error())
	case errors.Is(err, backend.ErrRevisionExhausted):
		return status.Error(codes.ResourceExhausted, err.Error())
	case errors.Is(err, errAuthRevisionExhausted):
		return status.Error(codes.ResourceExhausted, err.Error())
	case errors.Is(err, storage.ErrUnavailable):
		return status.Error(codes.Unavailable, err.Error())
	case errors.Is(err, storage.ErrKeyTooLarge):
		return status.Error(codes.ResourceExhausted, err.Error())
	case isTiKVLoadRegionDeadlineExceeded(err):
		// This failure occurs while locating a Region, before a mutation can be
		// submitted. Preserve etcd's per-attempt deadline contract so clientv3
		// can safely spend the remainder of its outer retry budget. Mapping it to
		// Unavailable makes mutable RPCs fail immediately by design.
		return status.Error(codes.DeadlineExceeded, err.Error())
	case isRetryableBackendTransportError(err):
		// TiKV client-go exposes some retryable region/connection failures only
		// as untyped errors. Do not leak them as gRPC Unknown: etcd clients retry
		// Unavailable, while Unknown is commonly treated as a permanent failure.
		return status.Error(codes.Unavailable, err.Error())
	default:
		return err
	}
}

func isTiKVLoadRegionDeadlineExceeded(err error) bool {
	for current := err; current != nil; current = errors.Unwrap(current) {
		if status.Code(current) != codes.Unknown {
			return false
		}
		if errors.Unwrap(current) != nil {
			continue
		}
		cause := current.Error()
		if !strings.HasPrefix(cause, "loadRegion from PD failed, key: ") &&
			!strings.HasPrefix(cause, "loadRegion from PD failed, regionID: ") {
			return false
		}
		const marker = ", err: rpc error: code = DeadlineExceeded desc = "
		return strings.Contains(cause, marker)
	}
	return false
}

func isRetryableBackendTransportError(err error) bool {
	for current := err; current != nil; current = errors.Unwrap(current) {
		// A wrapper can hide GRPCStatus from status.Code(err). Preserve any
		// authoritative non-Unknown status found deeper in the chain instead of
		// reclassifying its message as a TiKV transport reason.
		if status.Code(current) != codes.Unknown {
			return false
		}
		if errors.Unwrap(current) != nil {
			continue
		}
		cause := current.Error()
		return cause == "no available connections" || strings.HasPrefix(cause, "epoch_not_match:") ||
			isRetryableTiKVLoadRegionError(cause)
	}
	return false
}

func isRetryableTiKVLoadRegionError(cause string) bool {
	if !strings.HasPrefix(cause, "loadRegion from PD failed, key: ") &&
		!strings.HasPrefix(cause, "loadRegion from PD failed, regionID: ") {
		return false
	}
	const marker = ", err: rpc error: code = "
	markerIndex := strings.LastIndex(cause, marker)
	if markerIndex < 0 {
		return false
	}
	grpcCause := cause[markerIndex+len(marker):]
	return strings.HasPrefix(grpcCause, "DeadlineExceeded desc = ") ||
		strings.HasPrefix(grpcCause, "Unavailable desc = ")
}

type stampedServerStream struct {
	grpc.ServerStream
	s *RPCServer
}

func (w *stampedServerStream) RecvMsg(m any) error {
	if err := w.ServerStream.RecvMsg(m); err != nil {
		return err
	}
	if message, ok := m.(proto.Message); ok && uint(proto.Size(message)) > w.s.maxRequestBytes {
		return rpctypes.ErrGRPCRequestTooLarge
	}
	return nil
}

func (w *stampedServerStream) SendMsg(m any) error {
	term, err := w.s.responseRaftTerm(w.Context())
	if err != nil {
		return err
	}
	stampHeader(m, w.s.backend.ClusterID(), w.s.localMemberID(), term)
	return w.ServerStream.SendMsg(m)
}
