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
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"

	"github.com/kubewharf/kubebrain/pkg/metrics"
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
		grpc.ChainUnaryInterceptor(s.admitUnary, s.stampUnary),
		grpc.ChainStreamInterceptor(s.admitStream, s.stampStream),
		grpc.MaxRecvMsgSize(int(s.maxRequestBytes + grpcOverheadBytes)),
	}
}

// PeerServerOptions keeps response identity and request-size behavior identical
// on the peer listener, but reserves it from public-client overload admission.
func (s *RPCServer) PeerServerOptions() []grpc.ServerOption {
	return []grpc.ServerOption{
		grpc.ChainUnaryInterceptor(s.stampUnary),
		grpc.ChainStreamInterceptor(s.stampStream),
		grpc.MaxRecvMsgSize(int(s.maxRequestBytes + grpcOverheadBytes)),
	}
}

func (s *RPCServer) acquireRequest(method, kind string) bool {
	limit := int64(s.maxRequestsInFlight)
	if limit == 0 {
		return true
	}
	s.admissionMu.Lock()
	defer s.admissionMu.Unlock()
	if s.requestsInFlight >= limit {
		s.metricCli.EmitCounter("grpc.server.admission.rejected", 1,
			metrics.Tag("method", method), metrics.Tag("kind", kind))
		return false
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
	s.metricCli.EmitCounter("grpc.server.rate_limit.rejected", 1,
		metrics.Tag("method", method), metrics.Tag("kind", kind))
	return false
}

func (s *RPCServer) admitUnary(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
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

func (s *RPCServer) admitStream(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
	if !s.acquireRequest(info.FullMethod, "stream") {
		return rpctypes.ErrGRPCRequestTooManyRequests
	}
	if s.maxRequestsInFlight != 0 {
		defer s.releaseRequest()
	}
	return handler(srv, &rateLimitedServerStream{
		ServerStream: ss,
		server:       s,
		method:       info.FullMethod,
	})
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
	return uint64(crc32.ChecksumIEEE([]byte(s.backend.GetResourceLock().Identity())))
}

func stampHeader(reply any, clusterID, memberID, raftTerm uint64) {
	r, ok := reply.(interface {
		GetHeader() *etcdserverpb.ResponseHeader
	})
	if !ok {
		return
	}
	// Most etcd responses construct a non-nil Header. Defragment intentionally
	// returns an empty response with nil Header, matching the reference server;
	// do not synthesize metadata for response types that omit it.
	if h := r.GetHeader(); h != nil {
		h.ClusterId = clusterID
		h.MemberId = memberID
		h.RaftTerm = raftTerm
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

func (s *RPCServer) stampUnary(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
	if message, ok := req.(proto.Message); ok && uint(proto.Size(message)) > s.maxRequestBytes {
		return nil, rpctypes.ErrGRPCRequestTooLarge
	}
	resp, err := handler(ctx, req)
	if err == nil {
		term, termErr := s.responseRaftTerm(ctx)
		if termErr != nil {
			return nil, termErr
		}
		stampHeader(resp, s.backend.ClusterID(), s.localMemberID(), term)
	}
	return resp, authGRPCError(err)
}

func (s *RPCServer) stampStream(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
	return authGRPCError(handler(srv, &stampedServerStream{ServerStream: ss, s: s}))
}

func authGRPCError(err error) error {
	switch {
	case err == nil:
		return nil
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
	default:
		return err
	}
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
