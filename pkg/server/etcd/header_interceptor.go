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
	"hash/crc32"

	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"google.golang.org/grpc"
)

// HeaderStampServerOptions returns gRPC interceptors that stamp ClusterId and
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
func (s *RPCServer) HeaderStampServerOptions() []grpc.ServerOption {
	return []grpc.ServerOption{
		grpc.ChainUnaryInterceptor(s.stampUnary),
		grpc.ChainStreamInterceptor(s.stampStream),
	}
}

func (s *RPCServer) localMemberID() uint64 {
	return uint64(crc32.ChecksumIEEE([]byte(s.backend.GetResourceLock().Identity())))
}

func stampHeader(reply any, clusterID, memberID uint64) {
	r, ok := reply.(interface {
		GetHeader() *etcdserverpb.ResponseHeader
	})
	if !ok {
		return
	}
	// Every etcd response type constructs a non-nil Header (even the "empty"
	// lease headers are &ResponseHeader{}); a nil Header cannot be filled here
	// without reflecting into the reply, and none occur in practice.
	if h := r.GetHeader(); h != nil {
		h.ClusterId = clusterID
		h.MemberId = memberID
	}
}

func (s *RPCServer) stampUnary(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
	resp, err := handler(ctx, req)
	if err == nil {
		stampHeader(resp, s.backend.ClusterID(), s.localMemberID())
	}
	return resp, err
}

func (s *RPCServer) stampStream(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
	return handler(srv, &stampedServerStream{ServerStream: ss, s: s})
}

type stampedServerStream struct {
	grpc.ServerStream
	s *RPCServer
}

func (w *stampedServerStream) SendMsg(m any) error {
	stampHeader(m, w.s.backend.ClusterID(), w.s.localMemberID())
	return w.ServerStream.SendMsg(m)
}
