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

package etcd

import (
	"context"
	"fmt"
	"net"
	"strconv"
	"strings"

	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/kubewharf/kubebrain/pkg/backend/election"
)

// MemberList lists the current cluster membership.
func (s *RPCServer) MemberList(context.Context, *etcdserverpb.MemberListRequest) (*etcdserverpb.MemberListResponse, error) {
	s.metricCli.EmitCounter("member.list", 1)
	addresses := []string{s.backend.GetResourceLock().Identity(), s.peers.GetLeaderInfo()}
	members := make([]*etcdserverpb.Member, 0, len(addresses))
	seen := make(map[uint64]struct{}, len(addresses))
	for _, address := range addresses {
		if !election.IsLeaderKnown(address) {
			continue
		}
		id := s.memberIDFromAddress(address)
		if _, ok := seen[id]; ok {
			continue
		}
		seen[id] = struct{}{}
		members = append(members, &etcdserverpb.Member{
			ID:       id,
			Name:     address,
			PeerURLs: []string{memberURLFromAddress(address)},
			// ClientURLs must be dialable BY CLIENTS: clientv3's Sync/AutoSync
			// replaces the caller's endpoint list with them wholesale, so the
			// legacy identity-derived URL (http://host:PEER-port) sent TLS
			// clients to a plaintext port that does not serve KV.
			ClientURLs: []string{s.clientURLFromAddress(address)},
			IsLearner:  false,
		})
	}
	return &etcdserverpb.MemberListResponse{
		Header: &etcdserverpb.ResponseHeader{
			ClusterId: s.memberIDFromAddress(s.peers.GetLeaderInfo()),
			MemberId:  s.memberIDFromAddress(s.backend.GetResourceLock().Identity()),
			Revision:  int64(s.backend.GetCurrentRevision()),
		},
		Members: members,
	}, nil
}

// MemberAdd adds a member into the cluster.
func (s *RPCServer) MemberAdd(context.Context, *etcdserverpb.MemberAddRequest) (*etcdserverpb.MemberAddResponse, error) {
	s.metricCli.EmitCounter("member.add", 1)
	return nil, status.Error(codes.Unimplemented, "member add is not supported")
}

// MemberRemove removes an existing member from the cluster.
func (s *RPCServer) MemberRemove(context.Context, *etcdserverpb.MemberRemoveRequest) (*etcdserverpb.MemberRemoveResponse, error) {
	s.metricCli.EmitCounter("member.remove", 1)
	return nil, status.Error(codes.Unimplemented, "member remove is not supported")
}

// MemberUpdate updates the peer addresses of the member.
func (s *RPCServer) MemberUpdate(context.Context, *etcdserverpb.MemberUpdateRequest) (*etcdserverpb.MemberUpdateResponse, error) {
	s.metricCli.EmitCounter("member.update", 1)
	return nil, status.Error(codes.Unimplemented, "member update is not supported")
}

// MemberPromote promotes a member from raft learner (non-voting) to raft voting member.
func (s *RPCServer) MemberPromote(context.Context, *etcdserverpb.MemberPromoteRequest) (*etcdserverpb.MemberPromoteResponse, error) {
	s.metricCli.EmitCounter("member.promote", 1)
	return nil, status.Error(codes.Unimplemented, "member promote is not supported")
}

func memberURLFromAddress(address string) string {
	if strings.Contains(address, "://") {
		return address
	}
	return "http://" + address
}

// clientURLFromAddress rewrites a peer identity (host:peerPort) into the
// member's client endpoint using the advertised client port and TLS scheme
// (SetAdvertiseClientInfo). Falls back to the legacy peer-derived URL when the
// advertise info is unset or the identity does not parse.
func (s *RPCServer) clientURLFromAddress(address string) string {
	if s.advertiseClientPort == 0 || strings.Contains(address, "://") {
		return memberURLFromAddress(address)
	}
	host, _, err := net.SplitHostPort(address)
	if err != nil {
		return memberURLFromAddress(address)
	}
	scheme := "http"
	if s.advertiseClientHTTPS {
		scheme = "https"
	}
	return fmt.Sprintf("%s://%s", scheme, net.JoinHostPort(host, strconv.Itoa(s.advertiseClientPort)))
}
