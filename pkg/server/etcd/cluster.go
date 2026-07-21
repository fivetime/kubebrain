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
	"hash/crc32"
	"net"
	"net/url"
	"sort"
	"strconv"
	"strings"

	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	"github.com/kubewharf/kubebrain/pkg/backend/election"
)

// MemberList lists the current cluster membership.
func (s *RPCServer) MemberList(ctx context.Context, req *etcdserverpb.MemberListRequest) (*etcdserverpb.MemberListResponse, error) {
	s.metricCli.EmitCounter("member.list", 1)
	if req.GetLinearizable() {
		if err := s.peers.SyncReadRevision(ctx); err != nil {
			return nil, readBarrierStatusErr(err)
		}
	}
	if err := s.requireAuthenticated(ctx, false); err != nil {
		return nil, err
	}
	if len(s.staticMembers) > 0 {
		members := make([]*etcdserverpb.Member, len(s.staticMembers))
		for i := range s.staticMembers {
			members[i] = proto.Clone(s.staticMembers[i]).(*etcdserverpb.Member)
		}
		return s.memberListResponse(members), nil
	}
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
			ClientURLs: s.clientURLsFromAddress(address),
			IsLearner:  false,
		})
	}
	return s.memberListResponse(members), nil
}

func (s *RPCServer) memberListResponse(members []*etcdserverpb.Member) *etcdserverpb.MemberListResponse {
	return &etcdserverpb.MemberListResponse{
		Header: &etcdserverpb.ResponseHeader{
			ClusterId: s.backend.ClusterID(),
			MemberId:  s.memberIDFromAddress(s.backend.GetResourceLock().Identity()),
		},
		Members: members,
	}
}

// ParseInitialCluster parses etcd's name=peerURL comma-separated shape into
// the KubeBrain service membership exposed by MemberList. Explicit advertised
// client URLs override peer-host derivation for every member.
func ParseInitialCluster(spec string, clientPort int, clientHTTPS bool, advertiseClientURLs ...string) ([]*etcdserverpb.Member, error) {
	advertised, err := ValidateAdvertiseClientURLs(advertiseClientURLs)
	if err != nil {
		return nil, err
	}
	if strings.TrimSpace(spec) == "" {
		return nil, nil
	}
	if clientPort <= 0 || clientPort > 65535 {
		return nil, fmt.Errorf("invalid client port %d", clientPort)
	}
	scheme := "http"
	if clientHTTPS {
		scheme = "https"
	}
	seenNames := map[string]struct{}{}
	seenIDs := map[uint64]struct{}{}
	members := make([]*etcdserverpb.Member, 0)
	for _, entry := range strings.Split(spec, ",") {
		parts := strings.SplitN(strings.TrimSpace(entry), "=", 2)
		if len(parts) != 2 || strings.TrimSpace(parts[0]) == "" || strings.TrimSpace(parts[1]) == "" {
			return nil, fmt.Errorf("invalid initial-cluster entry %q: want name=http[s]://host:peerPort", entry)
		}
		name, rawURL := strings.TrimSpace(parts[0]), strings.TrimSpace(parts[1])
		if _, exists := seenNames[name]; exists {
			return nil, fmt.Errorf("duplicate initial-cluster member name %q", name)
		}
		u, err := url.Parse(rawURL)
		if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" || u.User != nil || u.Path != "" || u.RawQuery != "" || u.Fragment != "" {
			return nil, fmt.Errorf("invalid peer URL %q for member %q", rawURL, name)
		}
		host, port, err := net.SplitHostPort(u.Host)
		if err != nil || host == "" || port == "" {
			return nil, fmt.Errorf("invalid peer URL %q for member %q: host and port are required", rawURL, name)
		}
		peerPort, err := strconv.Atoi(port)
		if err != nil || peerPort <= 0 || peerPort > 65535 {
			return nil, fmt.Errorf("invalid peer URL %q for member %q: invalid port", rawURL, name)
		}
		identity := net.JoinHostPort(host, port)
		id := uint64(crc32.ChecksumIEEE([]byte(identity)))
		if _, exists := seenIDs[id]; exists {
			return nil, fmt.Errorf("duplicate initial-cluster peer identity %q", identity)
		}
		seenNames[name], seenIDs[id] = struct{}{}, struct{}{}
		clientURLs := advertised
		if len(clientURLs) == 0 {
			clientURLs = []string{fmt.Sprintf("%s://%s", scheme, net.JoinHostPort(host, strconv.Itoa(clientPort)))}
		}
		members = append(members, &etcdserverpb.Member{
			ID: id, Name: name, PeerURLs: []string{u.String()},
			ClientURLs: append([]string(nil), clientURLs...),
		})
	}
	sort.Slice(members, func(i, j int) bool { return members[i].ID < members[j].ID })
	return members, nil
}

// ValidateAdvertiseClientURLs validates and copies client URLs before they are
// published through MemberList. Clientv3 replaces its endpoint set with these
// values, so malformed or non-dialable URL shapes must fail startup.
func ValidateAdvertiseClientURLs(raw []string) ([]string, error) {
	seen := make(map[string]struct{}, len(raw))
	urls := make([]string, 0, len(raw))
	for _, value := range raw {
		value = strings.TrimSpace(value)
		u, err := url.Parse(value)
		if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" ||
			u.User != nil || (u.Path != "" && u.Path != "/") || u.RawQuery != "" || u.Fragment != "" {
			return nil, fmt.Errorf("invalid advertised client URL %q", value)
		}
		if _, _, err := net.SplitHostPort(u.Host); err != nil {
			return nil, fmt.Errorf("invalid advertised client URL %q: host and port are required", value)
		}
		canonical := strings.TrimSuffix(u.String(), "/")
		if _, ok := seen[canonical]; ok {
			return nil, fmt.Errorf("duplicate advertised client URL %q", canonical)
		}
		seen[canonical] = struct{}{}
		urls = append(urls, canonical)
	}
	return urls, nil
}

// MemberAdd adds a member into the cluster.
func (s *RPCServer) MemberAdd(ctx context.Context, _ *etcdserverpb.MemberAddRequest) (*etcdserverpb.MemberAddResponse, error) {
	s.metricCli.EmitCounter("member.add", 1)
	if err := s.requireAuthenticated(ctx, true); err != nil {
		return nil, err
	}
	return nil, status.Error(codes.Unimplemented, memberMutationUnsupportedMessage)
}

// MemberRemove removes an existing member from the cluster.
func (s *RPCServer) MemberRemove(ctx context.Context, _ *etcdserverpb.MemberRemoveRequest) (*etcdserverpb.MemberRemoveResponse, error) {
	s.metricCli.EmitCounter("member.remove", 1)
	if err := s.requireAuthenticated(ctx, true); err != nil {
		return nil, err
	}
	return nil, status.Error(codes.Unimplemented, memberMutationUnsupportedMessage)
}

// MemberUpdate updates the peer addresses of the member.
func (s *RPCServer) MemberUpdate(ctx context.Context, _ *etcdserverpb.MemberUpdateRequest) (*etcdserverpb.MemberUpdateResponse, error) {
	s.metricCli.EmitCounter("member.update", 1)
	if err := s.requireAuthenticated(ctx, true); err != nil {
		return nil, err
	}
	return nil, status.Error(codes.Unimplemented, memberMutationUnsupportedMessage)
}

// MemberPromote promotes a member from raft learner (non-voting) to raft voting member.
func (s *RPCServer) MemberPromote(ctx context.Context, _ *etcdserverpb.MemberPromoteRequest) (*etcdserverpb.MemberPromoteResponse, error) {
	s.metricCli.EmitCounter("member.promote", 1)
	if err := s.requireAuthenticated(ctx, true); err != nil {
		return nil, err
	}
	return nil, status.Error(codes.Unimplemented, memberMutationUnsupportedMessage)
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
	return s.clientURLsFromAddress(address)[0]
}

func (s *RPCServer) clientURLsFromAddress(address string) []string {
	if len(s.advertiseClientURLs) > 0 {
		return append([]string(nil), s.advertiseClientURLs...)
	}
	if s.advertiseClientPort == 0 || strings.Contains(address, "://") {
		return []string{memberURLFromAddress(address)}
	}
	host, _, err := net.SplitHostPort(address)
	if err != nil {
		return []string{memberURLFromAddress(address)}
	}
	scheme := "http"
	if s.advertiseClientHTTPS {
		scheme = "https"
	}
	return []string{fmt.Sprintf("%s://%s", scheme, net.JoinHostPort(host, strconv.Itoa(s.advertiseClientPort)))}
}
