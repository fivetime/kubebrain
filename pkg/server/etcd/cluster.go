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
	if err := s.requireAuthenticated(ctx, false); err != nil {
		return nil, err
	}
	if req.GetLinearizable() {
		if err := s.peers.SyncReadRevision(ctx); err != nil {
			return nil, readBarrierStatusErr(err)
		}
	}
	return s.memberListResponse(s.membersSnapshot()), nil
}

func (s *RPCServer) membersSnapshot() []*etcdserverpb.Member {
	if len(s.staticMembers) > 0 {
		members := make([]*etcdserverpb.Member, len(s.staticMembers))
		for i := range s.staticMembers {
			members[i] = proto.Clone(s.staticMembers[i]).(*etcdserverpb.Member)
		}
		return members
	}
	addresses := []string{s.backend.GetResourceLock().Identity(), s.peers.GetLeaderInfo()}
	members := make([]*etcdserverpb.Member, 0, len(addresses))
	seen := make(map[uint64]struct{}, len(addresses))
	for _, address := range addresses {
		if !election.IsLeaderKnown(address) {
			continue
		}
		id := s.memberIDForPeerIdentity(address)
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
	return members
}

func (s *RPCServer) memberListResponse(members []*etcdserverpb.Member) *etcdserverpb.MemberListResponse {
	return &etcdserverpb.MemberListResponse{
		Header: &etcdserverpb.ResponseHeader{
			ClusterId: s.backend.ClusterID(),
			MemberId:  s.memberIDForPeerIdentity(s.backend.GetResourceLock().Identity()),
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
	seenPeerIdentities := map[string]struct{}{}
	memberIndexes := map[string]int{}
	members := make([]*etcdserverpb.Member, 0)
	for _, entry := range strings.Split(spec, ",") {
		name, rawURL, err := parseInitialClusterEntry(entry)
		if err != nil {
			return nil, fmt.Errorf("invalid initial-cluster entry %q: want name=http[s]://host:peerPort", entry)
		}
		peerURL, identity, err := parseInitialClusterPeerURL(name, rawURL)
		if err != nil {
			return nil, err
		}
		if _, exists := seenPeerIdentities[identity]; exists {
			return nil, fmt.Errorf("duplicate initial-cluster peer identity %q", identity)
		}
		seenPeerIdentities[identity] = struct{}{}
		memberIndex, exists := memberIndexes[name]
		if !exists {
			memberIndex = len(members)
			memberIndexes[name] = memberIndex
			members = append(members, &etcdserverpb.Member{
				Name:       name,
				ClientURLs: append([]string(nil), advertised...),
			})
		}
		members[memberIndex].PeerURLs = append(members[memberIndex].PeerURLs, peerURL)
		if len(advertised) == 0 {
			host, _, _ := net.SplitHostPort(identity)
			members[memberIndex].ClientURLs = append(members[memberIndex].ClientURLs,
				fmt.Sprintf("%s://%s", scheme, net.JoinHostPort(host, strconv.Itoa(clientPort))))
		}
	}
	seenMemberIDs := map[uint64]struct{}{}
	for _, member := range members {
		sort.Strings(member.PeerURLs)
		identity, err := PeerIdentityFromURL(member.PeerURLs[0])
		if err != nil {
			return nil, err
		}
		member.ID = uint64(crc32.ChecksumIEEE([]byte(identity)))
		if _, exists := seenMemberIDs[member.ID]; exists {
			return nil, fmt.Errorf("duplicate initial-cluster member ID %d", member.ID)
		}
		seenMemberIDs[member.ID] = struct{}{}
		if len(advertised) == 0 {
			member.ClientURLs = uniqueSortedStrings(member.ClientURLs)
		}
	}
	sort.Slice(members, func(i, j int) bool { return members[i].ID < members[j].ID })
	return members, nil
}

func parseInitialClusterEntry(entry string) (name, rawURL string, err error) {
	parts := strings.SplitN(strings.TrimSpace(entry), "=", 2)
	if len(parts) != 2 || strings.TrimSpace(parts[0]) == "" || strings.TrimSpace(parts[1]) == "" {
		return "", "", fmt.Errorf("invalid initial-cluster entry %q", entry)
	}
	return strings.TrimSpace(parts[0]), strings.TrimSpace(parts[1]), nil
}

func parseInitialClusterPeerURL(name, rawURL string) (peerURL, identity string, err error) {
	if containsUnsafeClusterURLChar(rawURL) {
		return "", "", fmt.Errorf("invalid peer URL %q for member %q: unsupported characters", rawURL, name)
	}
	u, err := url.Parse(rawURL)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" || u.User != nil || u.Path != "" || u.RawQuery != "" || u.Fragment != "" {
		return "", "", fmt.Errorf("invalid peer URL %q for member %q", rawURL, name)
	}
	host, port, err := net.SplitHostPort(u.Host)
	if err != nil || host == "" || port == "" {
		return "", "", fmt.Errorf("invalid peer URL %q for member %q: host and port are required", rawURL, name)
	}
	peerPort, err := strconv.Atoi(port)
	if err != nil || peerPort <= 0 || peerPort > 65535 {
		return "", "", fmt.Errorf("invalid peer URL %q for member %q: invalid port", rawURL, name)
	}
	return u.String(), net.JoinHostPort(host, port), nil
}

func PeerIdentityFromURL(rawURL string) (string, error) {
	_, identity, err := parseInitialClusterPeerURL("member", rawURL)
	return identity, err
}

func uniqueSortedStrings(values []string) []string {
	sort.Strings(values)
	result := values[:0]
	var previous string
	for _, value := range values {
		if value == previous {
			continue
		}
		result = append(result, value)
		previous = value
	}
	return result
}

// ValidateAdvertiseClientURLs validates and copies client URLs before they are
// published through MemberList. Clientv3 replaces its endpoint set with these
// values, so malformed or non-dialable URL shapes must fail startup.
func ValidateAdvertiseClientURLs(raw []string) ([]string, error) {
	seen := make(map[string]struct{}, len(raw))
	urls := make([]string, 0, len(raw))
	for _, value := range raw {
		value = strings.TrimSpace(value)
		if containsUnsafeClusterURLChar(value) {
			return nil, fmt.Errorf("invalid advertised client URL %q: unsupported characters", value)
		}
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

func containsUnsafeClusterURLChar(value string) bool {
	for _, r := range value {
		if r < 0x20 || r == 0x7f || r == '"' || r == '\\' {
			return true
		}
	}
	return false
}

func (s *RPCServer) memberIDForPeerIdentity(address string) uint64 {
	if !election.IsLeaderKnown(address) {
		return 0
	}
	for _, member := range s.staticMembers {
		for _, peerURL := range member.PeerURLs {
			_, identity, err := parseInitialClusterPeerURL(member.Name, peerURL)
			if err == nil && identity == address {
				return member.ID
			}
		}
	}
	return s.memberIDFromAddress(address)
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
