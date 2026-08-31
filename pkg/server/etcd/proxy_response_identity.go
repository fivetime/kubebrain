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

import "go.etcd.io/etcd/api/v3/etcdserverpb"

type proxyResponseIdentity struct {
	clusterID uint64
	members   []*etcdserverpb.Member
}

type proxyResponseRevisionPolicy uint8

const (
	proxyResponseRevisionPositive proxyResponseRevisionPolicy = iota
	proxyResponseRevisionZero
)

func (s *RPCServer) expectedProxyResponseIdentity() proxyResponseIdentity {
	return proxyResponseIdentity{
		clusterID: s.backend.ClusterID(),
		members:   s.staticMembers,
	}
}

func proxyResponseIdentityHasMember(identity proxyResponseIdentity, memberID uint64) bool {
	for _, member := range identity.members {
		if member != nil && member.GetID() == memberID {
			return true
		}
	}
	return false
}

func validateProxyResponseHeader(response any, identity proxyResponseIdentity, revisionPolicy proxyResponseRevisionPolicy) string {
	headerResponse, ok := response.(interface {
		GetHeader() *etcdserverpb.ResponseHeader
	})
	if !ok || headerResponse.GetHeader() == nil {
		return "without a header"
	}
	header := headerResponse.GetHeader()
	if header.GetRevision() < 0 {
		return "with a negative header revision"
	}
	if header.GetClusterId() == 0 {
		return "with a zero cluster ID"
	}
	if header.GetClusterId() != identity.clusterID {
		return "for a foreign cluster"
	}
	if header.GetMemberId() == 0 {
		return "with a zero member ID"
	}
	if len(identity.members) > 0 {
		if !proxyResponseIdentityHasMember(identity, header.GetMemberId()) {
			return "for an unknown member"
		}
	}
	if header.GetRaftTerm() == 0 {
		return "with a zero raft term"
	}
	switch revisionPolicy {
	case proxyResponseRevisionPositive:
		if header.GetRevision() == 0 {
			return "with a non-positive header revision"
		}
	case proxyResponseRevisionZero:
		if header.GetRevision() != 0 {
			return "with a nonzero header revision"
		}
	}
	return ""
}
