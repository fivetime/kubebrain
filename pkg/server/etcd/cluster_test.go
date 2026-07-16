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
	"testing"

	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestMemberListReturnsCurrentMember(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	resp, err := server.MemberList(context.Background(), &etcdserverpb.MemberListRequest{})
	require.NoError(t, err)
	require.NotNil(t, resp.Header)
	require.NotZero(t, resp.Header.MemberId)
	require.Equal(t, server.backend.ClusterID(), resp.Header.ClusterId)
	require.Len(t, resp.Members, 1)
	require.Equal(t, "test-peer", resp.Members[0].Name)
	require.Equal(t, []string{"http://test-peer"}, resp.Members[0].PeerURLs)
	require.Equal(t, []string{"http://test-peer"}, resp.Members[0].ClientURLs)
}

func TestMemberListClientURLUsesAdvertisedClientPortAndScheme(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	// Fallback (unset advertise info): legacy identity-derived URL. Identity in
	// the test harness is not host:port shaped, so shaping must also fall back.
	require.Equal(t, "http://test-peer", server.clientURLFromAddress("test-peer"))

	server.SetAdvertiseClientInfo(3379, false)
	require.Equal(t, "http://10.0.0.1:3379", server.clientURLFromAddress("10.0.0.1:2380"))

	server.SetAdvertiseClientInfo(3379, true)
	require.Equal(t, "https://10.0.0.1:3379", server.clientURLFromAddress("10.0.0.1:2380"))
	// IPv6 identity keeps brackets.
	require.Equal(t, "https://[2001:db8::1]:3379", server.clientURLFromAddress("[2001:db8::1]:2380"))
	// Unparseable identity falls back rather than emitting a mangled URL.
	require.Equal(t, "http://test-peer", server.clientURLFromAddress("test-peer"))
}

func TestMemberMutationIsUnsupported(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	ctx := context.Background()
	_, err := server.MemberAdd(ctx, &etcdserverpb.MemberAddRequest{})
	require.Equal(t, codes.Unimplemented, status.Code(err))
	_, err = server.MemberRemove(ctx, &etcdserverpb.MemberRemoveRequest{})
	require.Equal(t, codes.Unimplemented, status.Code(err))
	_, err = server.MemberUpdate(ctx, &etcdserverpb.MemberUpdateRequest{})
	require.Equal(t, codes.Unimplemented, status.Code(err))
	_, err = server.MemberPromote(ctx, &etcdserverpb.MemberPromoteRequest{})
	require.Equal(t, codes.Unimplemented, status.Code(err))
}
