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
	"sort"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
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
	require.Zero(t, resp.Header.Revision, "etcd cluster response headers do not carry an MVCC revision")
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

	server.SetAdvertiseClientInfo(3379, true, "https://etcd.example.com:2379")
	require.Equal(t, []string{"https://etcd.example.com:2379"}, server.clientURLsFromAddress("10.0.0.1:2380"))
}

func TestParseInitialClusterAndMemberList(t *testing.T) {
	members, err := ParseInitialCluster(
		"kb-2=http://10.0.0.2:2380,kb-1=http://10.0.0.1:2380,kb-3=http://[2001:db8::3]:2380",
		2379, true,
	)
	require.NoError(t, err)
	require.Len(t, members, 3)

	server, closeFn := newTestRPCServer(t)
	defer closeFn()
	server.SetStaticMembers(members)
	resp, err := server.MemberList(context.Background(), &etcdserverpb.MemberListRequest{})
	require.NoError(t, err)
	require.Len(t, resp.Members, 3)

	names := make([]string, 0, 3)
	clientURLs := make([]string, 0, 3)
	for _, member := range resp.Members {
		names = append(names, member.Name)
		clientURLs = append(clientURLs, member.ClientURLs...)
	}
	sort.Strings(names)
	sort.Strings(clientURLs)
	require.Equal(t, []string{"kb-1", "kb-2", "kb-3"}, names)
	require.Equal(t, []string{
		"https://10.0.0.1:2379", "https://10.0.0.2:2379", "https://[2001:db8::3]:2379",
	}, clientURLs)

	resp.Members[0].Name = "mutated"
	again, err := server.MemberList(context.Background(), &etcdserverpb.MemberListRequest{})
	require.NoError(t, err)
	for _, member := range again.Members {
		require.NotEqual(t, "mutated", member.Name)
	}
}

func TestParseInitialClusterUsesAdvertisedClientURLs(t *testing.T) {
	members, err := ParseInitialCluster(
		"kb-1=http://10.0.0.1:2380,kb-2=http://10.0.0.2:2380",
		2379, false, "https://etcd.example.com:2379",
	)
	require.NoError(t, err)
	require.Len(t, members, 2)
	for _, member := range members {
		require.Equal(t, []string{"https://etcd.example.com:2379"}, member.ClientURLs)
	}
	members[0].ClientURLs[0] = "mutated"
	require.Equal(t, "https://etcd.example.com:2379", members[1].ClientURLs[0])
}

func TestStaticMembersUseAdvertisedClientURLs(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()
	server.SetAdvertiseClientInfo(2379, false, "https://etcd.example.com:2379")
	server.SetStaticMembers([]*etcdserverpb.Member{{
		ID: 1, Name: "kb-1", ClientURLs: []string{"http://10.0.0.1:2379"},
	}})

	response, err := server.MemberList(context.Background(), &etcdserverpb.MemberListRequest{})
	require.NoError(t, err)
	require.Equal(t, []string{"https://etcd.example.com:2379"}, response.Members[0].ClientURLs)
}

func TestValidateAdvertiseClientURLs(t *testing.T) {
	got, err := ValidateAdvertiseClientURLs([]string{
		"https://etcd.example.com:2379/", "http://[2001:db8::1]:2379",
	})
	require.NoError(t, err)
	require.Equal(t, []string{
		"https://etcd.example.com:2379", "http://[2001:db8::1]:2379",
	}, got)

	for _, urls := range [][]string{
		{"etcd.example.com:2379"},
		{"ftp://etcd.example.com:2379"},
		{"https://etcd.example.com"},
		{"https://user@etcd.example.com:2379"},
		{"https://etcd.example.com:2379/v3"},
		{"https://etcd.example.com:2379?tenant=a"},
		{"https://etcd.example.com:2379", "https://etcd.example.com:2379/"},
	} {
		_, err := ValidateAdvertiseClientURLs(urls)
		require.Error(t, err, "urls=%v", urls)
	}
}

func TestParseInitialClusterRejectsInvalidConfiguration(t *testing.T) {
	for _, spec := range []string{
		"missing-url",
		"a=10.0.0.1:2380",
		"a=http://10.0.0.1",
		"a=http://10.0.0.1:2380/path",
		"a=http://10.0.0.1:2380,a=http://10.0.0.2:2380",
		"a=http://10.0.0.1:2380,b=http://10.0.0.1:2380",
	} {
		_, err := ParseInitialCluster(spec, 2379, false)
		require.Error(t, err, "spec %q", spec)
	}
}

func TestMemberMutationIsUnsupported(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	ctx := context.Background()
	_, err := server.MemberAdd(ctx, &etcdserverpb.MemberAddRequest{})
	require.Equal(t, codes.Unimplemented, status.Code(err))
	require.Equal(t, memberMutationUnsupportedMessage, status.Convert(err).Message())
	_, err = server.MemberRemove(ctx, &etcdserverpb.MemberRemoveRequest{})
	require.Equal(t, codes.Unimplemented, status.Code(err))
	require.Equal(t, memberMutationUnsupportedMessage, status.Convert(err).Message())
	_, err = server.MemberUpdate(ctx, &etcdserverpb.MemberUpdateRequest{})
	require.Equal(t, codes.Unimplemented, status.Code(err))
	require.Equal(t, memberMutationUnsupportedMessage, status.Convert(err).Message())
	_, err = server.MemberPromote(ctx, &etcdserverpb.MemberPromoteRequest{})
	require.Equal(t, codes.Unimplemented, status.Code(err))
	require.Equal(t, memberMutationUnsupportedMessage, status.Convert(err).Message())
}

func TestMemberListLinearizableUsesReadBarrier(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()
	var calls atomic.Int32
	wantErr := errors.New("read barrier failed")
	server.peers = testPeerService{syncReadFn: func(context.Context) error {
		calls.Add(1)
		return wantErr
	}}

	_, err := server.MemberList(context.Background(), &etcdserverpb.MemberListRequest{})
	require.NoError(t, err)
	require.Zero(t, calls.Load())
	_, err = server.MemberList(context.Background(), &etcdserverpb.MemberListRequest{Linearizable: true})
	require.Equal(t, codes.Unavailable, status.Code(err))
	require.Equal(t, wantErr.Error(), status.Convert(err).Message())
	require.EqualValues(t, 1, calls.Load())
}

func TestMemberListLinearizablePreservesBarrierStatus(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	for _, wantErr := range []error{
		context.Canceled,
		context.DeadlineExceeded,
		status.Error(codes.ResourceExhausted, "barrier overloaded"),
	} {
		server.peers = testPeerService{syncReadFn: func(context.Context) error {
			return wantErr
		}}
		_, err := server.MemberList(context.Background(), &etcdserverpb.MemberListRequest{Linearizable: true})
		require.Equal(t, status.Code(wantErr), status.Code(err))
		require.Equal(t, status.Convert(wantErr).Message(), status.Convert(err).Message())
	}
}

func TestMemberAuthorizationMatchesEtcd(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()
	aliceCtx := setupAuthKVUser(t, server)
	plain := context.Background()

	_, err := server.MemberList(plain, &etcdserverpb.MemberListRequest{})
	require.ErrorIs(t, err, rpctypes.ErrUserEmpty)
	_, err = server.MemberList(aliceCtx, &etcdserverpb.MemberListRequest{})
	require.NoError(t, err)
	_, err = server.MemberAdd(aliceCtx, &etcdserverpb.MemberAddRequest{})
	require.ErrorIs(t, err, rpctypes.ErrPermissionDenied)

	rootToken, err := server.tokens.authenticate(plain, "root", "root-secret")
	require.NoError(t, err)
	rootCtx := metadata.NewIncomingContext(plain, metadata.Pairs(rpctypes.TokenFieldNameGRPC, rootToken))
	_, err = server.MemberAdd(rootCtx, &etcdserverpb.MemberAddRequest{})
	require.Equal(t, codes.Unimplemented, status.Code(err))
}
