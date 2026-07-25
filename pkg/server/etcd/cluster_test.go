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
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"sort"
	"strconv"
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

func TestPeerMembersHandlerReturnsEtcdPeerJSON(t *testing.T) {
	members, err := ParseInitialCluster(
		"kb-2=http://10.0.0.2:2380,kb-1=http://10.0.0.1:2380",
		2379, true,
	)
	require.NoError(t, err)
	server, closeFn := newTestRPCServer(t)
	defer closeFn()
	server.SetStaticMembers(members)

	rec := httptest.NewRecorder()
	server.GetPeerHttpHandlers()["/members"].ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/members", nil))

	require.Equal(t, http.StatusOK, rec.Code)
	require.Equal(t, "application/json", rec.Header().Get("Content-Type"))
	require.Equal(t, strconv.FormatUint(server.backend.ClusterID(), 16), rec.Header().Get(etcdClusterIDHeader))
	var got []struct {
		ID         uint64   `json:"id"`
		PeerURLs   []string `json:"peerURLs"`
		IsLearner  bool     `json:"isLearner"`
		Name       string   `json:"name"`
		ClientURLs []string `json:"clientURLs"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &got))
	require.Len(t, got, 2)
	sort.Slice(got, func(i, j int) bool { return got[i].Name < got[j].Name })
	wantIDs := make(map[string]uint64, len(members))
	for _, member := range members {
		wantIDs[member.Name] = member.ID
	}
	require.Equal(t, "kb-1", got[0].Name)
	require.Equal(t, wantIDs["kb-1"], got[0].ID)
	require.Equal(t, []string{"http://10.0.0.1:2380"}, got[0].PeerURLs)
	require.Equal(t, []string{"https://10.0.0.1:2379"}, got[0].ClientURLs)
	require.False(t, got[0].IsLearner)

	var raw []map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &raw))
	require.Contains(t, raw[0], "id")
	require.NotContains(t, raw[0], "ID")
}

func TestPeerMembersHandlerRejectsBadRequests(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()
	tests := []struct {
		name       string
		method     string
		target     string
		wantStatus int
		wantBody   string
	}{
		{name: "wrong method", method: http.MethodPost, target: "/members", wantStatus: http.StatusMethodNotAllowed, wantBody: "Method Not Allowed"},
		{name: "wrong path", method: http.MethodGet, target: "/members/extra", wantStatus: http.StatusBadRequest, wantBody: "bad path"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rec := httptest.NewRecorder()
			server.peerMembersHandler(rec, httptest.NewRequest(tt.method, tt.target, nil))
			require.Equal(t, tt.wantStatus, rec.Code)
			require.Contains(t, rec.Body.String(), tt.wantBody)
		})
	}
}

func TestPeerDowngradeEnabledHandlerReturnsFalse(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	rec := httptest.NewRecorder()
	server.GetPeerHttpHandlers()["/downgrade/enabled"].ServeHTTP(rec,
		httptest.NewRequest(http.MethodGet, "/downgrade/enabled", nil))

	require.Equal(t, http.StatusOK, rec.Code)
	require.Equal(t, "text/plain", rec.Header().Get("Content-Type"))
	require.Equal(t, strconv.FormatUint(server.backend.ClusterID(), 16), rec.Header().Get(etcdClusterIDHeader))
	require.Equal(t, "false", rec.Body.String())
}

func TestPeerDowngradeEnabledHandlerRejectsBadRequests(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()
	tests := []struct {
		name       string
		method     string
		target     string
		wantStatus int
		wantBody   string
	}{
		{name: "wrong method", method: http.MethodPost, target: "/downgrade/enabled", wantStatus: http.StatusMethodNotAllowed, wantBody: "Method Not Allowed"},
		{name: "wrong path", method: http.MethodGet, target: "/downgrade/enabled/extra", wantStatus: http.StatusBadRequest, wantBody: "bad path"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rec := httptest.NewRecorder()
			server.peerDowngradeEnabledHandler(rec, httptest.NewRequest(tt.method, tt.target, nil))
			require.Equal(t, tt.wantStatus, rec.Code)
			require.Contains(t, rec.Body.String(), tt.wantBody)
		})
	}
}

func TestPeerMemberPromoteHandlerReturnsPlatformBoundary(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	rec := httptest.NewRecorder()
	server.GetPeerHttpHandlers()["/members/promote/"].ServeHTTP(rec,
		httptest.NewRequest(http.MethodPost, "/members/promote/123", nil))

	require.Equal(t, http.StatusNotImplemented, rec.Code)
	require.Equal(t, strconv.FormatUint(server.backend.ClusterID(), 16), rec.Header().Get(etcdClusterIDHeader))
	require.Contains(t, rec.Body.String(), memberMutationUnsupportedMessage)
}

func TestPeerMemberPromoteHandlerRejectsBadRequests(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()
	tests := []struct {
		name       string
		method     string
		target     string
		wantStatus int
		wantBody   string
	}{
		{name: "wrong method", method: http.MethodGet, target: "/members/promote/123", wantStatus: http.StatusMethodNotAllowed, wantBody: "Method Not Allowed"},
		{name: "wrong path", method: http.MethodPost, target: "/members/promote", wantStatus: http.StatusBadRequest, wantBody: "bad path"},
		{name: "missing id", method: http.MethodPost, target: "/members/promote/", wantStatus: http.StatusNotFound, wantBody: "member  not found in cluster"},
		{name: "bad id", method: http.MethodPost, target: "/members/promote/not-a-number", wantStatus: http.StatusNotFound, wantBody: "member not-a-number not found in cluster"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rec := httptest.NewRecorder()
			server.peerMemberPromoteHandler(rec, httptest.NewRequest(tt.method, tt.target, nil))
			require.Equal(t, tt.wantStatus, rec.Code)
			require.Contains(t, rec.Body.String(), tt.wantBody)
		})
	}
}

func TestParseInitialClusterAggregatesRepeatedMemberPeerURLs(t *testing.T) {
	members, err := ParseInitialCluster(
		"mem1=http://10.0.0.1:2380,mem1=http://128.193.4.20:2380,mem2=http://10.0.0.2:2380",
		2379, true,
	)
	require.NoError(t, err)
	require.Len(t, members, 2)

	var mem1 *etcdserverpb.Member
	for _, member := range members {
		if member.Name == "mem1" {
			mem1 = member
			break
		}
	}
	require.NotNil(t, mem1)
	require.Equal(t, []string{
		"http://10.0.0.1:2380",
		"http://128.193.4.20:2380",
	}, mem1.PeerURLs)
	require.Equal(t, []string{
		"https://10.0.0.1:2379",
		"https://128.193.4.20:2379",
	}, mem1.ClientURLs)

	reordered, err := ParseInitialCluster(
		"mem1=http://128.193.4.20:2380,mem1=http://10.0.0.1:2380,mem2=http://10.0.0.2:2380",
		2379, true,
	)
	require.NoError(t, err)
	for _, member := range reordered {
		if member.Name == "mem1" {
			require.Equal(t, mem1.ID, member.ID)
			return
		}
	}
	t.Fatal("reordered mem1 is missing")
}

func TestStaticMemberIDMatchesAnyPeerIdentity(t *testing.T) {
	members, err := ParseInitialCluster(
		"kb-1=http://10.0.0.1:2380,kb-1=http://128.193.4.20:2380",
		2379, false,
	)
	require.NoError(t, err)
	require.Len(t, members, 1)

	server := &RPCServer{staticMembers: members}
	require.Equal(t, members[0].ID, server.memberIDForPeerIdentity("10.0.0.1:2380"))
	require.Equal(t, members[0].ID, server.memberIDForPeerIdentity("128.193.4.20:2380"))
	require.Equal(t, server.memberIDFromAddress("10.0.0.3:2380"), server.memberIDForPeerIdentity("10.0.0.3:2380"))
}

func TestStatusLeaderMatchesStaticMemberPeerIdentity(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	members, err := ParseInitialCluster(
		"kb-1=http://10.0.0.1:2380,kb-1=http://128.193.4.20:2380",
		2379, false,
	)
	require.NoError(t, err)
	server.SetStaticMembers(members)
	server.peers = testPeerService{isLeader: true, leaderInfo: "128.193.4.20:2380"}

	status, err := server.Status(context.Background(), &etcdserverpb.StatusRequest{})
	require.NoError(t, err)
	require.Equal(t, members[0].ID, status.Leader)
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

func TestValidateAdvertiseClientURLsRejectsUnsafeCharacters(t *testing.T) {
	for _, tc := range []struct {
		name string
		urls []string
	}{
		{name: "newline", urls: []string{"https://etcd.example.com\n:2379"}},
		{name: "tab", urls: []string{"https://etcd.example.com\t:2379"}},
		{name: "quote", urls: []string{"https://etcd.example.com\":2379"}},
		{name: "backslash", urls: []string{"https://etcd.example.com\\:2379"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := ValidateAdvertiseClientURLs(tc.urls)
			require.ErrorContains(t, err, "unsupported characters")
		})
	}
}

func TestParseInitialClusterRejectsInvalidConfiguration(t *testing.T) {
	for _, spec := range []string{
		"missing-url",
		"a=10.0.0.1:2380",
		"a=http://10.0.0.1",
		"a=http://10.0.0.1:2380/path",
		"a=http://10.0.0.1:2380,a=http://10.0.0.1:2380",
		"a=http://10.0.0.1:2380,b=http://10.0.0.1:2380",
	} {
		_, err := ParseInitialCluster(spec, 2379, false)
		require.Error(t, err, "spec %q", spec)
	}
}

func TestParseInitialClusterRejectsUnsafePeerURLCharacters(t *testing.T) {
	for _, tc := range []struct {
		name string
		spec string
	}{
		{name: "newline", spec: "a=http://10.0.0.1\n:2380"},
		{name: "tab", spec: "a=http://10.0.0.1\t:2380"},
		{name: "quote", spec: "a=http://10.0.0.1\":2380"},
		{name: "backslash", spec: "a=http://10.0.0.1\\:2380"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := ParseInitialCluster(tc.spec, 2379, false)
			require.ErrorContains(t, err, "unsupported characters")
		})
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

func TestMemberListLinearizableAuthenticatesBeforeReadBarrier(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()
	aliceCtx := setupAuthKVUser(t, server)

	var calls atomic.Int32
	barrierErr := errors.New("read barrier must not run for unauthenticated requests")
	server.peers = testPeerService{syncReadFn: func(context.Context) error {
		calls.Add(1)
		return barrierErr
	}}

	_, err := server.MemberList(context.Background(), &etcdserverpb.MemberListRequest{Linearizable: true})
	require.ErrorIs(t, err, rpctypes.ErrUserEmpty)
	require.Zero(t, calls.Load())

	_, err = server.MemberList(aliceCtx, &etcdserverpb.MemberListRequest{Linearizable: true})
	require.Equal(t, codes.Unavailable, status.Code(err))
	require.Equal(t, barrierErr.Error(), status.Convert(err).Message())
	require.EqualValues(t, 1, calls.Load())
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

func TestMemberRootAuthorizationClientCertificateErrorsMatchEtcd(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()
	setupAuthKVUser(t, server)
	server.SetClientCertAuth(true)
	ctx := context.Background()

	_, err := server.MemberAdd(verifiedTLSContext(ctx, ""), &etcdserverpb.MemberAddRequest{})
	require.ErrorIs(t, err, rpctypes.ErrUserEmpty)
	_, err = server.MemberAdd(verifiedTLSContext(ctx, "external-cn"), &etcdserverpb.MemberAddRequest{})
	require.ErrorIs(t, err, rpctypes.ErrUserNotFound)
	_, err = server.MemberAdd(verifiedTLSContext(ctx, "alice"), &etcdserverpb.MemberAddRequest{})
	require.ErrorIs(t, err, rpctypes.ErrPermissionDenied)
	_, err = server.MemberAdd(verifiedTLSContext(ctx, "root"), &etcdserverpb.MemberAddRequest{})
	require.Equal(t, codes.Unimplemented, status.Code(err))
}
