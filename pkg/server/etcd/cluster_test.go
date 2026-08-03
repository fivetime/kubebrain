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
	"fmt"
	"net/http"
	"net/http/httptest"
	"sort"
	"strconv"
	"strings"
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

func TestMemberListAndPeerMembersSortControlPlaneMembersByID(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()
	server.SetStaticMembers([]*etcdserverpb.Member{
		{ID: 30, Name: "third", PeerURLs: []string{"http://third:2380"}},
		{ID: 10, Name: "first", PeerURLs: []string{"http://first:2380"}},
		{ID: 20, Name: "second", PeerURLs: []string{"http://second:2380"}},
	})

	resp, err := server.MemberList(context.Background(), &etcdserverpb.MemberListRequest{})
	require.NoError(t, err)
	require.Len(t, resp.Members, 3)
	require.Equal(t, []uint64{10, 20, 30}, []uint64{
		resp.Members[0].ID,
		resp.Members[1].ID,
		resp.Members[2].ID,
	})

	rec := httptest.NewRecorder()
	server.peerMembersHandler(rec, httptest.NewRequest(http.MethodGet, "/members", nil))
	require.Equal(t, http.StatusOK, rec.Code)
	var peerMembers []peerHTTPMember
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &peerMembers))
	require.Len(t, peerMembers, 3)
	require.Equal(t, []uint64{10, 20, 30}, []uint64{
		peerMembers[0].ID,
		peerMembers[1].ID,
		peerMembers[2].ID,
	})

	// Sorting snapshots must not mutate the DBaaS control-plane registry.
	require.Equal(t, []uint64{30, 10, 20}, []uint64{
		server.staticMembers[0].ID,
		server.staticMembers[1].ID,
		server.staticMembers[2].ID,
	})
}

func TestMemberListAndPeerMembersSortFallbackMembersByID(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	localIdentity := server.backend.GetResourceLock().Identity()
	localID := server.memberIDForPeerIdentity(localIdentity)
	var leaderIdentity string
	for i := 1; i <= 10_000; i++ {
		candidate := fmt.Sprintf("fallback-leader-%d:2380", i)
		if server.memberIDForPeerIdentity(candidate) < localID {
			leaderIdentity = candidate
			break
		}
	}
	require.NotEmpty(t, leaderIdentity, "test needs a leader whose member ID sorts before the local member")
	leaderID := server.memberIDForPeerIdentity(leaderIdentity)
	server.peers = testPeerService{leaderInfo: leaderIdentity}

	resp, err := server.MemberList(context.Background(), &etcdserverpb.MemberListRequest{})
	require.NoError(t, err)
	require.Len(t, resp.Members, 2)
	require.Equal(t, []uint64{leaderID, localID}, []uint64{
		resp.Members[0].ID,
		resp.Members[1].ID,
	})

	rec := httptest.NewRecorder()
	server.peerMembersHandler(rec, httptest.NewRequest(http.MethodGet, "/members", nil))
	require.Equal(t, http.StatusOK, rec.Code)
	var peerMembers []peerHTTPMember
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &peerMembers))
	require.Len(t, peerMembers, 2)
	require.Equal(t, []uint64{leaderID, localID}, []uint64{
		peerMembers[0].ID,
		peerMembers[1].ID,
	})
}

func TestPeerMembersHandlerRejectsBadRequests(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()
	tests := []struct {
		name          string
		method        string
		target        string
		wantStatus    int
		wantBody      string
		wantClusterID bool
	}{
		{name: "wrong method", method: http.MethodPost, target: "/members", wantStatus: http.StatusMethodNotAllowed, wantBody: "Method Not Allowed"},
		{name: "wrong path", method: http.MethodGet, target: "/members/extra", wantStatus: http.StatusBadRequest, wantBody: "bad path", wantClusterID: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rec := httptest.NewRecorder()
			server.peerMembersHandler(rec, httptest.NewRequest(tt.method, tt.target, nil))
			require.Equal(t, tt.wantStatus, rec.Code)
			require.Contains(t, rec.Body.String(), tt.wantBody)
			if tt.wantClusterID {
				require.Equal(t, strconv.FormatUint(server.backend.ClusterID(), 16), rec.Header().Get(etcdClusterIDHeader))
			} else {
				require.Empty(t, rec.Header().Get(etcdClusterIDHeader))
			}
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
		name          string
		method        string
		target        string
		wantStatus    int
		wantBody      string
		wantClusterID bool
	}{
		{name: "wrong method", method: http.MethodPost, target: "/downgrade/enabled", wantStatus: http.StatusMethodNotAllowed, wantBody: "Method Not Allowed"},
		{name: "wrong path", method: http.MethodGet, target: "/downgrade/enabled/extra", wantStatus: http.StatusBadRequest, wantBody: "bad path", wantClusterID: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rec := httptest.NewRecorder()
			server.peerDowngradeEnabledHandler(rec, httptest.NewRequest(tt.method, tt.target, nil))
			require.Equal(t, tt.wantStatus, rec.Code)
			require.Contains(t, rec.Body.String(), tt.wantBody)
			if tt.wantClusterID {
				require.Equal(t, strconv.FormatUint(server.backend.ClusterID(), 16), rec.Header().Get(etcdClusterIDHeader))
			} else {
				require.Empty(t, rec.Header().Get(etcdClusterIDHeader))
			}
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

	for _, tc := range []struct {
		urls []string
		want string
	}{
		{urls: []string{"etcd.example.com:2379"}, want: `invalid advertised client URL "etcd.example.com:2379"`},
		{urls: []string{"ftp://etcd.example.com:2379"}, want: `invalid advertised client URL "ftp://etcd.example.com:2379"`},
		{urls: []string{"https://etcd.example.com"}, want: `invalid advertised client URL "https://etcd.example.com": host and port are required`},
		{urls: []string{"https://user@etcd.example.com:2379"}, want: `invalid advertised client URL "https://user@etcd.example.com:2379"`},
		{urls: []string{"https://etcd.example.com:2379/v3"}, want: `invalid advertised client URL "https://etcd.example.com:2379/v3"`},
		{urls: []string{"https://etcd.example.com:2379?tenant=a"}, want: `invalid advertised client URL "https://etcd.example.com:2379?tenant=a"`},
		{urls: []string{"https://etcd.example.com:2379", "https://etcd.example.com:2379/"}, want: `duplicate advertised client URL "https://etcd.example.com:2379"`},
	} {
		_, err := ValidateAdvertiseClientURLs(tc.urls)
		require.EqualError(t, err, tc.want)
	}
}

func TestValidateAdvertiseClientURLsRejectsUnsafeCharacters(t *testing.T) {
	for _, tc := range []struct {
		name string
		urls []string
	}{
		{name: "newline", urls: []string{"https://etcd.example.com\n:2379"}},
		{name: "tab", urls: []string{"https://etcd.example.com\t:2379"}},
		{name: "DEL", urls: []string{"https://etcd.example.com\x7f:2379"}},
		{name: "quote", urls: []string{"https://etcd.example.com\":2379"}},
		{name: "backslash", urls: []string{"https://etcd.example.com\\:2379"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := ValidateAdvertiseClientURLs(tc.urls)
			require.EqualError(t, err, fmt.Sprintf("invalid advertised client URL %q: unsupported characters", tc.urls[0]))
		})
	}
}

func TestParseInitialClusterRejectsInvalidConfiguration(t *testing.T) {
	for _, tc := range []struct {
		spec string
		want string
	}{
		{spec: "missing-url", want: `invalid initial-cluster entry "missing-url": want name=http[s]://host:peerPort`},
		{spec: "a=10.0.0.1:2380", want: `invalid peer URL "10.0.0.1:2380" for member "a"`},
		{spec: "a=http://10.0.0.1", want: `invalid peer URL "http://10.0.0.1" for member "a": host and port are required`},
		{spec: "a=http://10.0.0.1:2380/path", want: `invalid peer URL "http://10.0.0.1:2380/path" for member "a"`},
		{spec: "a=http://10.0.0.1:2380,a=http://10.0.0.1:2380", want: `duplicate initial-cluster peer identity "10.0.0.1:2380"`},
		{spec: "a=http://10.0.0.1:2380,b=http://10.0.0.1:2380", want: `duplicate initial-cluster peer identity "10.0.0.1:2380"`},
	} {
		_, err := ParseInitialCluster(tc.spec, 2379, false)
		require.EqualError(t, err, tc.want)
	}
}

func TestParseInitialClusterRejectsUnsafePeerURLCharacters(t *testing.T) {
	for _, tc := range []struct {
		name string
		spec string
	}{
		{name: "newline", spec: "a=http://10.0.0.1\n:2380"},
		{name: "tab", spec: "a=http://10.0.0.1\t:2380"},
		{name: "DEL", spec: "a=http://10.0.0.1\x7f:2380"},
		{name: "quote", spec: "a=http://10.0.0.1\":2380"},
		{name: "backslash", spec: "a=http://10.0.0.1\\:2380"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := ParseInitialCluster(tc.spec, 2379, false)
			rawURL := strings.TrimPrefix(tc.spec, "a=")
			require.EqualError(t, err, fmt.Sprintf("invalid peer URL %q for member %q: unsupported characters", rawURL, "a"))
		})
	}
}

func TestMemberMutationIsUnsupported(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()
	server.SetStaticMembers([]*etcdserverpb.Member{{
		ID: 1, Name: "learner", PeerURLs: []string{"http://127.0.0.1:2380"}, IsLearner: true,
	}})

	ctx := context.Background()
	_, err := server.MemberAdd(ctx, validMemberAddRequest())
	requireClusterPlatformReplacementError(t, err)
	_, err = server.MemberRemove(ctx, &etcdserverpb.MemberRemoveRequest{ID: 1})
	requireClusterPlatformReplacementError(t, err)
	_, err = server.MemberUpdate(ctx, &etcdserverpb.MemberUpdateRequest{ID: 1})
	requireClusterPlatformReplacementError(t, err)
	_, err = server.MemberPromote(ctx, &etcdserverpb.MemberPromoteRequest{ID: 1})
	requireClusterPlatformReplacementError(t, err)
}

func TestMemberAddRejectsMalformedPeerURLsBeforePlatformBoundary(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	for _, request := range []*etcdserverpb.MemberAddRequest{
		{},
		{PeerURLs: []string{"not-a-peer-url"}},
	} {
		response, err := server.MemberAdd(context.Background(), request)
		require.Nil(t, response)
		require.ErrorIs(t, err, rpctypes.ErrGRPCMemberBadURLs)
		require.Equal(t, status.Code(rpctypes.ErrGRPCMemberBadURLs), status.Code(err))
		require.Equal(t, status.Convert(rpctypes.ErrGRPCMemberBadURLs).Message(), status.Convert(err).Message())
	}
}

func TestMemberAddRejectsPeerURLConflictBeforePlatformBoundary(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()
	server.SetStaticMembers([]*etcdserverpb.Member{
		{ID: 11, Name: "first", PeerURLs: []string{"http://127.0.0.1:2380"}},
		{ID: 12, Name: "second", PeerURLs: []string{"http://127.0.0.2:2380"}},
	})

	response, err := server.MemberAdd(context.Background(), &etcdserverpb.MemberAddRequest{
		PeerURLs: []string{"http://127.0.0.1:2380", "http://127.0.0.3:2380"},
	})
	require.Nil(t, response)
	require.ErrorIs(t, err, rpctypes.ErrGRPCPeerURLExist)
	require.Equal(t, codes.FailedPrecondition, status.Code(err))

	response, err = server.MemberAdd(context.Background(), &etcdserverpb.MemberAddRequest{
		PeerURLs: []string{" \thttp://127.0.0.1:2380\n ", "http://127.0.0.3:2380"},
	})
	require.Nil(t, response)
	require.ErrorIs(t, err, rpctypes.ErrGRPCPeerURLExist)
	require.Equal(t, codes.FailedPrecondition, status.Code(err))

	response, err = server.MemberAdd(context.Background(), &etcdserverpb.MemberAddRequest{
		PeerURLs: []string{" \thttp://127.0.0.3:2380\n "},
	})
	require.Nil(t, response)
	requireClusterPlatformReplacementError(t, err)
}

func TestMemberMutationsClassifyKnownMemberStateBeforePlatformBoundary(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()
	server.SetStaticMembers([]*etcdserverpb.Member{
		{ID: 11, Name: "voter", PeerURLs: []string{"http://127.0.0.1:2380"}},
		{ID: 12, Name: "learner", PeerURLs: []string{"http://127.0.0.2:2380"}, IsLearner: true},
	})

	for _, call := range []func() error{
		func() error {
			_, err := server.MemberRemove(context.Background(), &etcdserverpb.MemberRemoveRequest{ID: 99})
			return err
		},
		func() error {
			_, err := server.MemberUpdate(context.Background(), &etcdserverpb.MemberUpdateRequest{ID: 99})
			return err
		},
		func() error {
			_, err := server.MemberPromote(context.Background(), &etcdserverpb.MemberPromoteRequest{ID: 99})
			return err
		},
	} {
		err := call()
		require.ErrorIs(t, err, rpctypes.ErrGRPCMemberNotFound)
		require.Equal(t, codes.NotFound, status.Code(err))
	}

	response, err := server.MemberPromote(context.Background(), &etcdserverpb.MemberPromoteRequest{ID: 11})
	require.Nil(t, response)
	require.ErrorIs(t, err, rpctypes.ErrGRPCMemberNotLearner)
	require.Equal(t, codes.FailedPrecondition, status.Code(err))
}

func TestMemberUpdateRejectsPeerURLConflictBeforePlatformBoundary(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()
	server.SetStaticMembers([]*etcdserverpb.Member{
		{ID: 11, Name: "first", PeerURLs: []string{"http://127.0.0.1:2380"}},
		{ID: 12, Name: "second", PeerURLs: []string{"http://127.0.0.2:2380"}},
	})

	response, err := server.MemberUpdate(context.Background(), &etcdserverpb.MemberUpdateRequest{
		ID: 11, PeerURLs: []string{"http://127.0.0.2:2380"},
	})
	require.Nil(t, response)
	require.ErrorIs(t, err, rpctypes.ErrGRPCPeerURLExist)
	require.Equal(t, codes.FailedPrecondition, status.Code(err))

	response, err = server.MemberUpdate(context.Background(), &etcdserverpb.MemberUpdateRequest{
		ID: 11, PeerURLs: []string{"http://127.0.0.1:2380"},
	})
	require.Nil(t, response)
	requireClusterPlatformReplacementError(t, err)
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
	requireClusterReadBarrierError(t, err, wantErr.Error())
	require.EqualValues(t, 1, calls.Load())
}

func TestMemberListLinearizableHeaderRevisionMatchesEtcd(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()
	var calls atomic.Int32
	server.peers = testPeerService{isLeader: true, syncReadFn: func(context.Context) error {
		calls.Add(1)
		return nil
	}}

	ctx := context.Background()
	put, err := server.Put(ctx, &etcdserverpb.PutRequest{
		Key: []byte("/registry/cluster/member-list-header"), Value: []byte("value"),
	})
	require.NoError(t, err)
	require.Positive(t, put.Header.Revision)

	for _, linearizable := range []bool{false, true} {
		resp, err := server.MemberList(ctx, &etcdserverpb.MemberListRequest{Linearizable: linearizable})
		require.NoError(t, err)
		require.NotNil(t, resp.Header)
		require.Zero(t, resp.Header.Revision, "etcd MemberList headers do not expose MVCC revision")
		require.NotZero(t, resp.Header.ClusterId)
		require.NotZero(t, resp.Header.MemberId)
		require.NotEmpty(t, resp.Members)
	}
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

func TestMemberListLinearizableReadBarrierPrecedesAuthentication(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()
	aliceCtx := setupAuthKVUser(t, server)

	var calls atomic.Int32
	barrierErr := errors.New("member list read barrier failed")
	server.peers = testPeerService{syncReadFn: func(context.Context) error {
		calls.Add(1)
		return barrierErr
	}}

	_, err := server.MemberList(context.Background(), &etcdserverpb.MemberListRequest{Linearizable: true})
	requireClusterReadBarrierError(t, err, barrierErr.Error())
	require.EqualValues(t, 1, calls.Load())

	_, err = server.MemberList(context.Background(), &etcdserverpb.MemberListRequest{})
	requireClusterAuthError(t, err, rpctypes.ErrUserEmpty, codes.Unknown, "etcdserver: user name is empty")
	require.EqualValues(t, 1, calls.Load(), "serializable member list must not run a read barrier")

	_, err = server.MemberList(aliceCtx, &etcdserverpb.MemberListRequest{Linearizable: true})
	requireClusterReadBarrierError(t, err, barrierErr.Error())
	require.EqualValues(t, 2, calls.Load())
}

func TestMemberAuthorizationMatchesEtcd(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()
	aliceCtx := setupAuthKVUser(t, server)
	plain := context.Background()

	_, err := server.MemberList(plain, &etcdserverpb.MemberListRequest{})
	requireClusterAuthError(t, err, rpctypes.ErrUserEmpty, codes.Unknown, "etcdserver: user name is empty")
	_, err = server.MemberList(aliceCtx, &etcdserverpb.MemberListRequest{})
	require.NoError(t, err)
	_, err = server.MemberAdd(aliceCtx, validMemberAddRequest())
	requireClusterAuthError(t, err, rpctypes.ErrPermissionDenied, codes.Unknown, "etcdserver: permission denied")

	rootToken, err := server.tokens.authenticate(plain, "root", "root-secret")
	require.NoError(t, err)
	rootCtx := metadata.NewIncomingContext(plain, metadata.Pairs(rpctypes.TokenFieldNameGRPC, rootToken))
	_, err = server.MemberAdd(rootCtx, validMemberAddRequest())
	requireClusterPlatformReplacementError(t, err)
}

func TestMemberRootAuthorizationClientCertificateErrorsMatchEtcd(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()
	setupAuthKVUser(t, server)
	server.SetClientCertAuth(true)
	ctx := context.Background()

	_, err := server.MemberAdd(verifiedTLSContext(ctx, ""), validMemberAddRequest())
	requireClusterAuthError(t, err, rpctypes.ErrUserEmpty, codes.Unknown, "etcdserver: user name is empty")
	_, err = server.MemberAdd(verifiedTLSContext(ctx, "external-cn"), validMemberAddRequest())
	requireClusterAuthError(t, err, rpctypes.ErrUserNotFound, codes.Unknown, "etcdserver: user name not found")
	_, err = server.MemberAdd(verifiedTLSContext(ctx, "alice"), validMemberAddRequest())
	requireClusterAuthError(t, err, rpctypes.ErrPermissionDenied, codes.Unknown, "etcdserver: permission denied")
	_, err = server.MemberAdd(verifiedTLSContext(ctx, "root"), validMemberAddRequest())
	requireClusterPlatformReplacementError(t, err)
}

func validMemberAddRequest() *etcdserverpb.MemberAddRequest {
	return &etcdserverpb.MemberAddRequest{PeerURLs: []string{"http://127.0.0.1:12380"}}
}

func requireClusterPlatformReplacementError(t *testing.T, err error) {
	t.Helper()
	require.EqualError(t, err, status.Error(codes.Unimplemented, memberMutationUnsupportedMessage).Error())
	require.Equal(t, codes.Unimplemented, status.Code(err))
	require.Equal(t, memberMutationUnsupportedMessage, status.Convert(err).Message())
}

func requireClusterReadBarrierError(t *testing.T, err error, message string) {
	t.Helper()
	require.EqualError(t, err, status.Error(codes.Unavailable, message).Error())
	require.Equal(t, codes.Unavailable, status.Code(err))
	require.Equal(t, message, status.Convert(err).Message())
}

func requireClusterAuthError(t *testing.T, err error, want error, code codes.Code, message string) {
	t.Helper()
	require.EqualError(t, err, message)
	require.ErrorIs(t, err, want)
	require.Equal(t, code, status.Code(err))
	require.Equal(t, message, status.Convert(err).Message())
}
