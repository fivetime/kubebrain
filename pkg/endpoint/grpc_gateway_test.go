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

package endpoint

import (
	"bufio"
	"context"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/authpb"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/mvccpb"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	"go.etcd.io/etcd/server/v3/etcdserver/api/v3election/v3electionpb"
	"go.etcd.io/etcd/server/v3/etcdserver/api/v3lock/v3lockpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/test/bufconn"
)

type gatewayKVServer struct {
	etcdserverpb.UnimplementedKVServer
	request            *etcdserverpb.RangeRequest
	putRequest         *etcdserverpb.PutRequest
	deleteRangeRequest *etcdserverpb.DeleteRangeRequest
	txnRequest         *etcdserverpb.TxnRequest
	compactRequest     *etcdserverpb.CompactionRequest
	md                 metadata.MD
	putMD              metadata.MD
	deleteRangeMD      metadata.MD
	txnMD              metadata.MD
	compactMD          metadata.MD
}

type gatewayClusterServer struct {
	etcdserverpb.UnimplementedClusterServer
	request *etcdserverpb.MemberListRequest
	md      metadata.MD
}

type gatewayMaintenanceServer struct {
	etcdserverpb.UnimplementedMaintenanceServer
	request *etcdserverpb.StatusRequest
	md      metadata.MD
}

type gatewayAuthServer struct {
	etcdserverpb.UnimplementedAuthServer
	request                     *etcdserverpb.AuthStatusRequest
	enableRequest               *etcdserverpb.AuthEnableRequest
	disableRequest              *etcdserverpb.AuthDisableRequest
	authenticateRequest         *etcdserverpb.AuthenticateRequest
	userAddRequest              *etcdserverpb.AuthUserAddRequest
	userGetRequest              *etcdserverpb.AuthUserGetRequest
	userListRequest             *etcdserverpb.AuthUserListRequest
	userDeleteRequest           *etcdserverpb.AuthUserDeleteRequest
	userChangePasswordRequest   *etcdserverpb.AuthUserChangePasswordRequest
	userGrantRoleRequest        *etcdserverpb.AuthUserGrantRoleRequest
	userRevokeRoleRequest       *etcdserverpb.AuthUserRevokeRoleRequest
	roleAddRequest              *etcdserverpb.AuthRoleAddRequest
	roleGetRequest              *etcdserverpb.AuthRoleGetRequest
	roleListRequest             *etcdserverpb.AuthRoleListRequest
	roleDeleteRequest           *etcdserverpb.AuthRoleDeleteRequest
	roleGrantPermissionRequest  *etcdserverpb.AuthRoleGrantPermissionRequest
	roleRevokePermissionRequest *etcdserverpb.AuthRoleRevokePermissionRequest
	md                          metadata.MD
	enableMD                    metadata.MD
	disableMD                   metadata.MD
	authenticateMD              metadata.MD
	userAddMD                   metadata.MD
	userGetMD                   metadata.MD
	userListMD                  metadata.MD
	userDeleteMD                metadata.MD
	userChangePasswordMD        metadata.MD
	userGrantRoleMD             metadata.MD
	userRevokeRoleMD            metadata.MD
	roleAddMD                   metadata.MD
	roleGetMD                   metadata.MD
	roleListMD                  metadata.MD
	roleDeleteMD                metadata.MD
	roleGrantPermissionMD       metadata.MD
	roleRevokePermissionMD      metadata.MD
}

type gatewayLockServer struct {
	v3lockpb.UnimplementedLockServer
	request *v3lockpb.UnlockRequest
	md      metadata.MD
}

type gatewayWatchServer struct {
	etcdserverpb.UnimplementedWatchServer
	md   metadata.MD
	next <-chan struct{}
}

func (s *gatewayWatchServer) Watch(stream etcdserverpb.Watch_WatchServer) error {
	if md, ok := metadata.FromIncomingContext(stream.Context()); ok {
		s.md = md.Copy()
	}
	request, err := stream.Recv()
	if err != nil {
		return err
	}
	if request.GetCreateRequest() == nil {
		return nil
	}
	if _, err := stream.Recv(); err != io.EOF {
		return err
	}
	if err := stream.Send(&etcdserverpb.WatchResponse{
		Header: &etcdserverpb.ResponseHeader{Revision: 31}, WatchId: 7, Created: true,
	}); err != nil {
		return err
	}
	select {
	case <-s.next:
	case <-stream.Context().Done():
		return stream.Context().Err()
	}
	return stream.Send(&etcdserverpb.WatchResponse{
		Header:  &etcdserverpb.ResponseHeader{Revision: 32},
		WatchId: 7,
		Events: []*mvccpb.Event{{
			Type: mvccpb.PUT,
			Kv:   &mvccpb.KeyValue{Key: []byte("watch-key"), Value: []byte("watch-value"), ModRevision: 32},
		}},
	})
}

type gatewayElectionServer struct {
	v3electionpb.UnimplementedElectionServer
	request *v3electionpb.LeaderRequest
	md      metadata.MD
	next    <-chan struct{}
}

type gatewayLeaseServer struct {
	etcdserverpb.UnimplementedLeaseServer
	grantRequest      *etcdserverpb.LeaseGrantRequest
	revokeRequest     *etcdserverpb.LeaseRevokeRequest
	timeToLiveRequest *etcdserverpb.LeaseTimeToLiveRequest
	leasesRequest     *etcdserverpb.LeaseLeasesRequest
	grantMD           metadata.MD
	revokeMD          metadata.MD
	timeToLiveMD      metadata.MD
	leasesMD          metadata.MD
	md                metadata.MD
}

type gatewayBlockingLockServer struct {
	v3lockpb.UnimplementedLockServer
	started  chan struct{}
	canceled chan struct{}
}

func (s *gatewayBlockingLockServer) Lock(ctx context.Context, _ *v3lockpb.LockRequest) (*v3lockpb.LockResponse, error) {
	close(s.started)
	<-ctx.Done()
	close(s.canceled)
	return nil, ctx.Err()
}

type gatewayBlockingElectionServer struct {
	v3electionpb.UnimplementedElectionServer
	started  chan struct{}
	canceled chan struct{}
}

func (s *gatewayBlockingElectionServer) Campaign(ctx context.Context, _ *v3electionpb.CampaignRequest) (*v3electionpb.CampaignResponse, error) {
	close(s.started)
	<-ctx.Done()
	close(s.canceled)
	return nil, ctx.Err()
}

func (s *gatewayLeaseServer) LeaseKeepAlive(stream etcdserverpb.Lease_LeaseKeepAliveServer) error {
	if md, ok := metadata.FromIncomingContext(stream.Context()); ok {
		s.md = md.Copy()
	}
	for {
		request, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		if err := stream.Send(&etcdserverpb.LeaseKeepAliveResponse{
			Header: &etcdserverpb.ResponseHeader{Revision: request.ID + 50},
			ID:     request.ID,
			TTL:    request.ID + 10,
		}); err != nil {
			return err
		}
	}
}

func (s *gatewayLeaseServer) LeaseGrant(ctx context.Context, request *etcdserverpb.LeaseGrantRequest) (*etcdserverpb.LeaseGrantResponse, error) {
	s.grantRequest = request
	if md, ok := metadata.FromIncomingContext(ctx); ok {
		s.grantMD = md.Copy()
	}
	return &etcdserverpb.LeaseGrantResponse{
		Header: &etcdserverpb.ResponseHeader{ClusterId: 81, MemberId: 82, Revision: 83, RaftTerm: 84},
		ID:     request.ID,
		TTL:    request.TTL,
		Error:  "lease warning",
	}, nil
}

func (s *gatewayLeaseServer) LeaseRevoke(ctx context.Context, request *etcdserverpb.LeaseRevokeRequest) (*etcdserverpb.LeaseRevokeResponse, error) {
	s.revokeRequest = request
	if md, ok := metadata.FromIncomingContext(ctx); ok {
		s.revokeMD = md.Copy()
	}
	return &etcdserverpb.LeaseRevokeResponse{
		Header: &etcdserverpb.ResponseHeader{ClusterId: 85, MemberId: 86, Revision: 87, RaftTerm: 88},
	}, nil
}

func (s *gatewayLeaseServer) LeaseTimeToLive(ctx context.Context, request *etcdserverpb.LeaseTimeToLiveRequest) (*etcdserverpb.LeaseTimeToLiveResponse, error) {
	s.timeToLiveRequest = request
	if md, ok := metadata.FromIncomingContext(ctx); ok {
		s.timeToLiveMD = md.Copy()
	}
	return &etcdserverpb.LeaseTimeToLiveResponse{
		Header:     &etcdserverpb.ResponseHeader{ClusterId: 89, MemberId: 90, Revision: 91, RaftTerm: 92},
		ID:         request.ID,
		TTL:        93,
		GrantedTTL: 94,
		Keys:       [][]byte{[]byte("lease-key")},
	}, nil
}

func (s *gatewayLeaseServer) LeaseLeases(ctx context.Context, request *etcdserverpb.LeaseLeasesRequest) (*etcdserverpb.LeaseLeasesResponse, error) {
	s.leasesRequest = request
	if md, ok := metadata.FromIncomingContext(ctx); ok {
		s.leasesMD = md.Copy()
	}
	return &etcdserverpb.LeaseLeasesResponse{
		Header: &etcdserverpb.ResponseHeader{ClusterId: 95, MemberId: 96, Revision: 97, RaftTerm: 98},
		Leases: []*etcdserverpb.LeaseStatus{{ID: 99}, {ID: 100}},
	}, nil
}

func (s *gatewayElectionServer) Observe(request *v3electionpb.LeaderRequest, stream v3electionpb.Election_ObserveServer) error {
	if md, ok := metadata.FromIncomingContext(stream.Context()); ok {
		s.md = md.Copy()
	}
	if err := stream.Send(&v3electionpb.LeaderResponse{
		Header: &etcdserverpb.ResponseHeader{Revision: 41},
		Kv:     &mvccpb.KeyValue{Key: request.Name, Value: []byte("leader-one"), ModRevision: 41},
	}); err != nil {
		return err
	}
	select {
	case <-s.next:
	case <-stream.Context().Done():
		return stream.Context().Err()
	}
	return stream.Send(&v3electionpb.LeaderResponse{
		Header: &etcdserverpb.ResponseHeader{Revision: 42},
		Kv:     &mvccpb.KeyValue{Key: request.Name, Value: []byte("leader-two"), ModRevision: 42},
	})
}

func (s *gatewayElectionServer) Leader(ctx context.Context, request *v3electionpb.LeaderRequest) (*v3electionpb.LeaderResponse, error) {
	s.request = request
	if md, ok := metadata.FromIncomingContext(ctx); ok {
		s.md = md.Copy()
	}
	return &v3electionpb.LeaderResponse{
		Header: &etcdserverpb.ResponseHeader{ClusterId: 31, MemberId: 32, Revision: 33, RaftTerm: 34},
		Kv:     &mvccpb.KeyValue{Key: request.Name, Value: []byte("leader"), ModRevision: 33},
	}, nil
}

func (s *gatewayLockServer) Unlock(ctx context.Context, request *v3lockpb.UnlockRequest) (*v3lockpb.UnlockResponse, error) {
	s.request = request
	if md, ok := metadata.FromIncomingContext(ctx); ok {
		s.md = md.Copy()
	}
	return &v3lockpb.UnlockResponse{
		Header: &etcdserverpb.ResponseHeader{ClusterId: 21, MemberId: 22, Revision: 23, RaftTerm: 24},
	}, nil
}

func (s *gatewayKVServer) Range(ctx context.Context, request *etcdserverpb.RangeRequest) (*etcdserverpb.RangeResponse, error) {
	s.request = request
	if md, ok := metadata.FromIncomingContext(ctx); ok {
		s.md = md.Copy()
	}
	return &etcdserverpb.RangeResponse{
		Header: &etcdserverpb.ResponseHeader{ClusterId: 11, MemberId: 12, Revision: 13, RaftTerm: 14},
		Kvs: []*mvccpb.KeyValue{{
			Key:            []byte("a"),
			CreateRevision: 2,
			ModRevision:    3,
			Version:        4,
			Value:          []byte("value"),
		}},
		Count: 1,
	}, nil
}

func (s *gatewayKVServer) Put(ctx context.Context, request *etcdserverpb.PutRequest) (*etcdserverpb.PutResponse, error) {
	s.putRequest = request
	if md, ok := metadata.FromIncomingContext(ctx); ok {
		s.putMD = md.Copy()
	}
	return &etcdserverpb.PutResponse{
		Header: &etcdserverpb.ResponseHeader{ClusterId: 111, MemberId: 112, Revision: 113, RaftTerm: 114},
		PrevKv: &mvccpb.KeyValue{
			Key:            request.Key,
			CreateRevision: 101,
			ModRevision:    102,
			Version:        103,
			Value:          []byte("old-value"),
			Lease:          104,
		},
	}, nil
}

func (s *gatewayKVServer) DeleteRange(ctx context.Context, request *etcdserverpb.DeleteRangeRequest) (*etcdserverpb.DeleteRangeResponse, error) {
	s.deleteRangeRequest = request
	if md, ok := metadata.FromIncomingContext(ctx); ok {
		s.deleteRangeMD = md.Copy()
	}
	return &etcdserverpb.DeleteRangeResponse{
		Header:  &etcdserverpb.ResponseHeader{ClusterId: 115, MemberId: 116, Revision: 117, RaftTerm: 118},
		Deleted: 2,
		PrevKvs: []*mvccpb.KeyValue{{
			Key:         request.Key,
			ModRevision: 119,
			Value:       []byte("deleted-value"),
		}},
	}, nil
}

func (s *gatewayKVServer) Txn(ctx context.Context, request *etcdserverpb.TxnRequest) (*etcdserverpb.TxnResponse, error) {
	s.txnRequest = request
	if md, ok := metadata.FromIncomingContext(ctx); ok {
		s.txnMD = md.Copy()
	}
	return &etcdserverpb.TxnResponse{
		Header:    &etcdserverpb.ResponseHeader{ClusterId: 120, MemberId: 121, Revision: 122, RaftTerm: 123},
		Succeeded: true,
		Responses: []*etcdserverpb.ResponseOp{{
			Response: &etcdserverpb.ResponseOp_ResponsePut{
				ResponsePut: &etcdserverpb.PutResponse{
					Header: &etcdserverpb.ResponseHeader{Revision: 122},
				},
			},
		}},
	}, nil
}

func (s *gatewayKVServer) Compact(ctx context.Context, request *etcdserverpb.CompactionRequest) (*etcdserverpb.CompactionResponse, error) {
	s.compactRequest = request
	if md, ok := metadata.FromIncomingContext(ctx); ok {
		s.compactMD = md.Copy()
	}
	return &etcdserverpb.CompactionResponse{
		Header: &etcdserverpb.ResponseHeader{ClusterId: 124, MemberId: 125, Revision: 126, RaftTerm: 127},
	}, nil
}

func (s *gatewayClusterServer) MemberList(ctx context.Context, request *etcdserverpb.MemberListRequest) (*etcdserverpb.MemberListResponse, error) {
	s.request = request
	if md, ok := metadata.FromIncomingContext(ctx); ok {
		s.md = md.Copy()
	}
	return &etcdserverpb.MemberListResponse{
		Header: &etcdserverpb.ResponseHeader{ClusterId: 51, MemberId: 52, Revision: 53, RaftTerm: 54},
		Members: []*etcdserverpb.Member{{
			ID:         55,
			Name:       "member-one",
			PeerURLs:   []string{"http://127.0.0.1:2380"},
			ClientURLs: []string{"http://127.0.0.1:2379"},
			IsLearner:  true,
		}},
	}, nil
}

func (s *gatewayMaintenanceServer) Status(ctx context.Context, request *etcdserverpb.StatusRequest) (*etcdserverpb.StatusResponse, error) {
	s.request = request
	if md, ok := metadata.FromIncomingContext(ctx); ok {
		s.md = md.Copy()
	}
	return &etcdserverpb.StatusResponse{
		Header:           &etcdserverpb.ResponseHeader{ClusterId: 61, MemberId: 62, Revision: 63, RaftTerm: 64},
		Version:          "3.7.0",
		DbSize:           600,
		Leader:           65,
		RaftIndex:        66,
		RaftTerm:         67,
		RaftAppliedIndex: 68,
		Errors:           []string{"alarm active"},
		DbSizeInUse:      69,
		IsLearner:        true,
		StorageVersion:   "3.7.0",
		DbSizeQuota:      70,
	}, nil
}

func (s *gatewayAuthServer) AuthStatus(ctx context.Context, request *etcdserverpb.AuthStatusRequest) (*etcdserverpb.AuthStatusResponse, error) {
	s.request = request
	if md, ok := metadata.FromIncomingContext(ctx); ok {
		s.md = md.Copy()
	}
	return &etcdserverpb.AuthStatusResponse{
		Header:       &etcdserverpb.ResponseHeader{ClusterId: 71, MemberId: 72, Revision: 73, RaftTerm: 74},
		Enabled:      true,
		AuthRevision: 75,
	}, nil
}

func (s *gatewayAuthServer) AuthEnable(ctx context.Context, request *etcdserverpb.AuthEnableRequest) (*etcdserverpb.AuthEnableResponse, error) {
	s.enableRequest = request
	if md, ok := metadata.FromIncomingContext(ctx); ok {
		s.enableMD = md.Copy()
	}
	return &etcdserverpb.AuthEnableResponse{
		Header: &etcdserverpb.ResponseHeader{ClusterId: 181, MemberId: 182, Revision: 183, RaftTerm: 184},
	}, nil
}

func (s *gatewayAuthServer) AuthDisable(ctx context.Context, request *etcdserverpb.AuthDisableRequest) (*etcdserverpb.AuthDisableResponse, error) {
	s.disableRequest = request
	if md, ok := metadata.FromIncomingContext(ctx); ok {
		s.disableMD = md.Copy()
	}
	return &etcdserverpb.AuthDisableResponse{
		Header: &etcdserverpb.ResponseHeader{ClusterId: 185, MemberId: 186, Revision: 187, RaftTerm: 188},
	}, nil
}

func (s *gatewayAuthServer) Authenticate(ctx context.Context, request *etcdserverpb.AuthenticateRequest) (*etcdserverpb.AuthenticateResponse, error) {
	s.authenticateRequest = request
	if md, ok := metadata.FromIncomingContext(ctx); ok {
		s.authenticateMD = md.Copy()
	}
	return &etcdserverpb.AuthenticateResponse{
		Header: &etcdserverpb.ResponseHeader{ClusterId: 131, MemberId: 132, Revision: 133, RaftTerm: 134},
		Token:  "issued-token",
	}, nil
}

func (s *gatewayAuthServer) UserAdd(ctx context.Context, request *etcdserverpb.AuthUserAddRequest) (*etcdserverpb.AuthUserAddResponse, error) {
	s.userAddRequest = request
	if md, ok := metadata.FromIncomingContext(ctx); ok {
		s.userAddMD = md.Copy()
	}
	return &etcdserverpb.AuthUserAddResponse{
		Header: &etcdserverpb.ResponseHeader{ClusterId: 135, MemberId: 136, Revision: 137, RaftTerm: 138},
	}, nil
}

func (s *gatewayAuthServer) UserGet(ctx context.Context, request *etcdserverpb.AuthUserGetRequest) (*etcdserverpb.AuthUserGetResponse, error) {
	s.userGetRequest = request
	if md, ok := metadata.FromIncomingContext(ctx); ok {
		s.userGetMD = md.Copy()
	}
	return &etcdserverpb.AuthUserGetResponse{
		Header: &etcdserverpb.ResponseHeader{ClusterId: 151, MemberId: 152, Revision: 153, RaftTerm: 154},
		Roles:  []string{"reader", "writer"},
	}, nil
}

func (s *gatewayAuthServer) UserList(ctx context.Context, request *etcdserverpb.AuthUserListRequest) (*etcdserverpb.AuthUserListResponse, error) {
	s.userListRequest = request
	if md, ok := metadata.FromIncomingContext(ctx); ok {
		s.userListMD = md.Copy()
	}
	return &etcdserverpb.AuthUserListResponse{
		Header: &etcdserverpb.ResponseHeader{ClusterId: 155, MemberId: 156, Revision: 157, RaftTerm: 158},
		Users:  []string{"alice", "bob"},
	}, nil
}

func (s *gatewayAuthServer) UserDelete(ctx context.Context, request *etcdserverpb.AuthUserDeleteRequest) (*etcdserverpb.AuthUserDeleteResponse, error) {
	s.userDeleteRequest = request
	if md, ok := metadata.FromIncomingContext(ctx); ok {
		s.userDeleteMD = md.Copy()
	}
	return &etcdserverpb.AuthUserDeleteResponse{
		Header: &etcdserverpb.ResponseHeader{ClusterId: 159, MemberId: 160, Revision: 161, RaftTerm: 162},
	}, nil
}

func (s *gatewayAuthServer) UserChangePassword(ctx context.Context, request *etcdserverpb.AuthUserChangePasswordRequest) (*etcdserverpb.AuthUserChangePasswordResponse, error) {
	s.userChangePasswordRequest = request
	if md, ok := metadata.FromIncomingContext(ctx); ok {
		s.userChangePasswordMD = md.Copy()
	}
	return &etcdserverpb.AuthUserChangePasswordResponse{
		Header: &etcdserverpb.ResponseHeader{ClusterId: 163, MemberId: 164, Revision: 165, RaftTerm: 166},
	}, nil
}

func (s *gatewayAuthServer) UserGrantRole(ctx context.Context, request *etcdserverpb.AuthUserGrantRoleRequest) (*etcdserverpb.AuthUserGrantRoleResponse, error) {
	s.userGrantRoleRequest = request
	if md, ok := metadata.FromIncomingContext(ctx); ok {
		s.userGrantRoleMD = md.Copy()
	}
	return &etcdserverpb.AuthUserGrantRoleResponse{
		Header: &etcdserverpb.ResponseHeader{ClusterId: 167, MemberId: 168, Revision: 169, RaftTerm: 170},
	}, nil
}

func (s *gatewayAuthServer) UserRevokeRole(ctx context.Context, request *etcdserverpb.AuthUserRevokeRoleRequest) (*etcdserverpb.AuthUserRevokeRoleResponse, error) {
	s.userRevokeRoleRequest = request
	if md, ok := metadata.FromIncomingContext(ctx); ok {
		s.userRevokeRoleMD = md.Copy()
	}
	return &etcdserverpb.AuthUserRevokeRoleResponse{
		Header: &etcdserverpb.ResponseHeader{ClusterId: 171, MemberId: 172, Revision: 173, RaftTerm: 174},
	}, nil
}

func (s *gatewayAuthServer) RoleAdd(ctx context.Context, request *etcdserverpb.AuthRoleAddRequest) (*etcdserverpb.AuthRoleAddResponse, error) {
	s.roleAddRequest = request
	if md, ok := metadata.FromIncomingContext(ctx); ok {
		s.roleAddMD = md.Copy()
	}
	return &etcdserverpb.AuthRoleAddResponse{
		Header: &etcdserverpb.ResponseHeader{ClusterId: 189, MemberId: 190, Revision: 191, RaftTerm: 192},
	}, nil
}

func (s *gatewayAuthServer) RoleGet(ctx context.Context, request *etcdserverpb.AuthRoleGetRequest) (*etcdserverpb.AuthRoleGetResponse, error) {
	s.roleGetRequest = request
	if md, ok := metadata.FromIncomingContext(ctx); ok {
		s.roleGetMD = md.Copy()
	}
	return &etcdserverpb.AuthRoleGetResponse{
		Header: &etcdserverpb.ResponseHeader{ClusterId: 193, MemberId: 194, Revision: 195, RaftTerm: 196},
		Perm: []*authpb.Permission{{
			PermType: authpb.WRITE,
			Key:      []byte("/registry/"),
			RangeEnd: []byte("/registry0"),
		}},
	}, nil
}

func (s *gatewayAuthServer) RoleList(ctx context.Context, request *etcdserverpb.AuthRoleListRequest) (*etcdserverpb.AuthRoleListResponse, error) {
	s.roleListRequest = request
	if md, ok := metadata.FromIncomingContext(ctx); ok {
		s.roleListMD = md.Copy()
	}
	return &etcdserverpb.AuthRoleListResponse{
		Header: &etcdserverpb.ResponseHeader{ClusterId: 197, MemberId: 198, Revision: 199, RaftTerm: 200},
		Roles:  []string{"reader", "writer"},
	}, nil
}

func (s *gatewayAuthServer) RoleDelete(ctx context.Context, request *etcdserverpb.AuthRoleDeleteRequest) (*etcdserverpb.AuthRoleDeleteResponse, error) {
	s.roleDeleteRequest = request
	if md, ok := metadata.FromIncomingContext(ctx); ok {
		s.roleDeleteMD = md.Copy()
	}
	return &etcdserverpb.AuthRoleDeleteResponse{
		Header: &etcdserverpb.ResponseHeader{ClusterId: 201, MemberId: 202, Revision: 203, RaftTerm: 204},
	}, nil
}

func (s *gatewayAuthServer) RoleGrantPermission(ctx context.Context, request *etcdserverpb.AuthRoleGrantPermissionRequest) (*etcdserverpb.AuthRoleGrantPermissionResponse, error) {
	s.roleGrantPermissionRequest = request
	if md, ok := metadata.FromIncomingContext(ctx); ok {
		s.roleGrantPermissionMD = md.Copy()
	}
	return &etcdserverpb.AuthRoleGrantPermissionResponse{
		Header: &etcdserverpb.ResponseHeader{ClusterId: 139, MemberId: 140, Revision: 141, RaftTerm: 142},
	}, nil
}

func (s *gatewayAuthServer) RoleRevokePermission(ctx context.Context, request *etcdserverpb.AuthRoleRevokePermissionRequest) (*etcdserverpb.AuthRoleRevokePermissionResponse, error) {
	s.roleRevokePermissionRequest = request
	if md, ok := metadata.FromIncomingContext(ctx); ok {
		s.roleRevokePermissionMD = md.Copy()
	}
	return &etcdserverpb.AuthRoleRevokePermissionResponse{
		Header: &etcdserverpb.ResponseHeader{ClusterId: 205, MemberId: 206, Revision: 207, RaftTerm: 208},
	}, nil
}

func TestGRPCGatewaySurfaceIsExplicit(t *testing.T) {
	services := make([]string, 0, len(grpcGatewayRegistrations))
	for _, registration := range grpcGatewayRegistrations {
		require.NotEmpty(t, registration.service)
		require.NotNil(t, registration.register)
		services = append(services, registration.service)
	}

	require.Equal(t, []string{
		etcdserverpb.KV_ServiceDesc.ServiceName,
		etcdserverpb.Watch_ServiceDesc.ServiceName,
		etcdserverpb.Lease_ServiceDesc.ServiceName,
		etcdserverpb.Cluster_ServiceDesc.ServiceName,
		etcdserverpb.Maintenance_ServiceDesc.ServiceName,
		etcdserverpb.Auth_ServiceDesc.ServiceName,
		v3lockpb.Lock_ServiceDesc.ServiceName,
		v3electionpb.Election_ServiceDesc.ServiceName,
	}, services, "review and classify every generated HTTP gateway service when the public surface changes")
}

func TestGRPCGatewayUsesGeneratedEtcdJSONContract(t *testing.T) {
	listener := bufconn.Listen(1024 * 1024)
	grpcServer := grpc.NewServer()
	kvServer := &gatewayKVServer{}
	clusterServer := &gatewayClusterServer{}
	maintenanceServer := &gatewayMaintenanceServer{}
	authServer := &gatewayAuthServer{}
	leaseServer := &gatewayLeaseServer{}
	lockServer := &gatewayLockServer{}
	electionServer := &gatewayElectionServer{}
	etcdserverpb.RegisterKVServer(grpcServer, kvServer)
	etcdserverpb.RegisterClusterServer(grpcServer, clusterServer)
	etcdserverpb.RegisterMaintenanceServer(grpcServer, maintenanceServer)
	etcdserverpb.RegisterAuthServer(grpcServer, authServer)
	etcdserverpb.RegisterLeaseServer(grpcServer, leaseServer)
	v3lockpb.RegisterLockServer(grpcServer, lockServer)
	v3electionpb.RegisterElectionServer(grpcServer, electionServer)
	go func() { _ = grpcServer.Serve(listener) }()
	t.Cleanup(grpcServer.Stop)

	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return listener.Dial()
		}),
	)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, conn.Close()) })

	handler, err := newGRPCGatewayMux(context.Background(), conn)
	require.NoError(t, err)
	request := httptest.NewRequest(http.MethodPost, "/v3/kv/range",
		strings.NewReader(`{"key":"YQ==","limit":"1","unknown_field":"discarded"}`))
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Authorization", "Bearer kv-token")
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)

	require.Equal(t, http.StatusOK, response.Code)
	require.Equal(t, "application/json", response.Header().Get("Content-Type"))
	require.JSONEq(t, `{
		"header":{"cluster_id":"11","member_id":"12","revision":"13","raft_term":"14"},
		"kvs":[{"key":"YQ==","create_revision":"2","mod_revision":"3","version":"4","value":"dmFsdWU="}],
		"count":"1"
	}`, response.Body.String())
	require.NotNil(t, kvServer.request)
	require.Equal(t, []byte("a"), kvServer.request.Key)
	require.Equal(t, int64(1), kvServer.request.Limit)
	require.Empty(t, request.Header.Values("Accept"))
	require.Equal(t, []string{grpcGatewayRequestMarkerValue}, kvServer.md.Get(grpcGatewayRequestMarkerKey))
	require.Equal(t, []string{"Bearer kv-token"}, kvServer.md.Get(rpctypes.TokenFieldNameSwagger))

	assertUnaryContract := func(path, body, token, expectedJSON string, requestSeen func() bool, md *metadata.MD) {
		t.Helper()
		request := httptest.NewRequest(http.MethodPost, path, strings.NewReader(body))
		request.Header.Set("Content-Type", "application/json")
		request.Header.Set("Authorization", token)
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, request)
		require.Equal(t, http.StatusOK, response.Code, response.Body.String())
		require.Equal(t, "application/json", response.Header().Get("Content-Type"))
		require.JSONEq(t, expectedJSON, response.Body.String())
		require.True(t, requestSeen(), path)
		require.Equal(t, []string{grpcGatewayRequestMarkerValue}, md.Get(grpcGatewayRequestMarkerKey))
		require.Equal(t, []string{token}, md.Get(rpctypes.TokenFieldNameSwagger))
	}

	assertUnaryContract("/v3/kv/put",
		`{"key":"cHV0LWtleQ==","value":"bmV3LXZhbHVl","lease":"101","prev_kv":true,"unknown_field":"discarded"}`,
		"Bearer kv-put-token", `{
			"header":{"cluster_id":"111","member_id":"112","revision":"113","raft_term":"114"},
			"prev_kv":{
				"key":"cHV0LWtleQ==",
				"create_revision":"101",
				"mod_revision":"102",
				"version":"103",
				"value":"b2xkLXZhbHVl",
				"lease":"104"
			}
		}`,
		func() bool {
			return kvServer.putRequest != nil &&
				string(kvServer.putRequest.Key) == "put-key" &&
				string(kvServer.putRequest.Value) == "new-value" &&
				kvServer.putRequest.Lease == 101 &&
				kvServer.putRequest.PrevKv
		}, &kvServer.putMD)
	assertUnaryContract("/v3/kv/deleterange",
		`{"key":"ZGVsZXRlLWE=","range_end":"ZGVsZXRlLXo=","prev_kv":true,"unknown_field":"discarded"}`,
		"Bearer kv-delete-token", `{
			"header":{"cluster_id":"115","member_id":"116","revision":"117","raft_term":"118"},
			"deleted":"2",
			"prev_kvs":[{
				"key":"ZGVsZXRlLWE=",
				"mod_revision":"119",
				"value":"ZGVsZXRlZC12YWx1ZQ=="
			}]
		}`,
		func() bool {
			return kvServer.deleteRangeRequest != nil &&
				string(kvServer.deleteRangeRequest.Key) == "delete-a" &&
				string(kvServer.deleteRangeRequest.RangeEnd) == "delete-z" &&
				kvServer.deleteRangeRequest.PrevKv
		}, &kvServer.deleteRangeMD)
	assertUnaryContract("/v3/kv/txn", `{
			"compare":[{
				"key":"dHhuLWtleQ==",
				"target":"VERSION",
				"result":"EQUAL",
				"version":"1",
				"unknown_field":"discarded"
			}],
			"success":[{
				"request_put":{"key":"dHhuLWtleQ==","value":"dHhuLXZhbHVl"}
			}],
			"unknown_field":"discarded"
		}`,
		"Bearer kv-txn-token", `{
			"header":{"cluster_id":"120","member_id":"121","revision":"122","raft_term":"123"},
			"succeeded":true,
			"responses":[{
				"response_put":{"header":{"revision":"122"}}
			}]
		}`,
		func() bool {
			if kvServer.txnRequest == nil ||
				len(kvServer.txnRequest.Compare) != 1 ||
				len(kvServer.txnRequest.Success) != 1 {
				return false
			}
			compare := kvServer.txnRequest.Compare[0]
			put := kvServer.txnRequest.Success[0].GetRequestPut()
			return string(compare.Key) == "txn-key" &&
				compare.Target == etcdserverpb.Compare_VERSION &&
				compare.Result == etcdserverpb.Compare_EQUAL &&
				compare.GetVersion() == 1 &&
				put != nil &&
				string(put.Key) == "txn-key" &&
				string(put.Value) == "txn-value"
		}, &kvServer.txnMD)
	assertUnaryContract("/v3/kv/compaction",
		`{"revision":"108","physical":true,"unknown_field":"discarded"}`,
		"Bearer kv-compact-token", `{
			"header":{"cluster_id":"124","member_id":"125","revision":"126","raft_term":"127"}
		}`,
		func() bool {
			return kvServer.compactRequest != nil &&
				kvServer.compactRequest.Revision == 108 &&
				kvServer.compactRequest.Physical
		}, &kvServer.compactMD)
	assertUnaryContract("/v3/cluster/member/list", `{}`, "Bearer cluster-token", `{
		"header":{"cluster_id":"51","member_id":"52","revision":"53","raft_term":"54"},
		"members":[{
			"ID":"55",
			"name":"member-one",
			"peerURLs":["http://127.0.0.1:2380"],
			"clientURLs":["http://127.0.0.1:2379"],
			"isLearner":true
		}]
	}`,
		func() bool { return clusterServer.request != nil }, &clusterServer.md)
	assertUnaryContract("/v3/maintenance/status", `{}`, "Bearer maintenance-token", `{
		"header":{"cluster_id":"61","member_id":"62","revision":"63","raft_term":"64"},
		"version":"3.7.0",
		"dbSize":"600",
		"leader":"65",
		"raftIndex":"66",
		"raftTerm":"67",
		"raftAppliedIndex":"68",
		"errors":["alarm active"],
		"dbSizeInUse":"69",
		"isLearner":true,
		"storageVersion":"3.7.0",
		"dbSizeQuota":"70"
	}`,
		func() bool { return maintenanceServer.request != nil }, &maintenanceServer.md)
	assertUnaryContract("/v3/auth/status", `{}`, "Bearer auth-token", `{
		"header":{"cluster_id":"71","member_id":"72","revision":"73","raft_term":"74"},
		"enabled":true,
		"authRevision":"75"
	}`,
		func() bool { return authServer.request != nil }, &authServer.md)
	assertUnaryContract("/v3/auth/enable", `{"unknown_field":"discarded"}`,
		"Bearer auth-enable-token", `{
			"header":{"cluster_id":"181","member_id":"182","revision":"183","raft_term":"184"}
		}`,
		func() bool { return authServer.enableRequest != nil }, &authServer.enableMD)
	assertUnaryContract("/v3/auth/disable", `{"unknown_field":"discarded"}`,
		"Bearer auth-disable-token", `{
			"header":{"cluster_id":"185","member_id":"186","revision":"187","raft_term":"188"}
		}`,
		func() bool { return authServer.disableRequest != nil }, &authServer.disableMD)
	assertUnaryContract("/v3/auth/authenticate",
		`{"name":"gateway-user","password":"gateway-password","unknown_field":"discarded"}`,
		"Bearer auth-bootstrap-token", `{
			"header":{"cluster_id":"131","member_id":"132","revision":"133","raft_term":"134"},
			"token":"issued-token"
		}`,
		func() bool {
			return authServer.authenticateRequest != nil &&
				authServer.authenticateRequest.Name == "gateway-user" &&
				authServer.authenticateRequest.Password == "gateway-password"
		}, &authServer.authenticateMD)
	assertUnaryContract("/v3/auth/user/add",
		`{"name":"service-user","options":{"no_password":true,"unknown_field":"discarded"},"unknown_field":"discarded"}`,
		"Bearer auth-user-token", `{
			"header":{"cluster_id":"135","member_id":"136","revision":"137","raft_term":"138"}
		}`,
		func() bool {
			return authServer.userAddRequest != nil &&
				authServer.userAddRequest.Name == "service-user" &&
				authServer.userAddRequest.Options != nil &&
				authServer.userAddRequest.Options.NoPassword
		}, &authServer.userAddMD)
	assertUnaryContract("/v3/auth/user/get",
		`{"name":"service-user","unknown_field":"discarded"}`,
		"Bearer auth-user-get-token", `{
			"header":{"cluster_id":"151","member_id":"152","revision":"153","raft_term":"154"},
			"roles":["reader","writer"]
		}`,
		func() bool {
			return authServer.userGetRequest != nil &&
				authServer.userGetRequest.Name == "service-user"
		}, &authServer.userGetMD)
	assertUnaryContract("/v3/auth/user/list",
		`{"unknown_field":"discarded"}`,
		"Bearer auth-user-list-token", `{
			"header":{"cluster_id":"155","member_id":"156","revision":"157","raft_term":"158"},
			"users":["alice","bob"]
		}`,
		func() bool { return authServer.userListRequest != nil }, &authServer.userListMD)
	assertUnaryContract("/v3/auth/user/delete",
		`{"name":"obsolete-user","unknown_field":"discarded"}`,
		"Bearer auth-user-delete-token", `{
			"header":{"cluster_id":"159","member_id":"160","revision":"161","raft_term":"162"}
		}`,
		func() bool {
			return authServer.userDeleteRequest != nil &&
				authServer.userDeleteRequest.Name == "obsolete-user"
		}, &authServer.userDeleteMD)
	assertUnaryContract("/v3/auth/user/changepw",
		`{"name":"service-user","password":"new-password","hashedPassword":"new-hash","unknown_field":"discarded"}`,
		"Bearer auth-user-password-token", `{
			"header":{"cluster_id":"163","member_id":"164","revision":"165","raft_term":"166"}
		}`,
		func() bool {
			return authServer.userChangePasswordRequest != nil &&
				authServer.userChangePasswordRequest.Name == "service-user" &&
				authServer.userChangePasswordRequest.Password == "new-password" &&
				authServer.userChangePasswordRequest.HashedPassword == "new-hash"
		}, &authServer.userChangePasswordMD)
	assertUnaryContract("/v3/auth/user/grant",
		`{"user":"service-user","role":"writer","unknown_field":"discarded"}`,
		"Bearer auth-user-grant-token", `{
			"header":{"cluster_id":"167","member_id":"168","revision":"169","raft_term":"170"}
		}`,
		func() bool {
			return authServer.userGrantRoleRequest != nil &&
				authServer.userGrantRoleRequest.User == "service-user" &&
				authServer.userGrantRoleRequest.Role == "writer"
		}, &authServer.userGrantRoleMD)
	assertUnaryContract("/v3/auth/user/revoke",
		`{"name":"service-user","role":"reader","unknown_field":"discarded"}`,
		"Bearer auth-user-revoke-token", `{
			"header":{"cluster_id":"171","member_id":"172","revision":"173","raft_term":"174"}
		}`,
		func() bool {
			return authServer.userRevokeRoleRequest != nil &&
				authServer.userRevokeRoleRequest.Name == "service-user" &&
				authServer.userRevokeRoleRequest.Role == "reader"
		}, &authServer.userRevokeRoleMD)
	assertUnaryContract("/v3/auth/role/add",
		`{"name":"auditor","unknown_field":"discarded"}`,
		"Bearer auth-role-add-token", `{
			"header":{"cluster_id":"189","member_id":"190","revision":"191","raft_term":"192"}
		}`,
		func() bool {
			return authServer.roleAddRequest != nil &&
				authServer.roleAddRequest.Name == "auditor"
		}, &authServer.roleAddMD)
	assertUnaryContract("/v3/auth/role/get",
		`{"role":"writer","unknown_field":"discarded"}`,
		"Bearer auth-role-get-token", `{
			"header":{"cluster_id":"193","member_id":"194","revision":"195","raft_term":"196"},
			"perm":[{
				"permType":"WRITE",
				"key":"L3JlZ2lzdHJ5Lw==",
				"range_end":"L3JlZ2lzdHJ5MA=="
			}]
		}`,
		func() bool {
			return authServer.roleGetRequest != nil &&
				authServer.roleGetRequest.Role == "writer"
		}, &authServer.roleGetMD)
	assertUnaryContract("/v3/auth/role/list",
		`{"unknown_field":"discarded"}`,
		"Bearer auth-role-list-token", `{
			"header":{"cluster_id":"197","member_id":"198","revision":"199","raft_term":"200"},
			"roles":["reader","writer"]
		}`,
		func() bool { return authServer.roleListRequest != nil }, &authServer.roleListMD)
	assertUnaryContract("/v3/auth/role/delete",
		`{"role":"obsolete-role","unknown_field":"discarded"}`,
		"Bearer auth-role-delete-token", `{
			"header":{"cluster_id":"201","member_id":"202","revision":"203","raft_term":"204"}
		}`,
		func() bool {
			return authServer.roleDeleteRequest != nil &&
				authServer.roleDeleteRequest.Role == "obsolete-role"
		}, &authServer.roleDeleteMD)
	assertUnaryContract("/v3/auth/role/grant",
		`{"name":"writer","perm":{"permType":"READWRITE","key":"L3JlZ2lzdHJ5Lw==","range_end":"L3JlZ2lzdHJ5MA==","unknown_field":"discarded"},"unknown_field":"discarded"}`,
		"Bearer auth-role-token", `{
			"header":{"cluster_id":"139","member_id":"140","revision":"141","raft_term":"142"}
		}`,
		func() bool {
			if authServer.roleGrantPermissionRequest == nil ||
				authServer.roleGrantPermissionRequest.Perm == nil {
				return false
			}
			permission := authServer.roleGrantPermissionRequest.Perm
			return authServer.roleGrantPermissionRequest.Name == "writer" &&
				permission.PermType == authpb.READWRITE &&
				string(permission.Key) == "/registry/" &&
				string(permission.RangeEnd) == "/registry0"
		}, &authServer.roleGrantPermissionMD)
	assertUnaryContract("/v3/auth/role/revoke",
		`{"role":"writer","key":"L3JlZ2lzdHJ5Lw==","range_end":"L3JlZ2lzdHJ5MA==","unknown_field":"discarded"}`,
		"Bearer auth-role-revoke-token", `{
			"header":{"cluster_id":"205","member_id":"206","revision":"207","raft_term":"208"}
		}`,
		func() bool {
			return authServer.roleRevokePermissionRequest != nil &&
				authServer.roleRevokePermissionRequest.Role == "writer" &&
				string(authServer.roleRevokePermissionRequest.Key) == "/registry/" &&
				string(authServer.roleRevokePermissionRequest.RangeEnd) == "/registry0"
		}, &authServer.roleRevokePermissionMD)
	assertUnaryContract("/v3/lease/grant", `{"TTL":"300","ID":"101","unknown_field":"discarded"}`,
		"Bearer lease-grant-token", `{
			"header":{"cluster_id":"81","member_id":"82","revision":"83","raft_term":"84"},
			"ID":"101",
			"TTL":"300",
			"error":"lease warning"
		}`,
		func() bool {
			return leaseServer.grantRequest != nil &&
				leaseServer.grantRequest.TTL == 300 &&
				leaseServer.grantRequest.ID == 101
		}, &leaseServer.grantMD)
	assertUnaryContract("/v3/lease/revoke", `{"ID":"102","unknown_field":"discarded"}`,
		"Bearer lease-revoke-token", `{
			"header":{"cluster_id":"85","member_id":"86","revision":"87","raft_term":"88"}
		}`,
		func() bool {
			return leaseServer.revokeRequest != nil && leaseServer.revokeRequest.ID == 102
		}, &leaseServer.revokeMD)
	assertUnaryContract("/v3/lease/timetolive", `{"ID":"103","keys":true,"unknown_field":"discarded"}`,
		"Bearer lease-ttl-token", `{
			"header":{"cluster_id":"89","member_id":"90","revision":"91","raft_term":"92"},
			"ID":"103",
			"TTL":"93",
			"grantedTTL":"94",
			"keys":["bGVhc2Uta2V5"]
		}`,
		func() bool {
			return leaseServer.timeToLiveRequest != nil &&
				leaseServer.timeToLiveRequest.ID == 103 &&
				leaseServer.timeToLiveRequest.Keys
		}, &leaseServer.timeToLiveMD)
	assertUnaryContract("/v3/lease/leases", `{"unknown_field":"discarded"}`,
		"Bearer lease-list-token", `{
			"header":{"cluster_id":"95","member_id":"96","revision":"97","raft_term":"98"},
			"leases":[{"ID":"99"},{"ID":"100"}]
		}`,
		func() bool { return leaseServer.leasesRequest != nil }, &leaseServer.leasesMD)
	assertUnaryContract("/v3/kv/lease/revoke", `{"ID":"104","unknown_field":"discarded"}`,
		"Bearer lease-revoke-alias-token", `{
			"header":{"cluster_id":"85","member_id":"86","revision":"87","raft_term":"88"}
		}`,
		func() bool {
			return leaseServer.revokeRequest != nil && leaseServer.revokeRequest.ID == 104
		}, &leaseServer.revokeMD)
	assertUnaryContract("/v3/kv/lease/timetolive", `{"ID":"105","keys":true,"unknown_field":"discarded"}`,
		"Bearer lease-ttl-alias-token", `{
			"header":{"cluster_id":"89","member_id":"90","revision":"91","raft_term":"92"},
			"ID":"105",
			"TTL":"93",
			"grantedTTL":"94",
			"keys":["bGVhc2Uta2V5"]
		}`,
		func() bool {
			return leaseServer.timeToLiveRequest != nil &&
				leaseServer.timeToLiveRequest.ID == 105 &&
				leaseServer.timeToLiveRequest.Keys
		}, &leaseServer.timeToLiveMD)
	assertUnaryContract("/v3/kv/lease/leases", `{"unknown_field":"discarded"}`,
		"Bearer lease-list-alias-token", `{
			"header":{"cluster_id":"95","member_id":"96","revision":"97","raft_term":"98"},
			"leases":[{"ID":"99"},{"ID":"100"}]
		}`,
		func() bool { return leaseServer.leasesRequest != nil }, &leaseServer.leasesMD)

	request = httptest.NewRequest(http.MethodPost, "/v3/lock/unlock",
		strings.NewReader(`{"key":"L2xvY2svMDE=","unknown_field":"discarded"}`))
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Authorization", "Bearer lock-token")
	response = httptest.NewRecorder()
	handler.ServeHTTP(response, request)

	require.Equal(t, http.StatusOK, response.Code)
	require.JSONEq(t, `{
		"header":{"cluster_id":"21","member_id":"22","revision":"23","raft_term":"24"}
	}`, response.Body.String())
	require.NotNil(t, lockServer.request)
	require.Equal(t, []byte("/lock/01"), lockServer.request.Key)
	require.Equal(t, []string{grpcGatewayRequestMarkerValue}, lockServer.md.Get(grpcGatewayRequestMarkerKey))
	require.Equal(t, []string{"Bearer lock-token"}, lockServer.md.Get(rpctypes.TokenFieldNameSwagger))

	request = httptest.NewRequest(http.MethodPost, "/v3/election/leader",
		strings.NewReader(`{"name":"ZWxlY3Rpb24="}`))
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Authorization", "Bearer election-token")
	response = httptest.NewRecorder()
	handler.ServeHTTP(response, request)

	require.Equal(t, http.StatusOK, response.Code)
	require.JSONEq(t, `{
		"header":{"cluster_id":"31","member_id":"32","revision":"33","raft_term":"34"},
		"kv":{"key":"ZWxlY3Rpb24=","mod_revision":"33","value":"bGVhZGVy"}
	}`, response.Body.String())
	require.NotNil(t, electionServer.request)
	require.Equal(t, []byte("election"), electionServer.request.Name)
	require.Equal(t, []string{grpcGatewayRequestMarkerValue}, electionServer.md.Get(grpcGatewayRequestMarkerKey))
	require.Equal(t, []string{"Bearer election-token"}, electionServer.md.Get(rpctypes.TokenFieldNameSwagger))
}

func TestGRPCGatewayStreamsWatchAndElectionResponses(t *testing.T) {
	listener := bufconn.Listen(1024 * 1024)
	grpcServer := grpc.NewServer()
	watchNext := make(chan struct{})
	electionNext := make(chan struct{})
	watchServer := &gatewayWatchServer{next: watchNext}
	leaseServer := &gatewayLeaseServer{}
	electionServer := &gatewayElectionServer{next: electionNext}
	etcdserverpb.RegisterWatchServer(grpcServer, watchServer)
	etcdserverpb.RegisterLeaseServer(grpcServer, leaseServer)
	v3electionpb.RegisterElectionServer(grpcServer, electionServer)
	go func() { _ = grpcServer.Serve(listener) }()
	t.Cleanup(grpcServer.Stop)

	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return listener.Dial() }),
	)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, conn.Close()) })
	handler, err := newGRPCGatewayMux(context.Background(), conn)
	require.NoError(t, err)
	httpServer := httptest.NewServer(handler)
	t.Cleanup(httpServer.Close)

	assertStream := func(path, body, token, first, second string, release chan struct{}) {
		t.Helper()
		request, err := http.NewRequest(http.MethodPost, httpServer.URL+path, strings.NewReader(body))
		require.NoError(t, err)
		request.Header.Set("Content-Type", "application/json")
		request.Header.Set("Authorization", token)
		response, err := http.DefaultClient.Do(request)
		require.NoError(t, err)
		defer response.Body.Close()
		require.Equal(t, http.StatusOK, response.StatusCode)
		require.Equal(t, "application/json", response.Header.Get("Content-Type"))
		require.Contains(t, response.TransferEncoding, "chunked")

		reader := bufio.NewReader(response.Body)
		line, err := reader.ReadString('\n')
		require.NoError(t, err)
		require.JSONEq(t, first, line)
		close(release)
		line, err = reader.ReadString('\n')
		require.NoError(t, err)
		require.JSONEq(t, second, line)
	}

	assertStream("/v3/watch", `{"create_request":{"key":"d2F0Y2gta2V5"}}`,
		"Bearer watch-token",
		`{"result":{"header":{"revision":"31"},"watch_id":"7","created":true}}`,
		`{"result":{"header":{"revision":"32"},"watch_id":"7","events":[{"kv":{"key":"d2F0Y2gta2V5","mod_revision":"32","value":"d2F0Y2gtdmFsdWU="}}]}}`,
		watchNext)
	assertStream("/v3/election/observe", `{"name":"ZWxlY3Rpb24="}`,
		"Bearer observe-token",
		`{"result":{"header":{"revision":"41"},"kv":{"key":"ZWxlY3Rpb24=","mod_revision":"41","value":"bGVhZGVyLW9uZQ=="}}}`,
		`{"result":{"header":{"revision":"42"},"kv":{"key":"ZWxlY3Rpb24=","mod_revision":"42","value":"bGVhZGVyLXR3bw=="}}}`,
		electionNext)
	require.Equal(t, []string{grpcGatewayRequestMarkerValue}, watchServer.md.Get(grpcGatewayRequestMarkerKey))
	require.Equal(t, []string{"Bearer watch-token"}, watchServer.md.Get(rpctypes.TokenFieldNameSwagger))
	require.Equal(t, []string{grpcGatewayRequestMarkerValue}, electionServer.md.Get(grpcGatewayRequestMarkerKey))
	require.Equal(t, []string{"Bearer observe-token"}, electionServer.md.Get(rpctypes.TokenFieldNameSwagger))

	request, err := http.NewRequest(http.MethodPost, httpServer.URL+"/v3/lease/keepalive",
		strings.NewReader("{\"ID\":\"1\"}\n{\"ID\":\"2\"}\n"))
	require.NoError(t, err)
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Authorization", "Bearer lease-token")
	response, err := http.DefaultClient.Do(request)
	require.NoError(t, err)
	defer response.Body.Close()
	require.Equal(t, http.StatusOK, response.StatusCode)
	require.Equal(t, "application/json", response.Header.Get("Content-Type"))
	require.Contains(t, response.TransferEncoding, "chunked")
	reader := bufio.NewReader(response.Body)
	line, err := reader.ReadString('\n')
	require.NoError(t, err)
	require.JSONEq(t, `{"result":{"header":{"revision":"51"},"ID":"1","TTL":"11"}}`, line)
	line, err = reader.ReadString('\n')
	require.NoError(t, err)
	require.JSONEq(t, `{"result":{"header":{"revision":"52"},"ID":"2","TTL":"12"}}`, line)
	_, err = reader.ReadString('\n')
	require.ErrorIs(t, err, io.EOF)
	require.Equal(t, []string{grpcGatewayRequestMarkerValue}, leaseServer.md.Get(grpcGatewayRequestMarkerKey))
	require.Equal(t, []string{"Bearer lease-token"}, leaseServer.md.Get(rpctypes.TokenFieldNameSwagger))
}

func TestGRPCGatewayPropagatesConcurrencyRequestCancellation(t *testing.T) {
	listener := bufconn.Listen(1024 * 1024)
	grpcServer := grpc.NewServer()
	lockServer := &gatewayBlockingLockServer{started: make(chan struct{}), canceled: make(chan struct{})}
	electionServer := &gatewayBlockingElectionServer{started: make(chan struct{}), canceled: make(chan struct{})}
	v3lockpb.RegisterLockServer(grpcServer, lockServer)
	v3electionpb.RegisterElectionServer(grpcServer, electionServer)
	go func() { _ = grpcServer.Serve(listener) }()
	t.Cleanup(grpcServer.Stop)

	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return listener.Dial() }),
	)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, conn.Close()) })
	handler, err := newGRPCGatewayMux(context.Background(), conn)
	require.NoError(t, err)
	httpServer := httptest.NewServer(handler)
	t.Cleanup(httpServer.Close)

	assertCanceled := func(path, body string, started, canceled <-chan struct{}) {
		t.Helper()
		ctx, cancel := context.WithCancel(context.Background())
		request, err := http.NewRequestWithContext(ctx, http.MethodPost, httpServer.URL+path, strings.NewReader(body))
		require.NoError(t, err)
		request.Header.Set("Content-Type", "application/json")
		done := make(chan error, 1)
		go func() {
			response, requestErr := http.DefaultClient.Do(request)
			if response != nil {
				response.Body.Close()
			}
			done <- requestErr
		}()
		select {
		case <-started:
		case <-time.After(time.Second):
			require.FailNow(t, "gRPC handler did not start", path)
		}
		cancel()
		require.ErrorIs(t, <-done, context.Canceled)
		select {
		case <-canceled:
		case <-time.After(time.Second):
			require.FailNow(t, "gRPC handler did not observe cancellation", path)
		}
	}

	assertCanceled("/v3/lock/lock", `{"name":"bG9jaw==","lease":"1"}`, lockServer.started, lockServer.canceled)
	assertCanceled("/v3/election/campaign", `{"name":"ZWxlY3Rpb24=","lease":"2","value":"dmFsdWU="}`,
		electionServer.started, electionServer.canceled)
}
