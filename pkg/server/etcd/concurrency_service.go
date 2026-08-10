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

	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/server/v3/etcdserver/api/v3election"
	"go.etcd.io/etcd/server/v3/etcdserver/api/v3election/v3electionpb"
	"go.etcd.io/etcd/server/v3/etcdserver/api/v3lock"
	"go.etcd.io/etcd/server/v3/etcdserver/api/v3lock/v3lockpb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type lockServer struct {
	v3lockpb.UnimplementedLockServer
	delegate v3lockpb.LockServer
}

func newLockServer(client *clientv3.Client) v3lockpb.LockServer {
	return &lockServer{delegate: v3lock.NewLockServer(client)}
}

func (s *lockServer) Lock(ctx context.Context, request *v3lockpb.LockRequest) (*v3lockpb.LockResponse, error) {
	response, err := s.delegate.Lock(ctx, request)
	return response, normalizeConcurrencyAuthError(err)
}

func (s *lockServer) Unlock(ctx context.Context, request *v3lockpb.UnlockRequest) (*v3lockpb.UnlockResponse, error) {
	response, err := s.delegate.Unlock(ctx, request)
	return response, normalizeConcurrencyAuthError(err)
}

type electionServer struct {
	v3electionpb.UnimplementedElectionServer
	delegate v3electionpb.ElectionServer
}

func newElectionServer(client *clientv3.Client) v3electionpb.ElectionServer {
	return &electionServer{delegate: v3election.NewElectionServer(client)}
}

func (s *electionServer) Campaign(ctx context.Context, request *v3electionpb.CampaignRequest) (*v3electionpb.CampaignResponse, error) {
	response, err := s.delegate.Campaign(ctx, request)
	return response, normalizeConcurrencyAuthError(err)
}

func (s *electionServer) Proclaim(ctx context.Context, request *v3electionpb.ProclaimRequest) (*v3electionpb.ProclaimResponse, error) {
	response, err := s.delegate.Proclaim(ctx, request)
	return response, normalizeConcurrencyAuthError(err)
}

func (s *electionServer) Leader(ctx context.Context, request *v3electionpb.LeaderRequest) (*v3electionpb.LeaderResponse, error) {
	response, err := s.delegate.Leader(ctx, request)
	return response, normalizeConcurrencyAuthError(err)
}

func (s *electionServer) Observe(request *v3electionpb.LeaderRequest, stream v3electionpb.Election_ObserveServer) error {
	return normalizeConcurrencyAuthError(s.delegate.Observe(request, stream))
}

func (s *electionServer) Resign(ctx context.Context, request *v3electionpb.ResignRequest) (*v3electionpb.ResignResponse, error) {
	response, err := s.delegate.Resign(ctx, request)
	return response, normalizeConcurrencyAuthError(err)
}

func normalizeConcurrencyAuthError(err error) error {
	if err == nil {
		return nil
	}
	// The upstream Lock/Election servers return errors from their internal
	// client verbatim. KubeBrain needs one narrow translation because its local
	// client preserves modern typed auth codes whereas the reference dedicated
	// services expose these four auth failures as Unknown. Do not translate every
	// error sharing those broad codes: transport interceptors and future request
	// validation can legitimately return InvalidArgument, Unauthenticated, or
	// PermissionDenied and upstream would preserve them.
	for _, authErr := range []error{
		rpctypes.ErrGRPCUserEmpty,
		rpctypes.ErrGRPCInvalidAuthToken,
		rpctypes.ErrGRPCPermissionDenied,
		rpctypes.ErrGRPCAuthOldRevision,
	} {
		if errors.Is(err, authErr) || sameGRPCStatus(err, authErr) {
			return status.Error(codes.Unknown, status.Convert(err).Message())
		}
	}
	return err
}

func sameGRPCStatus(left, right error) bool {
	return status.Code(left) == status.Code(right) &&
		status.Convert(left).Message() == status.Convert(right).Message()
}
