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
	"time"

	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	"go.etcd.io/etcd/server/v3/etcdserver/api/v3election/v3electionpb"
	"go.etcd.io/etcd/server/v3/etcdserver/api/v3lock/v3lockpb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

type fakeElectionObserveServer struct {
	v3electionpb.Election_ObserveServer
	ctx  context.Context
	sent int
}

func (s fakeElectionObserveServer) Context() context.Context {
	return s.ctx
}

func (s *fakeElectionObserveServer) Send(*v3electionpb.LeaderResponse) error {
	s.sent++
	return nil
}

type dedicatedConcurrencyAuthContexts struct {
	anonymous context.Context
	root      context.Context
	limited   context.Context
}

func setupDedicatedConcurrencyAuth(t *testing.T, server *RPCServer) dedicatedConcurrencyAuthContexts {
	t.Helper()
	ctx := context.Background()
	require.NoError(t, server.auth.userAdd(ctx, &etcdserverpb.AuthUserAddRequest{
		Name: "root", Password: "secret",
	}))
	require.NoError(t, server.auth.roleAdd(ctx, "root"))
	require.NoError(t, server.auth.userGrantRole(ctx, "root", "root"))
	require.NoError(t, server.auth.userAdd(ctx, &etcdserverpb.AuthUserAddRequest{
		Name: "limited", Password: "secret",
	}))
	require.NoError(t, server.auth.roleAdd(ctx, "limited"))
	require.NoError(t, server.auth.userGrantRole(ctx, "limited", "limited"))
	require.NoError(t, server.auth.enable(ctx))
	root, err := server.Authenticate(ctx, &etcdserverpb.AuthenticateRequest{
		Name: "root", Password: "secret",
	})
	require.NoError(t, err)
	limited, err := server.Authenticate(ctx, &etcdserverpb.AuthenticateRequest{
		Name: "limited", Password: "secret",
	})
	require.NoError(t, err)
	return dedicatedConcurrencyAuthContexts{
		anonymous: ctx,
		root: metadata.NewIncomingContext(ctx, metadata.Pairs(
			rpctypes.TokenFieldNameGRPC, root.Token,
		)),
		limited: metadata.NewIncomingContext(ctx, metadata.Pairs(
			rpctypes.TokenFieldNameGRPC, limited.Token,
		)),
	}
}

func TestDedicatedLockAndElectionServicesUseKubeBrainBackend(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer func() {
		_ = server.concurrencyClient.Close()
		closeFn()
	}()

	ctx := context.Background()
	lease, err := server.LeaseGrant(ctx, &etcdserverpb.LeaseGrantRequest{TTL: 30})
	require.NoError(t, err)
	contenderLease, err := server.LeaseGrant(ctx, &etcdserverpb.LeaseGrantRequest{TTL: 30})
	require.NoError(t, err)

	lockServer := newLockServer(server.concurrencyClient)
	locked, err := lockServer.Lock(ctx, &v3lockpb.LockRequest{
		Name:  []byte("/a356/lock"),
		Lease: lease.ID,
	})
	require.NoError(t, err)
	require.NotEmpty(t, locked.Key)

	storedLock, err := server.Range(ctx, &etcdserverpb.RangeRequest{Key: locked.Key})
	require.NoError(t, err)
	require.Len(t, storedLock.Kvs, 1)
	require.Equal(t, lease.ID, storedLock.Kvs[0].Lease)

	contenderResult := make(chan *v3lockpb.LockResponse, 1)
	contenderError := make(chan error, 1)
	go func() {
		response, lockErr := lockServer.Lock(ctx, &v3lockpb.LockRequest{
			Name:  []byte("/a356/lock"),
			Lease: contenderLease.ID,
		})
		contenderResult <- response
		contenderError <- lockErr
	}()
	select {
	case err := <-contenderError:
		t.Fatalf("contender completed before unlock: %v", err)
	case <-time.After(50 * time.Millisecond):
	}

	unlocked, err := lockServer.Unlock(ctx, &v3lockpb.UnlockRequest{Key: locked.Key})
	require.NoError(t, err)
	require.NotNil(t, unlocked.Header)
	select {
	case err := <-contenderError:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("contender did not acquire lock after unlock")
	}
	contender := <-contenderResult
	require.NotNil(t, contender)
	require.NotEqual(t, locked.Key, contender.Key)
	_, err = lockServer.Unlock(ctx, &v3lockpb.UnlockRequest{Key: contender.Key})
	require.NoError(t, err)
	storedLock, err = server.Range(ctx, &etcdserverpb.RangeRequest{Key: locked.Key})
	require.NoError(t, err)
	require.Empty(t, storedLock.Kvs)

	electionServer := newElectionServer(server.concurrencyClient)
	campaign, err := electionServer.Campaign(ctx, &v3electionpb.CampaignRequest{
		Name:  []byte("/a356/election"),
		Lease: lease.ID,
		Value: []byte("first"),
	})
	require.NoError(t, err)
	require.NotNil(t, campaign.Leader)
	require.NotEmpty(t, campaign.Leader.Key)

	leader, err := electionServer.Leader(ctx, &v3electionpb.LeaderRequest{Name: []byte("/a356/election")})
	require.NoError(t, err)
	require.Equal(t, []byte("first"), leader.Kv.Value)

	proclaimed, err := electionServer.Proclaim(ctx, &v3electionpb.ProclaimRequest{
		Leader: campaign.Leader,
		Value:  []byte("second"),
	})
	require.NoError(t, err)
	require.NotNil(t, proclaimed.Header)
	leader, err = electionServer.Leader(ctx, &v3electionpb.LeaderRequest{Name: []byte("/a356/election")})
	require.NoError(t, err)
	require.Equal(t, []byte("second"), leader.Kv.Value)

	resigned, err := electionServer.Resign(ctx, &v3electionpb.ResignRequest{Leader: campaign.Leader})
	require.NoError(t, err)
	require.NotNil(t, resigned.Header)
	leaderKey, err := server.Range(ctx, &etcdserverpb.RangeRequest{Key: campaign.Leader.Key})
	require.NoError(t, err)
	require.Empty(t, leaderKey.Kvs)
}

func TestDedicatedLockServicePreservesCallerAuthentication(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer func() {
		_ = server.concurrencyClient.Close()
		closeFn()
	}()

	ctx := context.Background()
	lease, err := server.LeaseGrant(ctx, &etcdserverpb.LeaseGrantRequest{TTL: 30})
	require.NoError(t, err)
	require.NoError(t, server.auth.userAdd(ctx, &etcdserverpb.AuthUserAddRequest{
		Name: "root", Password: "secret",
	}))
	require.NoError(t, server.auth.roleAdd(ctx, "root"))
	require.NoError(t, server.auth.userGrantRole(ctx, "root", "root"))
	require.NoError(t, server.auth.enable(ctx))

	lockServer := newLockServer(server.concurrencyClient)
	_, err = lockServer.Lock(ctx, &v3lockpb.LockRequest{Name: []byte("/a356/auth"), Lease: lease.ID})
	require.Equal(t, codes.Unknown, status.Code(err))
	require.Equal(t, status.Convert(rpctypes.ErrUserEmpty).Message(), status.Convert(err).Message())

	authenticated, err := server.Authenticate(ctx, &etcdserverpb.AuthenticateRequest{
		Name: "root", Password: "secret",
	})
	require.NoError(t, err)
	rootCtx := metadata.NewIncomingContext(ctx, metadata.Pairs(
		rpctypes.TokenFieldNameGRPC, authenticated.Token,
	))
	locked, err := lockServer.Lock(rootCtx, &v3lockpb.LockRequest{
		Name: []byte("/a356/auth"), Lease: lease.ID,
	})
	require.NoError(t, err)
	_, err = lockServer.Unlock(rootCtx, &v3lockpb.UnlockRequest{Key: locked.Key})
	require.NoError(t, err)
}

func TestDedicatedElectionObserveAuthFailuresReturnEmptyStream(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer func() {
		_ = server.concurrencyClient.Close()
		closeFn()
	}()

	auth := setupDedicatedConcurrencyAuth(t, server)

	electionServer := newElectionServer(server.concurrencyClient)
	tests := []struct {
		name string
		ctx  context.Context
	}{
		{name: "missing token", ctx: auth.anonymous},
		{
			name: "invalid token",
			ctx: metadata.NewIncomingContext(auth.anonymous, metadata.Pairs(
				rpctypes.TokenFieldNameGRPC, "invalid-token",
			)),
		},
		{name: "permission denied", ctx: auth.limited},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			observeCtx, cancel := context.WithTimeout(test.ctx, time.Second)
			defer cancel()
			stream := &fakeElectionObserveServer{ctx: observeCtx}
			err := electionServer.Observe(
				&v3electionpb.LeaderRequest{Name: []byte("/a721/auth-election/" + test.name)},
				stream,
			)
			require.NoError(t, err)
			require.Zero(t, stream.sent)
		})
	}
}

func TestDedicatedConcurrencyUnaryAuthFailuresReturnUnknown(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer func() {
		_ = server.concurrencyClient.Close()
		closeFn()
	}()

	auth := setupDedicatedConcurrencyAuth(t, server)
	lockServer := newLockServer(server.concurrencyClient)
	electionServer := newElectionServer(server.concurrencyClient)
	key := func(suffix string) []byte {
		return []byte("/a722/concurrency-auth" + suffix)
	}
	locked, err := lockServer.Lock(auth.root, &v3lockpb.LockRequest{
		Name: key("/lock"),
	})
	require.NoError(t, err)
	campaign, err := electionServer.Campaign(auth.root, &v3electionpb.CampaignRequest{
		Name:  key("/election"),
		Value: []byte("leader"),
	})
	require.NoError(t, err)

	authFailures := []struct {
		name    string
		ctx     context.Context
		message string
	}{
		{
			name:    "missing token",
			ctx:     auth.anonymous,
			message: status.Convert(rpctypes.ErrUserEmpty).Message(),
		},
		{
			name: "invalid token",
			ctx: metadata.NewIncomingContext(auth.anonymous, metadata.Pairs(
				rpctypes.TokenFieldNameGRPC, "invalid-token",
			)),
			message: status.Convert(rpctypes.ErrInvalidAuthToken).Message(),
		},
		{
			name:    "permission denied",
			ctx:     auth.limited,
			message: status.Convert(rpctypes.ErrPermissionDenied).Message(),
		},
	}
	calls := []struct {
		name string
		call func(context.Context) error
	}{
		{
			name: "lock",
			call: func(ctx context.Context) error {
				_, lockErr := lockServer.Lock(ctx, &v3lockpb.LockRequest{
					Name: key("/blocked-lock"),
				})
				return lockErr
			},
		},
		{
			name: "unlock",
			call: func(ctx context.Context) error {
				_, unlockErr := lockServer.Unlock(ctx, &v3lockpb.UnlockRequest{Key: locked.Key})
				return unlockErr
			},
		},
		{
			name: "campaign",
			call: func(ctx context.Context) error {
				_, campaignErr := electionServer.Campaign(ctx, &v3electionpb.CampaignRequest{
					Name:  key("/blocked-election"),
					Value: []byte("blocked"),
				})
				return campaignErr
			},
		},
		{
			name: "proclaim",
			call: func(ctx context.Context) error {
				_, proclaimErr := electionServer.Proclaim(ctx, &v3electionpb.ProclaimRequest{
					Leader: campaign.Leader,
					Value:  []byte("blocked"),
				})
				return proclaimErr
			},
		},
		{
			name: "leader",
			call: func(ctx context.Context) error {
				_, leaderErr := electionServer.Leader(ctx, &v3electionpb.LeaderRequest{
					Name: key("/election"),
				})
				return leaderErr
			},
		},
		{
			name: "resign",
			call: func(ctx context.Context) error {
				_, resignErr := electionServer.Resign(ctx, &v3electionpb.ResignRequest{
					Leader: campaign.Leader,
				})
				return resignErr
			},
		},
	}
	for _, call := range calls {
		t.Run(call.name, func(t *testing.T) {
			for _, authFailure := range authFailures {
				t.Run(authFailure.name, func(t *testing.T) {
					err := call.call(authFailure.ctx)
					require.Equal(t, codes.Unknown, status.Code(err))
					require.Equal(t, authFailure.message, status.Convert(err).Message())
				})
			}
		})
	}

	lockKV, err := server.Range(auth.root, &etcdserverpb.RangeRequest{Key: locked.Key})
	require.NoError(t, err)
	require.Len(t, lockKV.Kvs, 1)
	leaderKV, err := server.Range(auth.root, &etcdserverpb.RangeRequest{Key: campaign.Leader.Key})
	require.NoError(t, err)
	require.Len(t, leaderKV.Kvs, 1)
}

func TestDedicatedConcurrencyServiceErrorsMatchEtcd(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer func() {
		_ = server.concurrencyClient.Close()
		closeFn()
	}()

	ctx := context.Background()
	lockServer := newLockServer(server.concurrencyClient)
	electionServer := newElectionServer(server.concurrencyClient)
	tests := []struct {
		name    string
		call    func() error
		message string
	}{
		{
			name: "lock missing lease",
			call: func() error {
				_, err := lockServer.Lock(ctx, &v3lockpb.LockRequest{Name: []byte("/a360/lock"), Lease: 999999})
				return err
			},
			message: "etcdserver: requested lease not found",
		},
		{
			name: "unlock empty key",
			call: func() error {
				_, err := lockServer.Unlock(ctx, &v3lockpb.UnlockRequest{})
				return err
			},
			message: "etcdserver: key is not provided",
		},
		{
			name: "campaign missing lease",
			call: func() error {
				_, err := electionServer.Campaign(ctx, &v3electionpb.CampaignRequest{
					Name: []byte("/a360/election"), Lease: 999999, Value: []byte("value"),
				})
				return err
			},
			message: "etcdserver: requested lease not found",
		},
		{
			name: "proclaim missing leader",
			call: func() error {
				_, err := electionServer.Proclaim(ctx, &v3electionpb.ProclaimRequest{})
				return err
			},
			message: `"leader" field must be provided`,
		},
		{
			name: "resign missing leader",
			call: func() error {
				_, err := electionServer.Resign(ctx, &v3electionpb.ResignRequest{})
				return err
			},
			message: `"leader" field must be provided`,
		},
		{
			name: "leader not found",
			call: func() error {
				_, err := electionServer.Leader(ctx, &v3electionpb.LeaderRequest{Name: []byte("/a360/no-leader")})
				return err
			},
			message: "election: no leader",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := test.call()
			require.Equal(t, codes.Unknown, status.Code(err))
			require.Equal(t, test.message, status.Convert(err).Message())
		})
	}
}

func TestDedicatedConcurrencyZeroLeaseCreatesDefaultSession(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer func() {
		_ = server.concurrencyClient.Close()
		closeFn()
	}()

	ctx := context.Background()
	lockServer := newLockServer(server.concurrencyClient)
	locked, err := lockServer.Lock(ctx, &v3lockpb.LockRequest{Name: []byte("/a361/lock")})
	require.NoError(t, err)
	lockKV, err := server.Range(ctx, &etcdserverpb.RangeRequest{Key: locked.Key})
	require.NoError(t, err)
	require.Len(t, lockKV.Kvs, 1)
	lockLease := lockKV.Kvs[0].Lease
	require.NotZero(t, lockLease)
	lockTTL, err := server.LeaseTimeToLive(ctx, &etcdserverpb.LeaseTimeToLiveRequest{ID: lockLease})
	require.NoError(t, err)
	require.Equal(t, int64(60), lockTTL.GrantedTTL)
	require.Positive(t, lockTTL.TTL)
	_, err = lockServer.Unlock(ctx, &v3lockpb.UnlockRequest{Key: locked.Key})
	require.NoError(t, err)
	lockKV, err = server.Range(ctx, &etcdserverpb.RangeRequest{Key: locked.Key})
	require.NoError(t, err)
	require.Empty(t, lockKV.Kvs)
	lockTTL, err = server.LeaseTimeToLive(ctx, &etcdserverpb.LeaseTimeToLiveRequest{ID: lockLease})
	require.NoError(t, err)
	require.Positive(t, lockTTL.TTL)
	_, err = server.LeaseRevoke(ctx, &etcdserverpb.LeaseRevokeRequest{ID: lockLease})
	require.NoError(t, err)

	electionServer := newElectionServer(server.concurrencyClient)
	campaign, err := electionServer.Campaign(ctx, &v3electionpb.CampaignRequest{
		Name: []byte("/a361/election"), Value: []byte("leader"),
	})
	require.NoError(t, err)
	require.NotNil(t, campaign.Leader)
	require.NotZero(t, campaign.Leader.Lease)
	require.NotEqual(t, lockLease, campaign.Leader.Lease)
	electionKV, err := server.Range(ctx, &etcdserverpb.RangeRequest{Key: campaign.Leader.Key})
	require.NoError(t, err)
	require.Len(t, electionKV.Kvs, 1)
	require.Equal(t, campaign.Leader.Lease, electionKV.Kvs[0].Lease)
	electionTTL, err := server.LeaseTimeToLive(ctx, &etcdserverpb.LeaseTimeToLiveRequest{ID: campaign.Leader.Lease})
	require.NoError(t, err)
	require.Equal(t, int64(60), electionTTL.GrantedTTL)
	require.Positive(t, electionTTL.TTL)
	_, err = electionServer.Resign(ctx, &v3electionpb.ResignRequest{Leader: campaign.Leader})
	require.NoError(t, err)
	electionKV, err = server.Range(ctx, &etcdserverpb.RangeRequest{Key: campaign.Leader.Key})
	require.NoError(t, err)
	require.Empty(t, electionKV.Kvs)
	electionTTL, err = server.LeaseTimeToLive(ctx, &etcdserverpb.LeaseTimeToLiveRequest{ID: campaign.Leader.Lease})
	require.NoError(t, err)
	require.Positive(t, electionTTL.TTL)
	_, err = server.LeaseRevoke(ctx, &etcdserverpb.LeaseRevokeRequest{ID: campaign.Leader.Lease})
	require.NoError(t, err)
}
