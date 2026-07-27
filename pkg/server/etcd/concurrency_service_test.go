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
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/authpb"
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
	requireConcurrencyClientError(t, err, rpctypes.ErrUserEmpty, codes.Unknown, status.Convert(rpctypes.ErrUserEmpty).Message())

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
		wantErr error
		message string
	}{
		{
			name:    "missing token",
			ctx:     auth.anonymous,
			wantErr: rpctypes.ErrUserEmpty,
			message: status.Convert(rpctypes.ErrUserEmpty).Message(),
		},
		{
			name: "invalid token",
			ctx: metadata.NewIncomingContext(auth.anonymous, metadata.Pairs(
				rpctypes.TokenFieldNameGRPC, "invalid-token",
			)),
			wantErr: rpctypes.ErrInvalidAuthToken,
			message: status.Convert(rpctypes.ErrInvalidAuthToken).Message(),
		},
		{
			name:    "permission denied",
			ctx:     auth.limited,
			wantErr: rpctypes.ErrPermissionDenied,
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
					requireConcurrencyClientError(t, err, authFailure.wantErr, codes.Unknown, authFailure.message)
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

func TestDedicatedConcurrencyAuthorizationTracksRoleAndTokenLifecycle(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer func() {
		_ = server.concurrencyClient.Close()
		closeFn()
	}()

	ctx := context.Background()
	const (
		rootPassword  = "root-secret"
		alicePassword = "alice-secret"
		aliceNewPass  = "alice-new-secret"
		aliceRole     = "concurrency-authz"
		allowedPrefix = "/a966/concurrency/allowed/"
		deniedPrefix  = "/a966/concurrency/denied/"
	)
	permission := &authpb.Permission{
		PermType: authpb.READWRITE,
		Key:      []byte(allowedPrefix),
		RangeEnd: []byte("/a966/concurrency/allowed0"),
	}
	require.NoError(t, server.auth.userAdd(ctx, &etcdserverpb.AuthUserAddRequest{Name: "root", Password: rootPassword}))
	require.NoError(t, server.auth.roleAdd(ctx, "root"))
	require.NoError(t, server.auth.userGrantRole(ctx, "root", "root"))
	require.NoError(t, server.auth.userAdd(ctx, &etcdserverpb.AuthUserAddRequest{Name: "alice", Password: alicePassword}))
	require.NoError(t, server.auth.roleAdd(ctx, aliceRole))
	require.NoError(t, server.auth.roleGrantPermission(ctx, aliceRole, permission))
	require.NoError(t, server.auth.userGrantRole(ctx, "alice", aliceRole))
	lockLease, err := server.LeaseGrant(ctx, &etcdserverpb.LeaseGrantRequest{TTL: 30})
	require.NoError(t, err)
	electionLease, err := server.LeaseGrant(ctx, &etcdserverpb.LeaseGrantRequest{TTL: 30})
	require.NoError(t, err)
	require.NoError(t, server.auth.enable(ctx))

	rootAuth, err := server.Authenticate(ctx, &etcdserverpb.AuthenticateRequest{Name: "root", Password: rootPassword})
	require.NoError(t, err)
	rootCtx := metadata.NewIncomingContext(ctx, metadata.Pairs(rpctypes.TokenFieldNameGRPC, rootAuth.Token))
	aliceAuth, err := server.Authenticate(ctx, &etcdserverpb.AuthenticateRequest{Name: "alice", Password: alicePassword})
	require.NoError(t, err)
	aliceCtx := metadata.NewIncomingContext(ctx, metadata.Pairs(rpctypes.TokenFieldNameGRPC, aliceAuth.Token))

	lockServer := newLockServer(server.concurrencyClient)
	electionServer := newElectionServer(server.concurrencyClient)
	lockName := []byte(allowedPrefix + "lock")
	locked, err := lockServer.Lock(aliceCtx, &v3lockpb.LockRequest{Name: lockName, Lease: lockLease.ID})
	require.NoError(t, err)
	require.NotEmpty(t, locked.Key)
	_, err = lockServer.Unlock(aliceCtx, &v3lockpb.UnlockRequest{Key: locked.Key})
	require.NoError(t, err)
	_, err = lockServer.Lock(aliceCtx, &v3lockpb.LockRequest{Name: []byte(deniedPrefix + "lock"), Lease: lockLease.ID})
	requireConcurrencyClientError(t, err, rpctypes.ErrPermissionDenied, codes.Unknown, status.Convert(rpctypes.ErrPermissionDenied).Message())

	campaign, err := electionServer.Campaign(aliceCtx, &v3electionpb.CampaignRequest{
		Name:  []byte(allowedPrefix + "election"),
		Lease: electionLease.ID,
		Value: []byte("first"),
	})
	require.NoError(t, err)
	leader, err := electionServer.Leader(aliceCtx, &v3electionpb.LeaderRequest{Name: []byte(allowedPrefix + "election")})
	require.NoError(t, err)
	require.Equal(t, []byte("first"), leader.Kv.Value)
	_, err = electionServer.Proclaim(aliceCtx, &v3electionpb.ProclaimRequest{Leader: campaign.Leader, Value: []byte("second")})
	require.NoError(t, err)
	_, err = electionServer.Resign(aliceCtx, &v3electionpb.ResignRequest{Leader: campaign.Leader})
	require.NoError(t, err)
	_, err = electionServer.Campaign(aliceCtx, &v3electionpb.CampaignRequest{
		Name:  []byte(deniedPrefix + "election"),
		Lease: electionLease.ID,
		Value: []byte("denied"),
	})
	requireConcurrencyClientError(t, err, rpctypes.ErrPermissionDenied, codes.Unknown, status.Convert(rpctypes.ErrPermissionDenied).Message())

	_, err = server.RoleRevokePermission(rootCtx, &etcdserverpb.AuthRoleRevokePermissionRequest{
		Role: aliceRole, Key: permission.Key, RangeEnd: permission.RangeEnd,
	})
	require.NoError(t, err)
	_, err = lockServer.Lock(aliceCtx, &v3lockpb.LockRequest{Name: lockName, Lease: lockLease.ID})
	requireConcurrencyClientError(t, err, rpctypes.ErrPermissionDenied, codes.Unknown, status.Convert(rpctypes.ErrPermissionDenied).Message())
	_, err = server.RoleGrantPermission(rootCtx, &etcdserverpb.AuthRoleGrantPermissionRequest{Name: aliceRole, Perm: permission})
	require.NoError(t, err)
	locked, err = lockServer.Lock(aliceCtx, &v3lockpb.LockRequest{Name: lockName, Lease: lockLease.ID})
	require.NoError(t, err)
	_, err = lockServer.Unlock(aliceCtx, &v3lockpb.UnlockRequest{Key: locked.Key})
	require.NoError(t, err)

	_, err = server.UserChangePassword(rootCtx, &etcdserverpb.AuthUserChangePasswordRequest{
		Name: "alice", Password: aliceNewPass,
	})
	require.NoError(t, err)
	_, err = lockServer.Lock(aliceCtx, &v3lockpb.LockRequest{Name: lockName, Lease: lockLease.ID})
	requireConcurrencyClientError(t, err, rpctypes.ErrInvalidAuthToken, codes.Unknown, status.Convert(rpctypes.ErrInvalidAuthToken).Message())
}

func requireConcurrencyClientError(t *testing.T, err error, want error, code codes.Code, message string) {
	t.Helper()
	require.EqualError(t, err, message)
	require.ErrorIs(t, err, want)
	require.Equal(t, code, status.Code(err))
	require.Equal(t, message, status.Convert(err).Message())
}

func requireConcurrencyStatusError(t *testing.T, err error, code codes.Code, message string) {
	t.Helper()
	require.EqualError(t, err, message)
	require.Equal(t, code, status.Code(err))
	require.Equal(t, message, status.Convert(err).Message())
}

func TestDedicatedConcurrencyCancellationRemovesWaiters(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer func() {
		_ = server.concurrencyClient.Close()
		closeFn()
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	lockServer := newLockServer(server.concurrencyClient)
	electionServer := newElectionServer(server.concurrencyClient)
	leaseIDs := make([]int64, 6)
	for i := range leaseIDs {
		lease, err := server.LeaseGrant(ctx, &etcdserverpb.LeaseGrantRequest{TTL: 30})
		require.NoError(t, err)
		leaseIDs[i] = lease.ID
	}
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		for _, leaseID := range leaseIDs {
			_, _ = server.LeaseRevoke(cleanupCtx, &etcdserverpb.LeaseRevokeRequest{ID: leaseID})
		}
	})
	prefixCount := func(prefix []byte) int64 {
		t.Helper()
		response, err := server.Range(ctx, &etcdserverpb.RangeRequest{
			Key:       prefix,
			RangeEnd:  prefixEnd(prefix),
			CountOnly: true,
		})
		require.NoError(t, err)
		return response.Count
	}
	waitForPrefixCount := func(prefix []byte, expected int64) {
		t.Helper()
		require.Eventually(t, func() bool { return prefixCount(prefix) == expected },
			5*time.Second, 10*time.Millisecond, "prefix %q did not reach count %d", prefix, expected)
	}
	waitForError := func(done <-chan error, operation string) error {
		t.Helper()
		select {
		case err := <-done:
			return err
		case <-ctx.Done():
			t.Fatalf("%s did not finish after context cancellation: %v", operation, ctx.Err())
			return nil
		}
	}
	requireCanceled := func(err error) {
		t.Helper()
		if errors.Is(err, context.Canceled) {
			return
		}
		require.Equal(t, codes.Canceled, status.Code(err))
		require.Equal(t, "etcdserver: watch canceled", status.Convert(err).Message())
	}

	lockName := []byte("/a972/concurrency-cancel/lock")
	lockOwner, err := lockServer.Lock(ctx, &v3lockpb.LockRequest{Name: lockName, Lease: leaseIDs[0]})
	require.NoError(t, err)
	lockWaitCtx, cancelLockWait := context.WithCancel(ctx)
	lockWaitDone := make(chan error, 1)
	go func() {
		_, lockErr := lockServer.Lock(lockWaitCtx, &v3lockpb.LockRequest{Name: lockName, Lease: leaseIDs[1]})
		lockWaitDone <- lockErr
	}()
	waitForPrefixCount(lockName, 2)
	cancelLockWait()
	requireCanceled(waitForError(lockWaitDone, "lock waiter"))
	waitForPrefixCount(lockName, 1)
	_, err = lockServer.Unlock(ctx, &v3lockpb.UnlockRequest{Key: lockOwner.Key})
	require.NoError(t, err)
	lockSuccessor, err := lockServer.Lock(ctx, &v3lockpb.LockRequest{Name: lockName, Lease: leaseIDs[2]})
	require.NoError(t, err)
	require.NotEmpty(t, lockSuccessor.Key)
	_, err = lockServer.Unlock(ctx, &v3lockpb.UnlockRequest{Key: lockSuccessor.Key})
	require.NoError(t, err)
	waitForPrefixCount(lockName, 0)

	electionName := []byte("/a972/concurrency-cancel/election")
	electionOwner, err := electionServer.Campaign(ctx, &v3electionpb.CampaignRequest{
		Name:  electionName,
		Lease: leaseIDs[3],
		Value: []byte("owner"),
	})
	require.NoError(t, err)
	electionWaitCtx, cancelElectionWait := context.WithCancel(ctx)
	electionWaitDone := make(chan error, 1)
	go func() {
		_, campaignErr := electionServer.Campaign(electionWaitCtx, &v3electionpb.CampaignRequest{
			Name:  electionName,
			Lease: leaseIDs[4],
			Value: []byte("canceled"),
		})
		electionWaitDone <- campaignErr
	}()
	waitForPrefixCount(electionName, 2)
	cancelElectionWait()
	requireCanceled(waitForError(electionWaitDone, "election waiter"))
	waitForPrefixCount(electionName, 1)
	_, err = electionServer.Resign(ctx, &v3electionpb.ResignRequest{Leader: electionOwner.Leader})
	require.NoError(t, err)
	electionSuccessor, err := electionServer.Campaign(ctx, &v3electionpb.CampaignRequest{
		Name:  electionName,
		Lease: leaseIDs[5],
		Value: []byte("successor"),
	})
	require.NoError(t, err)
	require.NotNil(t, electionSuccessor.Leader)
	_, err = electionServer.Resign(ctx, &v3electionpb.ResignRequest{Leader: electionSuccessor.Leader})
	require.NoError(t, err)
	waitForPrefixCount(electionName, 0)
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
			requireConcurrencyStatusError(t, err, codes.Unknown, test.message)
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
