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
	"encoding/base64"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/authpb"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	clientv3 "go.etcd.io/etcd/client/v3"
	"golang.org/x/crypto/bcrypt"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

func TestClientAuthHeadersTrackCurrentRevision(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	grpcServer := grpc.NewServer(server.ClientServerOptions()...)
	etcdserverpb.RegisterKVServer(grpcServer, server)
	etcdserverpb.RegisterAuthServer(grpcServer, server)
	listener := bufconn.Listen(1 << 20)
	go func() { _ = grpcServer.Serve(listener) }()
	t.Cleanup(grpcServer.Stop)

	client, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{"bufnet"},
		DialTimeout: time.Second,
		DialOptions: []grpc.DialOption{
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
				return listener.Dial()
			}),
		},
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, client.Close()) })

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	put, err := client.Put(ctx, "/a1054/auth-client-header/key", "value")
	require.NoError(t, err)
	role := "a1054-auth-client-header"
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		_, _ = client.Delete(cleanupCtx, "/a1054/auth-client-header/", clientv3.WithPrefix())
		_, _ = client.RoleDelete(cleanupCtx, role)
	})

	statusResponse, err := client.AuthStatus(ctx)
	require.NoError(t, err)
	require.NotNil(t, statusResponse.Header)
	require.Equal(t, put.Header.Revision, statusResponse.Header.Revision)
	require.NotZero(t, statusResponse.Header.ClusterId)
	require.NotZero(t, statusResponse.Header.MemberId)
	require.Positive(t, statusResponse.Header.RaftTerm)

	roleAddResponse, err := client.RoleAdd(ctx, role)
	require.NoError(t, err)
	require.NotNil(t, roleAddResponse.Header)
	require.Equal(t, put.Header.Revision, roleAddResponse.Header.Revision)
	require.NotZero(t, roleAddResponse.Header.ClusterId)
	require.NotZero(t, roleAddResponse.Header.MemberId)
	require.Positive(t, roleAddResponse.Header.RaftTerm)

	permission := &authpb.Permission{
		PermType: authpb.READ,
		Key:      []byte("/a1054/auth-client-header/"),
		RangeEnd: []byte(clientv3.GetPrefixRangeEnd("/a1054/auth-client-header/")),
	}
	var grantOnce sync.Once
	var grantErr error
	server.peers = testPeerService{syncReadFn: func(context.Context) error {
		grantOnce.Do(func() {
			grantErr = server.auth.roleGrantPermission(context.Background(), role, permission)
		})
		return grantErr
	}}
	roleGetResponse, err := client.RoleGet(ctx, role)
	require.NoError(t, err)
	require.NotNil(t, roleGetResponse.Header)
	require.Equal(t, put.Header.Revision, roleGetResponse.Header.Revision)
	require.NotZero(t, roleGetResponse.Header.ClusterId)
	require.NotZero(t, roleGetResponse.Header.MemberId)
	require.Positive(t, roleGetResponse.Header.RaftTerm)
	require.Equal(t, []*authpb.Permission{permission}, roleGetResponse.Perm)
}

func TestClientAuthPasswordChangeInvalidatesOldPasswordAndToken(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	grpcServer := grpc.NewServer(server.ClientServerOptions()...)
	etcdserverpb.RegisterKVServer(grpcServer, server)
	etcdserverpb.RegisterAuthServer(grpcServer, server)
	listener := bufconn.Listen(1 << 20)
	go func() { _ = grpcServer.Serve(listener) }()
	t.Cleanup(grpcServer.Stop)

	dialOptions := []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return listener.Dial()
		}),
	}
	newClient := func(username, password string) *clientv3.Client {
		t.Helper()
		client, err := clientv3.New(clientv3.Config{
			Endpoints:   []string{"bufnet"},
			DialTimeout: time.Second,
			Username:    username,
			Password:    password,
			DialOptions: dialOptions,
		})
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, client.Close()) })
		return client
	}
	newTokenClient := func(token string) *clientv3.Client {
		t.Helper()
		client, err := clientv3.New(clientv3.Config{
			Endpoints:   []string{"bufnet"},
			DialTimeout: time.Second,
			Token:       token,
			DialOptions: dialOptions,
		})
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, client.Close()) })
		return client
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	bootstrap := newClient("", "")
	require.NoError(t, addAuthUserRoleAndPermission(ctx, bootstrap, "root", "root-secret", "root", "", ""))
	require.NoError(t, addAuthUserRoleAndPermission(
		ctx, bootstrap,
		"alice", "alice-secret", "a1058-reader",
		"/a1058/auth-client/", clientv3.GetPrefixRangeEnd("/a1058/auth-client/"),
	))
	_, err := bootstrap.AuthEnable(ctx)
	require.NoError(t, err)

	root := newClient("root", "root-secret")
	_, err = root.Put(ctx, "/a1058/auth-client/key", "secret")
	require.NoError(t, err)

	oldAuth, err := bootstrap.Authenticate(ctx, "alice", "alice-secret")
	require.NoError(t, err)
	rawAuth := etcdserverpb.NewAuthClient(bootstrap.ActiveConnection())
	rawOldAuth, err := rawAuth.Authenticate(ctx, &etcdserverpb.AuthenticateRequest{Name: "alice", Password: "alice-secret"})
	require.NoError(t, err)
	require.NotEmpty(t, rawOldAuth.Token)
	rawKV := etcdserverpb.NewKVClient(bootstrap.ActiveConnection())
	rawOldTokenCtx := metadata.AppendToOutgoingContext(ctx, rpctypes.TokenFieldNameGRPC, rawOldAuth.Token)
	rawBeforeChange, err := rawKV.Range(rawOldTokenCtx, &etcdserverpb.RangeRequest{Key: []byte("/a1058/auth-client/key")})
	require.NoError(t, err)
	require.Len(t, rawBeforeChange.Kvs, 1)
	require.Equal(t, "secret", string(rawBeforeChange.Kvs[0].Value))

	aliceOldToken := newTokenClient(oldAuth.Token)
	beforeChange, err := aliceOldToken.Get(ctx, "/a1058/auth-client/key")
	require.NoError(t, err)
	require.Len(t, beforeChange.Kvs, 1)
	require.Equal(t, "secret", string(beforeChange.Kvs[0].Value))

	_, err = root.UserChangePassword(ctx, "alice", "alice-changed")
	require.NoError(t, err)
	_, oldTokenErr := aliceOldToken.Get(ctx, "/a1058/auth-client/key")
	requireAuthClientError(t, oldTokenErr, codes.Unknown, "etcdserver: invalid auth token", rpctypes.ErrInvalidAuthToken)
	_, rawOldTokenErr := rawKV.Range(rawOldTokenCtx, &etcdserverpb.RangeRequest{Key: []byte("/a1058/auth-client/key")})
	requireAuthClientError(t, rawOldTokenErr, codes.Unauthenticated, "etcdserver: invalid auth token")

	_, oldPasswordErr := clientv3.New(clientv3.Config{
		Endpoints:   []string{"bufnet"},
		DialTimeout: time.Second,
		Username:    "alice",
		Password:    "alice-secret",
		DialOptions: dialOptions,
	})
	requireAuthClientError(t, oldPasswordErr, codes.Unknown, "etcdserver: authentication failed, invalid user ID or password", rpctypes.ErrAuthFailed)
	_, rawOldPasswordErr := rawAuth.Authenticate(ctx, &etcdserverpb.AuthenticateRequest{Name: "alice", Password: "alice-secret"})
	requireAuthClientError(t, rawOldPasswordErr, codes.InvalidArgument, "etcdserver: authentication failed, invalid user ID or password")
	rawNewAuth, err := rawAuth.Authenticate(ctx, &etcdserverpb.AuthenticateRequest{Name: "alice", Password: "alice-changed"})
	require.NoError(t, err)
	rawNewTokenCtx := metadata.AppendToOutgoingContext(ctx, rpctypes.TokenFieldNameGRPC, rawNewAuth.Token)
	rawAfterChange, err := rawKV.Range(rawNewTokenCtx, &etcdserverpb.RangeRequest{Key: []byte("/a1058/auth-client/key")})
	require.NoError(t, err)
	require.Len(t, rawAfterChange.Kvs, 1)
	require.Equal(t, "secret", string(rawAfterChange.Kvs[0].Value))

	aliceNewPassword := newClient("alice", "alice-changed")
	afterChange, err := aliceNewPassword.Get(ctx, "/a1058/auth-client/key")
	require.NoError(t, err)
	require.Len(t, afterChange.Kvs, 1)
	require.Equal(t, "secret", string(afterChange.Kvs[0].Value))
}

func TestClientAuthenticateRejectsTokenInvalidatedDuringHeaderBarrier(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()
	setupAuthKVUser(t, server)

	grpcServer := grpc.NewServer(server.ClientServerOptions()...)
	etcdserverpb.RegisterAuthServer(grpcServer, server)
	listener := bufconn.Listen(1 << 20)
	go func() { _ = grpcServer.Serve(listener) }()
	t.Cleanup(grpcServer.Stop)

	dialOptions := []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return listener.Dial()
		}),
	}
	client, err := clientv3.New(clientv3.Config{
		Endpoints: []string{"bufnet"}, DialTimeout: time.Second, DialOptions: dialOptions,
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, client.Close()) })

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	var barriers atomic.Int32
	server.peers = testPeerService{syncReadFn: func(context.Context) error {
		if barriers.Add(1) == 2 {
			return server.auth.userChangePassword(context.Background(), "root", "changed-secret", "")
		}
		return nil
	}}

	_, err = client.Authenticate(ctx, "root", "root-secret")
	requireAuthClientError(t, err, codes.Unknown, "etcdserver: authentication failed, invalid user ID or password", rpctypes.ErrAuthFailed)
	response, err := client.Authenticate(ctx, "root", "changed-secret")
	require.NoError(t, err)
	require.NotEmpty(t, response.Token)

	tokenClient, err := clientv3.New(clientv3.Config{
		Endpoints: []string{"bufnet"}, DialTimeout: time.Second, Token: response.Token, DialOptions: dialOptions,
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, tokenClient.Close()) })
	statusResponse, err := tokenClient.AuthStatus(ctx)
	require.NoError(t, err)
	require.True(t, statusResponse.Enabled)
}

func TestClientAuthAddUserAfterDeleteAndPasswordRotation(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	grpcServer := grpc.NewServer(server.ClientServerOptions()...)
	etcdserverpb.RegisterAuthServer(grpcServer, server)
	listener := bufconn.Listen(1 << 20)
	go func() { _ = grpcServer.Serve(listener) }()
	t.Cleanup(grpcServer.Stop)

	dialOptions := []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return listener.Dial()
		}),
	}
	newClient := func(username, password string) *clientv3.Client {
		t.Helper()
		client, err := clientv3.New(clientv3.Config{
			Endpoints:   []string{"bufnet"},
			DialTimeout: time.Second,
			Username:    username,
			Password:    password,
			DialOptions: dialOptions,
		})
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, client.Close()) })
		return client
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	bootstrap := newClient("", "")
	require.NoError(t, addAuthUserRoleAndPermission(ctx, bootstrap, "root", "root-secret", "root", "", ""))
	_, err := bootstrap.AuthEnable(ctx)
	require.NoError(t, err)
	root := newClient("root", "root-secret")
	rawAuth := etcdserverpb.NewAuthClient(bootstrap.ActiveConnection())
	rawRootAuth := etcdserverpb.NewAuthClient(root.ActiveConnection())

	_, err = root.UserAdd(ctx, "a1136-user", "first")
	require.NoError(t, err)
	_, err = bootstrap.Authenticate(ctx, "a1136-user", "first")
	require.NoError(t, err)
	_, err = root.UserDelete(ctx, "a1136-user")
	require.NoError(t, err)
	_, err = bootstrap.Authenticate(ctx, "a1136-user", "first")
	requireAuthClientError(t, err, codes.Unknown, "etcdserver: authentication failed, invalid user ID or password", rpctypes.ErrAuthFailed)

	_, err = root.UserAdd(ctx, "a1136-user", "first")
	require.NoError(t, err)
	_, err = bootstrap.Authenticate(ctx, "a1136-user", "first")
	require.NoError(t, err)
	_, err = root.UserChangePassword(ctx, "a1136-user", "second")
	require.NoError(t, err)
	_, err = root.UserChangePassword(ctx, "a1136-user", "third")
	require.NoError(t, err)
	_, err = bootstrap.Authenticate(ctx, "a1136-user", "first")
	requireAuthClientError(t, err, codes.Unknown, "etcdserver: authentication failed, invalid user ID or password", rpctypes.ErrAuthFailed)
	_, err = bootstrap.Authenticate(ctx, "a1136-user", "second")
	requireAuthClientError(t, err, codes.Unknown, "etcdserver: authentication failed, invalid user ID or password", rpctypes.ErrAuthFailed)
	_, err = bootstrap.Authenticate(ctx, "a1136-user", "third")
	require.NoError(t, err)

	_, err = rawRootAuth.UserAdd(ctx, &etcdserverpb.AuthUserAddRequest{Name: "a1136-raw-user", Password: "first"})
	require.NoError(t, err)
	_, err = rawAuth.Authenticate(ctx, &etcdserverpb.AuthenticateRequest{Name: "a1136-raw-user", Password: "first"})
	require.NoError(t, err)
	_, err = rawRootAuth.UserDelete(ctx, &etcdserverpb.AuthUserDeleteRequest{Name: "a1136-raw-user"})
	require.NoError(t, err)
	_, rawDeletedPasswordErr := rawAuth.Authenticate(ctx, &etcdserverpb.AuthenticateRequest{Name: "a1136-raw-user", Password: "first"})
	requireAuthClientError(t, rawDeletedPasswordErr, codes.InvalidArgument, "etcdserver: authentication failed, invalid user ID or password")

	_, err = rawRootAuth.UserAdd(ctx, &etcdserverpb.AuthUserAddRequest{Name: "a1136-raw-user", Password: "first"})
	require.NoError(t, err)
	_, err = rawAuth.Authenticate(ctx, &etcdserverpb.AuthenticateRequest{Name: "a1136-raw-user", Password: "first"})
	require.NoError(t, err)
	_, err = rawRootAuth.UserChangePassword(ctx, &etcdserverpb.AuthUserChangePasswordRequest{Name: "a1136-raw-user", Password: "second"})
	require.NoError(t, err)
	_, err = rawRootAuth.UserChangePassword(ctx, &etcdserverpb.AuthUserChangePasswordRequest{Name: "a1136-raw-user", Password: "third"})
	require.NoError(t, err)
	_, rawFirstPasswordErr := rawAuth.Authenticate(ctx, &etcdserverpb.AuthenticateRequest{Name: "a1136-raw-user", Password: "first"})
	requireAuthClientError(t, rawFirstPasswordErr, codes.InvalidArgument, "etcdserver: authentication failed, invalid user ID or password")
	_, rawSecondPasswordErr := rawAuth.Authenticate(ctx, &etcdserverpb.AuthenticateRequest{Name: "a1136-raw-user", Password: "second"})
	requireAuthClientError(t, rawSecondPasswordErr, codes.InvalidArgument, "etcdserver: authentication failed, invalid user ID or password")
	_, err = rawAuth.Authenticate(ctx, &etcdserverpb.AuthenticateRequest{Name: "a1136-raw-user", Password: "third"})
	require.NoError(t, err)

	_, err = rawRootAuth.UserAdd(ctx, &etcdserverpb.AuthUserAddRequest{Name: "a585-raw-empty-password", Password: "initial"})
	require.NoError(t, err)
	_, err = rawAuth.Authenticate(ctx, &etcdserverpb.AuthenticateRequest{Name: "a585-raw-empty-password", Password: "initial"})
	require.NoError(t, err)
	_, err = rawRootAuth.UserChangePassword(ctx, &etcdserverpb.AuthUserChangePasswordRequest{Name: "a585-raw-empty-password"})
	require.NoError(t, err)
	_, rawInitialAfterEmptyErr := rawAuth.Authenticate(ctx, &etcdserverpb.AuthenticateRequest{Name: "a585-raw-empty-password", Password: "initial"})
	requireAuthClientError(t, rawInitialAfterEmptyErr, codes.InvalidArgument, "etcdserver: authentication failed, invalid user ID or password")
	_, rawEmptyPasswordErr := rawAuth.Authenticate(ctx, &etcdserverpb.AuthenticateRequest{Name: "a585-raw-empty-password", Password: ""})
	requireAuthClientError(t, rawEmptyPasswordErr, codes.InvalidArgument, "etcdserver: authentication failed, invalid user ID or password")

	_, err = rawRootAuth.UserAdd(ctx, &etcdserverpb.AuthUserAddRequest{Name: "a1207-raw-hashed-password", Password: "plain"})
	require.NoError(t, err)
	_, err = rawAuth.Authenticate(ctx, &etcdserverpb.AuthenticateRequest{Name: "a1207-raw-hashed-password", Password: "plain"})
	require.NoError(t, err)
	_, rawInvalidHashedChangeErr := rawRootAuth.UserChangePassword(ctx, &etcdserverpb.AuthUserChangePasswordRequest{
		Name:           "a1207-raw-hashed-password",
		HashedPassword: "%%%",
	})
	requireAuthClientError(t, rawInvalidHashedChangeErr, codes.Unknown, "auth: authentication failed, password was given for no password user")

	hashedPassword, err := bcrypt.GenerateFromPassword([]byte("hashed"), bcrypt.MinCost)
	require.NoError(t, err)
	_, err = rawRootAuth.UserChangePassword(ctx, &etcdserverpb.AuthUserChangePasswordRequest{
		Name:           "a1207-raw-hashed-password",
		HashedPassword: base64.StdEncoding.EncodeToString(hashedPassword),
	})
	require.NoError(t, err)
	_, rawPlainAfterHashErr := rawAuth.Authenticate(ctx, &etcdserverpb.AuthenticateRequest{Name: "a1207-raw-hashed-password", Password: "plain"})
	requireAuthClientError(t, rawPlainAfterHashErr, codes.InvalidArgument, "etcdserver: authentication failed, invalid user ID or password")
	_, err = rawAuth.Authenticate(ctx, &etcdserverpb.AuthenticateRequest{Name: "a1207-raw-hashed-password", Password: "hashed"})
	require.NoError(t, err)
}

func TestClientAuthDisabledAllowsCredentialedClient(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	grpcServer := grpc.NewServer(server.ClientServerOptions()...)
	etcdserverpb.RegisterKVServer(grpcServer, server)
	etcdserverpb.RegisterAuthServer(grpcServer, server)
	listener := bufconn.Listen(1 << 20)
	go func() { _ = grpcServer.Serve(listener) }()
	t.Cleanup(grpcServer.Stop)

	client, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{"bufnet"},
		DialTimeout: time.Second,
		Username:    "root",
		Password:    "unused-while-auth-disabled",
		DialOptions: []grpc.DialOption{
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
				return listener.Dial()
			}),
		},
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, client.Close()) })

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	rawAuth := etcdserverpb.NewAuthClient(client.ActiveConnection())
	rawStatus, err := rawAuth.AuthStatus(ctx, &etcdserverpb.AuthStatusRequest{})
	require.NoError(t, err)
	require.False(t, rawStatus.Enabled)
	_, err = rawAuth.AuthDisable(ctx, &etcdserverpb.AuthDisableRequest{})
	require.NoError(t, err)
	_, err = client.AuthDisable(ctx)
	require.NoError(t, err)
	_, err = client.Put(ctx, "/a1126/auth-disabled/key", "value")
	require.NoError(t, err)
	got, err := client.Get(ctx, "/a1126/auth-disabled/key")
	require.NoError(t, err)
	require.Len(t, got.Kvs, 1)
	require.Equal(t, "value", string(got.Kvs[0].Value))
}

func TestClientAuthDisableKeepsCredentialedWatchUsableAfterReconnect(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()
	_ = setupAuthKVUser(t, server)

	register := func(grpcServer *grpc.Server) {
		etcdserverpb.RegisterKVServer(grpcServer, server)
		etcdserverpb.RegisterWatchServer(grpcServer, server)
		etcdserverpb.RegisterAuthServer(grpcServer, server)
	}
	firstGRPC := grpc.NewServer(server.ClientServerOptions()...)
	register(firstGRPC)
	firstListener := bufconn.Listen(1 << 20)
	go func() { _ = firstGRPC.Serve(firstListener) }()

	var listenerMu sync.RWMutex
	currentListener := firstListener
	client, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{"bufnet"},
		DialTimeout: 10 * time.Second,
		Username:    "root",
		Password:    "root-secret",
		DialOptions: []grpc.DialOption{
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
				listenerMu.RLock()
				listener := currentListener
				listenerMu.RUnlock()
				return listener.Dial()
			}),
		},
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, client.Close()) })

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	key := "/a3501/auth-disable-watch/key"
	watch := client.Watch(ctx, key, clientv3.WithRev(1), clientv3.WithCreatedNotify())
	created := <-watch
	require.NoError(t, created.Err())
	require.True(t, created.Created)
	require.Empty(t, created.Events)
	_, err = client.AuthDisable(ctx)
	require.NoError(t, err)

	secondGRPC := grpc.NewServer(server.ClientServerOptions()...)
	register(secondGRPC)
	secondListener := bufconn.Listen(1 << 20)
	listenerMu.Lock()
	currentListener = secondListener
	listenerMu.Unlock()
	firstGRPC.Stop()
	go func() { _ = secondGRPC.Serve(secondListener) }()
	t.Cleanup(secondGRPC.Stop)

	_, err = client.Put(ctx, key, "value")
	require.NoError(t, err)
	for {
		select {
		case response, ok := <-watch:
			require.True(t, ok, "credentialed watch closed after auth disable and transport reconnect")
			require.NoError(t, response.Err())
			if len(response.Events) == 0 {
				continue
			}
			require.Len(t, response.Events, 1)
			require.Equal(t, []byte(key), response.Events[0].Kv.Key)
			require.Equal(t, []byte("value"), response.Events[0].Kv.Value)
			return
		case <-ctx.Done():
			t.Fatal("credentialed watch did not resume after auth disable and transport reconnect")
		}
	}
}

func TestClientAuthUserErrorsMatchEtcd(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	grpcServer := grpc.NewServer(server.ClientServerOptions()...)
	etcdserverpb.RegisterAuthServer(grpcServer, server)
	listener := bufconn.Listen(1 << 20)
	go func() { _ = grpcServer.Serve(listener) }()
	t.Cleanup(grpcServer.Stop)

	client, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{"bufnet"},
		DialTimeout: time.Second,
		DialOptions: []grpc.DialOption{
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
				return listener.Dial()
			}),
		},
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, client.Close()) })

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	rawAuth := etcdserverpb.NewAuthClient(client.ActiveConnection())
	_, err = client.UserAdd(ctx, "a1129-user", "secret")
	require.NoError(t, err)
	_, err = rawAuth.UserAdd(ctx, &etcdserverpb.AuthUserAddRequest{Name: "a1129-raw-user", Password: "secret"})
	require.NoError(t, err)
	_, err = client.UserAdd(ctx, "a1129-user", "secret")
	requireAuthClientError(t, err, codes.Unknown, "etcdserver: user name already exists", rpctypes.ErrUserAlreadyExist)
	_, rawDuplicateUserErr := rawAuth.UserAdd(ctx, &etcdserverpb.AuthUserAddRequest{Name: "a1129-raw-user", Password: "secret"})
	requireAuthClientError(t, rawDuplicateUserErr, codes.FailedPrecondition, "etcdserver: user name already exists")

	_, err = client.UserDelete(ctx, "a1129-missing-user")
	requireAuthClientError(t, err, codes.Unknown, "etcdserver: user name not found", rpctypes.ErrUserNotFound)
	_, err = client.UserDelete(ctx, "")
	requireAuthClientError(t, err, codes.Unknown, "etcdserver: user name not found", rpctypes.ErrUserNotFound)
	_, err = client.UserChangePassword(ctx, "", "")
	requireAuthClientError(t, err, codes.Unknown, "etcdserver: user name not found", rpctypes.ErrUserNotFound)
	_, rawMissingUserErr := rawAuth.UserDelete(ctx, &etcdserverpb.AuthUserDeleteRequest{Name: "a1129-raw-missing-user"})
	requireAuthClientError(t, rawMissingUserErr, codes.FailedPrecondition, "etcdserver: user name not found")
	_, rawEmptyUserDeleteErr := rawAuth.UserDelete(ctx, &etcdserverpb.AuthUserDeleteRequest{Name: ""})
	requireAuthClientError(t, rawEmptyUserDeleteErr, codes.FailedPrecondition, "etcdserver: user name not found")
	_, rawEmptyUserChangeErr := rawAuth.UserChangePassword(ctx, &etcdserverpb.AuthUserChangePasswordRequest{Name: "", HashedPassword: "%%%"})
	requireAuthClientError(t, rawEmptyUserChangeErr, codes.FailedPrecondition, "etcdserver: user name not found")

	_, err = client.UserGrantRole(ctx, "a1129-user", "a1129-missing-role")
	requireAuthClientError(t, err, codes.Unknown, "etcdserver: role name not found", rpctypes.ErrRoleNotFound)
	_, err = client.UserGrantRole(ctx, "a1129-user", "")
	requireAuthClientError(t, err, codes.Unknown, "etcdserver: role name not found", rpctypes.ErrRoleNotFound)
	_, rawMissingRoleErr := rawAuth.UserGrantRole(ctx, &etcdserverpb.AuthUserGrantRoleRequest{
		User: "a1129-raw-user",
		Role: "a1129-raw-missing-role",
	})
	requireAuthClientError(t, rawMissingRoleErr, codes.FailedPrecondition, "etcdserver: role name not found")
	_, rawEmptyRoleGrantErr := rawAuth.UserGrantRole(ctx, &etcdserverpb.AuthUserGrantRoleRequest{
		User: "a1129-raw-user",
		Role: "",
	})
	requireAuthClientError(t, rawEmptyRoleGrantErr, codes.FailedPrecondition, "etcdserver: role name not found")

	_, err = client.RoleAdd(ctx, "a1129-unused-role")
	require.NoError(t, err)
	_, err = rawAuth.RoleAdd(ctx, &etcdserverpb.AuthRoleAddRequest{Name: "a1129-raw-unused-role"})
	require.NoError(t, err)
	_, err = client.UserRevokeRole(ctx, "a1129-user", "a1129-unused-role")
	requireAuthClientError(t, err, codes.Unknown, "etcdserver: role is not granted to the user", rpctypes.ErrRoleNotGranted)
	_, err = client.UserRevokeRole(ctx, "a1129-user", "")
	requireAuthClientError(t, err, codes.Unknown, "etcdserver: role is not granted to the user", rpctypes.ErrRoleNotGranted)
	_, rawRoleNotGrantedErr := rawAuth.UserRevokeRole(ctx, &etcdserverpb.AuthUserRevokeRoleRequest{
		Name: "a1129-raw-user",
		Role: "a1129-raw-unused-role",
	})
	requireAuthClientError(t, rawRoleNotGrantedErr, codes.FailedPrecondition, "etcdserver: role is not granted to the user")
	_, rawEmptyRoleRevokeErr := rawAuth.UserRevokeRole(ctx, &etcdserverpb.AuthUserRevokeRoleRequest{
		Name: "a1129-raw-user",
		Role: "",
	})
	requireAuthClientError(t, rawEmptyRoleRevokeErr, codes.FailedPrecondition, "etcdserver: role is not granted to the user")
}

func TestClientAuthRootProtectionAndDuplicateRoleErrors(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	grpcServer := grpc.NewServer(server.ClientServerOptions()...)
	etcdserverpb.RegisterAuthServer(grpcServer, server)
	listener := bufconn.Listen(1 << 20)
	go func() { _ = grpcServer.Serve(listener) }()
	t.Cleanup(grpcServer.Stop)

	dialOptions := []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return listener.Dial()
		}),
	}
	newClient := func(username, password string) *clientv3.Client {
		t.Helper()
		client, err := clientv3.New(clientv3.Config{
			Endpoints:   []string{"bufnet"},
			DialTimeout: time.Second,
			Username:    username,
			Password:    password,
			DialOptions: dialOptions,
		})
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, client.Close()) })
		return client
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	bootstrap := newClient("", "")
	require.NoError(t, addAuthUserRoleAndPermission(ctx, bootstrap, "root", "root-secret", "root", "", ""))
	_, err := bootstrap.AuthEnable(ctx)
	require.NoError(t, err)

	root := newClient("root", "root-secret")
	_, emptyRoleErr := root.RoleAdd(ctx, "")
	requireAuthClientError(t, emptyRoleErr, codes.Unknown, "etcdserver: role name is empty", rpctypes.ErrRoleEmpty)
	rawRootAuth := etcdserverpb.NewAuthClient(root.ActiveConnection())
	_, rawEmptyRoleErr := rawRootAuth.RoleAdd(ctx, &etcdserverpb.AuthRoleAddRequest{Name: ""})
	requireAuthClientError(t, rawEmptyRoleErr, codes.InvalidArgument, "etcdserver: role name is empty")
	_, emptyRoleDeleteErr := root.RoleDelete(ctx, "")
	requireAuthClientError(t, emptyRoleDeleteErr, codes.Unknown, "etcdserver: role name not found", rpctypes.ErrRoleNotFound)
	_, rawEmptyRoleDeleteErr := rawRootAuth.RoleDelete(ctx, &etcdserverpb.AuthRoleDeleteRequest{Role: ""})
	requireAuthClientError(t, rawEmptyRoleDeleteErr, codes.FailedPrecondition, "etcdserver: role name not found")

	_, err = root.RoleAdd(ctx, "a1060-reader")
	require.NoError(t, err)
	_, duplicateRoleErr := root.RoleAdd(ctx, "a1060-reader")
	requireAuthClientError(t, duplicateRoleErr, codes.Unknown, "etcdserver: role name already exists", rpctypes.ErrRoleAlreadyExist)
	_, rawDuplicateRoleErr := rawRootAuth.RoleAdd(ctx, &etcdserverpb.AuthRoleAddRequest{Name: "a1060-reader"})
	requireAuthClientError(t, rawDuplicateRoleErr, codes.FailedPrecondition, "etcdserver: role name already exists")

	_, deleteRootUserErr := root.UserDelete(ctx, "root")
	requireAuthClientError(t, deleteRootUserErr, codes.Unknown, "etcdserver: invalid auth management", rpctypes.ErrInvalidAuthMgmt)
	_, rawDeleteRootUserErr := rawRootAuth.UserDelete(ctx, &etcdserverpb.AuthUserDeleteRequest{Name: "root"})
	requireAuthClientError(t, rawDeleteRootUserErr, codes.InvalidArgument, "etcdserver: invalid auth management")

	_, revokeRootRoleErr := root.UserRevokeRole(ctx, "root", "root")
	requireAuthClientError(t, revokeRootRoleErr, codes.Unknown, "etcdserver: invalid auth management", rpctypes.ErrInvalidAuthMgmt)
	_, rawRevokeRootRoleErr := rawRootAuth.UserRevokeRole(ctx, &etcdserverpb.AuthUserRevokeRoleRequest{Name: "root", Role: "root"})
	requireAuthClientError(t, rawRevokeRootRoleErr, codes.InvalidArgument, "etcdserver: invalid auth management")

	_, deleteRootRoleErr := root.RoleDelete(ctx, "root")
	requireAuthClientError(t, deleteRootRoleErr, codes.Unknown, "etcdserver: invalid auth management", rpctypes.ErrInvalidAuthMgmt)
	_, rawDeleteRootRoleErr := rawRootAuth.RoleDelete(ctx, &etcdserverpb.AuthRoleDeleteRequest{Role: "root"})
	requireAuthClientError(t, rawDeleteRootRoleErr, codes.InvalidArgument, "etcdserver: invalid auth management")

	users, err := root.UserList(ctx)
	require.NoError(t, err)
	require.Contains(t, users.Users, "root")
	rootRole, err := root.RoleGet(ctx, "root")
	require.NoError(t, err)
	require.NotNil(t, rootRole)
}

func TestClientAuthRolePermissionLifecycleErrors(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	grpcServer := grpc.NewServer(server.ClientServerOptions()...)
	etcdserverpb.RegisterAuthServer(grpcServer, server)
	listener := bufconn.Listen(1 << 20)
	go func() { _ = grpcServer.Serve(listener) }()
	t.Cleanup(grpcServer.Stop)

	dialOptions := []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return listener.Dial()
		}),
	}
	newClient := func(username, password string) *clientv3.Client {
		t.Helper()
		client, err := clientv3.New(clientv3.Config{
			Endpoints:   []string{"bufnet"},
			DialTimeout: time.Second,
			Username:    username,
			Password:    password,
			DialOptions: dialOptions,
		})
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, client.Close()) })
		return client
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	bootstrap := newClient("", "")
	require.NoError(t, addAuthUserRoleAndPermission(ctx, bootstrap, "root", "root-secret", "root", "", ""))
	_, err := bootstrap.UserAdd(ctx, "alice", "alice-secret")
	require.NoError(t, err)
	_, err = bootstrap.AuthEnable(ctx)
	require.NoError(t, err)

	root := newClient("root", "root-secret")
	rawRootAuth := etcdserverpb.NewAuthClient(root.ActiveConnection())
	_, err = root.RoleAdd(ctx, "a1061-lifecycle")
	require.NoError(t, err)
	_, err = root.RoleGrantPermission(
		ctx,
		"a1061-lifecycle",
		"/a1061/",
		clientv3.GetPrefixRangeEnd("/a1061/"),
		clientv3.PermissionType(clientv3.PermRead),
	)
	require.NoError(t, err)
	_, err = root.RoleGrantPermission(
		ctx,
		"a1061-lifecycle",
		"/a1061/",
		clientv3.GetPrefixRangeEnd("/a1061/"),
		clientv3.PermissionType(clientv3.PermWrite),
	)
	require.NoError(t, err)
	role, err := root.RoleGet(ctx, "a1061-lifecycle")
	require.NoError(t, err)
	require.Len(t, role.Perm, 1)
	require.Equal(t, clientv3.PermissionType(clientv3.PermWrite), clientv3.PermissionType(role.Perm[0].PermType))
	rawRole, err := rawRootAuth.RoleGet(ctx, &etcdserverpb.AuthRoleGetRequest{Role: "a1061-lifecycle"})
	require.NoError(t, err)
	require.Len(t, rawRole.Perm, 1)
	require.Equal(t, authpb.WRITE, rawRole.Perm[0].PermType)

	_, missingPermissionErr := root.RoleRevokePermission(
		ctx,
		"a1061-lifecycle",
		"/missing/",
		clientv3.GetPrefixRangeEnd("/missing/"),
	)
	requireAuthClientError(t, missingPermissionErr, codes.Unknown, "etcdserver: permission is not granted to the role", rpctypes.ErrPermissionNotGranted)
	_, emptyRoleRevokePermissionErr := root.RoleRevokePermission(ctx, "", "/a1061/", clientv3.GetPrefixRangeEnd("/a1061/"))
	requireAuthClientError(t, emptyRoleRevokePermissionErr, codes.Unknown, "etcdserver: role name not found", rpctypes.ErrRoleNotFound)
	_, rawEmptyRoleRevokeErr := rawRootAuth.RoleRevokePermission(ctx, &etcdserverpb.AuthRoleRevokePermissionRequest{
		Role: "",
		Key:  []byte("/a1061/"),
	})
	requireAuthClientError(t, rawEmptyRoleRevokeErr, codes.FailedPrecondition, "etcdserver: role name not found")
	_, rawMissingRoleRevokeErr := rawRootAuth.RoleRevokePermission(ctx, &etcdserverpb.AuthRoleRevokePermissionRequest{
		Role: "missing",
		Key:  []byte("/a1061/"),
	})
	requireAuthClientError(t, rawMissingRoleRevokeErr, codes.FailedPrecondition, "etcdserver: role name not found")
	_, rawEmptyKeyRevokeErr := rawRootAuth.RoleRevokePermission(ctx, &etcdserverpb.AuthRoleRevokePermissionRequest{
		Role: "a1061-lifecycle",
	})
	requireAuthClientError(t, rawEmptyKeyRevokeErr, codes.FailedPrecondition, "etcdserver: permission is not granted to the role")
	_, rawMissingPermissionErr := rawRootAuth.RoleRevokePermission(ctx, &etcdserverpb.AuthRoleRevokePermissionRequest{
		Role:     "a1061-lifecycle",
		Key:      []byte("/missing/"),
		RangeEnd: []byte(clientv3.GetPrefixRangeEnd("/missing/")),
	})
	requireAuthClientError(t, rawMissingPermissionErr, codes.FailedPrecondition, "etcdserver: permission is not granted to the role")

	_, invalidRangeErr := root.RoleGrantPermission(
		ctx,
		"a1061-lifecycle",
		"z",
		"a",
		clientv3.PermissionType(clientv3.PermRead),
	)
	requireAuthClientError(t, invalidRangeErr, codes.Unknown, "etcdserver: invalid auth management", rpctypes.ErrInvalidAuthMgmt)
	_, emptyRoleGrantErr := root.RoleGrantPermission(
		ctx,
		"",
		"a",
		"",
		clientv3.PermissionType(clientv3.PermRead),
	)
	requireAuthClientError(t, emptyRoleGrantErr, codes.Unknown, "etcdserver: role name not found", rpctypes.ErrRoleNotFound)
	_, rawNilPermissionErr := rawRootAuth.RoleGrantPermission(ctx, &etcdserverpb.AuthRoleGrantPermissionRequest{
		Name: "a1061-lifecycle",
	})
	requireAuthClientError(t, rawNilPermissionErr, codes.InvalidArgument, "etcdserver: permission not given")
	_, rawEmptyPermissionErr := rawRootAuth.RoleGrantPermission(ctx, &etcdserverpb.AuthRoleGrantPermissionRequest{
		Name: "a1061-lifecycle",
		Perm: &authpb.Permission{},
	})
	requireAuthClientError(t, rawEmptyPermissionErr, codes.InvalidArgument, "etcdserver: invalid auth management")
	_, rawInvalidRangeErr := rawRootAuth.RoleGrantPermission(ctx, &etcdserverpb.AuthRoleGrantPermissionRequest{
		Name: "a1061-lifecycle",
		Perm: &authpb.Permission{
			PermType: authpb.READ,
			Key:      []byte("z"),
			RangeEnd: []byte("a"),
		},
	})
	requireAuthClientError(t, rawInvalidRangeErr, codes.InvalidArgument, "etcdserver: invalid auth management")
	_, rawEmptyRoleGrantErr := rawRootAuth.RoleGrantPermission(ctx, &etcdserverpb.AuthRoleGrantPermissionRequest{
		Name: "",
		Perm: &authpb.Permission{
			PermType: authpb.READ,
			Key:      []byte("a"),
		},
	})
	requireAuthClientError(t, rawEmptyRoleGrantErr, codes.FailedPrecondition, "etcdserver: role name not found")

	_, err = root.UserGrantRole(ctx, "alice", "a1061-lifecycle")
	require.NoError(t, err)
	aliceBeforeDelete, err := root.UserGet(ctx, "alice")
	require.NoError(t, err)
	require.Contains(t, aliceBeforeDelete.Roles, "a1061-lifecycle")
	_, err = root.RoleDelete(ctx, "a1061-lifecycle")
	require.NoError(t, err)
	aliceAfterDelete, err := rawRootAuth.UserGet(ctx, &etcdserverpb.AuthUserGetRequest{Name: "alice"})
	require.NoError(t, err)
	require.NotContains(t, aliceAfterDelete.Roles, "a1061-lifecycle")
}

func TestClientAuthImplicitRootRoleAndCredentialErrors(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	grpcServer := grpc.NewServer(server.ClientServerOptions()...)
	etcdserverpb.RegisterAuthServer(grpcServer, server)
	listener := bufconn.Listen(1 << 20)
	go func() { _ = grpcServer.Serve(listener) }()
	t.Cleanup(grpcServer.Stop)

	dialOptions := []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return listener.Dial()
		}),
	}
	client, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{"bufnet"},
		DialTimeout: time.Second,
		DialOptions: dialOptions,
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, client.Close()) })

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err = client.UserAdd(ctx, "root", "root-secret")
	require.NoError(t, err)
	_, err = client.UserGrantRole(ctx, "root", "root")
	require.NoError(t, err)
	_, err = client.UserAddWithOptions(ctx, "nopass", "", &clientv3.UserAddOptions{NoPassword: true})
	require.NoError(t, err)

	_, rootRoleBeforeEnableErr := client.RoleGet(ctx, "root")
	requireAuthClientError(t, rootRoleBeforeEnableErr, codes.Unknown, "etcdserver: role name not found", rpctypes.ErrRoleNotFound)
	rawBootstrapAuth := etcdserverpb.NewAuthClient(client.ActiveConnection())
	_, rawRootRoleBeforeEnableErr := rawBootstrapAuth.RoleGet(ctx, &etcdserverpb.AuthRoleGetRequest{Role: "root"})
	requireAuthClientError(t, rawRootRoleBeforeEnableErr, codes.FailedPrecondition, "etcdserver: role name not found")

	_, err = client.AuthEnable(ctx)
	require.NoError(t, err)
	statusResponse, err := client.AuthStatus(ctx)
	require.NoError(t, err)
	require.True(t, statusResponse.Enabled)
	require.Positive(t, statusResponse.AuthRevision)
	rawAnonymousAuth := etcdserverpb.NewAuthClient(client.ActiveConnection())
	rawStatus, err := rawAnonymousAuth.AuthStatus(ctx, &etcdserverpb.AuthStatusRequest{})
	require.NoError(t, err)
	require.True(t, rawStatus.Enabled)

	_, anonymousUserAddErr := client.UserAdd(ctx, "anonymous", "secret")
	requireAuthClientError(t, anonymousUserAddErr, codes.Unknown, "etcdserver: user name is empty", rpctypes.ErrUserEmpty)
	_, rawAnonymousDisableErr := rawAnonymousAuth.AuthDisable(ctx, &etcdserverpb.AuthDisableRequest{})
	requireAuthClientError(t, rawAnonymousDisableErr, codes.InvalidArgument, "etcdserver: user name is empty")

	rootClient, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{"bufnet"},
		DialTimeout: time.Second,
		Username:    "root",
		Password:    "root-secret",
		DialOptions: dialOptions,
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, rootClient.Close()) })
	_, rootRoleAfterEnableErr := rootClient.RoleGet(ctx, "root")
	requireAuthClientError(t, rootRoleAfterEnableErr, codes.Unknown, "etcdserver: role name not found", rpctypes.ErrRoleNotFound)
	rawRootAuth := etcdserverpb.NewAuthClient(rootClient.ActiveConnection())
	_, rawRootRoleAfterEnableErr := rawRootAuth.RoleGet(ctx, &etcdserverpb.AuthRoleGetRequest{Role: "root"})
	requireAuthClientError(t, rawRootRoleAfterEnableErr, codes.FailedPrecondition, "etcdserver: role name not found")

	_, err = rootClient.UserAdd(ctx, "alice", "alice-secret")
	require.NoError(t, err)
	_, err = rawRootAuth.UserAdd(ctx, &etcdserverpb.AuthUserAddRequest{Name: "raw-alice", Password: "raw-alice-secret"})
	require.NoError(t, err)
	_, err = rawRootAuth.RoleAdd(ctx, &etcdserverpb.AuthRoleAddRequest{Name: "a1204-allowed"})
	require.NoError(t, err)
	_, err = rawRootAuth.UserGrantRole(ctx, &etcdserverpb.AuthUserGrantRoleRequest{User: "alice", Role: "a1204-allowed"})
	require.NoError(t, err)
	aliceClient, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{"bufnet"},
		DialTimeout: time.Second,
		Username:    "alice",
		Password:    "alice-secret",
		DialOptions: dialOptions,
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, aliceClient.Close()) })
	rawAliceAuth := etcdserverpb.NewAuthClient(aliceClient.ActiveConnection())
	_, rawAliceDisableErr := rawAliceAuth.AuthDisable(ctx, &etcdserverpb.AuthDisableRequest{})
	requireAuthClientError(t, rawAliceDisableErr, codes.PermissionDenied, "etcdserver: permission denied")
	rawAliceSelf, err := rawAliceAuth.UserGet(ctx, &etcdserverpb.AuthUserGetRequest{Name: "alice"})
	require.NoError(t, err)
	require.Contains(t, rawAliceSelf.Roles, "a1204-allowed")
	_, rawAliceRootUserErr := rawAliceAuth.UserGet(ctx, &etcdserverpb.AuthUserGetRequest{Name: "root"})
	requireAuthClientError(t, rawAliceRootUserErr, codes.PermissionDenied, "etcdserver: permission denied")
	rawAliceRole, err := rawAliceAuth.RoleGet(ctx, &etcdserverpb.AuthRoleGetRequest{Role: "a1204-allowed"})
	require.NoError(t, err)
	require.Empty(t, rawAliceRole.Perm)
	_, rawAliceRootRoleErr := rawAliceAuth.RoleGet(ctx, &etcdserverpb.AuthRoleGetRequest{Role: "root"})
	requireAuthClientError(t, rawAliceRootRoleErr, codes.PermissionDenied, "etcdserver: permission denied")
	_, rawAliceUserListErr := rawAliceAuth.UserList(ctx, &etcdserverpb.AuthUserListRequest{})
	requireAuthClientError(t, rawAliceUserListErr, codes.PermissionDenied, "etcdserver: permission denied")
	_, rawAliceRoleListErr := rawAliceAuth.RoleList(ctx, &etcdserverpb.AuthRoleListRequest{})
	requireAuthClientError(t, rawAliceRoleListErr, codes.PermissionDenied, "etcdserver: permission denied")
	rawRootRoles, err := rawRootAuth.RoleList(ctx, &etcdserverpb.AuthRoleListRequest{})
	require.NoError(t, err)
	require.Contains(t, rawRootRoles.Roles, "a1204-allowed")

	_, wrongCredentialsErr := client.Authenticate(ctx, "missing", "wrong")
	requireAuthClientError(
		t,
		wrongCredentialsErr,
		codes.Unknown,
		"etcdserver: authentication failed, invalid user ID or password",
		rpctypes.ErrAuthFailed,
	)
	_, rawWrongCredentialsErr := rawAnonymousAuth.Authenticate(ctx, &etcdserverpb.AuthenticateRequest{Name: "missing", Password: "wrong"})
	requireAuthClientError(
		t,
		rawWrongCredentialsErr,
		codes.InvalidArgument,
		"etcdserver: authentication failed, invalid user ID or password",
	)

	_, noPasswordErr := client.Authenticate(ctx, "nopass", "password")
	requireAuthClientError(
		t,
		noPasswordErr,
		codes.Unknown,
		"auth: authentication failed, password was given for no password user",
	)
	_, rawNoPasswordErr := rawAnonymousAuth.Authenticate(ctx, &etcdserverpb.AuthenticateRequest{Name: "nopass", Password: "password"})
	requireAuthClientError(
		t,
		rawNoPasswordErr,
		codes.Unknown,
		"auth: authentication failed, password was given for no password user",
	)

	disableResponse, err := rawRootAuth.AuthDisable(ctx, &etcdserverpb.AuthDisableRequest{})
	require.NoError(t, err)
	require.NotNil(t, disableResponse.Header)
	disabledStatus, err := rawAnonymousAuth.AuthStatus(ctx, &etcdserverpb.AuthStatusRequest{})
	require.NoError(t, err)
	require.False(t, disabledStatus.Enabled)
}

func TestClientAuthKVDeniedOperationsPreserveData(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	grpcServer := grpc.NewServer(server.ClientServerOptions()...)
	etcdserverpb.RegisterKVServer(grpcServer, server)
	etcdserverpb.RegisterAuthServer(grpcServer, server)
	listener := bufconn.Listen(1 << 20)
	go func() { _ = grpcServer.Serve(listener) }()
	t.Cleanup(grpcServer.Stop)

	dialOptions := []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return listener.Dial()
		}),
	}
	newClient := func(username, password string) *clientv3.Client {
		t.Helper()
		client, err := clientv3.New(clientv3.Config{
			Endpoints:   []string{"bufnet"},
			DialTimeout: time.Second,
			Username:    username,
			Password:    password,
			DialOptions: dialOptions,
		})
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, client.Close()) })
		return client
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	bootstrap := newClient("", "")
	require.NoError(t, addAuthUserRoleAndPermission(ctx, bootstrap, "root", "root-secret", "root", "", ""))
	require.NoError(t, addAuthUserRoleAndPermission(
		ctx,
		bootstrap,
		"alice",
		"alice-secret",
		"a1063-allowed",
		"/a1063/allowed/",
		clientv3.GetPrefixRangeEnd("/a1063/allowed/"),
	))
	_, err := bootstrap.AuthEnable(ctx)
	require.NoError(t, err)

	root := newClient("root", "root-secret")
	alice := newClient("alice", "alice-secret")
	rawAliceKV := etcdserverpb.NewKVClient(alice.ActiveConnection())
	_, err = root.Put(ctx, "/a1063/allowed/key", "allowed")
	require.NoError(t, err)
	_, err = root.Put(ctx, "/a1063/denied/put", "before-put")
	require.NoError(t, err)
	_, err = root.Put(ctx, "/a1063/denied/delete", "before-delete")
	require.NoError(t, err)
	_, err = root.Put(ctx, "/a1063/denied/txn-put", "before-txn-put")
	require.NoError(t, err)
	_, err = root.Put(ctx, "/a1063/denied/txn-delete", "before-txn-delete")
	require.NoError(t, err)

	allowed, err := alice.Get(ctx, "/a1063/allowed/key")
	require.NoError(t, err)
	require.Len(t, allowed.Kvs, 1)
	require.Equal(t, "allowed", string(allowed.Kvs[0].Value))
	rawAllowed, err := rawAliceKV.Range(ctx, &etcdserverpb.RangeRequest{Key: []byte("/a1063/allowed/key")})
	require.NoError(t, err)
	require.Len(t, rawAllowed.Kvs, 1)
	require.Equal(t, "allowed", string(rawAllowed.Kvs[0].Value))

	_, deniedGetErr := alice.Get(ctx, "/a1063/denied/put")
	requireAuthClientError(t, deniedGetErr, codes.Unknown, "etcdserver: permission denied", rpctypes.ErrPermissionDenied)
	_, rawDeniedRangeErr := rawAliceKV.Range(ctx, &etcdserverpb.RangeRequest{Key: []byte("/a1063/denied/put")})
	requireAuthClientError(t, rawDeniedRangeErr, codes.PermissionDenied, "etcdserver: permission denied")

	_, deniedPutErr := alice.Put(ctx, "/a1063/denied/put", "after-put")
	requireAuthClientError(t, deniedPutErr, codes.Unknown, "etcdserver: permission denied", rpctypes.ErrPermissionDenied)
	_, rawDeniedPutErr := rawAliceKV.Put(ctx, &etcdserverpb.PutRequest{
		Key:   []byte("/a1063/denied/put"),
		Value: []byte("after-raw-put"),
	})
	requireAuthClientError(t, rawDeniedPutErr, codes.PermissionDenied, "etcdserver: permission denied")

	_, deniedDeleteErr := alice.Delete(ctx, "/a1063/denied/delete", clientv3.WithPrevKV())
	requireAuthClientError(t, deniedDeleteErr, codes.Unknown, "etcdserver: permission denied", rpctypes.ErrPermissionDenied)
	_, rawDeniedDeleteErr := rawAliceKV.DeleteRange(ctx, &etcdserverpb.DeleteRangeRequest{
		Key:    []byte("/a1063/denied/delete"),
		PrevKv: true,
	})
	requireAuthClientError(t, rawDeniedDeleteErr, codes.PermissionDenied, "etcdserver: permission denied")

	twoLevelNested := func(op clientv3.Op) clientv3.Op {
		return clientv3.OpTxn(nil, []clientv3.Op{
			clientv3.OpTxn(nil, []clientv3.Op{op}, nil),
		}, nil)
	}
	rawTwoLevelNested := func(op *etcdserverpb.RequestOp) *etcdserverpb.TxnRequest {
		return &etcdserverpb.TxnRequest{Success: []*etcdserverpb.RequestOp{{
			Request: &etcdserverpb.RequestOp_RequestTxn{RequestTxn: &etcdserverpb.TxnRequest{
				Success: []*etcdserverpb.RequestOp{op},
			}},
		}}}
	}
	_, nestedDeniedPutErr := alice.Txn(ctx).Then(
		twoLevelNested(clientv3.OpPut("/a1063/denied/txn-put", "after-txn-put")),
	).Commit()
	requireAuthClientError(t, nestedDeniedPutErr, codes.Unknown, "etcdserver: permission denied", rpctypes.ErrPermissionDenied)
	_, rawNestedDeniedPutErr := rawAliceKV.Txn(ctx, rawTwoLevelNested(rawGRPCPutRequestOp(&etcdserverpb.PutRequest{
		Key:   []byte("/a1063/denied/txn-put"),
		Value: []byte("after-raw-txn-put"),
	})))
	requireAuthClientError(t, rawNestedDeniedPutErr, codes.PermissionDenied, "etcdserver: permission denied")

	_, nestedDeniedDeleteErr := alice.Txn(ctx).Then(
		twoLevelNested(clientv3.OpDelete("/a1063/denied/txn-delete", clientv3.WithPrevKV())),
	).Commit()
	requireAuthClientError(t, nestedDeniedDeleteErr, codes.Unknown, "etcdserver: permission denied", rpctypes.ErrPermissionDenied)
	_, rawNestedDeniedDeleteErr := rawAliceKV.Txn(ctx, rawTwoLevelNested(&etcdserverpb.RequestOp{
		Request: &etcdserverpb.RequestOp_RequestDeleteRange{RequestDeleteRange: &etcdserverpb.DeleteRangeRequest{
			Key:    []byte("/a1063/denied/txn-delete"),
			PrevKv: true,
		}},
	}))
	requireAuthClientError(t, rawNestedDeniedDeleteErr, codes.PermissionDenied, "etcdserver: permission denied")

	for key, value := range map[string]string{
		"/a1063/denied/put":        "before-put",
		"/a1063/denied/delete":     "before-delete",
		"/a1063/denied/txn-put":    "before-txn-put",
		"/a1063/denied/txn-delete": "before-txn-delete",
	} {
		response, getErr := root.Get(ctx, key)
		require.NoError(t, getErr)
		require.Len(t, response.Kvs, 1, key)
		require.Equal(t, value, string(response.Kvs[0].Value), key)
	}
}

func TestClientAuthLeaseKeyVisibilityAndLeasedPutDenials(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	grpcServer := grpc.NewServer(server.ClientServerOptions()...)
	etcdserverpb.RegisterKVServer(grpcServer, server)
	etcdserverpb.RegisterAuthServer(grpcServer, server)
	etcdserverpb.RegisterLeaseServer(grpcServer, server)
	listener := bufconn.Listen(1 << 20)
	go func() { _ = grpcServer.Serve(listener) }()
	t.Cleanup(grpcServer.Stop)

	dialOptions := []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return listener.Dial()
		}),
	}
	newClient := func(username, password string) *clientv3.Client {
		t.Helper()
		client, err := clientv3.New(clientv3.Config{
			Endpoints:   []string{"bufnet"},
			DialTimeout: time.Second,
			Username:    username,
			Password:    password,
			DialOptions: dialOptions,
		})
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, client.Close()) })
		return client
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	bootstrap := newClient("", "")
	require.NoError(t, addAuthUserRoleAndPermission(ctx, bootstrap, "root", "root-secret", "root", "", ""))
	require.NoError(t, addAuthUserRoleAndPermission(
		ctx,
		bootstrap,
		"alice",
		"alice-secret",
		"a1064-allowed",
		"/a1064/allowed/",
		clientv3.GetPrefixRangeEnd("/a1064/allowed/"),
	))
	_, err := bootstrap.AuthEnable(ctx)
	require.NoError(t, err)

	root := newClient("root", "root-secret")
	alice := newClient("alice", "alice-secret")
	lease, err := root.Grant(ctx, 60)
	require.NoError(t, err)
	_, err = root.Put(ctx, "/a1064/protected/leased", "secret", clientv3.WithLease(lease.ID))
	require.NoError(t, err)

	userTTL, err := alice.TimeToLive(ctx, lease.ID)
	require.NoError(t, err)
	require.Equal(t, lease.ID, userTTL.ID)
	require.Positive(t, userTTL.TTL)
	require.Empty(t, userTTL.Keys)

	_, userTTLWithKeysErr := alice.TimeToLive(ctx, lease.ID, clientv3.WithAttachedKeys())
	requireAuthClientError(t, userTTLWithKeysErr, codes.Unknown, "etcdserver: permission denied", rpctypes.ErrPermissionDenied)
	rootTTLWithKeys, err := root.TimeToLive(ctx, lease.ID, clientv3.WithAttachedKeys())
	require.NoError(t, err)
	require.Contains(t, byteSlicesToStrings(rootTTLWithKeys.Keys), "/a1064/protected/leased")

	_, leasedPutErr := alice.Put(ctx, "/a1064/allowed/leased-put", "value", clientv3.WithLease(lease.ID))
	requireAuthClientError(t, leasedPutErr, codes.Unknown, "etcdserver: permission denied", rpctypes.ErrPermissionDenied)
	_, nestedLeasedPutErr := alice.Txn(ctx).Then(clientv3.OpTxn(
		nil,
		[]clientv3.Op{clientv3.OpPut("/a1064/allowed/nested-leased", "value", clientv3.WithLease(lease.ID))},
		nil,
	)).Commit()
	requireAuthClientError(t, nestedLeasedPutErr, codes.Unknown, "etcdserver: permission denied", rpctypes.ErrPermissionDenied)

	allowedKeys, err := root.Get(ctx, "/a1064/allowed/", clientv3.WithPrefix())
	require.NoError(t, err)
	require.Empty(t, allowedKeys.Kvs)
}

func TestClientAuthLeaseKeepAliveTracksPermissionChanges(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	grpcServer := grpc.NewServer(server.ClientServerOptions()...)
	etcdserverpb.RegisterKVServer(grpcServer, server)
	etcdserverpb.RegisterAuthServer(grpcServer, server)
	etcdserverpb.RegisterLeaseServer(grpcServer, server)
	listener := bufconn.Listen(1 << 20)
	go func() { _ = grpcServer.Serve(listener) }()
	t.Cleanup(grpcServer.Stop)

	dialOptions := []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return listener.Dial()
		}),
	}
	newClient := func(username, password string) *clientv3.Client {
		t.Helper()
		client, err := clientv3.New(clientv3.Config{
			Endpoints:   []string{"bufnet"},
			DialTimeout: time.Second,
			Username:    username,
			Password:    password,
			DialOptions: dialOptions,
		})
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, client.Close()) })
		return client
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	bootstrap := newClient("", "")
	require.NoError(t, addAuthUserRoleAndPermission(ctx, bootstrap, "root", "root-secret", "root", "", ""))
	require.NoError(t, addAuthUserRoleAndPermission(
		ctx,
		bootstrap,
		"alice",
		"alice-secret",
		"a1065-allowed",
		"/a1065/allowed/",
		clientv3.GetPrefixRangeEnd("/a1065/allowed/"),
	))
	_, err := bootstrap.RoleGrantPermission(
		ctx,
		"a1065-allowed",
		"/a1065/allowed/",
		clientv3.GetPrefixRangeEnd("/a1065/allowed/"),
		clientv3.PermissionType(clientv3.PermReadWrite),
	)
	require.NoError(t, err)
	_, err = bootstrap.AuthEnable(ctx)
	require.NoError(t, err)

	root := newClient("root", "root-secret")
	alice := newClient("alice", "alice-secret")
	lease, err := alice.Grant(ctx, 60)
	require.NoError(t, err)
	_, err = alice.Put(ctx, "/a1065/allowed/leased", "value", clientv3.WithLease(lease.ID))
	require.NoError(t, err)
	keepAlive, err := alice.KeepAliveOnce(ctx, lease.ID)
	require.NoError(t, err)
	require.Equal(t, lease.ID, keepAlive.ID)
	require.Positive(t, keepAlive.TTL)
	rawAliceLease := etcdserverpb.NewLeaseClient(alice.ActiveConnection())
	rawKeepAlive, err := rawAliceLease.LeaseKeepAlive(ctx)
	require.NoError(t, err)
	t.Cleanup(func() { _ = rawKeepAlive.CloseSend() })
	require.NoError(t, rawKeepAlive.Send(&etcdserverpb.LeaseKeepAliveRequest{ID: int64(lease.ID)}))
	rawKeepAliveResponse, err := rawKeepAlive.Recv()
	require.NoError(t, err)
	require.Equal(t, int64(lease.ID), rawKeepAliveResponse.ID)
	require.Positive(t, rawKeepAliveResponse.TTL)

	_, err = root.RoleRevokePermission(
		ctx,
		"a1065-allowed",
		"/a1065/allowed/",
		clientv3.GetPrefixRangeEnd("/a1065/allowed/"),
	)
	require.NoError(t, err)
	_, revokedKeepAliveErr := alice.KeepAliveOnce(ctx, lease.ID)
	requireAuthClientError(t, revokedKeepAliveErr, codes.Unknown, "etcdserver: permission denied", rpctypes.ErrPermissionDenied)
	require.NoError(t, rawKeepAlive.Send(&etcdserverpb.LeaseKeepAliveRequest{ID: int64(lease.ID)}))
	_, rawRevokedKeepAliveErr := rawKeepAlive.Recv()
	requireAuthClientError(t, rawRevokedKeepAliveErr, codes.PermissionDenied, "etcdserver: permission denied")

	_, err = root.RoleGrantPermission(
		ctx,
		"a1065-allowed",
		"/a1065/allowed/",
		clientv3.GetPrefixRangeEnd("/a1065/allowed/"),
		clientv3.PermissionType(clientv3.PermReadWrite),
	)
	require.NoError(t, err)
	restoredKeepAlive, err := alice.KeepAliveOnce(ctx, lease.ID)
	require.NoError(t, err)
	require.Equal(t, lease.ID, restoredKeepAlive.ID)
	require.Positive(t, restoredKeepAlive.TTL)
}

func TestClientAuthWatchStreamTracksPermissionChanges(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	grpcServer := grpc.NewServer(server.ClientServerOptions()...)
	etcdserverpb.RegisterKVServer(grpcServer, server)
	etcdserverpb.RegisterAuthServer(grpcServer, server)
	etcdserverpb.RegisterWatchServer(grpcServer, server)
	listener := bufconn.Listen(1 << 20)
	go func() { _ = grpcServer.Serve(listener) }()
	t.Cleanup(grpcServer.Stop)

	dialOptions := []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return listener.Dial()
		}),
	}
	newClient := func(username, password string) *clientv3.Client {
		t.Helper()
		client, err := clientv3.New(clientv3.Config{
			Endpoints:   []string{"bufnet"},
			DialTimeout: time.Second,
			Username:    username,
			Password:    password,
			DialOptions: dialOptions,
		})
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, client.Close()) })
		return client
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	bootstrap := newClient("", "")
	require.NoError(t, addAuthUserRoleAndPermission(ctx, bootstrap, "root", "root-secret", "root", "", ""))
	require.NoError(t, addAuthUserRoleAndPermission(
		ctx,
		bootstrap,
		"alice",
		"alice-secret",
		"a1066-allowed",
		"/a1066/allowed/",
		clientv3.GetPrefixRangeEnd("/a1066/allowed/"),
	))
	_, err := bootstrap.RoleGrantPermission(
		ctx,
		"a1066-allowed",
		"/a1066/allowed/",
		clientv3.GetPrefixRangeEnd("/a1066/allowed/"),
		clientv3.PermissionType(clientv3.PermReadWrite),
	)
	require.NoError(t, err)
	_, err = bootstrap.AuthEnable(ctx)
	require.NoError(t, err)

	root := newClient("root", "root-secret")
	alice := newClient("alice", "alice-secret")
	stream, err := etcdserverpb.NewWatchClient(alice.ActiveConnection()).Watch(ctx)
	require.NoError(t, err)
	t.Cleanup(func() { _ = stream.CloseSend() })
	watchCreate := func(id int64) *etcdserverpb.WatchRequest {
		return &etcdserverpb.WatchRequest{RequestUnion: &etcdserverpb.WatchRequest_CreateRequest{
			CreateRequest: &etcdserverpb.WatchCreateRequest{
				Key:     []byte("/a1066/allowed/key"),
				WatchId: id,
			},
		}}
	}

	const (
		firstWatchID    = int64(106601)
		deniedWatchID   = int64(106602)
		restoredWatchID = int64(106603)
	)
	require.NoError(t, stream.Send(watchCreate(firstWatchID)))
	firstCreated, err := stream.Recv()
	require.NoError(t, err)
	require.True(t, firstCreated.Created)
	require.False(t, firstCreated.Canceled)
	require.Equal(t, firstWatchID, firstCreated.WatchId)

	_, err = root.RoleRevokePermission(
		ctx,
		"a1066-allowed",
		"/a1066/allowed/",
		clientv3.GetPrefixRangeEnd("/a1066/allowed/"),
	)
	require.NoError(t, err)
	require.NoError(t, stream.Send(watchCreate(deniedWatchID)))
	deniedCreated, err := stream.Recv()
	require.NoError(t, err)
	require.True(t, deniedCreated.Created)
	require.True(t, deniedCreated.Canceled)
	require.Equal(t, int64(-1), deniedCreated.WatchId)
	require.Equal(t, rpctypes.ErrGRPCPermissionDenied.Error(), deniedCreated.CancelReason)

	_, err = root.Put(ctx, "/a1066/allowed/key", "during-revoke")
	require.NoError(t, err)
	existingEvent, err := stream.Recv()
	require.NoError(t, err)
	require.Equal(t, firstWatchID, existingEvent.WatchId)
	require.Len(t, existingEvent.Events, 1)
	require.Equal(t, "during-revoke", string(existingEvent.Events[0].Kv.Value))

	_, err = root.RoleGrantPermission(
		ctx,
		"a1066-allowed",
		"/a1066/allowed/",
		clientv3.GetPrefixRangeEnd("/a1066/allowed/"),
		clientv3.PermissionType(clientv3.PermReadWrite),
	)
	require.NoError(t, err)
	require.NoError(t, stream.Send(watchCreate(restoredWatchID)))
	restoredCreated, err := stream.Recv()
	require.NoError(t, err)
	require.True(t, restoredCreated.Created)
	require.False(t, restoredCreated.Canceled)
	require.Equal(t, restoredWatchID, restoredCreated.WatchId)

	_, err = root.Put(ctx, "/a1066/allowed/key", "after-restore")
	require.NoError(t, err)
	received := make(map[int64]bool, 2)
	for range 2 {
		response, recvErr := stream.Recv()
		require.NoError(t, recvErr)
		if len(response.Events) == 1 &&
			string(response.Events[0].Kv.Value) == "after-restore" {
			received[response.WatchId] = true
		}
	}
	require.True(t, received[firstWatchID])
	require.True(t, received[restoredWatchID])
}

func TestClientAuthTxnPutWithPrevKVDeniedForWriteOnlyRole(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	grpcServer := grpc.NewServer(server.ClientServerOptions()...)
	etcdserverpb.RegisterKVServer(grpcServer, server)
	etcdserverpb.RegisterAuthServer(grpcServer, server)
	listener := bufconn.Listen(1 << 20)
	go func() { _ = grpcServer.Serve(listener) }()
	t.Cleanup(grpcServer.Stop)

	dialOptions := []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return listener.Dial()
		}),
	}
	newClient := func(username, password string) *clientv3.Client {
		t.Helper()
		client, err := clientv3.New(clientv3.Config{
			Endpoints:   []string{"bufnet"},
			DialTimeout: time.Second,
			Username:    username,
			Password:    password,
			DialOptions: dialOptions,
		})
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, client.Close()) })
		return client
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	bootstrap := newClient("", "")
	require.NoError(t, addAuthUserRoleAndPermission(ctx, bootstrap, "root", "root-secret", "root", "", ""))
	_, err := bootstrap.UserAdd(ctx, "writer", "writer-secret")
	require.NoError(t, err)
	_, err = bootstrap.RoleAdd(ctx, "a1067-write-only")
	require.NoError(t, err)
	_, err = bootstrap.RoleGrantPermission(
		ctx,
		"a1067-write-only",
		"/a1067/write-only/key",
		"",
		clientv3.PermissionType(clientv3.PermWrite),
	)
	require.NoError(t, err)
	_, err = bootstrap.UserGrantRole(ctx, "writer", "a1067-write-only")
	require.NoError(t, err)
	_, err = bootstrap.AuthEnable(ctx)
	require.NoError(t, err)

	root := newClient("root", "root-secret")
	writer := newClient("writer", "writer-secret")
	rawWriterKV := etcdserverpb.NewKVClient(writer.ActiveConnection())
	_, err = root.Put(ctx, "/a1067/write-only/key", "before")
	require.NoError(t, err)
	_, rawPutPrevKVErr := rawWriterKV.Put(ctx, &etcdserverpb.PutRequest{
		Key:    []byte("/a1067/write-only/key"),
		Value:  []byte("raw-after"),
		PrevKv: true,
	})
	requireAuthClientError(t, rawPutPrevKVErr, codes.PermissionDenied, "etcdserver: permission denied")

	_, prevKVErr := writer.Txn(ctx).Then(
		clientv3.OpPut("/a1067/write-only/key", "after", clientv3.WithPrevKV()),
	).Commit()
	requireAuthClientError(t, prevKVErr, codes.Unknown, "etcdserver: permission denied", rpctypes.ErrPermissionDenied)
	_, rawPrevKVErr := rawWriterKV.Txn(ctx, &etcdserverpb.TxnRequest{
		Success: []*etcdserverpb.RequestOp{rawGRPCPutRequestOp(&etcdserverpb.PutRequest{
			Key:    []byte("/a1067/write-only/key"),
			Value:  []byte("raw-txn-after"),
			PrevKv: true,
		})},
	})
	requireAuthClientError(t, rawPrevKVErr, codes.PermissionDenied, "etcdserver: permission denied")

	twoLevelNested := func(op clientv3.Op) clientv3.Op {
		return clientv3.OpTxn(nil, []clientv3.Op{
			clientv3.OpTxn(nil, []clientv3.Op{op}, nil),
		}, nil)
	}
	rawTwoLevelNested := func(op *etcdserverpb.RequestOp) *etcdserverpb.TxnRequest {
		return &etcdserverpb.TxnRequest{Success: []*etcdserverpb.RequestOp{{
			Request: &etcdserverpb.RequestOp_RequestTxn{RequestTxn: &etcdserverpb.TxnRequest{
				Success: []*etcdserverpb.RequestOp{op},
			}},
		}}}
	}
	_, nestedPrevKVErr := writer.Txn(ctx).Then(
		twoLevelNested(clientv3.OpPut("/a1067/write-only/key", "nested-after", clientv3.WithPrevKV())),
	).Commit()
	requireAuthClientError(t, nestedPrevKVErr, codes.Unknown, "etcdserver: permission denied", rpctypes.ErrPermissionDenied)
	_, rawNestedPrevKVErr := rawWriterKV.Txn(ctx, rawTwoLevelNested(rawGRPCPutRequestOp(&etcdserverpb.PutRequest{
		Key:    []byte("/a1067/write-only/key"),
		Value:  []byte("raw-nested-after"),
		PrevKv: true,
	})))
	requireAuthClientError(t, rawNestedPrevKVErr, codes.PermissionDenied, "etcdserver: permission denied")

	value, err := root.Get(ctx, "/a1067/write-only/key")
	require.NoError(t, err)
	require.Len(t, value.Kvs, 1)
	require.Equal(t, "before", string(value.Kvs[0].Value))
}

func TestClientAuthClusterAndMaintenanceAuthorization(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	grpcServer := grpc.NewServer(server.ClientServerOptions()...)
	etcdserverpb.RegisterKVServer(grpcServer, server)
	etcdserverpb.RegisterAuthServer(grpcServer, server)
	etcdserverpb.RegisterClusterServer(grpcServer, server)
	etcdserverpb.RegisterMaintenanceServer(grpcServer, server)
	listener := bufconn.Listen(1 << 20)
	go func() { _ = grpcServer.Serve(listener) }()
	t.Cleanup(grpcServer.Stop)

	dialOptions := []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return listener.Dial()
		}),
	}
	newClient := func(username, password string) *clientv3.Client {
		t.Helper()
		client, err := clientv3.New(clientv3.Config{
			Endpoints:   []string{"bufnet"},
			DialTimeout: time.Second,
			Username:    username,
			Password:    password,
			DialOptions: dialOptions,
		})
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, client.Close()) })
		return client
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	bootstrap := newClient("", "")
	require.NoError(t, addAuthUserRoleAndPermission(ctx, bootstrap, "root", "root-secret", "root", "", ""))
	require.NoError(t, addAuthUserRoleAndPermission(
		ctx,
		bootstrap,
		"alice",
		"alice-secret",
		"a1068-reader",
		"/a1068/allowed/",
		clientv3.GetPrefixRangeEnd("/a1068/allowed/"),
	))
	_, err := bootstrap.Put(ctx, "/a1068/allowed/key", "value")
	require.NoError(t, err)
	_, err = bootstrap.AuthEnable(ctx)
	require.NoError(t, err)

	root := newClient("root", "root-secret")
	alice := newClient("alice", "alice-secret")
	_, anonymousStatusErr := bootstrap.Status(ctx, bootstrap.Endpoints()[0])
	requireAuthClientError(t, anonymousStatusErr, codes.Unknown, "etcdserver: user name is empty", rpctypes.ErrUserEmpty)
	_, anonymousMemberListErr := bootstrap.MemberList(ctx)
	requireAuthClientError(t, anonymousMemberListErr, codes.Unknown, "etcdserver: user name is empty", rpctypes.ErrUserEmpty)
	_, anonymousAlarmListErr := bootstrap.AlarmList(ctx)
	requireAuthClientError(t, anonymousAlarmListErr, codes.Unknown, "etcdserver: user name is empty", rpctypes.ErrUserEmpty)

	statusResponse, err := alice.Status(ctx, alice.Endpoints()[0])
	require.NoError(t, err)
	require.NotNil(t, statusResponse.Header)
	memberList, err := alice.MemberList(ctx)
	require.NoError(t, err)
	require.NotNil(t, memberList.Header)
	rawAnonymousCluster := etcdserverpb.NewClusterClient(bootstrap.ActiveConnection())
	_, rawAnonymousMemberListErr := rawAnonymousCluster.MemberList(ctx, &etcdserverpb.MemberListRequest{})
	requireAuthClientError(t, rawAnonymousMemberListErr, codes.InvalidArgument, "etcdserver: user name is empty")
	rawAliceCluster := etcdserverpb.NewClusterClient(alice.ActiveConnection())
	rawMemberList, err := rawAliceCluster.MemberList(ctx, &etcdserverpb.MemberListRequest{})
	require.NoError(t, err)
	require.NotNil(t, rawMemberList.Header)
	require.Zero(t, rawMemberList.Header.Revision)
	_, rawMemberAddErr := rawAliceCluster.MemberAdd(ctx, &etcdserverpb.MemberAddRequest{
		PeerURLs: []string{"http://127.0.0.1:12380"},
	})
	requireAuthClientError(t, rawMemberAddErr, codes.PermissionDenied, "etcdserver: permission denied")
	rawRootCluster := etcdserverpb.NewClusterClient(root.ActiveConnection())
	_, rawRootMemberAddErr := rawRootCluster.MemberAdd(ctx, &etcdserverpb.MemberAddRequest{
		PeerURLs: []string{"http://127.0.0.1:12381"},
	})
	requireAuthClientError(t, rawRootMemberAddErr, codes.Unimplemented, memberMutationUnsupportedMessage)
	alarmList, err := alice.AlarmList(ctx)
	require.NoError(t, err)
	require.NotNil(t, alarmList.Header)
	require.Empty(t, alarmList.Alarms)

	alarmDisarm, err := alice.AlarmDisarm(ctx, &clientv3.AlarmMember{})
	require.NoError(t, err)
	require.Empty(t, alarmDisarm.Alarms)
	_, rawActivateErr := etcdserverpb.NewMaintenanceClient(alice.ActiveConnection()).Alarm(
		ctx,
		&etcdserverpb.AlarmRequest{
			Action: etcdserverpb.AlarmRequest_ACTIVATE,
			Alarm:  etcdserverpb.AlarmType_NOSPACE,
		},
	)
	requireAuthClientError(t, rawActivateErr, codes.PermissionDenied, "etcdserver: permission denied")
	_, userHashErr := alice.HashKV(ctx, alice.Endpoints()[0], 0)
	requireAuthClientError(t, userHashErr, codes.Unknown, "etcdserver: permission denied", rpctypes.ErrPermissionDenied)
	rootHash, err := root.HashKV(ctx, root.Endpoints()[0], 0)
	require.NoError(t, err)
	require.NotNil(t, rootHash.Header)
}

func TestClientAuthCompactRequiresRoot(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	grpcServer := grpc.NewServer(server.ClientServerOptions()...)
	etcdserverpb.RegisterKVServer(grpcServer, server)
	etcdserverpb.RegisterAuthServer(grpcServer, server)
	listener := bufconn.Listen(1 << 20)
	go func() { _ = grpcServer.Serve(listener) }()
	t.Cleanup(grpcServer.Stop)

	dialOptions := []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return listener.Dial()
		}),
	}
	newClient := func(username, password string) *clientv3.Client {
		t.Helper()
		client, err := clientv3.New(clientv3.Config{
			Endpoints:   []string{"bufnet"},
			DialTimeout: time.Second,
			Username:    username,
			Password:    password,
			DialOptions: dialOptions,
		})
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, client.Close()) })
		return client
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	bootstrap := newClient("", "")
	first, err := bootstrap.Put(ctx, "/a1069/compact/first", "value")
	require.NoError(t, err)
	require.NoError(t, addAuthUserRoleAndPermission(ctx, bootstrap, "root", "root-secret", "root", "", ""))
	require.NoError(t, addAuthUserRoleAndPermission(
		ctx,
		bootstrap,
		"alice",
		"alice-secret",
		"a1069-reader",
		"/a1069/compact/",
		clientv3.GetPrefixRangeEnd("/a1069/compact/"),
	))
	_, err = bootstrap.AuthEnable(ctx)
	require.NoError(t, err)

	alice := newClient("alice", "alice-secret")
	root := newClient("root", "root-secret")
	second, err := root.Put(ctx, "/a1069/compact/second", "value")
	require.NoError(t, err)
	_, anonymousCompactErr := bootstrap.Compact(ctx, first.Header.Revision)
	requireAuthClientError(t, anonymousCompactErr, codes.Unknown, "etcdserver: user name is empty", rpctypes.ErrUserEmpty)
	rawAnonymousKV := etcdserverpb.NewKVClient(bootstrap.ActiveConnection())
	_, rawAnonymousCompactErr := rawAnonymousKV.Compact(ctx, &etcdserverpb.CompactionRequest{Revision: first.Header.Revision})
	requireAuthClientError(t, rawAnonymousCompactErr, codes.InvalidArgument, "etcdserver: user name is empty")

	_, userCompactErr := alice.Compact(ctx, first.Header.Revision)
	requireAuthClientError(t, userCompactErr, codes.Unknown, "etcdserver: permission denied", rpctypes.ErrPermissionDenied)
	rawAliceKV := etcdserverpb.NewKVClient(alice.ActiveConnection())
	_, rawUserCompactErr := rawAliceKV.Compact(ctx, &etcdserverpb.CompactionRequest{Revision: first.Header.Revision})
	requireAuthClientError(t, rawUserCompactErr, codes.PermissionDenied, "etcdserver: permission denied")

	rawRootKV := etcdserverpb.NewKVClient(root.ActiveConnection())
	rawCompact, err := rawRootKV.Compact(ctx, &etcdserverpb.CompactionRequest{Revision: first.Header.Revision})
	require.NoError(t, err)
	require.NotNil(t, rawCompact.Header)
	require.GreaterOrEqual(t, rawCompact.Header.Revision, first.Header.Revision)

	compact, err := root.Compact(ctx, second.Header.Revision)
	require.NoError(t, err)
	require.NotNil(t, compact.Header)
	require.GreaterOrEqual(t, compact.Header.Revision, second.Header.Revision)
}

func TestClientAuthPrivilegedMaintenanceAuthorization(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()
	server.SetStaticMembers([]*etcdserverpb.Member{{ID: 1, Name: "voter"}})

	grpcServer := grpc.NewServer(server.ClientServerOptions()...)
	etcdserverpb.RegisterKVServer(grpcServer, server)
	etcdserverpb.RegisterAuthServer(grpcServer, server)
	etcdserverpb.RegisterMaintenanceServer(grpcServer, server)
	listener := bufconn.Listen(1 << 20)
	go func() { _ = grpcServer.Serve(listener) }()
	t.Cleanup(grpcServer.Stop)

	dialOptions := []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return listener.Dial()
		}),
	}
	newClient := func(username, password string) *clientv3.Client {
		t.Helper()
		client, err := clientv3.New(clientv3.Config{
			Endpoints:   []string{"bufnet"},
			DialTimeout: time.Second,
			Username:    username,
			Password:    password,
			DialOptions: dialOptions,
		})
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, client.Close()) })
		return client
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	bootstrap := newClient("", "")
	require.NoError(t, addAuthUserRoleAndPermission(ctx, bootstrap, "root", "root-secret", "root", "", ""))
	require.NoError(t, addAuthUserRoleAndPermission(
		ctx,
		bootstrap,
		"alice",
		"alice-secret",
		"a1070-reader",
		"/a1070/allowed/",
		clientv3.GetPrefixRangeEnd("/a1070/allowed/"),
	))
	_, err := bootstrap.AuthEnable(ctx)
	require.NoError(t, err)

	alice := newClient("alice", "alice-secret")
	root := newClient("root", "root-secret")
	_, anonymousDefragErr := bootstrap.Defragment(ctx, bootstrap.Endpoints()[0])
	requireAuthClientError(t, anonymousDefragErr, codes.Unknown, "etcdserver: user name is empty", rpctypes.ErrUserEmpty)
	_, userDefragErr := alice.Defragment(ctx, alice.Endpoints()[0])
	requireAuthClientError(t, userDefragErr, codes.Unknown, "etcdserver: permission denied", rpctypes.ErrPermissionDenied)
	_, err = root.Defragment(ctx, root.Endpoints()[0])
	require.NoError(t, err)
	rawAnonymousMaintenance := etcdserverpb.NewMaintenanceClient(bootstrap.ActiveConnection())
	_, rawAnonymousStatusErr := rawAnonymousMaintenance.Status(ctx, &etcdserverpb.StatusRequest{})
	requireAuthClientError(t, rawAnonymousStatusErr, codes.InvalidArgument, "etcdserver: user name is empty")
	rawAliceMaintenance := etcdserverpb.NewMaintenanceClient(alice.ActiveConnection())
	rawStatus, err := rawAliceMaintenance.Status(ctx, &etcdserverpb.StatusRequest{})
	require.NoError(t, err)
	require.NotNil(t, rawStatus.Header)
	require.NotEmpty(t, rawStatus.Version)
	rawRootMaintenance := etcdserverpb.NewMaintenanceClient(root.ActiveConnection())

	snapshotReader, userSnapshotErr := alice.SnapshotWithVersion(ctx)
	if snapshotReader != nil && snapshotReader.Snapshot != nil {
		require.NoError(t, snapshotReader.Snapshot.Close())
	}
	requireAuthClientError(t, userSnapshotErr, codes.PermissionDenied, "etcdserver: permission denied")
	rootSnapshot, rootSnapshotErr := root.SnapshotWithVersion(ctx)
	require.NoError(t, rootSnapshotErr)
	require.Equal(t, Version, rootSnapshot.Version)
	rootSnapshotBytes, err := io.ReadAll(rootSnapshot.Snapshot)
	require.NoError(t, err)
	requireSnapshotIntegrityHash(t, rootSnapshotBytes)
	require.NoError(t, rootSnapshot.Snapshot.Close())
	rawSnapshotErr := func(client etcdserverpb.MaintenanceClient) error {
		t.Helper()
		stream, streamErr := client.Snapshot(ctx, &etcdserverpb.SnapshotRequest{})
		if streamErr != nil {
			return streamErr
		}
		_, recvErr := stream.Recv()
		return recvErr
	}
	requireAuthClientError(t, rawSnapshotErr(rawAliceMaintenance), codes.PermissionDenied, "etcdserver: permission denied")
	require.NoError(t, rawSnapshotErr(rawRootMaintenance))

	_, userMoveLeaderErr := alice.MoveLeader(ctx, 1)
	requireAuthClientError(t, userMoveLeaderErr, codes.Unknown, "etcdserver: permission denied", rpctypes.ErrPermissionDenied)
	_, rootMoveLeaderErr := root.MoveLeader(ctx, 1)
	requireAuthClientError(t, rootMoveLeaderErr, codes.Unimplemented, moveLeaderUnsupportedMessage)
	_, rawUserMoveLeaderErr := rawAliceMaintenance.MoveLeader(ctx, &etcdserverpb.MoveLeaderRequest{TargetID: 1})
	requireAuthClientError(t, rawUserMoveLeaderErr, codes.PermissionDenied, "etcdserver: permission denied")
	_, rawRootMoveLeaderErr := rawRootMaintenance.MoveLeader(ctx, &etcdserverpb.MoveLeaderRequest{TargetID: 1})
	requireAuthClientError(t, rawRootMoveLeaderErr, codes.Unimplemented, moveLeaderUnsupportedMessage)

	_, userDowngradeErr := alice.Downgrade(ctx, clientv3.DowngradeValidate, "3.6")
	requireAuthClientError(t, userDowngradeErr, codes.Unknown, "etcdserver: permission denied", rpctypes.ErrPermissionDenied)
	rootDowngradeResponse, rootDowngradeErr := root.Downgrade(ctx, clientv3.DowngradeValidate, "3.6")
	require.NoError(t, rootDowngradeErr)
	require.Equal(t, ClusterVersion, rootDowngradeResponse.Version)
	_, rawUserDowngradeErr := rawAliceMaintenance.Downgrade(ctx, &etcdserverpb.DowngradeRequest{
		Action:  etcdserverpb.DowngradeRequest_VALIDATE,
		Version: "3.6",
	})
	requireAuthClientError(t, rawUserDowngradeErr, codes.PermissionDenied, "etcdserver: permission denied")
	rawRootDowngradeResponse, rawRootDowngradeErr := rawRootMaintenance.Downgrade(ctx, &etcdserverpb.DowngradeRequest{
		Action:  etcdserverpb.DowngradeRequest_VALIDATE,
		Version: "3.6",
	})
	require.NoError(t, rawRootDowngradeErr)
	require.Equal(t, ClusterVersion, rawRootDowngradeResponse.GetVersion())
	require.NotNil(t, rawRootDowngradeResponse.GetHeader())
}

func TestClientAuthRangeStreamAuthorization(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	grpcServer := grpc.NewServer(server.ClientServerOptions()...)
	etcdserverpb.RegisterKVServer(grpcServer, server)
	etcdserverpb.RegisterAuthServer(grpcServer, server)
	listener := bufconn.Listen(1 << 20)
	go func() { _ = grpcServer.Serve(listener) }()
	t.Cleanup(grpcServer.Stop)

	dialOptions := []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return listener.Dial()
		}),
	}
	newClient := func(username, password string) *clientv3.Client {
		t.Helper()
		client, err := clientv3.New(clientv3.Config{
			Endpoints:   []string{"bufnet"},
			DialTimeout: time.Second,
			Username:    username,
			Password:    password,
			DialOptions: dialOptions,
		})
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, client.Close()) })
		return client
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	getStream := func(client *clientv3.Client, key string, options ...clientv3.OpOption) (*clientv3.GetResponse, error) {
		t.Helper()
		stream, err := client.GetStream(ctx, key, options...)
		if err != nil {
			return nil, err
		}
		response, err := clientv3.GetStreamToGetResponse(stream)
		return (*clientv3.GetResponse)(response), err
	}
	rawRangeStream := func(client *clientv3.Client, key, rangeEnd string) (*etcdserverpb.RangeResponse, error) {
		t.Helper()
		stream, err := etcdserverpb.NewKVClient(client.ActiveConnection()).RangeStream(ctx, &etcdserverpb.RangeRequest{
			Key:      []byte(key),
			RangeEnd: []byte(rangeEnd),
		})
		if err != nil {
			return nil, err
		}
		merged := &etcdserverpb.RangeResponse{}
		for {
			chunk, recvErr := stream.Recv()
			if recvErr != nil {
				if recvErr == io.EOF {
					return merged, nil
				}
				return nil, recvErr
			}
			response := chunk.GetRangeResponse()
			if response == nil {
				continue
			}
			merged.Kvs = append(merged.Kvs, response.Kvs...)
			if response.Header != nil {
				merged.Header = response.Header
				merged.Count = response.Count
				merged.More = response.More
			}
		}
	}

	bootstrap := newClient("", "")
	_, err := bootstrap.Put(ctx, "/a1071/allowed/key", "allowed")
	require.NoError(t, err)
	_, err = bootstrap.Put(ctx, "/a1071/protected/key", "protected")
	require.NoError(t, err)
	require.NoError(t, addAuthUserRoleAndPermission(ctx, bootstrap, "root", "root-secret", "root", "", ""))
	require.NoError(t, addAuthUserRoleAndPermission(
		ctx,
		bootstrap,
		"alice",
		"alice-secret",
		"a1071-reader",
		"/a1071/allowed/",
		clientv3.GetPrefixRangeEnd("/a1071/allowed/"),
	))
	_, err = bootstrap.AuthEnable(ctx)
	require.NoError(t, err)

	_, anonymousErr := getStream(bootstrap, "/a1071/allowed/", clientv3.WithPrefix())
	requireAuthClientError(t, anonymousErr, codes.Unknown, "etcdserver: user name is empty", rpctypes.ErrUserEmpty)
	_, rawAnonymousErr := rawRangeStream(bootstrap, "/a1071/allowed/", clientv3.GetPrefixRangeEnd("/a1071/allowed/"))
	requireAuthClientError(t, rawAnonymousErr, codes.InvalidArgument, "etcdserver: user name is empty")

	alice := newClient("alice", "alice-secret")
	allowed, err := getStream(alice, "/a1071/allowed/", clientv3.WithPrefix())
	require.NoError(t, err)
	require.Len(t, allowed.Kvs, 1)
	require.Equal(t, "allowed", string(allowed.Kvs[0].Value))
	rawAllowed, err := rawRangeStream(alice, "/a1071/allowed/", clientv3.GetPrefixRangeEnd("/a1071/allowed/"))
	require.NoError(t, err)
	require.Len(t, rawAllowed.Kvs, 1)
	require.Equal(t, "allowed", string(rawAllowed.Kvs[0].Value))

	_, deniedErr := getStream(alice, "/a1071/protected/", clientv3.WithPrefix())
	requireAuthClientError(t, deniedErr, codes.Unknown, "etcdserver: permission denied", rpctypes.ErrPermissionDenied)
	_, rawDeniedErr := rawRangeStream(alice, "/a1071/protected/", clientv3.GetPrefixRangeEnd("/a1071/protected/"))
	requireAuthClientError(t, rawDeniedErr, codes.PermissionDenied, "etcdserver: permission denied")

	root := newClient("root", "root-secret")
	all, err := getStream(root, "/a1071/", clientv3.WithPrefix())
	require.NoError(t, err)
	require.Len(t, all.Kvs, 2)
	require.Equal(t, int64(2), all.Count)
	rawAll, err := rawRangeStream(root, "/a1071/", clientv3.GetPrefixRangeEnd("/a1071/"))
	require.NoError(t, err)
	require.Len(t, rawAll.Kvs, 2)
	require.Equal(t, int64(2), rawAll.Count)
}

func TestClientAuthLeaseListProtectsInaccessibleAttachments(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	grpcServer := grpc.NewServer(server.ClientServerOptions()...)
	etcdserverpb.RegisterKVServer(grpcServer, server)
	etcdserverpb.RegisterAuthServer(grpcServer, server)
	etcdserverpb.RegisterLeaseServer(grpcServer, server)
	listener := bufconn.Listen(1 << 20)
	go func() { _ = grpcServer.Serve(listener) }()
	t.Cleanup(grpcServer.Stop)

	dialOptions := []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return listener.Dial()
		}),
	}
	newClient := func(username, password string) *clientv3.Client {
		t.Helper()
		client, err := clientv3.New(clientv3.Config{
			Endpoints:   []string{"bufnet"},
			DialTimeout: time.Second,
			Username:    username,
			Password:    password,
			DialOptions: dialOptions,
		})
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, client.Close()) })
		return client
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	bootstrap := newClient("", "")
	require.NoError(t, addAuthUserRoleAndPermission(ctx, bootstrap, "root", "root-secret", "root", "", ""))
	require.NoError(t, addAuthUserRoleAndPermission(
		ctx,
		bootstrap,
		"alice",
		"alice-secret",
		"a1072-reader",
		"/a1072/allowed/",
		clientv3.GetPrefixRangeEnd("/a1072/allowed/"),
	))
	_, err := bootstrap.RoleGrantPermission(
		ctx,
		"a1072-reader",
		"/a1072/allowed/",
		clientv3.GetPrefixRangeEnd("/a1072/allowed/"),
		clientv3.PermissionType(clientv3.PermReadWrite),
	)
	require.NoError(t, err)
	protectedLease, err := bootstrap.Grant(ctx, 60)
	require.NoError(t, err)
	_, err = bootstrap.Put(ctx, "/a1072/protected/leased", "secret", clientv3.WithLease(protectedLease.ID))
	require.NoError(t, err)
	_, err = bootstrap.AuthEnable(ctx)
	require.NoError(t, err)

	alice := newClient("alice", "alice-secret")
	root := newClient("root", "root-secret")
	_, anonymousLeasesErr := bootstrap.Leases(ctx)
	requireAuthClientError(t, anonymousLeasesErr, codes.Unknown, "etcdserver: user name is empty", rpctypes.ErrUserEmpty)
	_, userLeasesErr := alice.Leases(ctx)
	requireAuthClientError(t, userLeasesErr, codes.Unknown, "etcdserver: permission denied", rpctypes.ErrPermissionDenied)
	rootLeases, err := root.Leases(ctx)
	require.NoError(t, err)
	require.Contains(t, leaseIDs(rootLeases.Leases), protectedLease.ID)
	rawAnonymousLease := etcdserverpb.NewLeaseClient(bootstrap.ActiveConnection())
	_, rawAnonymousLeasesErr := rawAnonymousLease.LeaseLeases(ctx, &etcdserverpb.LeaseLeasesRequest{})
	requireAuthClientError(t, rawAnonymousLeasesErr, codes.InvalidArgument, "etcdserver: user name is empty")
	rawAliceLease := etcdserverpb.NewLeaseClient(alice.ActiveConnection())
	rawTTL, err := rawAliceLease.LeaseTimeToLive(ctx, &etcdserverpb.LeaseTimeToLiveRequest{ID: int64(protectedLease.ID)})
	require.NoError(t, err)
	require.Equal(t, int64(protectedLease.ID), rawTTL.ID)
	require.Positive(t, rawTTL.TTL)
	require.Empty(t, rawTTL.Keys)
	_, rawTTLWithKeysErr := rawAliceLease.LeaseTimeToLive(ctx, &etcdserverpb.LeaseTimeToLiveRequest{
		ID:   int64(protectedLease.ID),
		Keys: true,
	})
	requireAuthClientError(t, rawTTLWithKeysErr, codes.PermissionDenied, "etcdserver: permission denied")
	_, rawUserLeasesErr := rawAliceLease.LeaseLeases(ctx, &etcdserverpb.LeaseLeasesRequest{})
	requireAuthClientError(t, rawUserLeasesErr, codes.PermissionDenied, "etcdserver: permission denied")

	_, revokeErr := alice.Revoke(ctx, protectedLease.ID)
	requireAuthClientError(t, revokeErr, codes.Unknown, "etcdserver: permission denied", rpctypes.ErrPermissionDenied)
	_, rawRevokeErr := rawAliceLease.LeaseRevoke(ctx, &etcdserverpb.LeaseRevokeRequest{ID: int64(protectedLease.ID)})
	requireAuthClientError(t, rawRevokeErr, codes.PermissionDenied, "etcdserver: permission denied")
	protected, err := root.Get(ctx, "/a1072/protected/leased")
	require.NoError(t, err)
	require.Len(t, protected.Kvs, 1)
	require.Equal(t, "secret", string(protected.Kvs[0].Value))

	_, err = root.Revoke(ctx, protectedLease.ID)
	require.NoError(t, err)
	allowedLease, err := alice.Grant(ctx, 60)
	require.NoError(t, err)
	_, err = alice.Put(ctx, "/a1072/allowed/leased", "value", clientv3.WithLease(allowedLease.ID))
	require.NoError(t, err)
	aliceLeases, err := alice.Leases(ctx)
	require.NoError(t, err)
	require.Contains(t, leaseIDs(aliceLeases.Leases), allowedLease.ID)
}

func addAuthUserRoleAndPermission(
	ctx context.Context,
	client *clientv3.Client,
	user string,
	password string,
	role string,
	key string,
	rangeEnd string,
) error {
	if _, err := client.UserAdd(ctx, user, password); err != nil {
		return err
	}
	if _, err := client.RoleAdd(ctx, role); err != nil {
		return err
	}
	if role != "root" {
		if _, err := client.RoleGrantPermission(ctx, role, key, rangeEnd, clientv3.PermissionType(clientv3.PermRead)); err != nil {
			return err
		}
	}
	_, err := client.UserGrantRole(ctx, user, role)
	return err
}

func leaseIDs(leases []clientv3.LeaseStatus) []clientv3.LeaseID {
	ids := make([]clientv3.LeaseID, 0, len(leases))
	for _, lease := range leases {
		ids = append(ids, lease.ID)
	}
	return ids
}

func byteSlicesToStrings(values [][]byte) []string {
	strings := make([]string, 0, len(values))
	for _, value := range values {
		strings = append(strings, string(value))
	}
	return strings
}

func requireAuthClientError(t *testing.T, err error, code codes.Code, message string, wantErrorIs ...error) {
	t.Helper()
	if len(wantErrorIs) > 0 {
		require.EqualError(t, err, message)
	} else {
		require.EqualError(t, err, status.Error(code, message).Error())
	}
	for _, want := range wantErrorIs {
		require.ErrorIs(t, err, want)
	}
	require.Equal(t, code, status.Code(err))
	require.Equal(t, message, status.Convert(err).Message())
}
