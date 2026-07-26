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
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
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

	roleGetResponse, err := client.RoleGet(ctx, role)
	require.NoError(t, err)
	require.NotNil(t, roleGetResponse.Header)
	require.Equal(t, put.Header.Revision, roleGetResponse.Header.Revision)
	require.NotZero(t, roleGetResponse.Header.ClusterId)
	require.NotZero(t, roleGetResponse.Header.MemberId)
	require.Positive(t, roleGetResponse.Header.RaftTerm)
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
	aliceOldToken := newTokenClient(oldAuth.Token)
	beforeChange, err := aliceOldToken.Get(ctx, "/a1058/auth-client/key")
	require.NoError(t, err)
	require.Len(t, beforeChange.Kvs, 1)
	require.Equal(t, "secret", string(beforeChange.Kvs[0].Value))

	_, err = root.UserChangePassword(ctx, "alice", "alice-changed")
	require.NoError(t, err)
	_, oldTokenErr := aliceOldToken.Get(ctx, "/a1058/auth-client/key")
	requireAuthClientError(t, oldTokenErr, codes.Unknown, "etcdserver: invalid auth token")

	_, oldPasswordErr := clientv3.New(clientv3.Config{
		Endpoints:   []string{"bufnet"},
		DialTimeout: time.Second,
		Username:    "alice",
		Password:    "alice-secret",
		DialOptions: dialOptions,
	})
	requireAuthClientError(t, oldPasswordErr, codes.Unknown, "etcdserver: authentication failed, invalid user ID or password")

	aliceNewPassword := newClient("alice", "alice-changed")
	afterChange, err := aliceNewPassword.Get(ctx, "/a1058/auth-client/key")
	require.NoError(t, err)
	require.Len(t, afterChange.Kvs, 1)
	require.Equal(t, "secret", string(afterChange.Kvs[0].Value))
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

func requireAuthClientError(t *testing.T, err error, code codes.Code, message string) {
	t.Helper()
	require.Error(t, err)
	require.Equal(t, code, status.Code(err))
	require.Equal(t, message, status.Convert(err).Message())
}
