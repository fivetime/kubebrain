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
	_, err = root.RoleAdd(ctx, "a1060-reader")
	require.NoError(t, err)
	_, duplicateRoleErr := root.RoleAdd(ctx, "a1060-reader")
	requireAuthClientError(t, duplicateRoleErr, codes.Unknown, "etcdserver: role name already exists")

	_, deleteRootUserErr := root.UserDelete(ctx, "root")
	requireAuthClientError(t, deleteRootUserErr, codes.Unknown, "etcdserver: invalid auth management")
	_, revokeRootRoleErr := root.UserRevokeRole(ctx, "root", "root")
	requireAuthClientError(t, revokeRootRoleErr, codes.Unknown, "etcdserver: invalid auth management")
	_, deleteRootRoleErr := root.RoleDelete(ctx, "root")
	requireAuthClientError(t, deleteRootRoleErr, codes.Unknown, "etcdserver: invalid auth management")

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

	_, missingPermissionErr := root.RoleRevokePermission(
		ctx,
		"a1061-lifecycle",
		"/missing/",
		clientv3.GetPrefixRangeEnd("/missing/"),
	)
	requireAuthClientError(t, missingPermissionErr, codes.Unknown, "etcdserver: permission is not granted to the role")
	_, invalidRangeErr := root.RoleGrantPermission(
		ctx,
		"a1061-lifecycle",
		"z",
		"a",
		clientv3.PermissionType(clientv3.PermRead),
	)
	requireAuthClientError(t, invalidRangeErr, codes.Unknown, "etcdserver: invalid auth management")

	_, err = root.UserGrantRole(ctx, "alice", "a1061-lifecycle")
	require.NoError(t, err)
	aliceBeforeDelete, err := root.UserGet(ctx, "alice")
	require.NoError(t, err)
	require.Contains(t, aliceBeforeDelete.Roles, "a1061-lifecycle")
	_, err = root.RoleDelete(ctx, "a1061-lifecycle")
	require.NoError(t, err)
	aliceAfterDelete, err := root.UserGet(ctx, "alice")
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
	requireAuthClientError(t, rootRoleBeforeEnableErr, codes.Unknown, "etcdserver: role name not found")

	_, err = client.AuthEnable(ctx)
	require.NoError(t, err)
	statusResponse, err := client.AuthStatus(ctx)
	require.NoError(t, err)
	require.True(t, statusResponse.Enabled)
	require.Positive(t, statusResponse.AuthRevision)

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
	requireAuthClientError(t, rootRoleAfterEnableErr, codes.Unknown, "etcdserver: role name not found")
	_, err = rootClient.UserAdd(ctx, "alice", "alice-secret")
	require.NoError(t, err)

	_, wrongCredentialsErr := client.Authenticate(ctx, "missing", "wrong")
	requireAuthClientError(
		t,
		wrongCredentialsErr,
		codes.Unknown,
		"etcdserver: authentication failed, invalid user ID or password",
	)
	_, noPasswordErr := client.Authenticate(ctx, "nopass", "password")
	requireAuthClientError(
		t,
		noPasswordErr,
		codes.Unknown,
		"auth: authentication failed, password was given for no password user",
	)
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

	_, deniedGetErr := alice.Get(ctx, "/a1063/denied/put")
	requireAuthClientError(t, deniedGetErr, codes.Unknown, "etcdserver: permission denied")
	_, deniedPutErr := alice.Put(ctx, "/a1063/denied/put", "after-put")
	requireAuthClientError(t, deniedPutErr, codes.Unknown, "etcdserver: permission denied")
	_, deniedDeleteErr := alice.Delete(ctx, "/a1063/denied/delete", clientv3.WithPrevKV())
	requireAuthClientError(t, deniedDeleteErr, codes.Unknown, "etcdserver: permission denied")

	twoLevelNested := func(op clientv3.Op) clientv3.Op {
		return clientv3.OpTxn(nil, []clientv3.Op{
			clientv3.OpTxn(nil, []clientv3.Op{op}, nil),
		}, nil)
	}
	_, nestedDeniedPutErr := alice.Txn(ctx).Then(
		twoLevelNested(clientv3.OpPut("/a1063/denied/txn-put", "after-txn-put")),
	).Commit()
	requireAuthClientError(t, nestedDeniedPutErr, codes.Unknown, "etcdserver: permission denied")
	_, nestedDeniedDeleteErr := alice.Txn(ctx).Then(
		twoLevelNested(clientv3.OpDelete("/a1063/denied/txn-delete", clientv3.WithPrevKV())),
	).Commit()
	requireAuthClientError(t, nestedDeniedDeleteErr, codes.Unknown, "etcdserver: permission denied")

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
	requireAuthClientError(t, userTTLWithKeysErr, codes.Unknown, "etcdserver: permission denied")
	rootTTLWithKeys, err := root.TimeToLive(ctx, lease.ID, clientv3.WithAttachedKeys())
	require.NoError(t, err)
	require.Contains(t, byteSlicesToStrings(rootTTLWithKeys.Keys), "/a1064/protected/leased")

	_, leasedPutErr := alice.Put(ctx, "/a1064/allowed/leased-put", "value", clientv3.WithLease(lease.ID))
	requireAuthClientError(t, leasedPutErr, codes.Unknown, "etcdserver: permission denied")
	_, nestedLeasedPutErr := alice.Txn(ctx).Then(clientv3.OpTxn(
		nil,
		[]clientv3.Op{clientv3.OpPut("/a1064/allowed/nested-leased", "value", clientv3.WithLease(lease.ID))},
		nil,
	)).Commit()
	requireAuthClientError(t, nestedLeasedPutErr, codes.Unknown, "etcdserver: permission denied")

	allowedKeys, err := root.Get(ctx, "/a1064/allowed/", clientv3.WithPrefix())
	require.NoError(t, err)
	require.Empty(t, allowedKeys.Kvs)
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

func byteSlicesToStrings(values [][]byte) []string {
	strings := make([]string, 0, len(values))
	for _, value := range values {
		strings = append(strings, string(value))
	}
	return strings
}

func requireAuthClientError(t *testing.T, err error, code codes.Code, message string) {
	t.Helper()
	require.Error(t, err)
	require.Equal(t, code, status.Code(err))
	require.Equal(t, message, status.Convert(err).Message())
}
