// Copyright 2026 ByteDance and/or its affiliates
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package main

import (
	"context"
	"crypto/sha256"
	"crypto/x509"
	"net/url"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/authpb"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	"go.etcd.io/etcd/client/pkg/v3/transport"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/server/v3/embed"
	"go.uber.org/zap"
	"golang.org/x/crypto/bcrypt"
	"google.golang.org/protobuf/proto"

	"github.com/kubewharf/kubebrain/pkg/etcdsnapshot"
)

func TestSnapshotAuthFixtureInstallCleanupAndCollisionOwnership(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
	defer cancel()
	clientURL, peerURL, err := allocateRestoredSnapshotURLs(false)
	require.NoError(t, err)
	cfg := embed.NewConfig()
	cfg.Name = "snapshot-auth-source"
	cfg.Dir = t.TempDir()
	cfg.ListenClientUrls = []url.URL{clientURL}
	cfg.AdvertiseClientUrls = []url.URL{clientURL}
	cfg.ListenPeerUrls = []url.URL{peerURL}
	cfg.AdvertisePeerUrls = []url.URL{peerURL}
	cfg.InitialCluster = cfg.Name + "=" + peerURL.String()
	cfg.ZapLoggerBuilder = embed.NewZapLoggerBuilder(zap.NewNop())
	server, err := embed.StartEtcd(cfg)
	require.NoError(t, err)
	t.Cleanup(server.Close)
	select {
	case <-server.Server.ReadyNotify():
	case <-ctx.Done():
		t.Fatal("source etcd did not become ready")
	}
	client, err := clientv3.New(clientv3.Config{Endpoints: []string{clientURL.String()}, DialTimeout: 3 * time.Second, Context: ctx, Logger: zap.NewNop()})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, client.Close()) })
	initial, err := client.Get(ctx, "\x00")
	require.NoError(t, err)
	require.NotNil(t, initial.Header)

	fixture := newSnapshotAuthFixture("/probe/auth-install/")
	lastRevision, err := fixture.install(ctx, client, initial.Header.ClusterId, initial.Header.Revision, 3*time.Second)
	require.NoError(t, err)
	require.Positive(t, fixture.expected.revision)
	users, err := client.UserList(ctx)
	require.NoError(t, err)
	require.Len(t, users.Users, len(fixture.expected.users))
	roles, err := client.RoleList(ctx)
	require.NoError(t, err)
	require.Len(t, roles.Roles, len(fixture.expected.roles))
	lastRevision, err = fixture.cleanup(client, initial.Header.ClusterId, lastRevision, 3*time.Second)
	require.NoError(t, err)
	require.Positive(t, lastRevision)
	users, err = client.UserList(ctx)
	require.NoError(t, err)
	require.Empty(t, users.Users)
	roles, err = client.RoleList(ctx)
	require.NoError(t, err)
	require.Empty(t, roles.Roles)

	collision := newSnapshotAuthFixture("/probe/auth-collision/")
	_, err = client.RoleAdd(ctx, collision.expected.roles[0].name)
	require.NoError(t, err)
	_, err = collision.install(ctx, client, initial.Header.ClusterId, lastRevision, 3*time.Second)
	require.ErrorIs(t, err, rpctypes.ErrRoleAlreadyExist)
	_, cleanupErr := collision.cleanup(client, initial.Header.ClusterId, lastRevision, 3*time.Second)
	require.NoError(t, cleanupErr)
	_, err = client.RoleGet(ctx, collision.expected.roles[0].name)
	require.NoError(t, err, "fixture cleanup must not delete a colliding role it did not create")
	_, err = client.RoleDelete(ctx, collision.expected.roles[0].name)
	require.NoError(t, err)

	_, err = client.UserAdd(ctx, "root", "root-secret")
	require.NoError(t, err)
	_, err = client.UserGrantRole(ctx, "root", "root")
	require.NoError(t, err)
	_, err = client.AuthEnable(ctx)
	require.NoError(t, err)
	rootClient, err := clientv3.New(clientv3.Config{
		Endpoints: []string{clientURL.String()}, DialTimeout: 3 * time.Second, Context: ctx,
		Username: "root", Password: "root-secret", Logger: zap.NewNop(),
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, rootClient.Close()) })
	enabled := newSnapshotAuthFixture("/probe/auth-enabled/")
	lastRevision, err = enabled.install(ctx, rootClient, initial.Header.ClusterId, lastRevision, 3*time.Second)
	require.NoError(t, err)
	require.True(t, enabled.expected.enabled)
	lastRevision, err = enabled.cleanup(rootClient, initial.Header.ClusterId, lastRevision, 3*time.Second)
	require.NoError(t, err)
	statusResponse, err := rootClient.AuthStatus(ctx)
	require.NoError(t, err)
	require.True(t, statusResponse.Enabled)
}

func TestConsumeAndValidateSnapshotPreservesEnabledAuthPermissionMatrix(t *testing.T) {
	const prefix = "/probe/auth-enabled-restore/"
	fixture := newSnapshotAuthFixture(prefix)
	fixture.expected.revision = 29
	fixture.expected.enabled = true
	fixture.expected.adminUsername = "root"
	fixture.expected.adminPassword = "restored-root-secret"
	expected := newStreamProbeExpectations(prefix)
	state := snapshotAuthTestState(t, expected, &fixture.expected)
	state.Auth.Enabled = true
	rootHash, err := bcrypt.GenerateFromPassword([]byte(fixture.expected.adminPassword), bcrypt.MinCost)
	require.NoError(t, err)
	state.Auth.Users = append(state.Auth.Users, &authpb.User{
		Name: []byte(fixture.expected.adminUsername), Password: rootHash, Roles: []string{"root"},
		Options: &authpb.UserAddOptions{},
	})
	state.Auth.Roles = append(state.Auth.Roles, &authpb.Role{Name: []byte("root")})
	dir := t.TempDir()
	partial, err := consumeAndValidateSnapshotWithClusterAuth(t.Context(), snapshotAuthReceiver(t, state), dir, expected,
		restoredSnapshotTLSConfig{}, &fixture.expected, 3)
	require.NoError(t, err)
	require.True(t, partial)
	entries, readErr := os.ReadDir(dir)
	require.NoError(t, readErr)
	require.Empty(t, entries)

	disabledState := state
	disabledState.Auth.Enabled = false
	missingEnabledState := cloneRestoredSnapshotAuthExpectation(&fixture.expected)
	missingEnabledState.adminUsername = ""
	missingEnabledState.adminPassword = ""
	partial, err = consumeAndValidateSnapshotWithAuth(t.Context(), snapshotAuthReceiver(t, disabledState), dir, expected,
		restoredSnapshotTLSConfig{}, missingEnabledState)
	require.ErrorContains(t, err, "restored auth status mismatch")
	require.True(t, partial)
	entries, readErr = os.ReadDir(dir)
	require.NoError(t, readErr)
	require.Empty(t, entries)
}

func TestConsumeAndValidateSnapshotPreservesEnabledAuthPermissionMatrixWithTLS(t *testing.T) {
	const prefix = "/probe/auth-enabled-tls-restore/"
	fixture := newSnapshotAuthFixture(prefix)
	fixture.expected.revision = 31
	fixture.expected.enabled = true
	fixture.expected.adminUsername = "root"
	fixture.expected.adminPassword = "restored-root-tls-secret"
	expected := newStreamProbeExpectations(prefix)
	state := snapshotAuthTestState(t, expected, &fixture.expected)
	state.Auth.Enabled = true
	rootHash, err := bcrypt.GenerateFromPassword([]byte(fixture.expected.adminPassword), bcrypt.MinCost)
	require.NoError(t, err)
	state.Auth.Users = append(state.Auth.Users, &authpb.User{
		Name: []byte(fixture.expected.adminUsername), Password: rootHash, Roles: []string{"root"},
		Options: &authpb.UserAddOptions{},
	})
	state.Auth.Roles = append(state.Auth.Roles, &authpb.Role{Name: []byte("root")})
	identity, err := transport.SelfCert(zap.NewNop(), t.TempDir(), []string{"restored-auth-tls.example:443"}, 1,
		x509.ExtKeyUsageClientAuth)
	require.NoError(t, err)
	tlsCfg := restoredSnapshotTLSConfig{
		caFile: identity.CertFile, certFile: identity.CertFile, keyFile: identity.KeyFile,
		serverName: "restored-auth-tls.example",
	}
	dir := t.TempDir()
	partial, err := consumeAndValidateSnapshotWithClusterAuth(t.Context(), snapshotAuthReceiver(t, state), dir, expected,
		tlsCfg, &fixture.expected, 3)
	require.NoError(t, err)
	require.True(t, partial)
	entries, readErr := os.ReadDir(dir)
	require.NoError(t, readErr)
	require.Empty(t, entries)
}

func TestConsumeAndValidateSnapshotValidatesRestoredAuthPermissionMatrix(t *testing.T) {
	const prefix = "/probe/auth-matrix/"
	fixture := newSnapshotAuthFixture(prefix)
	fixture.expected.revision = 23
	expected := newStreamProbeExpectations(prefix)
	state := snapshotAuthTestState(t, expected, &fixture.expected)
	dir := t.TempDir()

	partial, err := consumeAndValidateSnapshotWithClusterAuth(t.Context(), snapshotAuthReceiver(t, state), dir, expected,
		restoredSnapshotTLSConfig{}, &fixture.expected, 3)
	require.NoError(t, err)
	require.True(t, partial)
	entries, readErr := os.ReadDir(dir)
	require.NoError(t, readErr)
	require.Empty(t, entries, "a validated auth matrix artifact must always be removed")

	for name, testCase := range map[string]struct {
		state    etcdsnapshot.State
		expected *restoredSnapshotAuthExpectation
		message  string
	}{
		"auth revision mismatch": {
			state: state,
			expected: func() *restoredSnapshotAuthExpectation {
				copy := cloneRestoredSnapshotAuthExpectation(&fixture.expected)
				copy.revision++
				return copy
			}(),
			message: "restored auth status mismatch",
		},
		"missing user": {
			state: func() etcdsnapshot.State {
				copy := state
				copy.Auth.Users = append([]*authpb.User(nil), state.Auth.Users[1:]...)
				return copy
			}(),
			expected: &fixture.expected, message: "is missing",
		},
		"wrong permission": {
			state: func() etcdsnapshot.State {
				copy := state
				copy.Auth.Roles = cloneAuthRoles(state.Auth.Roles)
				copy.Auth.Roles[0].KeyPermission[0].PermType = authpb.READWRITE
				return copy
			}(),
			expected: &fixture.expected, message: "permission mismatch",
		},
		"wrong password hash": {
			state: func() etcdsnapshot.State {
				copy := state
				copy.Auth.Users = cloneAuthUsers(state.Auth.Users)
				hash, hashErr := bcrypt.GenerateFromPassword([]byte("wrong-password"), bcrypt.MinCost)
				require.NoError(t, hashErr)
				copy.Auth.Users[0].Password = hash
				return copy
			}(),
			expected: &fixture.expected, message: "authenticate restored user",
		},
		"lost no-password option": {
			state: func() etcdsnapshot.State {
				copy := state
				copy.Auth.Users = cloneAuthUsers(state.Auth.Users)
				copy.Auth.Users[len(copy.Auth.Users)-1].Options.NoPassword = false
				return copy
			}(),
			expected: &fixture.expected, message: "authenticate restored no-password user",
		},
		"wrong access expectation": {
			state: state,
			expected: func() *restoredSnapshotAuthExpectation {
				copy := cloneRestoredSnapshotAuthExpectation(&fixture.expected)
				copy.access[0].write = true
				return copy
			}(),
			message: "restored auth Put was denied",
		},
	} {
		t.Run(name, func(t *testing.T) {
			partial, err := consumeAndValidateSnapshotWithAuth(t.Context(), snapshotAuthReceiver(t, testCase.state), dir,
				expected, restoredSnapshotTLSConfig{}, testCase.expected)
			require.ErrorContains(t, err, testCase.message)
			require.True(t, partial)
			entries, readErr := os.ReadDir(dir)
			require.NoError(t, readErr)
			require.Empty(t, entries, "a rejected auth matrix artifact must always be removed")
		})
	}
}

func snapshotAuthTestState(t *testing.T, expected []streamProbeExpectation,
	auth *restoredSnapshotAuthExpectation,
) etcdsnapshot.State {
	t.Helper()
	records := make([]etcdsnapshot.Record, 0, len(expected))
	for index := range expected {
		revision := int64(index + 2)
		expected[index].revision = revision
		records = append(records, etcdsnapshot.Record{
			Key: []byte(expected[index].key), Value: []byte(expected[index].value),
			CreateRevision: revision, ModRevision: revision, Version: 1,
		})
	}
	users := make([]*authpb.User, 0, len(auth.users))
	for _, expectedUser := range auth.users {
		var hash []byte
		if !expectedUser.noPassword {
			var err error
			hash, err = bcrypt.GenerateFromPassword([]byte(expectedUser.password), bcrypt.MinCost)
			require.NoError(t, err)
		}
		users = append(users, &authpb.User{
			Name: []byte(expectedUser.name), Password: hash, Roles: append([]string(nil), expectedUser.roles...),
			Options: &authpb.UserAddOptions{NoPassword: expectedUser.noPassword},
		})
	}
	roles := make([]*authpb.Role, 0, len(auth.roles))
	for _, expectedRole := range auth.roles {
		role := &authpb.Role{Name: []byte(expectedRole.name)}
		for _, permission := range expectedRole.permissions {
			role.KeyPermission = append(role.KeyPermission, proto.Clone(permission).(*authpb.Permission))
		}
		roles = append(roles, role)
	}
	return etcdsnapshot.State{
		Revision: 30, PreserveHistory: true, Records: records,
		Auth: etcdsnapshot.Auth{Revision: auth.revision, Users: users, Roles: roles},
	}
}

func snapshotAuthReceiver(t *testing.T, state etcdsnapshot.State) *fakeSnapshotReceiver {
	t.Helper()
	path := filepath.Join(t.TempDir(), "source.db")
	require.NoError(t, etcdsnapshot.WriteBackend(path, state))
	data, err := os.ReadFile(path)
	require.NoError(t, err)
	digest := sha256.Sum256(data)
	return &fakeSnapshotReceiver{steps: []snapshotReceiveStep{
		{response: snapshotResponse(data, 0, etcdsnapshot.StorageVersion)},
		{response: snapshotResponse(digest[:], 0, etcdsnapshot.StorageVersion)},
	}}
}

func snapshotResponse(blob []byte, remaining uint64, version string) *etcdserverpb.SnapshotResponse {
	return &etcdserverpb.SnapshotResponse{Blob: blob, RemainingBytes: remaining, Version: version}
}

func cloneRestoredSnapshotAuthExpectation(source *restoredSnapshotAuthExpectation) *restoredSnapshotAuthExpectation {
	copy := &restoredSnapshotAuthExpectation{
		revision: source.revision, enabled: source.enabled, adminUsername: source.adminUsername,
		adminPassword: source.adminPassword, rootKey: source.rootKey,
	}
	for _, user := range source.users {
		user.roles = append([]string(nil), user.roles...)
		copy.users = append(copy.users, user)
	}
	for _, role := range source.roles {
		cloned := restoredSnapshotAuthRoleExpectation{name: role.name}
		for _, permission := range role.permissions {
			cloned.permissions = append(cloned.permissions, proto.Clone(permission).(*authpb.Permission))
		}
		copy.roles = append(copy.roles, cloned)
	}
	copy.access = append([]restoredSnapshotAuthAccessExpectation(nil), source.access...)
	return copy
}

func cloneAuthUsers(source []*authpb.User) []*authpb.User {
	cloned := make([]*authpb.User, 0, len(source))
	for _, user := range source {
		cloned = append(cloned, proto.Clone(user).(*authpb.User))
	}
	return cloned
}

func cloneAuthRoles(source []*authpb.Role) []*authpb.Role {
	cloned := make([]*authpb.Role, 0, len(source))
	for _, role := range source {
		cloned = append(cloned, proto.Clone(role).(*authpb.Role))
	}
	return cloned
}
