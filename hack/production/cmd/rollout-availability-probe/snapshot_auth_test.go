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
	"go.etcd.io/etcd/api/v3/mvccpb"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	"go.etcd.io/etcd/client/pkg/v3/transport"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/server/v3/embed"
	"go.uber.org/zap"
	"golang.org/x/crypto/bcrypt"
	"google.golang.org/protobuf/proto"

	"github.com/kubewharf/kubebrain/pkg/etcdsnapshot"
)

func TestRestoredAuthWatchEvidence(t *testing.T) {
	response := clientv3.WatchResponse{
		Header:   &etcdserverpb.ResponseHeader{ClusterId: 11, MemberId: 12, Revision: 13, RaftTerm: 14},
		Canceled: true, CancelReason: "secret-token-must-not-be-added",
		Events: []*clientv3.Event{{Kv: &mvccpb.KeyValue{Value: []byte("private-value")}}},
	}
	require.Equal(t, "header_present=true cluster_id=11 member_id=12 revision=13 raft_term=14 created=false canceled=true compact_revision=0 events=1 context_done=false",
		restoredAuthWatchEvidence(response, nil))
	require.Equal(t, "header_present=false cluster_id=0 member_id=0 revision=0 raft_term=0 created=false canceled=false compact_revision=0 events=0 context_done=true",
		restoredAuthWatchEvidence(clientv3.WatchResponse{}, context.Canceled))
	response.Created, response.Canceled, response.CompactRevision = true, false, 9
	require.Contains(t, restoredAuthWatchEvidence(response, context.DeadlineExceeded),
		"created=true canceled=false compact_revision=9 events=1 context_done=true")
}

func TestSnapshotAuthFixtureInstallCleanupAndCollisionOwnership(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
	defer cancel()
	clientURL, peerURL, reservation, err := reserveRestoredSnapshotURLs(false)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, reservation.close()) })
	cfg := embed.NewConfig()
	cfg.Name = "snapshot-auth-source"
	cfg.Dir = t.TempDir()
	// This fixture tests auth ownership and cleanup, not password-hardening
	// cost. Keep real hashing/authentication without spending the shared
	// deadline on repeated default-cost hashes under the race detector.
	cfg.BcryptCost = uint(bcrypt.MinCost)
	cfg.ListenClientUrls = []url.URL{clientURL}
	cfg.AdvertiseClientUrls = []url.URL{clientURL}
	cfg.ListenPeerUrls = []url.URL{peerURL}
	cfg.AdvertisePeerUrls = []url.URL{peerURL}
	cfg.InitialCluster = cfg.Name + "=" + peerURL.String()
	cfg.ZapLoggerBuilder = embed.NewZapLoggerBuilder(zap.NewNop())
	require.NoError(t, reservation.close())
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
	_, err = enabled.cleanup(rootClient, initial.Header.ClusterId, lastRevision, 3*time.Second)
	require.NoError(t, err)
	statusResponse, err := rootClient.AuthStatus(ctx)
	require.NoError(t, err)
	require.True(t, statusResponse.Enabled)
}

func TestRestoredAuthLeaseAttachmentReadyRequiresExactReplicatedLease(t *testing.T) {
	const (
		key      = "/probe/leased"
		value    = "value"
		revision = int64(19)
		leaseID  = clientv3.LeaseID(23)
	)
	valid := &clientv3.GetResponse{Header: &etcdserverpb.ResponseHeader{Revision: revision}, Count: 1,
		Kvs: []*mvccpb.KeyValue{{Key: []byte(key), Value: []byte(value), ModRevision: revision, Lease: int64(leaseID)}}}
	require.True(t, restoredAuthLeaseAttachmentReady(valid, key, value, leaseID, revision))

	for name, mutate := range map[string]func(*clientv3.GetResponse){
		"nil header":     func(response *clientv3.GetResponse) { response.Header = nil },
		"stale revision": func(response *clientv3.GetResponse) { response.Header.Revision-- },
		"more":           func(response *clientv3.GetResponse) { response.More = true },
		"wrong count":    func(response *clientv3.GetResponse) { response.Count = 2 },
		"wrong key":      func(response *clientv3.GetResponse) { response.Kvs[0].Key = []byte("wrong") },
		"wrong value":    func(response *clientv3.GetResponse) { response.Kvs[0].Value = []byte("wrong") },
		"wrong mod":      func(response *clientv3.GetResponse) { response.Kvs[0].ModRevision-- },
		"wrong lease":    func(response *clientv3.GetResponse) { response.Kvs[0].Lease++ },
	} {
		t.Run(name, func(t *testing.T) {
			candidate := clientv3.GetResponse{
				Header: proto.Clone(valid.Header).(*etcdserverpb.ResponseHeader), Count: valid.Count, More: valid.More,
				Kvs: []*mvccpb.KeyValue{proto.Clone(valid.Kvs[0]).(*mvccpb.KeyValue)},
			}
			mutate(&candidate)
			require.False(t, restoredAuthLeaseAttachmentReady(&candidate, key, value, leaseID, revision))
		})
	}
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

func TestConsumeAndValidateSnapshotVerifiesScaleDatasetAfterOfficialRestore(t *testing.T) {
	const prefix = "/probe/snapshot-scale/"
	expected := newStreamProbeExpectations(prefix)[:1]
	expected[0].revision = 2
	scale, err := newSnapshotScaleExpectation(prefix, snapshotScaleKeys, snapshotScaleValueBytes)
	require.NoError(t, err)
	records := []etcdsnapshot.Record{{
		Key: []byte(expected[0].key), Value: []byte(expected[0].value),
		CreateRevision: expected[0].revision, ModRevision: expected[0].revision, Version: 1,
	}}
	for index := range scale.items {
		revision := int64(index + 3)
		scale.items[index].revision = revision
		records = append(records, etcdsnapshot.Record{
			Key: []byte(scale.items[index].key), Value: snapshotScaleValue(scale.items[index].key, scale.valueBytes),
			CreateRevision: revision, ModRevision: revision, Version: 1,
		})
	}
	expected[0].snapshotScale = scale
	state := etcdsnapshot.State{Revision: int64(len(scale.items) + 10), PreserveHistory: true, Records: records}
	dir := t.TempDir()

	partial, err := consumeAndValidateSnapshot(t.Context(), snapshotAuthReceiver(t, state), dir, expected,
		restoredSnapshotTLSConfig{})
	require.NoError(t, err)
	require.True(t, partial)
	entries, readErr := os.ReadDir(dir)
	require.NoError(t, readErr)
	require.Empty(t, entries)

	scale.items[17].hash[0] ^= 0xff
	partial, err = consumeAndValidateSnapshot(t.Context(), snapshotAuthReceiver(t, state), dir, expected,
		restoredSnapshotTLSConfig{})
	require.ErrorContains(t, err, "invalid Snapshot scale item 17")
	require.True(t, partial)
	entries, readErr = os.ReadDir(dir)
	require.NoError(t, readErr)
	require.Empty(t, entries, "a rejected scale artifact must always be removed")
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
	state = attachProductionSnapshotScale(t, prefix, expected, state)
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

func attachProductionSnapshotScale(t *testing.T, prefix string, expected []streamProbeExpectation,
	state etcdsnapshot.State,
) etcdsnapshot.State {
	t.Helper()
	require.NotEmpty(t, expected)
	scale, err := newSnapshotScaleExpectation(prefix, snapshotScaleKeys, snapshotScaleValueBytes)
	require.NoError(t, err)
	nextRevision := state.Revision + 1
	for index := range scale.items {
		scale.items[index].revision = nextRevision
		state.Records = append(state.Records, etcdsnapshot.Record{
			Key: []byte(scale.items[index].key), Value: snapshotScaleValue(scale.items[index].key, scale.valueBytes),
			CreateRevision: nextRevision, ModRevision: nextRevision, Version: 1,
		})
		nextRevision++
	}
	state.Revision = nextRevision
	expected[0].snapshotScale = scale
	return state
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

func TestRestoredAuthWatchCloseErrorPreservesContextCause(t *testing.T) {
	for name, testCase := range map[string]struct {
		ctx       func() context.Context
		wantCause error
	}{
		"live context": {
			ctx: func() context.Context { return t.Context() },
		},
		"canceled context": {
			ctx: func() context.Context {
				ctx, cancel := context.WithCancel(t.Context())
				cancel()
				return ctx
			},
			wantCause: context.Canceled,
		},
		"deadline context": {
			ctx: func() context.Context {
				ctx, cancel := context.WithDeadline(t.Context(), time.Now().Add(-time.Second))
				t.Cleanup(cancel)
				return ctx
			},
			wantCause: context.DeadlineExceeded,
		},
	} {
		t.Run(name, func(t *testing.T) {
			err := restoredAuthWatchCloseError(testCase.ctx(), "probe-user", "/probe/key")
			require.ErrorContains(t, err, `user="probe-user" key="/probe/key"`)
			if testCase.wantCause == nil {
				require.NotErrorIs(t, err, context.Canceled)
				require.NotErrorIs(t, err, context.DeadlineExceeded)
				return
			}
			require.ErrorIs(t, err, testCase.wantCause)
			require.True(t, retryableStreamError(err))
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
		forbiddenKeys: append([]string(nil), source.forbiddenKeys...),
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
