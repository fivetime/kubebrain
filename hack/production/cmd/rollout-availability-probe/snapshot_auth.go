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
	"errors"
	"fmt"
	"slices"
	"sort"
	"time"

	"go.etcd.io/etcd/api/v3/authpb"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

type restoredSnapshotAuthUserExpectation struct {
	name       string
	password   string
	roles      []string
	noPassword bool
}

type restoredSnapshotAuthRoleExpectation struct {
	name        string
	permissions []*authpb.Permission
}

type restoredSnapshotAuthAccessExpectation struct {
	username string
	key      string
	read     bool
	write    bool
}

type restoredSnapshotAuthExpectation struct {
	revision      uint64
	enabled       bool
	adminUsername string
	adminPassword string
	users         []restoredSnapshotAuthUserExpectation
	roles         []restoredSnapshotAuthRoleExpectation
	access        []restoredSnapshotAuthAccessExpectation
	rootKey       string
	forbiddenKeys []string
}

type snapshotAuthFixture struct {
	expected     restoredSnapshotAuthExpectation
	createdUsers []string
	createdRoles []string
}

type restoredSnapshotLeaseReplicationBarrier func(context.Context, string, string, clientv3.LeaseID, int64) error

func restoredAuthLeaseAttachmentReady(response *clientv3.GetResponse, key, value string, leaseID clientv3.LeaseID,
	minimumRevision int64,
) bool {
	if response == nil || response.Header == nil || response.Header.Revision < minimumRevision || response.More ||
		response.Count != 1 || len(response.Kvs) != 1 {
		return false
	}
	kv := response.Kvs[0]
	return kv != nil && string(kv.Key) == key && string(kv.Value) == value && kv.Lease == int64(leaseID) &&
		kv.ModRevision == minimumRevision
}

func waitForRestoredAuthLeaseReplication(ctx context.Context, adminConfig clientv3.Config, key, value string,
	leaseID clientv3.LeaseID, minimumRevision int64,
) error {
	if len(adminConfig.Endpoints) == 0 || key == "" || leaseID == 0 || minimumRevision <= 0 {
		return errors.New("restored auth lease replication barrier requires endpoints, key, lease, and revision")
	}
	for _, endpoint := range adminConfig.Endpoints {
		direct, err := newDirectRestoredClient(adminConfig, endpoint)
		if err != nil {
			return fmt.Errorf("create direct restored auth replication client for %q: %w", endpoint, err)
		}
		var lastResponse *clientv3.GetResponse
		var lastErr error
		for {
			lastResponse, lastErr = direct.Get(ctx, key, clientv3.WithSerializable())
			if lastErr == nil && restoredAuthLeaseAttachmentReady(lastResponse, key, value, leaseID, minimumRevision) {
				break
			}
			select {
			case <-time.After(20 * time.Millisecond):
			case <-ctx.Done():
				_ = direct.Close()
				return fmt.Errorf("wait for restored auth lease replication on %q: response=%+v err=%v: %w",
					endpoint, lastResponse, lastErr, context.Cause(ctx))
			}
		}
		if err := direct.Close(); err != nil {
			return fmt.Errorf("close direct restored auth replication client for %q: %w", endpoint, err)
		}
	}
	return nil
}

func newSnapshotAuthFixture(prefix string) *snapshotAuthFixture {
	digest := sha256.Sum256([]byte(prefix))
	base := fmt.Sprintf("kubebrain-rollout-%x", digest[:6])
	sharedPrefix := prefix + "stream/000"
	sharedEnd := clientv3.GetPrefixRangeEnd(sharedPrefix)
	exactKey := prefix + "stream/0010"
	outsideKey := prefix + "stream/0011"

	readRole := base + "-read"
	writeRole := base + "-write"
	readWriteRole := base + "-readwrite"
	exactReadRole := base + "-exact-read"
	roles := []restoredSnapshotAuthRoleExpectation{
		{name: readRole, permissions: []*authpb.Permission{{PermType: authpb.READ, Key: []byte(sharedPrefix), RangeEnd: []byte(sharedEnd)}}},
		{name: writeRole, permissions: []*authpb.Permission{{PermType: authpb.WRITE, Key: []byte(sharedPrefix), RangeEnd: []byte(sharedEnd)}}},
		{name: readWriteRole, permissions: []*authpb.Permission{{PermType: authpb.READWRITE, Key: []byte(sharedPrefix), RangeEnd: []byte(sharedEnd)}}},
		{name: exactReadRole, permissions: []*authpb.Permission{{PermType: authpb.READ, Key: []byte(exactKey)}}},
	}
	unionRoles := []string{readRole, writeRole}
	sort.Strings(unionRoles)
	users := []restoredSnapshotAuthUserExpectation{
		{name: base + "-reader", password: base + "-reader-secret", roles: []string{readRole}},
		{name: base + "-writer", password: base + "-writer-secret", roles: []string{writeRole}},
		{name: base + "-readwriter", password: base + "-readwriter-secret", roles: []string{readWriteRole}},
		{name: base + "-union", password: base + "-union-secret", roles: unionRoles},
		{name: base + "-exact-reader", password: base + "-exact-reader-secret", roles: []string{exactReadRole}},
		{name: base + "-no-password", roles: []string{readRole}, noPassword: true},
	}
	return &snapshotAuthFixture{expected: restoredSnapshotAuthExpectation{
		users: users,
		roles: roles,
		access: []restoredSnapshotAuthAccessExpectation{
			{username: users[0].name, key: prefix + "stream/0006", read: true},
			{username: users[0].name, key: outsideKey},
			{username: users[1].name, key: prefix + "stream/0007", write: true},
			{username: users[2].name, key: prefix + "stream/0008", read: true, write: true},
			{username: users[3].name, key: prefix + "stream/0009", read: true, write: true},
			{username: users[4].name, key: exactKey, read: true},
			{username: users[4].name, key: outsideKey},
		},
		rootKey:       prefix + "stream/root-admin",
		forbiddenKeys: []string{legacyFixtureMarkerKey(prefix)},
	}}
}

func (fixture *snapshotAuthFixture) install(ctx context.Context, client *clientv3.Client, clusterID uint64,
	lastRevision int64, commandTimeout time.Duration,
) (int64, error) {
	if fixture == nil {
		return lastRevision, errors.New("etcd Snapshot auth fixture is nil")
	}
	validateHeader := func(label string, header *etcdserverpb.ResponseHeader) error {
		_, revision, err := validateResponseHeader(header, clusterID, lastRevision)
		if err != nil {
			return fmt.Errorf("%s returned an invalid header: %w", label, err)
		}
		lastRevision = revision
		return nil
	}
	call := func(label string, operation func(context.Context) (*etcdserverpb.ResponseHeader, error)) error {
		opCtx, cancel := context.WithTimeout(ctx, commandTimeout)
		header, err := operation(opCtx)
		cancel()
		if err != nil {
			return fmt.Errorf("%s: %w", label, err)
		}
		return validateHeader(label, header)
	}

	opCtx, cancel := context.WithTimeout(ctx, commandTimeout)
	statusResponse, err := client.AuthStatus(opCtx)
	cancel()
	if err != nil {
		return lastRevision, fmt.Errorf("read auth status before Snapshot fixture: %w", err)
	}
	if statusResponse == nil {
		return lastRevision, errors.New("read auth status before Snapshot fixture returned a nil response")
	}
	if err := validateHeader("read auth status before Snapshot fixture", statusResponse.Header); err != nil {
		return lastRevision, err
	}
	fixture.expected.enabled = statusResponse.Enabled

	for _, role := range fixture.expected.roles {
		role := role
		if err := call("add Snapshot auth role "+role.name, func(opCtx context.Context) (*etcdserverpb.ResponseHeader, error) {
			response, err := client.RoleAdd(opCtx, role.name)
			if response == nil {
				return nil, err
			}
			return response.Header, err
		}); err != nil {
			return lastRevision, err
		}
		fixture.createdRoles = append(fixture.createdRoles, role.name)
		for _, permission := range role.permissions {
			permission := permission
			if err := call("grant Snapshot auth permission to "+role.name, func(opCtx context.Context) (*etcdserverpb.ResponseHeader, error) {
				response, err := client.RoleGrantPermission(opCtx, role.name, string(permission.Key), string(permission.RangeEnd),
					clientv3.PermissionType(permission.PermType))
				if response == nil {
					return nil, err
				}
				return response.Header, err
			}); err != nil {
				return lastRevision, err
			}
		}
	}
	for _, user := range fixture.expected.users {
		user := user
		if err := call("add Snapshot auth user "+user.name, func(opCtx context.Context) (*etcdserverpb.ResponseHeader, error) {
			var response *clientv3.AuthUserAddResponse
			var err error
			if user.noPassword {
				response, err = client.UserAddWithOptions(opCtx, user.name, "", &clientv3.UserAddOptions{NoPassword: true})
			} else {
				response, err = client.UserAdd(opCtx, user.name, user.password)
			}
			if response == nil {
				return nil, err
			}
			return response.Header, err
		}); err != nil {
			return lastRevision, err
		}
		fixture.createdUsers = append(fixture.createdUsers, user.name)
		for _, role := range user.roles {
			role := role
			if err := call("grant Snapshot auth role to "+user.name, func(opCtx context.Context) (*etcdserverpb.ResponseHeader, error) {
				response, err := client.UserGrantRole(opCtx, user.name, role)
				if response == nil {
					return nil, err
				}
				return response.Header, err
			}); err != nil {
				return lastRevision, err
			}
		}
	}
	opCtx, cancel = context.WithTimeout(ctx, commandTimeout)
	statusResponse, err = client.AuthStatus(opCtx)
	cancel()
	if err != nil {
		return lastRevision, fmt.Errorf("read installed Snapshot auth status: %w", err)
	}
	if statusResponse == nil {
		return lastRevision, errors.New("read installed Snapshot auth status returned a nil response")
	}
	if err := validateHeader("read installed Snapshot auth status", statusResponse.Header); err != nil {
		return lastRevision, err
	}
	if statusResponse.Enabled != fixture.expected.enabled || statusResponse.AuthRevision == 0 || statusResponse.AuthRevision <= fixture.expected.revision {
		return lastRevision, fmt.Errorf("installed Snapshot auth fixture returned invalid status: enabled=%t revision=%d previous=%d",
			statusResponse.Enabled, statusResponse.AuthRevision, fixture.expected.revision)
	}
	fixture.expected.revision = statusResponse.AuthRevision
	return lastRevision, nil
}

func (fixture *snapshotAuthFixture) cleanup(client *clientv3.Client, clusterID uint64, lastRevision int64,
	commandTimeout time.Duration,
) (int64, error) {
	if fixture == nil {
		return lastRevision, nil
	}
	var cleanupErr error
	call := func(label string, operation func(context.Context) (*etcdserverpb.ResponseHeader, error)) {
		opCtx, cancel := context.WithTimeout(context.Background(), max(5*time.Second, commandTimeout))
		header, err := operation(opCtx)
		cancel()
		if err == nil {
			_, revision, validateErr := validateResponseHeader(header, clusterID, lastRevision)
			if validateErr != nil {
				err = validateErr
			} else {
				lastRevision = revision
			}
		}
		if err != nil {
			cleanupErr = errors.Join(cleanupErr, fmt.Errorf("%s: %w", label, err))
		}
	}
	for index := len(fixture.createdUsers) - 1; index >= 0; index-- {
		name := fixture.createdUsers[index]
		call("delete Snapshot auth user "+name, func(opCtx context.Context) (*etcdserverpb.ResponseHeader, error) {
			response, err := client.UserDelete(opCtx, name)
			if response == nil {
				return nil, err
			}
			return response.Header, err
		})
	}
	for index := len(fixture.createdRoles) - 1; index >= 0; index-- {
		name := fixture.createdRoles[index]
		call("delete Snapshot auth role "+name, func(opCtx context.Context) (*etcdserverpb.ResponseHeader, error) {
			response, err := client.RoleDelete(opCtx, name)
			if response == nil {
				return nil, err
			}
			return response.Header, err
		})
	}
	opCtx, cancel := context.WithTimeout(context.Background(), max(5*time.Second, commandTimeout))
	usersResponse, err := client.UserList(opCtx)
	cancel()
	if err == nil {
		if usersResponse == nil {
			err = errors.New("etcd Snapshot auth user cleanup returned a nil response")
		}
	}
	if err == nil {
		_, revision, validateErr := validateResponseHeader(usersResponse.Header, clusterID, lastRevision)
		if validateErr != nil {
			err = validateErr
		} else {
			lastRevision = revision
			created := make(map[string]struct{}, len(fixture.createdUsers))
			for _, name := range fixture.createdUsers {
				created[name] = struct{}{}
			}
			for _, name := range usersResponse.Users {
				if _, leaked := created[name]; leaked {
					err = fmt.Errorf("etcd Snapshot auth user %q remains after cleanup", name)
					break
				}
			}
		}
	}
	if err != nil {
		cleanupErr = errors.Join(cleanupErr, fmt.Errorf("verify Snapshot auth user cleanup: %w", err))
	}
	opCtx, cancel = context.WithTimeout(context.Background(), max(5*time.Second, commandTimeout))
	rolesResponse, err := client.RoleList(opCtx)
	cancel()
	if err == nil {
		if rolesResponse == nil {
			err = errors.New("etcd Snapshot auth role cleanup returned a nil response")
		}
	}
	if err == nil {
		_, revision, validateErr := validateResponseHeader(rolesResponse.Header, clusterID, lastRevision)
		if validateErr != nil {
			err = validateErr
		} else {
			lastRevision = revision
			created := make(map[string]struct{}, len(fixture.createdRoles))
			for _, name := range fixture.createdRoles {
				created[name] = struct{}{}
			}
			for _, name := range rolesResponse.Roles {
				if _, leaked := created[name]; leaked {
					err = fmt.Errorf("etcd Snapshot auth role %q remains after cleanup", name)
					break
				}
			}
		}
	}
	if err != nil {
		cleanupErr = errors.Join(cleanupErr, fmt.Errorf("verify Snapshot auth role cleanup: %w", err))
	}
	opCtx, cancel = context.WithTimeout(context.Background(), max(5*time.Second, commandTimeout))
	statusResponse, err := client.AuthStatus(opCtx)
	cancel()
	if err == nil {
		if statusResponse == nil {
			err = errors.New("etcd Snapshot auth status cleanup returned a nil response")
		}
	}
	if err == nil {
		_, revision, validateErr := validateResponseHeader(statusResponse.Header, clusterID, lastRevision)
		if validateErr != nil {
			err = validateErr
		} else {
			lastRevision = revision
			if statusResponse.Enabled != fixture.expected.enabled {
				err = fmt.Errorf("source authentication state changed during Snapshot auth fixture cleanup: got=%t want=%t",
					statusResponse.Enabled, fixture.expected.enabled)
			}
		}
	}
	if err != nil {
		cleanupErr = errors.Join(cleanupErr, fmt.Errorf("verify Snapshot auth status after cleanup: %w", err))
	}
	return lastRevision, cleanupErr
}

func (fixture *snapshotAuthFixture) inspectOwnedState(ctx context.Context, client *clientv3.Client) ([]string, []string, error) {
	if fixture == nil || client == nil {
		return nil, nil, errors.New("etcd Snapshot auth ownership inspection requires fixture and client")
	}
	expectedUsers := make(map[string]restoredSnapshotAuthUserExpectation, len(fixture.expected.users))
	for _, user := range fixture.expected.users {
		expectedUsers[user.name] = user
	}
	expectedRoles := make(map[string]restoredSnapshotAuthRoleExpectation, len(fixture.expected.roles))
	for _, role := range fixture.expected.roles {
		expectedRoles[role.name] = role
	}

	usersResponse, err := client.UserList(ctx)
	if err != nil {
		return nil, nil, fmt.Errorf("list users for Snapshot auth ownership: %w", err)
	}
	if usersResponse == nil || usersResponse.Header == nil {
		return nil, nil, errors.New("list users for Snapshot auth ownership returned an empty response")
	}
	seenUsers := make(map[string]struct{}, len(usersResponse.Users))
	presentUsers := make([]string, 0, len(expectedUsers))
	for _, name := range usersResponse.Users {
		if _, duplicate := seenUsers[name]; duplicate {
			return nil, nil, fmt.Errorf("user list contains duplicate identity %q", name)
		}
		seenUsers[name] = struct{}{}
		response, getErr := client.UserGet(ctx, name)
		if getErr != nil {
			return nil, nil, fmt.Errorf("get user %q for Snapshot auth ownership: %w", name, getErr)
		}
		if response == nil || response.Header == nil {
			return nil, nil, fmt.Errorf("get user %q for Snapshot auth ownership returned an empty response", name)
		}
		roles := append([]string(nil), response.Roles...)
		sort.Strings(roles)
		if expected, owned := expectedUsers[name]; owned {
			wanted := append([]string(nil), expected.roles...)
			sort.Strings(wanted)
			if !slices.Equal(roles, wanted) {
				return nil, nil, fmt.Errorf("owned Snapshot auth user %q roles drifted: got=%v want=%v", name, roles, wanted)
			}
			presentUsers = append(presentUsers, name)
			continue
		}
		for _, role := range roles {
			if _, collision := expectedRoles[role]; collision {
				return nil, nil, fmt.Errorf("unowned user %q references owned Snapshot auth role %q", name, role)
			}
		}
	}

	rolesResponse, err := client.RoleList(ctx)
	if err != nil {
		return nil, nil, fmt.Errorf("list roles for Snapshot auth ownership: %w", err)
	}
	if rolesResponse == nil || rolesResponse.Header == nil {
		return nil, nil, errors.New("list roles for Snapshot auth ownership returned an empty response")
	}
	seenRoles := make(map[string]struct{}, len(rolesResponse.Roles))
	presentRoles := make([]string, 0, len(expectedRoles))
	for _, name := range rolesResponse.Roles {
		if _, duplicate := seenRoles[name]; duplicate {
			return nil, nil, fmt.Errorf("role list contains duplicate identity %q", name)
		}
		seenRoles[name] = struct{}{}
		expected, owned := expectedRoles[name]
		if !owned {
			continue
		}
		response, getErr := client.RoleGet(ctx, name)
		if getErr != nil {
			return nil, nil, fmt.Errorf("get role %q for Snapshot auth ownership: %w", name, getErr)
		}
		if response == nil || response.Header == nil || len(response.Perm) != len(expected.permissions) {
			return nil, nil, fmt.Errorf("owned Snapshot auth role %q permissions drifted", name)
		}
		for index := range response.Perm {
			if !proto.Equal(response.Perm[index], expected.permissions[index]) {
				return nil, nil, fmt.Errorf("owned Snapshot auth role %q permissions drifted at index %d: got=%+v want=%+v",
					name, index, response.Perm[index], expected.permissions[index])
			}
		}
		presentRoles = append(presentRoles, name)
	}
	presentRoleSet := make(map[string]struct{}, len(presentRoles))
	for _, name := range presentRoles {
		presentRoleSet[name] = struct{}{}
	}
	for _, name := range presentUsers {
		for _, role := range expectedUsers[name].roles {
			if _, present := presentRoleSet[role]; !present {
				return nil, nil, fmt.Errorf("owned Snapshot auth user %q references missing role %q", name, role)
			}
		}
	}
	sort.Strings(presentUsers)
	sort.Strings(presentRoles)
	return presentUsers, presentRoles, nil
}

func (fixture *snapshotAuthFixture) deleteOwnedState(ctx context.Context, client *clientv3.Client,
	presentUsers, presentRoles []string,
) error {
	if fixture == nil || client == nil {
		return errors.New("etcd Snapshot auth ownership deletion requires fixture and client")
	}
	userSet := make(map[string]struct{}, len(presentUsers))
	for _, name := range presentUsers {
		userSet[name] = struct{}{}
	}
	roleSet := make(map[string]struct{}, len(presentRoles))
	for _, name := range presentRoles {
		roleSet[name] = struct{}{}
	}
	for index := len(fixture.expected.users) - 1; index >= 0; index-- {
		name := fixture.expected.users[index].name
		if _, present := userSet[name]; !present {
			continue
		}
		response, err := client.UserDelete(ctx, name)
		if err != nil && !errors.Is(err, rpctypes.ErrUserNotFound) {
			return fmt.Errorf("delete owned Snapshot auth user %q: %w", name, err)
		}
		if err == nil && (response == nil || response.Header == nil) {
			return fmt.Errorf("delete owned Snapshot auth user %q returned an empty response", name)
		}
	}
	for index := len(fixture.expected.roles) - 1; index >= 0; index-- {
		name := fixture.expected.roles[index].name
		if _, present := roleSet[name]; !present {
			continue
		}
		response, err := client.RoleDelete(ctx, name)
		if err != nil && !errors.Is(err, rpctypes.ErrRoleNotFound) {
			return fmt.Errorf("delete owned Snapshot auth role %q: %w", name, err)
		}
		if err == nil && (response == nil || response.Header == nil) {
			return fmt.Errorf("delete owned Snapshot auth role %q returned an empty response", name)
		}
	}
	return nil
}

type restoredSnapshotAdminVerifier func(*clientv3.Client, clientv3.Config) error

func verifyRestoredSnapshotAuthWithAdmin(ctx context.Context, client *clientv3.Client, clientConfig, passwordClientConfig clientv3.Config,
	expected *restoredSnapshotAuthExpectation, revision int64, verifyAdmin restoredSnapshotAdminVerifier,
) (retErr error) {
	if expected == nil {
		if verifyAdmin == nil {
			return nil
		}
		return verifyAdmin(client, clientConfig)
	}
	if expected.revision == 0 || len(expected.users) == 0 || len(expected.roles) == 0 || len(expected.access) == 0 || expected.rootKey == "" {
		return errors.New("restored auth expectation requires a positive revision and non-empty users, roles, access matrix, and root key")
	}
	validateHeader := func(label string, header *etcdserverpb.ResponseHeader) error {
		if header == nil || header.ClusterId == 0 || header.MemberId == 0 || header.RaftTerm == 0 || header.Revision != revision {
			return fmt.Errorf("%s returned invalid restored identity: header=%+v snapshot_revision=%d", label, header, revision)
		}
		return nil
	}
	statusResponse, err := client.AuthStatus(ctx)
	if err != nil {
		return fmt.Errorf("read restored auth status: %w", err)
	}
	if statusResponse == nil {
		return errors.New("read restored auth status returned a nil response")
	}
	if err := validateHeader("restored auth status", statusResponse.Header); err != nil {
		return err
	}
	if statusResponse.Enabled != expected.enabled || statusResponse.AuthRevision != expected.revision {
		return fmt.Errorf("restored auth status mismatch: enabled=%t revision=%d want_enabled=%t want_revision=%d",
			statusResponse.Enabled, statusResponse.AuthRevision, expected.enabled, expected.revision)
	}
	usersResponse, err := client.UserList(ctx)
	if err != nil {
		return fmt.Errorf("list restored auth users: %w", err)
	}
	if usersResponse == nil {
		return errors.New("list restored auth users returned a nil response")
	}
	if err := validateHeader("restored auth user list", usersResponse.Header); err != nil {
		return err
	}
	userSet := make(map[string]struct{}, len(usersResponse.Users))
	for _, name := range usersResponse.Users {
		if _, duplicate := userSet[name]; duplicate {
			return fmt.Errorf("restored auth user list repeated %q", name)
		}
		userSet[name] = struct{}{}
	}
	userByName := make(map[string]restoredSnapshotAuthUserExpectation, len(expected.users))
	for _, user := range expected.users {
		if user.name == "" || user.noPassword == (user.password != "") || len(user.roles) == 0 {
			return fmt.Errorf("invalid restored auth user expectation for %q", user.name)
		}
		if _, duplicate := userByName[user.name]; duplicate {
			return fmt.Errorf("duplicate restored auth user expectation for %q", user.name)
		}
		userByName[user.name] = user
		if _, exists := userSet[user.name]; !exists {
			return fmt.Errorf("restored auth user %q is missing", user.name)
		}
		response, getErr := client.UserGet(ctx, user.name)
		if getErr != nil {
			return fmt.Errorf("get restored auth user %q: %w", user.name, getErr)
		}
		if response == nil {
			return fmt.Errorf("get restored auth user %q returned a nil response", user.name)
		}
		if err := validateHeader("restored auth user "+user.name, response.Header); err != nil {
			return err
		}
		if len(response.Roles) != len(user.roles) {
			return fmt.Errorf("restored auth user %q role count mismatch: got=%d want=%d", user.name, len(response.Roles), len(user.roles))
		}
		for index := range user.roles {
			if response.Roles[index] != user.roles[index] {
				return fmt.Errorf("restored auth user %q role mismatch at %d: got=%q want=%q",
					user.name, index, response.Roles[index], user.roles[index])
			}
		}
	}
	rolesResponse, err := client.RoleList(ctx)
	if err != nil {
		return fmt.Errorf("list restored auth roles: %w", err)
	}
	if rolesResponse == nil {
		return errors.New("list restored auth roles returned a nil response")
	}
	if err := validateHeader("restored auth role list", rolesResponse.Header); err != nil {
		return err
	}
	roleSet := make(map[string]struct{}, len(rolesResponse.Roles))
	for _, name := range rolesResponse.Roles {
		if _, duplicate := roleSet[name]; duplicate {
			return fmt.Errorf("restored auth role list repeated %q", name)
		}
		roleSet[name] = struct{}{}
	}
	roleByName := make(map[string]struct{}, len(expected.roles))
	for _, role := range expected.roles {
		if role.name == "" || len(role.permissions) == 0 {
			return fmt.Errorf("invalid restored auth role expectation for %q", role.name)
		}
		if _, duplicate := roleByName[role.name]; duplicate {
			return fmt.Errorf("duplicate restored auth role expectation for %q", role.name)
		}
		roleByName[role.name] = struct{}{}
		if _, exists := roleSet[role.name]; !exists {
			return fmt.Errorf("restored auth role %q is missing", role.name)
		}
		response, getErr := client.RoleGet(ctx, role.name)
		if getErr != nil {
			return fmt.Errorf("get restored auth role %q: %w", role.name, getErr)
		}
		if response == nil {
			return fmt.Errorf("get restored auth role %q returned a nil response", role.name)
		}
		if err := validateHeader("restored auth role "+role.name, response.Header); err != nil {
			return err
		}
		if len(response.Perm) != len(role.permissions) {
			return fmt.Errorf("restored auth role %q permission count mismatch: got=%d want=%d",
				role.name, len(response.Perm), len(role.permissions))
		}
		for index := range role.permissions {
			if !proto.Equal(response.Perm[index], role.permissions[index]) {
				return fmt.Errorf("restored auth role %q permission mismatch at %d", role.name, index)
			}
		}
	}

	adminClient := client
	adminConfig := clientConfig
	if !expected.enabled {
		const restoredRootPassword = "kubebrain-rollout-restored-root-secret"
		var rootResponse *clientv3.AuthUserGetResponse
		if _, rootExists := userSet["root"]; !rootExists {
			if _, err = client.UserAdd(ctx, "root", restoredRootPassword); err != nil {
				return fmt.Errorf("add root user to isolated restored etcd: %w", err)
			}
			rootResponse, err = client.UserGet(ctx, "root")
			if err != nil {
				return fmt.Errorf("read added root user from isolated restored etcd: %w", err)
			}
		} else {
			rootResponse, err = client.UserGet(ctx, "root")
			if err != nil {
				return fmt.Errorf("inspect restored root user: %w", err)
			}
			if _, err = client.UserChangePassword(ctx, "root", restoredRootPassword); err != nil {
				return fmt.Errorf("set isolated restored root password: %w", err)
			}
		}
		hasRootRole := false
		for _, role := range rootResponse.Roles {
			hasRootRole = hasRootRole || role == "root"
		}
		if !hasRootRole {
			if _, err = client.UserGrantRole(ctx, "root", "root"); err != nil {
				return fmt.Errorf("grant root role in isolated restored etcd: %w", err)
			}
		}
		if _, err = client.AuthEnable(ctx); err != nil {
			return fmt.Errorf("enable auth in isolated restored etcd: %w", err)
		}
		rootConfig := passwordClientConfig
		rootConfig.Username = "root"
		rootConfig.Password = restoredRootPassword
		rootConfig.Logger = zap.NewNop()
		rootClient, clientErr := clientv3.New(rootConfig)
		if clientErr != nil {
			return fmt.Errorf("authenticate isolated restored root user: %w", clientErr)
		}
		defer func() {
			if closeErr := rootClient.Close(); closeErr != nil {
				retErr = errors.Join(retErr, fmt.Errorf("close isolated restored root client: %w", closeErr))
			}
		}()
		adminClient = rootClient
		adminConfig = rootConfig
	}
	// mTLS authenticates the transport, but this dedicated empty-CN client has
	// neither a certificate username nor a password token. A source CN=root
	// connection cannot stand in for an unauthenticated authorization check.
	anonymous, err := clientv3.New(passwordClientConfig)
	if err != nil {
		return fmt.Errorf("create unauthenticated restored client: %w", err)
	}
	defer func() {
		if closeErr := anonymous.Close(); closeErr != nil {
			retErr = errors.Join(retErr, fmt.Errorf("close unauthenticated restored client: %w", closeErr))
		}
	}()
	if _, err = anonymous.Get(ctx, expected.access[0].key); !errors.Is(err, rpctypes.ErrUserEmpty) &&
		!errors.Is(err, rpctypes.ErrPermissionDenied) {
		return fmt.Errorf("unauthenticated restored Range after AuthEnable returned %v, want user empty or permission denied", err)
	}
	if _, err = adminClient.RoleList(ctx); err != nil {
		return fmt.Errorf("list roles as isolated restored root user: %w", err)
	}
	if _, err = adminClient.Put(ctx, expected.rootKey, "root-admin"); err != nil {
		return fmt.Errorf("write unrestricted key as isolated restored root user: %w", err)
	}
	rootRange, err := adminClient.Get(ctx, expected.rootKey)
	if err != nil {
		return fmt.Errorf("read unrestricted key as isolated restored root user: %w", err)
	}
	if len(rootRange.Kvs) != 1 || string(rootRange.Kvs[0].Value) != "root-admin" {
		return fmt.Errorf("isolated restored root read mismatch: kvs=%d", len(rootRange.Kvs))
	}
	if _, err = adminClient.Delete(ctx, expected.rootKey); err != nil {
		return fmt.Errorf("delete unrestricted key as isolated restored root user: %w", err)
	}

	clients := make(map[string]*clientv3.Client, len(expected.users))
	defer func() {
		for name, authenticated := range clients {
			if closeErr := authenticated.Close(); closeErr != nil {
				retErr = errors.Join(retErr, fmt.Errorf("close restored auth client %q: %w", name, closeErr))
			}
		}
	}()
	for _, user := range expected.users {
		if user.noPassword {
			_, authenticateErr := client.Authenticate(ctx, user.name, "must-be-rejected")
			const noPasswordMessage = "auth: authentication failed, password was given for no password user"
			if status.Code(authenticateErr) != codes.Unknown || status.Convert(authenticateErr).Message() != noPasswordMessage {
				return fmt.Errorf("authenticate restored no-password user %q returned %v, want code=%s message=%q",
					user.name, authenticateErr, codes.Unknown, noPasswordMessage)
			}
			continue
		}
		cfg := passwordClientConfig
		cfg.Username = user.name
		cfg.Password = user.password
		cfg.Logger = zap.NewNop()
		authenticated, clientErr := clientv3.New(cfg)
		if clientErr != nil {
			return fmt.Errorf("authenticate restored user %q: %w", user.name, clientErr)
		}
		clients[user.name] = authenticated
	}
	barrier := func(barrierCtx context.Context, key, value string, leaseID clientv3.LeaseID, revision int64) error {
		return waitForRestoredAuthLeaseReplication(barrierCtx, adminConfig, key, value, leaseID, revision)
	}
	for index, access := range expected.access {
		authenticated := clients[access.username]
		if authenticated == nil || access.key == "" {
			return fmt.Errorf("invalid restored auth access expectation at %d", index)
		}
		if err := verifyRestoredAuthAccess(ctx, authenticated, access, index, barrier); err != nil {
			return err
		}
	}
	if verifyAdmin != nil {
		if err := verifyAdmin(adminClient, adminConfig); err != nil {
			return fmt.Errorf("verify restored cluster as administrator: %w", err)
		}
	}
	return nil
}

func verifyRestoredAuthAccess(ctx context.Context, client *clientv3.Client, expected restoredSnapshotAuthAccessExpectation,
	index int, replicationBarrier restoredSnapshotLeaseReplicationBarrier,
) (retErr error) {
	started := time.Now()
	stage := "Range"
	defer func() {
		// Runs before the caller closes clients or restored members. Cleanup
		// time must not turn an active request into an apparent deadline failure.
		retErr = restoredAuthAccessFailure(retErr, stage, time.Since(started), ctx.Err())
	}()
	requirePermission := func(label string, allowed bool, operation func() error) error {
		stage = label
		err := operation()
		if allowed && err != nil {
			return fmt.Errorf("restored auth %s was denied for user=%q key=%q: %w", label, expected.username, expected.key, err)
		}
		if !allowed && !errors.Is(err, rpctypes.ErrPermissionDenied) {
			return fmt.Errorf("restored auth %s returned %v for denied user=%q key=%q", label, err, expected.username, expected.key)
		}
		return nil
	}
	if err := requirePermission("Range", expected.read, func() error {
		_, err := client.Get(ctx, expected.key)
		return err
	}); err != nil {
		return err
	}
	stage = "Watch"
	watchCtx, cancelWatch := context.WithCancel(ctx)
	watch := client.Watch(watchCtx, expected.key, clientv3.WithCreatedNotify())
	select {
	case response, ok := <-watch:
		if !ok {
			closeErr := restoredAuthWatchCloseError(watchCtx, expected.username, expected.key)
			cancelWatch()
			return closeErr
		}
		// Capture before our own cancellation; otherwise every response would
		// misleadingly appear to have arrived on a canceled context.
		watchEvidence := restoredAuthWatchEvidence(response, watchCtx.Err())
		cancelWatch()
		if expected.read {
			if err := response.Err(); err != nil || response.Canceled || !response.Created {
				return fmt.Errorf("restored auth Watch was denied for user=%q key=%q: %v (%s)", expected.username, expected.key, response.Err(), watchEvidence)
			}
		} else {
			watchErr := response.Err()
			if !errors.Is(watchErr, rpctypes.ErrPermissionDenied) && status.Code(watchErr) != codes.PermissionDenied &&
				response.CancelReason != rpctypes.ErrGRPCPermissionDenied.Error() &&
				(watchErr == nil || watchErr.Error() != rpctypes.ErrGRPCPermissionDenied.Error()) {
				return fmt.Errorf("restored auth Watch returned %v reason=%q for denied user=%q key=%q (%s)",
					watchErr, response.CancelReason, expected.username, expected.key, watchEvidence)
			}
		}
	case <-ctx.Done():
		cancelWatch()
		return fmt.Errorf("wait for restored auth Watch user=%q key=%q: %w", expected.username, expected.key, context.Cause(ctx))
	}
	value := fmt.Sprintf("restored-auth-%d", index)
	if err := requirePermission("Put", expected.write, func() error {
		_, err := client.Put(ctx, expected.key, value)
		return err
	}); err != nil {
		return err
	}
	if err := requirePermission("Txn Put", expected.write, func() error {
		_, err := client.Txn(ctx).Then(clientv3.OpPut(expected.key, value+"-txn")).Commit()
		return err
	}); err != nil {
		return err
	}
	if err := requirePermission("Put PrevKV", expected.read && expected.write, func() error {
		_, err := client.Put(ctx, expected.key, value+"-prev", clientv3.WithPrevKV())
		return err
	}); err != nil {
		return err
	}
	if err := requirePermission("Delete", expected.write, func() error {
		_, err := client.Delete(ctx, expected.key)
		return err
	}); err != nil {
		return err
	}
	if expected.write {
		stage = "LeaseGrant"
		lease, err := client.Grant(ctx, 30)
		if err != nil {
			return fmt.Errorf("grant restored auth lease for user=%q: %w", expected.username, err)
		}
		leasedValue := value + "-leased"
		stage = "LeasePut"
		leasedPut, putErr := client.Put(ctx, expected.key, leasedValue, clientv3.WithLease(lease.ID))
		if putErr != nil {
			return fmt.Errorf("attach restored auth lease for user=%q key=%q: %w", expected.username, expected.key, putErr)
		}
		if leasedPut == nil || leasedPut.Header == nil || leasedPut.Header.Revision <= 0 {
			return fmt.Errorf("attach restored auth lease for user=%q key=%q returned an invalid response", expected.username, expected.key)
		}
		stage = "LeaseReplication"
		if replicationBarrier == nil {
			return errors.New("restored auth lease replication barrier is required")
		}
		if err = replicationBarrier(ctx, expected.key, leasedValue, lease.ID, leasedPut.Header.Revision); err != nil {
			return fmt.Errorf("replicate restored auth lease for user=%q key=%q: %w", expected.username, expected.key, err)
		}
		stage = "LeaseTimeToLive"
		_, ttlErr := client.TimeToLive(ctx, lease.ID, clientv3.WithAttachedKeys())
		if expected.read && ttlErr != nil {
			return fmt.Errorf("read restored auth lease keys for user=%q: %w", expected.username, ttlErr)
		}
		if !expected.read && !errors.Is(ttlErr, rpctypes.ErrPermissionDenied) {
			return fmt.Errorf("restored auth lease key read returned %v for write-only user=%q", ttlErr, expected.username)
		}
		stage = "LeaseKeepAliveOnce"
		if _, err = client.KeepAliveOnce(ctx, lease.ID); err != nil {
			return fmt.Errorf("keep restored auth lease alive for user=%q: %w", expected.username, err)
		}
		stage = "LeaseRevoke"
		if _, err = client.Revoke(ctx, lease.ID); err != nil {
			return fmt.Errorf("revoke restored auth lease for user=%q: %w", expected.username, err)
		}
	}
	return nil
}

func restoredAuthAccessFailure(err error, stage string, elapsed time.Duration, contextErr error) error {
	if err == nil {
		return nil
	}
	// stage is selected only from constants above; context causes may contain
	// arbitrary caller text, so expose flags rather than their error strings.
	return fmt.Errorf("%w (auth_stage=%q access_elapsed_ms=%d context_done=%t deadline_exceeded=%t)",
		err, stage, elapsed.Milliseconds(), contextErr != nil, errors.Is(contextErr, context.DeadlineExceeded))
}

// Only fixed-format envelope metadata is added here: no token, credentials,
// event payload, or unrestricted server error text. Missing headers explicitly
// remain unknown; a transport-level error need not identify its serving member.
func restoredAuthWatchEvidence(response clientv3.WatchResponse, contextErr error) string {
	return fmt.Sprintf("header_present=%t cluster_id=%d member_id=%d revision=%d raft_term=%d created=%t canceled=%t compact_revision=%d events=%d context_done=%t",
		response.Header != nil, response.Header.GetClusterId(), response.Header.GetMemberId(),
		response.Header.GetRevision(), response.Header.GetRaftTerm(), response.Created, response.Canceled,
		response.CompactRevision, len(response.Events), contextErr != nil)
}

func restoredAuthWatchCloseError(ctx context.Context, username, key string) error {
	if cause := context.Cause(ctx); cause != nil {
		return fmt.Errorf("restored auth Watch closed for user=%q key=%q: %w", username, key, cause)
	}
	return fmt.Errorf("restored auth Watch closed for user=%q key=%q", username, key)
}
