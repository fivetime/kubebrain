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
	"net/url"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/authpb"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/server/v3/embed"
	"go.uber.org/zap"
)

func startFixtureOwnershipEtcd(t *testing.T) (*clientv3.Client, context.Context) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	t.Cleanup(cancel)
	clientURL, peerURL, err := allocateRestoredSnapshotURLs(false)
	require.NoError(t, err)
	cfg := embed.NewConfig()
	cfg.Name = "fixture-ownership"
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
		t.Fatal("fixture ownership etcd did not become ready")
	}
	client, err := clientv3.New(clientv3.Config{
		Endpoints: []string{clientURL.String()}, DialTimeout: 3 * time.Second, Context: ctx, Logger: zap.NewNop(),
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, client.Close()) })
	return client, ctx
}

func testFixtureOwnerIdentity() fixtureOwnerIdentity {
	return fixtureOwnerIdentity{
		Namespace: "tenant-a", ProbePod: "availability-a", ProbePodUID: "11111111-1111-4111-8111-111111111111",
		StatefulSet: "kubebrain", StatefulSetUID: "22222222-2222-4222-8222-222222222222",
	}
}

func TestOwnedFixtureCleanupRecoversAbruptProbeState(t *testing.T) {
	client, ctx := startFixtureOwnershipEtcd(t)
	identity := testFixtureOwnerIdentity()
	prefix := fixturePrefixRoot + identity.ProbePod + "/"
	ownership, err := newFixtureOwnership(prefix, identity)
	require.NoError(t, err)
	clusterID, revision, err := ownership.claim(ctx, client)
	require.NoError(t, err)
	require.NotZero(t, clusterID)

	leaseIDs := make([]clientv3.LeaseID, 0, 3)
	for _, ttl := range []int64{15, 900, 960} {
		lease, grantErr := client.Grant(ctx, ttl)
		require.NoError(t, grantErr)
		leaseIDs = append(leaseIDs, lease.ID)
		revision, err = ownership.recordLease(ctx, client, lease.ID, clusterID, max(revision, lease.ResponseHeader.Revision))
		require.NoError(t, err)
	}
	_, err = client.Put(ctx, prefix+"lease", "alive", clientv3.WithLease(leaseIDs[0]))
	require.NoError(t, err)
	_, err = client.Put(ctx, prefix+"persistent", "fixture")
	require.NoError(t, err)

	fixture := newSnapshotAuthFixture(prefix)
	_, err = fixture.install(ctx, client, clusterID, revision, 3*time.Second)
	require.NoError(t, err)

	// Do not call either in-process cleanup method. This is the state left by
	// SIGKILL after the durable ownership receipt and fixture mutations.
	summary, err := cleanupOwnedFixture(ctx, client, prefix, identity, 3*time.Second)
	require.NoError(t, err)
	require.Equal(t, fixtureCleanupSummary{
		Status: "recovered", OwnerUID: identity.ProbePodUID, Keys: 2,
		Users: len(fixture.expected.users), Roles: len(fixture.expected.roles), Leases: len(leaseIDs),
	}, summary)
	for _, leaseID := range leaseIDs {
		ttl, ttlErr := client.TimeToLive(ctx, leaseID)
		if ttlErr == nil {
			require.NotNil(t, ttl)
			require.Equal(t, int64(-1), ttl.TTL)
		} else {
			require.ErrorIs(t, ttlErr, rpctypes.ErrLeaseNotFound)
		}
	}

	summary, err = cleanupOwnedFixture(ctx, client, prefix, identity, 3*time.Second)
	require.NoError(t, err)
	require.Equal(t, fixtureCleanupSummary{Status: "absent"}, summary)
}

func TestExternalFixtureCleanupRecoversWithoutPublicOwnershipKey(t *testing.T) {
	client, ctx := startFixtureOwnershipEtcd(t)
	identity := testFixtureOwnerIdentity()
	prefix := fixturePrefixRoot + identity.ProbePod + "/"
	leaseIDs := []clientv3.LeaseID{7001, 7002, 7003}
	clusterID, revision, err := claimExternalFixtureOwnership(ctx, client, prefix, identity, leaseIDs)
	require.NoError(t, err)
	for index, ttl := range []int64{15, 900, 960} {
		lease, grantErr := grantFixtureLease(ctx, client, leaseIDs[index], ttl)
		require.NoError(t, grantErr)
		_, revision, grantErr = validateResponseHeader(lease.ResponseHeader, clusterID, revision)
		require.NoError(t, grantErr)
	}
	put, err := client.Put(ctx, prefix+"lease", "alive", clientv3.WithLease(leaseIDs[0]))
	require.NoError(t, err)
	revision = max(revision, put.Header.Revision)
	put, err = client.Put(ctx, prefix+"persistent", "fixture")
	require.NoError(t, err)
	revision = max(revision, put.Header.Revision)

	marker, err := client.Get(ctx, legacyFixtureMarkerKey(prefix))
	require.NoError(t, err)
	require.Empty(t, marker.Kvs)
	fixture := newSnapshotAuthFixture(prefix)
	_, err = fixture.install(ctx, client, clusterID, revision, 3*time.Second)
	require.NoError(t, err)

	summary, err := cleanupExternallyOwnedFixture(ctx, client, prefix, identity, leaseIDs, 3*time.Second)
	require.NoError(t, err)
	require.Equal(t, fixtureCleanupSummary{
		Status: "recovered", OwnerUID: identity.ProbePodUID, Keys: 2,
		Users: len(fixture.expected.users), Roles: len(fixture.expected.roles), Leases: len(leaseIDs),
	}, summary)
	summary, err = cleanupExternallyOwnedFixture(ctx, client, prefix, identity, leaseIDs, 3*time.Second)
	require.NoError(t, err)
	require.Equal(t, fixtureCleanupSummary{Status: "absent"}, summary)
}

func TestExternalFixtureClaimRejectsReceiptedLeaseCollision(t *testing.T) {
	client, ctx := startFixtureOwnershipEtcd(t)
	identity := testFixtureOwnerIdentity()
	prefix := fixturePrefixRoot + identity.ProbePod + "/"
	leaseIDs := []clientv3.LeaseID{7101, 7102, 7103}
	_, err := grantFixtureLease(ctx, client, leaseIDs[1], 30)
	require.NoError(t, err)
	_, _, err = claimExternalFixtureOwnership(ctx, client, prefix, identity, leaseIDs)
	require.ErrorContains(t, err, "already exists")
}

func TestOwnedFixtureCleanupRejectsSchemaDriftWithoutDeleting(t *testing.T) {
	client, ctx := startFixtureOwnershipEtcd(t)
	identity := testFixtureOwnerIdentity()
	prefix := fixturePrefixRoot + identity.ProbePod + "/"
	ownership, err := newFixtureOwnership(prefix, identity)
	require.NoError(t, err)
	_, _, err = ownership.claim(ctx, client)
	require.NoError(t, err)
	fixture := newSnapshotAuthFixture(prefix)
	driftedRole := fixture.expected.roles[0]
	_, err = client.RoleAdd(ctx, driftedRole.name)
	require.NoError(t, err)
	_, err = client.RoleGrantPermission(ctx, driftedRole.name, prefix+"wrong", "", clientv3.PermissionType(authpb.READWRITE))
	require.NoError(t, err)
	_, err = client.Put(ctx, prefix+"must-survive", "foreign")
	require.NoError(t, err)

	_, err = cleanupOwnedFixture(ctx, client, prefix, identity, 3*time.Second)
	require.ErrorContains(t, err, "permissions drifted")
	remaining, getErr := client.Get(ctx, prefix, clientv3.WithPrefix())
	require.NoError(t, getErr)
	require.Len(t, remaining.Kvs, 1, "cleanup must preserve data after ownership schema drift")
	_, getErr = client.Get(ctx, ownership.markerKey())
	require.NoError(t, getErr)
	role, getErr := client.RoleGet(ctx, driftedRole.name)
	require.NoError(t, getErr)
	require.Len(t, role.Perm, 1)

	_, err = client.RoleDelete(ctx, driftedRole.name)
	require.NoError(t, err)
	_, err = client.Delete(ctx, prefix, clientv3.WithPrefix())
	require.NoError(t, err)
	_, err = client.Delete(ctx, ownership.markerKey())
	require.NoError(t, err)
}

func TestOwnedFixtureCleanupRejectsUnreceiptedResidue(t *testing.T) {
	client, ctx := startFixtureOwnershipEtcd(t)
	identity := testFixtureOwnerIdentity()
	prefix := fixturePrefixRoot + identity.ProbePod + "/"
	_, err := client.Put(ctx, prefix+"unowned", "preserve")
	require.NoError(t, err)

	_, err = cleanupOwnedFixture(ctx, client, prefix, identity, 3*time.Second)
	require.ErrorContains(t, err, "ownership marker is absent but residue remains")
	remaining, getErr := client.Get(ctx, prefix, clientv3.WithPrefix())
	require.NoError(t, getErr)
	require.Len(t, remaining.Kvs, 1)
}

func TestFixtureOwnershipClaimRequiresEmptyPrefixAndExactIdentity(t *testing.T) {
	client, ctx := startFixtureOwnershipEtcd(t)
	identity := testFixtureOwnerIdentity()
	prefix := fixturePrefixRoot + identity.ProbePod + "/"
	_, err := client.Put(ctx, prefix+"collision", "preserve")
	require.NoError(t, err)
	ownership, err := newFixtureOwnership(prefix, identity)
	require.NoError(t, err)
	_, _, err = ownership.claim(ctx, client)
	require.ErrorContains(t, err, "prefix is not empty")
	marker, getErr := client.Get(ctx, ownership.markerKey())
	require.NoError(t, getErr)
	require.Empty(t, marker.Kvs)

	for name, mutate := range map[string]func(*fixtureOwnerIdentity){
		"namespace":   func(value *fixtureOwnerIdentity) { value.Namespace = "Tenant" },
		"pod":         func(value *fixtureOwnerIdentity) { value.ProbePod = "other" },
		"pod uid":     func(value *fixtureOwnerIdentity) { value.ProbePodUID = "not-a-uid" },
		"statefulset": func(value *fixtureOwnerIdentity) { value.StatefulSet = "" },
		"stateful uid": func(value *fixtureOwnerIdentity) {
			value.StatefulSetUID = "33333333-3333-4333-8333-33333333333z"
		},
	} {
		t.Run(name, func(t *testing.T) {
			candidate := identity
			mutate(&candidate)
			_, createErr := newFixtureOwnership(prefix, candidate)
			require.Error(t, createErr)
		})
	}
}
