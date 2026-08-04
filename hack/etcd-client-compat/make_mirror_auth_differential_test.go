package compat

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	clientv3 "go.etcd.io/etcd/client/v3"
)

const (
	mirrorRootPassword = "a228-root-secret"
	mirrorUserPassword = "a228-sync-secret"
)

func TestMakeMirrorAuthenticatedBidirectionalDifferential(t *testing.T) {
	referenceEndpoint := os.Getenv("REFERENCE_ETCD_ENDPOINT")
	kubeBrainEndpoint := os.Getenv("KUBEBRAIN_AUTH_MIRROR_ENDPOINT")
	if referenceEndpoint == "" || kubeBrainEndpoint == "" {
		t.Skip("set REFERENCE_ETCD_ENDPOINT and KUBEBRAIN_AUTH_MIRROR_ENDPOINT to disposable instances")
	}
	require.NotEqual(t, mirrorEndpointIdentity(referenceEndpoint), mirrorEndpointIdentity(kubeBrainEndpoint))
	if mainEndpoint := os.Getenv("KUBEBRAIN_ETCD_ENDPOINT"); mainEndpoint != "" {
		require.NotEqual(t, mirrorEndpointIdentity(mainEndpoint), mirrorEndpointIdentity(kubeBrainEndpoint),
			"KUBEBRAIN_AUTH_MIRROR_ENDPOINT must not be the shared main endpoint")
	}
	etcdctl := os.Getenv("ETCDCTL_BIN")
	if etcdctl == "" {
		etcdctl = "/root/etcd/bin/etcdctl"
	}
	if _, err := os.Stat(etcdctl); err != nil {
		t.Skipf("etcdctl binary unavailable: %v", err)
	}

	setupMirrorAuth(t, referenceEndpoint)
	setupMirrorAuth(t, kubeBrainEndpoint)
	referenceToKubeBrain := runAuthenticatedMakeMirrorDirection(
		t, etcdctl, referenceEndpoint, kubeBrainEndpoint, "auth-reference-to-kubebrain",
	)
	kubeBrainToReference := runAuthenticatedMakeMirrorDirection(
		t, etcdctl, kubeBrainEndpoint, referenceEndpoint, "auth-kubebrain-to-reference",
	)
	want := makeMirrorOutcome{
		BaseCopied:              true,
		Final:                   []string{"a=updated-a", "c=created-c"},
		UpdateAppliedAtomically: true,
	}
	require.Equal(t, want, referenceToKubeBrain)
	require.Equal(t, referenceToKubeBrain, kubeBrainToReference)
}

func setupMirrorAuth(t *testing.T, endpoint string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	t.Cleanup(cancel)
	bootstrap := authClient(t, endpoint, "", "")
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cleanupCancel()
		status, statusErr := bootstrap.AuthStatus(cleanupCtx)
		require.NoError(t, statusErr)
		if status.Enabled {
			root, clientErr := clientv3.New(clientv3.Config{
				Endpoints: []string{endpoint}, DialTimeout: 3 * time.Second,
				Username: "root", Password: mirrorRootPassword,
			})
			require.NoError(t, clientErr)
			_, disableErr := root.AuthDisable(cleanupCtx)
			require.NoError(t, disableErr)
			require.NoError(t, root.Close())
		}
		users, listUsersErr := bootstrap.UserList(cleanupCtx)
		require.NoError(t, listUsersErr)
		for _, user := range users.Users {
			detail, getErr := bootstrap.UserGet(cleanupCtx, user)
			require.NoError(t, getErr)
			for _, role := range detail.Roles {
				_, _ = bootstrap.UserRevokeRole(cleanupCtx, user, role)
			}
			_, deleteErr := bootstrap.UserDelete(cleanupCtx, user)
			require.NoError(t, deleteErr)
		}
		roles, listRolesErr := bootstrap.RoleList(cleanupCtx)
		require.NoError(t, listRolesErr)
		for _, role := range roles.Roles {
			_, deleteErr := bootstrap.RoleDelete(cleanupCtx, role)
			require.NoError(t, deleteErr)
		}
		_, deleteErr := bootstrap.Delete(cleanupCtx, "/dbaas-auth-mirror/", clientv3.WithPrefix())
		require.NoError(t, deleteErr)
		finalStatus, finalStatusErr := bootstrap.AuthStatus(cleanupCtx)
		require.NoError(t, finalStatusErr)
		require.False(t, finalStatus.Enabled)
		finalUsers, finalUsersErr := bootstrap.UserList(cleanupCtx)
		require.NoError(t, finalUsersErr)
		require.Empty(t, finalUsers.Users)
		finalRoles, finalRolesErr := bootstrap.RoleList(cleanupCtx)
		require.NoError(t, finalRolesErr)
		require.Empty(t, finalRoles.Roles)
		remaining, getErr := bootstrap.Get(cleanupCtx, "/dbaas-auth-mirror/", clientv3.WithPrefix())
		require.NoError(t, getErr)
		require.Zero(t, remaining.Count)
		leases, leaseErr := bootstrap.Leases(cleanupCtx)
		require.NoError(t, leaseErr)
		require.Empty(t, leases.Leases)
	})
	initialStatus, err := bootstrap.AuthStatus(ctx)
	require.NoError(t, err)
	require.False(t, initialStatus.Enabled)
	initialUsers, err := bootstrap.UserList(ctx)
	require.NoError(t, err)
	require.Empty(t, initialUsers.Users, "auth mirror test requires an empty disposable instance")
	_, err = bootstrap.UserAdd(ctx, "root", mirrorRootPassword)
	require.NoError(t, err)
	_, err = bootstrap.UserGrantRole(ctx, "root", "root")
	require.NoError(t, err)
	_, err = bootstrap.RoleAdd(ctx, "mirror-syncer")
	require.NoError(t, err)
	_, err = bootstrap.RoleGrantPermission(
		ctx,
		"mirror-syncer",
		"/dbaas-auth-mirror/",
		clientv3.GetPrefixRangeEnd("/dbaas-auth-mirror/"),
		clientv3.PermissionType(clientv3.PermReadWrite),
	)
	require.NoError(t, err)
	_, err = bootstrap.UserAdd(ctx, "mirror-syncer", mirrorUserPassword)
	require.NoError(t, err)
	_, err = bootstrap.UserGrantRole(ctx, "mirror-syncer", "mirror-syncer")
	require.NoError(t, err)
	_, err = bootstrap.AuthEnable(ctx)
	require.NoError(t, err)
	status, err := bootstrap.AuthStatus(ctx)
	require.NoError(t, err)
	require.True(t, status.Enabled)
	_, anonymousErr := bootstrap.Get(ctx, "/dbaas-auth-mirror/probe")
	require.ErrorIs(t, anonymousErr, rpctypes.ErrUserEmpty)

}

func mirrorEndpointIdentity(endpoint string) string {
	return strings.TrimRight(strings.TrimPrefix(strings.TrimPrefix(endpoint, "http://"), "https://"), "/")
}

func runAuthenticatedMakeMirrorDirection(
	t *testing.T,
	etcdctl, sourceEndpoint, destinationEndpoint, direction string,
) makeMirrorOutcome {
	t.Helper()
	source := authClient(t, sourceEndpoint, "mirror-syncer", mirrorUserPassword)
	destination := authClient(t, destinationEndpoint, "mirror-syncer", mirrorUserPassword)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	t.Cleanup(cancel)
	base := fmt.Sprintf("/dbaas-auth-mirror/%s/%d/", direction, time.Now().UnixNano())
	sourcePrefix := base + "source/"
	destinationPrefix := base + "destination/"
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cleanupCancel()
		_, sourceCleanupErr := source.Delete(cleanupCtx, base, clientv3.WithPrefix())
		require.NoError(t, sourceCleanupErr)
		_, destinationCleanupErr := destination.Delete(cleanupCtx, base, clientv3.WithPrefix())
		require.NoError(t, destinationCleanupErr)
	})

	seed, err := source.Txn(ctx).Then(
		clientv3.OpPut(sourcePrefix+"a", "seed-a"),
		clientv3.OpPut(sourcePrefix+"b", "seed-b"),
	).Commit()
	require.NoError(t, err)
	require.True(t, seed.Succeeded)

	stopMirror := startMirrorCommand(
		t,
		etcdctl,
		"--endpoints="+sourceEndpoint,
		"--user=mirror-syncer:"+mirrorUserPassword,
		"make-mirror",
		"--prefix="+sourcePrefix,
		"--dest-prefix="+destinationPrefix,
		"--dest-user=mirror-syncer:"+mirrorUserPassword,
		destinationEndpoint,
	)
	baseCopied := false
	require.Eventually(t, func() bool {
		response, getErr := destination.Get(ctx, destinationPrefix, clientv3.WithPrefix())
		if getErr != nil {
			return false
		}
		baseCopied = mirrorValues(response.Kvs, destinationPrefix) == "a=seed-a,b=seed-b"
		return baseCopied
	}, 10*time.Second, 20*time.Millisecond)

	update, err := source.Txn(ctx).Then(
		clientv3.OpPut(sourcePrefix+"a", "updated-a"),
		clientv3.OpDelete(sourcePrefix+"b"),
		clientv3.OpPut(sourcePrefix+"c", "created-c"),
	).Commit()
	require.NoError(t, err)
	require.True(t, update.Succeeded)

	var final []string
	updateAppliedAtomically := false
	require.Eventually(t, func() bool {
		response, getErr := destination.Get(ctx, destinationPrefix, clientv3.WithPrefix())
		if getErr != nil || mirrorValues(response.Kvs, destinationPrefix) !=
			"a=updated-a,c=created-c" {
			return false
		}
		final = mirrorValueSlice(response.Kvs, destinationPrefix)
		updateAppliedAtomically = len(response.Kvs) == 2 &&
			response.Kvs[0].ModRevision == response.Kvs[1].ModRevision
		return updateAppliedAtomically
	}, 10*time.Second, 20*time.Millisecond)
	stopMirror()

	return makeMirrorOutcome{
		BaseCopied:              baseCopied,
		Final:                   final,
		UpdateAppliedAtomically: updateAppliedAtomically,
	}
}
