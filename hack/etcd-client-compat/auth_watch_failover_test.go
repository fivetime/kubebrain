package compat

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"
)

// TestAuthorizedWatchSurvivesPermissionRevokeAndLeaderFailover exercises a
// single logical Watch through the combination that exposed the proxy bug:
// etcd authorizes the Watch at creation, a later permission revoke must not
// cancel it, and an internal follower-to-leader generation change must preserve
// that original decision. The endpoint must address a replica that is not the
// pod removed by the failover command.
func TestAuthorizedWatchSurvivesPermissionRevokeAndLeaderFailover(t *testing.T) {
	endpoint := os.Getenv("KUBEBRAIN_AUTH_WATCH_FOLLOWER_ENDPOINT")
	failoverCommand := os.Getenv("KUBEBRAIN_AUTH_WATCH_FAILOVER_COMMAND")
	if endpoint == "" || failoverCommand == "" {
		t.Skip("set KUBEBRAIN_AUTH_WATCH_FOLLOWER_ENDPOINT and KUBEBRAIN_AUTH_WATCH_FAILOVER_COMMAND")
	}
	namespace := os.Getenv("KUBEBRAIN_FAILOVER_NAMESPACE")
	if namespace == "" {
		namespace = "kubebrain-dev"
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	bootstrap := authClient(t, endpoint, "", "")
	authStatus, err := bootstrap.AuthStatus(ctx)
	require.NoError(t, err)
	require.False(t, authStatus.Enabled, "test requires a disposable cluster with auth disabled")

	suffix := fmt.Sprintf("%d", time.Now().UnixNano())
	rootUser := "root"
	aliceUser := "auth-watch-alice-" + suffix
	role := "auth-watch-role-" + suffix
	rootPassword := "root-secret-" + suffix
	alicePassword := "alice-secret-" + suffix
	prefix := "/dbaas-auth-watch-failover/" + suffix + "/"
	key := prefix + "key"

	_, err = bootstrap.UserAdd(ctx, rootUser, rootPassword)
	require.NoError(t, err)
	_, err = bootstrap.UserGrantRole(ctx, rootUser, "root")
	require.NoError(t, err)
	_, err = bootstrap.UserAdd(ctx, aliceUser, alicePassword)
	require.NoError(t, err)
	_, err = bootstrap.RoleAdd(ctx, role)
	require.NoError(t, err)
	_, err = bootstrap.RoleGrantPermission(ctx, role, prefix, clientv3.GetPrefixRangeEnd(prefix),
		clientv3.PermissionType(clientv3.PermReadWrite))
	require.NoError(t, err)
	_, err = bootstrap.UserGrantRole(ctx, aliceUser, role)
	require.NoError(t, err)
	_, err = bootstrap.AuthEnable(ctx)
	require.NoError(t, err)

	root := authClient(t, endpoint, rootUser, rootPassword)
	alice := authClient(t, endpoint, aliceUser, alicePassword)
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 45*time.Second)
		defer cleanupCancel()
		disabled := false
		for cleanupCtx.Err() == nil {
			if _, disableErr := root.AuthDisable(cleanupCtx); disableErr == nil {
				disabled = true
				break
			}
			time.Sleep(250 * time.Millisecond)
		}
		require.True(t, disabled, "cleanup must restore auth-disabled state")
		_, cleanupErr := bootstrap.Delete(cleanupCtx, prefix, clientv3.WithPrefix())
		require.NoError(t, cleanupErr)
		_, cleanupErr = bootstrap.UserDelete(cleanupCtx, aliceUser)
		require.NoError(t, cleanupErr)
		_, cleanupErr = bootstrap.RoleDelete(cleanupCtx, role)
		require.NoError(t, cleanupErr)
		_, cleanupErr = bootstrap.UserDelete(cleanupCtx, rootUser)
		require.NoError(t, cleanupErr)
	})

	watchCtx, watchCancel := context.WithCancel(ctx)
	defer watchCancel()
	watch := alice.Watch(watchCtx, key, clientv3.WithCreatedNotify())
	created := receiveAuthWatchResponse(t, ctx, watch, "initial create")
	require.True(t, created.Created)
	require.NoError(t, created.Err())

	_, err = root.RoleRevokePermission(ctx, role, prefix, clientv3.GetPrefixRangeEnd(prefix))
	require.NoError(t, err)
	_, err = root.Put(ctx, key, "during-revoke")
	require.NoError(t, err)
	requireAuthWatchValue(t, ctx, watch, "during-revoke")

	// A new logical Watch observes the latest permission revision even though
	// the already-created Watch above remains authorized.
	deniedCtx, deniedCancel := context.WithTimeout(ctx, 10*time.Second)
	denied := alice.Watch(deniedCtx, key, clientv3.WithCreatedNotify())
	deniedResponse := receiveAuthWatchResponse(t, deniedCtx, denied, "post-revoke create")
	require.Error(t, deniedResponse.Err())
	require.Contains(t, deniedResponse.Err().Error(), "etcdserver: permission denied")
	deniedCancel()

	output, err := runCompatShellCommandContext(t, ctx, failoverCommand)
	require.NoErrorf(t, err, "delete current KubeBrain leader: %s", strings.TrimSpace(string(output)))
	output, err = waitForKubeBrainRollout(t, ctx, namespace)
	require.NoErrorf(t, err, "wait for KubeBrain replacement: %s", strings.TrimSpace(string(output)))

	require.Eventually(t, func() bool {
		callCtx, callCancel := context.WithTimeout(ctx, 2*time.Second)
		defer callCancel()
		_, putErr := root.Put(callCtx, key, "after-failover")
		return putErr == nil
	}, 45*time.Second, 250*time.Millisecond, "authenticated writes must recover after leader replacement")
	requireAuthWatchValue(t, ctx, watch, "after-failover")
}

func receiveAuthWatchResponse(t *testing.T, ctx context.Context, watch clientv3.WatchChan, stage string) clientv3.WatchResponse {
	t.Helper()
	select {
	case response, ok := <-watch:
		require.True(t, ok, "Watch closed during %s", stage)
		return response
	case <-ctx.Done():
		t.Fatalf("timed out waiting for Watch response during %s: %v", stage, ctx.Err())
		return clientv3.WatchResponse{}
	}
}

func requireAuthWatchValue(t *testing.T, ctx context.Context, watch clientv3.WatchChan, expected string) {
	t.Helper()
	for {
		response := receiveAuthWatchResponse(t, ctx, watch, expected)
		require.NoError(t, response.Err(), "Watch canceled before %q", expected)
		for _, event := range response.Events {
			if string(event.Kv.Value) == expected {
				return
			}
		}
	}
}
