package compat

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func authClient(t *testing.T, endpoint, username, password string) *clientv3.Client {
	t.Helper()
	client, err := clientv3.New(clientv3.Config{
		Endpoints: []string{endpoint}, DialTimeout: 5 * time.Second,
		Username: username, Password: password,
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, client.Close()) })
	return client
}

// TestAuthLifecycle requires a disposable, empty KubeBrain keyspace. It is
// deliberately separate from KUBEBRAIN_ETCD_ENDPOINT because AuthEnable would
// cut off an apiserver sharing that endpoint.
func TestAuthLifecycle(t *testing.T) {
	endpoint := os.Getenv("KUBEBRAIN_AUTH_TEST_ENDPOINT")
	if endpoint == "" {
		t.Skip("set KUBEBRAIN_AUTH_TEST_ENDPOINT to a disposable KubeBrain instance")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	bootstrap := authClient(t, endpoint, "", "")

	_, err := bootstrap.UserAdd(ctx, "root", "root-secret")
	require.NoError(t, err)
	_, err = bootstrap.UserGrantRole(ctx, "root", "root")
	require.NoError(t, err)
	_, err = bootstrap.UserAdd(ctx, "alice", "alice-secret")
	require.NoError(t, err)
	_, err = bootstrap.UserAddWithOptions(ctx, "nopass", "", &clientv3.UserAddOptions{NoPassword: true})
	require.NoError(t, err)
	_, err = bootstrap.RoleAdd(ctx, "allowed")
	require.NoError(t, err)
	_, err = bootstrap.RoleGrantPermission(ctx, "allowed", "/allowed/", clientv3.GetPrefixRangeEnd("/allowed/"), clientv3.PermissionType(clientv3.PermReadWrite))
	require.NoError(t, err)
	_, err = bootstrap.UserGrantRole(ctx, "alice", "allowed")
	require.NoError(t, err)
	_, err = bootstrap.AuthEnable(ctx)
	require.NoError(t, err)
	authStatus, err := bootstrap.AuthStatus(ctx)
	require.NoError(t, err)
	require.True(t, authStatus.Enabled)
	_, err = clientv3.New(clientv3.Config{
		Endpoints: []string{endpoint}, DialTimeout: 5 * time.Second,
		Username: "nopass", Password: "password",
	})
	require.Equal(t, codes.Unknown, status.Code(err))
	require.Contains(t, err.Error(), "password was given for no password user")

	_, err = bootstrap.Put(ctx, "/allowed/anonymous", "denied")
	require.ErrorIs(t, err, rpctypes.ErrUserEmpty)
	alice := authClient(t, endpoint, "alice", "alice-secret")
	_, err = alice.Put(ctx, "/allowed/key", "value")
	require.NoError(t, err)
	_, err = alice.Put(ctx, "/denied/key", "value")
	require.ErrorIs(t, err, rpctypes.ErrPermissionDenied)
	get, err := alice.Get(ctx, "/allowed/key")
	require.NoError(t, err)
	require.Len(t, get.Kvs, 1)

	watchCtx, watchCancel := context.WithCancel(ctx)
	watch := alice.Watch(watchCtx, "/allowed/watch")
	_, err = alice.Put(ctx, "/allowed/watch", "event")
	require.NoError(t, err)
	select {
	case response := <-watch:
		require.NoError(t, response.Err())
		require.Len(t, response.Events, 1)
	case <-ctx.Done():
		t.Fatal("authorized watch did not receive its event")
	}
	watchCancel()
	deniedWatch := alice.Watch(ctx, "/denied/watch")
	select {
	case response := <-deniedWatch:
		require.Error(t, response.Err())
		require.Contains(t, response.Err().Error(), "permission denied")
	case <-ctx.Done():
		t.Fatal("unauthorized watch was not canceled")
	}

	lease, err := alice.Grant(ctx, 30)
	require.NoError(t, err)
	_, err = alice.Put(ctx, "/allowed/leased", "value", clientv3.WithLease(lease.ID))
	require.NoError(t, err)
	ttl, err := alice.TimeToLive(ctx, lease.ID, clientv3.WithAttachedKeys())
	require.NoError(t, err)
	require.Equal(t, [][]byte{[]byte("/allowed/leased")}, ttl.Keys)
	leases, err := alice.Leases(ctx)
	require.NoError(t, err)
	require.Contains(t, leases.Leases, clientv3.LeaseStatus{ID: lease.ID})
	keepAlive, err := alice.KeepAliveOnce(ctx, lease.ID)
	require.NoError(t, err)
	require.Positive(t, keepAlive.TTL)
	_, err = alice.Revoke(ctx, lease.ID)
	require.NoError(t, err)

	root := authClient(t, endpoint, "root", "root-secret")
	_, err = root.RoleAdd(ctx, "operator")
	require.NoError(t, err)
	// etcd simple-token semantics keep credentials valid across unrelated
	// role mutations while authorization reads the latest persisted snapshot.
	roles, err := root.RoleList(ctx)
	require.NoError(t, err)
	require.Contains(t, roles.Roles, "operator")
	_, err = root.AuthDisable(ctx)
	require.NoError(t, err)
	_, err = bootstrap.Put(ctx, "/anonymous-after-disable", "ok")
	require.NoError(t, err)
}
