package compat

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	clientv3 "go.etcd.io/etcd/client/v3"
)

// TestAuthLeaseTTLDoesNotLeakConcurrentlyAttachedKey requires a disposable
// auth-disabled endpoint. It temporarily enables auth and always disables it
// again through the root client before returning.
func TestAuthLeaseTTLDoesNotLeakConcurrentlyAttachedKey(t *testing.T) {
	endpoint := os.Getenv("KUBEBRAIN_AUTH_LEASE_SNAPSHOT_ENDPOINT")
	if endpoint == "" {
		t.Skip("set KUBEBRAIN_AUTH_LEASE_SNAPSHOT_ENDPOINT to a disposable auth-disabled KubeBrain instance")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	bootstrap := authClient(t, endpoint, "", "")

	status, err := bootstrap.AuthStatus(ctx)
	require.NoError(t, err)
	require.False(t, status.Enabled, "test endpoint must start with auth disabled")
	_, err = bootstrap.UserChangePassword(ctx, "root", "root-secret")
	require.NoError(t, err)

	suffix := fmt.Sprintf("%d", time.Now().UnixNano())
	username := "a72-alice-" + suffix
	role := "a72-role-" + suffix
	allowedKey := "/a72/allowed/" + suffix
	deniedKey := "/a72/denied/" + suffix
	_, err = bootstrap.UserAdd(ctx, username, "alice-secret")
	require.NoError(t, err)
	_, err = bootstrap.RoleAdd(ctx, role)
	require.NoError(t, err)
	_, err = bootstrap.RoleGrantPermission(
		ctx, role, allowedKey, clientv3.GetPrefixRangeEnd(allowedKey),
		clientv3.PermissionType(clientv3.PermReadWrite),
	)
	require.NoError(t, err)
	_, err = bootstrap.UserGrantRole(ctx, username, role)
	require.NoError(t, err)
	_, err = bootstrap.AuthEnable(ctx)
	require.NoError(t, err)

	root := authClient(t, endpoint, "root", "root-secret")
	defer func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cleanupCancel()
		_, _ = root.AuthDisable(cleanupCtx)
		_, _ = bootstrap.UserDelete(cleanupCtx, username)
		_, _ = bootstrap.RoleDelete(cleanupCtx, role)
	}()
	alice := authClient(t, endpoint, username, "alice-secret")
	lease, err := alice.Grant(ctx, 60)
	require.NoError(t, err)
	_, err = alice.Put(ctx, allowedKey, "allowed", clientv3.WithLease(lease.ID))
	require.NoError(t, err)

	runCtx, stop := context.WithTimeout(ctx, 8*time.Second)
	defer stop()
	errs := make(chan error, 64)
	var successes atomic.Int64
	var deniedCycles atomic.Int64
	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()
		for runCtx.Err() == nil {
			if _, putErr := root.Put(runCtx, deniedKey, "secret", clientv3.WithLease(lease.ID)); putErr != nil {
				if runCtx.Err() == nil {
					errs <- putErr
				}
				return
			}
			deniedCycles.Add(1)
			if _, putErr := root.Put(runCtx, deniedKey, "detached"); putErr != nil {
				if runCtx.Err() == nil {
					errs <- putErr
				}
				return
			}
		}
	}()

	for range 16 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for runCtx.Err() == nil {
				ttl, ttlErr := alice.TimeToLive(runCtx, lease.ID, clientv3.WithAttachedKeys())
				if ttlErr != nil {
					if runCtx.Err() == nil && !errorsIsPermissionDenied(ttlErr) {
						errs <- ttlErr
						return
					}
					continue
				}
				successes.Add(1)
				for _, key := range ttl.Keys {
					if string(key) == deniedKey {
						errs <- fmt.Errorf("successful lease TTL disclosed protected key %q", deniedKey)
						return
					}
				}
			}
		}()
	}

	wg.Wait()
	close(errs)
	for raceErr := range errs {
		require.NoError(t, raceErr)
	}
	require.Positive(t, deniedCycles.Load())
	require.Positive(t, successes.Load())
	t.Logf("validated %d successful TTL snapshots across %d protected attach/detach cycles",
		successes.Load(), deniedCycles.Load())

	cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cleanupCancel()
	_, _ = root.Delete(cleanupCtx, deniedKey)
	_, err = root.Revoke(cleanupCtx, lease.ID)
	require.NoError(t, err)
}

func errorsIsPermissionDenied(err error) bool {
	return errors.Is(err, rpctypes.ErrPermissionDenied)
}
