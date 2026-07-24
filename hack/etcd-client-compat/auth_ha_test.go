package compat

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	clientv3 "go.etcd.io/etcd/client/v3"
)

// TestAuthTokenSharedAcrossEndpoints expects an already enabled disposable
// cluster with root:root-secret and /ha/key. It authenticates exactly once,
// then presents that token independently to every listed replica endpoint.
func TestAuthTokenSharedAcrossEndpoints(t *testing.T) {
	raw := os.Getenv("KUBEBRAIN_AUTH_HA_ENDPOINTS")
	if raw == "" {
		t.Skip("set KUBEBRAIN_AUTH_HA_ENDPOINTS to comma-separated replica endpoints")
	}
	endpoints := strings.Split(raw, ",")
	require.GreaterOrEqual(t, len(endpoints), 2)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	issuer := authClient(t, endpoints[0], "", "")
	authenticated, err := issuer.Authenticate(ctx, "root", "root-secret")
	require.NoError(t, err)
	require.NotEmpty(t, authenticated.Token)
	for _, endpoint := range endpoints {
		client, err := clientv3.New(clientv3.Config{
			Endpoints: []string{endpoint}, DialTimeout: 5 * time.Second, Token: authenticated.Token,
		})
		require.NoError(t, err)
		get, err := client.Get(ctx, "/ha/key")
		require.NoError(t, err, "issuer token must verify on %s", endpoint)
		require.Len(t, get.Kvs, 1)
		require.Equal(t, "before-failover", string(get.Kvs[0].Value))
		require.NoError(t, client.Close())
	}
}

func TestAuthTokenSurvivesLeaderFailover(t *testing.T) {
	raw := os.Getenv("KUBEBRAIN_AUTH_HA_ENDPOINTS")
	leaderPod := os.Getenv("KUBEBRAIN_AUTH_HA_LEADER_POD")
	namespace := os.Getenv("KUBEBRAIN_AUTH_HA_NAMESPACE")
	failoverCommand := os.Getenv("KUBEBRAIN_AUTH_HA_FAILOVER_COMMAND")
	if raw == "" || (failoverCommand == "" && (leaderPod == "" || namespace == "")) {
		t.Skip("set KUBEBRAIN_AUTH_HA_ENDPOINTS and either KUBEBRAIN_AUTH_HA_FAILOVER_COMMAND or the leader pod/namespace variables")
	}
	endpoints := strings.Split(raw, ",")
	require.GreaterOrEqual(t, len(endpoints), 2)
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	issuer := authClient(t, endpoints[0], "", "")
	authenticated, err := issuer.Authenticate(ctx, "root", "root-secret")
	require.NoError(t, err)

	output, err := runAuthHAFailoverCommand(t, ctx, failoverCommand, namespace, leaderPod)
	require.NoError(t, err, string(output))

	for _, endpoint := range endpoints {
		client, err := clientv3.New(clientv3.Config{
			Endpoints: []string{endpoint}, DialTimeout: 2 * time.Second, Token: authenticated.Token,
		})
		require.NoError(t, err)
		deadline := time.Now().Add(30 * time.Second)
		for {
			_, err = client.Put(ctx, "/ha/key", "after-failover")
			if err == nil || time.Now().After(deadline) {
				break
			}
			time.Sleep(500 * time.Millisecond)
		}
		require.NoError(t, err, "pre-failover token must remain usable on %s", endpoint)
		get, err := client.Get(ctx, "/ha/key")
		require.NoError(t, err)
		require.Len(t, get.Kvs, 1)
		require.Equal(t, "after-failover", string(get.Kvs[0].Value))
		require.NoError(t, client.Close())
	}
}

func runAuthHAFailoverCommand(t *testing.T, ctx context.Context, command, namespace, leaderPod string) ([]byte, error) {
	t.Helper()
	if command != "" {
		return runCompatShellCommandContext(t, ctx, command)
	}
	return runCompatKubectlContext(t, ctx, "-n", namespace, "delete", "pod", leaderPod, "--wait=false")
}

func TestAuthTokenSurvivesEnabledRollout(t *testing.T) {
	endpoint := os.Getenv("KUBEBRAIN_AUTH_HA_ROLLOUT_ENDPOINT")
	namespace := os.Getenv("KUBEBRAIN_AUTH_HA_NAMESPACE")
	if endpoint == "" || namespace == "" {
		t.Skip("set KUBEBRAIN_AUTH_HA_ROLLOUT_ENDPOINT and KUBEBRAIN_AUTH_HA_NAMESPACE")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	issuer := authClient(t, endpoint, "", "")
	authenticated, err := issuer.Authenticate(ctx, "root", "root-secret")
	require.NoError(t, err)

	output, err := runCompatKubectlContext(t, ctx, "-n", namespace, "rollout", "restart", "deployment/kubebrain-auth-ha")
	require.NoError(t, err, string(output))
	output, err = runCompatKubectlContext(t, ctx, "-n", namespace, "rollout", "status", "deployment/kubebrain-auth-ha", "--timeout=75s")
	require.NoError(t, err, string(output))

	client, err := clientv3.New(clientv3.Config{
		Endpoints: []string{endpoint}, DialTimeout: 3 * time.Second, Token: authenticated.Token,
	})
	require.NoError(t, err)
	defer client.Close()
	deadline := time.Now().Add(30 * time.Second)
	var get *clientv3.GetResponse
	for {
		get, err = client.Get(ctx, "/ha/key")
		if err == nil || time.Now().After(deadline) {
			break
		}
		time.Sleep(500 * time.Millisecond)
	}
	require.NoError(t, err, "pre-rollout token must verify after every replica restarts")
	require.Len(t, get.Kvs, 1)
	require.Equal(t, "after-failover", string(get.Kvs[0].Value))
	statusResponse, err := client.AuthStatus(ctx)
	require.NoError(t, err)
	require.True(t, statusResponse.Enabled)
}

func TestConcurrentAuthMutationsSurviveLeaderFailover(t *testing.T) {
	endpoint := os.Getenv("KUBEBRAIN_AUTH_HA_ROLLOUT_ENDPOINT")
	leaderPod := os.Getenv("KUBEBRAIN_AUTH_HA_LEADER_POD")
	namespace := os.Getenv("KUBEBRAIN_AUTH_HA_NAMESPACE")
	failoverCommand := os.Getenv("KUBEBRAIN_AUTH_HA_FAILOVER_COMMAND")
	if endpoint == "" || (failoverCommand == "" && (leaderPod == "" || namespace == "")) {
		t.Skip("set KUBEBRAIN_AUTH_HA_ROLLOUT_ENDPOINT and either KUBEBRAIN_AUTH_HA_FAILOVER_COMMAND or the leader pod/namespace variables")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	root := authClient(t, endpoint, "root", "root-secret")
	before, err := root.AuthStatus(ctx)
	require.NoError(t, err)

	const count = 32
	errs := make([]error, count)
	start := make(chan struct{})
	var ready sync.WaitGroup
	var done sync.WaitGroup
	ready.Add(count)
	done.Add(count)
	for i := 0; i < count; i++ {
		go func(index int) {
			defer done.Done()
			ready.Done()
			<-start
			// Spread attempts across the election window instead of letting every
			// request commit before kubectl has terminated the old leader.
			time.Sleep(time.Duration(index%8) * 25 * time.Millisecond)
			name := fmt.Sprintf("failover-%02d", index)
			for {
				_, operationErr := root.RoleAdd(ctx, name)
				if operationErr == nil || errors.Is(operationErr, rpctypes.ErrRoleAlreadyExist) {
					errs[index] = nil
					return
				}
				if ctx.Err() != nil {
					errs[index] = operationErr
					return
				}
				time.Sleep(200 * time.Millisecond)
			}
		}(i)
	}
	ready.Wait()
	close(start)
	output, err := runAuthHAFailoverCommand(t, ctx, failoverCommand, namespace, leaderPod)
	require.NoError(t, err, string(output))
	done.Wait()
	for _, operationErr := range errs {
		require.NoError(t, operationErr)
	}

	roles, err := root.RoleList(ctx)
	require.NoError(t, err)
	roleSet := make(map[string]struct{}, len(roles.Roles))
	for _, role := range roles.Roles {
		roleSet[role] = struct{}{}
	}
	for i := 0; i < count; i++ {
		_, found := roleSet[fmt.Sprintf("failover-%02d", i)]
		require.True(t, found)
	}
	after, err := root.AuthStatus(ctx)
	require.NoError(t, err)
	require.Equal(t, before.AuthRevision+count, after.AuthRevision)
}
