package compat

import (
	"context"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
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
	if raw == "" || leaderPod == "" || namespace == "" {
		t.Skip("set KUBEBRAIN_AUTH_HA_ENDPOINTS, KUBEBRAIN_AUTH_HA_LEADER_POD, and KUBEBRAIN_AUTH_HA_NAMESPACE")
	}
	endpoints := strings.Split(raw, ",")
	require.GreaterOrEqual(t, len(endpoints), 2)
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	issuer := authClient(t, endpoints[0], "", "")
	authenticated, err := issuer.Authenticate(ctx, "root", "root-secret")
	require.NoError(t, err)

	command := exec.CommandContext(ctx, "kubectl", "-n", namespace, "delete", "pod", leaderPod, "--wait=false")
	output, err := command.CombinedOutput()
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

	command := exec.CommandContext(ctx, "kubectl", "-n", namespace, "rollout", "restart", "deployment/kubebrain-auth-ha")
	output, err := command.CombinedOutput()
	require.NoError(t, err, string(output))
	command = exec.CommandContext(ctx, "kubectl", "-n", namespace, "rollout", "status", "deployment/kubebrain-auth-ha", "--timeout=75s")
	output, err = command.CombinedOutput()
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
