package compat

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"
)

// TestKubeBrainColdRestartRequiresRecoveredPDQuorum proves the recovery
// boundary after every KubeBrain process starts with all PD endpoints absent:
// one reachable PD member must remain fail-closed, while two members may form a
// quorum and restore progress before the third endpoint is healed.
func TestKubeBrainColdRestartRequiresRecoveredPDQuorum(t *testing.T) {
	command := os.Getenv("KUBEBRAIN_PD_STAGED_RECOVERY_COMMAND")
	if command == "" {
		t.Skip("set KUBEBRAIN_PD_STAGED_RECOVERY_COMMAND to run destructive staged PD recovery")
	}
	endpoint := os.Getenv("KUBEBRAIN_ETCD_ENDPOINT")
	if endpoint == "" {
		t.Fatal("set KUBEBRAIN_ETCD_ENDPOINT explicitly for staged PD recovery")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 7*time.Minute)
	defer cancel()
	seedClient, err := clientv3.New(clientv3.Config{Endpoints: []string{endpoint}, DialTimeout: 5 * time.Second})
	require.NoError(t, err)
	prefix := testPrefix(t) + "/"
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cleanupCancel()
		_, _ = seedClient.Delete(cleanupCtx, prefix, clientv3.WithPrefix())
		require.NoError(t, seedClient.Close())
	})
	key := prefix + "baseline"
	seed, err := seedClient.Put(ctx, key, "before-blackout")
	require.NoError(t, err)
	require.Positive(t, seed.Header.Revision)
	oldUIDs := kubeBrainPodUIDs(t, ctx)
	require.Len(t, oldUIDs, 3)

	type commandResult struct {
		output []byte
		err    error
	}
	commandDone := make(chan commandResult, 1)
	go func() {
		output, commandErr := runCompatShellCommandContext(t, ctx, command)
		commandDone <- commandResult{output: output, err: commandErr}
	}()
	commandWaited := false
	defer func() {
		if commandWaited {
			return
		}
		result := <-commandDone
		require.NoErrorf(t, result.err, "staged PD recovery command cleanup: %s",
			strings.TrimSpace(string(result.output)))
	}()

	require.Eventually(t, func() bool {
		current := kubeBrainPodUIDs(t, ctx)
		if len(current) != 3 {
			return false
		}
		for _, uid := range current {
			for _, oldUID := range oldUIDs {
				if uid == oldUID {
					return false
				}
			}
		}
		return true
	}, 45*time.Second, 100*time.Millisecond, "all KubeBrain replicas must cold-start during PD total loss")

	require.Eventually(t, func() bool {
		return reachablePDCount(t, ctx) == 1
	}, 60*time.Second, 200*time.Millisecond, "staged recovery must expose exactly one PD member first")
	oneMemberClient, err := clientv3.New(clientv3.Config{Endpoints: []string{endpoint}, DialTimeout: time.Second})
	require.NoError(t, err)
	oneReadCtx, oneReadCancel := context.WithTimeout(ctx, time.Second)
	oneReadResponse, oneReadErr := oneMemberClient.Get(oneReadCtx, key)
	oneReadCancel()
	require.Nil(t, oneReadResponse, "one reachable PD member must not permit a linearizable read")
	require.Error(t, oneReadErr)
	oneWriteCtx, oneWriteCancel := context.WithTimeout(ctx, time.Second)
	oneWriteResponse, oneWriteErr := oneMemberClient.Put(oneWriteCtx, prefix+"one-member", "must-not-commit")
	oneWriteCancel()
	require.Nil(t, oneWriteResponse, "one reachable PD member must not acknowledge a write")
	require.Error(t, oneWriteErr)
	require.NoError(t, oneMemberClient.Close())
	require.Equal(t, 1, reachablePDCount(t, ctx), "single-member oracle must finish before quorum recovery")

	require.Eventually(t, func() bool {
		return reachablePDCount(t, ctx) == 2
	}, 60*time.Second, 200*time.Millisecond, "staged recovery must next expose exactly two PD members")
	quorumClient, err := clientv3.New(clientv3.Config{Endpoints: []string{endpoint}, DialTimeout: 5 * time.Second})
	require.NoError(t, err)
	defer func() { require.NoError(t, quorumClient.Close()) }()
	require.Eventually(t, func() bool {
		if reachablePDCount(t, ctx) != 2 {
			return false
		}
		callCtx, callCancel := context.WithTimeout(ctx, 3*time.Second)
		defer callCancel()
		response, getErr := quorumClient.Get(callCtx, key)
		return getErr == nil && len(response.Kvs) == 1 && string(response.Kvs[0].Value) == "before-blackout"
	}, 100*time.Second, 200*time.Millisecond, "two PD members must restore the committed baseline before the third heals")
	require.Equal(t, 2, reachablePDCount(t, ctx), "baseline recovery must precede third-member healing")
	writeCtx, writeCancel := context.WithTimeout(ctx, 5*time.Second)
	writeResponse, writeErr := quorumClient.Put(writeCtx, prefix+"quorum", "committed")
	writeCancel()
	require.NoError(t, writeErr)
	require.NotNil(t, writeResponse)
	require.Equal(t, 2, reachablePDCount(t, ctx), "quorum write must precede third-member healing")

	result := <-commandDone
	commandWaited = true
	require.NoErrorf(t, result.err, "staged PD recovery command: %s", strings.TrimSpace(string(result.output)))
	t.Logf("staged PD recovery command: %s", strings.TrimSpace(string(result.output)))
	require.Eventually(t, func() bool { return reachablePDCount(t, ctx) == 3 },
		30*time.Second, 200*time.Millisecond, "all PD endpoints must recover")
}

func reachablePDCount(t *testing.T, ctx context.Context) int {
	t.Helper()
	node := os.Getenv("KIND_NODE_CONTAINER")
	if node == "" {
		node = "kubebrain-dev-control-plane"
	}
	namespace := os.Getenv("TIDB_NAMESPACE")
	if namespace == "" {
		namespace = "tidb-cluster"
	}
	cluster := os.Getenv("TIDB_CLUSTER")
	if cluster == "" {
		cluster = "kb"
	}
	safeToken := regexp.MustCompile(`^[a-zA-Z0-9][a-zA-Z0-9_.-]*$`)
	require.Regexp(t, safeToken, node)
	require.Regexp(t, safeToken, namespace)
	require.Regexp(t, safeToken, cluster)
	output, err := runCompatShellCommandContext(t, ctx,
		fmt.Sprintf(`kubectl -n %s get pods -l app.kubernetes.io/instance=%s,app.kubernetes.io/component=pd -o json`,
			namespace, cluster))
	require.NoErrorf(t, err, "list PD endpoints: %s", strings.TrimSpace(string(output)))
	var pods struct {
		Items []struct {
			Status struct {
				PodIP string `json:"podIP"`
			} `json:"status"`
		} `json:"items"`
	}
	require.NoError(t, json.Unmarshal(output, &pods))
	require.Len(t, pods.Items, 3)
	count := 0
	for _, pod := range pods.Items {
		require.Regexp(t, regexp.MustCompile(`^([0-9]{1,3}\.){3}[0-9]{1,3}$`), pod.Status.PodIP)
		// Transport reachability is intentionally independent of the HTTP
		// status: an isolated PD member can answer /health with an unhealthy
		// status while still proving that its network partition was removed.
		probe := fmt.Sprintf("docker exec %s curl --silent --show-error --output /dev/null --max-time 1 http://%s:2379/health", node, pod.Status.PodIP)
		if _, probeErr := runCompatShellCommandContext(t, ctx, probe); probeErr == nil {
			count++
		}
	}
	return count
}
