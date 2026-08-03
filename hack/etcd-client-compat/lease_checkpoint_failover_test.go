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

func TestLongLeaseFailoverUsesRemainingTTLCheckpoint(t *testing.T) {
	failoverCommand := os.Getenv("KUBEBRAIN_LEASE_CHECKPOINT_FAILOVER_COMMAND")
	if failoverCommand == "" {
		t.Skip("set KUBEBRAIN_LEASE_CHECKPOINT_FAILOVER_COMMAND to delete the current live leader")
	}
	namespace := os.Getenv("KUBEBRAIN_FAILOVER_NAMESPACE")
	if namespace == "" {
		namespace = "kubebrain-dev"
	}

	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{compatEndpoint(t)},
		DialTimeout: 3 * time.Second,
	})
	require.NoError(t, err)
	defer func() { require.NoError(t, cli.Close()) }()

	ctx, cancel := context.WithTimeout(context.Background(), 7*time.Minute)
	defer cancel()
	grant, err := cli.Grant(ctx, 600)
	require.NoError(t, err)
	key := fmt.Sprintf("/dbaas-lease-checkpoint-failover/%d", time.Now().UnixNano())
	_, err = cli.Put(ctx, key, "value", clientv3.WithLease(grant.ID))
	require.NoError(t, err)

	// etcd's default checkpoint interval is five minutes. Crossing below 285
	// seconds leaves enough margin for the scheduled checkpoint to commit before
	// the leader is removed.
	require.Eventually(t, func() bool {
		ttl, ttlErr := cli.TimeToLive(ctx, grant.ID)
		return ttlErr == nil && ttl.TTL > 0 && ttl.TTL <= 285
	}, 330*time.Second, time.Second)

	output, err := runCompatShellCommandContext(t, ctx, failoverCommand)
	require.NoErrorf(t, err, "failover command: %s", strings.TrimSpace(string(output)))
	output, err = waitForKubeBrainRollout(t, ctx, namespace)
	require.NoErrorf(t, err, "wait for KubeBrain recovery: %s", strings.TrimSpace(string(output)))

	var recovered *clientv3.LeaseTimeToLiveResponse
	require.Eventually(t, func() bool {
		recovered, err = cli.TimeToLive(ctx, grant.ID, clientv3.WithAttachedKeys())
		return err == nil && recovered.TTL > 0
	}, 30*time.Second, 500*time.Millisecond)
	require.LessOrEqual(t, recovered.TTL, int64(310),
		"replacement leader must recover checkpointed remaining TTL, not reset the full 600s grant")
	require.Equal(t, int64(600), recovered.GrantedTTL)
	require.Equal(t, [][]byte{[]byte(key)}, recovered.Keys)
	got, err := cli.Get(ctx, key)
	require.NoError(t, err)
	require.Len(t, got.Kvs, 1)

	_, err = cli.Revoke(ctx, grant.ID)
	require.NoError(t, err)
}
