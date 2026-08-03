package compat

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"
)

func TestRequireLeaderUnaryLeaseAndWatch(t *testing.T) {
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{compatEndpoint(t)},
		DialTimeout: 5 * time.Second,
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, cli.Close()) })

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	key := fmt.Sprintf("/dbaas-require-leader/%d", time.Now().UnixNano())
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		_, _ = cli.Delete(cleanupCtx, key)
	})

	requireLeaderCtx := clientv3.WithRequireLeader(ctx)
	_, err = cli.Get(requireLeaderCtx, key)
	require.NoError(t, err)
	lease, err := cli.Grant(requireLeaderCtx, 30)
	require.NoError(t, err)
	keepAlive, err := cli.KeepAliveOnce(requireLeaderCtx, lease.ID)
	require.NoError(t, err)
	require.Positive(t, keepAlive.TTL)

	watch := cli.Watch(requireLeaderCtx, key)
	_, err = cli.Put(ctx, key, "value", clientv3.WithLease(lease.ID))
	require.NoError(t, err)
	select {
	case response := <-watch:
		require.NoError(t, response.Err())
		require.Len(t, response.Events, 1)
		require.Equal(t, "value", string(response.Events[0].Kv.Value))
	case <-ctx.Done():
		t.Fatal("timed out waiting for require-leader watch event")
	}
}
