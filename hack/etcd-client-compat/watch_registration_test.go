package compat

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"
)

// TestFromNowWatchDoesNotLoseImmediatePostCreateWrite exercises the server-side
// handoff from the Created response to backend watch registration. Every write
// committed immediately after Created must be delivered on the same stream.
func TestFromNowWatchDoesNotLoseImmediatePostCreateWrite(t *testing.T) {
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{compatEndpoint()},
		DialTimeout: 3 * time.Second,
	})
	require.NoError(t, err)
	defer func() { require.NoError(t, cli.Close()) }()

	for i := 0; i < 50; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		key := fmt.Sprintf("/dbaas-watch-registration/%d/%d", time.Now().UnixNano(), i)
		watchCh := cli.Watch(ctx, key, clientv3.WithCreatedNotify())

		created := <-watchCh
		require.NoError(t, created.Err())
		require.True(t, created.Created)
		_, err = cli.Put(ctx, key, "immediate")
		require.NoError(t, err)

		event := <-watchCh
		require.NoError(t, event.Err())
		require.Len(t, event.Events, 1)
		require.Equal(t, key, string(event.Events[0].Kv.Key))
		require.Equal(t, "immediate", string(event.Events[0].Kv.Value))
		_, _ = cli.Delete(ctx, key)
		cancel()
	}
}
