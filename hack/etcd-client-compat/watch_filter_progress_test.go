package compat

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"
)

// TestFilteredWatchProgressCoversSuppressedWrites verifies that a watch advances
// through writes removed by its server-side filters. The filtered PUT must not
// appear as an event, but an immediate progress request must cover its revision.
func TestFilteredWatchProgressCoversSuppressedWrites(t *testing.T) {
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{compatEndpoint()},
		DialTimeout: 3 * time.Second,
	})
	require.NoError(t, err)
	defer func() { require.NoError(t, cli.Close()) }()

	for i := 0; i < 25; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		key := fmt.Sprintf("/dbaas-watch-filter/%d/%d", time.Now().UnixNano(), i)
		watchCh := cli.Watch(ctx, key,
			clientv3.WithCreatedNotify(),
			clientv3.WithFilterPut(),
		)

		created := <-watchCh
		require.NoError(t, created.Err())
		require.True(t, created.Created)
		put, err := cli.Put(ctx, key, "filtered")
		require.NoError(t, err)
		require.NoError(t, cli.RequestProgress(ctx))

		for {
			response, ok := <-watchCh
			require.True(t, ok, "watch closed before covering filtered revision %d", put.Header.Revision)
			require.NoError(t, response.Err())
			require.Empty(t, response.Events, "NOPUT watch must suppress the PUT")
			require.NotNil(t, response.Header)
			if response.Header.Revision >= put.Header.Revision {
				break
			}
		}
		_, _ = cli.Delete(ctx, key)
		cancel()
	}
}
