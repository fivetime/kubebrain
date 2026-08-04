package compat

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"
)

// TestRangeCountOnlyTakesPrecedenceOverKeysOnly mirrors upstream etcd
// TestKVGetKeysOnlyWithCountOnly. CountOnly must suppress the KV payload even
// when KeysOnly is also requested.
func TestRangeCountOnlyTakesPrecedenceOverKeysOnly(t *testing.T) {
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{compatEndpoint(t)},
		DialTimeout: 3 * time.Second,
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, cli.Close()) })
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	prefix := fmt.Sprintf("/dbaas-range-keys-count/%d/", time.Now().UnixNano())
	registerPrefixCleanup(t, cli, prefix)
	for _, key := range []string{"a", "b", "c"} {
		_, err = cli.Put(ctx, prefix+key, "")
		require.NoError(t, err)
	}

	response, err := cli.Get(
		ctx,
		prefix,
		clientv3.WithPrefix(),
		clientv3.WithKeysOnly(),
		clientv3.WithCountOnly(),
	)
	require.NoError(t, err)
	require.Equal(t, int64(3), response.Count)
	require.Empty(t, response.Kvs)
	require.False(t, response.More)
}
