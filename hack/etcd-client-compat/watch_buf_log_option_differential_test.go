package compat

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
)

// TestWatchBufLogOptionPreservesWatchDelivery pins upstream etcd 312d7262d.
// WithWatchBufLog is a public clientv3 watch option; enabling it must preserve
// normal watch creation and event delivery against an etcd-compatible endpoint.
func TestWatchBufLogOptionPreservesWatchDelivery(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	client, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{compatEndpoint(t)},
		DialTimeout: 5 * time.Second,
		Logger:      zap.NewNop(),
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, client.Close()) })

	key := fmt.Sprintf("/a3764-watch-buf-log/%d", time.Now().UnixNano())
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		_, _ = client.Delete(cleanupCtx, key)
	})

	watch := client.Watch(ctx, key, clientv3.WithCreatedNotify(), clientv3.WithWatchBufLog())
	created := recvClientWatchResponse(t, ctx, watch)
	require.True(t, created.Created)
	require.False(t, created.Canceled)
	require.NotNil(t, created.Header)
	require.Empty(t, created.Events)

	put, err := client.Put(ctx, key, "value")
	require.NoError(t, err)

	event := recvClientWatchResponse(t, ctx, watch)
	require.False(t, event.Created)
	require.False(t, event.Canceled)
	require.NotNil(t, event.Header)
	require.Equal(t, put.Header.Revision, event.Header.Revision)
	require.Len(t, event.Events, 1)
	require.Equal(t, key, string(event.Events[0].Kv.Key))
	require.Equal(t, "value", string(event.Events[0].Kv.Value))
}

func recvClientWatchResponse(
	t *testing.T,
	ctx context.Context,
	watch clientv3.WatchChan,
) clientv3.WatchResponse {
	t.Helper()
	select {
	case response, ok := <-watch:
		require.True(t, ok)
		require.NoError(t, response.Err())
		return response
	case <-ctx.Done():
		t.Fatalf("timed out waiting for watch response: %v", ctx.Err())
		return clientv3.WatchResponse{}
	}
}
