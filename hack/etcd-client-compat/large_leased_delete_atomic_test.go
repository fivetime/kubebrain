package compat

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
)

func TestLargeLeasedDeleteRangeUsesOneRevision(t *testing.T) {
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{compatEndpoint()},
		DialTimeout: 3 * time.Second,
	})
	require.NoError(t, err)
	defer func() { require.NoError(t, cli.Close()) }()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	const keyCount = 129
	prefix := fmt.Sprintf("/dbaas-large-leased-delete/%d/", time.Now().UnixNano())
	grant, err := cli.Grant(ctx, 30)
	require.NoError(t, err)
	for i := 0; i < keyCount; i++ {
		_, err = cli.Put(ctx, fmt.Sprintf("%s%03d", prefix, i), "value", clientv3.WithLease(grant.ID))
		require.NoError(t, err)
	}
	before, err := cli.Get(ctx, prefix, clientv3.WithPrefix())
	require.NoError(t, err)
	require.Len(t, before.Kvs, keyCount)

	watchCtx, watchCancel := context.WithCancel(ctx)
	defer watchCancel()
	watch := cli.Watch(
		watchCtx,
		prefix,
		clientv3.WithPrefix(),
		clientv3.WithRev(before.Header.Revision+1),
		clientv3.WithPrevKV(),
	)
	deleted, err := cli.Delete(ctx, prefix, clientv3.WithPrefix(), clientv3.WithPrevKV())
	require.NoError(t, err)
	require.Equal(t, int64(keyCount), deleted.Deleted)
	require.Len(t, deleted.PrevKvs, keyCount)
	require.Equal(t, before.Header.Revision+1, deleted.Header.Revision)

	events := make([]*clientv3.Event, 0, keyCount)
	for len(events) < keyCount {
		select {
		case response, ok := <-watch:
			require.True(t, ok, "watch closed after %d/%d delete events", len(events), keyCount)
			require.NoError(t, response.Err())
			for _, event := range response.Events {
				require.Equal(t, mvccpb.DELETE, event.Type)
				require.Equal(t, deleted.Header.Revision, event.Kv.ModRevision)
				require.NotNil(t, event.PrevKv)
				require.Equal(t, int64(grant.ID), event.PrevKv.Lease)
				events = append(events, event)
			}
		case <-ctx.Done():
			t.Fatalf("received %d/%d delete events: %v", len(events), keyCount, ctx.Err())
		}
	}
	require.Len(t, events, keyCount)

	after, err := cli.Get(ctx, prefix, clientv3.WithPrefix())
	require.NoError(t, err)
	require.Empty(t, after.Kvs)
	ttl, err := cli.TimeToLive(ctx, grant.ID, clientv3.WithAttachedKeys())
	require.NoError(t, err)
	require.Empty(t, ttl.Keys)
	_, err = cli.Revoke(ctx, grant.ID)
	require.NoError(t, err)
}
