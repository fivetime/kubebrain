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

func TestLeaseRevokeDeletesAttachedKeysAtOneRevision(t *testing.T) {
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{compatEndpoint()},
		DialTimeout: 3 * time.Second,
	})
	require.NoError(t, err)
	defer func() { require.NoError(t, cli.Close()) }()

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	prefix := fmt.Sprintf("/dbaas-lease-revoke-atomic/%d/", time.Now().UnixNano())
	grant, err := cli.Grant(ctx, 30)
	require.NoError(t, err)
	_, err = cli.Put(ctx, prefix+"b", "value-b", clientv3.WithLease(grant.ID))
	require.NoError(t, err)
	lastPut, err := cli.Put(ctx, prefix+"a", "value-a", clientv3.WithLease(grant.ID))
	require.NoError(t, err)

	watchCtx, watchCancel := context.WithCancel(ctx)
	defer watchCancel()
	watch := cli.Watch(
		watchCtx,
		prefix,
		clientv3.WithPrefix(),
		clientv3.WithRev(lastPut.Header.Revision+1),
		clientv3.WithPrevKV(),
	)
	revoke, err := cli.Revoke(ctx, grant.ID)
	require.NoError(t, err)

	var events []*clientv3.Event
	for len(events) < 2 {
		select {
		case response, ok := <-watch:
			require.True(t, ok, "watch closed before both revoke deletes")
			require.NoError(t, response.Err())
			require.Equal(t, revoke.Header.Revision, response.Header.Revision)
			events = append(events, response.Events...)
		case <-ctx.Done():
			t.Fatalf("revoke delete events not received: %v", ctx.Err())
		}
	}
	require.Len(t, events, 2)
	for _, event := range events {
		require.Equal(t, mvccpb.DELETE, event.Type)
		require.Equal(t, revoke.Header.Revision, event.Kv.ModRevision)
		require.NotNil(t, event.PrevKv)
		require.Equal(t, int64(grant.ID), event.PrevKv.Lease)
	}
	require.Equal(t, prefix+"a", string(events[0].Kv.Key))
	require.Equal(t, prefix+"b", string(events[1].Kv.Key))

	got, err := cli.Get(ctx, prefix, clientv3.WithPrefix())
	require.NoError(t, err)
	require.Empty(t, got.Kvs)
	ttl, err := cli.TimeToLive(ctx, grant.ID, clientv3.WithAttachedKeys())
	require.NoError(t, err)
	require.Equal(t, int64(-1), ttl.TTL)
	require.Empty(t, ttl.Keys)
}
