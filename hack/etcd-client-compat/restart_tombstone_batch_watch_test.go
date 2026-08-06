package compat

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"
)

// TestReplicatedRestartPreservesCompactBoundaryTombstoneBatch verifies the
// multi-subrevision form of upstream TestResumeCompactionOnTombstone. A single
// prefix DeleteRange writes several tombstones at one main revision; physical
// compaction and complete serving-process replacement must retain every event
// in deterministic key order at that boundary.
func TestReplicatedRestartPreservesCompactBoundaryTombstoneBatch(t *testing.T) {
	kubeContext := os.Getenv("KUBEBRAIN_RESTART_CONTEXT")
	servingNamespace := os.Getenv("KUBEBRAIN_RESTART_NAMESPACE")
	servingPods := splitNonEmptyCSV(os.Getenv("KUBEBRAIN_RESTART_PODS"))
	if kubeContext == "" || servingNamespace == "" || len(servingPods) == 0 {
		t.Skip("set the explicit KUBEBRAIN_RESTART_CONTEXT, namespace, and serving Pod list")
	}
	require.Len(t, servingPods, 3)
	endpoint := os.Getenv("KUBEBRAIN_ETCD_ENDPOINT")
	if endpoint == "" {
		t.Fatal("set KUBEBRAIN_ETCD_ENDPOINT explicitly for replicated restart persistence")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{endpoint},
		DialTimeout: 5 * time.Second,
	})
	require.NoError(t, err)
	prefix := testPrefix(t) + "/batch/"
	keys := []string{prefix + "a", prefix + "b", prefix + "c"}
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cleanupCancel()
		_, _ = cli.Delete(cleanupCtx, prefix, clientv3.WithPrefix())
		require.NoError(t, cli.Close())
	})

	seedOps := make([]clientv3.Op, 0, len(keys))
	for index, key := range keys {
		seedOps = append(seedOps, clientv3.OpPut(key, fmt.Sprintf("value-%d", index)))
	}
	seed, err := cli.Txn(ctx).Then(seedOps...).Commit()
	require.NoError(t, err)
	require.True(t, seed.Succeeded)

	deleted, err := cli.Delete(ctx, prefix, clientv3.WithPrefix())
	require.NoError(t, err)
	require.Equal(t, int64(len(keys)), deleted.Deleted)
	require.Equal(t, seed.Header.Revision+1, deleted.Header.Revision)
	_, err = cli.Compact(ctx, deleted.Header.Revision, clientv3.WithCompactPhysical())
	require.NoError(t, err)

	for _, pod := range servingPods {
		replaceCompatPod(t, ctx, kubeContext, servingNamespace, pod)
	}
	requireEndpointReachable(t, grpcTarget(endpoint))

	watchCtx, watchCancel := context.WithTimeout(ctx, 15*time.Second)
	defer watchCancel()
	watch := cli.Watch(
		watchCtx, prefix,
		clientv3.WithPrefix(), clientv3.WithRev(deleted.Header.Revision), clientv3.WithPrevKV(),
	)
	events := make([]*clientv3.Event, 0, len(keys))
	for len(events) < len(keys) {
		select {
		case response, ok := <-watch:
			require.True(t, ok, "watch channel closed after %d/%d tombstones", len(events), len(keys))
			require.NoError(t, response.Err())
			require.Equal(t, deleted.Header.Revision, response.Header.Revision)
			events = append(events, response.Events...)
		case <-watchCtx.Done():
			t.Fatalf("timed out replaying compact-boundary tombstone batch after serving restart: got %d/%d: %v", len(events), len(keys), watchCtx.Err())
		}
	}
	require.Len(t, events, len(keys))
	for index, event := range events {
		require.Equal(t, clientv3.EventTypeDelete, event.Type)
		require.Equal(t, keys[index], string(event.Kv.Key))
		require.Equal(t, deleted.Header.Revision, event.Kv.ModRevision)
		require.Nil(t, event.PrevKv, "pre-delete value %q is below the compact watermark", keys[index])
	}

	current, err := cli.Get(ctx, prefix, clientv3.WithPrefix())
	require.NoError(t, err)
	require.Empty(t, current.Kvs)
}
