package compat

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"
)

// TestReplicatedRestartPreservesCompactBoundaryTombstone is opt-in because it
// sequentially replaces every KubeBrain Pod in the external test cluster. It
// mirrors upstream TestResumeCompactionOnTombstone: after physical compaction
// removes every pre-delete version, the boundary tombstone must remain
// replayable after every serving process has reconstructed its state.
func TestReplicatedRestartPreservesCompactBoundaryTombstone(t *testing.T) {
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
	prefix := testPrefix(t) + "/"
	key := prefix + "only-tombstone"
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cleanupCancel()
		_, _ = cli.Delete(cleanupCtx, prefix, clientv3.WithPrefix())
		require.NoError(t, cli.Close())
	})

	put, err := cli.Put(ctx, key, "deleted-before-compact")
	require.NoError(t, err)
	deleted, err := cli.Delete(ctx, key)
	require.NoError(t, err)
	require.Equal(t, int64(1), deleted.Deleted)
	require.Equal(t, put.Header.Revision+1, deleted.Header.Revision)
	_, err = cli.Compact(ctx, deleted.Header.Revision, clientv3.WithCompactPhysical())
	require.NoError(t, err)

	for _, pod := range servingPods {
		replaceCompatPod(t, ctx, kubeContext, servingNamespace, pod)
	}
	requireEndpointReachable(t, grpcTarget(endpoint))

	watchCtx, watchCancel := context.WithTimeout(ctx, 15*time.Second)
	defer watchCancel()
	watch := cli.Watch(watchCtx, key, clientv3.WithRev(deleted.Header.Revision), clientv3.WithPrevKV())
	select {
	case response, ok := <-watch:
		require.True(t, ok, "watch channel closed before compact-boundary tombstone replay")
		require.NoError(t, response.Err())
		require.Len(t, response.Events, 1)
		event := response.Events[0]
		require.Equal(t, clientv3.EventTypeDelete, event.Type)
		require.Equal(t, key, string(event.Kv.Key))
		require.Equal(t, deleted.Header.Revision, event.Kv.ModRevision)
		require.Nil(t, event.PrevKv, "the pre-delete value is below the compact watermark")
	case <-watchCtx.Done():
		t.Fatalf("timed out replaying compact-boundary tombstone after serving restart: %v", watchCtx.Err())
	}
}
