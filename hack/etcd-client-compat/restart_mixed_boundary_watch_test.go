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

// TestReplicatedRestartPreservesMixedCompactBoundary verifies that restore
// retains every subrevision when one transaction mixes a range deletion with
// a non-overlapping put. Physical compaction keeps the whole boundary revision,
// including both tombstones and the new live generation.
func TestReplicatedRestartPreservesMixedCompactBoundary(t *testing.T) {
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
	prefix := testPrefix(t) + "/mixed/"
	deadPrefix := prefix + "dead/"
	deadKeys := []string{deadPrefix + "a", deadPrefix + "b", deadPrefix + "c"}
	liveKey := prefix + "live/d"
	liveValue := "live-value"
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cleanupCancel()
		_, _ = cli.Delete(cleanupCtx, prefix, clientv3.WithPrefix())
		require.NoError(t, cli.Close())
	})

	seedOps := make([]clientv3.Op, 0, len(deadKeys))
	for index, key := range deadKeys {
		seedOps = append(seedOps, clientv3.OpPut(key, fmt.Sprintf("dead-value-%d", index)))
	}
	seed, err := cli.Txn(ctx).Then(seedOps...).Commit()
	require.NoError(t, err)
	require.True(t, seed.Succeeded)

	mixed, err := cli.Txn(ctx).Then(
		clientv3.OpDelete(deadPrefix, clientv3.WithPrefix()),
		clientv3.OpPut(liveKey, liveValue),
	).Commit()
	require.NoError(t, err)
	require.True(t, mixed.Succeeded)
	require.Len(t, mixed.Responses, 2)
	deleteResponse := mixed.Responses[0].GetResponseDeleteRange()
	require.NotNil(t, deleteResponse)
	require.Equal(t, int64(len(deadKeys)), deleteResponse.Deleted)
	require.NotNil(t, mixed.Responses[1].GetResponsePut())
	require.Equal(t, seed.Header.Revision+1, mixed.Header.Revision)
	_, err = cli.Compact(ctx, mixed.Header.Revision, clientv3.WithCompactPhysical())
	require.NoError(t, err)

	for _, pod := range servingPods {
		replaceCompatPod(t, ctx, kubeContext, servingNamespace, pod)
	}
	requireEndpointReachable(t, grpcTarget(endpoint))

	watchCtx, watchCancel := context.WithTimeout(ctx, 15*time.Second)
	defer watchCancel()
	watch := cli.Watch(
		watchCtx, prefix,
		clientv3.WithPrefix(), clientv3.WithRev(mixed.Header.Revision), clientv3.WithPrevKV(),
	)
	events := make([]*clientv3.Event, 0, len(deadKeys)+1)
	for len(events) < len(deadKeys)+1 {
		select {
		case response, ok := <-watch:
			require.True(t, ok, "watch channel closed after %d/%d events", len(events), len(deadKeys)+1)
			require.NoError(t, response.Err())
			require.Equal(t, mixed.Header.Revision, response.Header.Revision)
			events = append(events, response.Events...)
		case <-watchCtx.Done():
			t.Fatalf("timed out replaying mixed compact boundary after serving restart: got %d/%d: %v", len(events), len(deadKeys)+1, watchCtx.Err())
		}
	}
	require.Len(t, events, len(deadKeys)+1)
	for index, key := range deadKeys {
		event := events[index]
		require.Equal(t, clientv3.EventTypeDelete, event.Type)
		require.Equal(t, key, string(event.Kv.Key))
		require.Equal(t, mixed.Header.Revision, event.Kv.ModRevision)
		require.Nil(t, event.PrevKv, "pre-delete value %q is below the compact watermark", key)
	}
	putEvent := events[len(deadKeys)]
	require.Equal(t, clientv3.EventTypePut, putEvent.Type)
	require.Equal(t, liveKey, string(putEvent.Kv.Key))
	require.Equal(t, liveValue, string(putEvent.Kv.Value))
	require.Equal(t, mixed.Header.Revision, putEvent.Kv.CreateRevision)
	require.Equal(t, mixed.Header.Revision, putEvent.Kv.ModRevision)
	require.Equal(t, int64(1), putEvent.Kv.Version)
	require.Nil(t, putEvent.PrevKv)

	current, err := cli.Get(ctx, prefix, clientv3.WithPrefix())
	require.NoError(t, err)
	require.Len(t, current.Kvs, 1)
	require.Equal(t, liveKey, string(current.Kvs[0].Key))
	require.Equal(t, liveValue, string(current.Kvs[0].Value))
}
