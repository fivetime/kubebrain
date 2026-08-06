package compat

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
)

type restoreDeleteSnapshot struct {
	revision int64
	kvs      []*mvccpb.KeyValue
}

// TestReplicatedRestartPreservesInterleavedRestoreDeleteHistory is the
// black-box, multi-key counterpart of upstream TestRestoreDelete. It verifies
// current state and every distinct historical snapshot after all serving
// processes have been replaced.
func TestReplicatedRestartPreservesInterleavedRestoreDeleteHistory(t *testing.T) {
	endpoint := os.Getenv("KUBEBRAIN_ETCD_ENDPOINT")
	kubeContext := os.Getenv("KUBEBRAIN_RESTART_CONTEXT")
	namespace := os.Getenv("KUBEBRAIN_RESTART_NAMESPACE")
	pods := splitNonEmptyCSV(os.Getenv("KUBEBRAIN_RESTART_PODS"))
	if endpoint == "" || kubeContext == "" || namespace == "" || len(pods) == 0 {
		t.Skip("set KUBEBRAIN_ETCD_ENDPOINT and the explicit KUBEBRAIN_RESTART_* serving topology")
	}
	require.Len(t, pods, 3)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	runRestoreDeleteModel(t, ctx, endpoint, testPrefix(t)+"/restore-delete/", func() {
		for _, pod := range pods {
			replaceCompatPod(t, ctx, kubeContext, namespace, pod)
		}
		requireEndpointReachable(t, grpcTarget(endpoint))
	})
}

func TestReferenceEtcdRestartPreservesInterleavedRestoreDeleteHistory(t *testing.T) {
	binary := os.Getenv("REFERENCE_ETCD_BINARY")
	if binary == "" {
		t.Skip("set REFERENCE_ETCD_BINARY to run the restore/delete restart oracle")
	}
	const endpoint = "127.0.0.1:49379"
	args := []string{
		"--name", "restore-delete-model-oracle",
		"--data-dir", t.TempDir(),
		"--listen-client-urls", "http://" + endpoint,
		"--advertise-client-urls", "http://" + endpoint,
		"--listen-peer-urls", "http://127.0.0.1:49380",
		"--initial-advertise-peer-urls", "http://127.0.0.1:49380",
		"--initial-cluster", "restore-delete-model-oracle=http://127.0.0.1:49380",
		"--log-level", "error",
	}
	start := func() func() {
		stop := startCompatCommand(t, binary, args...)
		require.Eventually(t, func() bool {
			probeCtx, probeCancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
			defer probeCancel()
			cli, err := clientv3.New(clientv3.Config{Endpoints: []string{endpoint}, DialTimeout: 100 * time.Millisecond})
			if err != nil {
				return false
			}
			defer cli.Close()
			_, err = cli.Get(probeCtx, "/a3735/health")
			return err == nil
		}, 10*time.Second, 50*time.Millisecond)
		return stop
	}
	stop := start()
	t.Cleanup(func() { stop() })
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	runRestoreDeleteModel(t, ctx, endpoint, "/a3735/restore-delete/", func() {
		stop()
		stop = start()
	})
}

func runRestoreDeleteModel(t *testing.T, ctx context.Context, endpoint, prefix string, restart func()) {
	t.Helper()
	cli, err := clientv3.New(clientv3.Config{Endpoints: []string{endpoint}, DialTimeout: 5 * time.Second})
	require.NoError(t, err)
	defer cli.Close()
	defer func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cleanupCancel()
		_, _ = cli.Delete(cleanupCtx, prefix, clientv3.WithPrefix())
	}()

	var revisions []int64
	recordRevision := func(revision int64) {
		if len(revisions) == 0 || revisions[len(revisions)-1] != revision {
			revisions = append(revisions, revision)
		}
	}
	for index := 0; index < 20; index++ {
		key := fmt.Sprintf("%skey-%02d", prefix, index)
		put, putErr := cli.Put(ctx, key, fmt.Sprintf("create-%02d", index))
		require.NoError(t, putErr)
		recordRevision(put.Header.Revision)
		switch index % 3 {
		case 0:
			updateIndex := (index * 7) % (index + 1)
			update, updateErr := cli.Put(ctx,
				fmt.Sprintf("%skey-%02d", prefix, updateIndex),
				fmt.Sprintf("update-%02d-at-%02d", updateIndex, index),
			)
			require.NoError(t, updateErr)
			recordRevision(update.Header.Revision)
		case 1:
			deleteIndex := (index * 5) % (index + 1)
			deleted, deleteErr := cli.Delete(ctx, fmt.Sprintf("%skey-%02d", prefix, deleteIndex))
			require.NoError(t, deleteErr)
			recordRevision(deleted.Header.Revision)
		}
	}

	snapshots := make([]restoreDeleteSnapshot, 0, len(revisions))
	for _, revision := range revisions {
		response, rangeErr := cli.Get(ctx, prefix, clientv3.WithPrefix(), clientv3.WithRev(revision))
		require.NoError(t, rangeErr)
		kvs := make([]*mvccpb.KeyValue, len(response.Kvs))
		for index, kv := range response.Kvs {
			kvs[index] = &mvccpb.KeyValue{
				Key:            append([]byte(nil), kv.Key...),
				CreateRevision: kv.CreateRevision,
				ModRevision:    kv.ModRevision,
				Value:          append([]byte(nil), kv.Value...),
				Version:        kv.Version,
				Lease:          kv.Lease,
			}
		}
		snapshots = append(snapshots, restoreDeleteSnapshot{revision: revision, kvs: kvs})
	}
	currentBefore, err := cli.Get(ctx, prefix, clientv3.WithPrefix())
	require.NoError(t, err)
	require.NotEmpty(t, currentBefore.Kvs)

	restart()
	for _, snapshot := range snapshots {
		response, rangeErr := cli.Get(ctx, prefix, clientv3.WithPrefix(), clientv3.WithRev(snapshot.revision))
		require.NoErrorf(t, rangeErr, "historical range at revision %d after restart", snapshot.revision)
		require.Equal(t, snapshot.kvs, response.Kvs, "historical snapshot at revision %d changed across restart", snapshot.revision)
	}
	currentAfter, err := cli.Get(ctx, prefix, clientv3.WithPrefix())
	require.NoError(t, err)
	require.Equal(t, currentBefore.Kvs, currentAfter.Kvs)
}
