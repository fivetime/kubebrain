package compat

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// TestIdleReplicaReplacementDoesNotAdvanceRevision is destructive and must use
// a disposable cluster. Restarting the serving layer without a user mutation
// must not manufacture an observable MVCC revision.
func TestIdleReplicaReplacementDoesNotAdvanceRevision(t *testing.T) {
	endpoint := os.Getenv("KUBEBRAIN_IDLE_RESTART_ENDPOINT")
	namespace := os.Getenv("KUBEBRAIN_IDLE_RESTART_NAMESPACE")
	pods := splitNonEmptyCSV(os.Getenv("KUBEBRAIN_IDLE_RESTART_PODS"))
	if endpoint == "" || namespace == "" || len(pods) == 0 {
		t.Skip("set KUBEBRAIN_IDLE_RESTART_ENDPOINT, KUBEBRAIN_IDLE_RESTART_NAMESPACE, and KUBEBRAIN_IDLE_RESTART_PODS")
	}
	require.Len(t, pods, 3)
	kubeContext := os.Getenv("KUBEBRAIN_IDLE_RESTART_CONTEXT")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	key := []byte(testPrefix(t) + "/idle-restart-revision")
	conn, err := grpc.NewClient(grpcTarget(endpoint), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	kv := etcdserverpb.NewKVClient(conn)
	put, err := kv.Put(ctx, &etcdserverpb.PutRequest{Key: key, Value: []byte("value")})
	require.NoError(t, err)
	before, err := kv.Range(ctx, &etcdserverpb.RangeRequest{Key: key})
	require.NoError(t, err)
	require.Len(t, before.Kvs, 1)
	require.Equal(t, put.GetHeader().GetRevision(), before.GetHeader().GetRevision())
	require.NoError(t, conn.Close())

	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cleanupCancel()
		cleanupConn, cleanupErr := grpc.NewClient(grpcTarget(endpoint),
			grpc.WithTransportCredentials(insecure.NewCredentials()))
		if cleanupErr != nil {
			return
		}
		defer cleanupConn.Close()
		_, _ = etcdserverpb.NewKVClient(cleanupConn).DeleteRange(
			cleanupCtx, &etcdserverpb.DeleteRangeRequest{Key: key},
		)
	})

	replaceAllCompatPods(t, ctx, kubeContext, namespace, endpoint, pods)

	afterConn, err := grpc.NewClient(grpcTarget(endpoint), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	defer afterConn.Close()
	afterKV := etcdserverpb.NewKVClient(afterConn)
	after, err := afterKV.Range(ctx, &etcdserverpb.RangeRequest{Key: key})
	require.NoError(t, err)
	require.Len(t, after.Kvs, 1)
	require.Equal(t, before.GetHeader().GetRevision(), after.GetHeader().GetRevision(),
		"a serving-layer restart without user writes must preserve the public MVCC revision")
	updated, err := afterKV.Put(ctx, &etcdserverpb.PutRequest{Key: key, Value: []byte("after-restart")})
	require.NoError(t, err)
	require.Equal(t, after.GetHeader().GetRevision()+1, updated.GetHeader().GetRevision(),
		"the election timestamp must not create a gap in the public MVCC sequence")
	readUpdated, err := afterKV.Range(ctx, &etcdserverpb.RangeRequest{Key: key})
	require.NoError(t, err)
	require.Len(t, readUpdated.Kvs, 1)
	require.Equal(t, []byte("after-restart"), readUpdated.Kvs[0].Value)
	require.Equal(t, updated.GetHeader().GetRevision(), readUpdated.GetHeader().GetRevision())
}

// TestLatestCompactionReplicaReplacementDoesNotAdvanceRevision extends the
// idle-restart invariant to compact == current. Compaction removes history; it
// is not an MVCC mutation and must not manufacture a new public revision on the
// first read from freshly started serving processes.
func TestLatestCompactionReplicaReplacementDoesNotAdvanceRevision(t *testing.T) {
	endpoint := os.Getenv("KUBEBRAIN_IDLE_RESTART_ENDPOINT")
	namespace := os.Getenv("KUBEBRAIN_IDLE_RESTART_NAMESPACE")
	pods := splitNonEmptyCSV(os.Getenv("KUBEBRAIN_IDLE_RESTART_PODS"))
	if endpoint == "" || namespace == "" || len(pods) == 0 {
		t.Skip("set KUBEBRAIN_IDLE_RESTART_ENDPOINT, KUBEBRAIN_IDLE_RESTART_NAMESPACE, and KUBEBRAIN_IDLE_RESTART_PODS")
	}
	require.Len(t, pods, 3)
	kubeContext := os.Getenv("KUBEBRAIN_IDLE_RESTART_CONTEXT")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	key := []byte(testPrefix(t) + "/latest-compaction-idle-restart")
	conn, err := grpc.NewClient(grpcTarget(endpoint), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	kv := etcdserverpb.NewKVClient(conn)
	put, err := kv.Put(ctx, &etcdserverpb.PutRequest{Key: key, Value: []byte("value")})
	require.NoError(t, err)
	_, err = kv.Compact(ctx, &etcdserverpb.CompactionRequest{Revision: put.GetHeader().GetRevision()})
	require.NoError(t, err)
	require.NoError(t, conn.Close())

	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cleanupCancel()
		cleanupConn, cleanupErr := grpc.NewClient(grpcTarget(endpoint), grpc.WithTransportCredentials(insecure.NewCredentials()))
		if cleanupErr != nil {
			return
		}
		defer cleanupConn.Close()
		_, _ = etcdserverpb.NewKVClient(cleanupConn).DeleteRange(cleanupCtx, &etcdserverpb.DeleteRangeRequest{Key: key})
	})

	replaceAllCompatPods(t, ctx, kubeContext, namespace, endpoint, pods)
	afterConn, err := grpc.NewClient(grpcTarget(endpoint), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	defer afterConn.Close()
	after, err := etcdserverpb.NewKVClient(afterConn).Range(ctx, &etcdserverpb.RangeRequest{Key: key})
	require.NoError(t, err)
	require.Len(t, after.Kvs, 1)
	require.Equal(t, put.GetHeader().GetRevision(), after.GetHeader().GetRevision(),
		"compaction at the latest revision plus an idle serving-layer restart must preserve the public MVCC revision")
	updated, err := etcdserverpb.NewKVClient(afterConn).Put(ctx, &etcdserverpb.PutRequest{Key: key, Value: []byte("after-restart")})
	require.NoError(t, err)
	require.Equal(t, after.GetHeader().GetRevision()+1, updated.GetHeader().GetRevision())
}

func replaceAllCompatPods(t *testing.T, ctx context.Context, kubeContext, namespace, endpoint string, pods []string) {
	t.Helper()
	oldUIDs := make(map[string]string, len(pods))
	for _, pod := range pods {
		oldUIDs[pod] = kubectlPodField(t, kubeContext, namespace, pod, "{.metadata.uid}")
	}
	deleteArgs := kubectlContextArgs(kubeContext, "-n", namespace, "delete", "pod")
	deleteArgs = append(deleteArgs, pods...)
	deleteArgs = append(deleteArgs, "--wait=true", "--timeout=90s")
	output, deleteErr := runCompatKubectlContext(t, ctx, deleteArgs...)
	require.NoErrorf(t, deleteErr, "replace all replicas: %s", strings.TrimSpace(string(output)))
	for _, pod := range pods {
		require.Eventually(t, func() bool {
			newUID := kubectlPodFieldNoFail(kubeContext, namespace, pod, "{.metadata.uid}")
			ready := kubectlPodFieldNoFail(kubeContext, namespace, pod, "{.status.containerStatuses[0].ready}")
			return newUID != "" && newUID != oldUIDs[pod] && ready == "true"
		}, 90*time.Second, 500*time.Millisecond, "%s replacement did not become Ready", pod)
	}
	requireEndpointReachable(t, grpcTarget(endpoint))
}

func TestReferenceEtcdIdleRestartPreservesRevision(t *testing.T) {
	binary := os.Getenv("REFERENCE_ETCD_BINARY")
	if binary == "" {
		t.Skip("set REFERENCE_ETCD_BINARY to run the restart oracle")
	}
	const endpoint = "127.0.0.1:42379"
	args := []string{
		"--name", "idle-restart-oracle",
		"--data-dir", t.TempDir(),
		"--listen-client-urls", "http://" + endpoint,
		"--advertise-client-urls", "http://" + endpoint,
		"--listen-peer-urls", "http://127.0.0.1:42380",
		"--initial-advertise-peer-urls", "http://127.0.0.1:42380",
		"--initial-cluster", "idle-restart-oracle=http://127.0.0.1:42380",
	}
	start := func() (func(), *grpc.ClientConn, etcdserverpb.KVClient) {
		t.Helper()
		stop := startCompatCommand(t, binary, args...)
		conn, err := grpc.NewClient(endpoint, grpc.WithTransportCredentials(insecure.NewCredentials()))
		require.NoError(t, err)
		kv := etcdserverpb.NewKVClient(conn)
		require.Eventually(t, func() bool {
			callCtx, cancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
			defer cancel()
			_, rangeErr := kv.Range(callCtx, &etcdserverpb.RangeRequest{Key: []byte("/a3434/health")})
			return rangeErr == nil
		}, 10*time.Second, 50*time.Millisecond)
		return stop, conn, kv
	}

	stop, conn, kv := start()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	key := []byte("/a3434/idle-restart")
	_, err := kv.Put(ctx, &etcdserverpb.PutRequest{Key: key, Value: []byte("value")})
	require.NoError(t, err)
	before, err := kv.Range(ctx, &etcdserverpb.RangeRequest{Key: key})
	require.NoError(t, err)
	require.NoError(t, conn.Close())
	stop()

	_, restartedConn, restartedKV := start()
	defer restartedConn.Close()
	after, err := restartedKV.Range(ctx, &etcdserverpb.RangeRequest{Key: key})
	require.NoError(t, err)
	require.Equal(t, before.GetHeader().GetRevision(), after.GetHeader().GetRevision())
	updated, err := restartedKV.Put(ctx, &etcdserverpb.PutRequest{Key: key, Value: []byte("after-restart")})
	require.NoError(t, err)
	require.Equal(t, after.GetHeader().GetRevision()+1, updated.GetHeader().GetRevision())
}

func TestReferenceEtcdLatestCompactionIdleRestartPreservesRevision(t *testing.T) {
	binary := os.Getenv("REFERENCE_ETCD_BINARY")
	if binary == "" {
		t.Skip("set REFERENCE_ETCD_BINARY to run the restart oracle")
	}
	const endpoint = "127.0.0.1:42379"
	args := []string{
		"--name", "latest-compaction-idle-restart-oracle",
		"--data-dir", t.TempDir(),
		"--listen-client-urls", "http://" + endpoint,
		"--advertise-client-urls", "http://" + endpoint,
		"--listen-peer-urls", "http://127.0.0.1:42380",
		"--initial-advertise-peer-urls", "http://127.0.0.1:42380",
		"--initial-cluster", "latest-compaction-idle-restart-oracle=http://127.0.0.1:42380",
	}
	start := func() (func(), *grpc.ClientConn, etcdserverpb.KVClient) {
		t.Helper()
		stop := startCompatCommand(t, binary, args...)
		conn, err := grpc.NewClient(endpoint, grpc.WithTransportCredentials(insecure.NewCredentials()))
		require.NoError(t, err)
		kv := etcdserverpb.NewKVClient(conn)
		require.Eventually(t, func() bool {
			callCtx, callCancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
			defer callCancel()
			_, rangeErr := kv.Range(callCtx, &etcdserverpb.RangeRequest{Key: []byte("/a3435/health")})
			return rangeErr == nil
		}, 10*time.Second, 50*time.Millisecond)
		return stop, conn, kv
	}

	stop, conn, kv := start()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	key := []byte("/a3435/latest-compaction-idle-restart")
	put, err := kv.Put(ctx, &etcdserverpb.PutRequest{Key: key, Value: []byte("value")})
	require.NoError(t, err)
	_, err = kv.Compact(ctx, &etcdserverpb.CompactionRequest{Revision: put.GetHeader().GetRevision()})
	require.NoError(t, err)
	require.NoError(t, conn.Close())
	stop()

	_, restartedConn, restartedKV := start()
	defer restartedConn.Close()
	after, err := restartedKV.Range(ctx, &etcdserverpb.RangeRequest{Key: key})
	require.NoError(t, err)
	require.Equal(t, put.GetHeader().GetRevision(), after.GetHeader().GetRevision())
	updated, err := restartedKV.Put(ctx, &etcdserverpb.PutRequest{Key: key, Value: []byte("after-restart")})
	require.NoError(t, err)
	require.Equal(t, after.GetHeader().GetRevision()+1, updated.GetHeader().GetRevision())
}
