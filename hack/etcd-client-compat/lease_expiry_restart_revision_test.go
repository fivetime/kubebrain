package compat

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// TestLeaseExpiryReplicaReplacementPreservesRevision is destructive and must
// target a disposable three-replica data plane. Lease expiry is a user MVCC
// delete, so both its tombstone and its exact public revision must survive the
// loss of every serving process.
func TestLeaseExpiryReplicaReplacementPreservesRevision(t *testing.T) {
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

	key := []byte(testPrefix(t) + "/lease-expiry-restart")
	conn, err := grpc.NewClient(grpcTarget(endpoint), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	kv := etcdserverpb.NewKVClient(conn)
	lease := etcdserverpb.NewLeaseClient(conn)
	grant, err := lease.LeaseGrant(ctx, &etcdserverpb.LeaseGrantRequest{TTL: 2})
	require.NoError(t, err)
	put, err := kv.Put(ctx, &etcdserverpb.PutRequest{Key: key, Value: []byte("leased"), Lease: grant.ID})
	require.NoError(t, err)

	var expiredRevision int64
	require.Eventually(t, func() bool {
		response, rangeErr := kv.Range(ctx, &etcdserverpb.RangeRequest{Key: key})
		if rangeErr != nil || len(response.Kvs) != 0 || response.GetHeader().GetRevision() <= put.GetHeader().GetRevision() {
			return false
		}
		expiredRevision = response.GetHeader().GetRevision()
		return true
	}, 15*time.Second, 100*time.Millisecond, "leased key did not expire at a new MVCC revision")
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

	replaceAllCompatPods(t, ctx, kubeContext, namespace, pods)
	afterConn, err := grpc.NewClient(grpcTarget(endpoint), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	defer afterConn.Close()
	afterKV := etcdserverpb.NewKVClient(afterConn)
	after, err := afterKV.Range(ctx, &etcdserverpb.RangeRequest{Key: key})
	require.NoError(t, err)
	require.Empty(t, after.Kvs)
	require.Equal(t, expiredRevision, after.GetHeader().GetRevision(),
		"lease-expiry tombstone and committed revision must recover atomically from TiKV")
	updated, err := afterKV.Put(ctx, &etcdserverpb.PutRequest{Key: key, Value: []byte("after-restart")})
	require.NoError(t, err)
	require.Greater(t, updated.GetHeader().GetRevision(), expiredRevision)
}

func TestReferenceEtcdLeaseExpiryRestartPreservesRevision(t *testing.T) {
	binary := os.Getenv("REFERENCE_ETCD_BINARY")
	if binary == "" {
		t.Skip("set REFERENCE_ETCD_BINARY to run the restart oracle")
	}
	const endpoint = "127.0.0.1:42379"
	args := []string{
		"--name", "lease-expiry-restart-oracle",
		"--data-dir", t.TempDir(),
		"--listen-client-urls", "http://" + endpoint,
		"--advertise-client-urls", "http://" + endpoint,
		"--listen-peer-urls", "http://127.0.0.1:42380",
		"--initial-advertise-peer-urls", "http://127.0.0.1:42380",
		"--initial-cluster", "lease-expiry-restart-oracle=http://127.0.0.1:42380",
	}
	start := func() (func(), *grpc.ClientConn, etcdserverpb.KVClient, etcdserverpb.LeaseClient) {
		t.Helper()
		stop := startCompatCommand(t, binary, args...)
		conn, err := grpc.NewClient(endpoint, grpc.WithTransportCredentials(insecure.NewCredentials()))
		require.NoError(t, err)
		kv := etcdserverpb.NewKVClient(conn)
		require.Eventually(t, func() bool {
			callCtx, callCancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
			defer callCancel()
			_, rangeErr := kv.Range(callCtx, &etcdserverpb.RangeRequest{Key: []byte("/a3436/health")})
			return rangeErr == nil
		}, 10*time.Second, 50*time.Millisecond)
		return stop, conn, kv, etcdserverpb.NewLeaseClient(conn)
	}

	stop, conn, kv, lease := start()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	key := []byte("/a3436/lease-expiry-restart")
	grant, err := lease.LeaseGrant(ctx, &etcdserverpb.LeaseGrantRequest{TTL: 2})
	require.NoError(t, err)
	put, err := kv.Put(ctx, &etcdserverpb.PutRequest{Key: key, Value: []byte("leased"), Lease: grant.ID})
	require.NoError(t, err)
	var expiredRevision int64
	require.Eventually(t, func() bool {
		response, rangeErr := kv.Range(ctx, &etcdserverpb.RangeRequest{Key: key})
		if rangeErr != nil || len(response.Kvs) != 0 || response.GetHeader().GetRevision() <= put.GetHeader().GetRevision() {
			return false
		}
		expiredRevision = response.GetHeader().GetRevision()
		return true
	}, 15*time.Second, 100*time.Millisecond)
	require.NoError(t, conn.Close())
	stop()

	_, restartedConn, restartedKV, _ := start()
	defer restartedConn.Close()
	after, err := restartedKV.Range(ctx, &etcdserverpb.RangeRequest{Key: key})
	require.NoError(t, err)
	require.Empty(t, after.Kvs)
	require.Equal(t, expiredRevision, after.GetHeader().GetRevision())
}
