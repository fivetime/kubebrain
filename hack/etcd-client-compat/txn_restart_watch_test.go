package compat

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// TestTxnSnapshotAndWatchRecoverAcrossAllReplicaReplacements is destructive.
// One mixed UPDATE/DELETE/CREATE TiKV transaction must durably bind its user
// versions, public revision, ordered historical watch events, and uncompacted
// PrevKVs so a completely fresh serving tier cannot expose a torn snapshot or
// lose/reorder the transaction's catch-up events.
func TestTxnSnapshotAndWatchRecoverAcrossAllReplicaReplacements(t *testing.T) {
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

	prefix := testPrefix(t) + "/"
	conn := newRawCompatConn(t, endpoint)
	seedRevision, txnRevision := seedRestartTxn(t, ctx, conn, prefix)
	require.NoError(t, conn.Close())
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cleanupCancel()
		cleanupConn, cleanupErr := grpc.NewClient(grpcTarget(endpoint), grpc.WithTransportCredentials(insecure.NewCredentials()))
		if cleanupErr != nil {
			return
		}
		defer cleanupConn.Close()
		_, _ = etcdserverpb.NewKVClient(cleanupConn).DeleteRange(cleanupCtx, &etcdserverpb.DeleteRangeRequest{
			Key: []byte(prefix), RangeEnd: []byte(clientv3.GetPrefixRangeEnd(prefix)),
		})
	})

	replaceAllCompatPods(t, ctx, kubeContext, namespace, pods)
	afterConn := newRawCompatConn(t, endpoint)
	defer afterConn.Close()
	assertRestartTxnSnapshotAndWatch(t, ctx, afterConn, prefix, seedRevision, txnRevision)
}

func TestReferenceEtcdTxnSnapshotAndWatchRecoverAfterRestart(t *testing.T) {
	binary := os.Getenv("REFERENCE_ETCD_BINARY")
	if binary == "" {
		t.Skip("set REFERENCE_ETCD_BINARY to run the restart oracle")
	}
	const endpoint = "127.0.0.1:42379"
	args := []string{
		"--name", "txn-watch-restart-oracle",
		"--data-dir", t.TempDir(),
		"--listen-client-urls", "http://" + endpoint,
		"--advertise-client-urls", "http://" + endpoint,
		"--listen-peer-urls", "http://127.0.0.1:42380",
		"--initial-advertise-peer-urls", "http://127.0.0.1:42380",
		"--initial-cluster", "txn-watch-restart-oracle=http://127.0.0.1:42380",
	}
	start := func() (func(), *grpc.ClientConn) {
		t.Helper()
		stop := startCompatCommand(t, binary, args...)
		conn := newRawCompatConn(t, endpoint)
		kv := etcdserverpb.NewKVClient(conn)
		require.Eventually(t, func() bool {
			callCtx, callCancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
			defer callCancel()
			_, err := kv.Range(callCtx, &etcdserverpb.RangeRequest{Key: []byte("/a3437/health")})
			return err == nil
		}, 10*time.Second, 50*time.Millisecond)
		return stop, conn
	}

	stop, conn := start()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	prefix := "/a3437/txn-watch-restart/"
	seedRevision, txnRevision := seedRestartTxn(t, ctx, conn, prefix)
	require.NoError(t, conn.Close())
	stop()

	_, restartedConn := start()
	defer restartedConn.Close()
	assertRestartTxnSnapshotAndWatch(t, ctx, restartedConn, prefix, seedRevision, txnRevision)
}

// TestCompactedTxnWatchOrderRecoversAcrossAllReplicaReplacements pins the
// compact boundary: revision == compactRevision remains watchable, so physical
// cleanup must retain the transaction's ordered event log at that revision,
// while UPDATE/DELETE PrevKVs below the watermark remain client-invisible.
func TestCompactedTxnWatchOrderRecoversAcrossAllReplicaReplacements(t *testing.T) {
	endpoint := os.Getenv("KUBEBRAIN_IDLE_RESTART_ENDPOINT")
	namespace := os.Getenv("KUBEBRAIN_IDLE_RESTART_NAMESPACE")
	pods := splitNonEmptyCSV(os.Getenv("KUBEBRAIN_IDLE_RESTART_PODS"))
	if endpoint == "" || namespace == "" || len(pods) == 0 {
		t.Skip("set KUBEBRAIN_IDLE_RESTART_ENDPOINT, KUBEBRAIN_IDLE_RESTART_NAMESPACE, and KUBEBRAIN_IDLE_RESTART_PODS")
	}
	require.Len(t, pods, 3)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	prefix := testPrefix(t) + "/"
	conn := newRawCompatConn(t, endpoint)
	_, txnRevision := seedRestartTxn(t, ctx, conn, prefix)
	_, err := etcdserverpb.NewKVClient(conn).Compact(ctx, &etcdserverpb.CompactionRequest{
		Revision: txnRevision, Physical: true,
	})
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
		_, _ = etcdserverpb.NewKVClient(cleanupConn).DeleteRange(cleanupCtx, &etcdserverpb.DeleteRangeRequest{
			Key: []byte(prefix), RangeEnd: []byte(clientv3.GetPrefixRangeEnd(prefix)),
		})
	})

	replaceAllCompatPods(t, ctx, os.Getenv("KUBEBRAIN_IDLE_RESTART_CONTEXT"), namespace, pods)
	afterConn := newRawCompatConn(t, endpoint)
	defer afterConn.Close()
	assertRestartTxnCurrentAndWatch(t, ctx, afterConn, prefix, txnRevision, false)
}

func TestReferenceEtcdCompactedTxnWatchOrderRecoversAfterRestart(t *testing.T) {
	binary := os.Getenv("REFERENCE_ETCD_BINARY")
	if binary == "" {
		t.Skip("set REFERENCE_ETCD_BINARY to run the restart oracle")
	}
	const endpoint = "127.0.0.1:42379"
	args := []string{
		"--name", "compacted-txn-watch-restart-oracle",
		"--data-dir", t.TempDir(),
		"--listen-client-urls", "http://" + endpoint,
		"--advertise-client-urls", "http://" + endpoint,
		"--listen-peer-urls", "http://127.0.0.1:42380",
		"--initial-advertise-peer-urls", "http://127.0.0.1:42380",
		"--initial-cluster", "compacted-txn-watch-restart-oracle=http://127.0.0.1:42380",
	}
	start := func() (func(), *grpc.ClientConn) {
		t.Helper()
		stop := startCompatCommand(t, binary, args...)
		conn := newRawCompatConn(t, endpoint)
		require.Eventually(t, func() bool {
			callCtx, callCancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
			defer callCancel()
			_, err := etcdserverpb.NewKVClient(conn).Range(callCtx, &etcdserverpb.RangeRequest{Key: []byte("/a3437/compact-health")})
			return err == nil
		}, 10*time.Second, 50*time.Millisecond)
		return stop, conn
	}

	stop, conn := start()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	prefix := "/a3437/compacted-txn-watch-restart/"
	_, txnRevision := seedRestartTxn(t, ctx, conn, prefix)
	_, err := etcdserverpb.NewKVClient(conn).Compact(ctx, &etcdserverpb.CompactionRequest{
		Revision: txnRevision, Physical: true,
	})
	require.NoError(t, err)
	require.NoError(t, conn.Close())
	stop()
	_, restartedConn := start()
	defer restartedConn.Close()
	assertRestartTxnCurrentAndWatch(t, ctx, restartedConn, prefix, txnRevision, false)
}

// TestPriorCompactedTxnPrevKVRecoversAcrossAllReplicaReplacements covers the
// other compact boundary: the previous versions sit exactly AT the watermark,
// so a later transaction's Range(ModRevision-1) must still recover them.
func TestPriorCompactedTxnPrevKVRecoversAcrossAllReplicaReplacements(t *testing.T) {
	endpoint := os.Getenv("KUBEBRAIN_IDLE_RESTART_ENDPOINT")
	namespace := os.Getenv("KUBEBRAIN_IDLE_RESTART_NAMESPACE")
	pods := splitNonEmptyCSV(os.Getenv("KUBEBRAIN_IDLE_RESTART_PODS"))
	if endpoint == "" || namespace == "" || len(pods) == 0 {
		t.Skip("set KUBEBRAIN_IDLE_RESTART_ENDPOINT, KUBEBRAIN_IDLE_RESTART_NAMESPACE, and KUBEBRAIN_IDLE_RESTART_PODS")
	}
	require.Len(t, pods, 3)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	prefix := testPrefix(t) + "/"
	conn := newRawCompatConn(t, endpoint)
	seedRevision, txnRevision := seedRestartTxn(t, ctx, conn, prefix)
	_, err := etcdserverpb.NewKVClient(conn).Compact(ctx, &etcdserverpb.CompactionRequest{
		Revision: seedRevision, Physical: true,
	})
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
		_, _ = etcdserverpb.NewKVClient(cleanupConn).DeleteRange(cleanupCtx, &etcdserverpb.DeleteRangeRequest{
			Key: []byte(prefix), RangeEnd: []byte(clientv3.GetPrefixRangeEnd(prefix)),
		})
	})

	replaceAllCompatPods(t, ctx, os.Getenv("KUBEBRAIN_IDLE_RESTART_CONTEXT"), namespace, pods)
	afterConn := newRawCompatConn(t, endpoint)
	defer afterConn.Close()
	assertRestartTxnSnapshotAndWatch(t, ctx, afterConn, prefix, seedRevision, txnRevision)
}

func TestReferenceEtcdPriorCompactedTxnPrevKVRecoversAfterRestart(t *testing.T) {
	binary := os.Getenv("REFERENCE_ETCD_BINARY")
	if binary == "" {
		t.Skip("set REFERENCE_ETCD_BINARY to run the restart oracle")
	}
	const endpoint = "127.0.0.1:42379"
	args := []string{
		"--name", "prior-compacted-txn-prevkv-restart-oracle",
		"--data-dir", t.TempDir(),
		"--listen-client-urls", "http://" + endpoint,
		"--advertise-client-urls", "http://" + endpoint,
		"--listen-peer-urls", "http://127.0.0.1:42380",
		"--initial-advertise-peer-urls", "http://127.0.0.1:42380",
		"--initial-cluster", "prior-compacted-txn-prevkv-restart-oracle=http://127.0.0.1:42380",
	}
	start := func() (func(), *grpc.ClientConn) {
		t.Helper()
		stop := startCompatCommand(t, binary, args...)
		conn := newRawCompatConn(t, endpoint)
		kv := etcdserverpb.NewKVClient(conn)
		require.Eventually(t, func() bool {
			callCtx, callCancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
			defer callCancel()
			_, err := kv.Range(callCtx, &etcdserverpb.RangeRequest{Key: []byte("/a3440/health")})
			return err == nil
		}, 10*time.Second, 50*time.Millisecond)
		return stop, conn
	}

	stop, conn := start()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	prefix := "/a3440/prior-compacted-txn-prevkv-restart/"
	seedRevision, txnRevision := seedRestartTxn(t, ctx, conn, prefix)
	_, err := etcdserverpb.NewKVClient(conn).Compact(ctx, &etcdserverpb.CompactionRequest{
		Revision: seedRevision, Physical: true,
	})
	require.NoError(t, err)
	require.NoError(t, conn.Close())
	stop()
	_, restartedConn := start()
	defer restartedConn.Close()
	assertRestartTxnSnapshotAndWatch(t, ctx, restartedConn, prefix, seedRevision, txnRevision)
}

func newRawCompatConn(t *testing.T, endpoint string) *grpc.ClientConn {
	t.Helper()
	conn, err := grpc.NewClient(grpcTarget(endpoint), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	return conn
}

func seedRestartTxn(t *testing.T, ctx context.Context, conn *grpc.ClientConn, prefix string) (int64, int64) {
	t.Helper()
	kv := etcdserverpb.NewKVClient(conn)
	seed, err := kv.Txn(ctx, &etcdserverpb.TxnRequest{Success: []*etcdserverpb.RequestOp{
		{Request: &etcdserverpb.RequestOp_RequestPut{RequestPut: &etcdserverpb.PutRequest{Key: []byte(prefix + "m"), Value: []byte("seed-m")}}},
		{Request: &etcdserverpb.RequestOp_RequestPut{RequestPut: &etcdserverpb.PutRequest{Key: []byte(prefix + "z"), Value: []byte("seed-z")}}},
	}})
	require.NoError(t, err)
	require.True(t, seed.Succeeded)
	require.Len(t, seed.Responses, 2)
	txn, err := kv.Txn(ctx, &etcdserverpb.TxnRequest{Success: []*etcdserverpb.RequestOp{
		{Request: &etcdserverpb.RequestOp_RequestPut{RequestPut: &etcdserverpb.PutRequest{Key: []byte(prefix + "z"), Value: []byte("updated-z")}}},
		{Request: &etcdserverpb.RequestOp_RequestDeleteRange{RequestDeleteRange: &etcdserverpb.DeleteRangeRequest{Key: []byte(prefix + "m"), PrevKv: true}}},
		{Request: &etcdserverpb.RequestOp_RequestPut{RequestPut: &etcdserverpb.PutRequest{Key: []byte(prefix + "a"), Value: []byte("a")}}},
	}})
	require.NoError(t, err)
	require.True(t, txn.Succeeded)
	require.Len(t, txn.Responses, 3)
	require.Equal(t, int64(1), txn.Responses[1].GetResponseDeleteRange().Deleted)
	require.Greater(t, txn.GetHeader().GetRevision(), seed.GetHeader().GetRevision())
	return seed.GetHeader().GetRevision(), txn.GetHeader().GetRevision()
}

func assertRestartTxnSnapshotAndWatch(
	t *testing.T,
	ctx context.Context,
	conn *grpc.ClientConn,
	prefix string,
	seedRevision, txnRevision int64,
) {
	t.Helper()
	kv := etcdserverpb.NewKVClient(conn)
	rangeEnd := []byte(clientv3.GetPrefixRangeEnd(prefix))
	assertRestartTxnCurrentAndWatch(t, ctx, conn, prefix, txnRevision, true)
	historical, err := kv.Range(ctx, &etcdserverpb.RangeRequest{
		Key: []byte(prefix), RangeEnd: rangeEnd, Revision: seedRevision,
	})
	require.NoError(t, err)
	require.Equal(t, txnRevision, historical.GetHeader().GetRevision())
	require.Equal(t, []string{prefix + "m", prefix + "z"}, restartTxnKeys(historical))
	require.Equal(t, []string{"seed-m", "seed-z"}, restartTxnValues(historical))
}

func assertRestartTxnCurrentAndWatch(t *testing.T, ctx context.Context, conn *grpc.ClientConn, prefix string, txnRevision int64, wantPreviousValues bool) {
	t.Helper()
	rangeEnd := []byte(clientv3.GetPrefixRangeEnd(prefix))
	current, err := etcdserverpb.NewKVClient(conn).Range(ctx, &etcdserverpb.RangeRequest{Key: []byte(prefix), RangeEnd: rangeEnd})
	require.NoError(t, err)
	require.Equal(t, txnRevision, current.GetHeader().GetRevision())
	require.Equal(t, []string{prefix + "a", prefix + "z"}, restartTxnKeys(current))
	require.Equal(t, []string{"a", "updated-z"}, restartTxnValues(current))

	watchCtx, watchCancel := context.WithTimeout(ctx, 10*time.Second)
	defer watchCancel()
	stream, err := etcdserverpb.NewWatchClient(conn).Watch(watchCtx)
	require.NoError(t, err)
	require.NoError(t, stream.Send(&etcdserverpb.WatchRequest{
		RequestUnion: &etcdserverpb.WatchRequest_CreateRequest{CreateRequest: &etcdserverpb.WatchCreateRequest{
			Key: []byte(prefix), RangeEnd: rangeEnd, StartRevision: txnRevision, WatchId: 3437, PrevKv: true,
		}},
	}))
	created, err := stream.Recv()
	require.NoError(t, err)
	require.True(t, created.Created)
	require.Equal(t, int64(3437), created.WatchId)
	require.Equal(t, txnRevision, created.GetHeader().GetRevision())

	var eventKeys []string
	var eventTypes []mvccpb.Event_EventType
	var events []*mvccpb.Event
	for len(eventKeys) < 3 {
		response, recvErr := stream.Recv()
		require.NoError(t, recvErr)
		require.Equal(t, txnRevision, response.GetHeader().GetRevision())
		for _, event := range response.Events {
			require.Equal(t, txnRevision, event.Kv.ModRevision)
			events = append(events, event)
			eventKeys = append(eventKeys, string(event.Kv.Key))
			eventTypes = append(eventTypes, event.Type)
		}
	}
	require.Equal(t, []string{prefix + "z", prefix + "m", prefix + "a"}, eventKeys)
	require.Equal(t, []mvccpb.Event_EventType{
		mvccpb.PUT, mvccpb.DELETE, mvccpb.PUT,
	}, eventTypes)
	require.Nil(t, events[2].PrevKv, "creating PUT must not carry PrevKV")
	if wantPreviousValues {
		require.NotNil(t, events[0].PrevKv, "uncompacted UPDATE must carry PrevKV")
		require.Equal(t, []byte(prefix+"z"), events[0].PrevKv.Key)
		require.Equal(t, []byte("seed-z"), events[0].PrevKv.Value)
		require.NotNil(t, events[1].PrevKv, "uncompacted DELETE must carry PrevKV")
		require.Equal(t, []byte(prefix+"m"), events[1].PrevKv.Key)
		require.Equal(t, []byte("seed-m"), events[1].PrevKv.Value)
	} else {
		require.Nil(t, events[0].PrevKv, "UPDATE PrevKV below the compact watermark must be unavailable")
		require.Nil(t, events[1].PrevKv, "DELETE PrevKV below the compact watermark must be unavailable")
	}
}

func restartTxnKeys(response *etcdserverpb.RangeResponse) []string {
	keys := make([]string, 0, len(response.Kvs))
	for _, kv := range response.Kvs {
		keys = append(keys, string(kv.Key))
	}
	return keys
}

func restartTxnValues(response *etcdserverpb.RangeResponse) []string {
	values := make([]string, 0, len(response.Kvs))
	for _, kv := range response.Kvs {
		values = append(values, string(kv.Value))
	}
	return values
}
