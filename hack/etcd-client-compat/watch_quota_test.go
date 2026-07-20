package compat

import (
	"context"
	"net"
	"os"
	"os/exec"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// TestLogicalWatchQuota requires a disposable KubeBrain endpoint configured
// with --max-watches=1. It uses the public protobuf client directly because the
// high-level client transparently reconnects and multiplexes watch streams.
func TestLogicalWatchQuota(t *testing.T) {
	endpoint := os.Getenv("KUBEBRAIN_WATCH_QUOTA_ENDPOINT")
	if endpoint == "" {
		t.Skip("set KUBEBRAIN_WATCH_QUOTA_ENDPOINT to an endpoint configured with --max-watches=1")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	conn, err := grpc.NewClient(endpoint, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	defer func() { require.NoError(t, conn.Close()) }()
	stream, err := etcdserverpb.NewWatchClient(conn).Watch(ctx)
	require.NoError(t, err)

	sendCreate := func(id int64, key string) {
		require.NoError(t, stream.Send(&etcdserverpb.WatchRequest{
			RequestUnion: &etcdserverpb.WatchRequest_CreateRequest{
				CreateRequest: &etcdserverpb.WatchCreateRequest{WatchId: id, Key: []byte(key)},
			},
		}))
	}
	sendCreate(11, "/watch-quota/first")
	first, err := stream.Recv()
	require.NoError(t, err)
	require.True(t, first.Created)
	require.False(t, first.Canceled)
	require.Equal(t, int64(11), first.WatchId)

	sendCreate(12, "/watch-quota/rejected")
	rejected, err := stream.Recv()
	require.NoError(t, err)
	require.True(t, rejected.Created)
	require.True(t, rejected.Canceled)
	require.Equal(t, int64(-1), rejected.WatchId)
	require.Equal(t, "etcdserver: too many requests", rejected.CancelReason)

	require.NoError(t, stream.Send(&etcdserverpb.WatchRequest{
		RequestUnion: &etcdserverpb.WatchRequest_CancelRequest{
			CancelRequest: &etcdserverpb.WatchCancelRequest{WatchId: 11},
		},
	}))
	canceled, err := stream.Recv()
	require.NoError(t, err)
	require.True(t, canceled.Canceled)
	require.Equal(t, int64(11), canceled.WatchId)

	sendCreate(13, "/watch-quota/reused")
	reused, err := stream.Recv()
	require.NoError(t, err)
	require.True(t, reused.Created)
	require.False(t, reused.Canceled)
	require.Equal(t, int64(13), reused.WatchId)

	require.NoError(t, stream.Send(&etcdserverpb.WatchRequest{
		RequestUnion: &etcdserverpb.WatchRequest_CancelRequest{
			CancelRequest: &etcdserverpb.WatchCancelRequest{WatchId: 13},
		},
	}))
	canceled, err = stream.Recv()
	require.NoError(t, err)
	require.True(t, canceled.Canceled)
	require.Equal(t, int64(13), canceled.WatchId)
	require.NoError(t, stream.CloseSend())
}

// TestLogicalWatchQuotaResetsAfterDisconnectAndReplicaRestart requires a
// direct endpoint for one Pod in a disposable three-replica deployment
// configured with --max-watches=2. It proves that abrupt transport loss and
// process replacement cannot strand logical watch quota.
func TestLogicalWatchQuotaResetsAfterDisconnectAndReplicaRestart(t *testing.T) {
	endpoint := os.Getenv("KUBEBRAIN_WATCH_QUOTA_RESTART_ENDPOINT")
	namespace := os.Getenv("KUBEBRAIN_WATCH_QUOTA_RESTART_NAMESPACE")
	victimPod := os.Getenv("KUBEBRAIN_WATCH_QUOTA_RESTART_POD")
	if endpoint == "" || namespace == "" || victimPod == "" {
		t.Skip("set KUBEBRAIN_WATCH_QUOTA_RESTART_ENDPOINT, KUBEBRAIN_WATCH_QUOTA_RESTART_NAMESPACE, and KUBEBRAIN_WATCH_QUOTA_RESTART_POD")
	}
	kubeContext := os.Getenv("KUBEBRAIN_WATCH_QUOTA_RESTART_CONTEXT")

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	requireEndpointReachable(t, endpoint)

	conn, stream := openRawWatch(t, ctx, endpoint)
	createWatchAndRequire(t, stream, 101, "/watch-quota-restart/disconnect/one", false)
	createWatchAndRequire(t, stream, 102, "/watch-quota-restart/disconnect/two", false)
	createWatchAndRequire(t, stream, 103, "/watch-quota-restart/disconnect/rejected", true)
	require.NoError(t, conn.Close(), "close the transport without canceling either logical watch")
	_, err := stream.Recv()
	require.Error(t, err)

	conn, stream = openRawWatch(t, ctx, endpoint)
	createWatchAndRequire(t, stream, 201, "/watch-quota-restart/replacement/one", false)
	createWatchAndRequire(t, stream, 202, "/watch-quota-restart/replacement/two", false)
	createWatchAndRequire(t, stream, 203, "/watch-quota-restart/replacement/rejected", true)

	oldUID := kubectlPodField(t, kubeContext, namespace, victimPod, "{.metadata.uid}")
	deleteArgs := kubectlContextArgs(kubeContext,
		"-n", namespace, "delete", "pod", victimPod, "--wait=true", "--timeout=60s")
	output, err := exec.CommandContext(ctx, "kubectl", deleteArgs...).CombinedOutput()
	require.NoError(t, err, string(output))

	streamErr := make(chan error, 1)
	go func() {
		_, recvErr := stream.Recv()
		streamErr <- recvErr
	}()
	select {
	case err = <-streamErr:
		require.Error(t, err, "the old process watch stream must terminate")
	case <-time.After(30 * time.Second):
		t.Fatal("old watch stream remained open after its Pod was deleted")
	}
	require.NoError(t, conn.Close())

	var newUID string
	require.Eventually(t, func() bool {
		newUID = kubectlPodFieldNoFail(kubeContext, namespace, victimPod, "{.metadata.uid}")
		return newUID != "" &&
			newUID != oldUID &&
			kubectlPodFieldNoFail(
				kubeContext, namespace, victimPod, "{.status.containerStatuses[0].ready}",
			) == "true"
	}, 90*time.Second, 500*time.Millisecond)
	requireEndpointReachable(t, endpoint)

	conn, stream = openRawWatch(t, ctx, endpoint)
	defer func() { require.NoError(t, conn.Close()) }()
	createWatchAndRequire(t, stream, 301, "/watch-quota-restart/recovered/one", false)
	createWatchAndRequire(t, stream, 302, "/watch-quota-restart/recovered/two", false)
	createWatchAndRequire(t, stream, 303, "/watch-quota-restart/recovered/rejected", true)
	require.NotEqual(t, oldUID, newUID)
}

func requireEndpointReachable(t *testing.T, endpoint string) {
	t.Helper()
	require.Eventually(t, func() bool {
		conn, err := net.DialTimeout("tcp", endpoint, 500*time.Millisecond)
		if err != nil {
			return false
		}
		return conn.Close() == nil
	}, 30*time.Second, 200*time.Millisecond, "direct watch-quota endpoint must route to the replacement Pod")
}

func openRawWatch(
	t *testing.T,
	ctx context.Context,
	endpoint string,
) (*grpc.ClientConn, etcdserverpb.Watch_WatchClient) {
	t.Helper()
	conn, err := grpc.NewClient(endpoint, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	stream, err := etcdserverpb.NewWatchClient(conn).Watch(ctx)
	require.NoError(t, err)
	return conn, stream
}

func createWatchAndRequire(
	t *testing.T,
	stream etcdserverpb.Watch_WatchClient,
	id int64,
	key string,
	rejected bool,
) {
	t.Helper()
	require.NoError(t, stream.Send(&etcdserverpb.WatchRequest{
		RequestUnion: &etcdserverpb.WatchRequest_CreateRequest{
			CreateRequest: &etcdserverpb.WatchCreateRequest{WatchId: id, Key: []byte(key)},
		},
	}))
	var response *etcdserverpb.WatchResponse
	for {
		var err error
		response, err = stream.Recv()
		require.NoError(t, err)
		if response.Created {
			break
		}
	}
	require.Equal(t, rejected, response.Canceled)
	if rejected {
		require.Equal(t, int64(-1), response.WatchId)
		require.Equal(t, "etcdserver: too many requests", response.CancelReason)
		return
	}
	require.Equal(t, id, response.WatchId)
	require.Empty(t, response.CancelReason)
}
