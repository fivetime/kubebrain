package compat

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

func TestWatchCancellationMetricsLive(t *testing.T) {
	endpoint := os.Getenv("WATCH_CANCEL_ENDPOINT")
	metricsURL := os.Getenv("WATCH_CANCEL_METRICS_URL")
	if endpoint == "" || metricsURL == "" {
		t.Skip("set WATCH_CANCEL_ENDPOINT and WATCH_CANCEL_METRICS_URL")
	}
	conn, err := grpc.NewClient(grpcTarget(endpoint), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	defer conn.Close()
	client := etcdserverpb.NewWatchClient(conn)

	const iterations = 200
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	beforeCanceled := grpcHandledMetric(t, metricsURL, "etcdserverpb.Watch", "Watch", "Canceled")
	beforeUnknown := grpcHandledMetric(t, metricsURL, "etcdserverpb.Watch", "Watch", "Unknown")
	beforeUnavailable := grpcHandledMetric(t, metricsURL, "etcdserverpb.Watch", "Watch", "Unavailable")
	for i := 0; i < iterations; i++ {
		streamCtx, streamCancel := context.WithCancel(ctx)
		stream, streamErr := client.Watch(streamCtx)
		require.NoError(t, streamErr)
		require.NoError(t, stream.Send(&etcdserverpb.WatchRequest{
			RequestUnion: &etcdserverpb.WatchRequest_CreateRequest{
				CreateRequest: &etcdserverpb.WatchCreateRequest{
					Key: []byte(fmt.Sprintf("/dbaas/watch-cancel/%d", i)),
				},
			},
		}))
		created, recvErr := stream.Recv()
		require.NoError(t, recvErr)
		require.True(t, created.Created)
		require.False(t, created.Canceled)
		streamCancel()
		_, recvErr = stream.Recv()
		require.Equal(t, codes.Canceled, status.Code(recvErr))
	}

	require.Eventually(t, func() bool {
		return grpcHandledMetric(t, metricsURL, "etcdserverpb.Watch", "Watch", "Canceled") >= beforeCanceled+iterations
	}, 5*time.Second, 50*time.Millisecond)
	require.Equal(t, beforeUnknown,
		grpcHandledMetric(t, metricsURL, "etcdserverpb.Watch", "Watch", "Unknown"))
	require.Equal(t, beforeUnavailable,
		grpcHandledMetric(t, metricsURL, "etcdserverpb.Watch", "Watch", "Unavailable"))
}
