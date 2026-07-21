package compat

import (
	"context"
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

func TestRangeStreamCancellationMetricsLive(t *testing.T) {
	endpoint := os.Getenv("RANGE_STREAM_ENDPOINT")
	metricsURL := os.Getenv("RANGE_STREAM_METRICS_URL")
	if endpoint == "" || metricsURL == "" {
		t.Skip("set RANGE_STREAM_ENDPOINT and RANGE_STREAM_METRICS_URL")
	}
	conn, err := grpc.NewClient(grpcTarget(endpoint), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	defer conn.Close()
	client := etcdserverpb.NewKVClient(conn)

	beforeCanceled := grpcHandledMetric(t, metricsURL, "etcdserverpb.KV", "RangeStream", "Canceled")
	beforeUnknown := grpcHandledMetric(t, metricsURL, "etcdserverpb.KV", "RangeStream", "Unknown")
	beforeUnavailable := grpcHandledMetric(t, metricsURL, "etcdserverpb.KV", "RangeStream", "Unavailable")
	for i := 0; i < 200; i++ {
		ctx, cancel := context.WithCancel(context.Background())
		stream, streamErr := client.RangeStream(ctx, &etcdserverpb.RangeRequest{
			Key: []byte("/rangestream-cancel/"), RangeEnd: []byte("/rangestream-cancel0"),
		})
		require.NoError(t, streamErr)
		cancel()
		_, recvErr := stream.Recv()
		require.Equal(t, codes.Canceled, status.Code(recvErr))
	}

	require.Eventually(t, func() bool {
		return grpcHandledMetric(t, metricsURL, "etcdserverpb.KV", "RangeStream", "Canceled") > beforeCanceled
	}, 5*time.Second, 50*time.Millisecond)
	require.Equal(t, beforeUnknown,
		grpcHandledMetric(t, metricsURL, "etcdserverpb.KV", "RangeStream", "Unknown"))
	require.Equal(t, beforeUnavailable,
		grpcHandledMetric(t, metricsURL, "etcdserverpb.KV", "RangeStream", "Unavailable"))
}
