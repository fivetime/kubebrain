package compat

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	clientv3 "go.etcd.io/etcd/client/v3"
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
	setupCtx, setupCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer setupCancel()
	prefix := fmt.Sprintf("/rangestream-cancel/%d/", time.Now().UnixNano())
	rangeEnd := []byte(clientv3.GetPrefixRangeEnd(prefix))
	value := make([]byte, 64<<10)
	for i := 0; i < 64; i++ {
		_, err = client.Put(setupCtx, &etcdserverpb.PutRequest{
			Key: []byte(fmt.Sprintf("%s%03d", prefix, i)), Value: value,
		})
		require.NoError(t, err)
	}
	defer func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cleanupCancel()
		_, _ = client.DeleteRange(cleanupCtx, &etcdserverpb.DeleteRangeRequest{
			Key: []byte(prefix), RangeEnd: rangeEnd,
		})
	}()

	beforeCanceled := grpcHandledMetric(t, metricsURL, "etcdserverpb.KV", "RangeStream", "Canceled")
	beforeUnknown := grpcHandledMetric(t, metricsURL, "etcdserverpb.KV", "RangeStream", "Unknown")
	beforeUnavailable := grpcHandledMetric(t, metricsURL, "etcdserverpb.KV", "RangeStream", "Unavailable")
	for i := 0; i < 100; i++ {
		ctx, cancel := context.WithCancel(context.Background())
		stream, streamErr := client.RangeStream(ctx, &etcdserverpb.RangeRequest{
			Key: []byte(prefix), RangeEnd: rangeEnd,
		})
		require.NoError(t, streamErr)
		first, recvErr := stream.Recv()
		require.NoError(t, recvErr)
		require.NotEmpty(t, first.RangeResponse.Kvs)
		cancel()
		for recvErr == nil {
			_, recvErr = stream.Recv()
		}
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
