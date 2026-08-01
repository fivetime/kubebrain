package compat

import (
	"context"
	"io"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type dollarKeyRangeOutcome struct {
	LowerValue      string
	CurrentCount    int64
	HistoricalValue string
	PrefixCount     int64
	StreamCount     int64
}

// TestDollarKeyNarrowRangeDifferentialAgainstReferenceEtcd pins arbitrary etcd
// keys containing KubeBrain's legacy '$' object-key delimiter. The scanner unit
// test forces the TiKV split; this black-box layer fixes the client-visible
// exact/history/prefix/RangeStream contract against upstream etcd.
func TestDollarKeyNarrowRangeDifferentialAgainstReferenceEtcd(t *testing.T) {
	reference := os.Getenv("REFERENCE_ETCD_ENDPOINT")
	if reference == "" {
		t.Skip("set REFERENCE_ETCD_ENDPOINT to run dollar-key range differential tests")
	}
	referenceOutcome := runDollarKeyNarrowRangeScenario(t, reference, "reference")
	require.Equal(t, referenceOutcome, runDollarKeyNarrowRangeScenario(t, compatEndpoint(), "kubebrain"))
}

func runDollarKeyNarrowRangeScenario(t *testing.T, endpoint, instance string) dollarKeyRangeOutcome {
	t.Helper()
	endpoint = strings.TrimPrefix(strings.TrimPrefix(endpoint, "http://"), "https://")
	conn, err := grpc.NewClient(endpoint, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, conn.Close()) })
	kv := etcdserverpb.NewKVClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	t.Cleanup(cancel)

	prefix := []byte(testPrefix(t) + "/" + instance + "/a")
	lower := append([]byte(nil), prefix...)
	target := append(append([]byte(nil), prefix...), []byte("$target")...)
	rangeEnd := append(append([]byte(nil), target...), 0xff)
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		_, _ = kv.DeleteRange(cleanupCtx, &etcdserverpb.DeleteRangeRequest{Key: lower, RangeEnd: rangeEnd})
	})

	_, err = kv.Put(ctx, &etcdserverpb.PutRequest{Key: lower, Value: []byte("lower")})
	require.NoError(t, err)
	_, err = kv.Put(ctx, &etcdserverpb.PutRequest{Key: target, Value: []byte("v1")})
	require.NoError(t, err)
	updated, err := kv.Put(ctx, &etcdserverpb.PutRequest{Key: target, Value: []byte("v2")})
	require.NoError(t, err)
	_, err = kv.DeleteRange(ctx, &etcdserverpb.DeleteRangeRequest{Key: target})
	require.NoError(t, err)

	lowerResponse, err := kv.Range(ctx, &etcdserverpb.RangeRequest{Key: lower})
	require.NoError(t, err)
	require.Len(t, lowerResponse.Kvs, 1)
	current, err := kv.Range(ctx, &etcdserverpb.RangeRequest{Key: target})
	require.NoError(t, err)
	historical, err := kv.Range(ctx, &etcdserverpb.RangeRequest{Key: target, Revision: updated.Header.Revision})
	require.NoError(t, err)
	require.Len(t, historical.Kvs, 1)
	prefixResponse, err := kv.Range(ctx, &etcdserverpb.RangeRequest{Key: target, RangeEnd: rangeEnd})
	require.NoError(t, err)
	stream, err := kv.RangeStream(ctx, &etcdserverpb.RangeRequest{Key: target, RangeEnd: rangeEnd})
	require.NoError(t, err)
	var streamCount int64
	for {
		response, recvErr := stream.Recv()
		if recvErr != nil {
			require.ErrorIs(t, recvErr, io.EOF)
			break
		}
		streamCount += int64(len(response.GetRangeResponse().GetKvs()))
	}
	return dollarKeyRangeOutcome{
		LowerValue: string(lowerResponse.Kvs[0].Value), CurrentCount: current.Count,
		HistoricalValue: string(historical.Kvs[0].Value), PrefixCount: prefixResponse.Count,
		StreamCount: streamCount,
	}
}
