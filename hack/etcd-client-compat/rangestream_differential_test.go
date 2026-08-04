package compat

import (
	"context"
	"fmt"
	"math"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/protobuf/proto"
)

type normalizedRangeStreamScenario struct {
	Unlimited        normalizedRange
	Limited          normalizedRange
	CountOnly        normalizedRange
	KeysOnly         normalizedRange
	Explicit         normalizedRange
	Negative         normalizedRange
	MinLimit         normalizedRange
	MaxLimit         normalizedRange
	NegativeRevision normalizedRange
	MinRevision      normalizedRange
}

func TestRangeStreamProductionChunkTarget(t *testing.T) {
	endpoint := os.Getenv("KUBEBRAIN_ETCD_ENDPOINT")
	if endpoint == "" {
		t.Skip("set KUBEBRAIN_ETCD_ENDPOINT to run the live RangeStream chunk test")
	}
	cli, err := clientv3.New(clientv3.Config{Endpoints: []string{endpoint}, DialTimeout: 3 * time.Second})
	require.NoError(t, err)
	defer func() { require.NoError(t, cli.Close()) }()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	prefix := fmt.Sprintf("/dbaas-rangestream-chunks/%d/", time.Now().UnixNano())
	for i := 0; i < 9; i++ {
		value := make([]byte, 320*1024)
		value[0] = byte(i)
		_, err = cli.Put(ctx, fmt.Sprintf("%s%02d", prefix, i), string(value))
		require.NoError(t, err)
	}
	defer func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		_, _ = cli.Delete(cleanupCtx, prefix, clientv3.WithPrefix())
	}()

	check := func(wantKVs, wantCount int, wantMore bool, opts ...clientv3.OpOption) {
		stream, streamErr := cli.GetStream(ctx, prefix,
			append([]clientv3.OpOption{clientv3.WithPrefix()}, opts...)...)
		require.NoError(t, streamErr)
		count := 0
		chunks := 0
		var final *etcdserverpb.RangeResponse
		for chunk := range stream {
			require.NoError(t, chunk.Err())
			require.NotNil(t, chunk.RangeResponse)
			wire := &etcdserverpb.RangeStreamResponse{RangeResponse: chunk.RangeResponse}
			require.LessOrEqual(t, proto.Size(wire), 1572864)
			count += len(chunk.Kvs)
			chunks++
			if chunk.Header != nil {
				final = chunk.RangeResponse
			}
		}
		require.Equal(t, wantKVs, count)
		require.Greater(t, chunks, 1)
		require.NotNil(t, final)
		require.EqualValues(t, wantCount, final.Count)
		require.Equal(t, wantMore, final.More)
	}

	check(9, 9, false)
	check(8, 9, true, clientv3.WithLimit(8))
}

// TestRangeStreamDifferentialAgainstReferenceEtcd drives the public 3.7
// client/v3 GetStream API against both engines. It catches wire-level fields
// such as More and Count that a key-only stream smoke test cannot observe.
func TestRangeStreamDifferentialAgainstReferenceEtcd(t *testing.T) {
	reference := os.Getenv("REFERENCE_ETCD_ENDPOINT")
	if reference == "" {
		t.Skip("set REFERENCE_ETCD_ENDPOINT to run RangeStream differential tests")
	}
	kubebrain := runRangeStreamScenario(t, compatEndpoint(t), "kubebrain")
	etcd := runRangeStreamScenario(t, reference, "etcd")
	require.Equal(t, etcd.Unlimited, etcd.Negative, "a negative limit is unlimited")
	require.Equal(t, etcd.Unlimited, etcd.MinLimit, "MinInt64 must not overflow into a positive limit")
	require.Equal(t, etcd.Unlimited, etcd.MaxLimit, "MaxInt64 exceeds this result set without truncating it")
	require.Equal(t, etcd.Unlimited, etcd.NegativeRevision, "a negative revision selects the latest snapshot")
	require.Equal(t, etcd.Unlimited, etcd.MinRevision, "MinInt64 revision selects the latest snapshot without unsigned wrap")
	require.Equal(t, etcd, kubebrain)
}

func runRangeStreamScenario(t *testing.T, endpoint, instance string) normalizedRangeStreamScenario {
	t.Helper()
	cli, err := clientv3.New(clientv3.Config{Endpoints: []string{endpoint}, DialTimeout: 3 * time.Second})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, cli.Close()) })
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	t.Cleanup(cancel)

	prefix := fmt.Sprintf("/dbaas-rangestream-differential/%s/%d/", instance, time.Now().UnixNano())
	base, err := cli.Get(ctx, prefix, clientv3.WithPrefix())
	require.NoError(t, err)
	for i := 0; i < 12; i++ {
		_, err = cli.Put(ctx, fmt.Sprintf("%s%02d", prefix, i), fmt.Sprintf("value-%02d", i))
		require.NoError(t, err)
	}
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		_, _ = cli.Delete(cleanupCtx, prefix, clientv3.WithPrefix())
	})

	run := func(opts ...clientv3.OpOption) normalizedRange {
		unary, err := cli.Get(ctx, prefix, append([]clientv3.OpOption{clientv3.WithPrefix()}, opts...)...)
		require.NoError(t, err)
		stream, err := cli.GetStream(ctx, prefix, append([]clientv3.OpOption{clientv3.WithPrefix()}, opts...)...)
		require.NoError(t, err)
		streamed, err := clientv3.GetStreamToGetResponse(stream)
		require.NoError(t, err)
		want := normalizeRange(unary, prefix, base.Header.Revision)
		got := normalizeRange((*clientv3.GetResponse)(streamed), prefix, base.Header.Revision)
		require.Equal(t, want, got, "RangeStream must merge to unary Range on %s", endpoint)
		return got
	}

	return normalizedRangeStreamScenario{
		Unlimited:        run(),
		Limited:          run(clientv3.WithLimit(5)),
		CountOnly:        run(clientv3.WithCountOnly(), clientv3.WithLimit(1)),
		KeysOnly:         run(clientv3.WithKeysOnly()),
		Explicit:         run(clientv3.WithSort(clientv3.SortByKey, clientv3.SortAscend), clientv3.WithLimit(5)),
		Negative:         run(clientv3.WithLimit(-1)),
		MinLimit:         run(clientv3.WithLimit(math.MinInt64)),
		MaxLimit:         run(clientv3.WithLimit(math.MaxInt64)),
		NegativeRevision: run(clientv3.WithRev(-1)),
		MinRevision:      run(clientv3.WithRev(math.MinInt64)),
	}
}
