package compat

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"
)

type normalizedRangeStreamScenario struct {
	Unlimited normalizedRange
	Limited   normalizedRange
	CountOnly normalizedRange
	KeysOnly  normalizedRange
	Explicit  normalizedRange
}

// TestRangeStreamDifferentialAgainstReferenceEtcd drives the public 3.7
// client/v3 GetStream API against both engines. It catches wire-level fields
// such as More and Count that a key-only stream smoke test cannot observe.
func TestRangeStreamDifferentialAgainstReferenceEtcd(t *testing.T) {
	reference := os.Getenv("REFERENCE_ETCD_ENDPOINT")
	if reference == "" {
		t.Skip("set REFERENCE_ETCD_ENDPOINT to run RangeStream differential tests")
	}
	kubebrain := runRangeStreamScenario(t, compatEndpoint(), "kubebrain")
	etcd := runRangeStreamScenario(t, reference, "etcd")
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
		Unlimited: run(),
		Limited:   run(clientv3.WithLimit(5)),
		CountOnly: run(clientv3.WithCountOnly(), clientv3.WithLimit(1)),
		KeysOnly:  run(clientv3.WithKeysOnly()),
		Explicit:  run(clientv3.WithSort(clientv3.SortByKey, clientv3.SortAscend), clientv3.WithLimit(5)),
	}
}
