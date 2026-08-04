package compat

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	clientv3 "go.etcd.io/etcd/client/v3"
)

func TestWriteHeavyCompatibilityScenariosRegisterPrefixCleanup(t *testing.T) {
	tests := map[string]int{
		"compact_differential_test.go":                  1,
		"compact_physical_concurrency_test.go":          1,
		"hashkv_differential_test.go":                   1,
		"range_keys_count_test.go":                      1,
		"range_keys_limit_differential_test.go":         1,
		"txn_compare_enum_differential_test.go":         1,
		"txn_duplicate_interval_differential_test.go":   1,
		"txn_operation_validation_differential_test.go": 2,
		"txn_validation_order_differential_test.go":     2,
	}
	for file, want := range tests {
		t.Run(file, func(t *testing.T) {
			source, err := os.ReadFile(file)
			require.NoError(t, err)
			got := strings.Count(string(source), "registerPrefixCleanup(t,") +
				strings.Count(string(source), "registerRawPrefixCleanup(t,")
			require.Equal(t, want, got, "%s must retain cleanup for every endpoint scenario", file)
		})
	}
}

// registerPrefixCleanup keeps endpoint-driven compatibility tests from
// accumulating live user keys in a shared DBaaS data plane. The verification
// makes a silently failed best-effort delete fail the owning test.
func registerPrefixCleanup(t *testing.T, cli *clientv3.Client, prefix string) {
	t.Helper()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		_, err := cli.Delete(ctx, prefix, clientv3.WithPrefix())
		require.NoError(t, err)
		remaining, err := cli.Get(ctx, prefix, clientv3.WithPrefix(), clientv3.WithLimit(1))
		require.NoError(t, err)
		require.Empty(t, remaining.Kvs, "compatibility test prefix %q was not cleaned", prefix)
	})
}

func registerRawPrefixCleanup(t *testing.T, client etcdserverpb.KVClient, prefix string) {
	t.Helper()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		request := &etcdserverpb.DeleteRangeRequest{
			Key: []byte(prefix), RangeEnd: []byte(clientPrefixRangeEnd(prefix)),
		}
		_, err := client.DeleteRange(ctx, request)
		require.NoError(t, err)
		remaining, err := client.Range(ctx, &etcdserverpb.RangeRequest{
			Key: request.Key, RangeEnd: request.RangeEnd, Limit: 1,
		})
		require.NoError(t, err)
		require.Empty(t, remaining.Kvs, "compatibility test prefix %q was not cleaned", prefix)
	})
}
