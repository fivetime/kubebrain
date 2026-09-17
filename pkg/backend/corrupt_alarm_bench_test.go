package backend

import (
	"context"
	"fmt"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"

	"github.com/kubewharf/kubebrain/pkg/metrics/mock"
	"github.com/kubewharf/kubebrain/pkg/storage/memkv"
)

// BenchmarkDisarmCorruptHistory measures the real disarm path, including witness
// object verification. Leadership initialization uses a different verification
// mode, so its bounded-read tests cannot establish disarm's RPC scaling.
// BatchGets/op counts storage calls, not network latency: memkv has no network.
func BenchmarkDisarmCorruptHistory(b *testing.B) {
	for _, transactions := range []int{64, 256, 1024} {
		b.Run(fmt.Sprintf("transactions=%d", transactions), func(b *testing.B) {
			ctx := context.Background()
			store := &countingWitnessIndexBatchStorage{KvStorage: memkv.NewKvStorage()}
			backend := NewBackend(store, Config{
				Prefix: prefix + "/disarm-history", Identity: "disarm-history-benchmark",
				EnableEtcdCompatibility: true,
			}, mock.NewMinimalMetrics(gomock.NewController(b))).(*backend)
			b.Cleanup(func() { require.NoError(b, backend.Close()) })
			backend.SetCurrentRevision(100)
			for i := 0; i < transactions; i++ {
				_, _, err := backend.TxnApply(ctx, []TxnWriteOp{{
					Key: []byte(fmt.Sprintf("%s/disarm-history/%04d", prefix, i)), Value: []byte("value"),
				}}, nil)
				require.NoError(b, err)
			}
			b.ResetTimer()
			b.StopTimer()
			var calls int64
			for i := 0; i < b.N; i++ {
				require.NoError(b, backend.ArmCorrupt(ctx, 41001))
				store.calls.Store(0)
				b.StartTimer()
				removed, err := backend.DisarmCorrupt(ctx, 41001)
				b.StopTimer()
				require.NoError(b, err)
				require.True(b, removed)
				calls += int64(store.calls.Load())
			}
			b.ReportMetric(float64(calls)/float64(b.N), "BatchGets/op")
		})
	}
}
