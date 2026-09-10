package backend

import (
	"bytes"
	"context"
	"sync/atomic"
	"testing"

	"github.com/golang/mock/gomock"
	proto "github.com/kubewharf/kubebrain-client/api/v2rpc"
	"github.com/kubewharf/kubebrain/pkg/metrics/mock"
	"github.com/kubewharf/kubebrain/pkg/storage"
	"github.com/kubewharf/kubebrain/pkg/storage/memkv"
)

// Counters describe storage API calls, NOT network RPCs or TiKV latency.
// The context marker excludes initialization and unrelated background work.
type writeCostContextKey struct{}

type writeCostStore struct {
	storage.KvStorage
	gets, batchGets, iters, commits, atomicGets atomic.Int64
}

func (s *writeCostStore) UnwrapKvStorage() storage.KvStorage { return s.KvStorage }

// Preserve memkv's optional batch-read capability; hiding it would measure a
// decorator-induced fallback instead of the ordinary backend read path.
func (s *writeCostStore) BatchGet(ctx context.Context, keys [][]byte) (map[string][]byte, error) {
	if ctx.Value(writeCostContextKey{}) == s {
		s.batchGets.Add(1)
	}
	return s.KvStorage.(storage.BatchGetter).BatchGet(ctx, keys)
}

func (s *writeCostStore) Get(ctx context.Context, key []byte) ([]byte, error) {
	if ctx.Value(writeCostContextKey{}) == s {
		s.gets.Add(1)
	}
	return s.KvStorage.Get(ctx, key)
}

func (s *writeCostStore) Iter(ctx context.Context, start, end []byte, revision uint64, limit uint64) (storage.Iter, error) {
	if ctx.Value(writeCostContextKey{}) == s {
		s.iters.Add(1)
	}
	return s.KvStorage.Iter(ctx, start, end, revision, limit)
}

func (s *writeCostStore) BeginBatchWrite() storage.BatchWrite {
	return &writeCostBatch{BatchWrite: s.KvStorage.BeginBatchWrite(), store: s}
}

type writeCostBatch struct {
	storage.BatchWrite
	store *writeCostStore
}

func (b *writeCostBatch) Commit(ctx context.Context) error {
	if ctx.Value(writeCostContextKey{}) == b.store {
		b.store.commits.Add(1)
	}
	return b.BatchWrite.Commit(ctx)
}

func (b *writeCostBatch) Atomic(fn func(context.Context, storage.AtomicBatch) error) {
	b.BatchWrite.Atomic(func(ctx context.Context, txn storage.AtomicBatch) error {
		return fn(ctx, writeCostAtomic{AtomicBatch: txn, store: b.store})
	})
}

type writeCostAtomic struct {
	storage.AtomicBatch
	store *writeCostStore
}

func (a writeCostAtomic) Get(ctx context.Context, key []byte) ([]byte, error) {
	if ctx.Value(writeCostContextKey{}) == a.store {
		a.store.atomicGets.Add(1)
	}
	return a.AtomicBatch.Get(ctx, key)
}

// BenchmarkBackendWriteStorageCalls isolates existing backend write paths with
// an in-memory engine. It does not include RPC auth/quota admission, proxying,
// TLS, leases, or the full etcd Put shim, and cannot establish a cluster SLO.
// Fixed-count runs (e.g. -benchtime=100x) bound retained MVCC history.
func BenchmarkBackendWriteStorageCalls(b *testing.B) {
	for _, quotaCase := range []struct {
		name  string
		bytes int64
	}{{"QuotaDisabled", 0}, {"Quota2GiB", 2 << 30}} {
		for _, path := range []string{"TxnApply", "GetThenUpdate"} {
			b.Run(quotaCase.name+"/"+path, func(b *testing.B) {
				store := &writeCostStore{KvStorage: memkv.NewKvStorage()}
				b.Cleanup(func() {
					if err := store.Close(); err != nil {
						b.Error(err)
					}
				})
				backend := NewBackend(store, Config{
					Prefix: "/write-cost", Identity: "write-cost", EnableEtcdCompatibility: true,
					QuotaBackendBytes: quotaCase.bytes,
				}, mock.NewMinimalMetrics(gomock.NewController(b))).(*backend)
				backend.SetCurrentRevision(100)
				if quotaCase.bytes > 0 {
					if err := backend.EnsureQuotaInitialized(context.Background()); err != nil {
						b.Fatal(err)
					}
				}
				key, value := []byte("/write-cost/key"), []byte("value")
				seed, err := backend.Create(context.Background(), &proto.CreateRequest{Key: key, Value: value})
				if err != nil || !seed.Succeeded {
					b.Fatalf("seed failed: response=%v error=%v", seed, err)
				}
				ctx := context.WithValue(context.Background(), writeCostContextKey{}, store)
				lastRevision := seed.Header.Revision
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					var revision uint64
					if path == "TxnApply" {
						_, revision, err = backend.TxnApply(ctx, []TxnWriteOp{{Key: key, Value: value}}, nil)
					} else {
						current, getErr := backend.Get(ctx, &proto.GetRequest{Key: key})
						if getErr != nil || current.Kv == nil {
							b.Fatalf("read failed: response=%v error=%v", current, getErr)
						}
						updated, updateErr := backend.Update(ctx, &proto.UpdateRequest{
							Kv: &proto.KeyValue{Key: key, Value: value, Revision: current.Kv.Revision},
						})
						err = updateErr
						if err == nil {
							if !updated.Succeeded {
								b.Fatal("uncontended update failed its guard")
							}
							revision = updated.Header.Revision
						}
					}
					if err != nil || revision != lastRevision+1 {
						b.Fatalf("write failed: revision=%d previous=%d error=%v", revision, lastRevision, err)
					}
					lastRevision = revision
				}
				b.StopTimer()
				final, err := backend.Get(context.Background(), &proto.GetRequest{Key: key})
				if err != nil || final.Kv == nil || final.Kv.Revision != lastRevision ||
					!bytes.Equal(StripInlineValue(final.Kv.Value), value) {
					b.Fatalf("final state mismatch: response=%v error=%v", final, err)
				}
				if quotaCase.bytes > 0 {
					usage, quota, alarm, err := backend.QuotaStatus(context.Background())
					if err != nil || alarm || quota != quotaCase.bytes || usage != int64(len(key)+len(value)) {
						b.Fatalf("quota state mismatch: usage=%d quota=%d alarm=%t error=%v", usage, quota, alarm, err)
					}
				}
				for name, count := range map[string]int64{
					"storage-get/op": store.gets.Load(), "storage-iter/op": store.iters.Load(),
					"storage-batch-get/op": store.batchGets.Load(),
					"batch-commit/op":      store.commits.Load(), "atomic-get/op": store.atomicGets.Load(),
				} {
					b.ReportMetric(float64(count)/float64(b.N), name)
				}
			})
		}
	}
}
