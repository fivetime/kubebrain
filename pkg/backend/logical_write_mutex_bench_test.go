package backend

import (
	"sync"
	"testing"
)

// Microbenchmarks compare admission overhead, not end-to-end TiKV latency.
func BenchmarkLogicalWriteBarrier(b *testing.B) {
	for _, kind := range []string{"rwmutex", "cancelable"} {
		b.Run(kind, func(b *testing.B) {
			var lock interface {
				Lock()
				Unlock()
				RLock()
				RUnlock()
			}
			if kind == "rwmutex" {
				lock = new(sync.RWMutex)
			} else {
				lock = new(logicalWriteMutex)
			}
			lock.Lock()
			lock.Unlock()
			b.Run("reader", func(b *testing.B) {
				b.ReportAllocs()
				for b.Loop() {
					lock.RLock()
					lock.RUnlock()
				}
			})
			b.Run("exclusive", func(b *testing.B) {
				b.ReportAllocs()
				for b.Loop() {
					lock.Lock()
					lock.Unlock()
				}
			})
			b.Run("parallel_readers", func(b *testing.B) {
				b.ReportAllocs()
				b.RunParallel(func(pb *testing.PB) {
					for pb.Next() {
						lock.RLock()
						lock.RUnlock()
					}
				})
			})
			b.Run("mixed", func(b *testing.B) {
				b.ReportAllocs()
				b.RunParallel(func(pb *testing.PB) {
					i := 0
					for pb.Next() {
						if i%100 == 0 {
							lock.Lock()
							lock.Unlock()
						} else {
							lock.RLock()
							lock.RUnlock()
						}
						i++
					}
				})
			})
		})
	}
}
