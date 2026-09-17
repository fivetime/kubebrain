package etcd

import (
	"sync"
	"testing"
)

// Compare primitive costs only. This excludes TiKV, authorization, RPCs, and
// expiry work; it is not an end-to-end throughput or latency acceptance test.
func BenchmarkLeaseStateMutex(b *testing.B) {
	for _, kind := range []string{"sync", "weighted", "exclusive"} {
		b.Run(kind, func(b *testing.B) {
			b.Run("serial", func(b *testing.B) {
				var lock sync.Locker = &sync.Mutex{}
				if kind == "weighted" {
					lock = &leaseWriteMutex{}
				} else if kind == "exclusive" {
					lock = &leaseStateMutex{}
				}
				lock.Lock()
				lock.Unlock() // Exclude one-time lazy initialization.
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					lock.Lock()
					lock.Unlock()
				}
			})
			b.Run("contended", func(b *testing.B) {
				var lock sync.Locker = &sync.Mutex{}
				if kind == "weighted" {
					lock = &leaseWriteMutex{}
				} else if kind == "exclusive" {
					lock = &leaseStateMutex{}
				}
				lock.Lock()
				lock.Unlock()
				b.ReportAllocs()
				b.ResetTimer()
				b.RunParallel(func(pb *testing.PB) {
					for pb.Next() {
						lock.Lock()
						lock.Unlock()
					}
				})
			})
		})
	}
}
