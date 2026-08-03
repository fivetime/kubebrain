package compat

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
)

// TestConcurrentPutSameKeyNeverFails asserts etcd Put semantics: an unconditional
// Put never fails on concurrent modification. KubeBrain emulates Put with a
// Get-then-CAS loop; previously it gave up after a fixed number of attempts and
// returned a spurious error under contention on a hot key. Every Put here must
// succeed.
func TestConcurrentPutSameKeyNeverFails(t *testing.T) {
	key := testPrefix(t) + "/hot"
	const writers, perWriter = 16, 40

	var wg sync.WaitGroup
	errCh := make(chan error, writers*perWriter)
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			cli, err := clientv3.New(clientv3.Config{Endpoints: []string{compatEndpoint(t)}, DialTimeout: 5 * time.Second})
			if err != nil {
				errCh <- err
				return
			}
			defer cli.Close()
			for i := 0; i < perWriter; i++ {
				ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
				_, err := cli.Put(ctx, key, fmt.Sprintf("w%d-i%d", w, i))
				cancel()
				if err != nil {
					errCh <- fmt.Errorf("writer %d put %d: %w", w, i, err)
					return
				}
			}
		}(w)
	}
	wg.Wait()
	close(errCh)

	t.Cleanup(func() {
		cli, err := clientv3.New(clientv3.Config{Endpoints: []string{compatEndpoint(t)}, DialTimeout: 5 * time.Second})
		if err == nil {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			_, _ = cli.Delete(ctx, key)
			cancel()
			_ = cli.Close()
		}
	})

	var failures int
	for err := range errCh {
		failures++
		if failures <= 5 {
			t.Errorf("concurrent Put failed (etcd Put must never fail on conflict): %v", err)
		}
	}
	if failures > 0 {
		t.Fatalf("%d/%d concurrent Puts failed", failures, writers*perWriter)
	}
}
