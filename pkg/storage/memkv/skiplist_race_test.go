// Copyright 2026 ByteDance and/or its affiliates
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package memkv

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestConcurrentGetIterWriteNoRace pins #46: memkv Get and Iter (which inserts a
// sentry placeholder into the shared skiplist) must be synchronized against
// concurrent writes so a reader never observes the sentry or a corrupted skiplist.
// Run under -race.
func TestConcurrentGetIterWriteNoRace(t *testing.T) {
	s := NewKvStorage()
	defer s.Close()
	ctx := context.Background()

	// seed
	for i := 0; i < 50; i++ {
		b := s.BeginBatchWrite()
		b.Put([]byte(fmt.Sprintf("/k/%03d", i)), []byte("v"), 0)
		require.NoError(t, b.Commit(ctx))
	}

	var wg sync.WaitGroup
	stop := make(chan struct{})
	worker := func(fn func()) {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
				fn()
			}
		}
	}
	for i := 0; i < 4; i++ {
		wg.Add(3)
		go worker(func() { _, _ = s.Get(ctx, []byte("/k/010")) })
		go worker(func() {
			it, err := s.Iter(ctx, []byte("/k/"), []byte("/k0"), 0, 0)
			if err != nil {
				return
			}
			for it.Next(ctx) == nil {
				_ = it.Key()
				_ = it.Val()
			}
			_ = it.Close()
		})
		n := i
		go worker(func() {
			b := s.BeginBatchWrite()
			b.Put([]byte(fmt.Sprintf("/k/w%02d", n)), []byte("v"), 0)
			_ = b.Commit(ctx)
		})
	}
	// let them interleave briefly
	done := make(chan struct{})
	go func() {
		for i := 0; i < 2000; i++ {
			_, _ = s.Get(ctx, []byte("/k/020"))
		}
		close(done)
	}()
	<-done
	close(stop)
	wg.Wait()
}
