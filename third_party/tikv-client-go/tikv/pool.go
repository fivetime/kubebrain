// Copyright 2023 TiKV Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package tikv

import (
	"errors"
	"sync"
	"time"

	"github.com/tiancaiamao/gp"
)

// Pool is a simple interface for goroutine pool.
type Pool interface {
	Run(func()) error
	Close()
}

// Spool is a simple implementation of Pool.
type Spool struct {
	gp.Pool
	mu     sync.Mutex
	wg     sync.WaitGroup
	closed bool
}

// NewSpool creates a new Spool.
func NewSpool(n int, dur time.Duration) *Spool {
	return &Spool{
		Pool: *gp.New(n, dur),
	}
}

// Run implements Pool.Run.
func (p *Spool) Run(fn func()) error {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return errors.New("goroutine pool is closed")
	}
	p.wg.Add(1)
	p.Go(func() {
		defer p.wg.Done()
		fn()
	})
	p.mu.Unlock()
	return nil
}

// Close rejects new work and waits for every task accepted by Run. The
// embedded pool only stops idle workers; without this barrier KVStore could
// close PD/RPC dependencies while an in-flight task was still using them.
func (p *Spool) Close() {
	p.mu.Lock()
	if !p.closed {
		p.closed = true
		p.Pool.Close()
	}
	p.mu.Unlock()
	p.wg.Wait()
}
