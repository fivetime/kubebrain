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

package etcd

import (
	"context"
	"strconv"
	"sync"

	"go.etcd.io/etcd/api/v3/mvccpb"

	"github.com/kubewharf/kubebrain/pkg/backend"
)

// revKeyCacheCap bounds each generation of the (key,revision) caches. A watch
// fanout has W watcher streams converting the SAME event within a short window,
// so a modest cache coalesces their lookups; two generations bound memory.
const revKeyCacheCap = 8192

// revKeyCache is a tiny bounded cache keyed by (key,revision). The metadata and
// previous value of a specific key version are IMMUTABLE, so entries never go
// stale — the only concern is bounding memory, handled by rotating two
// generations when the live one fills.
type revKeyCache struct {
	mu   sync.Mutex
	cap  int
	cur  map[string]interface{}
	prev map[string]interface{}
}

func newRevKeyCache(capacity int) *revKeyCache {
	return &revKeyCache{cap: capacity, cur: make(map[string]interface{}, capacity), prev: map[string]interface{}{}}
}

func (c *revKeyCache) get(k string) (interface{}, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if v, ok := c.cur[k]; ok {
		return v, true
	}
	if v, ok := c.prev[k]; ok {
		c.cur[k] = v // promote so it survives the next rotation
		return v, true
	}
	return nil, false
}

func (c *revKeyCache) put(k string, v interface{}) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.cur) >= c.cap {
		c.prev = c.cur
		c.cur = make(map[string]interface{}, c.cap)
	}
	c.cur[k] = v
}

func revCacheKey(key []byte, revision uint64) string {
	return string(key) + "@" + strconv.FormatUint(revision, 10)
}

// cachedMetadata resolves create_revision/version for (key,revision), coalescing
// the identical lookups that every watcher stream would otherwise issue for the
// same event (the #10/#28 watch-fanout read amplification).
func (b *backendShim) cachedMetadata(ctx context.Context, key []byte, revision uint64) (backend.EtcdMetadata, error) {
	ck := revCacheKey(key, revision)
	if v, ok := b.metaCache.get(ck); ok {
		return v.(backend.EtcdMetadata), nil
	}
	v, err, _ := b.metaFlight.Do(ck, func() (interface{}, error) {
		m, e := b.backend.GetEtcdMetadata(ctx, key, revision)
		if e != nil {
			return backend.EtcdMetadata{}, e
		}
		b.metaCache.put(ck, m)
		return m, nil
	})
	if err != nil {
		return backend.EtcdMetadata{}, err
	}
	return v.(backend.EtcdMetadata), nil
}

// cachedPreviousEtcdKv resolves the previous value for (key,revision) once and
// shares it across watcher streams. The returned *mvccpb.KeyValue is treated as
// read-only by callers (it may be shared), matching the immutability of a past
// key version.
func (b *backendShim) cachedPreviousEtcdKv(key []byte, revision uint64) *mvccpb.KeyValue {
	if revision == 0 {
		return nil
	}
	ck := revCacheKey(key, revision)
	if v, ok := b.prevCache.get(ck); ok {
		return v.(*mvccpb.KeyValue)
	}
	v, _, _ := b.prevFlight.Do(ck, func() (interface{}, error) {
		// previousEtcdKv uses its own bounded (Background) context.
		pk := b.previousEtcdKv(context.Background(), key, revision)
		b.prevCache.put(ck, pk)
		return pk, nil
	})
	return v.(*mvccpb.KeyValue)
}
