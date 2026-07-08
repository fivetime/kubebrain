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
	"time"

	"go.etcd.io/etcd/api/v3/mvccpb"

	proto "github.com/kubewharf/kubebrain-client/api/v2rpc"
	"github.com/kubewharf/kubebrain/pkg/backend"
)

// prevKvRetryBudget bounds how long one previous-value lookup keeps retrying
// transient storage failures before giving up with an uncertain nil (#36).
// Generous on purpose: an uncertain nil on an update event makes the apiserver
// terminate every watcher of that resource and re-list, which at scale costs
// far more than blocking this one event stream a few seconds.
var prevKvRetryBudget = 5 * time.Second // var for tests

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

// prevHintCacheCap bounds each generation of the per-key previous-version hint
// cache. Sized for the hot-key working set (node heartbeats, controller status
// loops); entries hold pointers to already-materialized KeyValues.
const prevHintCacheCap = 65536

// prevHintEntry records the LAST converted watch event seen for a user key.
type prevHintEntry struct {
	rev       uint64
	kv        *mvccpb.KeyValue // nil for tombstones
	tombstone bool
}

// prevHintCache maps user key -> its latest converted watch event, with
// REVISION-MONOTONIC writes. The watch event stream contains every write in
// revision order, so for a PUT at revision r the previous version is exactly
// the key's latest event below r. Monotonicity is what makes the hint safe
// under concurrent watcher streams at different progress points: a slower
// stream can never overwrite a newer hint with an older one, and a hit is
// taken only when hint.rev < r — at that point the querying stream has itself
// converted (and hence hint-published, monotonic) every event of that key
// below r, so hint.rev IS the previous version. If any event >= r has been
// published, hint.rev >= r and the lookup falls back to the slow path.
//
// This removes the per-PUT revisioned TiKV Get from the watch conversion hot
// path. etcd resolves PrevKv with a local in-memory read, so laziness is free
// there; here each miss crosses the network (~10-20ms), which serialized the
// stream at ~60 events/s and let the committed watermark outrun every cacher
// (#45). Misses (cold keys, cache eviction) still take the slow path, which
// remains correct on its own.
type prevHintCache struct {
	mu   sync.Mutex
	cap  int
	cur  map[string]prevHintEntry
	prev map[string]prevHintEntry
}

func newPrevHintCache(capacity int) *prevHintCache {
	return &prevHintCache{cap: capacity, cur: make(map[string]prevHintEntry, capacity), prev: map[string]prevHintEntry{}}
}

func (c *prevHintCache) get(key string) (prevHintEntry, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if e, ok := c.cur[key]; ok {
		return e, true
	}
	if e, ok := c.prev[key]; ok {
		c.cur[key] = e // promote so it survives the next rotation
		return e, true
	}
	return prevHintEntry{}, false
}

// note records a converted event, keeping only the highest revision per key.
func (c *prevHintCache) note(key string, rev uint64, kv *mvccpb.KeyValue, tombstone bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if e, ok := c.cur[key]; ok && e.rev >= rev {
		return
	}
	if e, ok := c.prev[key]; ok && e.rev >= rev {
		// A newer entry exists in the old generation; promote it instead of
		// letting a stale write shadow it in cur.
		c.cur[key] = e
		return
	}
	if len(c.cur) >= c.cap {
		c.prev = c.cur
		c.cur = make(map[string]prevHintEntry, c.cap)
	}
	c.cur[key] = prevHintEntry{rev: rev, kv: kv, tombstone: tombstone}
}

// noteEvent publishes a converted watch event into the hint cache.
func (b *backendShim) noteEvent(key []byte, rev uint64, kv *mvccpb.KeyValue, tombstone bool) {
	b.prevHints.note(string(key), rev, kv, tombstone)
}

// hintedPreviousEtcdKv answers a PrevKv lookup from the hint cache when the
// key's latest published event sits below revision (see prevHintCache).
// tombstone hints are never a valid previous version for a PUT (a CREATE must
// sit in between, which would have republished the hint), so they miss.
func (b *backendShim) hintedPreviousEtcdKv(key []byte, revision uint64) (*mvccpb.KeyValue, bool) {
	e, ok := b.prevHints.get(string(key))
	if !ok || e.tombstone || e.rev >= revision {
		return nil, false
	}
	b.metricCli.EmitCounter("watch.prev_kv.hint_hit", 1)
	return e.kv, true
}

// prefetchPrevKvsConcurrency bounds the parallel cold-key PrevKv resolutions
// per event batch; TiKV point gets tolerate this fan-out comfortably.
const prefetchPrevKvsConcurrency = 16

// prefetchPrevKvs warms the prev-kv cache for every PUT event in the batch
// whose previous version is neither hinted nor cached, issuing the revisioned
// gets in parallel. cachedPreviousEtcdKv is singleflighted and idempotent, so
// the sequential conversion that follows hits the warmed cache.
func (b *backendShim) prefetchPrevKvs(events []*proto.Event) {
	var cold []*proto.Event
	for _, e := range events {
		if e == nil || e.Kv == nil || e.Type != proto.Event_PUT {
			continue
		}
		rev := watchEventRevision(e)
		if rev == 0 {
			continue
		}
		if _, ok := b.hintedPreviousEtcdKv(e.Kv.Key, rev); ok {
			continue
		}
		if _, ok := b.prevCache.get(revCacheKey(e.Kv.Key, rev)); ok {
			continue
		}
		cold = append(cold, e)
	}
	if len(cold) <= 1 {
		return
	}
	sem := make(chan struct{}, prefetchPrevKvsConcurrency)
	var wg sync.WaitGroup
	for _, e := range cold {
		wg.Add(1)
		sem <- struct{}{}
		go func(ev *proto.Event) {
			defer wg.Done()
			defer func() { <-sem }()
			b.cachedPreviousEtcdKv(ev.Kv.Key, watchEventRevision(ev))
		}(e)
	}
	wg.Wait()
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
	if kv, ok := b.hintedPreviousEtcdKv(key, revision); ok {
		return kv
	}
	ck := revCacheKey(key, revision)
	if v, ok := b.prevCache.get(ck); ok {
		return v.(*mvccpb.KeyValue)
	}
	v, _, _ := b.prevFlight.Do(ck, func() (interface{}, error) {
		// previousEtcdKv uses its own bounded (Background) context. Only a
		// CERTAIN answer (value / clean miss / compacted) may be cached: caching
		// a transient-failure nil would hand the poisoned nil to every watcher
		// stream converting the same event and amplify one storage hiccup into
		// a cacher re-list storm (#36).
		pk, certain := b.previousEtcdKv(context.Background(), key, revision)
		if certain {
			b.prevCache.put(ck, pk)
		}
		return pk, nil
	})
	return v.(*mvccpb.KeyValue)
}
