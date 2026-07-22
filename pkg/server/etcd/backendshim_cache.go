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
	"golang.org/x/sync/singleflight"

	proto "github.com/kubewharf/kubebrain-client/api/v2rpc"
	"github.com/kubewharf/kubebrain/pkg/backend"
)

// prevKvResolver owns the watch-fanout read-amplification caches and the logic
// that resolves an event's previous value / inline metadata: the hint cache
// (answers PrevKv straight from the event stream), the prev-value and metadata
// (key,revision) caches, and their singleflight groups. Extracted from the
// backendShim God object (audit A2); backendShim embeds a *prevKvResolver so its
// methods stay promoted. Storage access (Get, backend, metrics) is borrowed from
// the owning shim via srv rather than copied, so there is one source of truth.
type prevKvResolver struct {
	shim *backendShim

	metaCache  *revKeyCache
	metaFlight singleflight.Group
	prevCache  *revKeyCache
	prevFlight singleflight.Group
	// prevHints answers watch PrevKv lookups from the event stream itself (the
	// key's last converted event IS the previous version), removing the per-PUT
	// revisioned TiKV Get that serialized watch conversion at ~60 events/s.
	prevHints *prevHintCache
}

func newPrevKvResolver(shim *backendShim) *prevKvResolver {
	return &prevKvResolver{
		shim:      shim,
		metaCache: newRevKeyCache(revKeyCacheCap, revKeyCacheMaxBytes),
		prevCache: newRevKeyCache(revKeyCacheCap, revKeyCacheMaxBytes),
		prevHints: newPrevHintCache(prevHintCacheCap, prevHintCacheMaxBytes),
	}
}

// prevKvRetryBudget bounds how long one previous-value lookup keeps retrying
// transient storage failures before giving up with an uncertain nil (#36).
// Generous on purpose: an uncertain nil on an update event makes the apiserver
// terminate every watcher of that resource and re-list, which at scale costs
// far more than blocking this one event stream a few seconds.
var prevKvRetryBudget = 5 * time.Second // var for tests

// revKeyCacheCap bounds each generation of the (key,revision) caches by ENTRY
// COUNT. A watch fanout has W watcher streams converting the SAME event within a
// short window, so a modest cache coalesces their lookups; two generations bound
// memory.
const revKeyCacheCap = 8192

// revKeyCacheMaxBytes bounds each generation of the (key,revision) caches by
// accumulated VALUE BYTES. The prev-value cache holds *mvccpb.KeyValue, so a
// working set of large objects (secrets/configmaps up to ~1.5MB) could reach
// gigabytes on the entry-count bound alone. Whichever bound is hit first rotates
// the generation (#66-adjacent review-51 leftover: hint/value caches had a count
// cap but no byte cap).
const revKeyCacheMaxBytes = 64 << 20 // 64 MiB per generation

// cacheEntryOverhead approximates the fixed per-entry cost the value-byte
// accounting cannot see (map bucket + string header + interface header). Small
// and constant; it only needs to keep the byte bound from wildly under-counting
// many tiny entries.
const cacheEntryOverhead = 48

// maxCacheMapInitialCapacity keeps a configuration or test with a very large
// entry bound from allocating that many map buckets before the byte bound has
// admitted a single entry. Maps still grow up to cap as needed; this only
// limits eager allocation for the current generation and its rotations.
const maxCacheMapInitialCapacity = 8192

func newRevCacheMap(capacity int) map[string]revCacheEntry {
	return make(map[string]revCacheEntry, min(capacity, maxCacheMapInitialCapacity))
}

// revKeyCache is a tiny bounded cache keyed by (key,revision). The metadata and
// previous value of a specific key version are IMMUTABLE, so entries never go
// stale — the only concern is bounding memory, handled by rotating two
// generations when the live one hits EITHER its entry-count or byte bound.
type revKeyCache struct {
	mu       sync.Mutex
	cap      int
	maxBytes int64
	curBytes int64
	cur      map[string]revCacheEntry
	prev     map[string]revCacheEntry
}

// revCacheEntry pairs a cached value with the caller-supplied byte size used for
// the generation's memory bound.
type revCacheEntry struct {
	v    interface{}
	size int64
}

func newRevKeyCache(capacity int, maxBytes int64) *revKeyCache {
	return &revKeyCache{cap: capacity, maxBytes: maxBytes, cur: newRevCacheMap(capacity), prev: map[string]revCacheEntry{}}
}

func (c *revKeyCache) get(k string) (interface{}, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if e, ok := c.cur[k]; ok {
		return e.v, true
	}
	if e, ok := c.prev[k]; ok {
		c.setCurLocked(k, e) // promote so it survives the next rotation
		return e.v, true
	}
	return nil, false
}

// put caches v under k. size is the caller's estimate of v's byte footprint,
// used for the byte bound; pass 0 for constant-size values.
func (c *revKeyCache) put(k string, v interface{}, size int64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	entryBytes := int64(len(k)) + size + cacheEntryOverhead
	// Rotate when either bound is hit. The len>0 guard lets a single oversized
	// entry still be stored (in an otherwise-empty generation) instead of
	// rotating forever.
	if len(c.cur) >= c.cap || (c.curBytes+entryBytes > c.maxBytes && len(c.cur) > 0) {
		c.rotateLocked()
	}
	c.setCurLocked(k, revCacheEntry{v: v, size: size})
}

func (c *revKeyCache) setCurLocked(k string, e revCacheEntry) {
	if old, ok := c.cur[k]; ok {
		c.curBytes -= int64(len(k)) + old.size + cacheEntryOverhead
	}
	c.cur[k] = e
	c.curBytes += int64(len(k)) + e.size + cacheEntryOverhead
}

func (c *revKeyCache) rotateLocked() {
	c.prev = c.cur
	c.cur = newRevCacheMap(c.cap)
	c.curBytes = 0
}

func revCacheKey(key []byte, revision uint64) string {
	return string(key) + "@" + strconv.FormatUint(revision, 10)
}

// prevHintCacheCap bounds each generation of the per-key previous-version hint
// cache by ENTRY COUNT. Sized for the hot-key working set (node heartbeats,
// controller status loops); entries hold pointers to already-materialized
// KeyValues.
const prevHintCacheCap = 65536

// prevHintCacheMaxBytes bounds each generation of the hint cache by accumulated
// VALUE BYTES. A hint holds a *mvccpb.KeyValue, so 65536 large objects would be
// tens of GB on the count bound alone; whichever bound is hit first rotates.
const prevHintCacheMaxBytes = 256 << 20 // 256 MiB per generation

// kvBytes estimates a KeyValue's heap footprint for the cache byte bounds: key +
// value payload plus a fixed overhead for its scalar fields and slice headers.
func kvBytes(kv *mvccpb.KeyValue) int64 {
	if kv == nil {
		return 0
	}
	return int64(len(kv.Key)+len(kv.Value)) + kvFixedOverhead
}

const kvFixedOverhead = 64

// prevHintEntry records the LAST converted watch event seen for a user key.
type prevHintEntry struct {
	rev       uint64
	kv        *mvccpb.KeyValue // nil for tombstones
	tombstone bool
}

// prevHintCache maps user key -> its latest converted watch event, with
// REVISION-MONOTONIC writes (a slower stream can never overwrite a newer hint
// with an older one).
//
// hint.rev < r alone does NOT prove the hint is the DIRECT predecessor of the
// event at r: this process's conversion coverage of a key routinely has holes
// (a cacher re-list window with no watcher attached, a follower term between
// two leaderships), and writes flow regardless — the hint may be several
// versions stale. A hit therefore additionally requires proof from the value
// envelope's inline metadata: the querying event and the hint share a
// CreateRevision (same key lifetime — CreateRevision is a unique TSO, so
// lifetimes cannot collide) and the hint's Version is exactly one less than
// the querying event's (versions advance by one per write within a lifetime).
// Events without trustworthy inline metadata (legacy values) never hit.
//
// This removes the per-PUT revisioned TiKV Get from the watch conversion hot
// path. etcd resolves PrevKv with a local in-memory read, so laziness is free
// there; here each miss crosses the network (~10-20ms), which serialized the
// stream at ~60 events/s and let the committed watermark outrun every cacher
// (#45). Misses (cold keys, cache eviction) still take the slow path, which
// remains correct on its own.
type prevHintCache struct {
	mu       sync.Mutex
	cap      int
	maxBytes int64
	curBytes int64
	cur      map[string]prevHintEntry
	prev     map[string]prevHintEntry
}

func newPrevHintMap(capacity int) map[string]prevHintEntry {
	return make(map[string]prevHintEntry, min(capacity, maxCacheMapInitialCapacity))
}

func newPrevHintCache(capacity int, maxBytes int64) *prevHintCache {
	return &prevHintCache{cap: capacity, maxBytes: maxBytes, cur: newPrevHintMap(capacity), prev: map[string]prevHintEntry{}}
}

func hintEntryBytes(key string, e prevHintEntry) int64 {
	return int64(len(key)) + kvBytes(e.kv) + cacheEntryOverhead
}

func (c *prevHintCache) get(key string) (prevHintEntry, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if e, ok := c.cur[key]; ok {
		return e, true
	}
	if e, ok := c.prev[key]; ok {
		c.setCurLocked(key, e) // promote so it survives the next rotation
		return e, true
	}
	return prevHintEntry{}, false
}

func (c *prevHintCache) setCurLocked(key string, e prevHintEntry) {
	if old, ok := c.cur[key]; ok {
		c.curBytes -= hintEntryBytes(key, old)
	}
	c.cur[key] = e
	c.curBytes += hintEntryBytes(key, e)
}

func (c *prevHintCache) rotateLocked() {
	c.prev = c.cur
	c.cur = newPrevHintMap(c.cap)
	c.curBytes = 0
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
		c.setCurLocked(key, e)
		return
	}
	e := prevHintEntry{rev: rev, kv: kv, tombstone: tombstone}
	// Rotate when either bound is hit (entry count or accumulated bytes); the
	// len>0 guard lets a single oversized hint still be stored.
	if len(c.cur) >= c.cap || (c.curBytes+hintEntryBytes(key, e) > c.maxBytes && len(c.cur) > 0) {
		c.rotateLocked()
	}
	c.setCurLocked(key, e)
}

// noteEvent publishes a converted watch event into the hint cache.
func (r *prevKvResolver) noteEvent(key []byte, rev uint64, kv *mvccpb.KeyValue, tombstone bool) {
	r.prevHints.note(string(key), rev, kv, tombstone)
}

// hintedPreviousEtcdKv answers a PrevKv lookup from the hint cache when the
// hint is provably the DIRECT predecessor of the querying event: same key
// lifetime (equal CreateRevision) and consecutive Version (see prevHintCache —
// hint.rev < revision alone is not proof, conversion coverage has holes).
// version/createRev describe the QUERYING event, from its inline metadata;
// pass zeros when unknown, which always misses. tombstone hints are never a
// valid previous version for a PUT (a CREATE must sit in between, which would
// have republished the hint), so they miss.
func (r *prevKvResolver) hintedPreviousEtcdKv(key []byte, revision uint64, version, createRev int64) (*mvccpb.KeyValue, bool) {
	if version < 2 || createRev <= 0 {
		return nil, false
	}
	e, ok := r.prevHints.get(string(key))
	if !ok || e.tombstone || e.kv == nil || e.rev >= revision {
		return nil, false
	}
	if e.kv.Version != version-1 || e.kv.CreateRevision != createRev {
		return nil, false
	}
	return e.kv, true
}

// inlineVersionMeta decodes the Version/CreateRevision a raw stored value
// carries in its inline envelope, for the hint cache's direct-predecessor
// proof. Legacy (un-enveloped) values yield zeros, which never hit.
func inlineVersionMeta(value []byte) (version, createRev int64) {
	meta, _, inlined := backend.DecodeInlineValue(value)
	if !inlined {
		return 0, 0
	}
	return int64(meta.Version), int64(meta.CreateRevision)
}

// prefetchPrevKvsConcurrency bounds the parallel cold-key PrevKv resolutions
// per event batch; TiKV point gets tolerate this fan-out comfortably.
const prefetchPrevKvsConcurrency = 16

// prefetchPrevKvs warms the prev-kv cache for every PUT event in the batch
// whose previous version is neither hinted nor cached, issuing the revisioned
// gets in parallel. cachedPreviousEtcdKv is singleflighted and idempotent, so
// the sequential conversion that follows hits the warmed cache.
func (r *prevKvResolver) prefetchPrevKvs(events []*proto.Event) {
	var cold []*proto.Event
	for _, e := range events {
		if e == nil || e.Kv == nil || e.Type != proto.Event_PUT {
			continue
		}
		rev := watchEventRevision(e)
		if rev == 0 {
			continue
		}
		ver, crev := inlineVersionMeta(e.Kv.Value)
		if _, ok := r.hintedPreviousEtcdKv(e.Kv.Key, rev, ver, crev); ok {
			continue
		}
		if _, ok := r.prevCache.get(revCacheKey(e.Kv.Key, rev)); ok {
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
			// Zeros for the hint proof: these events already missed the hint in
			// the filter above, so go straight to the shared slow path.
			r.cachedPreviousEtcdKv(ev.Kv.Key, watchEventRevision(ev), 0, 0)
		}(e)
	}
	wg.Wait()
}

// cachedMetadata resolves create_revision/version for (key,revision), coalescing
// the identical lookups that every watcher stream would otherwise issue for the
// same event (the #10/#28 watch-fanout read amplification).
func (r *prevKvResolver) cachedMetadata(ctx context.Context, key []byte, revision uint64) (backend.EtcdMetadata, error) {
	ck := revCacheKey(key, revision)
	if v, ok := r.metaCache.get(ck); ok {
		return v.(backend.EtcdMetadata), nil
	}
	v, err, _ := r.metaFlight.Do(ck, func() (interface{}, error) {
		m, e := r.shim.backend.GetEtcdMetadata(ctx, key, revision)
		if e != nil {
			return backend.EtcdMetadata{}, e
		}
		r.metaCache.put(ck, m, 0) // EtcdMetadata is fixed-size scalars; overhead covers it
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
func (r *prevKvResolver) cachedPreviousEtcdKv(key []byte, revision uint64, version, createRev int64) *mvccpb.KeyValue {
	if revision == 0 {
		return nil
	}
	if kv, ok := r.hintedPreviousEtcdKv(key, revision, version, createRev); ok {
		r.shim.metricCli.EmitCounter("watch.prev_kv.hint_hit", 1)
		return kv
	}
	ck := revCacheKey(key, revision)
	if v, ok := r.prevCache.get(ck); ok {
		return v.(*mvccpb.KeyValue)
	}
	v, _, _ := r.prevFlight.Do(ck, func() (interface{}, error) {
		// previousEtcdKv uses its own bounded (Background) context. Only a
		// CERTAIN answer (value / clean miss / compacted) may be cached: caching
		// a transient-failure nil would hand the poisoned nil to every watcher
		// stream converting the same event and amplify one storage hiccup into
		// a cacher re-list storm (#36).
		pk, certain := r.previousEtcdKv(context.Background(), key, revision)
		if certain {
			r.prevCache.put(ck, pk, kvBytes(pk))
		}
		return pk, nil
	})
	return v.(*mvccpb.KeyValue)
}
