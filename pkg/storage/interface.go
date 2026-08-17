// Copyright 2022 ByteDance and/or its affiliates
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

package storage

import (
	"context"
	"fmt"
	"time"
)

type snapshotTimestampContextKey struct{}
type protectedSnapshotContextKey struct{}
type snapshotIteratorFallbackContextKey struct{}

// WithSnapshotTimestamp pins storage reads in ctx to an engine snapshot.
// This alone does not assert that the snapshot is GC-protected or that all of
// its Region routes were pre-warmed; ordinary historical and multi-step reads
// must remain able to consult the topology service on cache misses.
func WithSnapshotTimestamp(ctx context.Context, timestamp uint64) context.Context {
	return context.WithValue(ctx, snapshotTimestampContextKey{}, timestamp)
}

// WithProtectedSnapshotTimestamp additionally asserts that timestamp is held
// above engine GC and every required Region route was warmed and protected.
// Storage adapters may use cache-only stale replica routing only under this
// stronger context contract.
func WithProtectedSnapshotTimestamp(ctx context.Context, timestamp uint64) context.Context {
	ctx = WithSnapshotTimestamp(ctx, timestamp)
	return context.WithValue(ctx, protectedSnapshotContextKey{}, true)
}

func SnapshotTimestampFromContext(ctx context.Context) (uint64, bool) {
	timestamp, ok := ctx.Value(snapshotTimestampContextKey{}).(uint64)
	return timestamp, ok && timestamp != 0
}

func ProtectedSnapshotFromContext(ctx context.Context) bool {
	protected, _ := ctx.Value(protectedSnapshotContextKey{}).(bool)
	return protected
}

// WithSnapshotIteratorFallback permits an exact timestamped iterator when an
// optional SnapshotGetter capability is hidden by a storage decorator. Keep
// this scoped to short internal recovery reads; public serializable snapshots
// intentionally fail closed when their point-read capability is unavailable.
func WithSnapshotIteratorFallback(ctx context.Context) context.Context {
	return context.WithValue(ctx, snapshotIteratorFallbackContextKey{}, true)
}

func SnapshotIteratorFallbackFromContext(ctx context.Context) bool {
	enabled, _ := ctx.Value(snapshotIteratorFallbackContextKey{}).(bool)
	return enabled
}

// ExclusiveKvStorage defines the context individual KvStorage for the background job.
// * Leader runs background compaction periodically, which will may lead to high network io throughput on several tcp
// * conns of a single client. In this case, background compaction can make a notable impact on the latency of writing
// * requests which shares the same tcp conns with compact.
// * Thus, as an optional optimization, implement this interface besides KvStorage to get an exclusive one to reduce the
// * side effect of background compaction.
type ExclusiveKvStorage interface {
	// GetExclusiveKvStorage returns an exclusive KvStorage with exclusive client for operation of high io throughput in leader
	GetExclusiveKvStorage() KvStorage
}

// KvStorageUnwrapper is implemented by decorators that retain an underlying
// KvStorage. Optional capabilities cannot be promoted dynamically through a Go
// interface wrapper without defining every combination of capability methods;
// FindCapability follows this chain instead.
type KvStorageUnwrapper interface {
	UnwrapKvStorage() KvStorage
}

// FindCapability returns the first implementation of T on store or one of its
// decorator layers. It centralizes optional-capability discovery so wrappers do
// not silently disable GC, isolated scan clients, batch reads, or cluster IDs.
func FindCapability[T any](store KvStorage) (T, bool) {
	var zero T
	for depth := 0; store != nil && depth < 32; depth++ {
		if capability, ok := any(store).(T); ok {
			return capability, true
		}
		unwrapper, ok := any(store).(KvStorageUnwrapper)
		if !ok {
			return zero, false
		}
		next := unwrapper.UnwrapKvStorage()
		if next == nil {
			return zero, false
		}
		store = next
	}
	return zero, false
}

// GarbageCollector is an optional interface a KvStorage may implement to
// advance the underlying engine's MVCC garbage-collection safepoint. Engines
// whose old versions are reclaimed by an external component (or that keep no
// version history) simply do not implement it.
//
// KubeBrain encodes its own MVCC into keys and reads every snapshot at the
// CURRENT timestamp (all Iter calls pass ts=0), so it never needs engine-level
// history. But on TiKV every CAS overwrite of a revision key leaves an MVCC
// version that is only reclaimed once the cluster GC safepoint advances past
// it — and on a bare PD+TiKV deployment NOTHING advances that safepoint (it is
// TiDB's gc_worker that normally does), so versions pile up forever and every
// read degrades as it skips them (observed live: single-key GETs at 100ms+
// with gc_safe_point=0). Implementations advance the safepoint to
// now-lifetime, resolving stale transaction locks first as correctness
// requires.
type GarbageCollector interface {
	// GC advances the engine's GC safepoint to (now - lifetime) and returns
	// the new cluster safepoint. lifetime bounds the longest in-flight
	// snapshot/transaction the caller may still have outstanding.
	GC(ctx context.Context, lifetime time.Duration) (safepoint uint64, err error)
}

// ClusterIdentifier is optionally implemented by storages backed by a real
// cluster with a stable identity (TiKV: the PD cluster ID). etcd stamps a
// ClusterId on every response header, and etcd tooling (e.g. Cilium's
// clustermesh interceptors, cilium-dbg) uses it to detect talking to the wrong
// cluster; surfacing the real PD identity gives that check teeth (#78).
type ClusterIdentifier interface {
	// ClusterID returns the underlying cluster's stable identity.
	ClusterID() uint64
}

// KvStorage defines the storage engine on kv database.
type KvStorage interface {

	// GetTimestampOracle returns the logical timestamp if it could support
	// todo: deprecate it after KubeBrain native tso is implemented
	GetTimestampOracle(ctx context.Context) (timestamp uint64, err error)

	// GetPartitions returns the partitions that keys are spread over in
	// If it's not supported, just return [][]byte{start, end}, 0 , nil
	GetPartitions(ctx context.Context, start, end []byte) (partitions []Partition, err error)

	// Get returns value indexed by key
	// If it's not exist, return ErrKeyNotFound
	Get(ctx context.Context, key []byte) (val []byte, err error)

	// Iter get keys from `start` to `end` (`end` will be smaller than `start` if SupportIterForward is true)
	// todo: if refactor the format and procedure of write data, all iter may be from a smaller beginning to a lager ending.
	Iter(ctx context.Context, start []byte, end []byte, timestamp uint64, limit uint64) (Iter, error)

	// BeginBatchWrite returns a new BeginBatchWrite instance
	BeginBatchWrite() BatchWrite

	Writer

	FeatureSupport

	// Close the kv storage
	Close() error
}

// BatchGetter is an OPTIONAL KvStorage capability: fetch many keys in a single
// round trip. Backends that implement it (TiKV, via a snapshot BatchGet that the
// client batches by region and issues concurrently) let callers replace N
// sequential point reads with one batched read. Event-log watch-history replay
// uses this to avoid a per-event Get — each of which, on TiKV, is a full
// begin/commit transaction (a TSO plus two round trips), so a large catch-up
// window otherwise serializes into thousands of tiny transactions at bounded
// concurrency (the #43 replay-throughput bottleneck). Callers MUST type-assert
// and fall back to per-key Get when a backend does not implement it. The returned
// map is keyed by string(key) and omits keys that do not exist (a missing key is
// not an error; the caller decides what an absent key means).
type BatchGetter interface {
	BatchGet(ctx context.Context, keys [][]byte) (map[string][]byte, error)
}

// SnapshotGetter is an OPTIONAL KvStorage capability for reads at a caller-
// supplied engine snapshot timestamp. Unlike Get and BatchGet, these methods
// must not obtain a fresh timestamp from the storage oracle. A DBaaS data plane
// can therefore keep serving a previously established serializable checkpoint
// while its PD/TSO path is temporarily unavailable, provided that checkpoint
// is still protected from engine GC.
//
// The timestamp is storage-engine specific and must have been obtained from
// the same storage cluster. Callers MUST NOT infer one from an etcd MVCC
// revision: KubeBrain revisions and TiKV timestamps are independent sequences.
type SnapshotGetter interface {
	GetAt(ctx context.Context, key []byte, timestamp uint64) ([]byte, error)
	BatchGetAt(ctx context.Context, keys [][]byte, timestamp uint64) (map[string][]byte, error)
}

// SnapshotRegionWarmer is an OPTIONAL capability for keeping the storage
// client's routing caches usable without its topology service. starts must be
// physical keys that cover every Region needed by the caller. Implementations
// with multiple independent routing caches must touch every cache, not merely
// the next round-robin client. The method must not discover partitions itself:
// routing-cache population is separated from topology discovery so it cannot
// silently omit one of the caller's already-authoritative starts.
type SnapshotRegionWarmer interface {
	WarmSnapshotRegions(ctx context.Context, starts [][]byte, timestamp uint64) error
}

// SnapshotReadinessValidator is an OPTIONAL capability for proving that an
// engine snapshot can survive the storage failure model promised by the
// implementation. Unlike Region warming, validation is timestamp-specific and
// must be run for every candidate checkpoint before publication.
type SnapshotReadinessValidator interface {
	SnapshotReadyTimestamp(ctx context.Context, start, end []byte) (timestamp uint64, err error)
}

// SnapshotPublicationValidator is an OPTIONAL final fence for a routing
// directory refresh. It revalidates that timestamp remains safe for the current
// Region/Store topology after all routing caches have been warmed but before a
// caller publishes the checkpoint locally. Implementations must fail closed if
// topology changes during validation or if any required voter has not reached
// timestamp.
type SnapshotPublicationValidator interface {
	ValidateSnapshotPublication(ctx context.Context, start, end []byte, timestamp uint64) error
}

// SnapshotProtector is an OPTIONAL capability for pinning an engine snapshot
// against MVCC garbage collection. serviceID identifies one live consumer;
// implementations must expire the protection after ttl unless it is renewed.
// The returned value is the cluster-wide minimum service safepoint. A caller
// must reject timestamp when that minimum has already advanced past it.
type SnapshotProtector interface {
	ProtectSnapshot(ctx context.Context, serviceID string, ttl time.Duration, timestamp uint64) (minimum uint64, err error)
	ReleaseSnapshot(ctx context.Context, serviceID string) error
}

// FeatureSupport indicates whether storage engine support some non-core feature
type FeatureSupport interface {
	SupportTTL() bool
}

// Writer defines some methods to modify kv in storage engine
// it aims to avoid multirow transaction if possible when write one key only
// it can be implemented by wrapping the BatchWrite
type Writer interface {
	// Del removes kv from kv storage
	Del(ctx context.Context, key []byte) (err error)

	// DelCurrent removes kv ref by Iter
	// Should be implemented as delete-if-value-equal or delete-if-version-equal
	DelCurrent(ctx context.Context, iter Iter) (err error)
}

// AtomicBatch exposes reads and writes performed inside the transaction owned
// by BatchWrite. It is intentionally smaller than KvStorage: callers may derive
// keys and values from data read by the same commit, but cannot open snapshots
// or recursively commit another transaction.
type AtomicBatch interface {
	// Get reads through the transaction snapshot. Callers must not assume a read
	// alone participates in commit conflict detection: TiKV optimistic 2PC checks
	// mutation keys, whereas Badger also tracks reads. Stage Put/Del on safety
	// guards that must conflict with concurrent writers.
	Get(ctx context.Context, key []byte) ([]byte, error)
	Put(key []byte, val []byte, ttl int64) error
	Del(key []byte) error
}

// BatchWrite should support atomic batch pack with several operations
type BatchWrite interface {

	// PutIfNotExist creates kv if it doesn't exist.
	// If it exists, return ErrCASFailed while committed
	// *If it's available, return a Conflict instead of ErrCASFailed while committed to pass unexpected kv
	PutIfNotExist(key []byte, val []byte, ttl int64)

	// CAS compare and swap the value indexed by given key
	// todo: deprecate it if refactor the format and procedure of write data
	// If result of compare is false, return ErrCASFailed while committed
	// * If it's available, return a Conflict instead of ErrCASFailed committed to pass unexpected kv
	CAS(key []byte, newVal []byte, oldVal []byte, ttl int64)

	// Put kv to storage
	Put(key []byte, val []byte, ttl int64)

	// Del remove kv from storage
	Del(key []byte)

	// DelCurrent is an ugly design to unify the cas deleting in different storage implement
	DelCurrent(it Iter)

	// Atomic runs during Commit against the same storage transaction as the
	// statically staged operations. Returning an error aborts the whole batch.
	// This enables transaction-local read-modify-write and dynamic key encoding.
	// Callers must stage it after every static operation whose value it reads.
	Atomic(func(context.Context, AtomicBatch) error)

	// Commit commits batch atomically, return the first error in batch
	// * must return ErrUncertainResult if client can not know whether data is written
	Commit(ctx context.Context) error
}

// Iter is the iterator on a **snapshot** of kv storage with batch buffer
// Example:
//
//	iter := kvStorage.Iter(start, end, snapshotID, false)
//	defer iter.Close()
//	iterCtx := context.WithTimeout(ctx, timeout)
//	for {
//		err := iter.Next(iterCtx)
//		if err != nil {
//			if err == io.EOF {
//				// come to the end
//			}
//			// unexpected error
//		}
//		 key, value := iter.Key(), iter.Val()
//		// processing ...
//	}
type Iter interface {
	// Key returns keys in buffer
	Key() []byte

	// Val returns values in buffer
	Val() []byte

	// Next get data from kv storage
	// err should be io.EOF if there is no more keys
	Next(ctx context.Context) (err error)

	// Close the iter
	Close() error
}

var (
	ErrUnsupported   = fmt.Errorf("unsupported")
	ErrKeyNotFound   = fmt.Errorf("not found")
	ErrKeyDuplicated = fmt.Errorf("key duplicated")
	ErrCASFailed     = fmt.Errorf("cas failed")
	ErrUnexpectedRet = fmt.Errorf("unexpected return")
	ErrUnavailable   = fmt.Errorf("unavailable")
	ErrKeyTooLarge   = fmt.Errorf("storage key too large")
)

// Partition indicates the boarder of an ordered key region `[Start, End)`
type Partition struct {
	// Start is the least key of the region
	Start []byte

	// End is the key only greater than keys in this partition, and it may be equal to `Start` of next partition
	End []byte
}
