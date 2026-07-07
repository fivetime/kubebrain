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

package backend

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"k8s.io/client-go/tools/leaderelection/resourcelock"
	"k8s.io/klog/v2"

	proto "github.com/kubewharf/kubebrain-client/api/v2rpc"

	"github.com/kubewharf/kubebrain/pkg/backend/coder"
	"github.com/kubewharf/kubebrain/pkg/backend/common"
	"github.com/kubewharf/kubebrain/pkg/backend/countindex"
	"github.com/kubewharf/kubebrain/pkg/backend/creator"
	"github.com/kubewharf/kubebrain/pkg/backend/election"
	"github.com/kubewharf/kubebrain/pkg/backend/retry"
	"github.com/kubewharf/kubebrain/pkg/backend/scanner"
	"github.com/kubewharf/kubebrain/pkg/backend/tso"
	"github.com/kubewharf/kubebrain/pkg/metrics"
	"github.com/kubewharf/kubebrain/pkg/storage"
)

const (
	historyCapacity      = 200000
	watchersChanCapacity = 100000
	eventBatchSize       = 300
	// idleWaitTimeout bounds how long the event collector blocks waiting for a
	// write signal before re-checking the ring buffer. It is only a safety net
	// against a missed wake-up; the common case is an immediate writeSignal.
	idleWaitTimeout = 10 * time.Millisecond

	// collectorStallWarnAfter / collectorStallSkipAfter bound how long the event
	// collector tolerates an empty ring slot for an already-dealt revision before
	// warning, then skipping it. collectorStallSkipAfter is far above the write
	// RPC timeout (unaryRpcTimeout, 1s) so a still-in-flight writer can never be
	// mistaken for a dead one.
	collectorStallWarnAfter = 3 * time.Second
	collectorStallSkipAfter = 30 * time.Second

	// defaultHistoryScanRevBucket is the default width (in revisions) of the
	// watch-history singleflight bucket (#30): reconnecting watchers of the same
	// prefix whose revisions fall in the same bucket share one storage scan.
	// Chosen to absorb the revision spread between HA-apiserver replicas that
	// relist at slightly different instants, while keeping the shared scan window
	// only modestly wider than any single caller asked for. Tunable via
	// Config.HistoryScanRevBucket / --watch-history-scan-rev-bucket.
	defaultHistoryScanRevBucket = 4096

	// countIndexMetricInterval is how often the live count-index gauges
	// (tracked-key count, overflowed flag) are refreshed. The count_index.keys
	// gauge is otherwise only updated on a rebuild and goes stale as live writes
	// change the key set.
	countIndexMetricInterval = 15 * time.Second
)

type Backend interface {

	// Create inserts new key into storage
	Create(ctx context.Context, request *proto.CreateRequest) (*proto.CreateResponse, error)

	// Update set key into storage
	Update(ctx context.Context, request *proto.UpdateRequest) (*proto.UpdateResponse, error)

	// Delete removes key from storage
	Delete(ctx context.Context, request *proto.DeleteRequest) (*proto.DeleteResponse, error)

	// DeleteRange removes keys from storage in one storage batch.
	DeleteRange(ctx context.Context, kvs []*proto.KeyValue) (*DeleteRangeResponse, error)

	// TxnApply applies a set of put/delete ops (distinct keys) atomically at a
	// single revision, asserting the given compare guards. See the implementation
	// for semantics; returns ErrTxnGuardConflict when a guard's key changed.
	TxnApply(ctx context.Context, ops []TxnWriteOp, guards []TxnGuard) ([]TxnWriteResult, uint64, error)

	// GetEtcdMetadata returns etcd-compatible create revision and version for a key at modRevision.
	GetEtcdMetadata(ctx context.Context, key []byte, modRevision uint64) (EtcdMetadata, error)

	// Compact clears the kvs that are too old, running the physical scan
	// synchronously before returning.
	Compact(ctx context.Context, revision uint64) (*proto.CompactResponse, error)

	// CompactAsync advances the logical compact watermark synchronously (so reads
	// immediately observe the compaction) and performs the physical version GC in
	// the background. It returns the actual (possibly clamped) compacted revision.
	// Use it on the hot Compact RPC path so a large physical backlog cannot exceed
	// the caller's timeout.
	CompactAsync(ctx context.Context, revision uint64) (uint64, error)

	// GetCompactRevision returns the latest completed logical compaction revision.
	GetCompactRevision(ctx context.Context) (uint64, error)

	// GetCompactRevisionFresh reads the compact revision from storage bypassing
	// the TTL cache; for cold paths that hand the value to clients as
	// authoritative (compacted-watch cancel, #33).
	GetCompactRevisionFresh(ctx context.Context) (uint64, error)

	// Get read a kv from storage
	Get(ctx context.Context, r *proto.GetRequest) (*proto.GetResponse, error)

	// List read kvs in range
	List(ctx context.Context, r *proto.RangeRequest) (*proto.RangeResponse, error)

	// Count counts the number of kvs in range
	Count(ctx context.Context, r *proto.CountRequest) (*proto.CountResponse, error)

	// CountAtRevision returns the exact live-key count of [key,end) at rev from
	// the in-memory count index; served is false when it must fall back to a scan.
	CountAtRevision(ctx context.Context, key, end []byte, rev uint64) (count int64, served bool)

	// RebuildCountIndex rebuilds the count index; call on leadership acquisition.
	RebuildCountIndex(ctx context.Context) error

	// GetPartitions query the partition state of storage for ListByStream
	GetPartitions(ctx context.Context, r *proto.ListPartitionRequest) (*proto.ListPartitionResponse, error)

	// ListByStream reads kvs in range by stream
	ListByStream(ctx context.Context, startKey, endKey []byte, revision uint64) (<-chan *proto.StreamRangeResponse, error)

	// Watch subscribe the changes from revision on kvs with given prefix
	Watch(ctx context.Context, key string, revision uint64) (<-chan []*proto.Event, error)

	// GetResourceLock returns the resource lock for leader election
	GetResourceLock() resourcelock.Interface

	// GetCurrentRevision returns the read revision
	GetCurrentRevision() uint64

	// GetPublishedRevision returns the highest revision whose events have been
	// fully fanned out to watch subscribers. It is <= GetCurrentRevision (which
	// advances pre-publish), and is the safe floor for seeding a from-now watch's
	// progress: any event a freshly-registered subscriber still receives has a
	// revision strictly greater, so it cannot be skipped.
	GetPublishedRevision() uint64

	// WatchProgressNotifyInterval is the configured progress-notify cadence, used
	// by the server watch loop so its emission ticker matches the backend's
	// in-band marker cadence. Always > 0.
	WatchProgressNotifyInterval() time.Duration

	// SetCurrentRevision is used for init tso for leader
	SetCurrentRevision(uint64)

	// SetLeadershipFence registers the leadership-epoch source that the write
	// fence re-checks immediately before every data-batch commit, so a deposed
	// leader's in-flight write cannot be committed-yet-unwatched (FINDING #39).
	// fn returns (current epoch, still-safely-leading). Unset = fence disabled.
	SetLeadershipFence(fn func() (uint64, bool))
}

var _ Backend = (*backend)(nil)

// watch data flow is as follows
// 1. write event triggered by storage write, then inserted into ring buffer
// 2. fetch write event with right sequence from ring buffer
//   2.1 insert into cache ring
//   2.2 insert into watch chan to broadcast to watchers

type backend struct {
	tso tso.TSO

	election election.ResourceLockManager

	kv storage.KvStorage

	coder coder.Coder

	creator creator.Creator

	scanner scanner.Scanner

	asyncFifoRetry retry.AsyncFifoRetry

	config Config

	watchEventsRingBuffer []*watchEventSlot
	// notifyMu serializes watch-overflow resets against event appends: appends
	// (notify/notifyBatch) hold it for read, the overflow reset holds it for
	// write so the wipe + revision jump + watcher close is atomic w.r.t.
	// concurrent appends.
	notifyMu sync.RWMutex

	// maximum size of history watch event window.
	capacity int
	// history watch event cache for watch request catch up
	watchCache *Ring

	// channel to pass etcd watch event to watchers
	watchChan chan []*proto.Event
	// writeSignal wakes collectStorageWriteEvents when a new event is appended
	// to the ring buffer, so the collector can block while idle instead of
	// busy-spinning a full core. Buffered(1); senders use a non-blocking send so
	// signals coalesce and the write path never blocks.
	writeSignal chan struct{}
	// hold watchers
	watcherHub *WatcherHub

	// countIndex, when enabled, gives exact live-key counts at a revision
	// without scanning storage (approach A-index). nil when disabled.
	countIndex *countindex.TreeIndex

	// historyScanSem bounds the number of concurrent watch-history fallback
	// scans. After a cache reset (e.g. leader change) every reconnecting watcher
	// whose start revision predates the warm cache falls back to a full
	// prefix scan; without a cap, N watchers issue N concurrent scans and stampede
	// the storage engine (#30). The gate serializes the excess into a bounded
	// number of in-flight scans; waiters block (honoring ctx) rather than piling
	// on. Buffered to historyScanConcurrency.
	historyScanSem chan struct{}

	// historyScanGroup collapses a reconnect herd's concurrent history scans of
	// the SAME prefix into one shared scan (singleflight), so the herd's storage
	// cost is O(1) per prefix rather than O(N) (#30). Complements historyScanSem:
	// the semaphore bounds distinct concurrent scans, the group dedups identical
	// ones.
	historyScanGroup *scanGroup

	// Background physical compaction. CompactAsync advances the logical compact
	// watermark synchronously (so reads immediately see the compaction) and hands
	// the slow physical version-GC scan to runCompactor, so the etcd Compact RPC
	// returns promptly instead of blocking a caller's timeout on a large backlog.
	// compactTriggerRev (atomic) is the highest revision requested for background
	// GC; compactSignal wakes the worker; compactScanMu serializes every physical
	// scan (background and the synchronous Compact path) so they never overlap.
	compactTriggerRev uint64
	compactDoneRev    uint64 // atomic: highest revision whose background GC scan finished
	compactSignal     chan struct{}
	compactScanMu     sync.Mutex

	// compactRevCache memoizes the persisted compact revision so revisioned reads
	// (compaction checks, txn/watch validation) don't each do a storage Get for a
	// value that only advances ~once per compaction cycle (#48). Monotonic, so a
	// short TTL is safe; setCompactRecord refreshes it eagerly when it advances.
	compactRevCache compactRevCache

	// fenceFn, when set, returns this node's current leadership epoch and whether
	// it is still safely leading. fenceAdmit consults it just before every data
	// commit to fence a deposed leader's in-flight writes (FINDING #39); the event
	// collector's stall watchdog also consults it (leadingFresh). nil-holder on
	// single-node / direct-constructed test backends (fence disabled, fail-open).
	// Held in an atomic.Value (a fenceHolder) because the collector reads it
	// continuously while SetLeadershipFence may re-register it on a leadership
	// change.
	fenceFn atomic.Value

	metricCli metrics.Metrics
}

// Config is the configuration for backend
type Config struct {
	// EnableEtcdCompatibility make backend compatible with etcd3
	EnableEtcdCompatibility bool

	// Prefix is the range that backend is in charge of
	Prefix string

	// Identity is the identity for a unique backend
	Identity string

	// SkippedPrefixes is the range that backend is not in charge of
	SkippedPrefixes []string

	// WatchCacheSize is the cache size of events
	WatchCacheSize int

	// WatchFanoutBuffer is the per-subscriber fan-out channel buffer in batches
	// (0 = default). A subscriber that overruns it is detached into ring
	// catch-up rather than dropped (#34).
	WatchFanoutBuffer int

	// HistoryScanRevBucket buckets the watch-history singleflight key (#30):
	// watchers reconnecting to the same prefix whose requested revisions fall in
	// the same bucket share ONE storage scan. Larger buckets collapse a wider
	// spread of near-revision reconnects (e.g. HA-apiserver replicas relisting at
	// slightly different revisions) at the cost of a shared scan window up to one
	// bucket wider than any single caller requested. 0 => default
	// (defaultHistoryScanRevBucket); 1 => only exact-revision reconnects share.
	HistoryScanRevBucket uint64

	// EnableCountIndex maintains an in-memory versioned key index on the leader
	// for exact O(range) counts (approach A-index). Requires EnableEtcdCompatibility.
	EnableCountIndex bool

	// AutoCompactionRetention, when > 0, enables the leader-side safety-net
	// auto-compactor: it caps MVCC history to the last N revisions if the
	// apiserver's own compaction loop stops (KubeBrain never auto-compacts
	// otherwise). 0 (default) leaves auto-compaction off. See runAutoCompactor.
	AutoCompactionRetention uint64

	// CountIndexMaxKeys caps the index size; above it the index is disabled and
	// counts fall back to a scan (avoids OOM). 0 means no cap.
	CountIndexMaxKeys int

	// WatchProgressNotifyInterval is how often a watch progress notification is
	// advanced/emitted (the in-band published-watermark marker cadence and the
	// per-watch progress-notify emission). Smaller = faster kube-apiserver
	// ConsistentListFromCache convergence at more marker traffic. <=0 uses
	// defaultWatchProgressNotifyInterval.
	WatchProgressNotifyInterval time.Duration
}

// defaultWatchProgressNotifyInterval is the fallback progress-notify cadence when
// Config.WatchProgressNotifyInterval is unset (<=0). It matches the value the
// watch progress ticker used before it became configurable.
const defaultWatchProgressNotifyInterval = time.Second

// NewBackend builds a new backend
func NewBackend(kv storage.KvStorage, config Config, metricCli metrics.Metrics) Backend {
	config.complete()
	normalCoder := coder.NewNormalCoder()
	electionConfig := election.Config{Prefix: config.Prefix, Identity: config.Identity, Timeout: unaryRpcTimeout}
	b := &backend{
		kv:                    kv,
		tso:                   tso.NewTSO(),
		coder:                 normalCoder,
		creator:               creator.NewNaiveCreator(kv, normalCoder),
		election:              election.NewResourceLockManager(electionConfig, kv),
		scanner:               scanner.NewScanner(kv, normalCoder, config.getScannerConfig(), metricCli),
		config:                config,
		capacity:              config.WatchCacheSize,
		watchEventsRingBuffer: newWatchEventSlots(watchersChanCapacity),
		watchCache:            NewRing(config.WatchCacheSize),
		watchChan:             make(chan []*proto.Event, watchersChanCapacity),
		writeSignal:           make(chan struct{}, 1),
		watcherHub: &WatcherHub{
			subs:             make(map[chan []*proto.Event][]byte),
			catchingUp:       make(map[chan []*proto.Event]*catchUpState),
			metricCli:        metricCli,
			progressInterval: config.WatchProgressNotifyInterval,
			bufSize:          config.WatchFanoutBuffer,
		},
		historyScanSem:   make(chan struct{}, historyScanConcurrency),
		historyScanGroup: newScanGroup(),
		compactSignal:    make(chan struct{}, 1),
		metricCli:        metricCli,
	}

	if config.EnableCountIndex && config.EnableEtcdCompatibility {
		b.countIndex = countindex.New(config.CountIndexMaxKeys)
	}

	// Wire the fan-out hub's ring catch-up to the watch cache: a slow
	// subscriber replays its missed tail from the ring instead of being
	// dropped into an O(all-keys) re-list (#34).
	b.watcherHub.ringLookup = b.watchCache.FindEvents

	asyncRetryConfig := retry.Config{
		UnaryTimeout:  unaryRpcTimeout,
		CheckInterval: checkInterval,
		RetryInterval: retryInterval,
		Tombstone:     tombStoneBytes,
	}
	b.asyncFifoRetry = retry.NewAsyncFifoRetry(b.coder, b.kv, b.metricCli, b.tso, b.getLatestInternalVal, b.notify, asyncRetryConfig)

	// TODO stop chan
	// write into watch chan, trigger by create/ update/ delete method in storage interface
	go b.collectStorageWriteEvents()

	// broadcast fan-out to all subscribed watchers
	go b.watcherHub.Stream(b.watchChan)

	go b.asyncFifoRetry.Run(context.Background())

	// drain background physical-compaction requests scheduled by CompactAsync
	go b.runCompactor()
	// Opt-in safety net (config.AutoCompactionRetention > 0); a no-op otherwise.
	go b.runAutoCompactor()
	// Live count-index gauges (no-op when the index is disabled).
	go b.emitCountIndexMetrics()

	return b
}

type watchEventSlot struct {
	sync.Mutex
	events []*common.WatchEvent
}

func newWatchEventSlots(capacity int) []*watchEventSlot {
	slots := make([]*watchEventSlot, capacity)
	for i := range slots {
		slots[i] = &watchEventSlot{}
	}
	return slots
}

func (s *watchEventSlot) append(event *common.WatchEvent) {
	s.Lock()
	defer s.Unlock()
	s.events = append(s.events, event)
}

func (s *watchEventSlot) appendAll(events []*common.WatchEvent) {
	s.Lock()
	defer s.Unlock()
	s.events = append(s.events, events...)
}

func (s *watchEventSlot) take(revision uint64) []*common.WatchEvent {
	s.Lock()
	defer s.Unlock()
	if len(s.events) == 0 || s.events[0].Revision != revision {
		return nil
	}
	events := s.events
	s.events = nil
	return events
}

func (s *watchEventSlot) reset() {
	s.Lock()
	defer s.Unlock()
	s.events = nil
}

var ErrRevisionDriftBack = errors.New("revision drift back")

func (b *backend) deal(prevRevision uint64) (uint64, error) {
	rev, err := b.tso.Deal()
	if err != nil {
		return 0, err
	}
	if prevRevision > 0 && rev < prevRevision {
		klog.ErrorS(ErrRevisionDriftBack, "deal", "generated", rev, "prev", prevRevision)
		b.metricCli.EmitCounter("revision.generator.invalid", 1)
		return rev, ErrRevisionDriftBack
	}

	if rev%100 == 0 {
		b.metricCli.EmitGauge("revision.generator", rev)
	}

	return rev, nil
}

// collectorStallState tracks how long the event collector has been waiting on a
// single empty ring slot, so it can warn and then self-heal past an abandoned
// (dead-writer) revision. See collectStorageWriteEvents.
type collectorStallState struct {
	rev    uint64
	since  time.Time
	warned bool
}

// note evaluates one empty-slot observation for nextRevision. It returns whether
// to emit a one-shot stall warning and whether to skip (advance past) the hole.
// A skip is only ever returned when the revision is below dealt (so it was
// allocated) AND this node is leading AND the slot has been empty for skipAfter —
// which is far above the write RPC timeout, so a still-in-flight writer can never
// be mistaken for a dead one. now is injected for testing.
func (s *collectorStallState) note(nextRevision, dealt uint64, leading bool, now time.Time, warnAfter, skipAfter time.Duration) (warn, skip bool) {
	if nextRevision > dealt || !leading {
		s.rev = 0
		return false, false
	}
	if s.rev != nextRevision {
		s.rev, s.since, s.warned = nextRevision, now, false
		return false, false
	}
	elapsed := now.Sub(s.since)
	if elapsed >= skipAfter {
		s.rev = 0
		return false, true
	}
	if elapsed >= warnAfter && !s.warned {
		s.warned = true
		return true, false
	}
	return false, false
}

// reset clears stall tracking after the collector makes progress.
func (s *collectorStallState) reset() { s.rev = 0 }

func (b *backend) collectStorageWriteEvents() {
	// Stall watchdog state. Every dealt revision is paired with a notify (valid or
	// invalid) on every normal path, so a ring slot fills within the write RPC
	// timeout (unaryRpcTimeout). A slot that stays empty far longer while its
	// revision is below Dealt() means the writer goroutine died between deal and
	// notify (panic / killed) — the revision will never arrive, and the collector
	// would otherwise wait on it forever, freezing the committed revision and thus
	// every list/watch on this node (a silent cluster-wide freeze). We surface it
	// (warn+metric) and, once the writer is provably dead (>> the write timeout),
	// advance past the hole so the stream self-heals instead of freezing.
	var stall collectorStallState
	// infinite loop
	for {
		events := make([]*proto.Event, 0, eventBatchSize)
		for len(events) < eventBatchSize {
			nextRevision := b.GetCurrentRevision() + 1
			idx := nextRevision % watchersChanCapacity
			watchEvents := b.watchEventsRingBuffer[idx].take(nextRevision)
			if len(watchEvents) == 0 {
				if len(events) == 0 {
					// A hole is only possible on the leader (whose collector drives
					// the ring); on a follower the collector idles while peer-sync
					// advances the revision, so never treat a follower's empty slot
					// as a stall.
					warn, skip := stall.note(nextRevision, b.tso.Dealt(), b.leadingFresh(), time.Now(), collectorStallWarnAfter, collectorStallSkipAfter)
					if skip {
						// > the bounded write RPC timeout: the revision's writer is
						// provably dead (a live one would have notified, valid or
						// invalid, long ago). Advance past the hole; it carried no
						// event, so no watcher loses one.
						klog.Errorf("event collector skipping abandoned revision %d (dealt=%d) after stall; a writer died between deal and notify", nextRevision, b.tso.Dealt())
						b.metricCli.EmitCounter("watch.collector.skipped_revision", 1)
						b.SetCurrentRevision(nextRevision)
						b.advanceCountIndexReadyRev(nextRevision)
						continue
					}
					if warn {
						klog.Warningf("event collector stalled on revision %d (dealt=%d); a writer may have died between deal and notify", nextRevision, b.tso.Dealt())
						b.metricCli.EmitCounter("watch.collector.stalled", 1)
					}
					// Nothing to collect yet: block until a writer signals a new
					// event (or a short timeout as a safety net against a missed
					// wake-up) instead of busy-spinning a full core. This is the
					// common idle state — on a leader between writes, and always
					// on followers where revisions advance via SetCurrentRevision.
					select {
					case <-b.writeSignal:
					case <-time.After(idleWaitTimeout):
					}
					continue
				}
				// break inside loop for sending existing  events, then read events in a new loop
				break
			}
			stall.reset()
			b.metricCli.EmitGauge("watch.set.current.revision", nextRevision)
			for _, watchEvent := range watchEvents {
				// invalid watch event, i.e. cas failed
				if !watchEvent.Valid {
					if errors.Is(watchEvent.Err, storage.ErrUncertainResult) {
						// must enqueue before update revision, otherwise it may be compact
						b.asyncFifoRetry.Append(watchEvent)
					}
					continue
				}

				// Maintain the count index in commit order (leader only, since
				// only the leader's collector processes local writes).
				if b.countIndex != nil {
					b.countIndex.Apply(watchEvent.Key, watchEvent.Revision, watchEvent.ResourceVerb == proto.Event_DELETE)
				}

				e := &proto.Event{
					Type:     watchEvent.ResourceVerb,
					Revision: watchEvent.Revision,
				}
				if watchEvent.ResourceVerb == proto.Event_DELETE {
					e.Kv = &proto.KeyValue{
						Key:      watchEvent.Key,
						Value:    watchEvent.Value,
						Revision: watchEvent.PrevRevision,
					}
				} else {
					e.Kv = &proto.KeyValue{
						Key:      watchEvent.Key,
						Value:    watchEvent.Value,
						Revision: watchEvent.Revision,
					}
				}
				events = append(events, e)
				b.watchCache.Add(e)
			}
			b.SetCurrentRevision(nextRevision)
			b.advanceCountIndexReadyRev(nextRevision)
		}

		if len(events) > 0 {
			b.watchChan <- events
		}
	}
}

// signalWrite wakes the event collector without ever blocking the caller. The
// buffered(1) channel coalesces bursts of writes into a single pending wake-up.
func (b *backend) signalWrite() {
	select {
	case b.writeSignal <- struct{}{}:
	default:
	}
}

// GetResourceLock implements Backend interface
func (b *backend) GetResourceLock() resourcelock.Interface {
	return b.election.GetResourceLock()
}

// GetCurrentRevision implements Backend interface
func (b *backend) GetCurrentRevision() uint64 {
	return b.tso.GetRevision()
}

// GetPublishedRevision implements Backend interface
func (b *backend) GetPublishedRevision() uint64 {
	return b.watcherHub.PublishedRevision()
}

// WatchProgressNotifyInterval implements Backend interface. config.complete()
// guarantees it is > 0.
func (b *backend) WatchProgressNotifyInterval() time.Duration {
	return b.config.WatchProgressNotifyInterval
}

// SetCurrentRevision implements Backend interface
func (b *backend) SetCurrentRevision(revision uint64) {
	b.tso.Commit(revision)
}

// advanceCountIndexReadyRev advances the count index's ready watermark to a
// just-committed revision, so a "count at current" (rev=0) request is served
// from the index instead of falling back to a full scan. The collector calls it
// right after SetCurrentRevision: by then every live-key event for the revision
// has been Apply'd, and a revision that carries no count-index Apply — a
// CAS-failed write, an abandoned/holed revision, or simply the read revision
// advancing ahead of the last live-key event — changed no live keys, so the
// index is exact as of it. Previously readyRev only advanced inside Apply, so it
// lagged the committed revision and every rev=0 count missed Ready() and scanned
// (the index served only when the exact past revision was requested). Safe
// against a concurrent non-blocking rebuild: Reset floors its snapshot baseRev at
// the highest applied revision (max(committed, readyRev)), so the fresh tree
// captures every event this watermark covers, and readyRev only ever advances
// (monotonic) while Ready() stays false for the tree's loading window.
func (b *backend) advanceCountIndexReadyRev(revision uint64) {
	if b.countIndex != nil {
		b.countIndex.SetReadyRev(revision)
	}
}

// emitCountIndexMetrics periodically publishes live count-index gauges so
// operators can watch the tracked-key count against --count-index-max-keys and
// see when the index has overflowed (disabled, counts fall back to a scan). The
// count_index.keys gauge is otherwise refreshed only on a rebuild and goes stale
// as live writes add and delete keys.
func (b *backend) emitCountIndexMetrics() {
	if b.countIndex == nil {
		return
	}
	ticker := time.NewTicker(countIndexMetricInterval)
	defer ticker.Stop()
	for range ticker.C {
		b.metricCli.EmitGauge("count_index.keys", b.countIndex.Len())
		overflowed := 0
		if b.countIndex.Overflowed() {
			overflowed = 1
		}
		b.metricCli.EmitGauge("count_index.overflowed", overflowed)
	}
}

func responseHeader(rev uint64) *proto.ResponseHeader {
	return &proto.ResponseHeader{
		Revision: rev,
	}
}
