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

	// GetEtcdMetadata returns etcd-compatible create revision and version for a key at modRevision.
	GetEtcdMetadata(ctx context.Context, key []byte, modRevision uint64) (EtcdMetadata, error)

	// Compact clears the kvs that are too old
	Compact(ctx context.Context, revision uint64) (*proto.CompactResponse, error)

	// GetCompactRevision returns the latest completed logical compaction revision.
	GetCompactRevision(ctx context.Context) (uint64, error)

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

	// SetCurrentRevision is used for init tso for leader
	SetCurrentRevision(uint64)
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

	// EnableCountIndex maintains an in-memory versioned key index on the leader
	// for exact O(range) counts (approach A-index). Requires EnableEtcdCompatibility.
	EnableCountIndex bool

	// CountIndexMaxKeys caps the index size; above it the index is disabled and
	// counts fall back to a scan (avoids OOM). 0 means no cap.
	CountIndexMaxKeys int
}

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
			subs:      make(map[chan []*proto.Event]struct{}),
			metricCli: metricCli,
		},
		metricCli: metricCli,
	}

	if config.EnableCountIndex && config.EnableEtcdCompatibility {
		b.countIndex = countindex.New(config.CountIndexMaxKeys)
	}

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

func (b *backend) collectStorageWriteEvents() {
	// infinite loop
	for {
		events := make([]*proto.Event, 0, eventBatchSize)
		for len(events) < eventBatchSize {
			nextRevision := b.GetCurrentRevision() + 1
			idx := nextRevision % watchersChanCapacity
			watchEvents := b.watchEventsRingBuffer[idx].take(nextRevision)
			if len(watchEvents) == 0 {
				if len(events) == 0 {
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

// SetCurrentRevision implements Backend interface
func (b *backend) SetCurrentRevision(revision uint64) {
	b.tso.Commit(revision)
}

func responseHeader(rev uint64) *proto.ResponseHeader {
	return &proto.ResponseHeader{
		Revision: rev,
	}
}
