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

package scanner

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pkg/errors"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/klog/v2"

	proto "github.com/kubewharf/kubebrain-client/api/v2rpc"

	"github.com/kubewharf/kubebrain/pkg/backend/coder"
	"github.com/kubewharf/kubebrain/pkg/metrics"
	"github.com/kubewharf/kubebrain/pkg/storage"
)

const (
	scanInitDelay     = 1 * time.Second
	scanBackoffFactor = 3
	scanBackoffSteps  = 3
	iterTimeout       = 1000 * time.Second
	rangeStreamBatch  = 300

	// compactDeleteBatchSize is how many version-GC deletes a compaction worker
	// accumulates before committing them in ONE storage transaction. Compaction
	// used to delete every superseded version / tombstone with its own
	// begin/commit transaction (a TSO plus two round trips on TiKV), so a large
	// tombstone backlog serialized into millions of tiny transactions — the #66
	// throughput bottleneck. Batching collapses N deletes into N/batch commits.
	// Kept well under TiKV's per-txn key limit and sized so one flush stays a
	// modest write.
	compactDeleteBatchSize = 128

	// globalScanWorkers caps the number of partition-scan workers running
	// concurrently across ALL unlimited scans (#40). A kube-apiserver cold
	// start fires dozens of cachers' full-keyspace Lists at once; uncapped,
	// each List spawns one worker per TiKV region (dozens), so hundreds of
	// concurrent scans saturate TiKV's unified read pool and head-of-line
	// block every small read — observed live: a 2-key services List took
	// 12.6s during the start-up flood (vs <100ms steady), which starved the
	// apiserver's Fatal ip-repair PostStartHook and crash-looped the control
	// plane. Small ranges take one slot and pass through quickly; the cap
	// only queues the big scans against each other.
	globalScanWorkers = 24
)

// globalScanSem is the process-wide scan-worker semaphore (see globalScanWorkers).
var globalScanSem = make(chan struct{}, globalScanWorkers)

// NewScanner create a Scanner
func NewScanner(store storage.KvStorage, coder coder.Coder, config Config, metricCli metrics.Metrics) Scanner {
	return &scanner{
		store:     store,
		coder:     coder,
		config:    config,
		metricCli: metricCli,
	}
}

type scanner struct {
	store     storage.KvStorage
	coder     coder.Coder
	metricCli metrics.Metrics
	config    Config
}

// Config is the configuration of scanner
type Config struct {
	// CompactKey is internal key to store the compact state
	CompactKey []byte

	// Tombstone is the value bytes used to mark delete
	Tombstone []byte
}

// Range implements Scanner interface
func (r *scanner) Range(ctx context.Context, start []byte, end []byte, revision uint64, limit int64) ([]*proto.KeyValue, error) {
	if limit > 0 {
		return r.rangeWithLimit(ctx, start, end, revision, limit)
	}

	receiver := &commonResultReceiver{}
	_, err := r.scan(ctx, start, end, revision, false, false, receiver)
	if err != nil {
		return nil, err
	}
	return receiver.result, nil
}

func (r *scanner) rangeWithLimit(ctx context.Context, start []byte, end []byte, revision uint64, limit int64) ([]*proto.KeyValue, error) {
	tso, err := r.store.GetTimestampOracle(ctx)
	if err != nil {
		return nil, err
	}
	err = r.checkCompactRace(ctx, revision, false)
	if err != nil {
		return nil, err
	}
	receiver := &commonResultReceiver{limit: int(limit)}
	w := newWorker(workerConfig{
		idx:       0,
		partition: storage.Partition{Start: start, End: end},
		tso:       tso,
		revision:  revision,
		tombstone: r.config.Tombstone,
		compact:   false,
	}, r.store, r.coder, r.metricCli)
	_, err = w.run(ctx, receiver)
	if err != nil {
		return nil, err
	}
	return receiver.result, nil
}

// Count implements Scanner interface
func (r *scanner) Count(ctx context.Context, start []byte, end []byte, revision uint64) (int, error) {
	// A full partition-parallel scan that discards values and only counts live
	// keys — the count-index fallback path. O(keys in range).
	receiver := &emptyResultReceiver{}
	return r.scan(ctx, start, end, revision, false, false, receiver)
}

// RangeStream implements Scanner interface. keysOnly emits nil values (the
// count-index rebuild path) so the buffered channel does not hold ~300k object
// values at once (a failover-time memory spike); range reads that need the value
// pass false.
func (r *scanner) RangeStream(ctx context.Context, start []byte, end []byte, revision uint64, keysOnly bool) chan *proto.StreamRangeResponse {
	// Buffer sizing is part of the stream memory bound (k8s 1.37 review). A deep
	// shared buffer once held GB-scale spikes per stream (measured: +1.3GB on a
	// throttled 2GB stream). The ordered implementation now adds one chunk slot
	// per active partition worker; globalScanWorkers bounds those active slots,
	// while this shallow output buffer propagates HTTP/2 backpressure through the
	// coordinator. Together with rangeStreamBatchBytes, memory is bounded by
	// (global workers + output buffer) * ~1.5MiB rather than key count.
	// keysOnly streams (count-index rebuild) carry no values — tiny entries
	// where the deep buffer is cheap and keeps the rebuild scan unthrottled.
	buffer := 8
	if keysOnly {
		buffer = 1000
	}
	stream := make(chan *proto.StreamRangeResponse, buffer)

	go func() {
		defer close(stream)
		err := r.rangeStreamOrdered(ctx, start, end, revision, keysOnly, stream)
		select {
		case stream <- getListStreamEnd(revision, err):
		case <-ctx.Done():
		}
		if err != nil {
			if ctx.Err() != nil {
				// The caller tore the stream down (client disconnect, a canceled
				// watch-cache sync) and that cancellation aborted the scan. This is
				// normal operation — worker errors here are just "context canceled"
				// wearing different wrappers — so keep it out of the failure counter
				// that alerts watch, and out of the error log (seen mislabeling a
				// benign apiserver reconnect as a stream failure in the 1.37-alpha
				// cold-start test).
				klog.V(2).InfoS("range stream canceled by caller",
					"revision", revision, "start", string(start), "end", string(end), "err", err)
				r.metricCli.EmitCounter("backend.list.by.stream.canceled", 1)
				return
			}
			klog.Errorf("backend list stream with revision %d failed %v, start key is %s, end key is %s", revision, err, start, end)
			r.metricCli.EmitCounter("backend.list.by.stream.failed", 1)
		}
	}()

	return stream
}

// rangeStreamOrdered scans physical partitions concurrently but drains their
// bounded result channels in partition order. A shared output channel lets the
// fastest worker win the race and therefore violates etcd's natural ascending
// key order whenever a range crosses TiKV regions. One buffered chunk per
// partition preserves parallel prefetch without materializing a partition (or
// the full range) in memory.
func (r *scanner) rangeStreamOrdered(ctx context.Context, start, end []byte, revision uint64, keysOnly bool, output chan<- *proto.StreamRangeResponse) error {
	workerCtx, cancelWorkers := context.WithCancel(ctx)
	defer cancelWorkers()
	tso, err := r.store.GetTimestampOracle(workerCtx)
	if err != nil {
		return err
	}
	if err := r.checkCompactRace(workerCtx, revision, false); err != nil {
		return err
	}
	partitions, err := r.store.GetPartitions(workerCtx, start, end)
	if err != nil {
		return err
	}
	partitions = r.adjustPartitionsBorders(partitions)

	type partitionResult struct {
		chunks chan *proto.StreamRangeResponse
		err    chan error
	}
	results := make([]partitionResult, len(partitions))
	// Serialize only semaphore acquisition (not scanning) in partition order.
	// Without this baton, later partitions can win all globalScanWorkers slots,
	// fill their one-chunk buffers, and block while the coordinator waits for
	// partition 0, which is itself waiting for a slot: a circular wait.
	acquireTurn := make([]chan struct{}, len(partitions)+1)
	for idx := range acquireTurn {
		acquireTurn[idx] = make(chan struct{})
	}
	close(acquireTurn[0])
	for idx, partition := range partitions {
		results[idx] = partitionResult{
			chunks: make(chan *proto.StreamRangeResponse, 1),
			err:    make(chan error, 1),
		}
		go func(idx int, partition storage.Partition, result partitionResult) {
			defer close(result.chunks)
			useSem := len(partitions) > 1
			if useSem {
				select {
				case <-acquireTurn[idx]:
				case <-workerCtx.Done():
					close(acquireTurn[idx+1])
					result.err <- workerCtx.Err()
					return
				}
				select {
				case globalScanSem <- struct{}{}:
					defer func() { <-globalScanSem }()
				case <-workerCtx.Done():
					close(acquireTurn[idx+1])
					result.err <- workerCtx.Err()
					return
				}
				close(acquireTurn[idx+1])
			}

			receiver := newStreamReceiver(revision, result.chunks)
			worker := newWorker(workerConfig{
				idx:       idx,
				partition: partition,
				tso:       tso,
				revision:  revision,
				keysOnly:  keysOnly,
				tombstone: r.config.Tombstone,
			}, r.store, r.coder, r.metricCli)
			_, scanErr := worker.runWithBackoffRetry(workerCtx, receiver)
			if scanErr == nil {
				receiver.close()
			}
			result.err <- scanErr
		}(idx, partition, results[idx])
	}

	var firstErr error
	for _, result := range results {
		for chunk := range result.chunks {
			if firstErr != nil {
				continue // Drain workers after an earlier failure.
			}
			select {
			case output <- chunk:
			case <-ctx.Done():
				firstErr = ctx.Err()
				cancelWorkers()
			}
		}
		if partitionErr := <-result.err; firstErr == nil && partitionErr != nil {
			firstErr = partitionErr
			cancelWorkers()
		}
	}
	return firstErr
}

func getListStreamEnd(revision uint64, err error) *proto.StreamRangeResponse {
	response := &proto.StreamRangeResponse{
		// canceled means eof
		RangeResponse: &proto.RangeResponse{
			More:   false,
			Header: &proto.ResponseHeader{Revision: revision},
		},
	}
	if err != nil {
		// if err occurs, set it in CancelReason field
		response.Err = err.Error()
	}
	return response
}

// Compact implements Scanner interface
func (r *scanner) Compact(ctx context.Context, borders [][]byte, revision uint64) error {
	var firstErr error
	for i := 0; i+1 < len(borders); i += 2 {
		// Scan every border best-effort even if one fails: each border's GC is
		// independent, and returning the first error still surfaces the failure.
		if _, err := r.scan(ctx, borders[i], borders[i+1], revision, true, false, &emptyResultReceiver{}); err != nil {
			klog.ErrorS(err, "compact scan failed for border", "revision", revision, "start", borders[i], "end", borders[i+1])
			if firstErr == nil {
				firstErr = err
			}
		}
	}
	return firstErr
}

// compactKeysWorkers bounds the concurrent per-key micro-scans of an
// incremental compaction round. Each key costs one iterator open (one storage
// round trip) plus a handful of rows, so modest parallelism hides the RPC
// latency without leaning on the unified read pool the way full-keyspace
// partition workers do (which is why this does not share globalScanSem).
const compactKeysWorkers = 8

// CompactKeys implements Scanner interface: the incremental physical-GC path.
// Each user key is scanned over its full version range with the SAME worker
// machinery (compactRow rules) as a full Compact, so the GC semantics stay
// single-sourced. The per-key range [Encode(key,0), Encode(key,MaxUint64)+0x00)
// spans the key's revision-key row and every object version in ascending
// revision order — exactly the row order the worker's prev-tracking expects. If
// an unrelated key's rows happen to fall inside a range (sub-prefix keys
// sorting between a key's versions), they are compacted correctly too: the
// worker groups rows by decoded user key, and GC at the same watermark is
// idempotent.
func (r *scanner) CompactKeys(ctx context.Context, userKeys [][]byte, revision uint64) error {
	if len(userKeys) == 0 {
		return nil
	}
	store := r.store
	if exclusiveKvStorage, ok := r.store.(storage.ExclusiveKvStorage); ok {
		store = exclusiveKvStorage.GetExclusiveKvStorage()
	}
	tso, err := store.GetTimestampOracle(ctx)
	if err != nil {
		return err
	}
	if err := r.checkCompactRace(ctx, revision, true); err != nil {
		return err
	}

	shards := compactKeysWorkers
	if len(userKeys) < shards {
		shards = len(userKeys)
	}
	var wg sync.WaitGroup
	errList := make([]error, shards)
	wg.Add(shards)
	for s := 0; s < shards; s++ {
		go func(s int) {
			defer wg.Done()
			// One worker per shard: its delete buffer batches across the shard's
			// keys (flushed at compactDeleteBatchSize and at each run's end), and
			// per-key state (prev-tracking, skip bookkeeping) resets in run().
			w := newWorker(workerConfig{
				idx:       s,
				tso:       tso,
				revision:  revision,
				compact:   true,
				tombstone: r.config.Tombstone,
			}, store, r.coder, r.metricCli)
			for i := s; i < len(userKeys); i += shards {
				key := userKeys[i]
				w.partition = storage.Partition{
					Start: r.coder.EncodeObjectKey(key, 0),
					End:   append(r.coder.EncodeObjectKey(key, ^uint64(0)), 0),
				}
				if _, err := w.run(ctx, &emptyResultReceiver{}); err != nil {
					errList[s] = err
					return
				}
			}
		}(s)
	}
	wg.Wait()
	for _, e := range errList {
		if e != nil {
			return e
		}
	}
	return nil
}

// adjustPartitionsBorders adjust the borders of partitions to avoid object keys generated from an internal key
// are scanned by multiple workers, which may cause error.
func (r *scanner) adjustPartitionsBorders(ps []storage.Partition) (ret []storage.Partition) {
	sort.Slice(ps, func(i, j int) bool {
		return bytes.Compare(ps[i].Start, ps[j].Start) < 0
	})

	for i := 0; i < len(ps); i++ {
		// ! if there is an error, it means border is not an interval key, just not modify it.
		// ! but if there is no error, it means border is an interval key and should be adjusted.
		// ! specially: ignore the start of first partition and the end of last partition.
		if i != 0 {
			// start border may be moved forward except the first partition
			ps[i].Start = ps[i-1].End
		}

		if i != len(ps)-1 {
			// Snap the boundary down to the start (rev=0) of whatever user key it
			// falls within, so one key's versions never straddle two partitions.
			// This must handle borders that are NOT decodable full object keys
			// (e.g. a TiKV region split point {objectKey}\x00): the old
			// Decode-only path left those unadjusted, so a deleted key whose
			// tombstone landed in the next partition resurfaced as live in List.
			if b, ok := r.coder.RevisionBoundaryForBorder(ps[i].End); ok {
				ps[i].End = b
			}
		}
	}
	return ps
}

func (r *scanner) scan(ctx context.Context, start []byte, end []byte, revision uint64, compact bool, keysOnly bool, receiver resultReceiver) (int, error) {
	store := r.store
	if exclusiveKvStorage, ok := r.store.(storage.ExclusiveKvStorage); ok && compact {
		klog.InfoS("compact with exclusive kv storage", "start", string(start), "end", string(end), "rev", revision)
		store = exclusiveKvStorage.GetExclusiveKvStorage()
	}
	startTime := time.Now()

	tso, err := store.GetTimestampOracle(ctx)
	if err != nil {
		return 0, err
	}

	err = r.checkCompactRace(ctx, revision, compact)
	if err != nil {
		return 0, err
	}

	partitions, err := store.GetPartitions(ctx, start, end)
	if err != nil {
		return 0, err
	}

	partitions = r.adjustPartitionsBorders(partitions)

	var wg sync.WaitGroup
	errList := make([]error, len(partitions))
	receiverList := make([]resultReceiver, len(partitions))
	globalCount := int64(0)
	wg.Add(len(partitions))

	// run worker concurrently
	// Single-partition scans (small ranges: a service list, one prefix) bypass
	// the semaphore entirely: one worker is negligible read-pool load, and
	// queueing it behind the big multi-region scans is exactly the
	// head-of-line blocking the cap exists to prevent — during an apiserver
	// cold start the 2-key services List MUST come back within the ip-repair
	// hook's retry budget while the multi-million-key cacher Lists grind on.
	useSem := len(partitions) > 1
	for idx := range partitions {
		go func(idx int) {
			defer wg.Done()

			// Global scan-worker cap: acquire before touching storage so a burst
			// of concurrent full-keyspace scans queues here instead of saturating
			// the TiKV read pool (see globalScanWorkers).
			if useSem {
				select {
				case globalScanSem <- struct{}{}:
					defer func() { <-globalScanSem }()
				case <-ctx.Done():
					errList[idx] = ctx.Err()
					return
				}
			}

			// create a worker
			receiverList[idx] = receiver.fork()
			w := newWorker(workerConfig{
				idx:       idx,
				partition: partitions[idx],
				tso:       tso,
				revision:  revision,
				compact:   compact,
				keysOnly:  keysOnly,
				tombstone: r.config.Tombstone,
			}, store, r.coder, r.metricCli)

			// run worker
			localCount := 0
			localCount, errList[idx] = w.runWithBackoffRetry(ctx, receiverList[idx])
			atomic.AddInt64(&globalCount, int64(localCount))

		}(idx)
	}

	wg.Wait()
	// check error
	for _, e := range errList {
		if e != nil {
			return 0, e
		}
	}

	// merge result
	for _, forkedReceiver := range receiverList {
		receiver.merge(forkedReceiver)
		forkedReceiver.close()
	}

	// Per-request rate on the count-fallback path: keep it off the journald hot
	// path (2f34e9b) — a relist storm re-creates the compact-log backpressure.
	if klog.V(4).Enabled() {
		klog.V(4).InfoS("scan", "start", start, "end", end, "count", globalCount, "latency", time.Since(startTime))
	}
	return int(globalCount), nil
}

// worker fetches data from a partition
type worker struct {
	workerConfig

	store storage.KvStorage

	coder.Coder

	metricCli metrics.Metrics

	lastCompactFailedRawKey []byte

	// pendingDeletes buffers version-GC deletes for the current compaction scan so
	// they commit in batches (compactDeleteBatchSize) instead of one transaction
	// per key (#66). Reset at the start of every run() and flushed at batch
	// thresholds and at scan end.
	pendingDeletes []pendingDelete
}

// pendingDelete is one buffered compaction delete: the encoded object key to
// remove, plus the user key it belongs to for skip-on-error bookkeeping (a key
// whose delete persistently fails is remembered by user key and skipped next
// scan, so it cannot strand the batch forever).
type pendingDelete struct {
	objKey  []byte
	userKey []byte
	rev     uint64
}

type workerConfig struct {
	// idx is the index of worker
	idx int

	// partition indicate the border of scan
	partition storage.Partition

	// tso indicate the version of snapshot which the group of workers iter on
	tso uint64

	// revision indicate the visible revision of object key
	revision uint64

	// tombstone indicate the deleted key's value
	tombstone []byte

	// compact is the switch of compaction
	compact bool

	// keysOnly, when set, makes the worker emit KeyValues with a nil Value. The
	// count-index rebuild consumes only key+revision, but a value-carrying scan
	// buffers up to (channel cap * batch) KeyValues WITH their values — ~300k
	// large objects, a GB-scale memory spike right when a fresh leader rebuilds
	// on failover. Dropping the value at the source removes that spike; real
	// range reads (which need the value) leave it false.
	keysOnly bool
}

func newWorker(conf workerConfig, store storage.KvStorage, coder coder.Coder, metricCli metrics.Metrics) *worker {
	return &worker{
		workerConfig: conf,
		store:        store,
		Coder:        coder,
		metricCli:    metricCli,
	}
}

func (w *worker) runWithBackoffRetry(ctx context.Context, receiver resultReceiver) (int, error) {
	if klog.V(4).Enabled() {
		klog.V(4).InfoS("worker start processing", "worker", w.info())
	}
	var scanErr error
	var count int

	backOff := wait.Backoff{
		Duration: scanInitDelay,
		Factor:   scanBackoffFactor,
		Steps:    scanBackoffSteps,
	}

	// retry scan with backoff, sleep duration increase after each fail
	err := wait.ExponentialBackoff(backOff, func() (bool, error) {
		select {
		case <-ctx.Done():
			klog.InfoS("context canceled when worker run",
				"partitionsIdx", w.idx, "start", string(w.partition.Start), "end", string(w.partition.End), "compact", w.compact)
			// return err to avoid useless retry
			return false, errors.New("context canceled")
		default:
		}

		if count, scanErr = w.run(ctx, receiver); scanErr == nil {
			return true, nil
		}
		if !receiver.retriable() {
			// A streaming receiver has already emitted chunks it cannot recall;
			// re-scanning this partition from its start would re-send those keys
			// and silently violate RangeStream's disjoint-chunks contract. Abort
			// so the stream terminates with an error and the client relists.
			return false, fmt.Errorf("partition %d not retriable after partial stream: %w", w.idx, scanErr)
		}
		return false, nil
	})

	if err != nil {
		// fail after retry, scanErr records the latest scan err
		if err == wait.ErrWaitTimeout {
			err = fmt.Errorf("partition %d reached the max retry time: %d, encounter latest error %v", w.idx, scanBackoffSteps, scanErr)
		}
	}

	return count, err
}

func (w *worker) run(ctx context.Context, receiver resultReceiver) (int, error) {
	// record start time
	startTime := time.Now()
	scanCtx, cancel := context.WithTimeout(ctx, iterTimeout)

	// create iter
	it, err := w.store.Iter(scanCtx, w.partition.Start, w.partition.End, w.tso, 0)
	defer cancel()

	// todo: check compact race here
	if err != nil {
		klog.ErrorS(err, "scan failed", "worker", w.info())
		return 0, err
	}
	defer it.Close()
	receiver.reset()
	w.pendingDeletes = w.pendingDeletes[:0]
	count := 0
	valSize := int64(0)
	// iter cur
	var (
		curUserKey   []byte
		curRevision  uint64
		prevUserKey  []byte
		prevRevision uint64
		prevValue    []byte
	)

	for receiver.needMore() {
		select {
		case <-ctx.Done():
			klog.InfoS("context canceled when worker scan",
				"partitionsIdx", w.idx, "start", string(w.partition.Start), "end", string(w.partition.End), "compact", w.compact)
			return 0, fmt.Errorf("worker run context canceled")
		default:
		}

		// Hot loop: do NOT allocate a per-key context here. A previous version
		// wrapped every Next in context.WithTimeout — at 500 keys/page that was
		// one ctx+timer allocation per row, ~17% of total CPU in WithTimeout
		// itself plus a 31% GC share from the garbage, ~1.8ms/key end-to-end
		// (60x the raw TiKV scan cost) — and the timeout ctx was never even
		// honored: the TiKV iterator's Next does not take a context; per-RPC
		// deadlines live inside client-go, and scanCtx (iterTimeout) already
		// bounds the whole scan (#40).
		if err = it.Next(scanCtx); err != nil {
			break
		}

		// get key and value from iter
		key := it.Key()
		curUserKey, curRevision, err = w.Decode(key)
		if err != nil {
			if w.compact {
				klog.V(4).InfoS("skip non-object key during compact scan", "key", key, "err", err)
			} else {
				klog.Errorf("unmarshal object key %s failed %v", key, err)
			}
			continue
		}

		value := it.Val()
		valSize += int64(len(value))

		// revision greater than leader mvcc commit index or compact index, ignore
		if curRevision > w.revision {
			continue
		}

		// On a new user key, emit the previous key's newest live version to the
		// receiver (in compaction scans the receiver is a no-op, but count still
		// tracks). Compaction GC of superseded versions, tombstones, and
		// revision-key tombstones is delegated to compactRow, so the read hot
		// path carries no w.compact branching (the in-code TODO). compactRow
		// returns true when this row must NOT be tracked as the previous key
		// (a revision-key tombstone above the compact revision, left in place).
		if !bytes.Equal(curUserKey, prevUserKey) {
			if prevRevision > 0 && !bytes.Equal(prevValue, w.tombstone) {
				receiver.append(prevUserKey, w.emitValue(prevValue), prevRevision)
				count++
			}
		}
		if w.compact && w.compactRow(it, key, value, curUserKey, curRevision, prevUserKey, prevRevision) {
			continue
		}

		prevRevision = curRevision
		prevUserKey = curUserKey
		prevValue = it.Val()

		if w.compact && len(w.pendingDeletes) >= compactDeleteBatchSize {
			w.flushDeletes(ctx)
		}
	}

	endTime := time.Now()
	// Flush buffered GC deletes even on a timeout/error: a partial compaction scan
	// still makes progress — the deleted tombstones are gone on the next retry,
	// which advances past them instead of re-walking them (#66 completeness). ctx
	// (not the iterTimeout-bounded scanCtx) bounds the flush so a scan that hit
	// iterTimeout can still commit what it already found.
	if w.compact {
		w.flushDeletes(ctx)
	}
	if err != nil && err != io.EOF {
		klog.ErrorS(err, "worker error", "worker", w.info(), "count", count, "latency", endTime.Sub(startTime))
		return 0, err
	}
	// add last result
	if prevRevision > 0 && !bytes.Equal(prevValue, w.tombstone) && receiver.needMore() {
		receiver.append(prevUserKey, w.emitValue(prevValue), prevRevision)
		count++
	}

	receiver.flush()
	scanLatency := endTime.Sub(startTime)
	if klog.V(4).Enabled() {
		// w.info() is a fmt.Sprintf; guard so per-page LISTs pay nothing.
		klog.V(4).InfoS("worker done", "worker", w.info(), "latency", scanLatency, "count", count, "valSize", valSize)
	}
	w.metricCli.EmitHistogram("storage.scan_worker.latency", scanLatency.Seconds())
	w.metricCli.EmitHistogram("storage.scan_worker.size", valSize)
	w.metricCli.EmitHistogram("storage.scan_worker.count", count)
	return count, nil
}

// emitValue returns the value to hand the receiver: nil in keysOnly mode (the
// caller consumes only key+revision, so carrying the value just bloats the
// stream buffer), the value itself otherwise. Tombstone detection still uses the
// real value — only what reaches the receiver is dropped.
func (w *worker) emitValue(v []byte) []byte {
	if w.keysOnly {
		return nil
	}
	return v
}

func (w *worker) info() string {
	return fmt.Sprintf("partition is %d start key is %s end key is %s, compact is %v", w.idx, string(w.partition.Start), string(w.partition.End), w.compact)
}

func (w *worker) isSkippedRawKey(rawKey []byte, rev uint64) bool {
	if len(w.lastCompactFailedRawKey) > 0 && bytes.Compare(w.lastCompactFailedRawKey, rawKey) == 0 {
		klog.V(4).InfoS("compact skip", "rawKey", string(rawKey), "rev", rev)
		w.metricCli.EmitCounter("compact.skip", 1)
		return true
	}
	return false
}

func (w *worker) updateSkippedRawKey(rawKey []byte, rev uint64, err error) {
	klog.ErrorS(err, "compact failed", "rawKey", string(rawKey), "rev", rev)
	if !errors.Is(err, storage.ErrCASFailed) {
		w.lastCompactFailedRawKey = rawKey
	}
}

// compactRow applies the compaction GC side effects for one scanned row and
// reports whether run() should skip tracking this row as the previous key.
// Only called on a compaction scan (w.compact). Extracted from run()'s loop so
// the read path is a pure key-group iterator with no compaction branching
// (the in-code TODO). Behaviour is identical to the inline branches it replaces:
//   - a same-key older version is superseded by this newer one -> GC it;
//   - a delete tombstone object -> GC it;
//   - a revision-key tombstone (curRevision==0) at/below the compact revision
//     -> GC it; above it (a retried uncertain DELETE) -> leave it AND signal the
//     caller to skip prev-tracking (the original `continue`).
func (w *worker) compactRow(it storage.Iter, objectKey, value, curUserKey []byte, curRevision uint64, prevUserKey []byte, prevRevision uint64) (skipPrev bool) {
	// Same user key: this newer version supersedes the previous one; GC the old.
	if bytes.Equal(curUserKey, prevUserKey) && prevRevision > 0 {
		prevKey := w.EncodeObjectKey(prevUserKey, prevRevision)
		klog.V(4).InfoS("compact expired object key", "key", prevUserKey, "rev", prevRevision)
		w.compactKey(prevKey, prevUserKey, prevRevision)
	}
	// A delete tombstone object.
	if bytes.Equal(value, w.tombstone) {
		klog.V(4).InfoS("compact object key with tombstone", "key", curUserKey, "rev", curRevision)
		w.compactKey(objectKey, curUserKey, curRevision)
	}
	// A revision-key tombstone value (curRevision==0). Parsing through coder
	// keeps the tombstone wire format single-sourced instead of re-decoding by hand.
	if curRevision == 0 {
		if objRev, isTomb, perr := coder.ParseRevision(value); perr == nil && isTomb {
			// Skip GC if the parsed revision is larger than the requested one, to
			// avoid conflict with a retried uncertain DELETE — and do not track it
			// as the previous key.
			if objRev > w.revision {
				klog.V(4).InfoS("skip gc revision key", "key", string(curUserKey), "revision", objRev,
					"gcRev", w.revision)
				return true
			}
			klog.V(4).InfoS("compact index key", "key", curUserKey, "rev", curRevision, "val", objRev, "len", len(value))
			w.compactCurrent(it, curUserKey, curRevision)
		}
	}
	return false
}

// compactCurrent buffers a revision-key tombstone for deletion. It used to issue
// a DelCurrent (delete-if-value-equal) per key; the CAS was a safeguard against a
// concurrent write, but compaction only removes versions at/below the compact
// watermark, which are immutable, so an unconditional batched Del of the same key
// is equivalent and far cheaper (#66). iter.Key() aliases the iterator buffer, so
// enqueueDelete copies it.
func (w *worker) compactCurrent(iter storage.Iter, rawKey []byte, rev uint64) {
	if w.isSkippedRawKey(rawKey, rev) {
		return
	}
	w.enqueueDelete(iter.Key(), rawKey, rev)
}

// compactKey buffers a superseded version / tombstone object for deletion.
func (w *worker) compactKey(key []byte, rawKey []byte, rev uint64) {
	if w.isSkippedRawKey(rawKey, rev) {
		return
	}
	w.enqueueDelete(key, rawKey, rev)
}

// enqueueDelete buffers one object key for batched compaction GC. Both keys are
// copied: objKey may alias the iterator's buffer (invalidated on the next
// iter.Next), and userKey is retained for skip-on-error bookkeeping beyond this
// iteration.
func (w *worker) enqueueDelete(objKey, userKey []byte, rev uint64) {
	w.pendingDeletes = append(w.pendingDeletes, pendingDelete{
		objKey:  append([]byte(nil), objKey...),
		userKey: append([]byte(nil), userKey...),
		rev:     rev,
	})
}

// flushDeletes commits the buffered compaction deletes in one storage
// transaction. On a batch-commit failure it falls back to per-key deletes so a
// single poison key cannot strand the whole batch — the existing skip-on-error
// path then isolates it. Delete failures are benign (a later compaction reclaims
// the leftover), so this never returns an error that would force a partition
// re-scan.
func (w *worker) flushDeletes(ctx context.Context) {
	if len(w.pendingDeletes) == 0 {
		return
	}
	batch := w.store.BeginBatchWrite()
	for i := range w.pendingDeletes {
		batch.Del(w.pendingDeletes[i].objKey)
	}
	if err := batch.Commit(ctx); err == nil {
		w.metricCli.EmitCounter("compact", int64(len(w.pendingDeletes)))
		w.pendingDeletes = w.pendingDeletes[:0]
		return
	}
	w.metricCli.EmitCounter("compact.batch.err", 1)
	for i := range w.pendingDeletes {
		pd := w.pendingDeletes[i]
		if err := w.store.Del(ctx, pd.objKey); err != nil {
			w.metricCli.EmitCounter("compact.err", 1)
			w.updateSkippedRawKey(pd.userKey, pd.rev, err)
		} else {
			w.metricCli.EmitCounter("compact", 1)
		}
	}
	w.pendingDeletes = w.pendingDeletes[:0]
}

// checkCompactRace will guarantee range request and compact request don't conflict
func (r *scanner) checkCompactRace(ctx context.Context, revision uint64, compact bool) error {

	if compact {
		// compact operation: raise the compact revision, but NEVER lower it. The
		// compact key marks the revision below which data has been physically
		// removed and must be monotonic; an unconditional Put here could regress
		// it (e.g. a stale/retried compact at a smaller revision), after which
		// range requests at already-compacted revisions would wrongly pass the
		// guard below and return incomplete data.
		val, err := r.store.Get(ctx, r.config.CompactKey)
		if err != nil && err != storage.ErrKeyNotFound {
			return err
		}
		if len(val) >= 8 && binary.BigEndian.Uint64(val) >= revision {
			// already compacted at an equal-or-higher revision
			return nil
		}
		bs := make([]byte, 8)
		binary.BigEndian.PutUint64(bs, revision)
		batch := r.store.BeginBatchWrite()
		if len(val) > 0 {
			batch.CAS(r.config.CompactKey, bs, val, 0)
		} else {
			batch.PutIfNotExist(r.config.CompactKey, bs, 0)
		}
		err = batch.Commit(ctx)
		if errors.Is(err, storage.ErrCASFailed) {
			// a concurrent compactor advanced it; the watermark did not regress
			return nil
		}
		return err
	}

	// if scan is triggered by range and range stream, check compact race
	// check is processed after get snapshot, so that even compact happens concurrently, it will not affect data for this range
	// get compact revision
	val, err := r.store.Get(ctx, r.config.CompactKey)
	if err != nil {
		// if compact_key is not initialized, return nil
		if err == storage.ErrKeyNotFound {
			return nil
		}
		klog.Errorf("get compact revision failed %v", err)
		return err
	}
	// compare compact revision and range revision
	compactRevision := binary.BigEndian.Uint64(val)
	if compactRevision > revision {
		// revision has already been compacted
		err := fmt.Errorf("range stream revision %d less than compact revision %d", revision, compactRevision)
		return err
	}
	return nil
}
