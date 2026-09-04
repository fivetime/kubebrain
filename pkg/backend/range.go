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
	"bytes"
	"context"
	stderrors "errors"
	"fmt"
	"io"
	"math"
	"sort"
	"sync"
	"time"

	"github.com/pkg/errors"
	"golang.org/x/sync/errgroup"
	"k8s.io/klog/v2"

	proto "github.com/kubewharf/kubebrain-client/api/v2rpc"

	"github.com/kubewharf/kubebrain/pkg/backend/coder"
	"github.com/kubewharf/kubebrain/pkg/backend/countindex"
	"github.com/kubewharf/kubebrain/pkg/backend/scanner"
	"github.com/kubewharf/kubebrain/pkg/backend/streamerror"
	"github.com/kubewharf/kubebrain/pkg/storage"
	"github.com/kubewharf/kubebrain/pkg/util"
)

// Get implements Backend interface
func (b *backend) Get(ctx context.Context, r *proto.GetRequest) (resp *proto.GetResponse, err error) {
	ts := time.Now()
	defer func() {
		klog.V(klogLevel).InfoS("get",
			"key", Key(r.GetKey()),
			"rev", r.GetRevision(),
			"respRev", resp.GetHeader().GetRevision(),
			"respSize", resp.Size(),
			"latency", time.Since(ts))
	}()

	curRev, err := b.safeCurrentRevision(ctx)
	if err != nil {
		return nil, err
	}
	requireRev := r.GetRevision()

	val, modRev, err := b.get(ctx, r.Key, requireRev)
	if errors.Is(err, storage.ErrKeyNotFound) {
		return &proto.GetResponse{
			Header: responseHeader(curRev),
		}, nil
	} else if err != nil {
		klog.ErrorS(err, "backend get err", "key", util.LoggedKey(r.GetKey()), "revision", r.GetRevision())
		return nil, err
	}

	if modRev > curRev {
		curRev = modRev
	}
	if b.config.EnableEtcdCompatibility {
		if validationErr := b.validateEventObjectValue(ctx, r.Key, modRev, val); validationErr != nil {
			if errors.Is(validationErr, ErrInvalidMVCCMetadata) {
				validationErr = b.persistWitnessedObjectCorruption(
					ctx, r.Key, modRev, b.coder.EncodeObjectKey(r.Key, modRev), val, validationErr,
				)
			}
			return nil, validationErr
		}
	}

	resp = &proto.GetResponse{
		Header: responseHeader(curRev),
	}
	if val != nil {
		resp.Kv = &proto.KeyValue{
			Key:      r.Key,
			Value:    val,
			Revision: modRev,
		}
	}

	return resp, nil
}

// GetBatch resolves independent point reads against one engine snapshot with a
// bounded worker fan-out. It is an optional server-side optimization for etcd
// read-only Txn: the wire limit permits 128 operations, and executing those
// reads serially turns ordinary TiKV latency into a caller-visible timeout.
// Keeping the method outside Backend lets storage/test adapters that do not
// need the optimization retain their existing interface contract.
func (b *backend) GetBatch(ctx context.Context, keys [][]byte, revision uint64) ([]*proto.GetResponse, error) {
	ctx, err := b.withRangeSnapshotTimestamp(ctx)
	if err != nil {
		return nil, err
	}
	responses := make([]*proto.GetResponse, len(keys))
	group, groupCtx := errgroup.WithContext(ctx)
	group.SetLimit(maxDecodedRangeExactReadWorkers)
	for index, key := range keys {
		index, key := index, append([]byte(nil), key...)
		group.Go(func() error {
			response, getErr := b.Get(groupCtx, &proto.GetRequest{Key: key, Revision: revision})
			if getErr != nil {
				return getErr
			}
			responses[index] = response
			return nil
		})
	}
	if err = group.Wait(); err != nil {
		return nil, err
	}
	return responses, nil
}

func (b *backend) getLatestInternalVal(ctx context.Context, key []byte) (val []byte, modRevision uint64, err error) {
	return b.getInternalVal(ctx, key, 0)
}

// get returns the user-visible value and the revision it's modified with.
// NOTICE: return storage.ErrKeyNotFound if the value is not exist or is a tombstone.
//
//	modRevision maybe non-zero even if there is a storage.ErrKeyNotFound.
func (b *backend) get(ctx context.Context, key []byte, revision uint64) (val []byte, modRevision uint64, err error) {
	val, modRevision, err = b.getInternalVal(ctx, key, revision)
	if bytes.Equal(val, tombStoneBytes) {
		return nil, modRevision, storage.ErrKeyNotFound
	}
	return val, modRevision, err
}

func (b *backend) getInternalVal(ctx context.Context, key []byte, revision uint64) (val []byte, modRevision uint64, err error) {
	requestedRevision := revision
	latestExpectation := b.sampleLatestIndexExpectation(ctx, key, requestedRevision == 0)
	revisionValue, err := b.snapshotGet(ctx, b.coder.EncodeRevisionKey(key))
	if err != nil {
		if !errors.Is(err, storage.ErrKeyNotFound) {
			return nil, 0, err
		}
		if requestedRevision == 0 {
			if validationErr := validateMissingLatestRevisionIndex(key, latestExpectation); validationErr != nil {
				if validationErr = b.persistCurrentCountIndexCorruption(
					ctx, latestExpectation.readyRevision, validationErr,
				); validationErr != nil {
					return nil, 0, validationErr
				}
			}
		}
		// Pre-#31 compaction/retry races could leave an object whose revision
		// index is missing. Preserve the read-visible orphan recovery contract by
		// falling through to the decoded legacy scan below.
	} else {
		currentRevision, currentTombstone, parseErr := coder.ParseRevision(revisionValue)
		if parseErr != nil {
			cause := invalidMVCCMetadataError(parseErr, "decode revision index for key %q", key)
			if requestedRevision == 0 {
				return nil, 0, b.persistWitnessedRevisionIndexCorruption(ctx, cause)
			}
			return nil, 0, cause
		}
		latestIndexVerified := false
		if requestedRevision == 0 {
			latestIndexVerified, err = validateLatestRevisionIndex(
				key, currentRevision, currentTombstone, latestExpectation,
			)
			if err != nil {
				if err = b.persistCurrentCountIndexCorruption(
					ctx, latestExpectation.readyRevision, err,
				); err != nil {
					return nil, 0, err
				}
			}
		}

		// The revision index gives exact point reads a collision-free fast path.
		// This matters for the legacy object encoding {raw user key}${revision}:
		// versions of "a$extension" sort inside the reverse-scan interval for "a".
		// Reading the indexed object key directly cannot confuse those two keys.
		if requestedRevision == 0 || requestedRevision >= currentRevision {
			val, getErr := b.snapshotGet(ctx, b.coder.EncodeObjectKey(key, currentRevision))
			if getErr != nil {
				if latestIndexVerified && errors.Is(getErr, storage.ErrKeyNotFound) {
					cause := invalidMVCCMetadataError(getErr,
						"latest revision index for key %q references missing object revision %d", key, currentRevision)
					return nil, 0, b.persistWitnessedRevisionIndexCorruption(ctx, cause)
				}
				return nil, 0, getErr
			}
			if latestIndexVerified && bytes.Equal(val, tombStoneBytes) != currentTombstone {
				cause := invalidMVCCMetadataError(nil,
					"latest revision index for key %q has tombstone=%t but object revision %d has tombstone=%t",
					key, currentTombstone, currentRevision, bytes.Equal(val, tombStoneBytes))
				return nil, 0, b.persistWitnessedRevisionIndexCorruption(ctx, cause)
			}
			return val, currentRevision, nil
		}
	}

	if requestedRevision == 0 {
		requestedRevision = math.MaxUint64
	}
	startKey := b.coder.EncodeObjectKey(key, requestedRevision)
	endKey := b.coder.EncodeObjectKey(key, 0)
	// Historical reads and legacy orphan recovery need the reverse interval. It
	// is not a unique prefix when the raw key contains the delimiter, so do not
	// cap the iterator at the first physical row: decode and skip foreign keys.
	timestamp, _ := storage.SnapshotTimestampFromContext(ctx)
	iter, err := b.kv.Iter(ctx, startKey, endKey, timestamp, 0)
	if err != nil {
		return nil, 0, err
	}
	defer func() { err = stderrors.Join(err, iter.Close()) }()
	for {
		err = iter.Next(ctx)
		if err != nil {
			if err == io.EOF {
				// It's compacted after deletion, predates creation, or does not exist.
				return nil, 0, storage.ErrKeyNotFound
			}
			return nil, 0, err
		}

		userKey, candidateRevision, decodeErr := b.coder.Decode(iter.Key())
		if decodeErr != nil {
			return nil, 0, invalidMVCCMetadataError(decodeErr, "decode historical object key")
		}
		if candidateRevision == 0 || !bytes.Equal(userKey, key) {
			continue
		}
		return iter.Val(), candidateRevision, nil
	}
}

type latestIndexExpectation struct {
	revision      uint64
	readyRevision uint64
	tombstone     bool
	found         bool
	ready         bool
}

func (b *backend) sampleLatestIndexExpectation(ctx context.Context, key []byte, requested bool) latestIndexExpectation {
	if !requested || b.countIndex == nil {
		return latestIndexExpectation{}
	}
	if _, pinned := storage.SnapshotTimestampFromContext(ctx); pinned {
		return latestIndexExpectation{}
	}
	revision, tombstone, found, readyRevision, ready := b.countIndex.LatestIfReady(key)
	return latestIndexExpectation{
		revision: revision, tombstone: tombstone, found: found,
		readyRevision: readyRevision, ready: ready,
	}
}

func validateMissingLatestRevisionIndex(key []byte, expectation latestIndexExpectation) error {
	if !expectation.ready || !expectation.found {
		return nil
	}
	return invalidMVCCMetadataError(nil,
		"latest revision index for key %q is missing; count index expects revision %d tombstone=%t at ready revision %d",
		key, expectation.revision, expectation.tombstone, expectation.readyRevision)
}

// persistCurrentCountIndexCorruption only promotes an in-memory count-index
// contradiction when that snapshot still covers the durable user watermark.
// Followers do not continuously apply the leader's collector stream: a newer
// put/delete can therefore change (or remove) the physical revision index while
// their otherwise-ready tree still describes an older revision. The durable
// watermark is read only on this exceptional path, keeping normal reads free of
// extra storage I/O.
func (b *backend) persistCurrentCountIndexCorruption(
	ctx context.Context, readyRevision uint64, cause error,
) error {
	durableRevision, err := b.GetDurableRevision(ctx)
	if errors.Is(err, storage.ErrKeyNotFound) {
		durableRevision = 1
	} else if err != nil {
		return err
	}
	if durableRevision > readyRevision {
		return nil
	}
	return b.persistWitnessedRevisionIndexCorruption(ctx, cause)
}

// validateLatestRevisionIndex cross-checks a physical point-read index against
// the complete in-memory leader snapshot. The snapshot is advisory unless it
// is ready, and a physical revision above its ready watermark can be a valid
// write from a newer leader, so that case deliberately falls back to storage.
// A true result means the indexed target object may also be checked exactly.
func validateLatestRevisionIndex(
	key []byte, revision uint64, tombstone bool, expectation latestIndexExpectation,
) (bool, error) {
	if !expectation.ready || revision > expectation.readyRevision {
		return false, nil
	}
	if !expectation.found {
		// A retained tombstone can legitimately predate the rebuilt live snapshot.
		// A live physical index cannot: the ready snapshot is complete at readyRev.
		if tombstone {
			return false, nil
		}
		return false, invalidMVCCMetadataError(nil,
			"latest live revision index for key %q at revision %d is absent from count index ready at %d",
			key, revision, expectation.readyRevision)
	}
	if revision != expectation.revision || tombstone != expectation.tombstone {
		return false, invalidMVCCMetadataError(nil,
			"latest revision index for key %q is revision %d tombstone=%t; count index expects revision %d tombstone=%t at ready revision %d",
			key, revision, tombstone, expectation.revision, expectation.tombstone, expectation.readyRevision)
	}
	return true, nil
}

// List implements Backend interface
func (b *backend) List(ctx context.Context, r *proto.RangeRequest) (resp *proto.RangeResponse, err error) {
	return b.list(ctx, r, false)
}

// ListKeysOnly returns the same keys and MVCC metadata as List while dropping
// user values at the scanner/result boundary. It is an optional backend
// capability consumed by the etcd FastKeysOnly path; it intentionally remains
// outside Backend so adapters can fall back to List without source breakage.
func (b *backend) ListKeysOnly(ctx context.Context, r *proto.RangeRequest) (resp *proto.RangeResponse, err error) {
	return b.list(ctx, r, true)
}

func (b *backend) list(ctx context.Context, r *proto.RangeRequest, metadataOnly bool) (resp *proto.RangeResponse, err error) {
	ts := time.Now()
	defer func() {
		klog.V(klogLevel).InfoS("list",
			"start", Key(r.GetKey()),
			"end", Key(r.GetEnd()),
			"rev", r.GetRevision(),
			"limit", r.GetLimit(),
			"respCount", len(resp.GetKvs()),
			"respSize", resp.Size(),
			"latency", time.Since(ts))
	}()

	if len(r.End) == 0 {
		return nil, fmt.Errorf("invalid nil end field in RangeRequest")
	}

	reqRevision := r.Revision
	curRevision, err := b.safeCurrentRevision(ctx)
	if err != nil {
		return nil, err
	}
	if reqRevision == 0 {
		reqRevision = curRevision
	}

	if !isFromKeyEnd(r.End) && bytes.Compare(r.Key, r.End) >= 0 {
		return nil, errors.New("invalid range end")
	}

	// add limit to check if there is more value
	limit := r.Limit
	if limit > 0 && limit < math.MaxInt64 {
		limit++
	}
	latestExpectation := b.sampleLatestRangeExpectation(ctx, r.Key, r.End, r.Revision == 0, limit)

	decodedRange, err := b.requiresDecodedUserRange(ctx, r.Key, r.End, reqRevision)
	if err != nil {
		return nil, err
	}
	var kvs []*proto.KeyValue
	if decodedRange {
		kvs, err = b.decodedUserRange(ctx, r.Key, r.End, reqRevision)
		if metadataOnly {
			for _, kv := range kvs {
				if kv != nil {
					kv.Value = projectMetadataValue(kv.Value)
				}
			}
		}
		if err == nil && limit > 0 && int64(len(kvs)) > limit {
			kvs = kvs[:limit]
		}
	} else {
		key := b.rangeStartKey(r.Key)
		rangeEnd := b.rangeEndKey(r.End)
		if metadataOnly {
			if metadataScanner, ok := b.scanner.(scanner.MetadataScanner); ok {
				kvs, err = metadataScanner.RangeMetadata(ctx, key, rangeEnd, reqRevision, limit)
			} else {
				kvs, err = b.scanner.Range(ctx, key, rangeEnd, reqRevision, limit)
				for _, kv := range kvs {
					if kv != nil {
						kv.Value = projectMetadataValue(kv.Value)
					}
				}
			}
		} else {
			kvs, err = b.scanner.Range(ctx, key, rangeEnd, reqRevision, limit)
		}
	}
	if err != nil {
		klog.ErrorS(err, "backend range err", "key", util.LoggedKey(r.GetKey()), "end", util.LoggedKey(r.GetEnd()), "revision", r.GetRevision())
		if latestExpectation.ready && errors.Is(err, ErrInvalidMVCCMetadata) {
			return nil, b.persistWitnessedRevisionIndexCorruption(ctx, err)
		}
		return nil, err
	}
	if err := b.validateLatestRangeIndexes(ctx, kvs, latestExpectation); err != nil {
		return nil, err
	}
	resp = &proto.RangeResponse{
		Header: responseHeader(curRevision),
	}

	if limit > 0 && len(kvs) > int(r.Limit) {
		resp.More = true
		kvs = kvs[0:r.Limit]
	}
	if err := b.validateRangeObjectValues(ctx, kvs); err != nil {
		return nil, err
	}
	resp.Kvs = kvs
	return resp, nil
}

type latestRangeExpectation struct {
	states        []countindex.LatestState
	readyRevision uint64
	more          bool
	ready         bool
}

func (b *backend) sampleLatestRangeExpectation(
	ctx context.Context, start, end []byte, requested bool, limit int64,
) latestRangeExpectation {
	if !requested || b.countIndex == nil {
		return latestRangeExpectation{}
	}
	// A follower serializable checkpoint belongs to a different storage
	// snapshot/generation than the process-local latest count index.
	if _, pinned := storage.SnapshotTimestampFromContext(ctx); pinned {
		return latestRangeExpectation{}
	}
	if isFromKeyEnd(end) {
		end = nil
	}
	stateLimit := 0
	if limit > 0 {
		stateLimit = int(min(limit, int64(math.MaxInt)))
	}
	states, readyRevision, more, ready := b.countIndex.LatestRangeIfReady(start, end, stateLimit)
	return latestRangeExpectation{
		states: states, readyRevision: readyRevision, more: more, ready: ready,
	}
}

const latestRangeIndexBatchSize = 512

// validateLatestRangeIndexes applies the point-Get revision-index boundary to
// the latest range candidates. The ready in-memory snapshot identifies both
// exact returned revisions and live keys that must not disappear. Physical
// indexes are fetched in bounded batches; an index newer than readyRevision is
// a legitimate cross-leader/concurrent write and disables the comparison only
// for that key. As with point Get, the in-memory index is only a trigger: a
// durable transaction-witness validation must independently prove corruption
// before CORRUPT is armed.
func (b *backend) validateLatestRangeIndexes(
	ctx context.Context, kvs []*proto.KeyValue, expectation latestRangeExpectation,
) error {
	if !b.config.EnableEtcdCompatibility || !expectation.ready {
		return nil
	}
	expected := make(map[string]countindex.LatestState, len(expectation.states))
	keys := make([][]byte, 0, len(expectation.states)+len(kvs))
	seen := make(map[string]struct{}, cap(keys))
	addKey := func(key []byte) {
		if _, ok := seen[string(key)]; ok {
			return
		}
		seen[string(key)] = struct{}{}
		keys = append(keys, key)
	}
	for _, state := range expectation.states {
		expected[string(state.Key)] = state
		addKey(state.Key)
	}
	// A capped expectation is authoritative only through its last key. Later
	// physical candidates merely provide List's lookahead and are not evidence
	// of an unexpected key.
	var lastExpected []byte
	if len(expectation.states) != 0 {
		lastExpected = expectation.states[len(expectation.states)-1].Key
	}
	returned := make(map[string]*proto.KeyValue, len(kvs))
	for _, kv := range kvs {
		if kv == nil {
			continue
		}
		returned[string(kv.Key)] = kv
		if !expectation.more || (len(lastExpected) != 0 && bytes.Compare(kv.Key, lastExpected) <= 0) {
			addKey(kv.Key)
		}
	}

	for start := 0; start < len(keys); start += latestRangeIndexBatchSize {
		end := min(start+latestRangeIndexBatchSize, len(keys))
		chunk := keys[start:end]
		indexKeys := make([][]byte, len(chunk))
		for i, key := range chunk {
			indexKeys[i] = b.coder.EncodeRevisionKey(key)
		}
		values, err := b.snapshotEventLogValues(ctx, indexKeys)
		if err != nil {
			return err
		}
		for i, key := range chunk {
			state, expectedLive := expected[string(key)]
			returnedKV := returned[string(key)]
			value, found := values[string(indexKeys[i])]
			if !found {
				cause := invalidMVCCMetadataError(storage.ErrKeyNotFound,
					"latest range revision index for key %q is missing at count-index ready revision %d",
					key, expectation.readyRevision)
				return b.persistCurrentCountIndexCorruption(ctx, expectation.readyRevision, cause)
			}
			physicalRevision, tombstone, parseErr := coder.ParseRevision(value)
			if parseErr != nil {
				cause := invalidMVCCMetadataError(parseErr, "decode latest range revision index for key %q", key)
				return b.persistWitnessedRevisionIndexCorruption(ctx, cause)
			}
			if physicalRevision > expectation.readyRevision {
				continue
			}
			if !expectedLive {
				cause := invalidMVCCMetadataError(nil,
					"latest range returned key %q revision %d whose live revision index %d is absent from count index ready at %d",
					key, returnedKV.GetRevision(), physicalRevision, expectation.readyRevision)
				return b.persistCurrentCountIndexCorruption(ctx, expectation.readyRevision, cause)
			}
			if physicalRevision != state.Revision || tombstone {
				cause := invalidMVCCMetadataError(nil,
					"latest range revision index for key %q is revision %d tombstone=%t; count index expects live revision %d at ready revision %d",
					key, physicalRevision, tombstone, state.Revision, expectation.readyRevision)
				return b.persistCurrentCountIndexCorruption(ctx, expectation.readyRevision, cause)
			}
			if returnedKV == nil || returnedKV.Revision != state.Revision {
				actualRevision := uint64(0)
				if returnedKV != nil {
					actualRevision = returnedKV.Revision
				}
				cause := invalidMVCCMetadataError(nil,
					"latest range object for key %q is missing or at revision %d; revision index expects live revision %d",
					key, actualRevision, state.Revision)
				return b.persistCurrentCountIndexCorruption(ctx, expectation.readyRevision, cause)
			}
		}
	}
	return nil
}

// validateRangeObjectValues applies the same persisted-value safety boundary
// as point Get to the exact objects a range read is about to expose. Only a
// metadata failure backed by the object's complete transaction witness can arm
// CORRUPT; legacy or compacted history remains a request-local DataLoss error.
func (b *backend) validateRangeObjectValues(ctx context.Context, kvs []*proto.KeyValue) error {
	if !b.config.EnableEtcdCompatibility {
		return nil
	}
	for _, kv := range kvs {
		if kv == nil {
			continue
		}
		validationErr := b.validateEventObjectValue(ctx, kv.Key, kv.Revision, kv.Value)
		if validationErr == nil {
			continue
		}
		if errors.Is(validationErr, ErrInvalidMVCCMetadata) {
			validationErr = b.persistWitnessedObjectCorruption(
				ctx, kv.Key, kv.Revision, b.coder.EncodeObjectKey(kv.Key, kv.Revision), kv.Value, validationErr,
			)
		}
		return validationErr
	}
	return nil
}

func (b *backend) rangeStartKey(userKey []byte) []byte {
	if len(userKey) == 0 || userKey[len(userKey)-1] != 0 {
		return b.coder.EncodeObjectKey(userKey, 0)
	}
	// Kubernetes etcd3 pagination uses "last returned user key + \x00" as
	// the next range start. KubeBrain stores multiple internal versions as
	// "user key + '$' + revision", so encoding the trailing NUL literally
	// would seek before the previous key's versions and duplicate it.
	start := b.coder.EncodeObjectKey(userKey[:len(userKey)-1], math.MaxUint64)
	return append(start, 0)
}

func (b *backend) rangeEndKey(userKey []byte) []byte {
	if isFromKeyEnd(userKey) {
		return b.ks.ObjectKeyspaceEnd()
	}
	return b.coder.EncodeObjectKey(userKey, 0)
}

func isFromKeyEnd(userKey []byte) bool {
	return len(userKey) == 1 && userKey[0] == 0
}

func boundaryRequiresDecodedUserRange(start, end []byte) bool {
	if isFromKeyEnd(end) {
		end = nil
	}
	for _, boundary := range [][]byte{start, end} {
		for _, value := range boundary {
			if value <= '$' {
				return true
			}
		}
	}
	return false
}

const boundaryProbeCacheCapacity = 1024

type boundaryProbeCacheKey struct {
	revision uint64
	boundary string
}

type boundaryProbeCache struct {
	mu     sync.Mutex
	values map[boundaryProbeCacheKey]bool
	order  []boundaryProbeCacheKey
	next   int
}

func (c *boundaryProbeCache) get(key boundaryProbeCacheKey) (bool, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	value, ok := c.values[key]
	return value, ok
}

func (c *boundaryProbeCache) put(key boundaryProbeCacheKey, value bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.values == nil {
		c.values = make(map[boundaryProbeCacheKey]bool, boundaryProbeCacheCapacity)
		c.order = make([]boundaryProbeCacheKey, 0, boundaryProbeCacheCapacity)
	}
	if _, exists := c.values[key]; exists {
		c.values[key] = value
		return
	}
	if len(c.order) < boundaryProbeCacheCapacity {
		c.order = append(c.order, key)
	} else {
		delete(c.values, c.order[c.next])
		c.order[c.next] = key
		c.next = (c.next + 1) % boundaryProbeCacheCapacity
	}
	c.values[key] = value
}

func (b *backend) requiresDecodedUserRange(
	ctx context.Context,
	start, end []byte,
	revision uint64,
) (bool, error) {
	if boundaryRequiresDecodedUserRange(start, end) {
		return true, nil
	}
	boundaries := [][]byte{start}
	if !isFromKeyEnd(end) {
		boundaries = append(boundaries, end)
	}
	type probeResult struct {
		found bool
		err   error
	}
	results := make(chan probeResult, len(boundaries))
	for _, boundary := range boundaries {
		boundary := append([]byte(nil), boundary...)
		go func() {
			found, err := b.hasLowByteBoundaryExtension(ctx, boundary, revision)
			results <- probeResult{found: found, err: err}
		}()
	}
	var firstErr error
	for range boundaries {
		result := <-results
		if result.found {
			return true, nil
		}
		if firstErr == nil && result.err != nil {
			firstErr = result.err
		}
	}
	if firstErr != nil {
		return false, firstErr
	}
	return false, nil
}

func (b *backend) hasLowByteBoundaryExtension(
	ctx context.Context,
	boundary []byte,
	revision uint64,
) (bool, error) {
	cacheKey := boundaryProbeCacheKey{revision: revision, boundary: string(boundary)}
	if found, ok := b.boundaryProbeCache.get(cacheKey); ok {
		return found, nil
	}
	encoded := b.coder.EncodeObjectKey(boundary, 0)
	rawPrefix := encoded[:len(encoded)-9]
	start := append(append([]byte(nil), rawPrefix...), 0)
	end := append(append([]byte(nil), rawPrefix...), '$')
	kvs, err := b.scanner.Range(ctx, start, end, revision, 1)
	if err != nil {
		return false, err
	}
	found := len(kvs) != 0
	b.boundaryProbeCache.put(cacheKey, found)
	return found, nil
}

func (b *backend) decodedUserRange(
	ctx context.Context,
	start, end []byte,
	revision uint64,
) ([]*proto.KeyValue, error) {
	ctx, err := b.withRangeSnapshotTimestamp(ctx)
	if err != nil {
		return nil, err
	}
	userEnd := end
	if isFromKeyEnd(userEnd) {
		userEnd = nil
	}
	scanStart, scanEnd, exactKeys := b.decodedUserRangeScanPlan(start, end)
	kvs, err := b.scanner.RangeFilteredExcluding(
		ctx, scanStart, scanEnd, start, userEnd, exactKeys, revision,
	)
	if err != nil {
		return nil, err
	}
	exactKVs, err := b.readDecodedRangeExactKeys(ctx, exactKeys, revision)
	if err != nil {
		return nil, err
	}
	for _, kv := range exactKVs {
		if kv != nil {
			kvs = append(kvs, kv)
		}
	}
	sort.Slice(kvs, func(i, j int) bool {
		return bytes.Compare(kvs[i].Key, kvs[j].Key) < 0
	})
	return kvs, nil
}

// readDecodedRangeExactKeys reconciles the bounded escaped-ancestor set at the
// caller's pinned snapshot. TiKV can resolve the common latest/current case in
// two region-aware batches (revision indexes, then object rows); historical and
// legacy orphan rows fall back to bounded parallel exact reads.
func (b *backend) readDecodedRangeExactKeys(ctx context.Context, keys [][]byte, revision uint64) ([]*proto.KeyValue, error) {
	if len(keys) == 0 {
		return []*proto.KeyValue{}, nil
	}
	timestamp, pinned := storage.SnapshotTimestampFromContext(ctx)
	reader, batchSupported := storage.FindCapability[storage.SnapshotGetter](b.kv)
	if pinned && batchSupported {
		return b.readDecodedRangeExactKeysBatched(ctx, reader, timestamp, keys, revision)
	}
	return b.readDecodedRangeExactKeysParallel(ctx, keys, revision)
}

// visitDecodedRangeExactKeyChunks resolves object values in small windows even
// when the ordering directory supplies a much larger key page. Values may be
// close to the etcd request limit, so retaining an entire 300-key directory
// page would otherwise make one stream consume hundreds of MiB. Revision-index
// rows are small and remain one region-aware batch for the common pinned path.
func (b *backend) visitDecodedRangeExactKeyChunks(
	ctx context.Context,
	keys [][]byte,
	revision uint64,
	visit func([][]byte, []*proto.KeyValue) error,
) error {
	if len(keys) == 0 {
		return nil
	}
	timestamp, pinned := storage.SnapshotTimestampFromContext(ctx)
	reader, batchSupported := storage.FindCapability[storage.SnapshotGetter](b.kv)
	if !pinned || !batchSupported {
		for start := 0; start < len(keys); start += maxDecodedRangeStreamValueKeys {
			end := min(start+maxDecodedRangeStreamValueKeys, len(keys))
			kvs, err := b.readDecodedRangeExactKeysParallel(ctx, keys[start:end], revision)
			if err != nil {
				return err
			}
			if err = visit(keys[start:end], kvs); err != nil {
				return err
			}
		}
		return nil
	}

	revisionKeys := make([][]byte, len(keys))
	for index, key := range keys {
		revisionKeys[index] = b.coder.EncodeRevisionKey(key)
	}
	revisionValues, err := reader.BatchGetAt(ctx, revisionKeys, timestamp)
	if err != nil {
		return err
	}
	for start := 0; start < len(keys); start += maxDecodedRangeStreamValueKeys {
		end := min(start+maxDecodedRangeStreamValueKeys, len(keys))
		chunkKeys := keys[start:end]
		result := make([]*proto.KeyValue, len(chunkKeys))
		objectKeys := make([][]byte, 0, len(chunkKeys))
		objectIndexes := make([]int, 0, len(chunkKeys))
		objectRevisions := make([]uint64, 0, len(chunkKeys))
		fallbackIndexes := make([]int, 0, len(chunkKeys))
		for offset, revisionKey := range revisionKeys[start:end] {
			revisionValue, found := revisionValues[string(revisionKey)]
			if !found {
				fallbackIndexes = append(fallbackIndexes, offset)
				continue
			}
			currentRevision, _, parseErr := coder.ParseRevision(revisionValue)
			if parseErr != nil {
				return invalidMVCCMetadataError(parseErr, "decode revision index for key %q", chunkKeys[offset])
			}
			if revision != 0 && revision < currentRevision {
				fallbackIndexes = append(fallbackIndexes, offset)
				continue
			}
			objectKeys = append(objectKeys, b.coder.EncodeObjectKey(chunkKeys[offset], currentRevision))
			objectIndexes = append(objectIndexes, offset)
			objectRevisions = append(objectRevisions, currentRevision)
		}
		if len(objectKeys) != 0 {
			objectValues, batchErr := reader.BatchGetAt(ctx, objectKeys, timestamp)
			if batchErr != nil {
				return batchErr
			}
			for objectIndex, objectKey := range objectKeys {
				value, found := objectValues[string(objectKey)]
				if !found || bytes.Equal(value, tombStoneBytes) {
					continue
				}
				index := objectIndexes[objectIndex]
				result[index] = &proto.KeyValue{Key: chunkKeys[index], Value: value, Revision: objectRevisions[objectIndex]}
			}
		}
		if len(fallbackIndexes) != 0 {
			fallbackKeys := make([][]byte, len(fallbackIndexes))
			for index, resultIndex := range fallbackIndexes {
				fallbackKeys[index] = chunkKeys[resultIndex]
			}
			fallback, fallbackErr := b.readDecodedRangeExactKeysParallel(ctx, fallbackKeys, revision)
			if fallbackErr != nil {
				return fallbackErr
			}
			for index, resultIndex := range fallbackIndexes {
				result[resultIndex] = fallback[index]
			}
		}
		if err = visit(chunkKeys, result); err != nil {
			return err
		}
	}
	return nil
}

func (b *backend) readDecodedRangeExactKeysBatched(
	ctx context.Context,
	reader storage.SnapshotGetter,
	timestamp uint64,
	keys [][]byte,
	revision uint64,
) ([]*proto.KeyValue, error) {
	result := make([]*proto.KeyValue, len(keys))
	revisionKeys := make([][]byte, len(keys))
	for index, key := range keys {
		revisionKeys[index] = b.coder.EncodeRevisionKey(key)
	}
	revisionValues, err := reader.BatchGetAt(ctx, revisionKeys, timestamp)
	if err != nil {
		return nil, err
	}

	objectKeys := make([][]byte, 0, len(keys))
	objectIndexes := make([]int, 0, len(keys))
	objectRevisions := make([]uint64, 0, len(keys))
	fallbackIndexes := make([]int, 0, len(keys))
	for index, revisionKey := range revisionKeys {
		revisionValue, found := revisionValues[string(revisionKey)]
		if !found {
			// Preserve pre-#31 orphan recovery, which scans decoded legacy rows.
			fallbackIndexes = append(fallbackIndexes, index)
			continue
		}
		currentRevision, _, parseErr := coder.ParseRevision(revisionValue)
		if parseErr != nil {
			return nil, invalidMVCCMetadataError(parseErr, "decode revision index for key %q", keys[index])
		}
		if revision != 0 && revision < currentRevision {
			fallbackIndexes = append(fallbackIndexes, index)
			continue
		}
		objectKeys = append(objectKeys, b.coder.EncodeObjectKey(keys[index], currentRevision))
		objectIndexes = append(objectIndexes, index)
		objectRevisions = append(objectRevisions, currentRevision)
	}

	objectValues := map[string][]byte{}
	if len(objectKeys) != 0 {
		objectValues, err = reader.BatchGetAt(ctx, objectKeys, timestamp)
		if err != nil {
			return nil, err
		}
	}
	for objectIndex, objectKey := range objectKeys {
		value, found := objectValues[string(objectKey)]
		if !found || bytes.Equal(value, tombStoneBytes) {
			continue
		}
		index := objectIndexes[objectIndex]
		result[index] = &proto.KeyValue{Key: keys[index], Value: value, Revision: objectRevisions[objectIndex]}
	}

	if len(fallbackIndexes) == 0 {
		return result, nil
	}
	fallbackKeys := make([][]byte, len(fallbackIndexes))
	for index, resultIndex := range fallbackIndexes {
		fallbackKeys[index] = keys[resultIndex]
	}
	fallback, err := b.readDecodedRangeExactKeysParallel(ctx, fallbackKeys, revision)
	if err != nil {
		return nil, err
	}
	for index, resultIndex := range fallbackIndexes {
		result[resultIndex] = fallback[index]
	}
	return result, nil
}

// readDecodedRangeExactKeysParallel bounds stores without snapshot batch-get,
// historical lookups, and legacy orphan recovery to a small worker fan-out.
func (b *backend) readDecodedRangeExactKeysParallel(ctx context.Context, keys [][]byte, revision uint64) ([]*proto.KeyValue, error) {
	result := make([]*proto.KeyValue, len(keys))
	group, groupCtx := errgroup.WithContext(ctx)
	group.SetLimit(maxDecodedRangeExactReadWorkers)
	for index, key := range keys {
		index, key := index, key
		group.Go(func() error {
			value, modRevision, err := b.get(groupCtx, key, revision)
			if errors.Is(err, storage.ErrKeyNotFound) {
				return nil
			}
			if err != nil {
				return err
			}
			result[index] = &proto.KeyValue{Key: key, Value: value, Revision: modRevision}
			return nil
		})
	}
	return result, group.Wait()
}

// withRangeSnapshotTimestamp binds every physical step of a decoded range to
// one engine snapshot. A normal encoded range is one scanner transaction, but
// an escaped end ancestor is reconciled by point reads after that scan; without
// this pin a concurrent write can make one etcd Range response span snapshots.
func (b *backend) withRangeSnapshotTimestamp(ctx context.Context) (context.Context, error) {
	if _, pinned := storage.SnapshotTimestampFromContext(ctx); pinned {
		return ctx, nil
	}
	timestamp, err := b.kv.GetTimestampOracle(ctx)
	if err != nil {
		return nil, err
	}
	ctx = storage.WithSnapshotTimestamp(ctx, timestamp)
	if _, supported := storage.FindCapability[storage.SnapshotGetter](b.kv); !supported {
		ctx = storage.WithSnapshotIteratorFallback(ctx)
	}
	return ctx, nil
}

const (
	// A request end can contain O(len(end)) dangerous ancestors, while storing
	// every progressively longer prefix costs O(len(end)^2) bytes. Bound both
	// dimensions; beyond either budget the streaming full-keyspace fallback is
	// slower but keeps request memory linear and remains semantically exact.
	maxDecodedRangeExactAncestors     = 128
	maxDecodedRangeExactAncestorBytes = 64 << 10
	maxDecodedRangeExactReadWorkers   = 16
	maxDecodedRangeStreamValueKeys    = 16
)

// decodedUserRangeScanPlan returns a narrow raw interval plus the bounded set
// of end ancestors whose legacy version runs may cross its exclusive upper
// bound. The scanner excludes those keys and the caller reconciles them through
// exact revision-aware point reads, avoiding both a full-keyspace fallback and
// resurrection of an older version whose newer tombstone lies past raw end.
//
// {magic}+start is always a safe inclusive lower bound: every encoded version
// of start or an extension of start sorts after that raw prefix. {magic}+end is
// the narrow exclusive upper bound; only proper prefixes whose maximum valid
// encoding reaches that bound require point reconciliation.
func (b *backend) decodedUserRangeScanPlan(start, end []byte) ([]byte, []byte, [][]byte) {
	encodedStart := b.coder.EncodeObjectKey(start, 0)
	scanStart := encodedStart[:len(encodedStart)-9]
	if isFromKeyEnd(end) {
		return scanStart, b.ks.ObjectKeyspaceEnd(), nil
	}
	encodedEnd := b.coder.EncodeObjectKey(end, 0)
	scanEnd := encodedEnd[:len(encodedEnd)-9]
	var exactKeys [][]byte
	exactKeyBytes := 0
	for prefixLen := firstEndPrefixInRange(start, end); prefixLen < len(end); prefixLen++ {
		prefix := end[:prefixLen]
		if encodedAncestorMayReachEnd(end, prefixLen) {
			if len(exactKeys) == maxDecodedRangeExactAncestors ||
				exactKeyBytes+prefixLen > maxDecodedRangeExactAncestorBytes {
				return b.ks.ObjectKeyspaceStart(), b.ks.ObjectKeyspaceEnd(), nil
			}
			exactKeys = append(exactKeys, append([]byte(nil), prefix...))
			exactKeyBytes += prefixLen
		}
	}
	return scanStart, scanEnd, exactKeys
}

// firstEndPrefixInRange returns the shortest proper prefix of end which is >=
// start. Since start < end, every longer prefix is also >= start. Computing the
// common prefix once avoids comparing progressively longer slices O(n^2).
func firstEndPrefixInRange(start, end []byte) int {
	common := 0
	for common < len(start) && common < len(end) && start[common] == end[common] {
		common++
	}
	if common == len(start) {
		return common
	}
	return common + 1
}

// encodedAncestorMayReachEnd compares
//
//	ancestor + '$' + bigEndian(MaxInt64)
//
// with raw end without constructing the progressively longer encoded key. The
// shared ancestor bytes cancel, so only end's next byte and at most eight
// revision bytes matter.
func encodedAncestorMayReachEnd(end []byte, prefixLen int) bool {
	next := end[prefixLen]
	if next != '$' {
		return next < '$'
	}
	suffix := end[prefixLen+1:]
	for index := 0; index < 8; index++ {
		if index == len(suffix) {
			return true
		}
		maxRevisionByte := byte(0xff)
		if index == 0 {
			maxRevisionByte = 0x7f
		}
		if maxRevisionByte != suffix[index] {
			return maxRevisionByte > suffix[index]
		}
	}
	return len(suffix) == 8
}

// Count implements Backend interface
func (b *backend) Count(ctx context.Context, r *proto.CountRequest) (resp *proto.CountResponse, err error) {
	ts := time.Now()
	defer func() {
		klog.V(klogLevel).InfoS("count",
			"start", Key(r.GetKey()),
			"end", Key(r.GetEnd()),
			"respCount", resp.GetCount(),
			"latency", time.Since(ts))
	}()

	rev, err := b.safeCurrentRevision(ctx)
	if err != nil {
		return nil, err
	}
	if !b.config.EnableEtcdCompatibility {
		return &proto.CountResponse{
			Header: responseHeader(rev),
			Count:  uint64(0),
		}, nil
	}

	// Serve from the in-memory count index when available (approach A-index);
	// otherwise fall back to a full scan.
	if c, _, served := b.CountAtRevision(ctx, r.Key, r.End, rev); served {
		return &proto.CountResponse{Header: responseHeader(rev), Count: uint64(c)}, nil
	}
	count, err := b.countAtRevisionScan(ctx, r.Key, r.End, rev)
	if err != nil {
		klog.Errorf("backend count %v return err %v", r, err)
		return nil, err
	}
	return &proto.CountResponse{
		Header: responseHeader(rev),
		Count:  uint64(count),
	}, nil
}

// CountAtRevisionScan implements Backend. Unlike CountAtRevision, this method
// deliberately bypasses the in-memory index: callers use it only after the
// local/leader index ladder has declined the request. The scanner retains only
// an integer, so a historical CountOnly fallback never constructs a full KV
// slice and its inert Range filters cannot change cardinality.
func (b *backend) CountAtRevisionScan(ctx context.Context, key, end []byte, rev uint64) (count int64, headerRevision uint64, err error) {
	headerRevision, err = b.safeCurrentRevision(ctx)
	if err != nil {
		return 0, 0, err
	}
	if rev == 0 {
		rev = headerRevision
	} else if rev > headerRevision {
		// Match CountAtRevision: a cold replica may scan a storage snapshot newer
		// than its process-local watermark. A successful response header cannot
		// describe a revision older than the snapshot it counted.
		headerRevision = rev
	}
	count, err = b.countAtRevisionScan(ctx, key, end, rev)
	return count, headerRevision, err
}

func (b *backend) countAtRevisionScan(ctx context.Context, key, end []byte, rev uint64) (int64, error) {
	if !b.config.EnableEtcdCompatibility {
		return 0, nil
	}
	decodedRange, err := b.requiresDecodedUserRange(ctx, key, end, rev)
	if err != nil {
		return 0, err
	}
	var count int
	if decodedRange {
		ctx, err = b.withRangeSnapshotTimestamp(ctx)
		if err != nil {
			return 0, err
		}
		userEnd := end
		if isFromKeyEnd(userEnd) {
			userEnd = nil
		}
		scanStart, scanEnd, exactKeys := b.decodedUserRangeScanPlan(key, end)
		count, err = b.scanner.CountFilteredExcluding(
			ctx, scanStart, scanEnd, key, userEnd, exactKeys, rev,
		)
		if err == nil {
			err = b.visitDecodedRangeExactKeyChunks(ctx, exactKeys, rev, func(_ [][]byte, exactKVs []*proto.KeyValue) error {
				for _, kv := range exactKVs {
					if kv != nil {
						count++
					}
				}
				return nil
			})
		}
	} else {
		start, rangeEnd := b.rangeStartKey(key), b.rangeEndKey(end)
		count, err = b.scanner.Count(ctx, start, rangeEnd, rev)
	}
	if err != nil {
		return 0, err
	}
	return int64(count), nil
}

// GetPartitions implements Backend interface
func (b *backend) GetPartitions(ctx context.Context, r *proto.ListPartitionRequest) (resp *proto.ListPartitionResponse, err error) {
	ts := time.Now()
	defer func() {
		klog.V(klogLevel).InfoS("get partitions",
			"start", Key(r.GetKey()),
			"end", Key(r.GetEnd()),
			"respCount", resp.GetPartitionNum(),
			"latency", time.Since(ts))
	}()

	rev, err := b.safeCurrentRevision(ctx)
	if err != nil {
		return nil, err
	}
	start, end := b.coder.EncodeObjectKey(r.Key, 0), b.rangeEndKey(r.End)

	partitions, err := b.kv.GetPartitions(ctx, start, end)

	if err != nil {
		klog.Errorf("backend getPartitions %v return err %v", r, err)
		return nil, err
	}
	resp = &proto.ListPartitionResponse{
		Header:       responseHeader(rev),
		PartitionNum: int64(len(partitions)),
	}
	// kvs length = partition number + 1
	resp.PartitionKeys = make([][]byte, 0, len(partitions)+1)

	for idx, p := range partitions {
		// append range start of partition only
		resp.PartitionKeys = append(resp.PartitionKeys, p.Start)

		// append last end of partition
		if idx == len(partitions)-1 {
			resp.PartitionKeys = append(resp.PartitionKeys, p.End)
		}
	}
	return resp, nil
}

// ListByStream implements Backend interface
func (b *backend) ListByStream(ctx context.Context, startKey, endKey []byte, rev uint64) (<-chan *proto.StreamRangeResponse, error) {

	curRev, err := b.safeCurrentRevision(ctx)
	if err != nil {
		return nil, err
	}
	if rev == 0 {
		rev = curRev
	}
	klog.V(klogLevel).InfoS("list by stream", "start", Key(startKey), "end", Key(endKey), "rev", rev)
	stream := b.scanner.RangeStream(ctx, startKey, endKey, rev, false)
	return stream, nil
}

type latestRangeStreamContextKey struct{}

// WithLatestRangeStream records that a server pinned an original revision=0
// request to an explicit start revision before crossing the Backend interface.
// Without this marker the backend cannot distinguish that safe pin from a true
// historical request and would incorrectly skip latest-only integrity checks.
func WithLatestRangeStream(ctx context.Context) context.Context {
	return context.WithValue(ctx, latestRangeStreamContextKey{}, true)
}

// LatestRangeStreamFromContext reports whether WithLatestRangeStream marked ctx.
func LatestRangeStreamFromContext(ctx context.Context) bool {
	latest, _ := ctx.Value(latestRangeStreamContextKey{}).(bool)
	return latest
}

// RangeStream implements Backend interface: user-key range streaming for the
// etcd 3.7 KV.RangeStream RPC. It encodes the user range into the object
// keyspace (symmetric with List) — the scanner works on encoded object keys, so
// a raw user key never matches the magic-prefixed stored keys — pins the read
// revision, and streams the result in disjoint chunks.
func (b *backend) RangeStream(ctx context.Context, userStart, userEnd []byte, rev uint64) (<-chan *proto.StreamRangeResponse, error) {
	return b.rangeStream(ctx, userStart, userEnd, rev, false)
}

// RangeStreamKeysOnly is the FastKeysOnly counterpart of RangeStream. It keeps
// the metadata needed by the etcd response while dropping the user payload at
// the earliest scanner/chunk boundary. It remains an optional capability so
// existing Backend adapters retain the value-carrying fallback.
func (b *backend) RangeStreamKeysOnly(ctx context.Context, userStart, userEnd []byte, rev uint64) (<-chan *proto.StreamRangeResponse, error) {
	return b.rangeStream(ctx, userStart, userEnd, rev, true)
}

func (b *backend) rangeStream(ctx context.Context, userStart, userEnd []byte, rev uint64, metadataOnly bool) (<-chan *proto.StreamRangeResponse, error) {
	curRev, err := b.safeCurrentRevision(ctx)
	if err != nil {
		return nil, err
	}
	_, callerPinned := storage.SnapshotTimestampFromContext(ctx)
	latest := rev == 0 || LatestRangeStreamFromContext(ctx)
	if rev == 0 {
		rev = curRev
	}
	scanCtx, cancel := context.WithCancel(ctx)
	decodedRange, err := b.requiresDecodedUserRange(scanCtx, userStart, userEnd, rev)
	if err != nil {
		cancel()
		return nil, err
	}
	var stream <-chan *proto.StreamRangeResponse
	if decodedRange {
		scanCtx, err = b.withRangeSnapshotTimestamp(scanCtx)
		if err != nil {
			cancel()
			return nil, err
		}
		if _, ensureErr := b.ensureCountIndexAtRevision(scanCtx, rev); ensureErr != nil {
			cancel()
			return nil, ensureErr
		}
		var served bool
		stream, served, err = b.decodedUserRangeStreamFromCountIndex(scanCtx, userStart, userEnd, rev, metadataOnly)
		if err != nil {
			cancel()
			return nil, err
		}
		if !served {
			stream = b.decodedUserRangeStreamFromSpill(scanCtx, userStart, userEnd, rev, metadataOnly)
		}
	} else {
		key := b.rangeStartKey(userStart)
		rangeEnd := b.rangeEndKey(userEnd)
		klog.V(klogLevel).InfoS("range stream", "start", Key(userStart), "end", Key(userEnd), "rev", rev)
		if metadataOnly {
			if metadataScanner, ok := b.scanner.(scanner.MetadataScanner); ok {
				stream = metadataScanner.RangeStreamMetadata(scanCtx, key, rangeEnd, rev)
			} else {
				stream = projectMetadataRangeStream(scanCtx, b.scanner.RangeStream(scanCtx, key, rangeEnd, rev, false))
			}
		} else {
			stream = b.scanner.RangeStream(scanCtx, key, rangeEnd, rev, false)
		}
	}
	indexExpectation := b.newLatestRangeStreamExpectation(userStart, userEnd, rev, latest && !callerPinned)
	return b.validatedRangeStream(scanCtx, cancel, stream, rev, indexExpectation), nil
}

// projectMetadataRangeStream is the compatibility fallback for Scanner
// adapters that do not implement MetadataScanner. The production scanner
// projects inside its workers; this wrapper preserves correctness for older
// adapters, though they necessarily materialize each source chunk first.
func projectMetadataRangeStream(
	ctx context.Context,
	input <-chan *proto.StreamRangeResponse,
) <-chan *proto.StreamRangeResponse {
	output := make(chan *proto.StreamRangeResponse)
	go func() {
		defer close(output)
		for chunk := range input {
			if chunk != nil && chunk.RangeResponse != nil {
				for _, kv := range chunk.RangeResponse.Kvs {
					if kv != nil {
						kv.Value = projectMetadataValue(kv.Value)
					}
				}
			}
			select {
			case output <- chunk:
			case <-ctx.Done():
				return
			}
		}
	}()
	return output
}

// validatedRangeStream validates each bounded data chunk before it becomes
// visible. A failure is encoded through the existing terminal marker contract;
// canceling the private scan context also releases partition workers and TiKV
// iterators when a corrupt object appears before the end of a large range.
func (b *backend) validatedRangeStream(
	ctx context.Context,
	cancel context.CancelFunc,
	input <-chan *proto.StreamRangeResponse,
	revision uint64,
	indexExpectation *latestRangeStreamExpectation,
) <-chan *proto.StreamRangeResponse {
	output := make(chan *proto.StreamRangeResponse)
	go func() {
		defer close(output)
		defer cancel()
		for {
			select {
			case <-ctx.Done():
				return
			case chunk, ok := <-input:
				if !ok {
					return
				}
				if chunk != nil && chunk.RangeResponse != nil && chunk.RangeResponse.More {
					if indexExpectation != nil {
						if err := indexExpectation.validate(ctx, chunk.RangeResponse.Kvs); err != nil {
							select {
							case output <- rangeStreamErrorEnd(revision, err):
							case <-ctx.Done():
							}
							return
						}
					}
					if err := b.validateRangeObjectValues(ctx, chunk.RangeResponse.Kvs); err != nil {
						select {
						case output <- rangeStreamErrorEnd(revision, err):
						case <-ctx.Done():
						}
						return
					}
				} else if chunk != nil && chunk.RangeResponse != nil && chunk.Err == "" && indexExpectation != nil {
					if err := indexExpectation.finish(ctx); err != nil {
						select {
						case output <- rangeStreamErrorEnd(revision, err):
						case <-ctx.Done():
						}
						return
					}
				}
				select {
				case output <- chunk:
				case <-ctx.Done():
					return
				}
			}
		}
	}()
	return output
}

const latestRangeStreamIndexPage = 300

// latestRangeStreamExpectation is a bounded cursor over one count-index
// generation at the stream's pinned logical revision. It lets each physical
// chunk be checked before emission without materializing the complete range.
// Prior chunks cannot be withdrawn if a later chunk fails, so corruption is
// carried by the existing terminal stream error and consumers must discard the
// partial response, matching all other RangeStream terminal failures.
type latestRangeStreamExpectation struct {
	backend    *backend
	start      []byte
	end        []byte
	revision   uint64
	generation uint64
	pending    []countindex.LatestState
	after      []byte
	more       bool
}

func (b *backend) newLatestRangeStreamExpectation(
	start, end []byte, revision uint64, latest bool,
) *latestRangeStreamExpectation {
	if !latest || b.countIndex == nil {
		return nil
	}
	if isFromKeyEnd(end) {
		end = nil
	}
	states, generation, more, ready := b.countIndex.StatesPageIfReady(
		start, end, nil, revision, latestRangeStreamIndexPage, 0,
	)
	if !ready {
		b.metricCli.EmitCounter("backend.range_stream.latest_index_miss", 1)
		return nil
	}
	expectation := &latestRangeStreamExpectation{
		backend: b, start: append([]byte(nil), start...), end: append([]byte(nil), end...),
		revision: revision, generation: generation, pending: states, more: more,
	}
	if len(states) != 0 {
		expectation.after = append([]byte(nil), states[len(states)-1].Key...)
	}
	b.metricCli.EmitCounter("backend.range_stream.latest_index_hit", 1)
	return expectation
}

func (e *latestRangeStreamExpectation) loadMore() error {
	if !e.more {
		return nil
	}
	states, generation, more, ready := e.backend.countIndex.StatesPageIfReady(
		e.start, e.end, e.after, e.revision, latestRangeStreamIndexPage, e.generation,
	)
	if !ready || generation != e.generation {
		return fmt.Errorf("latest range ordering index changed during stream at revision %d", e.revision)
	}
	if len(states) == 0 && more {
		return fmt.Errorf("latest range ordering index made no progress at revision %d", e.revision)
	}
	e.pending = append(e.pending, states...)
	e.more = more
	if len(states) != 0 {
		e.after = append(e.after[:0], states[len(states)-1].Key...)
	}
	return nil
}

func (e *latestRangeStreamExpectation) ensure(n int) error {
	for len(e.pending) < n && e.more {
		if err := e.loadMore(); err != nil {
			return err
		}
	}
	return nil
}

func (e *latestRangeStreamExpectation) validate(ctx context.Context, kvs []*proto.KeyValue) error {
	if err := e.ensure(len(kvs)); err != nil {
		return err
	}
	if len(e.pending) < len(kvs) {
		cause := invalidMVCCMetadataError(nil,
			"latest range stream returned %d unexpected keys after its ordering index ended at revision %d",
			len(kvs)-len(e.pending), e.revision)
		return e.backend.persistWitnessedRevisionIndexCorruption(ctx, cause)
	}
	expected := e.pending[:len(kvs)]
	for i, kv := range kvs {
		if kv == nil || !bytes.Equal(kv.Key, expected[i].Key) || kv.Revision != expected[i].Revision {
			var actualKey []byte
			var actualRevision uint64
			if kv != nil {
				actualKey, actualRevision = kv.Key, kv.Revision
			}
			cause := invalidMVCCMetadataError(nil,
				"latest range stream expected key %q revision %d but received key %q revision %d",
				expected[i].Key, expected[i].Revision, actualKey, actualRevision)
			return e.backend.persistWitnessedRevisionIndexCorruption(ctx, cause)
		}
	}
	if err := e.backend.validateLatestRangeIndexes(ctx, kvs, latestRangeExpectation{
		states: expected, readyRevision: e.revision, ready: true,
	}); err != nil {
		return err
	}
	e.pending = e.pending[len(kvs):]
	return nil
}

func (e *latestRangeStreamExpectation) finish(ctx context.Context) error {
	if err := e.ensure(1); err != nil {
		return err
	}
	if len(e.pending) == 0 {
		return nil
	}
	state := e.pending[0]
	cause := invalidMVCCMetadataError(nil,
		"latest range stream ended before expected live key %q revision %d at stream revision %d",
		state.Key, state.Revision, e.revision)
	return e.backend.persistWitnessedRevisionIndexCorruption(ctx, cause)
}

func rangeStreamErrorEnd(revision uint64, err error) *proto.StreamRangeResponse {
	return &proto.StreamRangeResponse{
		RangeResponse: &proto.RangeResponse{
			Header: responseHeader(revision),
			More:   false,
		},
		Err: streamerror.Encode(err),
	}
}

const (
	decodedRangeStreamIndexPage  = 300
	decodedRangeStreamIndexBytes = 1536 * 1024
)

// decodedUserRangeStreamFromCountIndex uses the leader's complete, versioned,
// user-key-sorted count index as an ordering directory. It pages only logical
// keys, then resolves each bounded page against one pinned TiKV snapshot. This
// avoids materializing an arbitrarily large decoded-boundary range merely to
// repair the legacy object encoding's non-order-preserving '$' delimiter.
//
// The index is deliberately only a fast path: a follower, rebuilding/overflowed
// index, or revision older than its complete base returns served=false and the
// caller retains the existing decoded scan+sort fallback. A tree generation
// change after streaming starts is terminal; splicing pages from two rebuilds
// could skip or duplicate keys, so the client must relist.
func (b *backend) decodedUserRangeStreamFromCountIndex(
	ctx context.Context,
	userStart, userEnd []byte,
	revision uint64,
	metadataOnly bool,
) (<-chan *proto.StreamRangeResponse, bool, error) {
	if b.countIndex == nil {
		return nil, false, nil
	}
	end := userEnd
	if isFromKeyEnd(end) {
		end = nil
	}
	firstKeys, generation, more, ready := b.countIndex.KeysPageBytesIfReady(
		userStart, end, nil, revision, decodedRangeStreamIndexPage, decodedRangeStreamIndexBytes, 0,
	)
	if !ready {
		b.metricCli.EmitCounter("backend.range_stream.decoded_index_miss", 1)
		return nil, false, nil
	}
	pinnedCtx, err := b.withRangeSnapshotTimestamp(ctx)
	if err != nil {
		return nil, true, err
	}
	b.metricCli.EmitCounter("backend.range_stream.decoded_index_hit", 1)
	// Keep value pages under receiver backpressure. Buffering several legal
	// 16-value pages would defeat the resolver's large-value memory bound.
	stream := make(chan *proto.StreamRangeResponse)
	go func() {
		defer close(stream)
		send := func(response *proto.StreamRangeResponse) bool {
			select {
			case stream <- response:
				return true
			case <-ctx.Done():
				return false
			}
		}
		fail := func(streamErr error) {
			send(&proto.StreamRangeResponse{
				RangeResponse: &proto.RangeResponse{Header: responseHeader(revision)},
				Err:           streamerror.Encode(streamErr),
			})
		}

		keys := firstKeys
		for {
			readErr := b.visitDecodedRangeExactKeyChunks(pinnedCtx, keys, revision, func(chunkKeys [][]byte, kvs []*proto.KeyValue) error {
				for index, kv := range kvs {
					if kv == nil {
						return fmt.Errorf("decoded range index key %q is not live at revision %d", chunkKeys[index], revision)
					}
					if metadataOnly {
						kv.Value = projectMetadataValue(kv.Value)
					}
				}
				if !send(&proto.StreamRangeResponse{RangeResponse: &proto.RangeResponse{
					Header: responseHeader(revision), Kvs: kvs, More: true,
				}}) {
					return ctx.Err()
				}
				return nil
			})
			if readErr != nil {
				if ctx.Err() != nil {
					return
				}
				fail(readErr)
				return
			}
			if !more {
				send(&proto.StreamRangeResponse{RangeResponse: &proto.RangeResponse{Header: responseHeader(revision)}})
				return
			}
			after := keys[len(keys)-1]
			var ready bool
			keys, _, more, ready = b.countIndex.KeysPageBytesIfReady(
				userStart, end, after, revision, decodedRangeStreamIndexPage, decodedRangeStreamIndexBytes, generation,
			)
			if !ready {
				fail(fmt.Errorf("decoded range ordering index changed during stream at revision %d", revision))
				return
			}
		}
	}()
	return stream, true, nil
}

// SnapshotStream scans the complete physical object keyspace. Snapshot output
// is keyed by MVCC revision rather than user-key order, so it can use this path
// without the decoded full-range materialization required by KV.RangeStream.
func (b *backend) SnapshotStream(ctx context.Context, rev uint64) (<-chan *proto.StreamRangeResponse, error) {
	curRev, err := b.safeCurrentRevision(ctx)
	if err != nil {
		return nil, err
	}
	if rev == 0 {
		rev = curRev
	}
	scanCtx, cancel := context.WithCancel(ctx)
	stream := b.scanner.RangeStream(
		scanCtx, b.ks.ObjectKeyspaceStart(), b.ks.ObjectKeyspaceEnd(), rev, false,
	)
	return b.validatedRangeStream(scanCtx, cancel, stream, rev, nil), nil
}

const (
	snapshotHistoryChunkRecords = 300
	snapshotHistoryChunkBytes   = 1536 * 1024
)

// SnapshotHistoryStream emits every retained physical version from one storage
// iterator snapshot. The iterator and compaction pin are created before this
// method returns. Callers may therefore release a logical-write barrier as soon
// as this method succeeds; waiting for the first chunk would let a slow TiKV
// scan stall unrelated writes for the duration of its first batch request.
func (b *backend) SnapshotHistoryStream(ctx context.Context, rev uint64) (<-chan SnapshotHistoryChunk, error) {
	curRev, err := b.safeCurrentRevision(ctx)
	if err != nil {
		return nil, err
	}
	if rev == 0 {
		rev = curRev
	}
	compactRevision, err := b.GetCompactRevisionFresh(ctx)
	if err != nil {
		return nil, err
	}
	if compactRevision == math.MaxUint64 {
		return nil, fmt.Errorf("snapshot compact revision overflows pin: %d", compactRevision)
	}
	// Compact is clamped below the oldest pin. Pin compact+1 so the watermark
	// cannot move beyond the state captured for this historical snapshot while
	// its event-log ordering metadata is still being joined below.
	snapshotPin := compactRevision + 1
	b.snapshotPins.pin(snapshotPin)
	timestamp, _ := storage.SnapshotTimestampFromContext(ctx)
	iter, err := b.kv.Iter(ctx, b.ks.ObjectKeyspaceStart(), b.ks.ObjectKeyspaceEnd(), timestamp, 0)
	if err != nil {
		b.snapshotPins.unpin(snapshotPin)
		return nil, err
	}
	out := make(chan SnapshotHistoryChunk, 1)
	go func() {
		defer close(out)
		defer b.snapshotPins.unpin(snapshotPin)
		iteratorClosed := false
		closeIterator := func() error {
			if iteratorClosed {
				return nil
			}
			iteratorClosed = true
			return iter.Close()
		}
		defer func() { _ = closeIterator() }()
		send := func(chunk SnapshotHistoryChunk) bool {
			select {
			case out <- chunk:
				return true
			case <-ctx.Done():
				return false
			}
		}
		fail := func(err error) {
			send(SnapshotHistoryChunk{Revision: rev, Err: stderrors.Join(err, closeIterator())})
		}
		records := make([]SnapshotHistoryRecord, 0, snapshotHistoryChunkRecords)
		chunkBytes := 0
		flushChunk := func() bool {
			if len(records) == 0 {
				return true
			}
			eventKeys := make([][]byte, len(records))
			for i := range records {
				eventKeys[i] = b.ks.EncodeEventLogKey(records[i].ModRevision, records[i].Key)
			}
			values, getErr := b.snapshotEventLogValues(ctx, eventKeys)
			if getErr != nil {
				fail(fmt.Errorf("load snapshot event order: %w", getErr))
				return false
			}
			for i, eventKey := range eventKeys {
				value, found := values[string(eventKey)]
				if !found {
					continue
				}
				verb, _, subRevision, total, ordered, valid := coder.DecodeOrderedEventLogValue(value)
				if !valid || !ordered || total == 0 || subRevision >= total {
					continue
				}
				isDelete := verb == byte(proto.Event_DELETE)
				if isDelete != records[i].Tombstone || (!isDelete && verb != byte(proto.Event_CREATE) && verb != byte(proto.Event_PUT)) {
					continue
				}
				records[i].SubRevision = subRevision
				records[i].TotalChanges = total
				records[i].Ordered = true
			}
			chunk := SnapshotHistoryChunk{Records: records, Revision: rev}
			records = make([]SnapshotHistoryRecord, 0, snapshotHistoryChunkRecords)
			chunkBytes = 0
			return send(chunk)
		}
		appendRecord := func(record SnapshotHistoryRecord) bool {
			records = append(records, record)
			chunkBytes += len(record.Key) + len(record.Value) + 48
			if len(records) >= snapshotHistoryChunkRecords || chunkBytes >= snapshotHistoryChunkBytes {
				return flushChunk()
			}
			return true
		}
		var familyBoundary []byte
		var family []SnapshotHistoryRecord
		flushFamily := func() bool {
			latest := make(map[string]int, len(family))
			for i := range family {
				latest[string(family[i].Key)] = i
			}
			for _, index := range latest {
				if !family[index].Tombstone {
					family[index].Current = true
				}
			}
			for i := range family {
				if !appendRecord(family[i]) {
					return false
				}
			}
			family = family[:0]
			return true
		}
		for {
			if err := iter.Next(ctx); err != nil {
				if err == io.EOF {
					if closeErr := closeIterator(); closeErr != nil {
						fail(closeErr)
						return
					}
					if flushFamily() && flushChunk() {
						send(SnapshotHistoryChunk{Revision: rev, Done: true})
					}
					return
				}
				fail(err)
				return
			}
			rawKey := iter.Key()
			if b.ks.IsInternalStorageKey(rawKey) {
				continue
			}
			userKey, modRevision, decodeErr := b.coder.Decode(rawKey)
			if decodeErr != nil {
				fail(invalidMVCCMetadataError(decodeErr, "decode snapshot object key"))
				return
			}
			boundary, boundaryOK := b.coder.RevisionBoundaryForBorder(rawKey)
			if !boundaryOK {
				fail(fmt.Errorf("snapshot object key has no revision family boundary: %x", rawKey))
				return
			}
			if familyBoundary != nil && !bytes.Equal(boundary, familyBoundary) {
				if !flushFamily() {
					return
				}
			}
			familyBoundary = append(familyBoundary[:0], boundary...)
			if modRevision == 0 || modRevision > rev || bytes.HasPrefix(userKey, internalKeyspacePrefix) {
				continue
			}
			stored := append([]byte(nil), iter.Val()...)
			record := SnapshotHistoryRecord{
				Key:         append([]byte(nil), userKey...),
				ModRevision: modRevision,
				Tombstone:   bytes.Equal(stored, tombStoneBytes),
				LeaseKnown:  bytes.Equal(stored, tombStoneBytes),
			}
			if !record.Tombstone {
				if b.config.EnableEtcdCompatibility {
					validationErr := b.validateEventObjectValue(ctx, userKey, modRevision, stored)
					if errors.Is(validationErr, ErrInvalidMVCCMetadata) {
						validationErr = b.persistWitnessedObjectCorruption(
							ctx, userKey, modRevision, append([]byte(nil), rawKey...), stored, validationErr,
						)
					}
					if validationErr != nil {
						fail(validationErr)
						return
					}
				}
				meta, rawValue, inlined, decodeErr := DecodeInlineValueChecked(stored)
				if decodeErr != nil {
					fail(decodeErr)
					return
				}
				if !inlined {
					meta, decodeErr = b.GetEtcdMetadata(ctx, userKey, modRevision)
					if decodeErr != nil {
						fail(decodeErr)
						return
					}
					rawValue = stored
				} else {
					if validationErr := ValidateEtcdMetadataAtRevision(meta, modRevision, "snapshot inline value metadata"); validationErr != nil {
						fail(validationErr)
						return
					}
					record.LeaseKnown = InlineValueLeaseKnown(stored)
				}
				record.Value = append([]byte(nil), rawValue...)
				record.CreateRevision = meta.CreateRevision
				record.Version = meta.Version
				record.Lease = meta.Lease
			}
			family = append(family, record)
		}
	}()
	return out, nil
}

// snapshotEventLogValues preserves transaction subrevision metadata on every
// storage implementation. BatchGetter is an optimization, not a semantic
// capability: silently skipping the join on Badger (or another plain
// KvStorage) reorders same-revision writes by key in exported etcd snapshots.
func (b *backend) snapshotEventLogValues(ctx context.Context, keys [][]byte) (map[string][]byte, error) {
	if timestamp, pinned := storage.SnapshotTimestampFromContext(ctx); pinned {
		if reader, ok := storage.FindCapability[storage.SnapshotGetter](b.kv); ok {
			return reader.BatchGetAt(ctx, keys, timestamp)
		}
		if !storage.SnapshotIteratorFallbackFromContext(ctx) {
			return nil, ErrSerializableCheckpointUnavailable
		}
	}
	if batchGetter, ok := storage.FindCapability[storage.BatchGetter](b.kv); ok {
		return batchGetter.BatchGet(ctx, keys)
	}
	values := make(map[string][]byte, len(keys))
	for _, key := range keys {
		value, err := b.kv.Get(ctx, key)
		if errors.Is(err, storage.ErrKeyNotFound) {
			continue
		}
		if err != nil {
			return nil, err
		}
		values[string(key)] = value
	}
	return values, nil
}
