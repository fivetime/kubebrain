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
	"fmt"
	"io"
	"math"
	"sort"
	"time"

	"github.com/pkg/errors"
	"k8s.io/klog/v2"

	proto "github.com/kubewharf/kubebrain-client/api/v2rpc"

	"github.com/kubewharf/kubebrain/pkg/backend/coder"
	"github.com/kubewharf/kubebrain/pkg/storage"
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
	if err == storage.ErrKeyNotFound {
		return &proto.GetResponse{
			Header: responseHeader(curRev),
		}, nil
	} else if err != nil {
		klog.ErrorS(err, "backend get err", "key", string(r.GetKey()), "revision", r.GetRevision())
		return nil, err
	}

	if modRev > curRev {
		curRev = modRev
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
	revisionValue, err := b.kv.Get(ctx, b.coder.EncodeRevisionKey(key))
	if err != nil {
		if !errors.Is(err, storage.ErrKeyNotFound) {
			return nil, 0, err
		}
		// Pre-#31 compaction/retry races could leave an object whose revision
		// index is missing. Preserve the read-visible orphan recovery contract by
		// falling through to the decoded legacy scan below.
	} else {
		currentRevision, _, parseErr := coder.ParseRevision(revisionValue)
		if parseErr != nil {
			return nil, 0, parseErr
		}

		// The revision index gives exact point reads a collision-free fast path.
		// This matters for the legacy object encoding {raw user key}${revision}:
		// versions of "a$extension" sort inside the reverse-scan interval for "a".
		// Reading the indexed object key directly cannot confuse those two keys.
		if requestedRevision == 0 || requestedRevision >= currentRevision {
			val, getErr := b.kv.Get(ctx, b.coder.EncodeObjectKey(key, currentRevision))
			if getErr != nil {
				return nil, 0, getErr
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
	iter, err := b.kv.Iter(ctx, startKey, endKey, 0, 0)
	if err != nil {
		return nil, 0, err
	}
	defer iter.Close()
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
			return nil, 0, decodeErr
		}
		if candidateRevision == 0 || !bytes.Equal(userKey, key) {
			continue
		}
		return iter.Val(), candidateRevision, nil
	}
}

// List implements Backend interface
func (b *backend) List(ctx context.Context, r *proto.RangeRequest) (resp *proto.RangeResponse, err error) {
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

	decodedRange, err := b.requiresDecodedUserRange(ctx, r.Key, r.End, reqRevision)
	if err != nil {
		return nil, err
	}
	var kvs []*proto.KeyValue
	if decodedRange {
		kvs, err = b.decodedUserRange(ctx, r.Key, r.End, reqRevision)
		if err == nil && limit > 0 && int64(len(kvs)) > limit {
			kvs = kvs[:limit]
		}
	} else {
		key := b.rangeStartKey(r.Key)
		rangeEnd := b.rangeEndKey(r.End)
		kvs, err = b.scanner.Range(ctx, key, rangeEnd, reqRevision, limit)
	}
	if err != nil {
		klog.ErrorS(err, "backend range err", "key", string(r.GetKey()), "end", string(r.GetEnd()), "revision", r.GetRevision())
		return nil, err
	}
	resp = &proto.RangeResponse{
		Header: responseHeader(curRevision),
	}

	if limit > 0 && len(kvs) > int(r.Limit) {
		resp.More = true
		kvs = kvs[0:r.Limit]
	}
	resp.Kvs = kvs
	return resp, nil
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
	encoded := b.coder.EncodeObjectKey(boundary, 0)
	rawPrefix := encoded[:len(encoded)-9]
	start := append(append([]byte(nil), rawPrefix...), 0)
	end := append(append([]byte(nil), rawPrefix...), '$')
	kvs, err := b.scanner.Range(ctx, start, end, revision, 1)
	if err != nil {
		return false, err
	}
	return len(kvs) != 0, nil
}

func (b *backend) decodedUserRange(
	ctx context.Context,
	start, end []byte,
	revision uint64,
) ([]*proto.KeyValue, error) {
	kvs, err := b.scanner.Range(
		ctx, b.ks.ObjectKeyspaceStart(), b.ks.ObjectKeyspaceEnd(), revision, 0,
	)
	if err != nil {
		return nil, err
	}
	filtered := kvs[:0]
	fromKey := isFromKeyEnd(end)
	for _, kv := range kvs {
		if bytes.Compare(kv.Key, start) < 0 {
			continue
		}
		if !fromKey && bytes.Compare(kv.Key, end) >= 0 {
			continue
		}
		filtered = append(filtered, kv)
	}
	sort.Slice(filtered, func(i, j int) bool {
		return bytes.Compare(filtered[i].Key, filtered[j].Key) < 0
	})
	return filtered, nil
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
	if c, served := b.CountAtRevision(ctx, r.Key, r.End, rev); served {
		return &proto.CountResponse{Header: responseHeader(rev), Count: uint64(c)}, nil
	}

	decodedRange, err := b.requiresDecodedUserRange(ctx, r.Key, r.End, rev)
	if err != nil {
		return nil, err
	}
	var count int
	if decodedRange {
		kvs, rangeErr := b.decodedUserRange(ctx, r.Key, r.End, rev)
		err = rangeErr
		count = len(kvs)
	} else {
		key, rangeEnd := b.rangeStartKey(r.Key), b.rangeEndKey(r.End)
		count, err = b.scanner.Count(ctx, key, rangeEnd, rev)
	}
	if err != nil {
		klog.Errorf("backend count %v return err %v", r, err)
		return nil, err
	}
	return &proto.CountResponse{
		Header: responseHeader(rev),
		Count:  uint64(count),
	}, nil
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

// RangeStream implements Backend interface: user-key range streaming for the
// etcd 3.7 KV.RangeStream RPC. It encodes the user range into the object
// keyspace (symmetric with List) — the scanner works on encoded object keys, so
// a raw user key never matches the magic-prefixed stored keys — pins the read
// revision, and streams the result in disjoint chunks.
func (b *backend) RangeStream(ctx context.Context, userStart, userEnd []byte, rev uint64) (<-chan *proto.StreamRangeResponse, error) {
	curRev, err := b.safeCurrentRevision(ctx)
	if err != nil {
		return nil, err
	}
	if rev == 0 {
		rev = curRev
	}
	decodedRange, err := b.requiresDecodedUserRange(ctx, userStart, userEnd, rev)
	if err != nil {
		return nil, err
	}
	if decodedRange {
		kvs, rangeErr := b.decodedUserRange(ctx, userStart, userEnd, rev)
		if rangeErr != nil {
			return nil, rangeErr
		}
		stream := make(chan *proto.StreamRangeResponse)
		go func() {
			defer close(stream)
			responses := make([]*proto.StreamRangeResponse, 0, 2)
			if len(kvs) != 0 {
				responses = append(responses, &proto.StreamRangeResponse{
					RangeResponse: &proto.RangeResponse{
						Header: responseHeader(rev),
						Kvs:    kvs,
						More:   true,
					},
				})
			}
			responses = append(responses, &proto.StreamRangeResponse{
				RangeResponse: &proto.RangeResponse{
					Header: responseHeader(rev),
				},
			})
			for _, response := range responses {
				select {
				case stream <- response:
				case <-ctx.Done():
					return
				}
			}
		}()
		return stream, nil
	}
	key := b.rangeStartKey(userStart)
	rangeEnd := b.rangeEndKey(userEnd)
	klog.V(klogLevel).InfoS("range stream", "start", Key(userStart), "end", Key(userEnd), "rev", rev)
	return b.scanner.RangeStream(ctx, key, rangeEnd, rev, false), nil
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
	return b.scanner.RangeStream(
		ctx, b.ks.ObjectKeyspaceStart(), b.ks.ObjectKeyspaceEnd(), rev, false,
	), nil
}

const (
	snapshotHistoryChunkRecords = 300
	snapshotHistoryChunkBytes   = 1536 * 1024
)

// SnapshotHistoryStream emits every retained physical version from one storage
// iterator snapshot. The iterator is created before this method returns, so a
// caller can use receipt of the first chunk as the same pin handshake used by
// SnapshotStream and release its logical-write barrier afterwards.
func (b *backend) SnapshotHistoryStream(ctx context.Context, rev uint64) (<-chan SnapshotHistoryChunk, error) {
	curRev, err := b.safeCurrentRevision(ctx)
	if err != nil {
		return nil, err
	}
	if rev == 0 {
		rev = curRev
	}
	iter, err := b.kv.Iter(ctx, b.ks.ObjectKeyspaceStart(), b.ks.ObjectKeyspaceEnd(), 0, 0)
	if err != nil {
		return nil, err
	}
	out := make(chan SnapshotHistoryChunk, 1)
	go func() {
		defer close(out)
		defer iter.Close()
		send := func(chunk SnapshotHistoryChunk) bool {
			select {
			case out <- chunk:
				return true
			case <-ctx.Done():
				return false
			}
		}
		records := make([]SnapshotHistoryRecord, 0, snapshotHistoryChunkRecords)
		chunkBytes := 0
		flushChunk := func() bool {
			if len(records) == 0 {
				return true
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
					if flushFamily() && flushChunk() {
						send(SnapshotHistoryChunk{Revision: rev, Done: true})
					}
					return
				}
				send(SnapshotHistoryChunk{Revision: rev, Err: err})
				return
			}
			rawKey := iter.Key()
			if b.ks.IsInternalStorageKey(rawKey) {
				continue
			}
			userKey, modRevision, decodeErr := b.coder.Decode(rawKey)
			if decodeErr != nil {
				continue
			}
			boundary, boundaryOK := b.coder.RevisionBoundaryForBorder(rawKey)
			if !boundaryOK {
				send(SnapshotHistoryChunk{Revision: rev, Err: fmt.Errorf("snapshot object key has no revision family boundary: %x", rawKey)})
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
				meta, rawValue, inlined := DecodeInlineValue(stored)
				if !inlined {
					meta, decodeErr = b.GetEtcdMetadata(ctx, userKey, modRevision)
					if decodeErr != nil {
						send(SnapshotHistoryChunk{Revision: rev, Err: decodeErr})
						return
					}
					rawValue = stored
				} else {
					record.LeaseKnown = true
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
