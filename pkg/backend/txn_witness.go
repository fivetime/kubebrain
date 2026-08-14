// Copyright 2026 ByteDance and/or its affiliates
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

package backend

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"sort"

	"k8s.io/klog/v2"

	proto "github.com/kubewharf/kubebrain-client/api/v2rpc"
	"github.com/kubewharf/kubebrain/pkg/backend/coder"
	"github.com/kubewharf/kubebrain/pkg/storage"
)

var txnWitnessPrefix = []byte("txn/witness/")

const leaseIncarnationFenceVersion = byte(2)

// ErrTxnWitnessUnsupportedVersion means storage was written by a newer binary.
// It fences this process from leadership without classifying healthy, opaque
// metadata as data corruption; an operator must roll forward, not repair data.
var ErrTxnWitnessUnsupportedVersion = errors.New("unsupported transaction witness version")

const (
	txnWitnessVersion = byte(1)
	txnWitnessSize    = 1 + 4 + sha256.Size
)

var leaseIncarnationFormatFenceValue = func() []byte {
	value := make([]byte, txnWitnessSize)
	value[0] = leaseIncarnationFenceVersion
	copy(value[1:], []byte("lease-incarnation-v1"))
	return value
}()

type txnWitnessRecord struct {
	count  uint32
	digest [sha256.Size]byte
}

func txnWitnessLogicalKey(revision uint64) []byte {
	key := make([]byte, len(txnWitnessPrefix)+8)
	copy(key, txnWitnessPrefix)
	binary.BigEndian.PutUint64(key[len(txnWitnessPrefix):], revision)
	return key
}

func leaseIncarnationFormatFenceKey() []byte {
	return txnWitnessLogicalKey(math.MaxInt64)
}

// EnsureLeaseIncarnationFormatFence makes the incarnation-bearing lease layout
// an explicit roll-forward boundary. The marker occupies the reserved final
// witness revision and uses a future witness version. A pre-incarnation binary
// therefore withdraws during leadership initialization instead of acquiring a
// term and looping forever on strict lease metadata decoding.
func (b *backend) EnsureLeaseIncarnationFormatFence(ctx context.Context) error {
	key := leaseIncarnationFormatFenceKey()
	for attempts := 0; attempts < 2; attempts++ {
		current, err := b.InternalGet(ctx, key)
		switch {
		case err == nil && bytes.Equal(current, leaseIncarnationFormatFenceValue):
			return nil
		case err == nil:
			return fmt.Errorf("%w: incompatible lease incarnation format fence", ErrTxnWitnessUnsupportedVersion)
		case !errors.Is(err, storage.ErrKeyNotFound):
			return err
		}
		err = b.InternalCAS(ctx, []InternalCASOp{{
			Key: key, ExpectedExists: false, Value: leaseIncarnationFormatFenceValue,
		}})
		if err == nil {
			return nil
		}
		if !errors.Is(err, storage.ErrCASFailed) {
			return err
		}
	}
	return fmt.Errorf("persist lease incarnation format fence: %w", storage.ErrCASFailed)
}

func encodeTxnWitness(entries []eventLogRawEntry) []byte {
	sort.Slice(entries, func(i, j int) bool { return bytes.Compare(entries[i].key, entries[j].key) < 0 })
	h := sha256.New()
	for i := range entries {
		writeWitnessDigestEntry(h, entries[i])
	}
	out := make([]byte, txnWitnessSize)
	out[0] = txnWitnessVersion
	binary.BigEndian.PutUint32(out[1:5], uint32(len(entries)))
	copy(out[5:], h.Sum(nil))
	return out
}

func decodeTxnWitness(raw []byte) (txnWitnessRecord, error) {
	if len(raw) > 0 && raw[0] != txnWitnessVersion {
		return txnWitnessRecord{}, fmt.Errorf("%w: %d", ErrTxnWitnessUnsupportedVersion, raw[0])
	}
	if len(raw) != txnWitnessSize {
		return txnWitnessRecord{}, fmt.Errorf("invalid transaction witness encoding")
	}
	record := txnWitnessRecord{count: binary.BigEndian.Uint32(raw[1:5])}
	if record.count == 0 {
		return txnWitnessRecord{}, fmt.Errorf("transaction witness has zero events")
	}
	copy(record.digest[:], raw[5:])
	return record, nil
}

type eventLogRawEntry struct {
	key   []byte
	value []byte
}

type witnessHashWriter interface {
	Write([]byte) (int, error)
}

func writeWitnessDigestEntry(h witnessHashWriter, entry eventLogRawEntry) {
	var size [4]byte
	binary.BigEndian.PutUint32(size[:], uint32(len(entry.key)))
	_, _ = h.Write(size[:])
	_, _ = h.Write(entry.key)
	binary.BigEndian.PutUint32(size[:], uint32(len(entry.value)))
	_, _ = h.Write(size[:])
	_, _ = h.Write(entry.value)
}

func (b *backend) stageTxnWitness(txn storage.AtomicBatch, preps []txnPrep, revision uint64) error {
	entries := make([]eventLogRawEntry, 0, txnEffectiveUserWriteCount(preps))
	total := uint32(txnEffectiveUserWriteCount(preps))
	var sub uint32
	for i := range preps {
		p := &preps[i]
		if !p.effective || p.op.Internal {
			continue
		}
		verb, previousRevision := eventMarkerIdentity(p)
		key, value := encodeEventLogEntry(b.ks, revision, p.op.Key, verb, previousRevision, sub, total)
		entries = append(entries, eventLogRawEntry{key: key, value: value})
		sub++
	}
	if len(entries) == 0 {
		return nil
	}
	return txn.Put(b.ks.EncodeInternalKey(txnWitnessLogicalKey(revision)), encodeTxnWitness(entries), 0)
}

func eventMarkerIdentity(p *txnPrep) (verb proto.Event_EventType, previousRevision uint64) {
	verb, previousRevision = proto.Event_PUT, p.curRev
	if p.op.Delete {
		verb = proto.Event_DELETE
	} else if p.create {
		verb, previousRevision = proto.Event_CREATE, 0
	}
	return verb, previousRevision
}

// validateEventLogWindowWitnesses proves that every user revision in a trusted
// replay window has exactly the event-marker set sealed by its committing TiKV
// transaction. Per-entry total fields detect a partial multi-key revision, but
// cannot detect an entire missing single-key revision; the durable witnesses
// close that gap and also bind the marker bytes, including operation order.
func (b *backend) validateEventLogWindowWitnesses(
	ctx context.Context, entries []eventLogPending, fromRevision, toRevision uint64,
) error {
	if fromRevision == 0 || toRevision < fromRevision {
		return fmt.Errorf("invalid transaction witness window [%d,%d]", fromRevision, toRevision)
	}
	byRevision := make(map[uint64][]eventLogRawEntry)
	for i := range entries {
		entry := entries[i]
		key, value := encodeEventLogEntry(
			b.ks, entry.rev, entry.userKey, entry.verb, entry.prevRev, entry.sub, entry.total,
		)
		byRevision[entry.rev] = append(byRevision[entry.rev], eventLogRawEntry{key: key, value: value})
	}

	base := b.ks.EncodeInternalKey(txnWitnessPrefix)
	start := b.ks.EncodeInternalKey(txnWitnessLogicalKey(fromRevision))
	end := rawPrefixEnd(b.ks.EncodeInternalKey(txnWitnessLogicalKey(toRevision)))
	iter, err := b.kv.Iter(ctx, start, end, 0, 0)
	if err != nil {
		return err
	}
	defer iter.Close()
	next := fromRevision
	for {
		if err := iter.Next(ctx); err != nil {
			if err == io.EOF {
				break
			}
			return err
		}
		key := iter.Key()
		if len(key) != len(base)+8 || !bytes.HasPrefix(key, base) {
			return fmt.Errorf("transaction witness window contains malformed key")
		}
		revision := binary.BigEndian.Uint64(key[len(base):])
		if revision != next {
			return fmt.Errorf("transaction witness mismatch: missing witness for revision %d before revision %d", next, revision)
		}
		record, decodeErr := decodeTxnWitness(iter.Val())
		if decodeErr != nil {
			return fmt.Errorf("transaction witness mismatch at revision %d: %w", revision, decodeErr)
		}
		encoded := encodeTxnWitness(byRevision[revision])
		if len(byRevision[revision]) == 0 || !bytes.Equal(encoded, iter.Val()) {
			return fmt.Errorf(
				"transaction witness mismatch at revision %d: found %d events, expected %d",
				revision, len(byRevision[revision]), record.count,
			)
		}
		delete(byRevision, revision)
		next++
	}
	if next <= toRevision {
		return fmt.Errorf("transaction witness mismatch: missing witness for revision %d", next)
	}
	if len(byRevision) != 0 {
		return fmt.Errorf("transaction witness mismatch: event revision has no witness")
	}
	return nil
}

// validatePersistedTxnWitnesses verifies only revisions explicitly sealed by
// this binary. Legacy revisions have no seal and remain upgrade-compatible.
// verifyObjects is reserved for CORRUPT disarm: leadership startup must remain
// one sequential witness/event merge, while an operator asking to reopen writes
// must also prove every uncompacted event's referenced object version exists.
func (b *backend) validatePersistedTxnWitnesses(ctx context.Context, verifyObjects bool) error {
	compactRevision := uint64(0)
	if verifyObjects {
		var err error
		compactRevision, err = b.GetCompactRevisionFresh(ctx)
		if err != nil {
			return err
		}
	}
	start := b.ks.EncodeInternalKey(txnWitnessPrefix)
	witnesses, err := b.kv.Iter(ctx, start, rawPrefixEnd(start), 0, 0)
	if err != nil {
		return err
	}
	defer witnesses.Close()
	var events storage.Iter
	defer func() {
		if events != nil {
			_ = events.Close()
		}
	}()

	// Both physical families sort by big-endian revision. Merge their iterators
	// in one pass and retain only the current transaction's hash state: startup
	// memory is O(events in one txn), not O(uncompacted transactions).
	var eventReady, eventEOF bool
	var eventRevision uint64
	var eventUserKey []byte
	var corruptFound bool
	for {
		if err := witnesses.Next(ctx); err != nil {
			if err == io.EOF {
				if corruptFound {
					return ErrTxnWitnessCorrupt
				}
				return nil
			}
			return err
		}
		key := append([]byte(nil), witnesses.Key()...)
		raw := append([]byte(nil), witnesses.Val()...)
		logical := key[len(start)-len(txnWitnessPrefix):]
		var revision uint64
		var cause error
		if bytes.Equal(logical, leaseIncarnationFormatFenceKey()) {
			revision = math.MaxInt64
			if bytes.Equal(raw, leaseIncarnationFormatFenceValue) {
				continue
			}
			if len(raw) > 0 && raw[0] > leaseIncarnationFenceVersion {
				return fmt.Errorf("lease incarnation format fence: %w: %d", ErrTxnWitnessUnsupportedVersion, raw[0])
			}
			cause = fmt.Errorf("%w: invalid lease incarnation format fence", ErrTxnWitnessCorrupt)
		} else if len(logical) != len(txnWitnessPrefix)+8 {
			cause = fmt.Errorf("%w: malformed transaction witness key", ErrTxnWitnessCorrupt)
		} else {
			revision = binary.BigEndian.Uint64(logical[len(txnWitnessPrefix):])
			if revision == 0 || revision > math.MaxInt64 {
				cause = fmt.Errorf("%w: invalid transaction witness revision", ErrTxnWitnessCorrupt)
			}
		}
		var record txnWitnessRecord
		var decodeErr error
		if cause == nil {
			record, decodeErr = decodeTxnWitness(raw)
		}
		if cause == nil && errors.Is(decodeErr, ErrTxnWitnessUnsupportedVersion) {
			return fmt.Errorf("transaction witness at revision %d: %w", revision, decodeErr)
		}
		if cause == nil && decodeErr != nil {
			cause = fmt.Errorf("%w: %v", ErrTxnWitnessCorrupt, decodeErr)
		}
		if cause == nil {
			if events == nil {
				// Witness keys are revision ordered, so the first valid seal is the
				// lowest revision that can matter. Do not rescan an arbitrarily large
				// legacy event prefix that predates witness support.
				events, err = b.kv.Iter(ctx, b.ks.EventLogRangeStart(revision), b.ks.EventLogRangeEnd(math.MaxInt64), 0, 0)
				if err != nil {
					return err
				}
			}
			h := sha256.New()
			var count uint32
			var referenceErr error
			objectKeys := make([][]byte, 0, record.count)
			seenObjects := make(map[string]struct{}, record.count)
			for !eventEOF {
				if !eventReady {
					if nextErr := events.Next(ctx); nextErr != nil {
						if nextErr == io.EOF {
							eventEOF = true
							break
						}
						return nextErr
					}
					eventRevision, eventUserKey, decodeErr = b.ks.DecodeEventLogKey(events.Key())
					if decodeErr != nil {
						return decodeErr
					}
					eventReady = true
				}
				if eventRevision < revision {
					eventReady = false
					continue
				}
				if eventRevision > revision {
					break
				}
				writeWitnessDigestEntry(h, eventLogRawEntry{key: events.Key(), value: events.Val()})
				if verifyObjects && revision >= compactRevision && referenceErr == nil {
					verbByte, previousRevision, _, _, _, ok := coder.DecodeOrderedEventLogValue(events.Val())
					valueRevision := revision
					verb := proto.Event_EventType(verbByte)
					switch {
					case !ok:
						referenceErr = fmt.Errorf("event value is undecodable")
					case verb == proto.Event_DELETE && previousRevision > 0 && previousRevision < revision:
						valueRevision = previousRevision
					case verb == proto.Event_CREATE && previousRevision == 0:
					case verb == proto.Event_PUT && previousRevision > 0 && previousRevision < revision:
					default:
						referenceErr = fmt.Errorf("event has invalid object reference")
					}
					if referenceErr == nil {
						objectKey := b.coder.EncodeObjectKey(eventUserKey, valueRevision)
						if _, duplicate := seenObjects[string(objectKey)]; !duplicate {
							seenObjects[string(objectKey)] = struct{}{}
							objectKeys = append(objectKeys, objectKey)
						}
					}
				}
				count++
				eventReady = false
			}
			if count != record.count || !bytes.Equal(h.Sum(nil), record.digest[:]) {
				cause = fmt.Errorf("%w: persisted witness mismatch at revision %d: found %d events, expected %d",
					ErrTxnWitnessCorrupt, revision, count, record.count)
			} else if referenceErr != nil {
				cause = fmt.Errorf("%w: persisted witness object reference at revision %d: %v",
					ErrTxnWitnessCorrupt, revision, referenceErr)
			} else if verifyObjects && revision >= compactRevision {
				_, incomplete, loadErr := b.loadEventValues(ctx, objectKeys)
				if loadErr != nil {
					return loadErr
				}
				if incomplete {
					// Physical compaction may have advanced after our initial
					// snapshot. Only a still-supported revision is corruption.
					refreshed, refreshErr := b.GetCompactRevisionFresh(ctx)
					if refreshErr != nil {
						return refreshErr
					}
					compactRevision = max(compactRevision, refreshed)
					if revision >= compactRevision {
						cause = fmt.Errorf("%w: persisted witness at revision %d references a missing object version",
							ErrTxnWitnessCorrupt, revision)
					}
				}
			}
		}
		if cause == nil {
			continue
		}
		present, checkErr := b.txnWitnessStillPresent(ctx, key, raw)
		if checkErr != nil {
			return checkErr
		}
		if !present {
			continue // compaction removed the seal before deleting these events
		}
		if err := b.armPersistedWitnessCorrupt(ctx, revision, cause); err != nil {
			return err
		}
		corruptFound = true
	}
}

func (b *backend) txnWitnessStillPresent(ctx context.Context, key, expected []byte) (bool, error) {
	current, err := b.kv.Get(ctx, key)
	if errors.Is(err, storage.ErrKeyNotFound) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if !bytes.Equal(current, expected) {
		return false, fmt.Errorf("transaction witness changed during leadership validation")
	}
	return true, nil
}

func (b *backend) armPersistedWitnessCorrupt(ctx context.Context, revision uint64, cause error) error {
	if err := b.ArmCorrupt(ctx, b.localAlarmMemberID()); err != nil {
		return fmt.Errorf("persist CORRUPT alarm for transaction witness revision %d: %w", revision, err)
	}
	b.metricCli.EmitCounter("txn.witness.restart_corrupt", 1)
	klog.ErrorS(cause, "persisted transaction witness is corrupt; armed CORRUPT alarm",
		"revision", revision, "memberID", b.localAlarmMemberID())
	return nil
}

func (b *backend) cleanupTxnWitnesses(ctx context.Context, through uint64) bool {
	if through == 0 {
		return true
	}
	start := b.ks.EncodeInternalKey(txnWitnessPrefix)
	end := b.ks.EncodeInternalKey(txnWitnessLogicalKey(through))
	end = rawPrefixEnd(end)
	for {
		it, err := b.kv.Iter(ctx, start, end, 0, uint64(eventLogCleanupBatch))
		if err != nil {
			klog.ErrorS(err, "transaction witness cleanup iter failed", "through", through)
			return false
		}
		var keys [][]byte
		for {
			err = it.Next(ctx)
			if err != nil {
				break
			}
			keys = append(keys, append([]byte(nil), it.Key()...))
		}
		_ = it.Close()
		if err != nil && err != io.EOF {
			klog.ErrorS(err, "transaction witness cleanup scan failed", "through", through)
			return false
		}
		if len(keys) == 0 {
			return true
		}
		batch := b.kv.BeginBatchWrite()
		for _, key := range keys {
			batch.Del(key)
		}
		if err := batch.Commit(ctx); err != nil {
			klog.ErrorS(err, "transaction witness cleanup batch failed", "through", through)
			return false
		}
		b.metricCli.EmitCounter("txn.witness.cleaned", len(keys))
	}
}
