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
	"github.com/kubewharf/kubebrain/pkg/metrics"
	"github.com/kubewharf/kubebrain/pkg/storage"
)

var readIntegrityFenceTargets = []string{"object", "revision_index"}
var readIntegrityFenceOutcomes = []string{"armed", "failed"}

func initReadIntegrityFenceMetrics(metricCli metrics.Metrics) {
	if metricCli == nil {
		return
	}
	for _, target := range readIntegrityFenceTargets {
		for _, outcome := range readIntegrityFenceOutcomes {
			_ = metricCli.EmitCounter("read.integrity.fence", int64(0),
				metrics.Tag("target", target), metrics.Tag("outcome", outcome))
		}
	}
}

func emitReadIntegrityFence(metricCli metrics.Metrics, target, outcome string) {
	if metricCli == nil {
		return
	}
	_ = metricCli.EmitCounter("read.integrity.fence", 1,
		metrics.Tag("target", target), metrics.Tag("outcome", outcome))
}

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

type txnRevisionIndexExpectation struct {
	userKey  []byte
	revision uint64
	verb     proto.Event_EventType
}

type txnRevisionIndexCorruption struct {
	key            []byte
	expected       []byte
	missing        bool
	revision       uint64
	futureRevision uint64
}

const txnRevisionIndexValidationBatch = 512

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
) (retErr error) {
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
	defer func() { retErr = errors.Join(retErr, iter.Close()) }()
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
func (b *backend) validatePersistedTxnWitnesses(ctx context.Context, verifyObjects bool) (retErr error) {
	compactRevision, err := b.GetCompactRevisionFresh(ctx)
	if err != nil {
		return err
	}
	durableRevision, err := b.GetDurableRevision(ctx)
	if errors.Is(err, storage.ErrKeyNotFound) {
		// A missing durable watermark is validated by leadership initialization;
		// do not invent an upper bound while scanning a legacy/no-witness store.
		durableRevision = math.MaxUint64
	} else if err != nil {
		return err
	}
	start := b.ks.EncodeInternalKey(txnWitnessPrefix)
	witnesses, err := b.kv.Iter(ctx, start, rawPrefixEnd(start), 0, 0)
	if err != nil {
		return err
	}
	defer func() { retErr = errors.Join(retErr, witnesses.Close()) }()
	var events storage.Iter
	defer func() {
		if events != nil {
			retErr = errors.Join(retErr, events.Close())
		}
	}()

	// Both physical families sort by big-endian revision. Merge their iterators
	// in one pass and retain only the current transaction's hash state: startup
	// memory is O(events in one txn), not O(uncompacted transactions).
	var eventReady, eventEOF bool
	var eventRevision uint64
	var eventUserKey []byte
	var corruptFound bool
	pendingIndexes := make([]txnRevisionIndexExpectation, 0, txnRevisionIndexValidationBatch)
	pendingSeals := make(map[uint64]struct {
		key []byte
		raw []byte
	})
	flushIndexes := func() error {
		if len(pendingIndexes) == 0 {
			return nil
		}
		for attempts := 0; len(pendingIndexes) != 0 && attempts <= len(pendingIndexes); attempts++ {
			retained := pendingIndexes[:0]
			for i := range pendingIndexes {
				if pendingIndexes[i].revision > compactRevision {
					retained = append(retained, pendingIndexes[i])
				}
			}
			pendingIndexes = retained
			if len(pendingIndexes) == 0 {
				clear(pendingSeals)
				return nil
			}
			evidence, indexErr := b.validateTxnRevisionIndexes(ctx, pendingIndexes, durableRevision)
			if indexErr == nil {
				pendingIndexes = pendingIndexes[:0]
				clear(pendingSeals)
				return nil
			}
			if !errors.Is(indexErr, ErrTxnWitnessCorrupt) {
				return indexErr
			}
			seal, ok := pendingSeals[evidence.revision]
			if !ok {
				return fmt.Errorf("revision-index corruption has no transaction seal at revision %d", evidence.revision)
			}
			present, checkErr := b.txnWitnessStillPresent(ctx, seal.key, seal.raw)
			if checkErr != nil {
				return checkErr
			}
			refreshed, refreshErr := b.GetCompactRevisionFresh(ctx)
			if refreshErr != nil {
				return refreshErr
			}
			compactRevision = max(compactRevision, refreshed)
			if !present || evidence.revision <= compactRevision {
				filtered := pendingIndexes[:0]
				for i := range pendingIndexes {
					if pendingIndexes[i].revision != evidence.revision {
						filtered = append(filtered, pendingIndexes[i])
					}
				}
				pendingIndexes = filtered
				delete(pendingSeals, evidence.revision)
				continue
			}
			stillPresent, evidenceErr := b.txnRevisionIndexCorruptionStillPresent(ctx, evidence)
			if evidenceErr != nil {
				return evidenceErr
			}
			if !stillPresent {
				continue // an operator repaired the index before the alarm linearization point
			}
			if err := b.armPersistedWitnessCorrupt(ctx, evidence.revision, indexErr); err != nil {
				return err
			}
			corruptFound = true
			pendingIndexes = pendingIndexes[:0]
			clear(pendingSeals)
			return nil
		}
		return fmt.Errorf("revision-index validation did not converge after concurrent repairs")
	}
	queueIndexes := func(expectations []txnRevisionIndexExpectation, key, raw []byte, revision uint64) error {
		for len(expectations) != 0 {
			space := txnRevisionIndexValidationBatch - len(pendingIndexes)
			take := min(space, len(expectations))
			pendingIndexes = append(pendingIndexes, expectations[:take]...)
			pendingSeals[revision] = struct {
				key []byte
				raw []byte
			}{key: key, raw: raw}
			expectations = expectations[take:]
			if len(pendingIndexes) == txnRevisionIndexValidationBatch {
				if err := flushIndexes(); err != nil {
					return err
				}
			}
		}
		return nil
	}
	for {
		if err := witnesses.Next(ctx); err != nil {
			if err == io.EOF {
				if flushErr := flushIndexes(); flushErr != nil {
					return flushErr
				}
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
			indexExpectations := make([]txnRevisionIndexExpectation, 0, record.count)
			objectKeys := make([][]byte, 0, record.count)
			seenObjects := make(map[string]struct{}, record.count)
			objectRevisions := make(map[string]uint64, record.count)
			objectUserKeys := make(map[string][]byte, record.count)
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
				if referenceErr == nil {
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
						indexExpectations = append(indexExpectations, txnRevisionIndexExpectation{
							userKey: append([]byte(nil), eventUserKey...), revision: revision, verb: verb,
						})
					}
					if verifyObjects && revision > compactRevision && referenceErr == nil {
						objectKey := b.coder.EncodeObjectKey(eventUserKey, valueRevision)
						if _, duplicate := seenObjects[string(objectKey)]; !duplicate {
							objectID := string(objectKey)
							seenObjects[objectID] = struct{}{}
							objectRevisions[objectID] = valueRevision
							objectUserKeys[objectID] = append([]byte(nil), eventUserKey...)
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
			}
			if cause == nil && verifyObjects && revision > compactRevision {
				values, incomplete, loadErr := b.loadEventValues(ctx, objectKeys)
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
					if revision > compactRevision {
						cause = fmt.Errorf("%w: persisted witness at revision %d references a missing object version",
							ErrTxnWitnessCorrupt, revision)
					}
				} else {
					for _, objectKey := range objectKeys {
						objectID := string(objectKey)
						validationErr := b.validateEventObjectValue(
							ctx, objectUserKeys[objectID], objectRevisions[objectID], values[objectID],
						)
						if validationErr == nil {
							continue
						}
						if !errors.Is(validationErr, ErrInvalidMVCCMetadata) {
							return validationErr
						}
						refreshed, refreshErr := b.GetCompactRevisionFresh(ctx)
						if refreshErr != nil {
							return refreshErr
						}
						compactRevision = max(compactRevision, refreshed)
						if revision > compactRevision {
							cause = fmt.Errorf("%w: persisted witness at revision %d references an invalid object value: %v",
								ErrTxnWitnessCorrupt, revision, validationErr)
						}
						break
					}
				}
			}
			if cause == nil && revision > compactRevision {
				if queueErr := queueIndexes(indexExpectations, key, raw, revision); queueErr != nil {
					return queueErr
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

func (b *backend) validateTxnRevisionIndexes(
	ctx context.Context, expectations []txnRevisionIndexExpectation, durableRevision uint64,
) (*txnRevisionIndexCorruption, error) {
	if len(expectations) == 0 {
		return nil, nil
	}
	indexKeys := make([][]byte, len(expectations))
	for i := range expectations {
		indexKeys[i] = b.coder.EncodeRevisionKey(expectations[i].userKey)
	}
	values, incomplete, err := b.loadEventValues(ctx, indexKeys)
	if err != nil {
		return nil, err
	}
	type targetObjectExpectation struct {
		key             []byte
		userKey         []byte
		indexTombstone  bool
		objectRevision  uint64
		witnessRevision uint64
	}
	targets := make([]targetObjectExpectation, 0, len(expectations))
	seenTargets := make(map[string]struct{}, len(expectations))
	for i := range expectations {
		expectation := expectations[i]
		indexKey := indexKeys[i]
		raw, found := values[string(indexKey)]
		if !found {
			if !incomplete {
				return nil, fmt.Errorf("revision index batch omitted key %q", expectation.userKey)
			}
			evidence := &txnRevisionIndexCorruption{key: indexKey, missing: true, revision: expectation.revision}
			return evidence, fmt.Errorf("%w: persisted witness revision %d key %q has no revision index",
				ErrTxnWitnessCorrupt, expectation.revision, expectation.userKey)
		}
		currentRevision, tombstone, parseErr := coder.ParseRevision(raw)
		if parseErr != nil {
			evidence := &txnRevisionIndexCorruption{
				key: indexKey, expected: append([]byte(nil), raw...), revision: expectation.revision,
			}
			return evidence, fmt.Errorf("%w: persisted witness revision %d key %q has invalid revision index: %v",
				ErrTxnWitnessCorrupt, expectation.revision, expectation.userKey, parseErr)
		}
		wantTombstone := expectation.verb == proto.Event_DELETE
		if currentRevision > durableRevision {
			// The validator reads the durable watermark before the index batch.
			// A normal transaction may commit between those reads; its index and
			// durable watermark are atomic, but the older watermark snapshot is
			// not. Re-read the authoritative marker after observing the index so
			// only a revision that is still future can become corruption evidence.
			refreshedDurable, durableErr := b.GetDurableRevision(ctx)
			if durableErr != nil {
				return nil, durableErr
			}
			if refreshedDurable > durableRevision {
				durableRevision = refreshedDurable
			}
		}
		if currentRevision > durableRevision {
			evidence := &txnRevisionIndexCorruption{
				key: indexKey, expected: append([]byte(nil), raw...), revision: expectation.revision,
				futureRevision: currentRevision,
			}
			return evidence, fmt.Errorf(
				"%w: current revision index for key %q is %d above durable revision %d",
				ErrTxnWitnessCorrupt, expectation.userKey, currentRevision, durableRevision,
			)
		}
		if currentRevision < expectation.revision ||
			(currentRevision == expectation.revision && tombstone != wantTombstone) {
			evidence := &txnRevisionIndexCorruption{
				key: indexKey, expected: append([]byte(nil), raw...), revision: expectation.revision,
			}
			return evidence, fmt.Errorf(
				"%w: persisted witness revision %d key %q conflicts with current revision index %d tombstone=%t",
				ErrTxnWitnessCorrupt, expectation.revision, expectation.userKey, currentRevision, tombstone,
			)
		}
		objectKey := b.coder.EncodeObjectKey(expectation.userKey, currentRevision)
		if _, duplicate := seenTargets[string(objectKey)]; !duplicate {
			seenTargets[string(objectKey)] = struct{}{}
			targets = append(targets, targetObjectExpectation{
				key: objectKey, userKey: expectation.userKey, indexTombstone: tombstone,
				objectRevision: currentRevision, witnessRevision: expectation.revision,
			})
		}
	}
	objectKeys := make([][]byte, len(targets))
	for i := range targets {
		objectKeys[i] = targets[i].key
	}
	objects, incomplete, err := b.loadEventValues(ctx, objectKeys)
	if err != nil {
		return nil, err
	}
	for i := range targets {
		target := targets[i]
		value, found := objects[string(target.key)]
		if !found {
			if !incomplete {
				return nil, fmt.Errorf("target object batch omitted key %q at revision %d", target.userKey, target.objectRevision)
			}
			evidence := &txnRevisionIndexCorruption{
				key: target.key, missing: true, revision: target.witnessRevision,
			}
			return evidence, fmt.Errorf(
				"%w: current revision index for key %q references missing object revision %d",
				ErrTxnWitnessCorrupt, target.userKey, target.objectRevision,
			)
		}
		objectTombstone := bytes.Equal(value, tombStoneBytes)
		if objectTombstone != target.indexTombstone {
			evidence := &txnRevisionIndexCorruption{
				key: target.key, expected: append([]byte(nil), value...), revision: target.witnessRevision,
			}
			return evidence, fmt.Errorf(
				"%w: current revision index for key %q has tombstone=%t but object revision %d has tombstone=%t",
				ErrTxnWitnessCorrupt, target.userKey, target.indexTombstone, target.objectRevision, objectTombstone,
			)
		}
	}
	return nil, nil
}

func (b *backend) txnRevisionIndexCorruptionStillPresent(
	ctx context.Context, evidence *txnRevisionIndexCorruption,
) (bool, error) {
	if evidence.futureRevision != 0 {
		durableRevision, err := b.GetDurableRevision(ctx)
		if err != nil {
			return false, err
		}
		if durableRevision >= evidence.futureRevision {
			return false, nil
		}
	}
	current, err := b.kv.Get(ctx, evidence.key)
	if errors.Is(err, storage.ErrKeyNotFound) {
		return evidence.missing, nil
	}
	if err != nil {
		return false, err
	}
	return !evidence.missing && bytes.Equal(current, evidence.expected), nil
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

// persistWitnessedObjectCorruption turns a point-read metadata failure into a
// durable safety transition only when the exact object is covered by a complete
// transaction witness. Legacy/unsealed and already-compacted revisions still
// return DataLoss, but cannot create an alarm that Disarm is unable to prove.
func (b *backend) persistWitnessedObjectCorruption(
	ctx context.Context, userKey []byte, revision uint64, objectKey, expectedValue []byte, cause error,
) error {
	if revision == 0 || !errors.Is(cause, ErrInvalidMVCCMetadata) {
		return cause
	}
	compactRevision, err := b.GetCompactRevisionFresh(ctx)
	if err != nil || (compactRevision > 0 && revision <= compactRevision) {
		return cause
	}

	witnessKey := b.ks.EncodeInternalKey(txnWitnessLogicalKey(revision))
	witnessRaw, err := b.kv.Get(ctx, witnessKey)
	if err != nil {
		return cause
	}
	if _, err := decodeTxnWitness(witnessRaw); err != nil {
		return cause
	}
	iter, err := b.kv.Iter(ctx, b.ks.EventLogRangeStart(revision), b.ks.EventLogRangeEnd(revision), 0, 0)
	if err != nil {
		return cause
	}
	var rawEntries []eventLogRawEntry
	targetCovered := false
	for {
		nextErr := iter.Next(ctx)
		if nextErr != nil {
			closeErr := iter.Close()
			if nextErr != io.EOF {
				return errors.Join(cause, nextErr, closeErr)
			}
			if closeErr != nil {
				return errors.Join(cause, closeErr)
			}
			break
		}
		entryKey := append([]byte(nil), iter.Key()...)
		entryValue := append([]byte(nil), iter.Val()...)
		entryRevision, entryUserKey, decodeErr := b.ks.DecodeEventLogKey(entryKey)
		if decodeErr != nil || entryRevision != revision {
			return errors.Join(cause, iter.Close())
		}
		verbByte, previousRevision, _, _, _, ok := coder.DecodeOrderedEventLogValue(entryValue)
		verb := proto.Event_EventType(verbByte)
		if !ok || (verb != proto.Event_CREATE && verb != proto.Event_PUT) {
			if bytes.Equal(entryUserKey, userKey) {
				return errors.Join(cause, iter.Close())
			}
		} else if bytes.Equal(entryUserKey, userKey) {
			targetCovered = (verb == proto.Event_CREATE && previousRevision == 0) ||
				(verb == proto.Event_PUT && previousRevision > 0 && previousRevision < revision)
		}
		rawEntries = append(rawEntries, eventLogRawEntry{key: entryKey, value: entryValue})
	}
	if !targetCovered || !bytes.Equal(encodeTxnWitness(rawEntries), witnessRaw) {
		return cause
	}

	// Repair and compaction can race this diagnostic read. Reconfirm all durable
	// evidence immediately before raising the shared write fence.
	currentValue, err := b.kv.Get(ctx, objectKey)
	if err != nil || !bytes.Equal(currentValue, expectedValue) {
		return cause
	}
	compactRevision, err = b.GetCompactRevisionFresh(ctx)
	if err != nil || (compactRevision > 0 && revision <= compactRevision) {
		return cause
	}
	present, err := b.txnWitnessStillPresent(ctx, witnessKey, witnessRaw)
	if err != nil || !present {
		return cause
	}
	alarmCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), unaryRpcTimeout)
	defer cancel()
	if alarmErr := b.ArmCorrupt(alarmCtx, b.localAlarmMemberID()); alarmErr != nil {
		b.metricCli.EmitCounter("read.object.corrupt_alarm_failed", 1)
		emitReadIntegrityFence(b.metricCli, "object", "failed")
		return errors.Join(cause, fmt.Errorf("persist CORRUPT alarm: %w", alarmErr))
	}
	b.metricCli.EmitCounter("read.object.corrupt_alarm_armed", 1)
	emitReadIntegrityFence(b.metricCli, "object", "armed")
	klog.ErrorS(cause, "witnessed object value is corrupt; armed CORRUPT alarm",
		"key", Key(userKey), "revision", revision, "memberID", b.localAlarmMemberID())
	return cause
}

// persistWitnessedRevisionIndexCorruption upgrades a request-local point-read
// failure to a durable write fence only after the cold transaction-witness
// validator independently proves the physical index/object contradiction.
// The count index is intentionally never sufficient evidence for an alarm.
func (b *backend) persistWitnessedRevisionIndexCorruption(ctx context.Context, cause error) error {
	if !errors.Is(cause, ErrInvalidMVCCMetadata) {
		return cause
	}
	alarmCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), unaryRpcTimeout)
	defer cancel()
	validationErr := b.validatePersistedTxnWitnesses(alarmCtx, false)
	switch {
	case validationErr == nil:
		return cause
	case errors.Is(validationErr, ErrTxnWitnessCorrupt):
		b.metricCli.EmitCounter("read.revision_index.corrupt_alarm_armed", 1)
		emitReadIntegrityFence(b.metricCli, "revision_index", "armed")
		return cause
	default:
		b.metricCli.EmitCounter("read.revision_index.corrupt_alarm_failed", 1)
		emitReadIntegrityFence(b.metricCli, "revision_index", "failed")
		return errors.Join(cause, fmt.Errorf("validate persisted transaction witnesses: %w", validationErr))
	}
}

func (b *backend) armPersistedWitnessCorrupt(ctx context.Context, revision uint64, cause error) error {
	if err := b.ArmCorrupt(ctx, b.localAlarmMemberID()); err != nil {
		b.metricCli.EmitCounter("txn.witness.restart_corruption", 1, metrics.Tag("outcome", "failed"))
		return fmt.Errorf("persist CORRUPT alarm for transaction witness revision %d: %w", revision, err)
	}
	b.metricCli.EmitCounter("txn.witness.restart_corrupt", 1)
	b.metricCli.EmitCounter("txn.witness.restart_corruption", 1, metrics.Tag("outcome", "armed"))
	klog.ErrorS(cause, "persisted transaction witness is corrupt; armed CORRUPT alarm",
		"revision", revision, "memberID", b.localAlarmMemberID())
	return nil
}

func initRestartWitnessCorruptionMetrics(metricCli metrics.Metrics) {
	if metricCli == nil {
		return
	}
	for _, outcome := range []string{"armed", "failed"} {
		_ = metricCli.EmitCounter("txn.witness.restart_corruption", 0, metrics.Tag("outcome", outcome))
	}
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
		if err == io.EOF {
			err = nil
		}
		err = errors.Join(err, it.Close())
		if err != nil {
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
