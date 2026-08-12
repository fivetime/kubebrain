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
	"hash"
	"io"
	"math"
	"sort"

	"k8s.io/klog/v2"

	proto "github.com/kubewharf/kubebrain-client/api/v2rpc"
	"github.com/kubewharf/kubebrain/pkg/storage"
)

var txnWitnessPrefix = []byte("txn/witness/")

const (
	txnWitnessVersion = byte(1)
	txnWitnessSize    = 1 + 4 + sha256.Size
)

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
	if len(raw) != txnWitnessSize || raw[0] != txnWitnessVersion {
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

// validatePersistedTxnWitnesses verifies only revisions explicitly sealed by
// this binary. Legacy revisions have no seal and remain upgrade-compatible.
func (b *backend) validatePersistedTxnWitnesses(ctx context.Context) error {
	start := b.ks.EncodeInternalKey(txnWitnessPrefix)
	it, err := b.kv.Iter(ctx, start, rawPrefixEnd(start), 0, 0)
	if err != nil {
		return err
	}
	type corruptWitness struct {
		revision uint64
		cause    error
		key      []byte
		raw      []byte
	}
	type persistedWitness struct {
		record txnWitnessRecord
		raw    []byte
	}
	records := make(map[uint64]persistedWitness)
	var corrupt []corruptWitness
	var minRevision, maxRevision uint64
	for {
		if err := it.Next(ctx); err != nil {
			if err == io.EOF {
				break
			}
			_ = it.Close()
			return err
		}
		key := append([]byte(nil), it.Key()...)
		raw := append([]byte(nil), it.Val()...)
		logical := key[len(start)-len(txnWitnessPrefix):]
		if len(logical) != len(txnWitnessPrefix)+8 {
			corrupt = append(corrupt, corruptWitness{cause: fmt.Errorf("%w: malformed transaction witness key", ErrTxnWitnessCorrupt), key: key, raw: raw})
			continue
		}
		revision := binary.BigEndian.Uint64(logical[len(txnWitnessPrefix):])
		if revision == 0 || revision > math.MaxInt64 {
			corrupt = append(corrupt, corruptWitness{revision: revision,
				cause: fmt.Errorf("%w: invalid transaction witness revision", ErrTxnWitnessCorrupt), key: key, raw: raw})
			continue
		}
		record, decodeErr := decodeTxnWitness(raw)
		if decodeErr != nil {
			corrupt = append(corrupt, corruptWitness{revision: revision,
				cause: fmt.Errorf("%w: %v", ErrTxnWitnessCorrupt, decodeErr), key: key, raw: raw})
			continue
		}
		records[revision] = persistedWitness{record: record, raw: raw}
		if minRevision == 0 || revision < minRevision {
			minRevision = revision
		}
		if revision > maxRevision {
			maxRevision = revision
		}
	}
	_ = it.Close()
	for _, item := range corrupt {
		present, err := b.txnWitnessStillPresent(ctx, item.key, item.raw)
		if err != nil {
			return err
		}
		if !present {
			continue // compaction deleted the seal after the scan snapshot
		}
		if err := b.armPersistedWitnessCorrupt(ctx, item.revision, item.cause); err != nil {
			return err
		}
	}
	if len(records) == 0 {
		return nil
	}

	// Event-log keys sort by (revision,userKey), so one sequential scan validates
	// every retained seal. Avoid one TiKV transaction per revision during leader
	// recovery: a long compaction window can contain millions of transactions.
	type observedWitness struct {
		h     hash.Hash
		count uint32
	}
	observed := make(map[uint64]*observedWitness, len(records))
	for revision := range records {
		observed[revision] = &observedWitness{h: sha256.New()}
	}
	it, err = b.kv.Iter(ctx, b.ks.EventLogRangeStart(minRevision), b.ks.EventLogRangeEnd(maxRevision), 0, 0)
	if err != nil {
		return err
	}
	for {
		if err := it.Next(ctx); err != nil {
			if err == io.EOF {
				break
			}
			_ = it.Close()
			return err
		}
		revision, _, decodeErr := b.ks.DecodeEventLogKey(it.Key())
		if decodeErr != nil {
			_ = it.Close()
			return decodeErr
		}
		if state := observed[revision]; state != nil {
			writeWitnessDigestEntry(state.h, eventLogRawEntry{key: it.Key(), value: it.Val()})
			state.count++
		}
	}
	_ = it.Close()
	for revision, persisted := range records {
		state := observed[revision]
		if state.count == persisted.record.count && bytes.Equal(state.h.Sum(nil), persisted.record.digest[:]) {
			continue
		}
		witnessKey := b.ks.EncodeInternalKey(txnWitnessLogicalKey(revision))
		present, err := b.txnWitnessStillPresent(ctx, witnessKey, persisted.raw)
		if err != nil {
			return err
		}
		if !present {
			continue // compaction removed the seal before deleting these events
		}
		cause := fmt.Errorf("%w: persisted witness mismatch at revision %d: found %d events, expected %d",
			ErrTxnWitnessCorrupt, revision, state.count, persisted.record.count)
		if err := b.armPersistedWitnessCorrupt(ctx, revision, cause); err != nil {
			return err
		}
	}
	return nil
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
