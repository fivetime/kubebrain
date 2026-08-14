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

package backend

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"k8s.io/klog/v2"

	proto "github.com/kubewharf/kubebrain-client/api/v2rpc"

	"github.com/kubewharf/kubebrain/pkg/backend/coder"
	"github.com/kubewharf/kubebrain/pkg/backend/common"
	"github.com/kubewharf/kubebrain/pkg/storage"
)

// ErrTxnGuardConflict is returned by TxnApply when a compare guard's key changed
// since it was read, so the caller must re-evaluate the txn's compares (its
// chosen branch may have flipped) and retry.
var ErrTxnGuardConflict = errors.New("txn compare guard conflict")

// ErrTxnDuplicateKey rejects a backend transaction that would stage multiple
// mutations against the same keyspace/key. etcd validates this at its RPC
// boundary; enforcing it again here protects internal lease/revoke callers and
// direct Backend users from storage-dependent read-your-write CAS behavior.
var ErrTxnDuplicateKey = errors.New("txn contains duplicate write key")

// ErrTxnInvalidRequest identifies an impossible backend transaction shape.
// Public etcd RPCs reject these earlier; this boundary protects internal/direct
// callers without assigning them a durable revision.
var ErrTxnInvalidRequest = errors.New("invalid txn apply request")

// ErrTxnWitnessCorrupt means durable records contradict the atomic transaction
// shape. The resolver must not guess an outcome and must arm CORRUPT so public
// mutations fail closed until an operator repairs and disarms the member.
var ErrTxnWitnessCorrupt = errors.New("txn durable witness is corrupt")

// TxnWriteOp is one write in a transaction: a Put (Delete=false) or a
// single-key Delete. Value is the raw (un-enveloped) put value.
type TxnWriteOp struct {
	Delete bool
	// Internal persists service metadata in the tenant-scoped raw keyspace. It
	// participates in the same storage commit but not user MVCC or watch output.
	Internal bool
	Key      []byte
	Value    []byte
	// Lease is the lease ID bound to this put's new version (0 if none). It is
	// inlined into the version's value envelope so the lease is recorded
	// per-version (review #9). Ignored for deletes.
	Lease int64
	// PrevLeaseKnown says the caller captured the previous version's lease from
	// an authoritative attachment snapshot that remains stable through this
	// transaction. When an update/delete turns a legacy raw/v1 current row into
	// history, TxnApply uses this provenance to upgrade that old row in place to
	// v2/v3. The rewrite is part of the same storage batch, so it consumes no
	// extra MVCC revision and emits no extra watch event.
	PrevLeaseKnown bool
	PrevLease      int64
}

// TxnGuard asserts that a compared key is either still live at exactly Revision,
// or still absent when Absent is true. Absence includes a missing revision key
// and a tombstoned revision key. This closes the create-between-compare-and-
// commit window for Version/CreateRevision(key)==0 transactions.
type TxnGuard struct {
	Key      []byte
	Revision uint64
	Absent   bool
}

// TxnWriteResult is the outcome of one TxnWriteOp, all sharing the txn Revision.
type TxnWriteResult struct {
	Key          []byte
	Revision     uint64       // the single txn revision (0 if the txn made no write)
	Created      bool         // put created (or recreated) the key rather than updating it
	Deleted      bool         // delete removed a live key
	PrevRevision uint64       // previous mod revision (update/delete)
	PrevValue    []byte       // previous stored (enveloped) value (update/delete); nil otherwise
	Meta         EtcdMetadata // resulting create_revision/version for a put
}

// TxnApply applies a set of put/delete ops atomically at a single revision.
//
// All ops commit in one storage batch, so either every write lands at the same
// revision or none does (fixing the generic-txn path's per-op distinct revisions
// and partial application, #4 Tier 1). Each op is guarded by a CAS against the
// exact revision-key bytes read in the same pass, so a concurrent modification
// fails the whole batch; a definite CAS failure is retried (bounded by the RPC
// timeout) to emulate etcd's unconditional overwrite. An uncertain commit is
// resolved as one transaction from its durable event-log marker; it is never
// split into per-key retry writes.
//
// Ops must target distinct keys within each user/internal keyspace. A user key
// and an internal key may share raw bytes because their encoded keyspaces are
// disjoint. Duplicate writes are rejected before any read or revision allocation.
func (b *backend) TxnApply(ctx context.Context, ops []TxnWriteOp, guards []TxnGuard) (results []TxnWriteResult, revision uint64, err error) {
	if err := validateTxnApplyRequest(ops, guards); err != nil {
		return nil, 0, err
	}
	if err := b.waitPendingRevision(ctx); err != nil {
		return nil, 0, err
	}
	unlock := b.lockLogicalWrite(ctx)
	defer unlock()
	b.revisionWriteMu.Lock()
	defer b.revisionWriteMu.Unlock()
	// Another writer may have installed an uncertain revision after the first
	// check while we waited for either lock. Recheck while serialized. Keeping
	// the shared logical lock here is safe: the resolver only needs that same
	// shared side if it must arm CORRUPT. Acquiring the logical barrier before
	// revisionWriteMu is also required so a range-transaction owner can write
	// while an external writer is blocked at the barrier.
	if err := b.waitPendingRevision(ctx); err != nil {
		return nil, 0, err
	}
	deadline := time.Now().Add(unaryRpcTimeout)
	if callerDeadline, ok := ctx.Deadline(); ok {
		// The etcd layer supplies its request budget (10s by default), already
		// clamped by any shorter client deadline. Do not replace that valid
		// budget with the backend's 1s fallback: a transient TiKV conflict or
		// leader transfer can legitimately outlive one second. Direct/internal
		// callers without a deadline retain the bounded fallback above.
		deadline = callerDeadline
	}
	for {
		if cerr := ctx.Err(); cerr != nil {
			return nil, 0, cerr
		}
		if time.Now().After(deadline) {
			return nil, 0, storage.ErrUnavailable
		}
		results, revision, retry, err := b.tryTxnApply(ctx, ops, guards)
		if retry {
			continue
		}
		return results, revision, err
	}
}

func validateTxnApplyRequest(ops []TxnWriteOp, guards []TxnGuard) error {
	seenUser := make(map[string]struct{}, len(ops))
	seenInternal := make(map[string]struct{}, len(ops))
	for i := range ops {
		if len(ops[i].Key) == 0 {
			return fmt.Errorf("%w: write %d has empty key", ErrTxnInvalidRequest, i)
		}
		seen := seenUser
		if ops[i].Internal {
			if bytes.HasPrefix(ops[i].Key, txnWitnessPrefix) {
				return fmt.Errorf("%w: write %d targets reserved transaction witness metadata", ErrTxnInvalidRequest, i)
			}
			seen = seenInternal
		}
		key := string(ops[i].Key)
		if _, duplicate := seen[key]; duplicate {
			return fmt.Errorf("%w: internal=%t key=%q", ErrTxnDuplicateKey, ops[i].Internal, ops[i].Key)
		}
		seen[key] = struct{}{}
	}
	for i := range guards {
		guard := guards[i]
		if len(guard.Key) == 0 {
			return fmt.Errorf("%w: guard %d has empty key", ErrTxnInvalidRequest, i)
		}
		if guard.Absent && guard.Revision != 0 {
			return fmt.Errorf("%w: absent guard %d has revision %d", ErrTxnInvalidRequest, i, guard.Revision)
		}
		if !guard.Absent && guard.Revision == 0 {
			return fmt.Errorf("%w: present guard %d has zero revision", ErrTxnInvalidRequest, i)
		}
	}
	return nil
}

// txnPrep holds the pre-read state and planned outcome for one op.
type txnPrep struct {
	op          TxnWriteOp
	rvBytes     []byte // exact revision-key value read (nil if key absent)
	curRev      uint64 // current mod revision (0 if absent)
	effective   bool   // whether this op produces a batch write
	create      bool   // put: create/recreate rather than update
	prevValue   []byte // enveloped previous value (update/delete)
	prevMeta    EtcdMetadata
	migratePrev bool
	meta        EtcdMetadata
}

type txnGuardPrep struct {
	guard   TxnGuard
	key     []byte
	rvBytes []byte
	missing bool
}

func atomicExpect(ctx context.Context, txn storage.AtomicBatch, key, expected []byte, missing bool) error {
	current, err := txn.Get(ctx, key)
	if missing {
		if errors.Is(err, storage.ErrKeyNotFound) {
			return nil
		}
		if err != nil {
			return err
		}
		return storage.ErrCASFailed
	}
	if err != nil {
		if errors.Is(err, storage.ErrKeyNotFound) {
			return storage.ErrCASFailed
		}
		return err
	}
	if !bytes.Equal(current, expected) {
		return storage.ErrCASFailed
	}
	return nil
}

// stageTxnAtomic expresses every TxnApply guard and mutation using reads and
// writes inside one storage transaction. Value guards that must conflict on
// TiKV also stage a mutation: TiKV optimistic prewrite does not validate an
// arbitrary AtomicBatch.Get read set. Object/event keys may still be derived
// from a revision allocated by that transaction.
func (b *backend) stageTxnAtomic(ctx context.Context, txn storage.AtomicBatch, preps []txnPrep, guardPreps []txnGuardPrep, newRevision uint64, quotaUsageRaw []byte, nextQuotaUsage int64, corruptGuard corruptAlarmCommitGuard) error {
	guardKey := b.ks.EncodeInternalKey(corruptGuard.key)
	if err := atomicExpect(ctx, txn, guardKey, corruptGuard.expected, !corruptGuard.exists); err != nil {
		return err
	}
	// TiKV optimistic transactions validate mutation keys, not an arbitrary read
	// set. Write the same shard value (or create+delete the legacy control key)
	// so concurrent Arm/Disarm necessarily conflicts at prewrite.
	if corruptGuard.exists {
		if err := txn.Put(guardKey, corruptGuard.expected, 0); err != nil {
			return err
		}
	} else {
		if err := txn.Put(guardKey, []byte{0}, 0); err != nil {
			return err
		}
		if err := txn.Del(guardKey); err != nil {
			return err
		}
	}
	if b.config.QuotaBackendBytes > 0 {
		key := b.ks.EncodeInternalKey(quotaUsageKey)
		if err := atomicExpect(ctx, txn, key, quotaUsageRaw, quotaUsageRaw == nil); err != nil {
			return err
		}
		if err := txn.Put(key, encodeQuotaUsage(nextQuotaUsage), 0); err != nil {
			return err
		}
	}
	for _, gp := range guardPreps {
		if err := atomicExpect(ctx, txn, gp.key, gp.rvBytes, gp.missing); err != nil {
			return err
		}
	}

	eventTotal := uint32(txnEffectiveUserWriteCount(preps))
	var eventSubRevision uint32
	for i := range preps {
		p := &preps[i]
		if !p.effective {
			continue
		}
		if p.op.Internal {
			key := b.ks.EncodeInternalKey(p.op.Key)
			if err := atomicExpect(ctx, txn, key, p.rvBytes, p.rvBytes == nil); err != nil {
				return err
			}
			if p.op.Delete {
				if err := txn.Del(key); err != nil {
					return err
				}
			} else if err := txn.Put(key, p.op.Value, 0); err != nil {
				return err
			}
			continue
		}

		if p.migratePrev {
			previousObjectKey := b.coder.EncodeObjectKey(p.op.Key, p.curRev)
			if err := atomicExpect(ctx, txn, previousObjectKey, p.prevValue, false); err != nil {
				return err
			}
			_, raw, _, err := DecodeInlineValueChecked(p.prevValue)
			if err != nil {
				return err
			}
			if err := txn.Put(previousObjectKey, encodeValueWithMeta(raw, p.prevMeta), 0); err != nil {
				return err
			}
		}

		var encoded encodedPutMutation
		switch {
		case p.op.Delete:
			deleted := b.encodeDeleteMutationAt(p.op.Key, p.curRev, newRevision, eventSubRevision, eventTotal)
			if err := atomicExpect(ctx, txn, deleted.revisionKey, p.rvBytes, false); err != nil {
				return err
			}
			if err := txn.Put(deleted.revisionKey, deleted.newRevisionValue, 0); err != nil {
				return err
			}
			if err := txn.Put(deleted.objectKey, deleted.objectValue, 0); err != nil {
				return err
			}
			if err := txn.Put(deleted.eventKey, deleted.eventValue, 0); err != nil {
				return err
			}
			eventSubRevision++
			continue
		case p.create:
			p.meta = EtcdMetadata{CreateRevision: newRevision, Version: 1, Lease: p.op.Lease}
			encoded = b.encodeCreateMutation(p.op.Key, p.op.Value, p.meta, newRevision, eventSubRevision, eventTotal)
		default:
			if p.meta.CreateRevision == 0 {
				p.meta.CreateRevision = p.curRev
			}
			if p.meta.Version == 0 {
				p.meta.Version = 1
			}
			p.meta.Version++
			p.meta.Lease = p.op.Lease
			encoded = b.encodePutMutation(p.op.Key, p.op.Value, p.meta, p.curRev, newRevision, proto.Event_PUT, eventSubRevision, eventTotal)
		}
		if err := atomicExpect(ctx, txn, encoded.revisionKey, p.rvBytes, p.rvBytes == nil); err != nil {
			return err
		}
		if err := txn.Put(encoded.revisionKey, encoded.newRevisionValue, 0); err != nil {
			return err
		}
		for _, object := range encoded.objectMutations {
			if err := txn.Put(object.key, object.value, 0); err != nil {
				return err
			}
		}
		if err := txn.Put(encoded.eventKey, encoded.eventValue, 0); err != nil {
			return err
		}
		eventSubRevision++
	}
	return b.stageTxnWitness(txn, preps, newRevision)
}

func (b *backend) tryTxnApply(ctx context.Context, ops []TxnWriteOp, guards []TxnGuard) (results []TxnWriteResult, newRevision uint64, retry bool, err error) {
	preps := make([]txnPrep, 0, len(ops))
	prepByKey := make(map[string]*txnPrep, len(ops))
	baseRevision := b.GetCurrentRevision()
	members, corruptGenerationRaw, corruptGenerationExists, alarmErr := b.readStableCorruptAlarmState(ctx)
	if alarmErr != nil {
		return nil, baseRevision, false, alarmErr
	}
	if len(members) != 0 {
		return nil, baseRevision, false, ErrCorruptAlarmActive
	}

	// Phase 1: read each key's current revision-key state (and, for
	// update/delete, its previous value + metadata).
	for _, op := range ops {
		p := txnPrep{op: op}
		if op.Internal {
			rawKey := b.ks.EncodeInternalKey(op.Key)
			rv, gerr := b.kv.Get(ctx, rawKey)
			switch {
			case errors.Is(gerr, storage.ErrKeyNotFound):
				p.effective = !op.Delete
			case gerr != nil:
				return nil, 0, false, gerr
			default:
				p.rvBytes = rv
				p.effective = true
				p.prevValue = rv
			}
			preps = append(preps, p)
			continue
		}
		revisionKey := b.coder.EncodeRevisionKey(op.Key)
		rv, gerr := b.kv.Get(ctx, revisionKey)
		absent, tombstone := false, false
		switch {
		case errors.Is(gerr, storage.ErrKeyNotFound):
			absent = true
		case gerr != nil:
			return nil, 0, false, gerr
		default:
			cr, isTomb, perr := coder.ParseRevision(rv)
			if perr != nil {
				return nil, 0, false, invalidMVCCMetadataError(perr, "decode revision index for key %q", op.Key)
			}
			p.rvBytes = rv
			p.curRev = cr
			tombstone = isTomb
			baseRevision = maxUint64(baseRevision, cr)
		}

		if op.Delete {
			// deleting an absent or already-tombstoned key is a no-op
			p.effective = !absent && !tombstone
			if p.effective {
				val, _, verr := b.getInternalVal(ctx, op.Key, p.curRev)
				if verr != nil {
					return nil, 0, false, verr
				}
				p.prevValue = val
				if b.config.EnableEtcdCompatibility && op.PrevLeaseKnown && !InlineValueLeaseKnown(val) {
					meta, _, ok, decodeErr := DecodeInlineValueChecked(val)
					if decodeErr != nil {
						return nil, 0, false, decodeErr
					}
					if !ok {
						meta, verr = b.GetEtcdMetadata(ctx, op.Key, p.curRev)
						if verr != nil {
							return nil, 0, false, verr
						}
					} else if validationErr := ValidateEtcdMetadataAtRevision(meta, p.curRev, "previous inline value metadata"); validationErr != nil {
						return nil, 0, false, validationErr
					}
					meta.Lease = op.PrevLease
					p.prevMeta, p.migratePrev = meta, true
				}
			}
		} else {
			p.effective = true
			p.create = absent || tombstone
			if !p.create {
				val, _, verr := b.getInternalVal(ctx, op.Key, p.curRev)
				if verr != nil {
					return nil, 0, false, verr
				}
				p.prevValue = val
				meta, _, ok, decodeErr := DecodeInlineValueChecked(val)
				if decodeErr != nil {
					return nil, 0, false, decodeErr
				}
				if !ok {
					meta, verr = b.GetEtcdMetadata(ctx, op.Key, p.curRev)
					if verr != nil {
						return nil, 0, false, verr
					}
				} else if validationErr := ValidateEtcdMetadataAtRevision(meta, p.curRev, "previous inline value metadata"); validationErr != nil {
					return nil, 0, false, validationErr
				}
				if b.config.EnableEtcdCompatibility && op.PrevLeaseKnown && !InlineValueLeaseKnown(val) {
					meta.Lease = op.PrevLease
					p.prevMeta, p.migratePrev = meta, true
				}
				p.meta = meta
			}
		}
		preps = append(preps, p)
	}
	for i := range preps {
		if preps[i].op.Internal {
			continue
		}
		prepByKey[string(preps[i].op.Key)] = &preps[i]
	}

	// Validate guards that overlap write keys against the write pre-read. The
	// write's own CAS then protects that exact state through commit, avoiding a
	// second operation on the same revision key in the batch.
	for _, g := range guards {
		if p := prepByKey[string(g.Key)]; p != nil {
			if g.Absent {
				if !p.create {
					return nil, 0, false, ErrTxnGuardConflict
				}
			} else if p.create || p.curRev != g.Revision {
				return nil, 0, false, ErrTxnGuardConflict
			}
		}
	}
	guardPreps := make([]txnGuardPrep, 0, len(guards))
	for _, g := range guards {
		if prepByKey[string(g.Key)] != nil {
			continue
		}
		gp := txnGuardPrep{guard: g, key: b.coder.EncodeRevisionKey(g.Key)}
		rv, gerr := b.kv.Get(ctx, gp.key)
		if errors.Is(gerr, storage.ErrKeyNotFound) {
			gp.missing = true
			if !g.Absent {
				return nil, 0, false, ErrTxnGuardConflict
			}
		} else if gerr != nil {
			return nil, 0, false, gerr
		} else {
			curRev, tombstone, perr := coder.ParseRevision(rv)
			if perr != nil {
				return nil, 0, false, invalidMVCCMetadataError(perr, "decode transaction guard revision index for key %q", g.Key)
			}
			if (g.Absent && !tombstone) || (!g.Absent && (tombstone || curRev != g.Revision)) {
				return nil, 0, false, ErrTxnGuardConflict
			}
			gp.rvBytes = rv
		}
		guardPreps = append(guardPreps, gp)
	}

	// Internal-only metadata mutations still need a physical commit, but must
	// not consume a user MVCC revision. This is important for operations such as
	// revoking an empty lease, whose only write is deleting lease metadata.
	hasUserWrite := false
	hasInternalWrite := false
	for i := range preps {
		if !preps[i].effective {
			continue
		}
		if preps[i].op.Internal {
			hasInternalWrite = true
		} else {
			hasUserWrite = true
		}
	}
	if !hasUserWrite && hasInternalWrite {
		corruptGuard, alarmErr := b.corruptAlarmCommitGuardFor(ctx, corruptGenerationRaw, corruptGenerationExists)
		if alarmErr != nil {
			return nil, baseRevision, false, alarmErr
		}
		if cerr := b.fenceAdmit(ctx); cerr != nil {
			return nil, baseRevision, false, cerr
		}
		batch := b.kv.BeginBatchWrite()
		stageCorruptAlarmCommitGuard(batch, b.ks.EncodeInternalKey(corruptGuard.key), corruptGuard)
		guardKeys := make(map[string]struct{}, len(guards))
		for _, gp := range guardPreps {
			guardKeys[string(gp.key)] = struct{}{}
			if gp.missing {
				batch.PutIfNotExist(gp.key, []byte{0}, 0)
				batch.Del(gp.key)
			} else {
				batch.CAS(gp.key, gp.rvBytes, gp.rvBytes, 0)
			}
		}
		for i := range preps {
			p := &preps[i]
			if !p.effective {
				continue
			}
			key := b.ks.EncodeInternalKey(p.op.Key)
			switch {
			case p.op.Delete:
				batch.CAS(key, []byte{0}, p.rvBytes, 0)
				batch.Del(key)
			case p.rvBytes == nil:
				batch.PutIfNotExist(key, p.op.Value, 0)
			default:
				batch.CAS(key, p.op.Value, p.rvBytes, 0)
			}
		}
		if cerr := b.commitUserBatch(ctx, batch); cerr != nil {
			if errors.Is(cerr, storage.ErrCASFailed) {
				if b.txnConflictIsGuard(cerr, guardKeys, len(guards) > 0) {
					return nil, baseRevision, false, ErrTxnGuardConflict
				}
				return nil, baseRevision, true, nil
			}
			return nil, baseRevision, false, cerr
		}
		results = make([]TxnWriteResult, len(preps))
		for i := range preps {
			results[i] = TxnWriteResult{Key: preps[i].op.Key, Revision: baseRevision}
		}
		return results, baseRevision, false, nil
	}

	// No effective write (e.g. all deletes on absent keys): return no-op results
	// without consuming a revision.
	if !hasUserWrite {
		cur := b.GetCurrentRevision()
		results = make([]TxnWriteResult, len(preps))
		for i := range preps {
			results[i] = TxnWriteResult{Key: preps[i].op.Key, Revision: cur}
		}
		return results, cur, false, nil
	}
	corruptGuard, alarmErr := b.corruptAlarmCommitGuardFor(ctx, corruptGenerationRaw, corruptGenerationExists)
	if alarmErr != nil {
		return nil, baseRevision, false, alarmErr
	}

	var (
		quotaUsageRaw  []byte
		nextQuotaUsage int64
	)
	hasPut := false
	for i := range preps {
		if preps[i].effective && !preps[i].op.Internal && !preps[i].op.Delete {
			hasPut = true
			break
		}
	}
	if hasPut {
		_, alarmErr := b.kv.Get(ctx, b.ks.EncodeInternalKey(quotaAlarmKey))
		switch {
		case alarmErr == nil:
			return nil, baseRevision, false, ErrNoSpace
		case errors.Is(alarmErr, storage.ErrKeyNotFound):
		default:
			return nil, baseRevision, false, alarmErr
		}
	}
	if b.config.QuotaBackendBytes > 0 {
		tracking, trackingErr := b.kv.Get(ctx, b.ks.EncodeInternalKey(quotaTrackingKey))
		switch {
		case errors.Is(trackingErr, storage.ErrKeyNotFound):
			return nil, baseRevision, false, ErrQuotaUninitialized
		case trackingErr != nil:
			return nil, baseRevision, false, trackingErr
		case !bytes.Equal(tracking, quotaTrackingClean):
			return nil, baseRevision, false, ErrQuotaUninitialized
		}
		raw, usageErr := b.kv.Get(ctx, b.ks.EncodeInternalKey(quotaUsageKey))
		currentUsage := int64(0)
		switch {
		case errors.Is(usageErr, storage.ErrKeyNotFound):
			return nil, baseRevision, false, ErrQuotaUninitialized
		case usageErr != nil:
			return nil, baseRevision, false, usageErr
		default:
			quotaUsageRaw = raw
			currentUsage, usageErr = decodeQuotaUsage(raw)
			if usageErr != nil {
				return nil, baseRevision, false, usageErr
			}
		}
		delta := int64(0)
		for i := range preps {
			p := &preps[i]
			if !p.effective || p.op.Internal {
				continue
			}
			if p.op.Delete {
				previousSize, sizeErr := logicalStoredValueSize(p.prevValue)
				if sizeErr != nil {
					return nil, baseRevision, false, sizeErr
				}
				delta -= int64(len(p.op.Key)) + previousSize
				continue
			}
			if p.create {
				delta += int64(len(p.op.Key))
			} else {
				previousSize, sizeErr := logicalStoredValueSize(p.prevValue)
				if sizeErr != nil {
					return nil, baseRevision, false, sizeErr
				}
				delta -= previousSize
			}
			delta += int64(len(p.op.Value))
		}
		nextQuotaUsage = currentUsage + delta
		if nextQuotaUsage < 0 {
			return nil, baseRevision, false, fmt.Errorf(
				"quota usage underflow: current=%d delta=%d", currentUsage, delta,
			)
		}
		if hasPut && (currentUsage >= b.config.QuotaBackendBytes ||
			nextQuotaUsage > b.config.QuotaBackendBytes) {
			if alarmErr := b.activateNoSpace(ctx); alarmErr != nil {
				return nil, baseRevision, false, alarmErr
			}
			return nil, baseRevision, false, ErrNoSpace
		}
	}

	// Phase 2: fence and fold the observed committed floor into the durable
	// counter before allocating inside the user transaction.
	if cerr := b.fenceAdmit(ctx); cerr != nil {
		return nil, baseRevision, false, cerr
	}
	guardKeys := make(map[string]struct{}, len(guards))
	for _, gp := range guardPreps {
		guardKeys[string(gp.key)] = struct{}{}
	}
	for _, g := range guards {
		if prepByKey[string(g.Key)] != nil {
			guardKeys[string(b.coder.EncodeRevisionKey(g.Key))] = struct{}{}
		}
	}

	// Phase 3: allocate revision and stage every guard/mutation/event/counter in
	// one storage transaction. A definite conflict rolls back the allocation, so
	// it creates no collector hole and must not publish an invalid event.
	batch := b.kv.BeginBatchWrite()
	allocated := b.stageNextDurableRevisionAfter(batch, baseRevision, func(callbackCtx context.Context, txn storage.AtomicBatch, revision uint64) error {
		return b.stageTxnAtomic(callbackCtx, txn, preps, guardPreps, revision, quotaUsageRaw, nextQuotaUsage, corruptGuard)
	})
	cerr := b.commitUserBatch(ctx, batch)
	newRevision = *allocated
	if cerr != nil {
		if errors.Is(cerr, storage.ErrUncertainResult) && txnHasEffectiveUserWrite(preps) {
			// Some storage failures happen before the Atomic callback starts. No
			// candidate revision was derived or written in that case, so there is
			// no marker to resolve and no collector slot to release.
			if newRevision == 0 {
				return nil, 0, false, cerr
			}
			// A multi-key txn must never enter the single-key uncertain retry
			// queue: if the original batch committed, that queue would rewrite
			// each key at a separate revision and expose torn transaction state.
			// Event-log records were staged in the same atomic batch, so resolve
			// the whole outcome from those durable markers and publish once.
			finishPending := b.beginPendingRevision()
			b.uncertainTxnPins.pin(newRevision)
			started := b.startWorker(func(ctx context.Context) {
				defer finishPending()
				defer b.uncertainTxnPins.unpin(newRevision)
				b.resolveUncertainTxn(ctx, preps, newRevision)
			})
			if !started {
				b.uncertainTxnPins.unpin(newRevision)
				finishPending()
			}
			klog.ErrorS(cerr, "txn apply commit result uncertain; resolving as one transaction",
				"revision", newRevision, "ops", len(ops))
			return nil, newRevision, false, cerr
		}
		if errors.Is(cerr, storage.ErrCASFailed) {
			// Distinguish a compare-guard conflict (the caller must re-evaluate the
			// txn's branch) from a write-key conflict (just re-apply the writes).
			if b.txnConflictIsGuard(cerr, guardKeys, len(guards) > 0) {
				return nil, newRevision, false, ErrTxnGuardConflict
			}
			// A write key changed since the pre-read; retry against the new state
			// (bounded by the caller loop's deadline) to emulate unconditional puts.
			return nil, newRevision, true, nil
		}
		klog.ErrorS(cerr, "txn apply commit failed", "revision", newRevision, "ops", len(ops))
		return nil, newRevision, false, cerr
	}
	b.tso.AdvanceDealFloor(newRevision)

	// Phase 5: build results and publish one event batch at newRevision.
	results = make([]TxnWriteResult, len(preps))
	events := make([]*common.WatchEvent, 0, len(preps))
	for i := range preps {
		p := &preps[i]
		res := TxnWriteResult{Key: p.op.Key, Revision: newRevision}
		if p.op.Internal {
			results[i] = res
			continue
		}
		if !p.effective {
			res.Revision = newRevision
			results[i] = res
			continue
		}
		if p.op.Delete {
			res.Deleted = true
			res.PrevRevision = p.curRev
			res.PrevValue = p.prevValue
			events = append(events, &common.WatchEvent{
				Revision:     newRevision,
				PrevRevision: p.curRev,
				Valid:        true,
				ResourceVerb: proto.Event_DELETE,
				Key:          p.op.Key,
				Value:        p.prevValue,
			})
		} else {
			res.Meta = p.meta
			verb := proto.Event_PUT
			if p.create {
				res.Created = true
				verb = proto.Event_CREATE
			} else {
				res.PrevRevision = p.curRev
				res.PrevValue = p.prevValue
			}
			events = append(events, &common.WatchEvent{
				Revision:     newRevision,
				PrevRevision: p.curRev,
				Valid:        true,
				ResourceVerb: verb,
				Key:          p.op.Key,
				Value:        b.eventValue(p.op.Value, p.meta, nil),
			})
		}
		results[i] = res
	}
	b.notifyBatch(events)
	if b.config.QuotaBackendBytes > 0 {
		b.metricCli.EmitGauge("quota.logical_usage_bytes", nextQuotaUsage)
	}
	b.waitCommittedRevision(ctx, newRevision) // apply-then-ack (#35)
	return results, newRevision, false, nil
}

func txnHasEffectiveUserWrite(preps []txnPrep) bool {
	return txnEffectiveUserWriteCount(preps) != 0
}

func txnEffectiveUserWriteCount(preps []txnPrep) int {
	count := 0
	for i := range preps {
		if preps[i].effective && !preps[i].op.Internal {
			count++
		}
	}
	return count
}

func (b *backend) resolveUncertainTxn(workerCtx context.Context, preps []txnPrep, revision uint64) {
	retryDelay := 100 * time.Millisecond
	corruptArmed := false
	for {
		ctx, cancel := context.WithTimeout(workerCtx, unaryRpcTimeout)
		committed, err := b.txnCommitRecorded(ctx, preps, revision)
		cancel()
		if err != nil {
			if workerCtx.Err() != nil {
				return
			}
			if errors.Is(err, ErrTxnWitnessCorrupt) && !corruptArmed {
				alarmCtx, alarmCancel := context.WithTimeout(workerCtx, unaryRpcTimeout)
				alarmErr := b.ArmCorrupt(alarmCtx, b.localAlarmMemberID())
				alarmCancel()
				if alarmErr == nil {
					corruptArmed = true
					b.metricCli.EmitCounter("txn.uncertain.witness_corrupt", 1)
					klog.ErrorS(err, "armed CORRUPT alarm for inconsistent uncertain transaction witness",
						"revision", revision, "memberID", b.localAlarmMemberID())
				} else {
					b.metricCli.EmitCounter("txn.uncertain.witness_corrupt_alarm_failed", 1)
					klog.ErrorS(alarmErr, "failed to arm CORRUPT alarm for inconsistent uncertain transaction witness",
						"revision", revision)
				}
			}
			b.metricCli.EmitCounter("txn.uncertain.resolve.retry", 1)
			klog.ErrorS(err, "failed to resolve uncertain txn; retrying whole transaction",
				"revision", revision, "retryAfter", retryDelay)
			timer := time.NewTimer(retryDelay)
			select {
			case <-workerCtx.Done():
				if !timer.Stop() {
					<-timer.C
				}
				return
			case <-timer.C:
			}
			if retryDelay < time.Second {
				retryDelay *= 2
				if retryDelay > time.Second {
					retryDelay = time.Second
				}
			}
			continue
		}
		if committed {
			klog.InfoS("resolved uncertain txn as committed", "revision", revision)
			b.tso.AdvanceDealFloor(revision)
			b.notifyBatch(b.txnWatchEvents(preps, revision))
			b.metricCli.EmitCounter("txn.uncertain.resolve.committed", 1)
			return
		}
		// The allocator counter and every user/event mutation were in the same
		// transaction, so an all-absent marker result proves this candidate never
		// existed. Do not advance the local floor or publish an invalid ring slot;
		// the next writer may safely reuse the exact revision.
		klog.InfoS("resolved uncertain txn as not committed", "revision", revision)
		b.metricCli.EmitCounter("txn.uncertain.resolve.not_committed", 1)
		return
	}
}

// txnCommitRecorded checks the exact event-log records written atomically with
// every effective user mutation. All exact records means the transaction
// committed; none means it did not. A missing subset or mismatched payload
// contradicts TiKV atomicity/durable integrity and is retried rather than guessed.
func (b *backend) txnCommitRecorded(ctx context.Context, preps []txnPrep, revision uint64) (bool, error) {
	expected, found := 0, 0
	total := uint32(txnEffectiveUserWriteCount(preps))
	var subRevision uint32
	for i := range preps {
		p := &preps[i]
		if !p.effective || p.op.Internal {
			continue
		}
		expected++
		verb, previousRevision := proto.Event_PUT, p.curRev
		if p.op.Delete {
			verb = proto.Event_DELETE
		} else if p.create {
			verb, previousRevision = proto.Event_CREATE, 0
		}
		key, expectedValue := encodeEventLogEntry(b.ks, revision, p.op.Key, verb, previousRevision, subRevision, total)
		subRevision++
		value, err := b.kv.Get(ctx, key)
		switch {
		case err == nil && bytes.Equal(value, expectedValue):
			found++
		case err == nil:
			return false, fmt.Errorf("%w: event marker mismatch for key %q at revision %d", ErrTxnWitnessCorrupt, p.op.Key, revision)
		case errors.Is(err, storage.ErrKeyNotFound):
		default:
			return false, err
		}
	}
	switch {
	case expected == 0:
		return false, errors.New("uncertain txn has no user event markers")
	case found == expected:
		return true, nil
	case found == 0:
		return false, nil
	default:
		return false, fmt.Errorf("%w: mixed event markers: found %d of %d at revision %d",
			ErrTxnWitnessCorrupt, found, expected, revision)
	}
}

func (b *backend) txnWatchEvents(preps []txnPrep, revision uint64) []*common.WatchEvent {
	events := make([]*common.WatchEvent, 0, len(preps))
	for i := range preps {
		p := &preps[i]
		if !p.effective || p.op.Internal {
			continue
		}
		verb := proto.Event_PUT
		value := b.eventValue(p.op.Value, p.meta, nil)
		if p.op.Delete {
			verb = proto.Event_DELETE
			value = p.prevValue
		} else if p.create {
			verb = proto.Event_CREATE
		}
		events = append(events, &common.WatchEvent{
			Revision:     revision,
			PrevRevision: p.curRev,
			Valid:        true,
			ResourceVerb: verb,
			Key:          p.op.Key,
			Value:        value,
		})
	}
	return events
}

// txnConflictIsGuard reports whether a commit CAS failure was on a compare-guard
// key. When the storage error identifies the conflicting key it is matched
// exactly; when it does not (some engines return an opaque CAS error), a txn that
// carried guards is conservatively treated as a guard conflict so the caller
// re-evaluates rather than spinning on an unconditional-overwrite retry.
func (b *backend) txnConflictIsGuard(cerr error, guardKeys map[string]struct{}, hadGuards bool) bool {
	if len(guardKeys) == 0 {
		return false
	}
	var conflict *storage.Conflict
	if errors.As(cerr, &conflict) {
		_, ok := guardKeys[string(conflict.Key)]
		return ok
	}
	return hadGuards
}

type encodedMutation struct {
	key   []byte
	value []byte
}

func (b *backend) encodeTxnObjectMutations(objectKey, key, value []byte, meta EtcdMetadata, revision uint64) []encodedMutation {
	if b.config.EnableEtcdCompatibility {
		return []encodedMutation{{key: objectKey, value: encodeValueWithMeta(value, meta)}}
	}
	return []encodedMutation{
		{key: objectKey, value: value},
		{key: b.coder.EncodeObjectKey(b.etcdMetadataUserKey(key), revision), value: encodeEtcdMetadata(meta)},
	}
}

func (b *backend) waitPendingRevision(ctx context.Context) error {
	b.revisionPendingMu.Lock()
	pending := b.revisionPending
	b.revisionPendingMu.Unlock()
	if pending == nil {
		return nil
	}
	select {
	case <-pending:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (b *backend) beginPendingRevision() func() {
	b.revisionPendingMu.Lock()
	if b.revisionPending != nil {
		b.revisionPendingMu.Unlock()
		panic("multiple pending revision reservations")
	}
	pending := make(chan struct{})
	b.revisionPending = pending
	b.revisionPendingMu.Unlock()
	var once sync.Once
	return func() {
		once.Do(func() {
			b.revisionPendingMu.Lock()
			if b.revisionPending == pending {
				b.revisionPending = nil
				close(pending)
			}
			b.revisionPendingMu.Unlock()
		})
	}
}
