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
	"context"
	"errors"
	"fmt"
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

// TxnWriteOp is one write in a transaction: a Put (Delete=false) or a
// single-key Delete. Value is the raw (un-enveloped) put value.
type TxnWriteOp struct {
	Delete bool
	Key    []byte
	Value  []byte
	// Lease is the lease ID bound to this put's new version (0 if none). It is
	// inlined into the version's value envelope so the lease is recorded
	// per-version (review #9). Ignored for deletes.
	Lease int64
}

// TxnGuard asserts that a compared key is still live at exactly Revision when the
// txn commits — an optimistic-concurrency guard that makes the compare and the
// writes serializable (#4 Tier 2). Guard keys MUST be disjoint from the write
// keys (a compared-and-written key is guarded by its own write CAS).
type TxnGuard struct {
	Key      []byte
	Revision uint64
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
// timeout) to emulate etcd's unconditional overwrite, while any other commit
// error publishes invalid events (so the event collector never stalls on the
// dealt revision) and is returned for the async retry queue to re-resolve.
//
// Ops MUST target distinct keys; the caller is responsible for that (multi-write
// to the same key needs intra-txn ordering that this batch does not model).
func (b *backend) TxnApply(ctx context.Context, ops []TxnWriteOp, guards []TxnGuard) (results []TxnWriteResult, revision uint64, err error) {
	deadline := time.Now().Add(unaryRpcTimeout)
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

// txnPrep holds the pre-read state and planned outcome for one op.
type txnPrep struct {
	op        TxnWriteOp
	rvBytes   []byte // exact revision-key value read (nil if key absent)
	curRev    uint64 // current mod revision (0 if absent)
	effective bool   // whether this op produces a batch write
	create    bool   // put: create/recreate rather than update
	prevValue []byte // enveloped previous value (update/delete)
	meta      EtcdMetadata
}

func (b *backend) tryTxnApply(ctx context.Context, ops []TxnWriteOp, guards []TxnGuard) (results []TxnWriteResult, newRevision uint64, retry bool, err error) {
	preps := make([]txnPrep, 0, len(ops))
	baseRevision := b.GetCurrentRevision()

	// Phase 1: read each key's current revision-key state (and, for
	// update/delete, its previous value + metadata).
	for _, op := range ops {
		p := txnPrep{op: op}
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
				return nil, 0, false, perr
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
				meta, _, ok := decodeValueWithMeta(val)
				if !ok {
					meta, verr = b.GetEtcdMetadata(ctx, op.Key, p.curRev)
					if verr != nil {
						return nil, 0, false, verr
					}
				}
				p.meta = meta
			}
		}
		preps = append(preps, p)
	}

	// No effective write (e.g. all deletes on absent keys): return no-op results
	// without consuming a revision.
	hasWrite := false
	for i := range preps {
		if preps[i].effective {
			hasWrite = true
			break
		}
	}
	if !hasWrite {
		cur := b.GetCurrentRevision()
		results = make([]TxnWriteResult, len(preps))
		for i := range preps {
			results[i] = TxnWriteResult{Key: preps[i].op.Key, Revision: cur}
		}
		return results, cur, false, nil
	}

	// Phase 2: allocate the single txn revision.
	newRevision, derr := b.deal(baseRevision)
	if derr != nil {
		// The revision was consumed from the TSO; publish invalid events so the
		// collector advances past it instead of stalling forever.
		b.notifyInvalidTxn(preps, newRevision, derr)
		return nil, newRevision, false, derr
	}
	if newRevision <= baseRevision {
		e := fmt.Errorf("cas failed, new revision is %d base revision is %d", newRevision, baseRevision)
		b.notifyInvalidTxn(preps, newRevision, e)
		return nil, newRevision, false, e
	}

	// Fence just before opening the batch: reject if leadership changed since the
	// txn was admitted, filling an invalid ring slot for every dealt revision so
	// the collector never stalls (FINDING #39).
	if cerr := b.fenceAdmit(ctx); cerr != nil {
		b.notifyInvalidTxn(preps, newRevision, cerr)
		return nil, newRevision, false, cerr
	}

	// Phase 3: build one batch. Compare guards go first (lowest batch index) so a
	// guard conflict is reported ahead of any write conflict — a changed compared
	// key means the txn's branch may have flipped and must be re-evaluated, which
	// takes precedence over merely re-applying a write.
	batch := b.kv.BeginBatchWrite()
	guardKeys := make(map[string]struct{}, len(guards))
	for _, g := range guards {
		gk := b.coder.EncodeRevisionKey(g.Key)
		guardKeys[string(gk)] = struct{}{}
		// no-op CAS: assert the key is still live at exactly g.Revision (an 8-byte
		// live revision value); any update/delete/compaction changes the bytes.
		revBytes := uint64ToBytes(g.Revision)
		batch.CAS(gk, revBytes, revBytes, 0)
	}
	newRevLive := uint64ToBytes(newRevision)
	newRevDeleted := append(uint64ToBytes(newRevision), 0)
	for i := range preps {
		p := &preps[i]
		if !p.effective {
			continue
		}
		revisionKey := b.coder.EncodeRevisionKey(p.op.Key)
		objectKey := b.coder.EncodeObjectKey(p.op.Key, newRevision)
		// Each write stages its event-log entry on the same batch (#45), with the
		// verb/prevRev Phase 5 publishes for it — the replay path treats a missing
		// entry at a committed revision as a failed-CAS hole and silently skips it,
		// so a leaked write here is a silently lost watch event (review #51).
		switch {
		case p.op.Delete:
			batch.CAS(revisionKey, newRevDeleted, p.rvBytes, 0)
			batch.Put(objectKey, tombStoneBytes, 0)
			appendEventLog(b.ks, batch, newRevision, p.op.Key, proto.Event_DELETE, p.curRev)
		case p.create:
			p.meta = EtcdMetadata{CreateRevision: newRevision, Version: 1, Lease: p.op.Lease}
			if p.rvBytes == nil {
				batch.PutIfNotExist(revisionKey, newRevLive, 0)
			} else {
				// recreate over a tombstone: CAS from its exact stored bytes
				batch.CAS(revisionKey, newRevLive, p.rvBytes, 0)
			}
			b.putTxnObject(batch, objectKey, p.op.Key, p.op.Value, p.meta, newRevision)
			appendEventLog(b.ks, batch, newRevision, p.op.Key, proto.Event_CREATE, 0)
		default: // update
			if p.meta.CreateRevision == 0 {
				p.meta.CreateRevision = p.curRev
			}
			if p.meta.Version == 0 {
				p.meta.Version = 1
			}
			p.meta.Version++
			// Rebind to this op's lease (review #9); do not inherit the prior
			// version's lease.
			p.meta.Lease = p.op.Lease
			batch.CAS(revisionKey, newRevLive, p.rvBytes, 0)
			b.putTxnObject(batch, objectKey, p.op.Key, p.op.Value, p.meta, newRevision)
			appendEventLog(b.ks, batch, newRevision, p.op.Key, proto.Event_PUT, p.curRev)
		}
	}

	// Phase 4: atomic commit.
	if cerr := batch.Commit(ctx); cerr != nil {
		b.notifyInvalidTxn(preps, newRevision, cerr)
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

	// Phase 5: build results and publish one event batch at newRevision.
	results = make([]TxnWriteResult, len(preps))
	events := make([]*common.WatchEvent, 0, len(preps))
	for i := range preps {
		p := &preps[i]
		res := TxnWriteResult{Key: p.op.Key, Revision: newRevision}
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
	b.waitCommittedRevision(ctx, newRevision) // apply-then-ack (#35)
	return results, newRevision, false, nil
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

// putTxnObject writes the object value for a put, inlining metadata in compat
// mode (approach A) or writing the separate etcdmeta keyspace otherwise —
// mirroring createBatchWithMetadata / update.
func (b *backend) putTxnObject(batch storage.BatchWrite, objectKey, key, value []byte, meta EtcdMetadata, revision uint64) {
	if b.config.EnableEtcdCompatibility {
		batch.Put(objectKey, encodeValueWithMeta(value, meta), 0)
		return
	}
	batch.Put(objectKey, value, 0)
	b.putEtcdMetadata(batch, key, revision, meta)
}

// notifyInvalidTxn fills the dealt revision's ring slot with invalid per-key
// events when a txn batch does not commit, so the event collector advances past
// the consumed revision and the async retry queue re-resolves each key — exactly
// like DeleteRange's commit-failure path.
func (b *backend) notifyInvalidTxn(preps []txnPrep, newRevision uint64, cause error) {
	invalid := make([]*common.WatchEvent, 0, len(preps))
	for i := range preps {
		p := &preps[i]
		if !p.effective {
			continue
		}
		verb := proto.Event_PUT
		if p.op.Delete {
			verb = proto.Event_DELETE
		}
		invalid = append(invalid, &common.WatchEvent{
			Revision:     newRevision,
			PrevRevision: p.curRev,
			Valid:        false,
			ResourceVerb: verb,
			Key:          p.op.Key,
			Value:        p.op.Value,
			Err:          cause,
		})
	}
	b.notifyBatch(invalid)
}
