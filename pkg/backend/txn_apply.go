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

var errTxnResolvedNotCommitted = errors.New("uncertain txn resolved as not committed")

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
// timeout) to emulate etcd's unconditional overwrite, while any other commit
// error publishes invalid events (so the event collector never stalls on the
// dealt revision) and is returned for the async retry queue to re-resolve.
//
// Ops MUST target distinct keys; the caller is responsible for that (multi-write
// to the same key needs intra-txn ordering that this batch does not model).
func (b *backend) TxnApply(ctx context.Context, ops []TxnWriteOp, guards []TxnGuard) (results []TxnWriteResult, revision uint64, err error) {
	unlock := b.lockLogicalWrite(ctx)
	defer unlock()
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

type txnGuardPrep struct {
	guard   TxnGuard
	key     []byte
	rvBytes []byte
	missing bool
}

func (b *backend) tryTxnApply(ctx context.Context, ops []TxnWriteOp, guards []TxnGuard) (results []TxnWriteResult, newRevision uint64, retry bool, err error) {
	preps := make([]txnPrep, 0, len(ops))
	prepByKey := make(map[string]*txnPrep, len(ops))
	baseRevision := b.GetCurrentRevision()

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
				return nil, 0, false, perr
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
		if cerr := b.fenceAdmit(ctx); cerr != nil {
			return nil, baseRevision, false, cerr
		}
		batch := b.kv.BeginBatchWrite()
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

	var (
		quotaUsageRaw  []byte
		nextQuotaUsage int64
	)
	if b.config.QuotaBackendBytes > 0 {
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
		delta := int64(0)
		for i := range preps {
			p := &preps[i]
			if !p.effective || p.op.Internal {
				continue
			}
			if p.op.Delete {
				delta -= int64(len(p.op.Key)) + logicalStoredValueSize(p.prevValue)
				continue
			}
			if p.create {
				delta += int64(len(p.op.Key))
			} else {
				delta -= logicalStoredValueSize(p.prevValue)
			}
			delta += int64(len(p.op.Value))
		}
		nextQuotaUsage = currentUsage + delta
		if nextQuotaUsage < 0 {
			return nil, baseRevision, false, fmt.Errorf(
				"quota usage underflow: current=%d delta=%d", currentUsage, delta,
			)
		}
		if delta > 0 && nextQuotaUsage > b.config.QuotaBackendBytes {
			if alarmErr := b.activateNoSpace(ctx); alarmErr != nil {
				return nil, baseRevision, false, alarmErr
			}
			return nil, baseRevision, false, ErrNoSpace
		}
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
	if b.config.QuotaBackendBytes > 0 {
		usageKey := b.ks.EncodeInternalKey(quotaUsageKey)
		nextUsage := encodeQuotaUsage(nextQuotaUsage)
		if quotaUsageRaw == nil {
			batch.PutIfNotExist(usageKey, nextUsage, 0)
		} else {
			batch.CAS(usageKey, nextUsage, quotaUsageRaw, 0)
		}
	}
	guardKeys := make(map[string]struct{}, len(guards))
	for _, gp := range guardPreps {
		guardKeys[string(gp.key)] = struct{}{}
		if gp.missing {
			// Assert absence without leaving a marker. PutIfNotExist establishes a
			// transactional read/write conflict; the following delete removes the
			// temporary value in the same atomic batch.
			batch.PutIfNotExist(gp.key, []byte{0}, 0)
			batch.Del(gp.key)
			continue
		}
		// A no-op CAS protects either the exact tombstone or the live revision.
		batch.CAS(gp.key, gp.rvBytes, gp.rvBytes, 0)
	}
	for _, g := range guards {
		if prepByKey[string(g.Key)] != nil {
			guardKeys[string(b.coder.EncodeRevisionKey(g.Key))] = struct{}{}
		}
	}
	newRevLive := uint64ToBytes(newRevision)
	newRevDeleted := append(uint64ToBytes(newRevision), 0)
	for i := range preps {
		p := &preps[i]
		if !p.effective {
			continue
		}
		if p.op.Internal {
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
	if cerr := b.commitUserBatch(ctx, batch); cerr != nil {
		if errors.Is(cerr, storage.ErrUncertainResult) && txnHasEffectiveUserWrite(preps) {
			// A multi-key txn must never enter the single-key uncertain retry
			// queue: if the original batch committed, that queue would rewrite
			// each key at a separate revision and expose torn transaction state.
			// Event-log records were staged in the same atomic batch, so resolve
			// the whole outcome from those durable markers and publish once.
			b.uncertainTxnPins.pin(newRevision)
			started := b.startWorker(func(ctx context.Context) {
				defer b.uncertainTxnPins.unpin(newRevision)
				b.resolveUncertainTxn(ctx, preps, newRevision)
			})
			if !started {
				b.uncertainTxnPins.unpin(newRevision)
			}
			klog.ErrorS(cerr, "txn apply commit result uncertain; resolving as one transaction",
				"revision", newRevision, "ops", len(ops))
			return nil, newRevision, false, cerr
		}
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
	for i := range preps {
		if preps[i].effective && !preps[i].op.Internal {
			return true
		}
	}
	return false
}

func (b *backend) resolveUncertainTxn(workerCtx context.Context, preps []txnPrep, revision uint64) {
	retryDelay := 100 * time.Millisecond
	for {
		ctx, cancel := context.WithTimeout(workerCtx, unaryRpcTimeout)
		committed, err := b.txnCommitRecorded(ctx, preps, revision)
		cancel()
		if err != nil {
			if workerCtx.Err() != nil {
				return
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
			b.notifyBatch(b.txnWatchEvents(preps, revision))
			b.metricCli.EmitCounter("txn.uncertain.resolve.committed", 1)
			return
		}
		// Use a definite marker so the collector advances without feeding these
		// events into the single-key repair queue.
		klog.InfoS("resolved uncertain txn as not committed", "revision", revision)
		b.notifyInvalidTxn(preps, revision, errTxnResolvedNotCommitted)
		b.metricCli.EmitCounter("txn.uncertain.resolve.not_committed", 1)
		return
	}
}

// txnCommitRecorded checks event-log records written atomically with every
// effective user mutation. All present means the transaction committed; none
// means it did not. A mixed result contradicts TiKV batch atomicity and is
// retried rather than guessed.
func (b *backend) txnCommitRecorded(ctx context.Context, preps []txnPrep, revision uint64) (bool, error) {
	expected, found := 0, 0
	for i := range preps {
		p := &preps[i]
		if !p.effective || p.op.Internal {
			continue
		}
		expected++
		_, err := b.kv.Get(ctx, b.ks.EncodeEventLogKey(revision, p.op.Key))
		switch {
		case err == nil:
			found++
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
		return false, fmt.Errorf("uncertain txn has mixed event markers: found %d of %d at revision %d",
			found, expected, revision)
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
		if !p.effective || p.op.Internal {
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
