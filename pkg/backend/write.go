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
	"fmt"
	"time"

	"github.com/pkg/errors"
	"k8s.io/klog/v2"

	proto "github.com/kubewharf/kubebrain-client/api/v2rpc"

	"github.com/kubewharf/kubebrain/pkg/backend/coder"
	"github.com/kubewharf/kubebrain/pkg/storage"
)

// Create implements Backend interface
func (b *backend) Create(ctx context.Context, put *proto.CreateRequest) (resp *proto.CreateResponse, err error) {
	if b.config.QuotaBackendBytes > 0 {
		return b.quotaCreate(ctx, put)
	}
	unlock := b.lockLogicalWrite(ctx)
	defer unlock()
	ts := time.Now()
	defer func() {
		txnLog("create",
			put.GetKey(),
			len(put.GetValue()),
			0,
			resp.GetHeader().GetRevision(),
			resp.GetSucceeded(),
			time.Since(ts),
			err)
	}()

	revision, err := b.create(ctx, put.Key, put.Value, put.Lease)
	b.notify(ctx, put.Key, b.eventValue(put.Value, EtcdMetadata{CreateRevision: revision, Version: 1, Lease: put.Lease}, err), revision, 0, err == nil, proto.Event_CREATE, err)
	// ACK only after the committed watermark reaches this write, so the client
	// can immediately read its own write at rev=0 (etcd apply-then-ack, #35).
	b.waitCommittedRevision(ctx, revision)
	if errors.Is(err, storage.ErrCASFailed) {
		return &proto.CreateResponse{
			Header:    responseHeader(revision),
			Succeeded: false,
		}, nil
	} else if err != nil {
		klog.ErrorS(err, "backend create err", "key", string(put.GetKey()), "revision", revision)
		return nil, err
	}

	return &proto.CreateResponse{
		Header:    responseHeader(revision),
		Succeeded: true,
	}, nil
}

func (b *backend) create(ctx context.Context, key []byte, value []byte, lease int64) (revision uint64, error error) {
	revision, err := b.deal(0)
	if err != nil {
		// Best-effort only: with prev=0, deal's error paths return revision 0 and
		// the in-process TSO never fails a Deal, so today this branch cannot hand
		// back a consumed revision — a TSO implementation that consumes and THEN
		// errors would still leave a ring hole for the stall watchdog (review
		// #51). Kept for the day deal(0) can report the dealt value on error.
		return revision, err
	}

	// Key expiry is driven entirely by the lease attached to the key at the etcd
	// layer (etcd v3 has no non-lease TTL): the lease's granted TTL is honored
	// per-key by the server-layer lease manager, which deletes the bound keys when
	// the lease expires. The backend therefore writes no storage-level TTL.
	// Previously an unanchored bytes.Contains(key, "/events/") heuristic stamped a
	// hardcoded 3600s TTL, which ignored the real granted TTL, ignored keepalive
	// renewal, and mis-expired unrelated keys whose path merely contained
	// "/events/" (data loss) — see #16.
	retryOldRev, retriable, err := b.createWithMetadata(ctx, key, value, revision, lease)
	if err == nil || !retriable {
		return revision, err
	}

	// The key's revision index holds a stale tombstone (or vanished between the
	// CAS and the diagnostic read): retry as a CAS over that index — but at a
	// FRESH revision, releasing the first one as an invalid event right away.
	// The collector's committed watermark cannot pass a dealt revision until its
	// notify, and every network round-trip in between (the diagnostic Get plus a
	// second batch) stalls every concurrent writer's commit-wait behind this one
	// key's conflict resolution (#44: 35% CAS-failure load degraded bare PUT
	// p50 to 451ms while the batches themselves took 30ms).
	b.notify(ctx, key, nil, revision, 0, false, proto.Event_CREATE, err)
	b.metricCli.EmitCounter("create.conflict_retry.rebase", 1)
	revision, err = b.deal(0)
	if err != nil {
		return revision, err
	}
	revisionKey := b.coder.EncodeRevisionKey(key)
	objectKey := b.coder.EncodeObjectKey(key, revision)
	// retryOldRev nil means the index key was missing at the diagnostic read:
	// re-attempt the optimistic create; otherwise CAS over the stale tombstone.
	err = b.createBatchWithMetadata(ctx, revisionKey, objectKey, key, value, uint64ToBytes(revision), retryOldRev, revision, retryOldRev == nil, lease)
	return revision, err
}

// createWithMetadata makes ONE optimistic create attempt. On a CAS conflict it
// diagnoses the index key and reports whether a retry can succeed (stale
// tombstone below our revision, or a concurrently-vanished index). It never
// issues the second batch itself: the retry must run at a fresh revision (see
// create) so the collector's watermark is not held behind the extra
// round-trips.
func (b *backend) createWithMetadata(ctx context.Context, key []byte, value []byte, revision uint64, lease int64) (retryOldRev []byte, retriable bool, err error) {
	revisionKey := b.coder.EncodeRevisionKey(key)
	objectKey := b.coder.EncodeObjectKey(key, revision)
	revisionBytes := uint64ToBytes(revision)

	err = b.createBatchWithMetadata(ctx, revisionKey, objectKey, key, value, revisionBytes, nil, revision, true, lease)
	if err == nil {
		return nil, false, nil
	}
	if !errors.Is(err, storage.ErrCASFailed) {
		return nil, false, err
	}

	var oldRev []byte
	if conflict, ok := err.(*storage.Conflict); ok && conflict.Idx == 0 {
		oldRev = conflict.Val
	} else {
		var gerr error
		oldRev, gerr = b.kv.Get(ctx, revisionKey)
		if gerr != nil {
			if errors.Is(gerr, storage.ErrKeyNotFound) {
				// Index vanished between the CAS and this read (concurrent
				// delete+compact); the optimistic create can be retried.
				return nil, true, err
			}
			return nil, false, storage.ErrUnavailable
		}
	}

	prevRevision, isTombstone, parseErr := coder.ParseRevision(oldRev)
	if parseErr != nil {
		return nil, false, parseErr
	}
	if isTombstone && prevRevision < revision {
		return oldRev, true, err
	}
	return nil, false, storage.ErrCASFailed
}

// createBatchWithMetadata writes the revision-key and object-key with no
// storage-level TTL (ttl=0): key expiry is the lease manager's responsibility,
// not the backend's — see create.
func (b *backend) createBatchWithMetadata(ctx context.Context, revisionKey, objectKey, key, value, newRevisionBytes, oldRevisionBytes []byte, revision uint64, requireNotExist bool, lease int64) error {
	// Fence the write just before opening the batch: reject if leadership changed
	// since admission so a deposed leader cannot commit at a revision the new
	// leader's collector has already advanced past (FINDING #39).
	if err := b.fenceAdmit(ctx); err != nil {
		return err
	}
	batch := b.kv.BeginBatchWrite()
	if requireNotExist {
		batch.PutIfNotExist(revisionKey, newRevisionBytes, 0)
	} else {
		batch.CAS(revisionKey, newRevisionBytes, oldRevisionBytes, 0)
	}
	// Inline create_revision/version (and lease, review #9) into the value so
	// reads need no separate metadata lookup and the etcdmeta keyspace stops
	// growing (approach A); putTxnObject is the single writer of that convention.
	meta := EtcdMetadata{CreateRevision: revision, Version: 1, Lease: lease}
	b.putTxnObject(batch, objectKey, key, value, meta, revision)
	appendEventLog(b.ks, batch, revision, key, proto.Event_CREATE, 0, 0, 1)
	b.stageDurableRevision(batch, revision)
	return b.commitUserBatch(ctx, batch)
}

// Delete implements Backend interface
func (b *backend) Delete(ctx context.Context, r *proto.DeleteRequest) (*proto.DeleteResponse, error) {
	if b.config.QuotaBackendBytes > 0 {
		return b.quotaDelete(ctx, r)
	}
	unlock := b.lockLogicalWrite(ctx)
	defer unlock()
	return b.deleteOnce(ctx, r, true)
}

func (b *backend) deleteOnce(ctx context.Context, r *proto.DeleteRequest, allowHeal bool) (resp *proto.DeleteResponse, err error) {
	ts := time.Now()
	defer func() {
		txnLog("delete",
			r.GetKey(),
			0,
			r.GetRevision(),
			resp.GetHeader().GetRevision(),
			resp.GetSucceeded(),
			time.Since(ts),
			err)

	}()

	rev, old, err := b.delete(ctx, r.Revision, r.Key)

	// A missing key is an etcd no-op: it allocates no revision and emits no
	// event. Other failures may already have consumed a TSO revision and must
	// still publish an invalid event so the collector can advance past it.
	if !errors.Is(err, storage.ErrKeyNotFound) {
		b.notify(ctx, r.Key, old.Val, rev, old.Revision, err == nil, proto.Event_DELETE, err)
		b.waitCommittedRevision(ctx, rev) // apply-then-ack (#35)
	}

	resp = &proto.DeleteResponse{
		Header:    responseHeader(rev),
		Succeeded: err == nil,
	}

	if err == storage.ErrKeyNotFound {
		// key is not exist
		return resp, nil
	} else if errors.Is(err, storage.ErrCASFailed) {
		// An orphaned key (object present, revision-index missing) CASes the
		// missing index and can never be deleted. Heal the index once and retry so
		// the delete self-recovers instead of the key being undeletable forever.
		if allowHeal {
			if healed, healErr := b.healOrphanIndex(ctx, r.Key); healErr == nil && healed {
				return b.deleteOnce(ctx, r, false)
			}
		}
		// possible case:
		// 1. expect revision is too old
		// 2. concurrent modification
		val, modRevision, getErr := b.get(ctx, r.Key, 0)
		if getErr != nil {
			resp.Kv = &proto.KeyValue{
				Key:      r.Key,
				Value:    old.Val,
				Revision: old.Revision,
			}
			return resp, nil
		}

		resp.Header.Revision = maxUint64(resp.Header.Revision, modRevision)
		resp.Kv = &proto.KeyValue{
			Key:      r.Key,
			Value:    val,
			Revision: modRevision,
		}
		return resp, nil
	} else if err != nil {
		klog.ErrorS(err, "backend delete err", "key", string(r.GetKey()), "old revision", r.GetRevision(), "new revision", rev)
		return nil, err
	}

	resp.Succeeded = true
	resp.Kv = &proto.KeyValue{
		Key:      old.Key,
		Value:    old.Val,
		Revision: old.Revision,
	}
	return resp, nil
}

func (b *backend) mustDeal(prevRev uint64) (rev uint64) {
	rev, _ = b.deal(prevRev)
	return rev
}

func (b *backend) delete(ctx context.Context, oldRevision uint64, key []byte) (newRevision uint64, old KeyVal, err error) {
	expectedRevision := oldRevision

	// get the latest value
	oldVal, modRevision, err := b.get(ctx, key, 0)
	if err != nil {
		if errors.Is(err, storage.ErrKeyNotFound) {
			return b.GetCurrentRevision(), KeyVal{}, err
		}
		getRev := b.mustDeal(oldRevision)
		return getRev, KeyVal{}, err
	}

	old = KeyVal{Key: key, Revision: modRevision, Val: oldVal}

	newRevision, err = b.deal(oldRevision)
	if err != nil {
		// The revision was already consumed from the TSO; return it so the
		// caller's notify publishes an invalid event, otherwise the event
		// collector stalls on this revision forever.
		return newRevision, old, err
	}

	if expectedRevision > 0 && expectedRevision != modRevision {
		// expect revision is too old
		klog.InfoS("delete cas failed", "expectRev", expectedRevision, "actualRev", modRevision)
		return newRevision, old, storage.ErrCASFailed
	} else if expectedRevision <= 0 {
		// delete the latest
		expectedRevision = modRevision
	}

	if newRevision <= modRevision {
		// there maybe a concurrent txn which is dealt later but is done more quickly
		klog.InfoS("delete cas failed", "newRev", newRevision, "modRev", expectedRevision)
		return newRevision, old, fmt.Errorf("cas failed, new revision is %d existing revision is %d", newRevision, modRevision)
	}

	objectKey := b.coder.EncodeObjectKey(key, newRevision)
	revisionKey := b.coder.EncodeRevisionKey(key)
	expectedRevisionBytes := uint64ToBytes(expectedRevision)
	newRevisionBytes := append(uint64ToBytes(newRevision), 0) // delete revision
	// delete revision, key is {raw_key}:{0}, value is {revision}{deletion_flag}

	// Fence just before the batch: the caller's notify publishes an invalid event
	// for newRevision so the collector advances past the consumed revision.
	if err = b.fenceAdmit(ctx); err != nil {
		return newRevision, old, err
	}
	batch := b.kv.BeginBatchWrite()
	batch.CAS(revisionKey, newRevisionBytes, expectedRevisionBytes, 0)
	batch.Put(objectKey, tombStoneBytes, 0)
	appendEventLog(b.ks, batch, newRevision, key, proto.Event_DELETE, expectedRevision, 0, 1)
	b.stageDurableRevision(batch, newRevision)
	err = b.commitUserBatch(ctx, batch)

	// todo: need an internal retry if there is any conflict error?
	return newRevision, old, err
}

// healOrphanIndex repairs a key whose object versions exist but whose
// revision-index (rev=0) slot is missing. Such an orphan (left by a pre-#31
// compaction/retry race that GC'd a deletion-flagged index while a retried DELETE
// left the base object) is read-visible — b.get scans object keys and ignores the
// index — but permanently un-writable: every Update/Delete CASes the missing index
// key, which the storage reports as a conflict, so the write retries forever
// ("still contended"). This restores the index to point at the latest object
// revision so the next write proceeds normally. It is a pure repair: it allocates
// NO new revision and publishes NO event — it only re-materializes the index
// pointer for an already-committed object revision. Returns true if it healed an
// orphan (including when a concurrent writer recreated the index first).
func (b *backend) healOrphanIndex(ctx context.Context, key []byte) (bool, error) {
	// Only a live object (non-tombstone) needs a usable index; a deleted+compacted
	// key with no index reads as not-found, which is already correct.
	_, modRev, err := b.get(ctx, key, 0)
	if err != nil {
		return false, nil
	}
	revisionKey := b.coder.EncodeRevisionKey(key)
	if _, gerr := b.kv.Get(ctx, revisionKey); gerr == nil {
		// Index present: this was an ordinary stale/concurrent CAS, not an orphan.
		return false, nil
	} else if !errors.Is(gerr, storage.ErrKeyNotFound) {
		return false, gerr
	}
	batch := b.kv.BeginBatchWrite()
	batch.PutIfNotExist(revisionKey, uint64ToBytes(modRev), 0)
	if cerr := batch.Commit(ctx); cerr != nil {
		if errors.Is(cerr, storage.ErrCASFailed) {
			// A concurrent write recreated the index; the orphan is resolved.
			return true, nil
		}
		return false, cerr
	}
	klog.InfoS("healed orphan revision index", "key", string(key), "revision", modRev)
	b.metricCli.EmitCounter("backend.orphan_index.heal", 1)
	return true, nil
}

// DeleteRange removes a set of live keys atomically. Like etcd, all keys deleted
// by one range request share the same modification revision.
func (b *backend) DeleteRange(ctx context.Context, kvs []*proto.KeyValue) (resp *DeleteRangeResponse, err error) {
	ts := time.Now()
	defer func() {
		var respRev uint64
		var succeeded bool
		if resp != nil && resp.Header != nil {
			respRev = resp.Header.Revision
			succeeded = resp.Succeeded
		}
		txnLog("delete-range",
			nil,
			len(kvs),
			0,
			respRev,
			succeeded,
			time.Since(ts),
			err)
	}()

	resp = &DeleteRangeResponse{
		Header:    responseHeader(b.GetCurrentRevision()),
		Succeeded: true,
		Kvs:       make([]*proto.KeyValue, 0, len(kvs)),
	}
	if len(kvs) == 0 {
		return resp, nil
	}

	ops := make([]TxnWriteOp, 0, len(kvs))
	guards := make([]TxnGuard, 0, len(kvs))
	for _, kv := range kvs {
		if kv == nil || len(kv.Key) == 0 || kv.Revision == 0 {
			continue
		}
		key := append([]byte(nil), kv.Key...)
		ops = append(ops, TxnWriteOp{Delete: true, Key: key})
		guards = append(guards, TxnGuard{Key: key, Revision: kv.Revision})
	}
	if len(ops) == 0 {
		return resp, nil
	}
	results, revision, err := b.TxnApply(ctx, ops, guards)
	if err != nil {
		resp.Succeeded = false
		return resp, err
	}
	resp.Header = responseHeader(revision)
	for _, result := range results {
		if !result.Deleted {
			continue
		}
		resp.Kvs = append(resp.Kvs, &proto.KeyValue{
			Key: result.Key, Value: result.PrevValue, Revision: result.PrevRevision,
		})
	}
	return resp, nil
}

// Update implements Backend interface
func (b *backend) Update(ctx context.Context, r *proto.UpdateRequest) (*proto.UpdateResponse, error) {
	if b.config.QuotaBackendBytes > 0 {
		return b.quotaUpdate(ctx, r)
	}
	unlock := b.lockLogicalWrite(ctx)
	defer unlock()
	return b.updateOnce(ctx, r, true)
}

func (b *backend) updateOnce(ctx context.Context, r *proto.UpdateRequest, allowHeal bool) (resp *proto.UpdateResponse, err error) {
	ts := time.Now()
	defer func() {
		txnLog("update",
			r.GetKv().GetKey(),
			len(r.GetKv().GetValue()),
			r.GetKv().Revision,
			resp.GetHeader().GetRevision(),
			resp.GetSucceeded(),
			time.Since(ts),
			err)
	}()

	var (
		key     = r.Kv.Key
		value   = r.Kv.Value
		prevRev = r.Kv.Revision
		lease   = r.Lease
	)
	var curRev uint64
	if prevRev == 0 {
		curRev, err = b.create(ctx, key, value, lease)
		b.notify(ctx, key, b.eventValue(value, EtcdMetadata{CreateRevision: curRev, Version: 1, Lease: lease}, err), curRev, prevRev, err == nil, proto.Event_CREATE, err)
	} else {
		var meta EtcdMetadata
		curRev, meta, err = b.update(ctx, prevRev, key, value, lease)
		b.notify(ctx, key, b.eventValue(value, meta, err), curRev, prevRev, err == nil, proto.Event_PUT, err)
	}

	b.waitCommittedRevision(ctx, curRev) // apply-then-ack (#35)

	resp = &proto.UpdateResponse{
		Header:    responseHeader(curRev),
		Succeeded: err == nil,
	}
	if errors.Is(err, storage.ErrCASFailed) {
		// An orphaned key (object present, revision-index missing) CASes the
		// missing index and fails forever. Heal the index once and retry so the
		// write self-recovers instead of poisoning the key permanently.
		if allowHeal {
			if healed, healErr := b.healOrphanIndex(ctx, key); healErr == nil && healed {
				return b.updateOnce(ctx, r, false)
			}
		}
		// cas failed, just return the latest value
		val, modRevision, err := b.get(ctx, key, 0)
		if err != nil {
			if errors.Is(err, storage.ErrKeyNotFound) {
				resp.Kv = nil
				return resp, nil
			}
			return nil, err
		}
		resp.Header.Revision = maxUint64(resp.Header.Revision, modRevision)
		resp.Kv = &proto.KeyValue{
			Key:      key,
			Value:    val,
			Revision: modRevision,
		}
		return resp, nil
	} else if err != nil {
		klog.ErrorS(err, "backend update err", "key", string(r.GetKv().GetKey()), "old revision", r.GetKv().GetRevision(), "new revision", curRev)
		return nil, err
	}
	return resp, nil
}

func (b *backend) update(ctx context.Context, oldRevision uint64, key []byte, value []byte, lease int64) (revision uint64, newMeta EtcdMetadata, err error) {
	// Read the prior version's metadata BEFORE dealing the revision: the lookup
	// depends only on (key, oldRevision). Every network round-trip between deal
	// and the batch commit sits inside the collector's head-of-line window — the
	// committed watermark cannot pass the dealt revision until its notify — so
	// keeping this Get inside the window stalled every concurrent writer's
	// commit-wait by an extra round-trip per update (#44). Failing here consumes
	// no revision, so the caller's notify (revision 0) is dropped harmlessly.
	meta, err := b.GetEtcdMetadata(ctx, key, oldRevision)
	if err != nil {
		return 0, EtcdMetadata{}, err
	}
	var newRevision uint64
	newRevision, err = b.deal(oldRevision)
	if err != nil {
		// The revision was already consumed from the TSO (Deal never rolls
		// back). Return it so the caller's notify publishes an invalid event
		// filling its ring slot; returning 0 here would leave the event
		// collector waiting on this revision forever, freezing the committed
		// revision and thus every list/watch on the cluster.
		return newRevision, EtcdMetadata{}, err
	}
	if meta.CreateRevision == 0 {
		meta.CreateRevision = oldRevision
	}
	if meta.Version == 0 {
		meta.Version = 1
	}
	meta.Version++
	// The lease is a property of this new version, not inherited from the prior
	// one: an update can rebind to a different lease or clear it (review #9).
	meta.Lease = lease

	objectKey := b.coder.EncodeObjectKey(key, newRevision)
	revisionKey := b.coder.EncodeRevisionKey(key)
	oldRevisionBytes := uint64ToBytes(oldRevision)
	newRevisionBytes := uint64ToBytes(newRevision)

	// Fence just before the batch (FINDING #39); the caller's notify publishes an
	// invalid event for newRevision so the collector advances past it.
	if err = b.fenceAdmit(ctx); err != nil {
		return newRevision, meta, err
	}
	batch := b.kv.BeginBatchWrite()
	batch.CAS(revisionKey, newRevisionBytes, oldRevisionBytes, 0)
	b.putTxnObject(batch, objectKey, key, value, meta, newRevision)
	appendEventLog(b.ks, batch, newRevision, key, proto.Event_PUT, oldRevision, 0, 1)
	b.stageDurableRevision(batch, newRevision)
	return newRevision, meta, b.commitUserBatch(ctx, batch)
}

// eventValue wraps a successful PUT/CREATE watch event's value with its inline
// metadata (approach A-core-2) so watchers get create_revision/version with no
// storage lookup. Returns the raw value on failure or in non-compat mode.
func (b *backend) eventValue(value []byte, meta EtcdMetadata, err error) []byte {
	if err != nil || !b.config.EnableEtcdCompatibility {
		return value
	}
	return encodeValueWithMeta(value, meta)
}
