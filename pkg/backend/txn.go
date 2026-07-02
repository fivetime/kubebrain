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
	"time"

	"github.com/pkg/errors"
	"k8s.io/klog/v2"

	proto "github.com/kubewharf/kubebrain-client/api/v2rpc"

	"github.com/kubewharf/kubebrain/pkg/backend/coder"
	"github.com/kubewharf/kubebrain/pkg/backend/common"
	"github.com/kubewharf/kubebrain/pkg/storage"
)

// Create implements Backend interface
func (b *backend) Create(ctx context.Context, put *proto.CreateRequest) (resp *proto.CreateResponse, err error) {
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

	revision, err := b.create(ctx, put.Key, put.Value)
	b.notify(ctx, put.Key, put.Value, revision, 0, err == nil, proto.Event_CREATE, err)
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

func (b *backend) create(ctx context.Context, key []byte, value []byte) (revision uint64, error error) {
	revision, err := b.deal(0)
	if err != nil {
		return 0, err
	}

	ttl := int64(0)
	if bytes.Contains(key, events) {
		ttl = eventsTTL
	}
	err = b.createWithMetadata(ctx, key, value, revision, ttl)
	return revision, err
}

func (b *backend) createWithMetadata(ctx context.Context, key []byte, value []byte, revision uint64, ttl int64) error {
	revisionKey := b.coder.EncodeRevisionKey(key)
	objectKey := b.coder.EncodeObjectKey(key, revision)
	revisionBytes := uint64ToBytes(revision)

	err := b.createBatchWithMetadata(ctx, revisionKey, objectKey, key, value, revisionBytes, nil, revision, ttl, true)
	if err == nil {
		return nil
	}
	if !errors.Is(err, storage.ErrCASFailed) {
		return err
	}

	var oldRev []byte
	if conflict, ok := err.(*storage.Conflict); ok && conflict.Idx == 0 {
		oldRev = conflict.Val
	} else {
		oldRev, err = b.kv.Get(ctx, revisionKey)
		if err != nil {
			if errors.Is(err, storage.ErrKeyNotFound) {
				return b.createBatchWithMetadata(ctx, revisionKey, objectKey, key, value, revisionBytes, nil, revision, ttl, true)
			}
			return storage.ErrUnavailable
		}
	}

	prevRevision, isTombstone, parseErr := coder.ParseRevision(oldRev)
	if parseErr != nil {
		return parseErr
	}
	if isTombstone && prevRevision < revision {
		return b.createBatchWithMetadata(ctx, revisionKey, objectKey, key, value, revisionBytes, oldRev, revision, ttl, false)
	}
	return storage.ErrCASFailed
}

func (b *backend) createBatchWithMetadata(ctx context.Context, revisionKey, objectKey, key, value, newRevisionBytes, oldRevisionBytes []byte, revision uint64, ttl int64, requireNotExist bool) error {
	batch := b.kv.BeginBatchWrite()
	if requireNotExist {
		batch.PutIfNotExist(revisionKey, newRevisionBytes, ttl)
	} else {
		batch.CAS(revisionKey, newRevisionBytes, oldRevisionBytes, ttl)
	}
	batch.Put(objectKey, value, ttl)
	b.putEtcdMetadata(batch, key, revision, EtcdMetadata{CreateRevision: revision, Version: 1})
	return batch.Commit(ctx)
}

// Delete implements Backend interface
func (b *backend) Delete(ctx context.Context, r *proto.DeleteRequest) (resp *proto.DeleteResponse, err error) {
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

	b.notify(ctx, r.Key, old.Val, rev, old.Revision, err == nil, proto.Event_DELETE, err)

	resp = &proto.DeleteResponse{
		Header:    responseHeader(rev),
		Succeeded: err == nil,
	}

	if err == storage.ErrKeyNotFound {
		// key is not exist
		return resp, nil
	} else if errors.Is(err, storage.ErrCASFailed) {
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

	batch := b.kv.BeginBatchWrite()
	batch.CAS(revisionKey, newRevisionBytes, expectedRevisionBytes, 0)
	batch.Put(objectKey, tombStoneBytes, 0)
	err = batch.Commit(ctx)

	// todo: need an internal retry if there is any conflict error?
	return newRevision, old, err
}

// DeleteRange removes a set of live keys in one storage batch. Like etcd, all
// keys deleted by one range request share the same modification revision.
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

	type pendingDelete struct {
		key         []byte
		value       []byte
		oldRevision uint64
	}
	pending := make([]pendingDelete, 0, len(kvs))
	var maxOldRevision uint64
	for _, kv := range kvs {
		if kv == nil || len(kv.Key) == 0 || kv.Revision == 0 {
			continue
		}
		key := append([]byte(nil), kv.Key...)
		value := append([]byte(nil), kv.Value...)
		oldRevision := kv.Revision
		pending = append(pending, pendingDelete{
			key:         key,
			value:       value,
			oldRevision: oldRevision,
		})
		maxOldRevision = maxUint64(maxOldRevision, oldRevision)
	}
	if len(pending) == 0 {
		return resp, nil
	}
	baseRevision := maxUint64(maxOldRevision, b.GetCurrentRevision())
	newRevision, dealErr := b.deal(baseRevision)
	if dealErr != nil {
		// The revision was already consumed from the TSO; publish an invalid
		// event so the event collector can skip past it instead of stalling
		// on it forever (freezing all lists/watches).
		b.notify(ctx, pending[0].key, nil, newRevision, 0, false, proto.Event_DELETE, dealErr)
		return nil, dealErr
	}
	if newRevision <= baseRevision {
		err := fmt.Errorf("cas failed, new revision is %d existing revision is %d", newRevision, baseRevision)
		b.notify(ctx, pending[0].key, nil, newRevision, 0, false, proto.Event_DELETE, err)
		return nil, err
	}
	batch := b.kv.BeginBatchWrite()
	newRevisionBytes := append(uint64ToBytes(newRevision), 0)
	for _, item := range pending {
		revisionKey := b.coder.EncodeRevisionKey(item.key)
		objectKey := b.coder.EncodeObjectKey(item.key, newRevision)
		batch.CAS(revisionKey, newRevisionBytes, uint64ToBytes(item.oldRevision), 0)
		batch.Put(objectKey, tombStoneBytes, 0)
	}
	if err := batch.Commit(ctx); err != nil {
		resp.Succeeded = false
		// Fill the dealt revision's ring slot with invalid per-key events so
		// (a) the collector advances past it and (b) an uncertain commit is
		// re-resolved per key by the async retry queue, exactly like the
		// single-key delete failure path.
		invalidEvents := make([]*common.WatchEvent, 0, len(pending))
		for _, item := range pending {
			invalidEvents = append(invalidEvents, &common.WatchEvent{
				Revision:     newRevision,
				PrevRevision: item.oldRevision,
				Valid:        false,
				ResourceVerb: proto.Event_DELETE,
				Key:          item.key,
				Value:        item.value,
				Err:          err,
			})
		}
		b.notifyBatch(invalidEvents)
		return nil, err
	}

	resp.Header = responseHeader(newRevision)
	watchEvents := make([]*common.WatchEvent, 0, len(pending))
	for _, item := range pending {
		resp.Kvs = append(resp.Kvs, &proto.KeyValue{
			Key:      item.key,
			Value:    item.value,
			Revision: item.oldRevision,
		})
		watchEvents = append(watchEvents, &common.WatchEvent{
			Revision:     newRevision,
			PrevRevision: item.oldRevision,
			Valid:        true,
			ResourceVerb: proto.Event_DELETE,
			Key:          item.key,
			Value:        item.value,
		})
	}
	b.notifyBatch(watchEvents)
	return resp, nil
}

// Update implements Backend interface
func (b *backend) Update(ctx context.Context, r *proto.UpdateRequest) (resp *proto.UpdateResponse, err error) {
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
		curRev, err = b.create(ctx, key, value)
		b.notify(ctx, key, value, curRev, prevRev, err == nil, proto.Event_CREATE, err)
	} else {
		curRev, err = b.update(ctx, prevRev, key, value, lease)
		b.notify(ctx, key, value, curRev, prevRev, err == nil, proto.Event_PUT, err)
	}

	resp = &proto.UpdateResponse{
		Header:    responseHeader(curRev),
		Succeeded: err == nil,
	}
	if errors.Is(err, storage.ErrCASFailed) {
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

func (b *backend) update(ctx context.Context, oldRevision uint64, key []byte, value []byte, lease int64) (revision uint64, err error) {
	var newRevision uint64
	newRevision, err = b.deal(oldRevision)
	if err != nil {
		// The revision was already consumed from the TSO (Deal never rolls
		// back). Return it so the caller's notify publishes an invalid event
		// filling its ring slot; returning 0 here would leave the event
		// collector waiting on this revision forever, freezing the committed
		// revision and thus every list/watch on the cluster.
		return newRevision, err
	}
	meta, err := b.GetEtcdMetadata(ctx, key, oldRevision)
	if err != nil {
		// Same as above: the dealt revision must reach the ring buffer even
		// though the write never started.
		return newRevision, err
	}
	if meta.CreateRevision == 0 {
		meta.CreateRevision = oldRevision
	}
	if meta.Version == 0 {
		meta.Version = 1
	}
	meta.Version++

	objectKey := b.coder.EncodeObjectKey(key, newRevision)
	revisionKey := b.coder.EncodeRevisionKey(key)
	oldRevisionBytes := uint64ToBytes(oldRevision)
	newRevisionBytes := uint64ToBytes(newRevision)

	batch := b.kv.BeginBatchWrite()
	batch.CAS(revisionKey, newRevisionBytes, oldRevisionBytes, 0)
	batch.Put(objectKey, value, 0)
	b.putEtcdMetadata(batch, key, newRevision, meta)
	return newRevision, batch.Commit(ctx)
}

func (b *backend) notify(ctx context.Context,
	key []byte, val []byte, revision, preRevision uint64, valid bool, eventType proto.Event_EventType, err error) {
	if revision == 0 {
		b.metricCli.EmitCounter("watch.event.buffer.invalid", 1)
		// todo: panic or not ?
		return
	}

	// todo: abstract as an individual component
	watchEvent := &common.WatchEvent{
		Revision:     revision,
		PrevRevision: preRevision,
		Valid:        valid,
		ResourceVerb: eventType,
		Key:          key,
		Value:        val,
		Err:          err,
	}
	// buffer full
	// TODO dynamic size
	if revision-b.GetCurrentRevision() >= watchersChanCapacity {
		b.handleWatchEventOverflow(revision)
		return
	}
	b.watchEventsRingBuffer[int64(revision)%watchersChanCapacity].append(watchEvent)
	b.metricCli.EmitGauge("watch.revision.lag", watchEvent.Revision-b.GetCurrentRevision())
}

func (b *backend) notifyBatch(events []*common.WatchEvent) {
	if len(events) == 0 {
		return
	}
	revision := events[0].Revision
	if revision == 0 {
		b.metricCli.EmitCounter("watch.event.buffer.invalid", 1)
		return
	}
	if revision-b.GetCurrentRevision() >= watchersChanCapacity {
		b.handleWatchEventOverflow(revision)
		return
	}
	b.watchEventsRingBuffer[int64(revision)%watchersChanCapacity].appendAll(events)
	b.metricCli.EmitGauge("watch.revision.lag", revision-b.GetCurrentRevision())
}

func (b *backend) handleWatchEventOverflow(revision uint64) {
	currentRevision := b.GetCurrentRevision()
	b.metricCli.EmitCounter("watch.event.buffer.full", 1)
	klog.ErrorS(nil, "watch event buffer full, resetting watch state", "currentRevision", currentRevision, "revision", revision, "capacity", watchersChanCapacity)
	for i := range b.watchEventsRingBuffer {
		b.watchEventsRingBuffer[i].reset()
	}
	b.watchCache.Reset()
	b.SetCurrentRevision(revision)
	b.watcherHub.CloseAll()
}
