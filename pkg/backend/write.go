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
	"time"

	"github.com/pkg/errors"
	"k8s.io/klog/v2"

	proto "github.com/kubewharf/kubebrain-client/api/v2rpc"

	"github.com/kubewharf/kubebrain/pkg/storage"
)

// Create implements Backend interface.
func (b *backend) Create(ctx context.Context, put *proto.CreateRequest) (*proto.CreateResponse, error) {
	return b.transactionalCreate(ctx, put)
}

// Delete implements Backend interface.
func (b *backend) Delete(ctx context.Context, request *proto.DeleteRequest) (*proto.DeleteResponse, error) {
	return b.transactionalDelete(ctx, request)
}

// Update implements Backend interface.
func (b *backend) Update(ctx context.Context, request *proto.UpdateRequest) (*proto.UpdateResponse, error) {
	return b.transactionalUpdate(ctx, request)
}

type encodedPutMutation struct {
	revisionKey           []byte
	expectedRevisionValue []byte
	newRevisionValue      []byte
	objectMutations       []encodedMutation
	eventKey              []byte
	eventValue            []byte
}

func (b *backend) encodeCreateMutation(key, value []byte, meta EtcdMetadata, revision uint64, subRevision, total uint32) encodedPutMutation {
	return b.encodePutMutation(key, value, meta, 0, revision, proto.Event_CREATE, subRevision, total)
}

// encodePutMutation contains every revision-dependent byte staged by TxnApply.
func (b *backend) encodePutMutation(key, value []byte, meta EtcdMetadata, previousRevision, newRevision uint64, verb proto.Event_EventType, subRevision, total uint32) encodedPutMutation {
	objectKey := b.coder.EncodeObjectKey(key, newRevision)
	eventKey, eventValue := encodeEventLogEntry(b.ks, newRevision, key, verb, previousRevision, subRevision, total)
	return encodedPutMutation{
		revisionKey:           b.coder.EncodeRevisionKey(key),
		expectedRevisionValue: uint64ToBytes(previousRevision),
		newRevisionValue:      uint64ToBytes(newRevision),
		objectMutations:       b.encodeTxnObjectMutations(objectKey, key, value, meta, newRevision),
		eventKey:              eventKey,
		eventValue:            eventValue,
	}
}

type encodedDeleteMutation struct {
	revisionKey           []byte
	expectedRevisionValue []byte
	newRevisionValue      []byte
	objectKey             []byte
	objectValue           []byte
	eventKey              []byte
	eventValue            []byte
}

func (b *backend) encodeDeleteMutation(key []byte, expectedRevision, newRevision uint64) encodedDeleteMutation {
	return b.encodeDeleteMutationAt(key, expectedRevision, newRevision, 0, 1)
}

func (b *backend) encodeDeleteMutationAt(key []byte, expectedRevision, newRevision uint64, subRevision, total uint32) encodedDeleteMutation {
	eventKey, eventValue := encodeEventLogEntry(b.ks, newRevision, key, proto.Event_DELETE, expectedRevision, subRevision, total)
	return encodedDeleteMutation{
		revisionKey:           b.coder.EncodeRevisionKey(key),
		expectedRevisionValue: uint64ToBytes(expectedRevision),
		newRevisionValue:      append(uint64ToBytes(newRevision), 0),
		objectKey:             b.coder.EncodeObjectKey(key, newRevision),
		objectValue:           tombStoneBytes,
		eventKey:              eventKey,
		eventValue:            eventValue,
	}
}

// healOrphanIndex repairs a live object whose revision index is missing. It is
// revision-neutral: no user revision or watch event is created.
func (b *backend) healOrphanIndex(ctx context.Context, key []byte) (bool, error) {
	_, modRevision, err := b.get(ctx, key, 0)
	if err != nil {
		return false, nil
	}
	revisionKey := b.coder.EncodeRevisionKey(key)
	if _, getErr := b.kv.Get(ctx, revisionKey); getErr == nil {
		return false, nil
	} else if !errors.Is(getErr, storage.ErrKeyNotFound) {
		return false, getErr
	}
	batch := b.kv.BeginBatchWrite()
	batch.PutIfNotExist(revisionKey, uint64ToBytes(modRevision), 0)
	if commitErr := batch.Commit(ctx); commitErr != nil {
		if errors.Is(commitErr, storage.ErrCASFailed) {
			return true, nil
		}
		return false, commitErr
	}
	klog.InfoS("healed orphan revision index", "key", string(key), "revision", modRevision)
	b.metricCli.EmitCounter("backend.orphan_index.heal", 1)
	return true, nil
}

// DeleteRange removes a set of live keys atomically at one modification revision.
func (b *backend) DeleteRange(ctx context.Context, kvs []*proto.KeyValue) (resp *DeleteRangeResponse, err error) {
	started := time.Now()
	defer func() {
		var revision uint64
		var succeeded bool
		if resp != nil && resp.Header != nil {
			revision = resp.Header.Revision
			succeeded = resp.Succeeded
		}
		txnLog("delete-range", nil, len(kvs), 0, revision, succeeded, time.Since(started), err)
	}()

	resp = &DeleteRangeResponse{
		Header: responseHeader(b.GetCurrentRevision()), Succeeded: true,
		Kvs: make([]*proto.KeyValue, 0, len(kvs)),
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
	results, revision, applyErr := b.TxnApply(ctx, ops, guards)
	if applyErr != nil {
		resp.Succeeded = false
		return resp, applyErr
	}
	resp.Header = responseHeader(revision)
	for _, result := range results {
		if result.Deleted {
			resp.Kvs = append(resp.Kvs, &proto.KeyValue{
				Key: result.Key, Value: result.PrevValue, Revision: result.PrevRevision,
			})
		}
	}
	return resp, nil
}

// eventValue wraps a successful PUT/CREATE event value with inline etcd metadata.
func (b *backend) eventValue(value []byte, meta EtcdMetadata, err error) []byte {
	if err != nil || !b.config.EnableEtcdCompatibility {
		return value
	}
	return encodeValueWithMeta(value, meta)
}
