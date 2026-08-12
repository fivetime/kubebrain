// Copyright 2026 ByteDance and/or its affiliates
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

package backend

import (
	"bytes"
	"context"

	proto "github.com/kubewharf/kubebrain-client/api/v2rpc"

	"github.com/kubewharf/kubebrain/pkg/storage"
)

// stageUncertainRepairAtomic rewrites an already-stored object at a fresh
// transaction-local revision. storedValue is intentionally copied verbatim: it
// may be an inline metadata envelope or a tombstone and must never be wrapped a
// second time by the ordinary PUT encoder.
func (b *backend) stageUncertainRepairAtomic(batch storage.BatchWrite, key, storedValue []byte, previousOperationRevision, eventPreviousRevision uint64) *uint64 {
	return b.stageNextDurableRevision(batch, func(ctx context.Context, txn storage.AtomicBatch, revision uint64) error {
		deleted := bytes.Equal(storedValue, tombStoneBytes)
		expectedIndex := uint64ToBytes(previousOperationRevision)
		newIndex := uint64ToBytes(revision)
		verb := proto.Event_PUT
		if deleted {
			expectedIndex = append(expectedIndex, 0)
			newIndex = append(newIndex, 0)
			verb = proto.Event_DELETE
		}
		revisionKey := b.coder.EncodeRevisionKey(key)
		if err := atomicExpect(ctx, txn, revisionKey, expectedIndex, false); err != nil {
			return err
		}
		if err := txn.Put(revisionKey, newIndex, 0); err != nil {
			return err
		}
		if err := txn.Put(b.coder.EncodeObjectKey(key, revision), storedValue, 0); err != nil {
			return err
		}
		eventKey, eventValue := encodeEventLogEntry(b.ks, revision, key, verb, eventPreviousRevision, 0, 1)
		return txn.Put(eventKey, eventValue, 0)
	})
}
