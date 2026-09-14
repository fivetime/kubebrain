package backend

import (
	"context"

	"github.com/kubewharf/kubebrain/pkg/backend/coder"
	"github.com/kubewharf/kubebrain/pkg/storage"
)

// Read indexes alongside the existing alarm batch instead of opening a fresh
// storage read for each operation. Fixed engine snapshots retain their original
// read dispatch; BatchGetter alone does not promise timestamp-pinned reads.
func (b *backend) txnPreparationKeys(ctx context.Context, ops []TxnWriteOp, guards []TxnGuard) [][]byte {
	if _, fixed := storage.SnapshotTimestampFromContext(ctx); fixed {
		return nil
	}
	if _, ok := storage.FindCapability[storage.BatchGetter](b.kv); !ok {
		return nil
	}
	keys := make([][]byte, 0, len(ops)+len(guards))
	seen := make(map[string]struct{}, cap(keys))
	add := func(key []byte) {
		if _, exists := seen[string(key)]; !exists {
			seen[string(key)] = struct{}{}
			keys = append(keys, key)
		}
	}
	for _, op := range ops {
		if op.Internal {
			add(b.ks.EncodeInternalKey(op.Key))
		} else {
			add(b.coder.EncodeRevisionKey(op.Key))
		}
	}
	for _, guard := range guards {
		add(b.coder.EncodeRevisionKey(guard.Key))
	}
	return keys
}

// Batch immutable previous-version reads only after the index snapshot is
// available. Commit still compares every index. Ambiguous/legacy index states
// and pinned snapshots retain the original ordered preparation path.
func (b *backend) prefetchTxnPreviousObjects(ctx context.Context, ops []TxnWriteOp, indexes map[string][]byte) (map[string][]byte, error) {
	if indexes == nil {
		return nil, nil
	}
	if _, pinned := storage.SnapshotTimestampFromContext(ctx); pinned {
		return nil, nil
	}
	getter, ok := storage.FindCapability[storage.BatchGetter](b.kv)
	if !ok {
		return nil, nil
	}
	keys := make([][]byte, 0, len(ops))
	for _, op := range ops {
		if op.Internal {
			continue
		}
		raw, exists := indexes[string(b.coder.EncodeRevisionKey(op.Key))]
		if !exists {
			return nil, nil // orphan repair may restart preparation
		}
		revision, tombstone, err := coder.ParseRevision(raw)
		if err != nil || tombstone {
			return nil, nil
		}
		keys = append(keys, b.coder.EncodeObjectKey(op.Key, revision))
	}
	if len(keys) < 2 {
		return nil, nil // no extra batch for the ordinary single-key Put
	}
	return getter.BatchGet(ctx, keys)
}

func (b *backend) readPrefetchedTxnPreviousObject(ctx context.Context, key []byte, revision uint64, values map[string][]byte) ([]byte, error) {
	physicalKey := string(b.coder.EncodeObjectKey(key, revision))
	if value, exists := values[physicalKey]; exists {
		// Preparation owns this map; duplicate user writes are rejected before
		// it is built. Do not retain discarded old values through the commit.
		delete(values, physicalKey)
		return value, nil
	}
	// Missing physical versions still use historical lookup and its validation.
	return b.readTxnPreviousObject(ctx, key, revision)
}

func (b *backend) readTxnPreparationKey(ctx context.Context, key []byte, prefetched map[string][]byte) ([]byte, error) {
	if prefetched == nil {
		return b.kv.Get(ctx, key)
	}
	value, exists := prefetched[string(key)]
	if !exists {
		return nil, storage.ErrKeyNotFound
	}
	return value, nil
}
