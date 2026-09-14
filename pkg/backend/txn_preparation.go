package backend

import (
	"context"

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
