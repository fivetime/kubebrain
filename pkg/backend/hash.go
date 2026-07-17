package backend

import (
	"context"
	"hash/crc32"
	"io"
)

var hashKVTable = crc32.MakeTable(crc32.Castagnoli)

// HashKV checksums the physical object MVCC state visible at revision. Internal
// service metadata is intentionally excluded, matching etcd HashKV's role as a
// hash of the user KV bucket rather than every backend bucket.
func (b *backend) HashKV(ctx context.Context, revision int64) (uint32, int64, error) {
	b.logicalWriteMu.Lock()
	defer b.logicalWriteMu.Unlock()
	if err := ctx.Err(); err != nil {
		return 0, 0, err
	}

	if revision == 0 {
		current, err := b.safeCurrentRevision(ctx)
		if err != nil {
			return 0, 0, err
		}
		revision = int64(current)
	}

	it, err := b.kv.Iter(ctx, b.ks.ObjectKeyspaceStart(), b.ks.ObjectKeyspaceEnd(), 0, 0)
	if err != nil {
		return 0, 0, err
	}
	defer it.Close()

	h := crc32.New(hashKVTable)
	_, _ = h.Write([]byte("key"))
	for {
		if err := it.Next(ctx); err != nil {
			if err == io.EOF {
				break
			}
			return 0, 0, err
		}
		key := it.Key()
		if len(key) > 0 && !b.ks.IsInternalStorageKey(key) {
			_, objectRevision, decodeErr := b.coder.Decode(key)
			if decodeErr != nil {
				return 0, 0, decodeErr
			}
			// Revision-zero entries are per-key indexes, not MVCC values.
			if revision >= 0 && objectRevision > 0 && objectRevision <= uint64(revision) {
				_, _ = h.Write(key)
				_, _ = h.Write(it.Val())
			}
		}
	}
	return h.Sum32(), revision, nil
}
