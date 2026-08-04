package backend

import (
	"bytes"
	"context"
	"errors"
	"hash/crc32"
	"io"
	"sort"
)

var hashKVTable = crc32.MakeTable(crc32.Castagnoli)

var (
	ErrHashKVCompacted = errors.New("hash revision has been compacted")
	ErrHashKVFuture    = errors.New("hash revision is in the future")
)

type BackendHashResult struct {
	Hash            uint32
	CurrentRevision int64
}

// Hash checksums the complete encoded tenant keyspace. Unlike HashKV it does
// not interpret MVCC revisions or logical compaction: physical rows and
// internal service metadata are deliberately part of this diagnostic value,
// matching etcd's distinction between backend Hash and user-key HashKV.
func (b *backend) Hash(ctx context.Context) (BackendHashResult, error) {
	b.logicalWriteMu.Lock()
	defer b.logicalWriteMu.Unlock()
	if err := ctx.Err(); err != nil {
		return BackendHashResult{}, err
	}
	it, err := b.kv.Iter(ctx, b.ks.ObjectKeyspaceStart(), b.ks.ObjectKeyspaceEnd(), 0, 0)
	if err != nil {
		return BackendHashResult{}, err
	}
	defer it.Close()
	h := crc32.New(hashKVTable)
	// Upstream backend.Hash includes each bbolt bucket name before its rows.
	// KubeBrain has one tenant-scoped encoded storage domain, so use a stable
	// domain separator rather than pretending its TiKV layout is a bbolt file.
	_, _ = h.Write([]byte("kubebrain-backend"))
	for {
		if err = it.Next(ctx); err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return BackendHashResult{}, err
		}
		_, _ = h.Write(it.Key())
		_, _ = h.Write(it.Val())
	}
	current, err := b.safeCurrentRevision(ctx)
	if err != nil {
		return BackendHashResult{}, err
	}
	return BackendHashResult{Hash: h.Sum32(), CurrentRevision: int64(current)}, nil
}

// HashKV checksums the logical object MVCC state visible at revision. Versions
// retired by logical compaction are excluded before physical GC catches up.
// Internal service metadata is excluded, matching etcd HashKV's user-KV scope.
func (b *backend) HashKV(ctx context.Context, revision int64) (HashKVResult, error) {
	b.logicalWriteMu.Lock()
	defer b.logicalWriteMu.Unlock()
	if err := ctx.Err(); err != nil {
		return HashKVResult{}, err
	}

	current, err := b.safeCurrentRevision(ctx)
	if err != nil {
		return HashKVResult{}, err
	}
	if revision == 0 {
		revision = int64(current)
	}
	compactRevision, hasCompactRevision, err := b.loadCompactRevisionState(ctx)
	if err != nil {
		return HashKVResult{}, err
	}
	if revision > 0 && hasCompactRevision && uint64(revision) < compactRevision {
		return HashKVResult{}, ErrHashKVCompacted
	}
	if revision > int64(current) {
		return HashKVResult{}, ErrHashKVFuture
	}
	responseCompactRevision := int64(-1)
	if hasCompactRevision {
		responseCompactRevision = int64(compactRevision)
	}

	it, err := b.kv.Iter(ctx, b.ks.ObjectKeyspaceStart(), b.ks.ObjectKeyspaceEnd(), 0, 0)
	if err != nil {
		return HashKVResult{}, err
	}
	defer it.Close()

	h := crc32.New(hashKVTable)
	_, _ = h.Write([]byte("key"))
	type hashRow struct {
		key   []byte
		value []byte
	}
	var familyBoundary []byte
	compactedLatest := make(map[string]hashRow)
	retained := make([]hashRow, 0)
	flushFamily := func() {
		rows := retained
		for _, row := range compactedLatest {
			if !bytes.Equal(row.value, tombStoneBytes) {
				rows = append(rows, row)
			}
		}
		sort.Slice(rows, func(i, j int) bool { return bytes.Compare(rows[i].key, rows[j].key) < 0 })
		for _, row := range rows {
			_, _ = h.Write(row.key)
			_, _ = h.Write(row.value)
		}
		clear(compactedLatest)
		retained = retained[:0]
	}
	for {
		if err := it.Next(ctx); err != nil {
			if err == io.EOF {
				break
			}
			return HashKVResult{}, err
		}
		key := it.Key()
		if len(key) > 0 && !b.ks.IsInternalStorageKey(key) {
			userKey, objectRevision, decodeErr := b.coder.Decode(key)
			if decodeErr != nil {
				return HashKVResult{}, decodeErr
			}
			boundary, ok := b.coder.RevisionBoundaryForBorder(key)
			if !ok {
				return HashKVResult{}, errors.New("object key has no revision family boundary")
			}
			if familyBoundary != nil && !bytes.Equal(boundary, familyBoundary) {
				flushFamily()
			}
			familyBoundary = append(familyBoundary[:0], boundary...)
			// Revision-zero entries are per-key indexes, not MVCC values.
			if revision >= 0 && objectRevision > 0 && objectRevision <= uint64(revision) {
				row := hashRow{
					key:   append([]byte(nil), key...),
					value: append([]byte(nil), it.Val()...),
				}
				if hasCompactRevision && objectRevision <= compactRevision {
					compactedLatest[string(userKey)] = row
					continue
				}
				retained = append(retained, row)
			}
		}
	}
	flushFamily()
	return HashKVResult{
		Hash:            h.Sum32(),
		HashRevision:    revision,
		CurrentRevision: int64(current),
		CompactRevision: responseCompactRevision,
	}, nil
}
