package backend

import (
	"bytes"
	"context"
	"errors"
	"hash/crc32"
	"io"
	"sort"

	"github.com/kubewharf/kubebrain/pkg/storage"
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
func (b *backend) Hash(ctx context.Context) (result BackendHashResult, retErr error) {
	checkpoint, pinned := SerializableCheckpointFromContext(ctx)
	if !pinned {
		b.logicalWriteMu.Lock()
		defer b.logicalWriteMu.Unlock()
	}
	if err := ctx.Err(); err != nil {
		return BackendHashResult{}, err
	}
	timestamp := uint64(0)
	if pinned {
		timestamp = checkpoint.Timestamp
	}
	it, err := b.kv.Iter(ctx, b.ks.ObjectKeyspaceStart(), b.ks.ObjectKeyspaceEnd(), timestamp, 0)
	if err != nil {
		return BackendHashResult{}, err
	}
	defer func() { retErr = errors.Join(retErr, it.Close()) }()
	h := crc32.New(hashKVTable)
	// Upstream backend.Hash includes each bbolt bucket name before its rows.
	// KubeBrain has one tenant-scoped encoded storage domain, so use a stable
	// domain separator rather than pretending its TiKV layout is a bbolt file.
	_, _ = h.Write([]byte("kubebrain-backend"))
	checkpointKey := b.ks.EncodeInternalKey(serializableCheckpointKey)
	for {
		if err = it.Next(ctx); err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return BackendHashResult{}, err
		}
		// Match etcd schema.DefaultIgnores' treatment of backend bookkeeping
		// (term, consistent-index and storage-version). This checkpoint's TiKV
		// timestamp is refreshed by a background worker even when no user or
		// service state changes; hashing it makes two adjacent Maintenance Hash
		// calls disagree at the same public revision.
		if bytes.Equal(it.Key(), checkpointKey) {
			continue
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
func (b *backend) HashKV(ctx context.Context, revision int64) (result HashKVResult, retErr error) {
	_, pinned := storage.SnapshotTimestampFromContext(ctx)
	if !pinned {
		b.logicalWriteMu.Lock()
		defer b.logicalWriteMu.Unlock()
	}
	if err := ctx.Err(); err != nil {
		return HashKVResult{}, err
	}
	// A witnessed corrupt row must persist the shared write fence before this
	// exclusive hash barrier is released. Mark ownership so ArmCorrupt's
	// InternalCAS does not try to reacquire logicalWriteMu through its read side.
	// A protected snapshot was established under that same barrier and is
	// immutable; it must not queue behind an ambiguous live mutation. Its corrupt
	// witness path retains the ordinary self-locking behavior.
	if !pinned {
		ctx = b.withLogicalWriteOwnership(ctx)
	}

	current, err := b.safeCurrentRevision(ctx)
	if err != nil {
		return HashKVResult{}, err
	}
	if revision == 0 {
		revision = int64(current)
	}
	var compactRevision uint64
	var hasCompactRevision bool
	if checkpoint, ok := SerializableCheckpointFromContext(ctx); ok {
		compactRevision = checkpoint.CompactRevision
		hasCompactRevision = compactRevision != 0
	} else {
		compactRevision, hasCompactRevision, err = b.loadCompactRevisionState(ctx)
		if err != nil {
			return HashKVResult{}, err
		}
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

	timestamp, _ := storage.SnapshotTimestampFromContext(ctx)
	it, err := b.kv.Iter(ctx, b.ks.ObjectKeyspaceStart(), b.ks.ObjectKeyspaceEnd(), timestamp, 0)
	if err != nil {
		return HashKVResult{}, err
	}
	defer func() { retErr = errors.Join(retErr, it.Close()) }()

	h := crc32.New(hashKVTable)
	_, _ = h.Write([]byte("key"))
	type hashRow struct {
		key      []byte
		userKey  []byte
		revision uint64
		value    []byte
	}
	var familyBoundary []byte
	compactedLatest := make(map[string]hashRow)
	retained := make([]hashRow, 0)
	flushFamily := func() error {
		rows := retained
		for _, row := range compactedLatest {
			if !bytes.Equal(row.value, tombStoneBytes) {
				rows = append(rows, row)
			}
		}
		sort.Slice(rows, func(i, j int) bool { return bytes.Compare(rows[i].key, rows[j].key) < 0 })
		for _, row := range rows {
			if b.config.EnableEtcdCompatibility && !bytes.Equal(row.value, tombStoneBytes) {
				validationErr := b.validateEventObjectValue(ctx, row.userKey, row.revision, row.value)
				if errors.Is(validationErr, ErrInvalidMVCCMetadata) {
					validationErr = b.persistWitnessedObjectCorruption(
						ctx, row.userKey, row.revision, row.key, row.value, validationErr,
					)
				}
				if validationErr != nil {
					return validationErr
				}
			}
			_, _ = h.Write(row.key)
			_, _ = h.Write(row.value)
		}
		clear(compactedLatest)
		retained = retained[:0]
		return nil
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
				return HashKVResult{}, invalidMVCCMetadataError(decodeErr, "decode hash object key")
			}
			boundary, ok := b.coder.RevisionBoundaryForBorder(key)
			if !ok {
				return HashKVResult{}, errors.New("object key has no revision family boundary")
			}
			if familyBoundary != nil && !bytes.Equal(boundary, familyBoundary) {
				if err := flushFamily(); err != nil {
					return HashKVResult{}, err
				}
			}
			familyBoundary = append(familyBoundary[:0], boundary...)
			// Revision-zero entries are per-key indexes, not MVCC values.
			if revision >= 0 && objectRevision > 0 && objectRevision <= uint64(revision) {
				row := hashRow{
					key:      append([]byte(nil), key...),
					userKey:  append([]byte(nil), userKey...),
					revision: objectRevision,
					value:    append([]byte(nil), it.Val()...),
				}
				if hasCompactRevision && objectRevision <= compactRevision {
					compactedLatest[string(userKey)] = row
					continue
				}
				retained = append(retained, row)
			}
		}
	}
	if err := flushFamily(); err != nil {
		return HashKVResult{}, err
	}
	return HashKVResult{
		Hash:            h.Sum32(),
		HashRevision:    revision,
		CurrentRevision: int64(current),
		CompactRevision: responseCompactRevision,
	}, nil
}
