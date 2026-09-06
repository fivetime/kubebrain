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

const (
	// HashKV is an ordered full-physical-keyspace diagnostic. TiKV's iterator
	// fetches one Region at a time, which made a modest multi-Region keyspace take
	// longer than etcdctl's default five-second command timeout. Prefetch a small
	// ordered window of Regions concurrently, while the caller still consumes
	// rows in exact encoded-key order (CRC input order is part of the API).
	hashKVPartitionConcurrency = 8
	hashKVPartitionBatchRows   = 256
	hashKVPartitionBatchBytes  = 1 << 20
)

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
func (b *backend) HashKV(ctx context.Context, revision int64) (HashKVResult, error) {
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
	timestamp, pinned := storage.SnapshotTimestampFromContext(ctx)
	if !pinned {
		timestamp, err = b.kv.GetTimestampOracle(ctx)
		if err != nil {
			return HashKVResult{}, err
		}
	}
	err = b.forEachHashKVRow(ctx, timestamp, pinned, func(key, value []byte) error {
		if len(key) > 0 && !b.ks.IsInternalStorageKey(key) {
			userKey, objectRevision, decodeErr := b.coder.Decode(key)
			if decodeErr != nil {
				return invalidMVCCMetadataError(decodeErr, "decode hash object key")
			}
			boundary, ok := b.coder.RevisionBoundaryForBorder(key)
			if !ok {
				return errors.New("object key has no revision family boundary")
			}
			if familyBoundary != nil && !bytes.Equal(boundary, familyBoundary) {
				if err := flushFamily(); err != nil {
					return err
				}
			}
			familyBoundary = append(familyBoundary[:0], boundary...)
			// Revision-zero entries are per-key indexes, not MVCC values.
			if revision >= 0 && objectRevision > 0 && objectRevision <= uint64(revision) {
				row := hashRow{
					key:      key,
					userKey:  userKey,
					revision: objectRevision,
					value:    value,
				}
				if hasCompactRevision && objectRevision <= compactRevision {
					compactedLatest[string(userKey)] = row
					return nil
				}
				retained = append(retained, row)
			}
		}
		return nil
	})
	if err != nil {
		return HashKVResult{}, err
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

type hashKVPhysicalRow struct {
	key   []byte
	value []byte
}

type hashKVPartitionResult struct {
	rows chan []hashKVPhysicalRow
	done chan error
}

// forEachHashKVRow visits the physical object keyspace in strict ascending
// order. Healthy reads discover TiKV Region partitions and prefetch a bounded
// window in parallel. A protected checkpoint deliberately remains a single
// iterator: its raison d'etre is operation without PD, and partition discovery
// would reintroduce a topology-service dependency into that degraded path.
func (b *backend) forEachHashKVRow(
	ctx context.Context,
	timestamp uint64,
	pinned bool,
	visit func(key, value []byte) error,
) error {
	start, end := b.ks.ObjectKeyspaceStart(), b.ks.ObjectKeyspaceEnd()
	partitions := []storage.Partition{{Start: start, End: end}}
	if !pinned {
		discovered, err := b.kv.GetPartitions(ctx, start, end)
		if err == nil && validHashKVPartitions(discovered, start, end) {
			partitions = discovered
		} else if ctx.Err() != nil {
			return ctx.Err()
		}
		// Partition discovery is an optimization. If PD races an outage after the
		// snapshot timestamp was obtained, retain the old single-iterator path.
	}

	workerCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	results := make([]hashKVPartitionResult, len(partitions))
	startPartition := func(index int) {
		result := hashKVPartitionResult{
			rows: make(chan []hashKVPhysicalRow, 1),
			done: make(chan error, 1),
		}
		results[index] = result
		partition := partitions[index]
		go func() {
			defer close(result.rows)
			result.done <- b.readHashKVPartition(workerCtx, partition, timestamp, result.rows)
		}()
	}

	active := hashKVPartitionConcurrency
	if active > len(partitions) {
		active = len(partitions)
	}
	for index := 0; index < active; index++ {
		startPartition(index)
	}
	for index := range partitions {
		for rows := range results[index].rows {
			for _, row := range rows {
				if err := visit(row.key, row.value); err != nil {
					cancel()
					return err
				}
			}
		}
		if err := <-results[index].done; err != nil {
			cancel()
			return err
		}
		if next := index + active; next < len(partitions) {
			startPartition(next)
		}
	}
	return nil
}

func validHashKVPartitions(partitions []storage.Partition, start, end []byte) bool {
	if len(partitions) == 0 || !bytes.Equal(partitions[0].Start, start) ||
		!bytes.Equal(partitions[len(partitions)-1].End, end) {
		return false
	}
	for index, partition := range partitions {
		if bytes.Compare(partition.Start, partition.End) >= 0 {
			return false
		}
		if index != 0 && !bytes.Equal(partitions[index-1].End, partition.Start) {
			return false
		}
	}
	return true
}

func (b *backend) readHashKVPartition(
	ctx context.Context,
	partition storage.Partition,
	timestamp uint64,
	output chan<- []hashKVPhysicalRow,
) (retErr error) {
	it, err := b.kv.Iter(ctx, partition.Start, partition.End, timestamp, 0)
	if err != nil {
		return err
	}
	defer func() { retErr = errors.Join(retErr, it.Close()) }()

	rows := make([]hashKVPhysicalRow, 0, hashKVPartitionBatchRows)
	bytesBuffered := 0
	flush := func() error {
		if len(rows) == 0 {
			return nil
		}
		select {
		case output <- rows:
			rows = make([]hashKVPhysicalRow, 0, hashKVPartitionBatchRows)
			bytesBuffered = 0
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	for {
		if err := it.Next(ctx); err != nil {
			if errors.Is(err, io.EOF) {
				return flush()
			}
			return err
		}
		key := append([]byte(nil), it.Key()...)
		value := append([]byte(nil), it.Val()...)
		rows = append(rows, hashKVPhysicalRow{key: key, value: value})
		bytesBuffered += len(key) + len(value)
		if len(rows) >= hashKVPartitionBatchRows || bytesBuffered >= hashKVPartitionBatchBytes {
			if err := flush(); err != nil {
				return err
			}
		}
	}
}
