package backend

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"math"

	"github.com/pkg/errors"

	"github.com/kubewharf/kubebrain/pkg/storage"
)

// internalKeyspacePrefix is the reserved namespace for KubeBrain's own
// bookkeeping keys, kept collision-free from user (kube-apiserver) keys by the
// leading NUL byte — real k8s keys start with '/'. Every internal keyspace lives
// under it (etcd metadata here; lease records in the etcd server layer). These
// are all latest-only state: only the newest version of each internal key is
// ever read, so the whole namespace is folded into compaction (see
// getCompactBorders) to retire superseded versions and tombstones (#6/#15/#38).
var internalKeyspacePrefix = []byte("\x00kubebrain/")

var etcdMetadataPrefix = []byte("\x00kubebrain/etcdmeta/")

type EtcdMetadata struct {
	CreateRevision uint64
	Version        uint64
	// Lease is the lease ID bound to this specific MVCC version (0 if none). It
	// is inlined per-version so historical reads, prevKv, and delete events
	// report the lease the key held at that revision, not just its current
	// binding (review #9). Only v2 value envelopes carry it; legacy/v1 versions
	// decode as 0 and fall back to the live key->lease index.
	Lease int64
}

func (b *backend) GetEtcdMetadata(ctx context.Context, key []byte, modRevision uint64) (EtcdMetadata, error) {
	if modRevision == 0 {
		return EtcdMetadata{}, nil
	}
	// Prefer inline metadata carried in the object value (approach A).
	stored, storedRevision, err := b.getInternalVal(ctx, key, modRevision)
	if err == nil {
		// Historical lookup returns the newest object at or before the requested
		// revision. Inline metadata is authoritative only for that exact object
		// version; accepting a predecessor here would repeat the stale legacy-row
		// join that the exact metadata lookup below deliberately avoids.
		if storedRevision == modRevision {
			meta, _, ok, decodeErr := DecodeInlineValueChecked(stored)
			if decodeErr != nil {
				return EtcdMetadata{}, decodeErr
			}
			if ok {
				return meta, nil
			}
		}
	} else if !errors.Is(err, storage.ErrKeyNotFound) {
		// A transient storage failure is NOT "no inline metadata": sliding into
		// the legacy fallback here can fabricate {CreateRevision: modRevision,
		// Version: 1} for a key that has perfectly good inline metadata, silently
		// resetting create_revision/version on the next update (review #51).
		// Surface the error; the caller retries or fails the operation.
		return EtcdMetadata{}, err
	}
	// Legacy fallback: the separate etcdmeta keyspace (data written before A).
	meta, err := b.getEtcdMetadata(ctx, key, modRevision)
	if errors.Is(err, storage.ErrKeyNotFound) {
		if recovered, proven, recoverErr := b.recoverRetainedEtcdMetadata(ctx, key, modRevision); recoverErr != nil {
			return EtcdMetadata{}, recoverErr
		} else if proven {
			return recovered, nil
		}
		return EtcdMetadata{CreateRevision: modRevision, Version: 1}, nil
	}
	return meta, err
}

// recoverRetainedEtcdMetadata reconstructs pre-metadata create/version fields
// when the retained object family proves a complete generation. Compaction may
// leave only an arbitrary live anchor, whose original version is unknowable;
// that case returns proven=false instead of manufacturing precision. A visible
// tombstone, an inline envelope, or a first live version strictly newer than the
// fixed compact watermark establishes an authoritative generation boundary.
func (b *backend) recoverRetainedEtcdMetadata(
	ctx context.Context, key []byte, modRevision uint64,
) (EtcdMetadata, bool, error) {
	compactRevision, err := b.loadCompactRevision(ctx)
	if err != nil {
		return EtcdMetadata{}, false, err
	}
	if modRevision == math.MaxUint64 {
		return EtcdMetadata{}, false, nil
	}
	endRevision := modRevision + 1
	timestamp, _ := storage.SnapshotTimestampFromContext(ctx)
	legacyMetadata, err := b.loadRetainedLegacyEtcdMetadata(ctx, key, endRevision, timestamp)
	if err != nil {
		return EtcdMetadata{}, false, err
	}
	iter, err := b.kv.Iter(
		ctx,
		b.coder.EncodeObjectKey(key, 0),
		b.coder.EncodeObjectKey(key, endRevision),
		timestamp, 0,
	)
	if err != nil {
		return EtcdMetadata{}, false, err
	}
	defer iter.Close()

	var current EtcdMetadata
	anchored := false
	afterTombstone := false
	unknownLiveGeneration := false
	for {
		if err := iter.Next(ctx); err != nil {
			if errors.Is(err, io.EOF) {
				return EtcdMetadata{}, false, nil
			}
			return EtcdMetadata{}, false, err
		}
		userKey, revision, decodeErr := b.coder.Decode(iter.Key())
		if decodeErr != nil {
			return EtcdMetadata{}, false, invalidMVCCMetadataError(decodeErr, "decode retained metadata recovery object key")
		}
		if revision == 0 || revision > modRevision || !bytes.Equal(userKey, key) {
			continue
		}
		stored := iter.Val()
		if bytes.Equal(stored, tombStoneBytes) {
			anchored = false
			afterTombstone = true
			unknownLiveGeneration = false
			continue
		}
		inline, _, inlined, inlineErr := DecodeInlineValueChecked(stored)
		if inlineErr != nil {
			return EtcdMetadata{}, false, inlineErr
		}
		legacy, hasLegacy := legacyMetadata[revision]
		if inlined && hasLegacy &&
			(inline.CreateRevision != legacy.CreateRevision || inline.Version != legacy.Version) {
			return EtcdMetadata{}, false, invalidMVCCMetadataError(
				fmt.Errorf("inline and legacy metadata disagree at revision %d", revision),
				"recover metadata for key %q", key,
			)
		}
		authoritative := inline
		hasAuthoritative := inlined
		if !hasAuthoritative && hasLegacy {
			authoritative = legacy
			hasAuthoritative = true
		}
		switch {
		case hasAuthoritative:
			if anchored && (authoritative.CreateRevision != current.CreateRevision || authoritative.Version != current.Version+1) {
				return EtcdMetadata{}, false, invalidMVCCMetadataError(
					fmt.Errorf("retained metadata discontinuity at revision %d", revision),
					"recover metadata for key %q", key,
				)
			}
			current = authoritative
			anchored = true
			afterTombstone = false
			unknownLiveGeneration = false
		case anchored:
			current.Version++
		case afterTombstone || (revision > compactRevision && !unknownLiveGeneration):
			current = EtcdMetadata{CreateRevision: revision, Version: 1}
			anchored = true
			afterTombstone = false
			unknownLiveGeneration = false
		default:
			unknownLiveGeneration = true
		}
		if revision == modRevision {
			if !anchored {
				return EtcdMetadata{}, false, nil
			}
			return current, true, nil
		}
	}
}

func (b *backend) loadRetainedLegacyEtcdMetadata(
	ctx context.Context, key []byte, endRevision, timestamp uint64,
) (map[uint64]EtcdMetadata, error) {
	metaKey := b.etcdMetadataUserKey(key)
	iter, err := b.kv.Iter(
		ctx,
		b.coder.EncodeObjectKey(metaKey, 0),
		b.coder.EncodeObjectKey(metaKey, endRevision),
		timestamp, 0,
	)
	if err != nil {
		return nil, err
	}
	defer iter.Close()
	result := make(map[uint64]EtcdMetadata)
	for {
		if err := iter.Next(ctx); err != nil {
			if errors.Is(err, io.EOF) {
				return result, nil
			}
			return nil, err
		}
		userKey, revision, decodeErr := b.coder.Decode(iter.Key())
		if decodeErr != nil {
			return nil, invalidMVCCMetadataError(decodeErr, "decode retained legacy metadata object key")
		}
		if revision == 0 || revision >= endRevision || !bytes.Equal(userKey, metaKey) {
			continue
		}
		meta, decodeErr := decodeEtcdMetadata(iter.Val())
		if decodeErr != nil {
			return nil, decodeErr
		}
		result[revision] = meta
	}
}

func (b *backend) getEtcdMetadata(ctx context.Context, key []byte, revision uint64) (EtcdMetadata, error) {
	metaKey := b.etcdMetadataUserKey(key)
	// Every legacy metadata row describes exactly one object revision. Borrowing
	// the nearest predecessor silently applies stale Version/CreateRevision when
	// the target row is absent. The exact physical key is collision-free even
	// though range scans over the old unescaped '$' namespace are not.
	raw, err := b.snapshotGet(ctx, b.coder.EncodeObjectKey(metaKey, revision))
	if err != nil {
		return EtcdMetadata{}, err
	}
	return decodeEtcdMetadata(raw)
}

func (b *backend) putEtcdMetadata(batch storage.BatchWrite, key []byte, revision uint64, meta EtcdMetadata) {
	batch.Put(b.coder.EncodeObjectKey(b.etcdMetadataUserKey(key), revision), encodeEtcdMetadata(meta), 0)
}

func (b *backend) etcdMetadataUserKey(key []byte) []byte {
	metaKey := make([]byte, 0, len(etcdMetadataPrefix)+len(key))
	metaKey = append(metaKey, etcdMetadataPrefix...)
	metaKey = append(metaKey, key...)
	return metaKey
}

func encodeEtcdMetadata(meta EtcdMetadata) []byte {
	buf := make([]byte, 16)
	binary.BigEndian.PutUint64(buf[:8], meta.CreateRevision)
	binary.BigEndian.PutUint64(buf[8:], meta.Version)
	return buf
}

func decodeEtcdMetadata(raw []byte) (EtcdMetadata, error) {
	if len(raw) != 16 {
		return EtcdMetadata{}, errors.Wrapf(ErrInvalidMVCCMetadata, "invalid etcd metadata length %d", len(raw))
	}
	meta := EtcdMetadata{
		CreateRevision: binary.BigEndian.Uint64(raw[:8]),
		Version:        binary.BigEndian.Uint64(raw[8:]),
	}
	if err := validateEtcdMetadata(meta, "legacy etcd metadata"); err != nil {
		return EtcdMetadata{}, err
	}
	return meta, nil
}

func validateEtcdMetadata(meta EtcdMetadata, source string) error {
	switch {
	case meta.CreateRevision == 0:
		return fmt.Errorf("%w: %s create revision is zero", ErrInvalidMVCCMetadata, source)
	case meta.CreateRevision > math.MaxInt64:
		return fmt.Errorf("%w: %s create revision %d exceeds MaxInt64", ErrInvalidMVCCMetadata, source, meta.CreateRevision)
	case meta.Version == 0:
		return fmt.Errorf("%w: %s version is zero", ErrInvalidMVCCMetadata, source)
	case meta.Version > math.MaxInt64:
		return fmt.Errorf("%w: %s version %d exceeds MaxInt64", ErrInvalidMVCCMetadata, source, meta.Version)
	default:
		return nil
	}
}
