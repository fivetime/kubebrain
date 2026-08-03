package backend

import (
	"bytes"
	"context"
	"encoding/binary"
	"io"

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
	stored, _, err := b.getInternalVal(ctx, key, modRevision)
	if err == nil {
		if meta, _, ok := decodeValueWithMeta(stored); ok {
			return meta, nil
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
		return EtcdMetadata{CreateRevision: modRevision, Version: 1}, nil
	}
	return meta, err
}

func (b *backend) getEtcdMetadata(ctx context.Context, key []byte, revision uint64) (EtcdMetadata, error) {
	metaKey := b.etcdMetadataUserKey(key)
	// The legacy metadata namespace reused the unescaped object-key '$'
	// delimiter. A metadata key for an arbitrary-byte extension of key can sort
	// inside this reverse interval, so decode each row and skip foreign keys
	// instead of trusting the first physical result.
	iter, err := b.kv.Iter(ctx, b.coder.EncodeObjectKey(metaKey, revision), b.coder.EncodeObjectKey(metaKey, 0), 0, 0)
	if err != nil {
		return EtcdMetadata{}, err
	}
	defer iter.Close()
	for {
		if err := iter.Next(ctx); err != nil {
			if err == io.EOF {
				return EtcdMetadata{}, storage.ErrKeyNotFound
			}
			return EtcdMetadata{}, err
		}
		userKey, candidateRevision, decodeErr := b.coder.Decode(iter.Key())
		if decodeErr != nil {
			return EtcdMetadata{}, decodeErr
		}
		if candidateRevision == 0 || !bytes.Equal(userKey, metaKey) {
			continue
		}
		return decodeEtcdMetadata(iter.Val())
	}
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
		return EtcdMetadata{}, errors.Errorf("invalid etcd metadata length %d", len(raw))
	}
	return EtcdMetadata{
		CreateRevision: binary.BigEndian.Uint64(raw[:8]),
		Version:        binary.BigEndian.Uint64(raw[8:]),
	}, nil
}
