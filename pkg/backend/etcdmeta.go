package backend

import (
	"bytes"
	"context"
	"encoding/binary"
	"io"

	"github.com/pkg/errors"

	proto "github.com/kubewharf/kubebrain-client/api/v2rpc"

	"github.com/kubewharf/kubebrain/pkg/storage"
)

var etcdMetadataPrefix = []byte("\x00kubebrain/etcdmeta/")

type EtcdMetadata struct {
	CreateRevision uint64
	Version        uint64
}

func (b *backend) GetEtcdMetadata(ctx context.Context, key []byte, modRevision uint64) (EtcdMetadata, error) {
	if modRevision == 0 {
		return EtcdMetadata{}, nil
	}
	meta, err := b.getEtcdMetadata(ctx, key, modRevision)
	if errors.Is(err, storage.ErrKeyNotFound) {
		return EtcdMetadata{CreateRevision: modRevision, Version: 1}, nil
	}
	return meta, err
}

func (b *backend) getEtcdMetadata(ctx context.Context, key []byte, revision uint64) (EtcdMetadata, error) {
	metaKey := b.etcdMetadataUserKey(key)
	iter, err := b.kv.Iter(ctx, b.coder.EncodeObjectKey(metaKey, revision), b.coder.EncodeObjectKey(metaKey, 0), 0, 1)
	if err != nil {
		return EtcdMetadata{}, err
	}
	defer iter.Close()
	if err := iter.Next(ctx); err != nil {
		if err == io.EOF {
			return EtcdMetadata{}, storage.ErrKeyNotFound
		}
		return EtcdMetadata{}, err
	}
	return decodeEtcdMetadata(iter.Val())
}

// GetEtcdMetadataBatch fetches create_revision/version for a page of keys in a
// single MVCC range scan over the metadata keyspace at readRevision, instead of
// one point read per key. Because every returned object version is the latest
// <= readRevision, its metadata is also the latest <= readRevision, so a single
// latest-per-key range scan matches. Returns a map keyed by the user key; keys
// with no metadata are simply absent (caller falls back).
func (b *backend) GetEtcdMetadataBatch(ctx context.Context, keys [][]byte, readRevision uint64) (map[string]EtcdMetadata, error) {
	if len(keys) == 0 || readRevision == 0 {
		return nil, nil
	}
	minKey, maxKey := keys[0], keys[0]
	for _, k := range keys[1:] {
		if bytes.Compare(k, minKey) < 0 {
			minKey = k
		}
		if bytes.Compare(k, maxKey) > 0 {
			maxKey = k
		}
	}
	// End must sort after every stored version of maxKey. The object-key encoding
	// uses '$' as the key/revision delimiter (0x24 > 0x00), so maxKey+"\x00"
	// would sort BEFORE maxKey's versions; use PrefixEnd(maxKey) to include them.
	resp, err := b.List(ctx, &proto.RangeRequest{
		Key:      b.etcdMetadataUserKey(minKey),
		End:      b.etcdMetadataUserKey(PrefixEnd(maxKey)),
		Revision: readRevision,
		Limit:    int64(len(keys)),
	})
	if err != nil {
		return nil, err
	}
	out := make(map[string]EtcdMetadata, len(resp.Kvs))
	for _, kv := range resp.Kvs {
		if !bytes.HasPrefix(kv.Key, etcdMetadataPrefix) {
			continue
		}
		userKey := kv.Key[len(etcdMetadataPrefix):]
		meta, err := decodeEtcdMetadata(kv.Value)
		if err != nil {
			continue
		}
		out[string(userKey)] = meta
	}
	return out, nil
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
