package backend

import (
	"context"
	"encoding/binary"
	"io"

	"github.com/pkg/errors"

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
