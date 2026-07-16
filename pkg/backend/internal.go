// Copyright 2026 ByteDance and/or its affiliates
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0

package backend

import (
	"context"
	"io"

	"github.com/kubewharf/kubebrain/pkg/storage"
)

func rawPrefixEnd(prefix []byte) []byte {
	end := append([]byte(nil), prefix...)
	for i := len(end) - 1; i >= 0; i-- {
		if end[i] != 0xff {
			end[i]++
			return end[:i+1]
		}
	}
	return nil
}

func (b *backend) InternalGet(ctx context.Context, key []byte) ([]byte, error) {
	return b.kv.Get(ctx, b.ks.EncodeInternalKey(key))
}

func (b *backend) InternalRange(ctx context.Context, prefix []byte) (map[string][]byte, error) {
	start := b.ks.EncodeInternalKey(prefix)
	it, err := b.kv.Iter(ctx, start, rawPrefixEnd(start), 0, 0)
	if err != nil {
		return nil, err
	}
	defer it.Close()
	out := make(map[string][]byte)
	for {
		err = it.Next(ctx)
		if err == io.EOF {
			return out, nil
		}
		if err != nil {
			return nil, err
		}
		rawKey := it.Key()
		logicalKey := rawKey[len(start)-len(prefix):]
		out[string(logicalKey)] = append([]byte(nil), it.Val()...)
	}
}

func (b *backend) InternalPut(ctx context.Context, key, value []byte) error {
	unlock := b.lockLogicalWrite(ctx)
	defer unlock()
	if err := b.fenceAdmit(ctx); err != nil {
		return err
	}
	batch := b.kv.BeginBatchWrite()
	batch.Put(b.ks.EncodeInternalKey(key), value, 0)
	return batch.Commit(ctx)
}

func (b *backend) InternalDelete(ctx context.Context, key []byte) error {
	unlock := b.lockLogicalWrite(ctx)
	defer unlock()
	if err := b.fenceAdmit(ctx); err != nil {
		return err
	}
	batch := b.kv.BeginBatchWrite()
	batch.Del(b.ks.EncodeInternalKey(key))
	err := batch.Commit(ctx)
	if err == storage.ErrKeyNotFound {
		return nil
	}
	return err
}
