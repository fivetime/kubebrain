// Copyright 2026 ByteDance and/or its affiliates
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0

package backend

import (
	"bytes"
	"context"
	"errors"
	"io"

	"github.com/kubewharf/kubebrain/pkg/storage"
)

// InternalCASOp is one exact-value guarded internal metadata mutation.
// ExpectedExists=false asserts that Key is absent. Delete=true removes an
// existing key; deleting an expected-absent key is a guarded no-op.
type InternalCASOp struct {
	Key            []byte
	Value          []byte
	Expected       []byte
	ExpectedExists bool
	Delete         bool
}

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

func (b *backend) InternalCAS(ctx context.Context, ops []InternalCASOp) error {
	if len(ops) == 0 {
		return nil
	}
	unlock := b.lockLogicalWrite(ctx)
	defer unlock()
	if err := b.fenceAdmit(ctx); err != nil {
		return err
	}

	type prepared struct {
		op      InternalCASOp
		encoded []byte
		current []byte
		missing bool
	}
	preps := make([]prepared, 0, len(ops))
	seen := make(map[string]struct{}, len(ops))
	for _, op := range ops {
		encoded := b.ks.EncodeInternalKey(op.Key)
		if _, duplicate := seen[string(encoded)]; duplicate {
			return errors.New("duplicate internal CAS key")
		}
		seen[string(encoded)] = struct{}{}

		current, err := b.kv.Get(ctx, encoded)
		switch {
		case errors.Is(err, storage.ErrKeyNotFound):
			if op.ExpectedExists {
				return storage.ErrCASFailed
			}
			preps = append(preps, prepared{op: op, encoded: encoded, missing: true})
		case err != nil:
			return err
		default:
			if !op.ExpectedExists || !bytes.Equal(current, op.Expected) {
				return storage.ErrCASFailed
			}
			preps = append(preps, prepared{op: op, encoded: encoded, current: current})
		}
	}

	batch := b.kv.BeginBatchWrite()
	for _, prep := range preps {
		switch {
		case prep.missing && prep.op.Delete:
			// The asserted-absent delete is already satisfied.
		case prep.missing:
			batch.PutIfNotExist(prep.encoded, prep.op.Value, 0)
		case prep.op.Delete:
			batch.CAS(prep.encoded, []byte{0}, prep.current, 0)
			batch.Del(prep.encoded)
		default:
			batch.CAS(prep.encoded, prep.op.Value, prep.current, 0)
		}
	}
	return batch.Commit(ctx)
}
