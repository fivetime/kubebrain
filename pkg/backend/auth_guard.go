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

	"github.com/kubewharf/kubebrain/pkg/storage"
)

// ErrInternalWriteGuardConflict means internal metadata changed after a user
// write was authorized but before its storage transaction committed.
var ErrInternalWriteGuardConflict = errors.New("internal write guard changed")

type internalWriteGuard struct {
	key      []byte
	expected []byte
}

type internalWriteGuardContextKey struct{}

// WithInternalWriteGuard requires key to retain expected through the user
// write's atomic storage commit. The key is in the backend's internal keyspace.
func WithInternalWriteGuard(ctx context.Context, key, expected []byte) context.Context {
	return context.WithValue(ctx, internalWriteGuardContextKey{}, internalWriteGuard{
		key: append([]byte(nil), key...), expected: append([]byte(nil), expected...),
	})
}

func (b *backend) commitUserBatch(ctx context.Context, batch storage.BatchWrite) error {
	guard, guarded := ctx.Value(internalWriteGuardContextKey{}).(internalWriteGuard)
	var encodedGuard []byte
	if guarded {
		encodedGuard = b.ks.EncodeInternalKey(guard.key)
		// A no-op CAS adds the internal key to the same transaction's conflict
		// set without changing its value.
		batch.CAS(encodedGuard, guard.expected, guard.expected, 0)
	}
	err := batch.Commit(ctx)
	if !guarded || err == nil {
		return err
	}
	var conflict *storage.Conflict
	if errors.As(err, &conflict) && bytes.Equal(conflict.Key, encodedGuard) {
		return ErrInternalWriteGuardConflict
	}
	return err
}
