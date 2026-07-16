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
	"testing"

	"github.com/stretchr/testify/require"

	proto "github.com/kubewharf/kubebrain-client/api/v2rpc"
)

func TestInternalWriteGuardRejectsStaleAuthorizedWrite(t *testing.T) {
	b, ctx := newTxnApplyBackend(t)
	guardKey := []byte("auth/config")
	v1, v2 := []byte("revision-1"), []byte("revision-2")
	require.NoError(t, b.InternalPut(ctx, guardKey, v1))
	updateKey := []byte(prefix + "/auth-guard/update")
	deleteKey := []byte(prefix + "/auth-guard/delete")
	rangeKey := []byte(prefix + "/auth-guard/range/a")
	seed := func(key []byte) uint64 {
		response, err := b.Create(ctx, &proto.CreateRequest{Key: key, Value: []byte("original")})
		require.NoError(t, err)
		require.True(t, response.Succeeded)
		return response.Header.Revision
	}
	updateRevision := seed(updateKey)
	deleteRevision := seed(deleteKey)
	rangeRevision := seed(rangeKey)
	guarded := WithInternalWriteGuard(ctx, guardKey, v1)
	require.NoError(t, b.InternalCAS(ctx, []InternalCASOp{{
		Key: guardKey, Expected: v1, ExpectedExists: true, Value: v2,
	}}))

	key := []byte(prefix + "/auth-guard/create")
	_, err := b.Create(guarded, &proto.CreateRequest{Key: key, Value: []byte("must-not-commit")})
	require.ErrorIs(t, err, ErrInternalWriteGuardConflict)
	value, revision := liveValue(t, b, context.Background(), key)
	require.Empty(t, value)
	require.Zero(t, revision)

	_, err = b.Update(guarded, &proto.UpdateRequest{Kv: &proto.KeyValue{
		Key: updateKey, Value: []byte("changed"), Revision: updateRevision,
	}})
	require.ErrorIs(t, err, ErrInternalWriteGuardConflict)
	value, revision = liveValue(t, b, context.Background(), updateKey)
	require.Equal(t, "original", value)
	require.Equal(t, updateRevision, revision)

	_, err = b.Delete(guarded, &proto.DeleteRequest{Key: deleteKey, Revision: deleteRevision})
	require.ErrorIs(t, err, ErrInternalWriteGuardConflict)
	value, revision = liveValue(t, b, context.Background(), deleteKey)
	require.Equal(t, "original", value)
	require.Equal(t, deleteRevision, revision)

	_, err = b.DeleteRange(guarded, []*proto.KeyValue{{Key: rangeKey, Revision: rangeRevision}})
	require.ErrorIs(t, err, ErrInternalWriteGuardConflict)
	value, revision = liveValue(t, b, context.Background(), rangeKey)
	require.Equal(t, "original", value)
	require.Equal(t, rangeRevision, revision)

	_, _, err = b.TxnApply(guarded, []TxnWriteOp{{Key: []byte(prefix + "/auth-guard/txn"), Value: []byte("no")}}, nil)
	require.ErrorIs(t, err, ErrInternalWriteGuardConflict)
	value, revision = liveValue(t, b, context.Background(), []byte(prefix+"/auth-guard/txn"))
	require.Empty(t, value)
	require.Zero(t, revision)
}

func TestInternalWriteGuardAllowsCurrentAuthorizedWrite(t *testing.T) {
	b, ctx := newTxnApplyBackend(t)
	guardKey := []byte("auth/config")
	expected := []byte("revision-1")
	require.NoError(t, b.InternalPut(ctx, guardKey, expected))

	key := []byte(prefix + "/auth-guard/current")
	guarded := WithInternalWriteGuard(ctx, guardKey, expected)
	response, err := b.Create(guarded, &proto.CreateRequest{Key: key, Value: []byte("committed")})
	require.NoError(t, err)
	require.True(t, response.Succeeded)

	value, revision := liveValue(t, b, context.Background(), key)
	require.Equal(t, "committed", value)
	require.NotZero(t, revision)
	actual, err := b.InternalGet(ctx, guardKey)
	require.NoError(t, err)
	require.Equal(t, expected, actual)
}
