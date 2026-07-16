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
)

func TestInternalKVDoesNotAdvanceUserRevision(t *testing.T) {
	b, ctx := newTxnApplyBackend(t)
	before := b.GetCurrentRevision()
	require.NoError(t, b.InternalPut(ctx, []byte("leases/42"), []byte("meta")))
	require.Equal(t, before, b.GetCurrentRevision())

	got, err := b.InternalGet(ctx, []byte("leases/42"))
	require.NoError(t, err)
	require.Equal(t, []byte("meta"), got)
	all, err := b.InternalRange(context.Background(), []byte("leases/"))
	require.NoError(t, err)
	require.Equal(t, map[string][]byte{"leases/42": []byte("meta")}, all)

	require.NoError(t, b.InternalDelete(ctx, []byte("leases/42")))
	require.Equal(t, before, b.GetCurrentRevision())
}
