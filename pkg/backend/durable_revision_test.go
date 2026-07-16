// Copyright 2026 ByteDance and/or its affiliates
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

package backend

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	proto "github.com/kubewharf/kubebrain-client/api/v2rpc"
)

func TestDurableRevisionTracksResolvedUserWrites(t *testing.T) {
	b, ctx := newTxnApplyBackend(t)
	resp, err := b.Create(ctx, &proto.CreateRequest{
		Key:   []byte(prefix + "/durable-revision/key"),
		Value: []byte("value"),
	})
	require.NoError(t, err)
	require.True(t, resp.Succeeded)

	require.Eventually(t, func() bool {
		revision, getErr := b.GetDurableRevision(ctx)
		return getErr == nil && revision >= resp.Header.Revision
	}, time.Second, time.Millisecond)
}

func TestDurableRevisionNeverMovesBackward(t *testing.T) {
	b, _ := newTxnApplyBackend(t)
	b.persistDurableRevision(200)
	b.persistDurableRevision(150)
	revision, err := b.GetDurableRevision(context.Background())
	require.NoError(t, err)
	require.Equal(t, uint64(200), revision)
}
