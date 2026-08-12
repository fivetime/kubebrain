// Copyright 2026 ByteDance and/or its affiliates
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package backend

import (
	"encoding/binary"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCorruptAlarmGenerationAdvancesOnIdempotentRearmAndDisarm(t *testing.T) {
	b, ctx := newTxnApplyBackend(t)
	const memberID = uint64(41001)

	require.NoError(t, b.ArmCorrupt(ctx, memberID))
	raw, err := b.InternalGet(ctx, corruptAlarmGenerationKey)
	require.NoError(t, err)
	require.Equal(t, uint64(1), binary.BigEndian.Uint64(raw))

	require.NoError(t, b.ArmCorrupt(ctx, memberID))
	raw, err = b.InternalGet(ctx, corruptAlarmGenerationKey)
	require.NoError(t, err)
	require.Equal(t, uint64(2), binary.BigEndian.Uint64(raw),
		"an idempotent member rearm must still publish a cross-replica generation")

	removed, err := b.DisarmCorrupt(ctx, memberID)
	require.NoError(t, err)
	require.True(t, removed)
	raw, err = b.InternalGet(ctx, corruptAlarmGenerationKey)
	require.NoError(t, err)
	require.Equal(t, uint64(3), binary.BigEndian.Uint64(raw))
}

func TestCorruptAlarmGenerationMalformedFailsClosed(t *testing.T) {
	b, ctx := newTxnApplyBackend(t)
	const memberID = uint64(41002)
	require.NoError(t, b.ArmCorrupt(ctx, memberID))
	require.NoError(t, b.InternalPut(ctx, corruptAlarmGenerationKey, []byte("bad")))

	err := b.ArmCorrupt(ctx, memberID)
	require.ErrorIs(t, err, ErrInvalidAlarmMetadata)
	removed, err := b.DisarmCorrupt(ctx, memberID)
	require.ErrorIs(t, err, ErrInvalidAlarmMetadata)
	require.False(t, removed)
	members, err := b.CorruptAlarms(ctx)
	require.NoError(t, err)
	require.Equal(t, []uint64{memberID}, members)
}
