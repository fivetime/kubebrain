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
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/kubewharf/kubebrain/pkg/storage"
)

func TestIsCorruptAlarmFenceError(t *testing.T) {
	for _, err := range []error{ErrCorruptAlarmActive, ErrCorruptAlarmChanged, ErrInvalidAlarmMetadata} {
		require.True(t, isCorruptAlarmFenceError(fmt.Errorf("wrapped: %w", err)))
	}
	require.False(t, isCorruptAlarmFenceError(errors.New("transient orphan repair failure")))
	require.False(t, isCorruptAlarmFenceError(nil))
}

func TestCorruptAlarmFenceShardOffsetIsStableAndIdentityScoped(t *testing.T) {
	require.Zero(t, corruptAlarmFenceShardOffset(""))

	identities := []string{"kubebrain-0", "kubebrain-1", "kubebrain-2"}
	seen := make(map[uint64]string, len(identities))
	for _, identity := range identities {
		offset := corruptAlarmFenceShardOffset(identity)
		require.Less(t, offset, uint64(corruptAlarmFenceShardCount))
		require.Equal(t, offset, corruptAlarmFenceShardOffset(identity))
		if previous, exists := seen[offset]; exists {
			t.Fatalf("test identities %q and %q collide on corrupt fence shard %02x", previous, identity, offset)
		}
		seen[offset] = identity
	}
}

func TestCorruptAlarmGenerationAdvancesOnIdempotentRearmAndDisarm(t *testing.T) {
	b, ctx := newTxnApplyBackend(t)
	const memberID = uint64(41001)
	require.NoError(t, b.ValidateCorruptAlarmMetadata(ctx), "legacy tenant with no fence keys is valid")

	require.NoError(t, b.ArmCorrupt(ctx, memberID))
	raw, err := b.InternalGet(ctx, corruptAlarmGenerationKey)
	require.NoError(t, err)
	require.Equal(t, uint64(1), binary.BigEndian.Uint64(raw))
	control, err := b.InternalGet(ctx, corruptAlarmFenceControlKey)
	require.NoError(t, err)
	require.Equal(t, []byte{1}, control)
	for shard := uint64(0); shard < corruptAlarmFenceShardCount; shard++ {
		guard, guardErr := b.InternalGet(ctx, corruptAlarmFenceShardKey(shard))
		require.NoError(t, guardErr)
		require.Equal(t, raw, guard)
	}
	require.NoError(t, b.ValidateCorruptAlarmMetadata(ctx))

	require.NoError(t, b.ArmCorrupt(ctx, memberID))
	raw, err = b.InternalGet(ctx, corruptAlarmGenerationKey)
	require.NoError(t, err)
	require.Equal(t, uint64(2), binary.BigEndian.Uint64(raw),
		"an idempotent member rearm must still publish a cross-replica generation")
	for shard := uint64(0); shard < corruptAlarmFenceShardCount; shard++ {
		guard, guardErr := b.InternalGet(ctx, corruptAlarmFenceShardKey(shard))
		require.NoError(t, guardErr)
		require.Equal(t, raw, guard)
	}

	removed, err := b.DisarmCorrupt(ctx, memberID)
	require.NoError(t, err)
	require.True(t, removed)
	raw, err = b.InternalGet(ctx, corruptAlarmGenerationKey)
	require.NoError(t, err)
	require.Equal(t, uint64(3), binary.BigEndian.Uint64(raw))
}

func TestCorruptAlarmFenceShardCorruptionFailsLeadershipValidationAndWrites(t *testing.T) {
	b, ctx := newTxnApplyBackend(t)
	require.NoError(t, b.ArmCorrupt(ctx, 41003))
	removed, err := b.DisarmCorrupt(ctx, 41003)
	require.NoError(t, err)
	require.True(t, removed)
	corruptShard := b.corruptAlarmFenceShard.Load() % corruptAlarmFenceShardCount
	require.NoError(t, b.InternalPut(ctx, corruptAlarmFenceShardKey(corruptShard), []byte("wrong")))

	err = b.ValidateCorruptAlarmMetadata(ctx)
	require.ErrorIs(t, err, ErrInvalidAlarmMetadata)
	require.ErrorContains(t, err, fmt.Sprintf("shard %02x generation mismatch", corruptShard))
	_, _, err = b.TxnApply(ctx, []TxnWriteOp{{Key: []byte(prefix + "/bad-corrupt-fence"), Value: []byte("x")}}, nil)
	require.ErrorIs(t, err, ErrInvalidAlarmMetadata)
	_, err = b.kv.Get(ctx, b.coder.EncodeRevisionKey([]byte(prefix+"/bad-corrupt-fence")))
	require.ErrorIs(t, err, storage.ErrKeyNotFound)
}

func TestCorruptAlarmFenceMigratesExistingGenerationAtomically(t *testing.T) {
	b, ctx := newTxnApplyBackend(t)
	const memberID = uint64(41004)
	require.NoError(t, b.InternalPut(ctx, corruptAlarmKey, []byte("[41004]")))
	generation := make([]byte, 8)
	binary.BigEndian.PutUint64(generation, 7)
	require.NoError(t, b.InternalPut(ctx, corruptAlarmGenerationKey, generation))
	require.NoError(t, b.ValidateCorruptAlarmMetadata(ctx), "A4493 generation without shards is a valid migration source")

	require.NoError(t, b.ArmCorrupt(ctx, memberID))
	next, err := b.InternalGet(ctx, corruptAlarmGenerationKey)
	require.NoError(t, err)
	require.Equal(t, uint64(8), binary.BigEndian.Uint64(next))
	require.NoError(t, b.ValidateCorruptAlarmMetadata(ctx))
	for shard := uint64(0); shard < corruptAlarmFenceShardCount; shard++ {
		guard, guardErr := b.InternalGet(ctx, corruptAlarmFenceShardKey(shard))
		require.NoError(t, guardErr)
		require.Equal(t, next, guard)
	}
}

func TestCorruptAlarmGenerationMalformedFailsClosed(t *testing.T) {
	max := make([]byte, 8)
	binary.BigEndian.PutUint64(max, ^uint64(0))
	for _, test := range []struct {
		name string
		raw  []byte
	}{
		{name: "invalid length", raw: []byte("bad")},
		{name: "zero", raw: make([]byte, 8)},
		{name: "exhausted", raw: max},
	} {
		t.Run(test.name, func(t *testing.T) {
			b, ctx := newTxnApplyBackend(t)
			const memberID = uint64(41002)
			require.NoError(t, b.ArmCorrupt(ctx, memberID))
			require.NoError(t, b.InternalPut(ctx, corruptAlarmGenerationKey, test.raw))

			err := b.ArmCorrupt(ctx, memberID)
			require.ErrorIs(t, err, ErrInvalidAlarmMetadata)
			removed, err := b.DisarmCorrupt(ctx, memberID)
			require.ErrorIs(t, err, ErrInvalidAlarmMetadata)
			require.False(t, removed)
			removed, err = b.DisarmCorrupt(ctx, memberID+1)
			require.ErrorIs(t, err, ErrInvalidAlarmMetadata,
				"a wrong-member no-op must not certify malformed safety metadata")
			require.False(t, removed)
			_, err = b.CorruptAlarms(ctx)
			require.ErrorIs(t, err, ErrInvalidAlarmMetadata,
				"alarm reads and write gates must not ignore an unusable generation")
			members, _, _, err := b.readCorruptAlarms(ctx)
			require.NoError(t, err)
			require.Equal(t, []uint64{memberID}, members, "failed validation must not erase the alarm owner")
		})
	}
}
