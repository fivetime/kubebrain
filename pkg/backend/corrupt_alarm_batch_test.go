package backend

import (
	"bytes"
	"context"
	"errors"
	"testing"

	"github.com/kubewharf/kubebrain/pkg/storage"
	"github.com/stretchr/testify/require"
)

// Deliberately expose only Get: a concurrent alarm change must still force
// the legacy seqlock to retry instead of accepting the old member set.
type changingCorruptAlarmStore struct {
	storage.KvStorage
	generationKey                []byte
	before, after                []byte
	generationReads, memberReads int
}

func (s *changingCorruptAlarmStore) Get(_ context.Context, key []byte) ([]byte, error) {
	if bytes.Equal(key, s.generationKey) {
		s.generationReads++
		if s.generationReads == 1 {
			return s.before, nil
		}
		return s.after, nil
	}
	s.memberReads++
	if s.memberReads == 1 {
		return []byte(`[1]`), nil
	}
	return []byte(`[9]`), nil
}

func TestCorruptAlarmBatchFallbackRetriesGenerationChange(t *testing.T) {
	base, ctx := newQuotaBackend(t, 100)
	before, err := encodeNextCorruptAlarmGeneration(1)
	require.NoError(t, err)
	after, err := encodeNextCorruptAlarmGeneration(2)
	require.NoError(t, err)
	s := &changingCorruptAlarmStore{KvStorage: base.kv,
		generationKey: base.ks.EncodeInternalKey(corruptAlarmGenerationKey), before: before, after: after}
	b := &backend{ks: base.ks, kv: s}
	members, raw, exists, err := b.readStableCorruptAlarmState(ctx)
	require.NoError(t, err)
	require.Equal(t, []uint64{9}, members)
	require.Equal(t, after, raw)
	require.True(t, exists)
	require.Equal(t, 4, s.generationReads)
	require.Equal(t, 2, s.memberReads)
}

func TestCorruptAlarmBatchMatchesFallback(t *testing.T) {
	generation, err := encodeNextCorruptAlarmGeneration(4)
	require.NoError(t, err)
	for _, tc := range []struct {
		name                string
		generation, members []byte
		err                 error
	}{
		{name: "absent"},
		{name: "armed", generation: generation, members: []byte(`[1,9]`)},
		{name: "disarmed", generation: generation},
		{name: "legacy members", members: []byte(`[1]`)},
		{name: "empty members", generation: generation, members: []byte(`[]`)},
		{name: "unordered", generation: generation, members: []byte(`[9,1]`)},
		{name: "duplicate", generation: generation, members: []byte(`[1,1]`)},
		{name: "null", generation: generation, members: []byte(`null`)},
		{name: "trailing JSON", generation: generation, members: []byte(`[] []`)},
		{name: "empty value", generation: generation, members: []byte{}},
		{name: "generation error first", generation: []byte{1}, members: []byte(`null`)},
		{name: "zero generation", generation: make([]byte, 8)},
		{name: "canceled", err: context.Canceled},
		{name: "unavailable", err: storage.ErrUnavailable},
	} {
		t.Run(tc.name, func(t *testing.T) {
			base, ctx := newQuotaBackend(t, 100)
			b := &backend{ks: base.ks}
			keys := [][]byte{b.ks.EncodeInternalKey(corruptAlarmGenerationKey), b.ks.EncodeInternalKey(corruptAlarmKey)}
			values := map[string][]byte{}
			if tc.generation != nil {
				values[string(keys[0])] = tc.generation
			}
			if tc.members != nil {
				values[string(keys[1])] = tc.members
			}
			s := &quotaStatusReadStore{KvStorage: base.kv, values: values, err: tc.err}
			b.kv = &struct{ storage.KvStorage }{s}
			wantMembers, wantGeneration, wantExists, wantErr := b.readStableCorruptAlarmState(ctx)
			s.gets = 0
			b.kv = s
			members, raw, exists, err := b.readStableCorruptAlarmState(ctx)
			require.Equal(t, wantMembers, members)
			require.Equal(t, wantGeneration, raw)
			require.Equal(t, wantExists, exists)
			if wantErr == nil {
				require.NoError(t, err)
			} else {
				require.EqualError(t, err, wantErr.Error())
				if tc.err != nil {
					require.True(t, errors.Is(err, tc.err))
				}
			}
			require.Zero(t, s.gets)
			require.Equal(t, 1, s.batches)
			require.Equal(t, keys, s.keys)
			require.Equal(t, ctx, s.ctx)
		})
	}
}

func TestCorruptAlarmBatchPreservesPinnedReads(t *testing.T) {
	base, ctx := newQuotaBackend(t, 100)
	b := &backend{ks: base.ks}
	s := &quotaStatusReadStore{KvStorage: base.kv, values: map[string][]byte{}}
	b.kv = s
	ctx = storage.WithSnapshotTimestamp(ctx, 123)
	members, raw, exists, err := b.readStableCorruptAlarmState(ctx)
	require.NoError(t, err)
	require.Nil(t, members)
	require.Nil(t, raw)
	require.False(t, exists)
	require.Equal(t, 3, s.gets)
	require.Zero(t, s.batches)
	require.Equal(t, uint64(123), s.timestamp)
}
