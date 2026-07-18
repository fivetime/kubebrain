package backend

import (
	"context"
	"hash/crc32"
	"testing"

	"github.com/stretchr/testify/require"

	proto "github.com/kubewharf/kubebrain-client/api/v2rpc"
)

func TestHashKVTracksDataAndPreservesHistoricalRevision(t *testing.T) {
	b, ctx := newTxnApplyBackend(t)
	key := []byte(prefix + "/hash/key")

	created, err := b.Create(ctx, &proto.CreateRequest{Key: key, Value: []byte("v1")})
	require.NoError(t, err)
	require.True(t, created.Succeeded)
	rev1 := created.Header.Revision
	waitCommitted(t, b, rev1)

	hash1, err := b.HashKV(ctx, int64(rev1))
	require.NoError(t, err)
	require.Equal(t, int64(rev1), hash1.HashRevision)
	require.Equal(t, int64(rev1), hash1.CurrentRevision)
	require.Equal(t, int64(-1), hash1.CompactRevision)

	updated, err := b.Update(ctx, &proto.UpdateRequest{Kv: &proto.KeyValue{
		Key: key, Value: []byte("v2"), Revision: rev1,
	}})
	require.NoError(t, err)
	require.True(t, updated.Succeeded)
	rev2 := updated.Header.Revision
	waitCommitted(t, b, rev2)

	hash2, err := b.HashKV(ctx, 0)
	require.NoError(t, err)
	require.Equal(t, int64(rev2), hash2.HashRevision)
	require.Equal(t, int64(rev2), hash2.CurrentRevision)
	require.NotEqual(t, hash1.Hash, hash2.Hash, "a retained MVCC version must change the hash")

	historical, err := b.HashKV(ctx, int64(rev1))
	require.NoError(t, err)
	require.Equal(t, hash1.Hash, historical.Hash, "later writes must not alter an earlier revision hash")
	require.Equal(t, int64(rev2), historical.CurrentRevision)

	negative, err := b.HashKV(ctx, -1)
	require.NoError(t, err)
	require.Equal(t, int64(-1), negative.HashRevision)
	require.Equal(t, int64(rev2), negative.CurrentRevision)
	require.Equal(t, crc32.Checksum([]byte("key"), hashKVTable), negative.Hash)
	require.NotEqual(t, hash2.Hash, negative.Hash, "negative revision must not select current data")
}

func TestHashKVHonorsCancellation(t *testing.T) {
	b, _ := newTxnApplyBackend(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := b.HashKV(ctx, 0)
	require.ErrorIs(t, err, context.Canceled)
}

func TestHashKVIsStableAcrossPhysicalCompaction(t *testing.T) {
	b, ctx := newTxnApplyBackend(t)
	key := []byte(prefix + "/hash/compact")

	created, err := b.Create(ctx, &proto.CreateRequest{Key: key, Value: []byte("v1")})
	require.NoError(t, err)
	last := created.Header.Revision
	for _, value := range []string{"v2", "v3"} {
		updated, updateErr := b.Update(ctx, &proto.UpdateRequest{Kv: &proto.KeyValue{
			Key: key, Value: []byte(value), Revision: last,
		}})
		require.NoError(t, updateErr)
		require.True(t, updated.Succeeded)
		last = updated.Header.Revision
	}
	deletedKey := []byte(prefix + "/hash/compact-deleted")
	deletedCreated, err := b.Create(ctx, &proto.CreateRequest{Key: deletedKey, Value: []byte("gone")})
	require.NoError(t, err)
	deleted, err := b.Delete(ctx, &proto.DeleteRequest{Key: deletedKey, Revision: deletedCreated.Header.Revision})
	require.NoError(t, err)
	require.True(t, deleted.Succeeded)
	last = deleted.Header.Revision
	waitCommitted(t, b, last)

	beforeLogicalCompact, err := b.HashKV(ctx, 0)
	require.NoError(t, err)
	require.Equal(t, int64(-1), beforeLogicalCompact.CompactRevision)

	advanced, err := b.setCompactRecord(ctx, last)
	require.NoError(t, err)
	require.True(t, advanced)
	afterLogicalCompact, err := b.HashKV(ctx, 0)
	require.NoError(t, err)
	require.Equal(t, int64(last), afterLogicalCompact.CompactRevision)
	require.NotEqual(t, beforeLogicalCompact.Hash, afterLogicalCompact.Hash,
		"logical compaction must exclude versions scheduled for physical GC")

	require.NoError(t, b.physicalCompact(ctx, last))
	afterPhysicalCompact, err := b.HashKV(ctx, 0)
	require.NoError(t, err)
	require.Equal(t, afterLogicalCompact, afterPhysicalCompact,
		"HashKV must describe logical state and remain stable when physical GC catches up")

	_, err = b.HashKV(ctx, int64(last)-1)
	require.ErrorIs(t, err, ErrHashKVCompacted)
	_, err = b.HashKV(ctx, int64(b.GetCurrentRevision())+1)
	require.ErrorIs(t, err, ErrHashKVFuture)
}
