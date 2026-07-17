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

	hash1, hashedRev, currentRev, err := b.HashKV(ctx, int64(rev1))
	require.NoError(t, err)
	require.Equal(t, int64(rev1), hashedRev)
	require.Equal(t, int64(rev1), currentRev)

	updated, err := b.Update(ctx, &proto.UpdateRequest{Kv: &proto.KeyValue{
		Key: key, Value: []byte("v2"), Revision: rev1,
	}})
	require.NoError(t, err)
	require.True(t, updated.Succeeded)
	rev2 := updated.Header.Revision
	waitCommitted(t, b, rev2)

	hash2, hashedRev, currentRev, err := b.HashKV(ctx, 0)
	require.NoError(t, err)
	require.Equal(t, int64(rev2), hashedRev)
	require.Equal(t, int64(rev2), currentRev)
	require.NotEqual(t, hash1, hash2, "a retained MVCC version must change the hash")

	historical, _, currentRev, err := b.HashKV(ctx, int64(rev1))
	require.NoError(t, err)
	require.Equal(t, hash1, historical, "later writes must not alter an earlier revision hash")
	require.Equal(t, int64(rev2), currentRev)

	negative, hashedRev, currentRev, err := b.HashKV(ctx, -1)
	require.NoError(t, err)
	require.Equal(t, int64(-1), hashedRev)
	require.Equal(t, int64(rev2), currentRev)
	require.Equal(t, crc32.Checksum([]byte("key"), hashKVTable), negative)
	require.NotEqual(t, hash2, negative, "negative revision must not select current data")
}

func TestHashKVHonorsCancellation(t *testing.T) {
	b, _ := newTxnApplyBackend(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, _, _, err := b.HashKV(ctx, 0)
	require.ErrorIs(t, err, context.Canceled)
}
