package nativepitr

import (
	"bytes"
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestReplayDefaultStorePersistsLargeKeysAndRejectsConflicts(t *testing.T) {
	dir := t.TempDir()
	store, err := newReplayDefaultStore(dir)
	require.NoError(t, err)
	path := store.path
	key := strings.Repeat("k", 64<<10)
	value := bytes.Repeat([]byte("value"), 1024)

	require.NoError(t, store.Put(key, value))
	require.NoError(t, store.Put(key, value))
	require.NoError(t, store.Put("empty", nil))
	require.EqualError(t, store.Put(key, []byte("different")),
		"stream log contains conflicting default-CF entries")
	require.NoError(t, store.Flush())
	got, found, err := store.Get(key)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, value, got)
	got[0] ^= 0xff
	again, found, err := store.Get(key)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, value, again, "Get must not expose bbolt mmap-backed memory")
	empty, found, err := store.Get("empty")
	require.NoError(t, err)
	require.True(t, found)
	require.Empty(t, empty)

	require.NoError(t, store.Close())
	_, err = os.Stat(path)
	require.ErrorIs(t, err, os.ErrNotExist)
}

func TestMaterializeReplayUsesAndCleansExplicitScratchDirectory(t *testing.T) {
	_, receipt, root, _, _ := replayFixture(t)
	scratch := t.TempDir()

	manifest, mutations, err := MaterializeReplayWithScratchDir(
		receipt, digest, root, scratch, 119, 150,
	)

	require.NoError(t, err)
	require.Equal(t, 3, manifest.MutationCount)
	require.Len(t, mutations, 3)
	entries, err := os.ReadDir(scratch)
	require.NoError(t, err)
	require.Empty(t, entries, "rebuildable default-CF index must be removed after materialization")
}

func TestReplayDefaultStoreRollsBackUnflushedBatchOnClose(t *testing.T) {
	dir := t.TempDir()
	store, err := newReplayDefaultStore(dir)
	require.NoError(t, err)
	path := store.path
	require.NoError(t, store.Put("key", []byte("value")))

	require.NoError(t, store.Close())
	_, err = os.Stat(path)
	require.ErrorIs(t, err, os.ErrNotExist)
}

func TestReplayDefaultStoreCommitsBoundedBatches(t *testing.T) {
	store, err := newReplayDefaultStore(t.TempDir())
	require.NoError(t, err)
	defer func() { require.NoError(t, store.Close()) }()
	value := bytes.Repeat([]byte{'v'}, replayDefaultStoreBatchBytes)

	require.NoError(t, store.Put("large", value))
	require.Nil(t, store.tx, "the size threshold must commit rather than retain an unbounded write transaction")
	got, found, err := store.Get("large")
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, value, got)
}
