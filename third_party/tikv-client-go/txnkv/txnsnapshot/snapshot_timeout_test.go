package txnsnapshot

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestSnapshotRequestTimeout(t *testing.T) {
	snapshot := &KVSnapshot{}
	defaultTimeout := 60 * time.Second

	require.Equal(t, defaultTimeout, snapshot.readTimeout(defaultTimeout))

	snapshot.SetRequestTimeout(2 * time.Second)
	require.Equal(t, 2*time.Second, snapshot.readTimeout(defaultTimeout))

	snapshot.SetRequestTimeout(0)
	require.Equal(t, defaultTimeout, snapshot.readTimeout(defaultTimeout))
}

func TestSnapshotCacheOnlyRegionRead(t *testing.T) {
	snapshot := &KVSnapshot{}
	require.False(t, snapshot.cacheOnlyRegionRead)
	require.False(t, snapshot.mu.isStaleness)

	snapshot.SetCacheOnlyRegionRead(true)
	snapshot.SetIsStalenessReadOnly(true)
	require.True(t, snapshot.cacheOnlyRegionRead)
	require.True(t, snapshot.mu.isStaleness)
}
