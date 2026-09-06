package txnsnapshot

import (
	"context"
	"testing"
	"time"

	"github.com/pingcap/kvproto/pkg/kvrpcpb"
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

func TestScannerCachedNextDoesNotCreateBackoffer(t *testing.T) {
	// A cached, error-free row needs no Region lookup, RPC, lock resolution or
	// retry budget. Keep snapshot nil so this test fails immediately if that fast
	// path starts constructing a snapshot-bound backoffer again.
	scanner := &Scanner{
		valid: true,
		cache: []*kvrpcpb.KvPair{{Key: []byte("first")}, {Key: []byte("second")}},
	}

	require.NoError(t, scanner.NextWithContext(context.Background()))
	require.Equal(t, []byte("second"), scanner.Key())
}
