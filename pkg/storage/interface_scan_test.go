package storage

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestScanBatchSizeContextBounds(t *testing.T) {
	base := context.Background()

	for _, invalid := range []int{-1, 0, 1, MaxScanBatchSize + 1} {
		ctx := WithScanBatchSize(base, invalid)
		require.Equal(t, base, ctx)
		_, ok := ScanBatchSizeFromContext(ctx)
		require.False(t, ok)
	}

	for _, valid := range []int{2, 1024, MaxScanBatchSize} {
		ctx := WithScanBatchSize(base, valid)
		got, ok := ScanBatchSizeFromContext(ctx)
		require.True(t, ok)
		require.Equal(t, valid, got)
	}
}

type stableRowsTestIter struct{ Iter }

func (*stableRowsTestIter) StableIteratorRows() {}

type wrappedRowsTestIter struct{ Iter }

func (i *wrappedRowsTestIter) UnwrapIterator() Iter { return i.Iter }

func TestIteratorRowsAreStableThroughDecorators(t *testing.T) {
	stable := &stableRowsTestIter{}
	require.True(t, IteratorRowsAreStable(stable))
	require.True(t, IteratorRowsAreStable(&wrappedRowsTestIter{Iter: stable}))
	require.False(t, IteratorRowsAreStable(&wrappedRowsTestIter{}))
	require.False(t, IteratorRowsAreStable(&wrappedRowsTestIter{Iter: &wrappedRowsTestIter{}}))
}
