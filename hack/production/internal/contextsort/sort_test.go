package contextsort

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSliceSortsStably(t *testing.T) {
	type item struct {
		key, sequence int
	}
	items := []item{{2, 0}, {1, 1}, {2, 2}, {1, 3}}
	require.NoError(t, Slice(context.Background(), items, func(left, right item) bool {
		return left.key < right.key
	}))
	require.Equal(t, []item{{1, 1}, {1, 3}, {2, 0}, {2, 2}}, items)
}

func TestSliceStopsDuringCancellation(t *testing.T) {
	items := make([]int, 64)
	for i := range items {
		items[i] = len(items) - i
	}
	ctx := &cancelAfterErrChecks{Context: context.Background(), remaining: 5}
	require.ErrorIs(t, Slice(ctx, items, func(left, right int) bool { return left < right }), context.Canceled)
}

func TestSliceRejectsInvalidInputs(t *testing.T) {
	//lint:ignore SA1012 This negative test verifies rejection of an absent context.
	require.Error(t, Slice[int](nil, nil, func(left, right int) bool { return left < right }))
	require.Error(t, Slice[int](context.Background(), nil, nil))
}

type cancelAfterErrChecks struct {
	context.Context
	remaining int
}

func (c *cancelAfterErrChecks) Err() error {
	c.remaining--
	if c.remaining <= 0 {
		return context.Canceled
	}
	return nil
}
