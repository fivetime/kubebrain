package main

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNextKeyDoesNotMutateInput(t *testing.T) {
	in := []byte("abc")
	out := nextKey(in)

	require.Equal(t, []byte("abc"), in)
	require.Equal(t, []byte{'a', 'b', 'c', 0}, out)
}
