package main

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestBoundedBufferCapsRetainedOutput(t *testing.T) {
	var buffer boundedBuffer
	payload := bytes.Repeat([]byte("x"), maxKubectlOutput+4096)
	written, err := buffer.Write(payload)
	require.NoError(t, err)
	require.Equal(t, len(payload), written)
	require.True(t, buffer.overflow)
	require.Equal(t, maxKubectlOutput, buffer.Len())

	written, err = buffer.Write([]byte("more"))
	require.NoError(t, err)
	require.Equal(t, 4, written)
	require.Equal(t, maxKubectlOutput, buffer.Len())
}
