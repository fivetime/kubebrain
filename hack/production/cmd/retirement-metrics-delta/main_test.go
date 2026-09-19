package main

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRejectInvalidArgumentsWithoutSuccess(t *testing.T) {
	for _, args := range [][]string{nil, {"--stage", "other"}, {"--before", "/missing", "--after", "/missing", "--stage", "peer", "--outcome", "confirmed"}, {"--unknown"}} {
		var output bytes.Buffer
		require.Error(t, run(args, &output))
		require.Empty(t, output.String())
	}
}
