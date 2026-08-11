package main

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestExecuteRequiresBoundedApprovedInputs(t *testing.T) {
	err := execute(context.Background(), options{}, &bytes.Buffer{}, time.Now)
	require.ErrorContains(t, err, "required")
}

func TestParseAddrsIsStrict(t *testing.T) {
	got, err := parseAddrs("pd-a:2379,pd-b:2379")
	require.NoError(t, err)
	require.Equal(t, []string{"pd-a:2379", "pd-b:2379"}, got)
	for _, value := range []string{"https://pd:2379", "pd:2379,pd:2379", ""} {
		_, err := parseAddrs(value)
		require.Error(t, err)
	}
}
