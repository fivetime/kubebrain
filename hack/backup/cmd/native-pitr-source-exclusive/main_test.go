package main

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestExecuteValidatesOptionsBeforeConnecting(t *testing.T) {
	err := execute(context.Background(), options{}, io.Discard, time.Now)
	require.ErrorContains(t, err, "full-snapshot and a positive timeout are required")
}

func TestReadReceiptRejectsOversizedInput(t *testing.T) {
	path := filepath.Join(t.TempDir(), "full.json")
	require.NoError(t, os.WriteFile(path, make([]byte, maxReceiptBytes+1), 0o600))
	_, err := readReceipt(path)
	require.ErrorContains(t, err, "receipt exceeds")
}

func TestParseAddrsCanonicalizesAndRejectsAmbiguity(t *testing.T) {
	got, err := parseAddrs("pd-b:2379, pd-a:2379")
	require.NoError(t, err)
	require.Equal(t, []string{"pd-a:2379", "pd-b:2379"}, got)
	for _, raw := range []string{"", "pd:2379,pd:2379", "http://pd:2379"} {
		_, err := parseAddrs(raw)
		require.Error(t, err)
	}
}
