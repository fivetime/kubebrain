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
	require.ErrorContains(t, err, "valid action, plan, target-pd-addrs")
}

func TestReadBoundedRejectsOversizedReceipt(t *testing.T) {
	path := filepath.Join(t.TempDir(), "receipt.json")
	require.NoError(t, os.WriteFile(path, make([]byte, maxReceiptBytes+1), 0o600))
	_, err := readBounded(path)
	require.ErrorContains(t, err, "receipt exceeds size limit")
}

func TestParseAddrsRejectsAmbiguousEndpoints(t *testing.T) {
	_, err := parseAddrs("pd-a:2379,pd-a:2379")
	require.ErrorContains(t, err, "unique host:port")
	_, err = parseAddrs("https://pd-a:2379")
	require.ErrorContains(t, err, "unique host:port")
}
