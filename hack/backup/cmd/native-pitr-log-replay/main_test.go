package main

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestExecuteRequiresBoundedApprovedInputs(t *testing.T) {
	err := execute(context.Background(), options{}, &bytes.Buffer{}, time.Now)
	require.ErrorContains(t, err, "required")
}

func TestReadBoundedRejectsOversizedReceipt(t *testing.T) {
	path := filepath.Join(t.TempDir(), "receipt.json")
	require.NoError(t, os.WriteFile(path, make([]byte, maxReceiptBytes+1), 0o600))
	_, err := readBounded(path)
	require.ErrorContains(t, err, "exceeds")
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

func TestReplayScratchDirDefaultsToLogRootParent(t *testing.T) {
	require.Equal(t, "/evidence", replayScratchDir(options{logRoot: "/evidence/log-mirror"}))
	require.Equal(t, "/scratch", replayScratchDir(options{
		logRoot: "/evidence/log-mirror", scratchDir: "/scratch",
	}))
}
