package main

import (
	"encoding/base64"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/kubewharf/kubebrain/hack/backup/internal/backupfile"
	"github.com/kubewharf/kubebrain/hack/backup/internal/record"
	"github.com/stretchr/testify/require"
)

type failingWriter struct{ err error }

func (w failingWriter) Write([]byte) (int, error) { return 0, w.err }

func TestRunRejectsMissingAndUnexpectedArguments(t *testing.T) {
	require.ErrorContains(t, run(nil, os.Stdout), "--input and --output are required")
	require.ErrorContains(t, run([]string{"extra"}, os.Stdout), "unexpected positional arguments")
}

func TestRunPropagatesSuccessOutputFailure(t *testing.T) {
	input := filepath.Join(t.TempDir(), "logical.jsonl")
	writer, err := backupfile.NewAtomicWriter(input, "/", 42)
	require.NoError(t, err)
	require.NoError(t, writer.Add(record.Record{
		Key: base64.StdEncoding.EncodeToString([]byte("/a")), Value: base64.StdEncoding.EncodeToString([]byte("v")),
		CreateRevision: 42, ModRevision: 42, Version: 1,
	}))
	_, err = writer.Commit()
	require.NoError(t, err)
	output := filepath.Join(t.TempDir(), "snapshot.db")
	writeErr := errors.New("status output failed")

	err = run([]string{"--input", input, "--output", output, "--acknowledge-auth-disabled"}, failingWriter{err: writeErr})

	require.ErrorIs(t, err, writeErr)
	_, statErr := os.Stat(output)
	require.NoError(t, statErr, "conversion must remain durable even when status reporting fails")
}
