package objectstore

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestWriteReceiptAtomicIsIdempotentAndNonOverwriting(t *testing.T) {
	path := filepath.Join(t.TempDir(), "receipt.json")
	receipt := completeReceipt()
	require.NoError(t, WriteReceiptAtomic(path, receipt))
	require.NoError(t, WriteReceiptAtomic(path, receipt))

	info, err := os.Stat(path)
	require.NoError(t, err)
	require.Equal(t, os.FileMode(0o600), info.Mode().Perm())

	changed := receipt
	changed.BackupID = "other"
	require.ErrorContains(t, WriteReceiptAtomic(path, changed), "refusing to overwrite")
	actual, err := ReadReceipt(path)
	require.NoError(t, err)
	require.Equal(t, receipt, actual)
}
