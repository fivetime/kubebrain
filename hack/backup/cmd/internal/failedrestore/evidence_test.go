package failedrestore

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestReadBoundedRejectsOversizedEvidence(t *testing.T) {
	path := filepath.Join(t.TempDir(), "evidence.json")
	require.NoError(t, os.WriteFile(path, make([]byte, (8<<20)+1), 0o600))
	_, err := readBounded(path)
	require.ErrorContains(t, err, "exceeds 8 MiB")
}
