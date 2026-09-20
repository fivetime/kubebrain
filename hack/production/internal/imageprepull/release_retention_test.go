package imageprepull

import (
	"bytes"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRetainReleaseResponse(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, os.Chmod(dir, 0700))
	output := bytes.Repeat([]byte{0, 255, 1, 2}, (2<<20)/4)
	require.NoError(t, RetainReleaseResponse(dir, "archive", output, errors.New("digest mismatch")))
	path := filepath.Join(dir, "github-archive.json")
	data, err := os.ReadFile(path)
	require.NoError(t, err)
	var result struct {
		Stage  string
		Output []byte
		Error  string
	}
	require.NoError(t, json.Unmarshal(data, &result))
	require.Equal(t, output, result.Output)
	require.Equal(t, "digest mismatch", result.Error)
	require.Equal(t, "archive", result.Stage)
	info, err := os.Stat(path)
	require.NoError(t, err)
	require.Equal(t, os.FileMode(0600), info.Mode().Perm())
	require.Error(t, RetainReleaseResponse(dir, "archive", []byte("replacement"), nil))
	after, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Equal(t, data, after)
}

func TestRetainReleaseResponseRejectsUnsafeInputs(t *testing.T) {
	for _, mode := range []string{"public", "directory-symlink", "file-symlink", "stage", "oversize", "metadata-oversize"} {
		t.Run(mode, func(t *testing.T) {
			dir := t.TempDir()
			require.NoError(t, os.Chmod(dir, 0700))
			stage, output := "archive", []byte("data")
			switch mode {
			case "public":
				require.NoError(t, os.Chmod(dir, 0755))
			case "directory-symlink":
				link := filepath.Join(t.TempDir(), "link")
				require.NoError(t, os.Symlink(dir, link))
				dir = link
			case "file-symlink":
				require.NoError(t, os.Symlink("other", filepath.Join(dir, "github-archive.json")))
			case "stage":
				stage = "../outside"
			case "oversize":
				output = make([]byte, (2<<20)+1)
			case "metadata-oversize":
				stage = "run-before"
				output = make([]byte, (1<<20)+1)
			}
			require.Error(t, RetainReleaseResponse(dir, stage, output, nil))
		})
	}
}
