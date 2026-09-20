package leasefault

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/kubewharf/kubebrain/hack/production/internal/processgroup"
	"github.com/stretchr/testify/require"
)

type evidenceErrorFunc func() string

func (f evidenceErrorFunc) Error() string { return f() }

func TestRetainRecoveryObserver(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, os.Chmod(dir, 0700))
	output := []byte{'x', '\n', 0, 255}
	for range 2 {
		require.NoError(t, RetainRecoveryObserver(dir, "restored", output, errors.New("observation failed")))
	}
	files, err := filepath.Glob(filepath.Join(dir, "recovery-restored.*.json"))
	require.NoError(t, err)
	require.Len(t, files, 2, "never overwrite previous observations")
	for _, path := range files {
		st, err := os.Lstat(path)
		require.NoError(t, err)
		require.True(t, st.Mode().IsRegular())
		require.Equal(t, os.FileMode(0600), st.Mode().Perm())
		data, err := os.ReadFile(path)
		require.NoError(t, err)
		var record struct {
			Output []byte
			Error  string
			At     time.Time
		}
		require.NoError(t, json.Unmarshal(data, &record))
		require.Equal(t, output, record.Output)
		require.Equal(t, "observation failed", record.Error)
		require.False(t, record.At.IsZero())
	}
}

func TestRetainRecoveryObserverRejectsUnsafeInputs(t *testing.T) {
	for _, mode := range []string{"stage", "long-stage", "public", "symlink", "output", "error"} {
		t.Run(mode, func(t *testing.T) {
			dir := t.TempDir()
			require.NoError(t, os.Chmod(dir, 0700))
			path, stage := dir, "join"
			var output []byte
			var observed error
			switch mode {
			case "stage":
				stage = "../elsewhere"
			case "long-stage":
				stage = strings.Repeat("a", 65)
			case "public":
				require.NoError(t, os.Chmod(dir, 0755))
			case "symlink":
				path = filepath.Join(t.TempDir(), "link")
				require.NoError(t, os.Symlink(dir, path))
			case "output":
				output = make([]byte, processgroup.DefaultOutputLimitBytes+1)
			case "error":
				observed = errors.New(strings.Repeat("e", (64<<10)+1))
			}
			require.Error(t, RetainRecoveryObserver(path, stage, output, observed))
			entries, err := os.ReadDir(dir)
			require.NoError(t, err)
			require.Empty(t, entries)
		})
	}
}

func TestRetainRecoveryObserverPinsDirectoryHandle(t *testing.T) {
	parent := t.TempDir()
	dir, moved := filepath.Join(parent, "owner"), filepath.Join(parent, "retained")
	require.NoError(t, os.Mkdir(dir, 0700))
	observed := evidenceErrorFunc(func() string {
		// Deterministic replacement after the directory handle has been opened.
		require.NoError(t, os.Rename(dir, moved))
		require.NoError(t, os.Mkdir(dir, 0700))
		return "observer failed"
	})
	require.ErrorContains(t, RetainRecoveryObserver(dir, "join", []byte("evidence"), observed), "directory changed")
	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	require.Empty(t, entries, "must not write to replacement directory")
	entries, err = os.ReadDir(moved)
	require.NoError(t, err)
	require.Len(t, entries, 1, "preserve evidence in the original directory")
}
