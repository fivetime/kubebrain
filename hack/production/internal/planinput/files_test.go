package planinput

import (
	"math"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestReadFile(t *testing.T) {
	for _, mode := range []string{"private", "public-allowed", "public-denied", "symlink", "fifo", "directory", "oversized", "relative", "unclean", "zero-limit", "negative-limit", "overflow-limit"} {
		t.Run(mode, func(t *testing.T) {
			dir := t.TempDir()
			path := filepath.Join(dir, "input")
			require.NoError(t, os.WriteFile(path, []byte("data"), 0600))
			private, limit := true, int64(4)
			switch mode {
			case "public-allowed", "public-denied":
				require.NoError(t, os.Chmod(path, 0644))
				private = mode != "public-allowed"
			case "symlink":
				require.NoError(t, os.Symlink(path, path+".link"))
				path += ".link"
			case "fifo":
				path += ".fifo"
				require.NoError(t, syscall.Mkfifo(path, 0600))
			case "directory":
				path = dir
			case "oversized":
				limit = 3
			case "relative":
				path = "input"
			case "unclean":
				path = dir + "/./input"
			case "zero-limit":
				limit = 0
			case "negative-limit":
				limit = -1
			case "overflow-limit":
				limit = math.MaxInt64
			}
			data, err := ReadFile(path, private, limit)
			if mode == "private" || mode == "public-allowed" {
				require.NoError(t, err)
				require.Equal(t, "data", string(data))
			} else {
				require.Error(t, err)
				require.Nil(t, data)
			}
		})
	}
}

func TestSHA256(t *testing.T) {
	digest := SHA256([]byte("data"))
	require.Equal(t, "3a6eb0790f39ac87c94f3856b2dd2c5d110e6811602261a9a923d3bb23adc8b7", digest)
	require.True(t, ValidSHA256(digest))
	for _, s := range []string{strings.ToUpper(digest), digest[:63], digest + "0", "", strings.Repeat("z", 64)} {
		require.False(t, ValidSHA256(s))
	}
}
