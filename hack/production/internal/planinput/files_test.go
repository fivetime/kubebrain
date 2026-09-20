package planinput

import (
	"bytes"
	"context"
	"io"
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
			digest, digestErr := FileSHA256(context.Background(), path, private, limit)
			if mode == "private" || mode == "public-allowed" {
				require.NoError(t, err)
				require.Equal(t, "data", string(data))
				require.NoError(t, digestErr)
				require.Equal(t, SHA256(data), digest)
			} else {
				require.Error(t, err)
				require.Nil(t, data)
				require.Error(t, digestErr)
				require.Empty(t, digest)
			}
		})
	}
}

func TestFileSHA256ReadsFreshBytes(t *testing.T) {
	path := filepath.Join(t.TempDir(), "input")
	for _, size := range []int{0, 1, 32768, 32769, 1 << 20} {
		for _, value := range []byte{'x', 'y'} {
			data := bytes.Repeat([]byte{value}, size)
			require.NoError(t, os.WriteFile(path, data, 0600))
			digest, err := FileSHA256(context.Background(), path, true, 1<<20)
			require.NoError(t, err)
			require.Equal(t, SHA256(data), digest, "no cached same-inode/same-size digest")
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	digest, err := FileSHA256(ctx, path, true, 1<<20)
	require.ErrorIs(t, err, context.Canceled)
	require.Empty(t, digest)
	digest, err = FileSHA256(nil, path, true, 1<<20)
	require.Error(t, err)
	require.Empty(t, digest)
}

func TestDigestReaderCancellationBetweenChunks(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	r := contextReader{ctx: ctx, reader: bytes.NewReader([]byte("ab"))}
	buffer := make([]byte, 1)
	n, err := r.Read(buffer)
	require.NoError(t, err)
	require.Equal(t, 1, n)
	cancel()
	n, err = r.Read(buffer)
	require.ErrorIs(t, err, context.Canceled)
	require.Zero(t, n)
}

func TestInputPostReadChecks(t *testing.T) {
	for _, mode := range []string{"replacement", "public", "growing"} {
		t.Run(mode, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "input")
			require.NoError(t, os.WriteFile(path, []byte("data"), 0600))
			err := readInput(path, true, 4, func(r io.Reader) error {
				if mode == "growing" {
					f, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0)
					require.NoError(t, err)
					_, err = f.Write([]byte("x"))
					require.NoError(t, err)
					require.NoError(t, f.Close())
				}
				_, err := io.Copy(io.Discard, r)
				require.NoError(t, err)
				if mode == "replacement" {
					require.NoError(t, os.Rename(path, path+".old"))
					require.NoError(t, os.WriteFile(path, []byte("data"), 0600))
				} else if mode == "public" {
					require.NoError(t, os.Chmod(path, 0644))
				}
				return nil
			})
			require.Error(t, err)
		})
	}
}

func BenchmarkInputDigest(b *testing.B) {
	path := filepath.Join(b.TempDir(), "input")
	const size = 16 << 20
	require.NoError(b, os.WriteFile(path, bytes.Repeat([]byte("x"), size), 0600))
	for _, stream := range []bool{false, true} {
		name := "read-all"
		if stream {
			name = "stream"
		}
		b.Run(name, func(b *testing.B) {
			b.ReportAllocs()
			b.SetBytes(size)
			for i := 0; i < b.N; i++ {
				if stream {
					_, err := FileSHA256(context.Background(), path, true, size)
					require.NoError(b, err)
				} else {
					data, err := ReadFile(path, true, size)
					require.NoError(b, err)
					require.True(b, ValidSHA256(SHA256(data)))
				}
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
