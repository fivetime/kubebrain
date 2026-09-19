// Package planinput reads locally pinned inputs for production commands.
// File/hash verification is not cluster, image, CI or execution authorization.
package planinput

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"os"
	"path/filepath"
	"syscall"
)

func SHA256(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

func ValidSHA256(s string) bool {
	b, err := hex.DecodeString(s)
	return err == nil && len(b) == sha256.Size && hex.EncodeToString(b) == s
}

// ReadFile accepts canonical absolute regular files, never a final symlink or
// FIFO/device. Intermediate directory ownership must be independently admitted.
// The named entry must keep the same inode while reading; callers must compare
// SHA256 to an independently frozen digest to reject same-inode content changes.
func ReadFile(path string, private bool, limit int64) ([]byte, error) {
	if !filepath.IsAbs(path) || filepath.Clean(path) != path || limit < 1 || limit > 128<<20 {
		return nil, errors.New("require canonical absolute path and bounded input size")
	}
	before, err := os.Lstat(path)
	if err != nil {
		return nil, err
	}
	if !before.Mode().IsRegular() || (private && before.Mode().Perm()&0077 != 0) || before.Size() > limit {
		return nil, errors.New("unsafe or oversized input file")
	}
	f, err := os.OpenFile(path, os.O_RDONLY|syscall.O_NOFOLLOW|syscall.O_NONBLOCK, 0)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	opened, err := f.Stat()
	if err != nil || !os.SameFile(before, opened) {
		return nil, errors.New("input file changed while opening")
	}
	data, err := io.ReadAll(io.LimitReader(f, limit+1))
	if err != nil {
		return nil, err
	}
	if int64(len(data)) > limit {
		return nil, errors.New("oversized input file")
	}
	after, err := os.Lstat(path)
	if err != nil || !os.SameFile(opened, after) || !after.Mode().IsRegular() || (private && after.Mode().Perm()&0077 != 0) {
		return nil, errors.New("input file replaced or made public")
	}
	return data, nil
}
