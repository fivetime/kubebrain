// Package planinput reads locally pinned inputs for production commands.
// File/hash verification is not cluster, image, CI or execution authorization.
package planinput

import (
	"context"
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
	var data []byte
	err := readInput(path, private, limit, func(r io.Reader) (err error) {
		data, err = io.ReadAll(r)
		return err
	})
	if err != nil {
		return nil, err
	}
	return data, nil
}

// FileSHA256 reopens and hashes the complete bounded input on every call, using
// constant-size buffering. It shares ReadFile's path, mode and inode checks.
// Cancellation is observed between reads; it cannot interrupt a kernel read.
// A digest is returned only after EOF and the final file/context checks pass.
func FileSHA256(ctx context.Context, path string, private bool, limit int64) (string, error) {
	if ctx == nil {
		return "", errors.New("file digest requires context")
	}
	if err := ctx.Err(); err != nil {
		return "", err
	}
	h := sha256.New()
	err := readInput(path, private, limit, func(r io.Reader) error {
		_, err := io.Copy(h, contextReader{ctx: ctx, reader: r})
		return err
	})
	if err := errors.Join(err, ctx.Err()); err != nil {
		return "", err
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

type contextReader struct {
	ctx    context.Context
	reader io.Reader
}

func (r contextReader) Read(p []byte) (int, error) {
	if err := r.ctx.Err(); err != nil {
		return 0, err
	}
	return r.reader.Read(p)
}

func readInput(path string, private bool, limit int64, consume func(io.Reader) error) error {
	if !filepath.IsAbs(path) || filepath.Clean(path) != path || limit < 1 || limit > 128<<20 {
		return errors.New("require canonical absolute path and bounded input size")
	}
	before, err := os.Lstat(path)
	if err != nil {
		return err
	}
	if !before.Mode().IsRegular() || (private && before.Mode().Perm()&0077 != 0) || before.Size() > limit {
		return errors.New("unsafe or oversized input file")
	}
	f, err := os.OpenFile(path, os.O_RDONLY|syscall.O_NOFOLLOW|syscall.O_NONBLOCK, 0)
	if err != nil {
		return err
	}
	defer f.Close()
	opened, err := f.Stat()
	if err != nil || !os.SameFile(before, opened) {
		return errors.New("input file changed while opening")
	}
	r := &io.LimitedReader{R: f, N: limit + 1}
	if err := consume(r); err != nil {
		return err
	}
	if r.N == 0 {
		return errors.New("oversized input file")
	}
	after, err := os.Lstat(path)
	if err != nil || !os.SameFile(opened, after) || !after.Mode().IsRegular() || (private && after.Mode().Perm()&0077 != 0) {
		return errors.New("input file replaced or made public")
	}
	return nil
}
