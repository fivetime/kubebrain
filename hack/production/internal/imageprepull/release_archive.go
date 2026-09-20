package imageprepull

import (
	"archive/zip"
	"bytes"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
)

// ParseReleaseArchive consumes downloaded bytes without extracting any paths.
// expectedDigest must come from authenticated artifact metadata bound to the
// successful run/attempt, NOT a hash computed from these untrusted bytes. This
// parser does not authenticate GitHub or establish artifact ownership itself.
func ParseReleaseArchive(data []byte, expectedDigest string, wanted ReleaseIdentity) (ReleaseEvidence, []byte, error) {
	fail := func(err error) (ReleaseEvidence, []byte, error) { return ReleaseEvidence{}, nil, err }
	if len(data) == 0 || len(data) > 2<<20 || !validDigest(expectedDigest) || fmt.Sprintf("sha256:%x", sha256.Sum256(data)) != expectedDigest {
		return fail(errors.New("release archive differs from admitted digest or size bound"))
	}
	reader, err := zip.NewReader(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		return fail(err)
	}
	if len(reader.File) != 2 {
		return fail(errors.New("release archive requires exactly two files"))
	}
	files := map[string][]byte{}
	for _, file := range reader.File {
		limit := int64(32 << 10)
		switch file.Name {
		case "index.json":
			limit = 1 << 20
		case "release.json":
		default:
			return fail(errors.New("unexpected release archive path"))
		}
		if _, exists := files[file.Name]; exists || !file.Mode().IsRegular() || file.UncompressedSize64 > uint64(limit) {
			return fail(errors.New("invalid, duplicate or oversized release archive member"))
		}
		stream, err := file.Open()
		if err != nil {
			return fail(err)
		}
		content, readErr := io.ReadAll(io.LimitReader(stream, limit+1))
		if err := errors.Join(readErr, stream.Close()); err != nil {
			return fail(err)
		}
		if int64(len(content)) > limit || uint64(len(content)) != file.UncompressedSize64 {
			return fail(errors.New("release archive member size mismatch"))
		}
		files[file.Name] = content
	}
	receipt, err := ParseReleaseEvidence(files["release.json"], files["index.json"], wanted)
	if err != nil {
		return fail(err)
	}
	return receipt, files["index.json"], nil
}
