package pitrinventory

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"strings"
	"unicode"
	"unicode/utf8"
)

const (
	Format   = "kubebrain.native-pitr-object-inventory.v1"
	MaxBytes = 64 << 20
)

type Entry struct {
	Name            string `json:"name"`
	ObjectKey       string `json:"object_key"`
	VersionID       string `json:"version_id"`
	Bytes           int64  `json:"bytes"`
	SHA256          string `json:"sha256"`
	RetentionMode   string `json:"retention_mode"`
	RetainUntilUnix int64  `json:"retain_until_unix"`
}

type Receipt struct {
	Format                string  `json:"format"`
	ObjectStoreID         string  `json:"object_store_id"`
	Bucket                string  `json:"bucket"`
	Prefix                string  `json:"prefix"`
	Entries               []Entry `json:"entries"`
	ObjectCount           int     `json:"object_count"`
	TotalBytes            uint64  `json:"total_bytes"`
	Pages                 int     `json:"pages"`
	DeleteMarkers         int     `json:"delete_markers"`
	PaginationExhausted   bool    `json:"pagination_exhausted"`
	ExactVersionsVerified bool    `json:"exact_versions_verified"`
	MinRetainUntilUnix    int64   `json:"min_retain_until_unix"`
	CheckedAtUnix         int64   `json:"checked_at_unix"`
}

func (r Receipt) Validate() error {
	if r.Format != Format || !safe(r.ObjectStoreID) || !safe(r.Bucket) || !relativePrefix(r.Prefix) || r.Entries == nil || r.ObjectCount != len(r.Entries) || r.Pages <= 0 || r.DeleteMarkers != 0 || !r.PaginationExhausted || !r.ExactVersionsVerified || r.CheckedAtUnix <= 0 || r.MinRetainUntilUnix <= r.CheckedAtUnix {
		return errors.New("native PITR object inventory receipt is incomplete")
	}
	var total uint64
	previous := ""
	for _, entry := range r.Entries {
		if !relative(entry.Name) || entry.ObjectKey != strings.TrimSuffix(r.Prefix, "/")+"/"+entry.Name || !relative(entry.ObjectKey) || !safe(entry.VersionID) || entry.Bytes <= 0 || !hexSHA256(entry.SHA256) || (entry.RetentionMode != "COMPLIANCE" && entry.RetentionMode != "GOVERNANCE") || entry.RetainUntilUnix < r.MinRetainUntilUnix || entry.Name <= previous {
			return errors.New("native PITR object inventory contains an invalid, duplicate, or unsorted entry")
		}
		previous = entry.Name
		if ^uint64(0)-total < uint64(entry.Bytes) {
			return errors.New("native PITR object inventory byte total overflows uint64")
		}
		total += uint64(entry.Bytes)
	}
	if total != r.TotalBytes {
		return errors.New("native PITR object inventory byte total does not match")
	}
	return nil
}

func Decode(reader io.Reader) (Receipt, error) {
	var receipt Receipt
	decoder := json.NewDecoder(reader)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&receipt); err != nil {
		return receipt, fmt.Errorf("decode native PITR object inventory: %w", err)
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return receipt, errors.New("native PITR object inventory has trailing JSON")
	}
	if err := receipt.Validate(); err != nil {
		return receipt, err
	}
	return receipt, nil
}

func ReadCanonical(filePath string) (Receipt, []byte, error) {
	var zero Receipt
	f, err := os.Open(filePath)
	if err != nil {
		return zero, nil, err
	}
	defer f.Close()
	b, err := io.ReadAll(io.LimitReader(f, MaxBytes+1))
	if err != nil {
		return zero, nil, err
	}
	if len(b) > MaxBytes {
		return zero, nil, fmt.Errorf("native PITR object inventory exceeds %d bytes", MaxBytes)
	}
	receipt, err := Decode(bytes.NewReader(b))
	if err != nil {
		return zero, nil, err
	}
	canonical, err := json.Marshal(receipt)
	if err != nil {
		return zero, nil, err
	}
	canonical = append(canonical, '\n')
	if !bytes.Equal(b, canonical) {
		return zero, nil, errors.New("native PITR object inventory is not canonical")
	}
	return receipt, b, nil
}

func safe(value string) bool {
	return value != "" && utf8.ValidString(value) && strings.IndexFunc(value, func(r rune) bool { return unicode.IsSpace(r) || unicode.IsControl(r) }) < 0
}

func relative(value string) bool {
	return safe(value) && !strings.HasPrefix(value, "/") && value != "." && value != ".." && !strings.HasPrefix(value, "../") && path.Clean(value) == value
}

func relativePrefix(value string) bool {
	return relative(strings.TrimSuffix(value, "/"))
}

func hexSHA256(value string) bool {
	if len(value) != 64 {
		return false
	}
	for _, c := range []byte(value) {
		if !((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f')) {
			return false
		}
	}
	return true
}
