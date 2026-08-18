package backupfile

import (
	"bufio"
	"bytes"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"hash"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/kubewharf/kubebrain/hack/backup/internal/record"
)

const (
	Format       = "kubebrain.logical.v2"
	LegacyFormat = "kubebrain.logical.v1"
	// Keep this aligned with upstream etcd server/lease.MaxLeaseTTL.
	maxLeaseTTLSeconds int64 = 9000000000
)

type Header struct {
	Type          string `json:"type"`
	Prefix        string `json:"prefix"`
	Revision      int64  `json:"revision"`
	CreatedAtUnix int64  `json:"created_at_unix,omitempty"`
}

type Footer struct {
	Type    string `json:"type"`
	Records int    `json:"records"`
	Leases  int    `json:"leases,omitempty"`
	SHA256  string `json:"sha256"`
}

type Status struct {
	Format        string `json:"format"`
	Prefix        string `json:"prefix"`
	Revision      int64  `json:"revision"`
	CreatedAtUnix int64  `json:"created_at_unix,omitempty"`
	Records       int    `json:"records"`
	Leases        int    `json:"leases"`
	SHA256        string `json:"sha256"`
}

type AtomicWriter struct {
	output   string
	temp     *os.File
	hash     hash.Hash
	records  int
	leases   int
	prefix   string
	revision int64
	created  int64
	closed   bool
}

func NewAtomicWriter(output, prefix string, revision int64) (*AtomicWriter, error) {
	if output == "" {
		return nil, errors.New("output path is empty")
	}
	dir := filepath.Dir(output)
	temp, err := os.CreateTemp(dir, "."+filepath.Base(output)+".tmp-*")
	if err != nil {
		return nil, err
	}
	if err := temp.Chmod(0o600); err != nil {
		return nil, errors.Join(err, temp.Close(), os.Remove(temp.Name()))
	}
	writer := &AtomicWriter{
		output: output, temp: temp, hash: sha256.New(),
		prefix: prefix, revision: revision, created: time.Now().UTC().Unix(),
	}
	header := Header{
		Type: Format, Prefix: prefix, Revision: revision, CreatedAtUnix: writer.created,
	}
	if err := writer.writeHashedJSON(header); err != nil {
		return nil, errors.Join(err, writer.abort())
	}
	return writer, nil
}

func (w *AtomicWriter) Add(rec record.Record) error {
	if w.closed {
		return errors.New("backup writer is closed")
	}
	if err := w.writeHashedJSON(rec); err != nil {
		return err
	}
	w.records++
	return nil
}

func (w *AtomicWriter) AddLease(lease record.Lease) error {
	if w.closed {
		return errors.New("backup writer is closed")
	}
	lease.Type = "lease"
	if lease.ID == 0 || lease.TTL <= 0 || lease.GrantedTTL < 0 || lease.GrantedTTL > maxLeaseTTLSeconds {
		return fmt.Errorf("invalid lease id=%d ttl=%d granted_ttl=%d", lease.ID, lease.TTL, lease.GrantedTTL)
	}
	if err := w.writeHashedJSON(lease); err != nil {
		return err
	}
	w.leases++
	return nil
}

func (w *AtomicWriter) Commit() (status Status, retErr error) {
	if w.closed {
		return Status{}, errors.New("backup writer is closed")
	}
	sum := hex.EncodeToString(w.hash.Sum(nil))
	footer := Footer{Type: "footer", Records: w.records, Leases: w.leases, SHA256: sum}
	if err := writeJSONLine(w.temp, footer); err != nil {
		return Status{}, errors.Join(err, w.abort())
	}
	if err := w.temp.Sync(); err != nil {
		return Status{}, errors.Join(err, w.abort())
	}
	if err := w.temp.Close(); err != nil {
		w.closed = true
		return Status{}, errors.Join(err, os.Remove(w.temp.Name()))
	}
	w.closed = true
	tempName := w.temp.Name()
	defer func() {
		if removeErr := os.Remove(tempName); removeErr != nil && !errors.Is(removeErr, os.ErrNotExist) {
			retErr = errors.Join(retErr, removeErr)
		}
	}()
	if err := os.Link(tempName, w.output); err != nil {
		if errors.Is(err, os.ErrExist) {
			return Status{}, fmt.Errorf("backup output already exists %q: %w", w.output, os.ErrExist)
		}
		return Status{}, err
	}
	if err := os.Remove(tempName); err != nil {
		return Status{}, fmt.Errorf("remove published backup temporary link: %w", err)
	}
	dir, err := os.Open(filepath.Dir(w.output))
	if err != nil {
		return Status{}, err
	}
	if err := dir.Sync(); err != nil {
		return Status{}, errors.Join(err, dir.Close())
	}
	if err := dir.Close(); err != nil {
		return Status{}, err
	}
	return Status{
		Format: Format, Prefix: w.prefix, Revision: w.revision, CreatedAtUnix: w.created,
		Records: w.records, Leases: w.leases, SHA256: sum,
	}, nil
}

func (w *AtomicWriter) Abort() error { return w.abort() }

func (w *AtomicWriter) abort() error {
	if w == nil || w.closed {
		return nil
	}
	w.closed = true
	return errors.Join(w.temp.Close(), os.Remove(w.temp.Name()))
}

func (w *AtomicWriter) writeHashedJSON(value any) error {
	var line bytes.Buffer
	if err := writeJSONLine(&line, value); err != nil {
		return err
	}
	if _, err := w.hash.Write(line.Bytes()); err != nil {
		return err
	}
	_, err := w.temp.Write(line.Bytes())
	return err
}

func writeJSONLine(writer io.Writer, value any) error {
	encoder := json.NewEncoder(writer)
	encoder.SetEscapeHTML(false)
	return encoder.Encode(value)
}

type Verified struct {
	file   *os.File
	path   string
	status Status
}

func OpenVerified(path string) (verified *Verified, retErr error) {
	source, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer func() {
		if closeErr := source.Close(); closeErr != nil {
			retErr = errors.Join(retErr, closeErr)
			if verified != nil {
				retErr = errors.Join(retErr, verified.Close())
				verified = nil
			}
		}
	}()

	copyFile, err := os.CreateTemp("", "kubebrain-logical-backup-verified-*")
	if err != nil {
		return nil, err
	}
	cleanup := func() error {
		return errors.Join(copyFile.Close(), os.Remove(copyFile.Name()))
	}
	if err := copyFile.Chmod(0o600); err != nil {
		return nil, errors.Join(err, cleanup())
	}
	if _, err := io.Copy(copyFile, source); err != nil {
		return nil, errors.Join(err, cleanup())
	}
	if _, err := copyFile.Seek(0, io.SeekStart); err != nil {
		return nil, errors.Join(err, cleanup())
	}
	status, err := validate(copyFile)
	if err != nil {
		return nil, errors.Join(err, cleanup())
	}
	if _, err := copyFile.Seek(0, io.SeekStart); err != nil {
		return nil, errors.Join(err, cleanup())
	}
	return &Verified{file: copyFile, path: copyFile.Name(), status: status}, nil
}

func Inspect(path string) (status Status, retErr error) {
	verified, err := OpenVerified(path)
	if err != nil {
		return Status{}, err
	}
	defer func() { retErr = errors.Join(retErr, verified.Close()) }()
	return verified.Status(), nil
}

func (v *Verified) Status() Status {
	return v.status
}

func (v *Verified) Path() string {
	return v.path
}

func (v *Verified) Records(fn func(record.Record) error) error {
	if _, err := v.file.Seek(0, io.SeekStart); err != nil {
		return err
	}
	scanner := bufio.NewScanner(v.file)
	scanner.Buffer(make([]byte, 1024*1024), 64*1024*1024)
	if !scanner.Scan() {
		return errors.New("backup header is missing")
	}
	for scanner.Scan() {
		line := scanner.Bytes()
		lineType, err := backupLineType(line)
		if err != nil {
			return err
		}
		if lineType == "footer" {
			return nil
		}
		if lineType == "lease" {
			continue
		}
		if lineType != "" {
			return fmt.Errorf("unexpected backup record type %q", lineType)
		}
		var rec record.Record
		if err := decodeBackupLine(line, &rec, true); err != nil {
			return err
		}
		if err := fn(rec); err != nil {
			return err
		}
	}
	if err := scanner.Err(); err != nil {
		return err
	}
	return errors.New("backup footer is missing")
}

func (v *Verified) Leases(fn func(record.Lease) error) error {
	if _, err := v.file.Seek(0, io.SeekStart); err != nil {
		return err
	}
	scanner := bufio.NewScanner(v.file)
	scanner.Buffer(make([]byte, 1024*1024), 64*1024*1024)
	if !scanner.Scan() {
		return errors.New("backup header is missing")
	}
	for scanner.Scan() {
		line := scanner.Bytes()
		lineType, err := backupLineType(line)
		if err != nil {
			return err
		}
		switch lineType {
		case "footer":
			return nil
		case "lease":
			var lease record.Lease
			if err := decodeBackupLine(line, &lease, true); err != nil {
				return err
			}
			if err := fn(lease); err != nil {
				return err
			}
		case "":
		default:
			return fmt.Errorf("unexpected backup record type %q", lineType)
		}
	}
	if err := scanner.Err(); err != nil {
		return err
	}
	return errors.New("backup footer is missing")
}

func (v *Verified) Close() error {
	return errors.Join(v.file.Close(), os.Remove(v.path))
}

func validate(reader io.Reader) (Status, error) {
	scanner := bufio.NewScanner(reader)
	scanner.Buffer(make([]byte, 1024*1024), 64*1024*1024)
	if !scanner.Scan() {
		return Status{}, errors.New("backup header is missing")
	}
	headerLine := append([]byte(nil), scanner.Bytes()...)
	var header Header
	if err := decodeBackupLine(headerLine, &header, true); err != nil {
		return Status{}, fmt.Errorf("invalid backup header: %w", err)
	}
	if header.Type != Format && header.Type != LegacyFormat {
		return Status{}, fmt.Errorf("unsupported backup format %q", header.Type)
	}
	if header.Revision <= 0 {
		return Status{}, fmt.Errorf("invalid backup revision %d", header.Revision)
	}
	if header.CreatedAtUnix < 0 {
		return Status{}, fmt.Errorf("invalid backup creation timestamp %d", header.CreatedAtUnix)
	}

	digest := sha256.New()
	digest.Write(headerLine)
	digest.Write([]byte{'\n'})
	records := 0
	leases := 0
	seenKeys := make(map[string]struct{})
	seenLeases := make(map[int64]struct{})
	var footer Footer
	foundFooter := false
	for scanner.Scan() {
		line := append([]byte(nil), scanner.Bytes()...)
		lineType, err := backupLineType(line)
		if err != nil {
			return Status{}, fmt.Errorf("invalid backup line %d: %w", records+2, err)
		}
		if lineType == "footer" {
			if err := decodeBackupLine(line, &footer, true); err != nil {
				return Status{}, fmt.Errorf("invalid backup footer: %w", err)
			}
			foundFooter = true
			if scanner.Scan() {
				return Status{}, errors.New("backup contains data after footer")
			}
			break
		}
		if lineType == "lease" {
			if header.Type != Format {
				return Status{}, fmt.Errorf("lease metadata is not supported in backup format %q", header.Type)
			}
			var lease record.Lease
			if err := decodeBackupLine(line, &lease, true); err != nil {
				return Status{}, fmt.Errorf("invalid backup lease %d: %w", leases+1, err)
			}
			if lease.ID == 0 || lease.TTL <= 0 || lease.GrantedTTL < 0 || lease.GrantedTTL > maxLeaseTTLSeconds {
				return Status{}, fmt.Errorf("invalid backup lease id=%d ttl=%d granted_ttl=%d", lease.ID, lease.TTL, lease.GrantedTTL)
			}
			if _, exists := seenLeases[lease.ID]; exists {
				return Status{}, fmt.Errorf("duplicate backup lease %d", lease.ID)
			}
			seenLeases[lease.ID] = struct{}{}
			digest.Write(line)
			digest.Write([]byte{'\n'})
			leases++
			continue
		}
		if lineType != "" {
			return Status{}, fmt.Errorf("unexpected backup record type %q", lineType)
		}
		var rec record.Record
		if err := decodeBackupLine(line, &rec, true); err != nil {
			return Status{}, fmt.Errorf("invalid backup record %d: %w", records+1, err)
		}
		key, err := base64.StdEncoding.DecodeString(rec.Key)
		if err != nil {
			return Status{}, fmt.Errorf("invalid backup record %d key: %w", records+1, err)
		}
		if !bytes.HasPrefix(key, []byte(header.Prefix)) {
			return Status{}, fmt.Errorf("backup record %d key %q is outside manifest prefix %q",
				records+1, key, header.Prefix)
		}
		if _, exists := seenKeys[string(key)]; exists {
			return Status{}, fmt.Errorf("duplicate backup key %q", key)
		}
		seenKeys[string(key)] = struct{}{}
		if _, err := base64.StdEncoding.DecodeString(rec.Value); err != nil {
			return Status{}, fmt.Errorf("invalid backup record %d value: %w", records+1, err)
		}
		if header.Type == Format && rec.Lease != 0 {
			if _, exists := seenLeases[rec.Lease]; !exists {
				return Status{}, fmt.Errorf("backup record %d references undeclared lease %d", records+1, rec.Lease)
			}
		}
		digest.Write(line)
		digest.Write([]byte{'\n'})
		records++
	}
	if err := scanner.Err(); err != nil {
		return Status{}, err
	}
	if !foundFooter {
		return Status{}, errors.New("backup footer is missing")
	}
	if footer.Records != records {
		return Status{}, fmt.Errorf("backup record count mismatch: footer=%d actual=%d", footer.Records, records)
	}
	if footer.Leases != leases {
		return Status{}, fmt.Errorf("backup lease count mismatch: footer=%d actual=%d", footer.Leases, leases)
	}
	actualHash := hex.EncodeToString(digest.Sum(nil))
	if footer.SHA256 != actualHash {
		return Status{}, fmt.Errorf("backup SHA-256 mismatch: footer=%s actual=%s", footer.SHA256, actualHash)
	}
	return Status{
		Format:        header.Type,
		Prefix:        header.Prefix,
		Revision:      header.Revision,
		CreatedAtUnix: header.CreatedAtUnix,
		Records:       records,
		Leases:        leases,
		SHA256:        actualHash,
	}, nil
}

func backupLineType(line []byte) (string, error) {
	var object map[string]json.RawMessage
	if err := decodeBackupLine(line, &object, false); err != nil {
		return "", err
	}
	if object == nil {
		return "", errors.New("backup line is not a JSON object")
	}
	raw, exists := object["type"]
	if !exists {
		return "", nil
	}
	var lineType string
	if err := json.Unmarshal(raw, &lineType); err != nil {
		return "", fmt.Errorf("backup line type is invalid: %w", err)
	}
	return lineType, nil
}

func decodeBackupLine(line []byte, target any, disallowUnknown bool) error {
	decoder := json.NewDecoder(bytes.NewReader(line))
	if disallowUnknown {
		decoder.DisallowUnknownFields()
	}
	if err := decoder.Decode(target); err != nil {
		return err
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		return errors.New("backup line contains trailing JSON")
	}
	return nil
}
