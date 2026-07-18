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

	"github.com/kubewharf/kubebrain/hack/backup/internal/record"
)

const Format = "kubebrain.logical.v1"

type Header struct {
	Type     string `json:"type"`
	Prefix   string `json:"prefix"`
	Revision int64  `json:"revision"`
}

type Footer struct {
	Type    string `json:"type"`
	Records int    `json:"records"`
	SHA256  string `json:"sha256"`
}

type Status struct {
	Format   string `json:"format"`
	Prefix   string `json:"prefix"`
	Revision int64  `json:"revision"`
	Records  int    `json:"records"`
	SHA256   string `json:"sha256"`
}

type AtomicWriter struct {
	output   string
	temp     *os.File
	hash     hash.Hash
	records  int
	prefix   string
	revision int64
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
		temp.Close()
		os.Remove(temp.Name())
		return nil, err
	}
	writer := &AtomicWriter{
		output: output, temp: temp, hash: sha256.New(),
		prefix: prefix, revision: revision,
	}
	header := Header{Type: Format, Prefix: prefix, Revision: revision}
	if err := writer.writeHashedJSON(header); err != nil {
		writer.Abort()
		return nil, err
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

func (w *AtomicWriter) Commit() (Status, error) {
	if w.closed {
		return Status{}, errors.New("backup writer is closed")
	}
	sum := hex.EncodeToString(w.hash.Sum(nil))
	footer := Footer{Type: "footer", Records: w.records, SHA256: sum}
	if err := writeJSONLine(w.temp, footer); err != nil {
		w.Abort()
		return Status{}, err
	}
	if err := w.temp.Sync(); err != nil {
		w.Abort()
		return Status{}, err
	}
	if err := w.temp.Close(); err != nil {
		w.closed = true
		os.Remove(w.temp.Name())
		return Status{}, err
	}
	w.closed = true
	if err := os.Rename(w.temp.Name(), w.output); err != nil {
		os.Remove(w.temp.Name())
		return Status{}, err
	}
	dir, err := os.Open(filepath.Dir(w.output))
	if err != nil {
		return Status{}, err
	}
	if err := dir.Sync(); err != nil {
		dir.Close()
		return Status{}, err
	}
	if err := dir.Close(); err != nil {
		return Status{}, err
	}
	return Status{
		Format: Format, Prefix: w.prefix, Revision: w.revision,
		Records: w.records, SHA256: sum,
	}, nil
}

func (w *AtomicWriter) Abort() {
	if w == nil || w.closed {
		return
	}
	w.closed = true
	_ = w.temp.Close()
	_ = os.Remove(w.temp.Name())
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

func OpenVerified(path string) (*Verified, error) {
	source, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer source.Close()

	copyFile, err := os.CreateTemp("", "kubebrain-logical-backup-verified-*")
	if err != nil {
		return nil, err
	}
	cleanup := func() {
		copyFile.Close()
		os.Remove(copyFile.Name())
	}
	if err := copyFile.Chmod(0o600); err != nil {
		cleanup()
		return nil, err
	}
	if _, err := io.Copy(copyFile, source); err != nil {
		cleanup()
		return nil, err
	}
	if _, err := copyFile.Seek(0, io.SeekStart); err != nil {
		cleanup()
		return nil, err
	}
	status, err := validate(copyFile)
	if err != nil {
		cleanup()
		return nil, err
	}
	if _, err := copyFile.Seek(0, io.SeekStart); err != nil {
		cleanup()
		return nil, err
	}
	return &Verified{file: copyFile, path: copyFile.Name(), status: status}, nil
}

func Inspect(path string) (Status, error) {
	verified, err := OpenVerified(path)
	if err != nil {
		return Status{}, err
	}
	defer verified.Close()
	return verified.Status(), nil
}

func (v *Verified) Status() Status {
	return v.status
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
		var probe struct {
			Type string `json:"type"`
		}
		if err := json.Unmarshal(line, &probe); err != nil {
			return err
		}
		if probe.Type == "footer" {
			return nil
		}
		if probe.Type != "" {
			return fmt.Errorf("unexpected backup record type %q", probe.Type)
		}
		var rec record.Record
		if err := json.Unmarshal(line, &rec); err != nil {
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

func (v *Verified) Close() error {
	err := v.file.Close()
	removeErr := os.Remove(v.path)
	if err != nil {
		return err
	}
	return removeErr
}

func validate(reader io.Reader) (Status, error) {
	scanner := bufio.NewScanner(reader)
	scanner.Buffer(make([]byte, 1024*1024), 64*1024*1024)
	if !scanner.Scan() {
		return Status{}, errors.New("backup header is missing")
	}
	headerLine := append([]byte(nil), scanner.Bytes()...)
	var header Header
	if err := json.Unmarshal(headerLine, &header); err != nil {
		return Status{}, fmt.Errorf("invalid backup header: %w", err)
	}
	if header.Type != Format {
		return Status{}, fmt.Errorf("unsupported backup format %q", header.Type)
	}
	if header.Revision <= 0 {
		return Status{}, fmt.Errorf("invalid backup revision %d", header.Revision)
	}

	digest := sha256.New()
	digest.Write(headerLine)
	digest.Write([]byte{'\n'})
	records := 0
	seenKeys := make(map[string]struct{})
	var footer Footer
	foundFooter := false
	for scanner.Scan() {
		line := append([]byte(nil), scanner.Bytes()...)
		var probe struct {
			Type string `json:"type"`
		}
		if err := json.Unmarshal(line, &probe); err != nil {
			return Status{}, fmt.Errorf("invalid backup line %d: %w", records+2, err)
		}
		if probe.Type == "footer" {
			if err := json.Unmarshal(line, &footer); err != nil {
				return Status{}, fmt.Errorf("invalid backup footer: %w", err)
			}
			foundFooter = true
			if scanner.Scan() {
				return Status{}, errors.New("backup contains data after footer")
			}
			break
		}
		if probe.Type != "" {
			return Status{}, fmt.Errorf("unexpected backup record type %q", probe.Type)
		}
		var rec record.Record
		if err := json.Unmarshal(line, &rec); err != nil {
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
	actualHash := hex.EncodeToString(digest.Sum(nil))
	if footer.SHA256 != actualHash {
		return Status{}, fmt.Errorf("backup SHA-256 mismatch: footer=%s actual=%s", footer.SHA256, actualHash)
	}
	return Status{
		Format:   header.Type,
		Prefix:   header.Prefix,
		Revision: header.Revision,
		Records:  records,
		SHA256:   actualHash,
	}, nil
}
