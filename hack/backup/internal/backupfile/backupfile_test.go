package backupfile

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/kubewharf/kubebrain/hack/backup/internal/record"
	"github.com/stretchr/testify/require"
)

func TestAtomicWriterAndVerifiedReader(t *testing.T) {
	path := filepath.Join(t.TempDir(), "backup.jsonl")
	writer, err := NewAtomicWriter(path, "/registry", 42)
	require.NoError(t, err)
	require.NoError(t, writer.AddLease(record.Lease{ID: 123, TTL: 30, GrantedTTL: 60}))
	require.NoError(t, writer.Add(record.Record{Key: "L3JlZ2lzdHJ5L2E=", Value: "YQ==", CreateRevision: 39, ModRevision: 40, Version: 2}))
	require.NoError(t, writer.Add(record.Record{Key: "L3JlZ2lzdHJ5L2I=", Value: "Yg==", CreateRevision: 41, ModRevision: 41, Version: 1, Lease: 123}))
	status, err := writer.Commit()
	require.NoError(t, err)
	require.Equal(t, 2, status.Records)
	require.Positive(t, status.CreatedAtUnix)

	verified, err := OpenVerified(path)
	require.NoError(t, err)
	defer verified.Close()
	require.Equal(t, Status{
		Format:        Format,
		Prefix:        "/registry",
		Revision:      42,
		CreatedAtUnix: status.CreatedAtUnix,
		Records:       2,
		Leases:        1,
		SHA256:        status.SHA256,
	}, verified.Status())
	var records []record.Record
	require.NoError(t, verified.Records(func(rec record.Record) error {
		records = append(records, rec)
		return nil
	}))
	require.Len(t, records, 2)
	var leases []record.Lease
	require.NoError(t, verified.Leases(func(lease record.Lease) error {
		leases = append(leases, lease)
		return nil
	}))
	require.Equal(t, []record.Lease{{Type: "lease", ID: 123, TTL: 30, GrantedTTL: 60}}, leases)
}

func TestOpenVerifiedReadsLegacyFormat(t *testing.T) {
	header := []byte("{\"type\":\"kubebrain.logical.v1\",\"prefix\":\"/registry\",\"revision\":42}\n")
	data := []byte("{\"key\":\"L3JlZ2lzdHJ5L2E=\",\"value\":\"YQ==\",\"mod_revision\":40,\"create_revision\":39,\"version\":1,\"lease\":0}\n")
	digest := sha256.Sum256(append(append([]byte(nil), header...), data...))
	footer := []byte("{\"type\":\"footer\",\"records\":1,\"sha256\":\"" + hex.EncodeToString(digest[:]) + "\"}\n")
	path := filepath.Join(t.TempDir(), "legacy.jsonl")
	require.NoError(t, os.WriteFile(path, append(append(header, data...), footer...), 0o600))

	status, err := Inspect(path)
	require.NoError(t, err)
	require.Equal(t, LegacyFormat, status.Format)
	require.Equal(t, 1, status.Records)
	require.Zero(t, status.Leases)
	require.Zero(t, status.CreatedAtUnix)
}

func TestOpenVerifiedReadsV2WithoutCreationTimestamp(t *testing.T) {
	header := []byte("{\"type\":\"kubebrain.logical.v2\",\"prefix\":\"/registry\",\"revision\":42}\n")
	digest := sha256.Sum256(header)
	footer := []byte("{\"type\":\"footer\",\"records\":0,\"sha256\":\"" + hex.EncodeToString(digest[:]) + "\"}\n")
	path := filepath.Join(t.TempDir(), "old-v2.jsonl")
	require.NoError(t, os.WriteFile(path, append(header, footer...), 0o600))

	status, err := Inspect(path)
	require.NoError(t, err)
	require.Equal(t, Format, status.Format)
	require.Zero(t, status.CreatedAtUnix)
}

func TestAtomicWriterCommitRejectsInvalidMetadata(t *testing.T) {
	tests := map[string]struct {
		populate func(*AtomicWriter) error
		want     string
	}{
		"undeclared lease": {
			populate: func(writer *AtomicWriter) error {
				return writer.Add(record.Record{Key: "L3JlZ2lzdHJ5L2E=", Value: "YQ==", CreateRevision: 1, ModRevision: 1, Version: 1, Lease: 123})
			},
			want: "backup record 1 references undeclared lease 123",
		},
		"duplicate lease": {
			populate: func(writer *AtomicWriter) error {
				if err := writer.AddLease(record.Lease{ID: 123, TTL: 30}); err != nil {
					return err
				}
				return writer.AddLease(record.Lease{ID: 123, TTL: 30})
			},
			want: "duplicate backup lease 123",
		},
		"outside prefix": {
			populate: func(writer *AtomicWriter) error {
				return writer.Add(record.Record{Key: "L291dHNpZGUva2V5", Value: "YQ==", CreateRevision: 1, ModRevision: 1, Version: 1})
			},
			want: `backup record 1 key "/outside/key" is outside manifest prefix "/registry"`,
		},
		"duplicate key": {
			populate: func(writer *AtomicWriter) error {
				if err := writer.Add(record.Record{Key: "L3JlZ2lzdHJ5L2tleQ==", Value: "YQ==", CreateRevision: 1, ModRevision: 1, Version: 1}); err != nil {
					return err
				}
				return writer.Add(record.Record{Key: "L3JlZ2lzdHJ5L2tleQ==", Value: "Yg==", CreateRevision: 1, ModRevision: 2, Version: 2})
			},
			want: `duplicate backup key "/registry/key"`,
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "backup.jsonl")
			writer, err := NewAtomicWriter(path, "/registry", 42)
			require.NoError(t, err)
			require.NoError(t, tc.populate(writer))
			_, err = writer.Commit()
			require.ErrorContains(t, err, tc.want)
			require.NoFileExists(t, path)
		})
	}
}

func TestOpenVerifiedRejectsInvalidLeaseMetadata(t *testing.T) {
	tests := map[string]struct {
		lines   [][]byte
		records int
		leases  int
		want    string
	}{
		"undeclared lease": {
			lines:   [][]byte{[]byte(`{"key":"L3JlZ2lzdHJ5L2E=","value":"YQ==","create_revision":1,"mod_revision":1,"version":1,"lease":123}`)},
			records: 1,
			want:    "backup record 1 references undeclared lease 123",
		},
		"duplicate lease": {
			lines: [][]byte{
				[]byte(`{"type":"lease","id":123,"ttl":30}`),
				[]byte(`{"type":"lease","id":123,"ttl":30}`),
			},
			leases: 2,
			want:   "duplicate backup lease 123",
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "backup.jsonl")
			require.NoError(t, os.WriteFile(path, backupJSONLLines(t, tc.lines, tc.records, tc.leases, ""), 0o600))

			_, err := OpenVerified(path)
			require.ErrorContains(t, err, tc.want)
		})
	}
}

func TestOpenVerifiedRejectsImpossibleMVCCMetadata(t *testing.T) {
	tests := map[string]struct {
		line string
		want string
	}{
		"empty key": {
			line: `{"key":"","value":"YQ==","create_revision":1,"mod_revision":1,"version":1}`,
			want: "key is empty",
		},
		"missing create revision": {
			line: `{"key":"L3JlZ2lzdHJ5L2E=","value":"YQ==","mod_revision":1,"version":1}`,
			want: "invalid MVCC metadata create=0 mod=1 version=1 snapshot=42",
		},
		"mod before create": {
			line: `{"key":"L3JlZ2lzdHJ5L2E=","value":"YQ==","create_revision":2,"mod_revision":1,"version":1}`,
			want: "invalid MVCC metadata create=2 mod=1 version=1 snapshot=42",
		},
		"mod after snapshot": {
			line: `{"key":"L3JlZ2lzdHJ5L2E=","value":"YQ==","create_revision":1,"mod_revision":43,"version":1}`,
			want: "invalid MVCC metadata create=1 mod=43 version=1 snapshot=42",
		},
		"missing version": {
			line: `{"key":"L3JlZ2lzdHJ5L2E=","value":"YQ==","create_revision":1,"mod_revision":1}`,
			want: "invalid MVCC metadata create=1 mod=1 version=0 snapshot=42",
		},
		"version cannot fit": {
			line: `{"key":"L3JlZ2lzdHJ5L2E=","value":"YQ==","create_revision":40,"mod_revision":41,"version":3}`,
			want: "version 3 cannot fit between create revision 40 and mod revision 41",
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "backup.jsonl")
			require.NoError(t, os.WriteFile(path, backupJSONL(t, []byte(tc.line), 1, 0, ""), 0o600))

			_, err := OpenVerified(path)
			require.ErrorContains(t, err, tc.want)
		})
	}
}

func TestAddLeaseAcceptsEtcdPromotionExtensionAboveGrantedTTL(t *testing.T) {
	writer, err := NewAtomicWriter(filepath.Join(t.TempDir(), "backup.jsonl"), "/registry", 42)
	require.NoError(t, err)
	defer func() { _ = writer.Abort() }()
	require.NoError(t, writer.AddLease(record.Lease{ID: 123, TTL: 30, GrantedTTL: 29}))
}

func TestAddLeaseBoundsGrantedTTLAtEtcdMaximum(t *testing.T) {
	writer, err := NewAtomicWriter(filepath.Join(t.TempDir(), "backup.jsonl"), "/registry", 42)
	require.NoError(t, err)
	defer func() { _ = writer.Abort() }()
	require.NoError(t, writer.AddLease(record.Lease{
		ID: 123, TTL: maxLeaseTTLSeconds, GrantedTTL: maxLeaseTTLSeconds,
	}))
	require.ErrorContains(t, writer.AddLease(record.Lease{
		ID: 124, TTL: 30, GrantedTTL: maxLeaseTTLSeconds + 1,
	}), "invalid lease")
}

func TestOpenVerifiedRejectsOversizedGrantedTTL(t *testing.T) {
	contents := backupJSONL(t, []byte(fmt.Sprintf(
		`{"type":"lease","id":123,"ttl":%d,"granted_ttl":%d}`,
		30, maxLeaseTTLSeconds+1,
	)), 0, 1, "")
	path := filepath.Join(t.TempDir(), "oversized-grant.jsonl")
	require.NoError(t, os.WriteFile(path, contents, 0o600))

	_, err := OpenVerified(path)
	require.ErrorContains(t, err, fmt.Sprintf("granted_ttl=%d", maxLeaseTTLSeconds+1))
}

func TestOpenVerifiedRejectsTruncatedAndCorruptBackup(t *testing.T) {
	path := filepath.Join(t.TempDir(), "backup.jsonl")
	writer, err := NewAtomicWriter(path, "/registry", 42)
	require.NoError(t, err)
	require.NoError(t, writer.Add(record.Record{Key: "L3JlZ2lzdHJ5L2E=", Value: "YQ==", CreateRevision: 1, ModRevision: 1, Version: 1}))
	_, err = writer.Commit()
	require.NoError(t, err)
	complete, err := os.ReadFile(path)
	require.NoError(t, err)

	tests := map[string]struct {
		contents []byte
		want     string
	}{
		"missing footer": {contents: complete[:len(complete)/2], want: "invalid backup line"},
		"corrupt record": {contents: []byte(`{"type":"kubebrain.logical.v1","prefix":"/registry","revision":42}
{"key":"L3JlZ2lzdHJ5L2E=","value":"corrupt","mod_revision":1,"create_revision":1,"version":1,"lease":0}
{"type":"footer","records":1,"sha256":"bad"}
`), want: "invalid backup record 1 value"},
		"trailing data": {contents: append(append([]byte(nil), complete...), []byte("{}\n")...), want: "backup contains data after footer"},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			corruptPath := filepath.Join(t.TempDir(), "corrupt.jsonl")
			require.NoError(t, os.WriteFile(corruptPath, tc.contents, 0o600))
			_, err := OpenVerified(corruptPath)
			require.ErrorContains(t, err, tc.want)
		})
	}
}

func TestOpenVerifiedRejectsUnknownJSONFields(t *testing.T) {
	tests := map[string][]byte{
		"header": []byte(`{"type":"kubebrain.logical.v2","prefix":"/registry","revision":42,"unexpected":true}
{"type":"footer","records":0,"leases":0,"sha256":"ignored"}
`),
		"record": backupJSONL(t, []byte(`{"key":"L3JlZ2lzdHJ5L2E=","value":"YQ==","mod_revision":40,"create_revision":39,"version":1,"lease":0,"unexpected":true}`), 1, 0, ""),
		"lease":  backupJSONL(t, []byte(`{"type":"lease","id":123,"ttl":30,"granted_ttl":60,"unexpected":true}`), 0, 1, ""),
		"footer": backupJSONL(t, nil, 0, 0, `,"unexpected":true`),
	}
	for name, contents := range tests {
		t.Run(name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "unknown.jsonl")
			require.NoError(t, os.WriteFile(path, contents, 0o600))
			_, err := OpenVerified(path)
			require.ErrorContains(t, err, "unknown field")
		})
	}
}

func TestAbortDoesNotReplaceExistingBackup(t *testing.T) {
	path := filepath.Join(t.TempDir(), "backup.jsonl")
	require.NoError(t, os.WriteFile(path, []byte("existing"), 0o600))
	writer, err := NewAtomicWriter(path, "/registry", 1)
	require.NoError(t, err)
	require.NoError(t, writer.Add(record.Record{Key: "L3JlZ2lzdHJ5L2E=", CreateRevision: 1, ModRevision: 1, Version: 1}))
	require.NoError(t, writer.Abort())

	contents, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Equal(t, "existing", string(contents))
	matches, err := filepath.Glob(filepath.Join(filepath.Dir(path), ".backup.jsonl.tmp-*"))
	require.NoError(t, err)
	require.Empty(t, matches)
}

func TestCommitDoesNotOverwriteExistingBackup(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "backup.jsonl")
	require.NoError(t, os.WriteFile(path, []byte("existing\n"), 0o600))
	writer, err := NewAtomicWriter(path, "/registry", 1)
	require.NoError(t, err)
	require.NoError(t, writer.Add(record.Record{Key: "L3JlZ2lzdHJ5L2E=", CreateRevision: 1, ModRevision: 1, Version: 1}))

	_, err = writer.Commit()
	require.ErrorIs(t, err, os.ErrExist)
	require.ErrorContains(t, err, "backup output already exists")
	contents, readErr := os.ReadFile(path)
	require.NoError(t, readErr)
	require.Equal(t, "existing\n", string(contents))
	matches, globErr := filepath.Glob(filepath.Join(dir, ".backup.jsonl.tmp-*"))
	require.NoError(t, globErr)
	require.Empty(t, matches)
}

func TestOpenVerifiedRejectsManifestMismatchAndDuplicateKeys(t *testing.T) {
	tests := map[string]struct {
		lines [][]byte
		want  string
	}{
		"outside prefix": {
			lines: [][]byte{[]byte(`{"key":"L291dHNpZGUva2V5","value":"YQ==","create_revision":1,"mod_revision":1,"version":1}`)},
			want:  `backup record 1 key "/outside/key" is outside manifest prefix "/registry"`,
		},
		"duplicate key": {
			lines: [][]byte{
				[]byte(`{"key":"L3JlZ2lzdHJ5L2tleQ==","value":"YQ==","create_revision":1,"mod_revision":1,"version":1}`),
				[]byte(`{"key":"L3JlZ2lzdHJ5L2tleQ==","value":"Yg==","create_revision":1,"mod_revision":2,"version":2}`),
			},
			want: `duplicate backup key "/registry/key"`,
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "backup.jsonl")
			require.NoError(t, os.WriteFile(path, backupJSONLLines(t, tc.lines, len(tc.lines), 0, ""), 0o600))

			_, err := OpenVerified(path)
			require.ErrorContains(t, err, tc.want)
		})
	}
}

func backupJSONL(t *testing.T, line []byte, records, leases int, footerExtra string) []byte {
	t.Helper()
	var lines [][]byte
	if line != nil {
		lines = [][]byte{line}
	}
	return backupJSONLLines(t, lines, records, leases, footerExtra)
}

func backupJSONLLines(t *testing.T, lines [][]byte, records, leases int, footerExtra string) []byte {
	t.Helper()
	header := []byte("{\"type\":\"kubebrain.logical.v2\",\"prefix\":\"/registry\",\"revision\":42}\n")
	digest := sha256.New()
	_, err := digest.Write(header)
	require.NoError(t, err)
	var body []byte
	for _, line := range lines {
		body = append(body, line...)
		body = append(body, '\n')
		_, err = digest.Write(line)
		require.NoError(t, err)
		_, err = digest.Write([]byte{'\n'})
		require.NoError(t, err)
	}
	sum := hex.EncodeToString(digest.Sum(nil))
	footer := []byte(fmt.Sprintf(
		"{\"type\":\"footer\",\"records\":%d,\"leases\":%d,\"sha256\":\"%s\"%s}\n",
		records, leases, sum, footerExtra,
	))
	return append(append(header, body...), footer...)
}
