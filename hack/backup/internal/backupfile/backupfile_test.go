package backupfile

import (
	"crypto/sha256"
	"encoding/hex"
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
	require.NoError(t, writer.Add(record.Record{Key: "L3JlZ2lzdHJ5L2E=", Value: "YQ==", ModRevision: 40}))
	require.NoError(t, writer.Add(record.Record{Key: "L3JlZ2lzdHJ5L2I=", Value: "Yg==", ModRevision: 41, Lease: 123}))
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

func TestOpenVerifiedRejectsInvalidLeaseMetadata(t *testing.T) {
	tests := map[string]func(*AtomicWriter) error{
		"undeclared lease": func(writer *AtomicWriter) error {
			return writer.Add(record.Record{Key: "L3JlZ2lzdHJ5L2E=", Value: "YQ==", Lease: 123})
		},
		"duplicate lease": func(writer *AtomicWriter) error {
			if err := writer.AddLease(record.Lease{ID: 123, TTL: 30}); err != nil {
				return err
			}
			return writer.AddLease(record.Lease{ID: 123, TTL: 30})
		},
	}
	for name, populate := range tests {
		t.Run(name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "backup.jsonl")
			writer, err := NewAtomicWriter(path, "/registry", 42)
			require.NoError(t, err)
			require.NoError(t, populate(writer))
			_, err = writer.Commit()
			require.NoError(t, err)

			_, err = OpenVerified(path)
			require.Error(t, err)
		})
	}
}

func TestAddLeaseRejectsGrantedTTLBelowRemainingTTL(t *testing.T) {
	writer, err := NewAtomicWriter(filepath.Join(t.TempDir(), "backup.jsonl"), "/registry", 42)
	require.NoError(t, err)
	defer writer.Abort()
	require.ErrorContains(t, writer.AddLease(record.Lease{ID: 123, TTL: 30, GrantedTTL: 29}), "granted_ttl")
}

func TestOpenVerifiedRejectsTruncatedAndCorruptBackup(t *testing.T) {
	path := filepath.Join(t.TempDir(), "backup.jsonl")
	writer, err := NewAtomicWriter(path, "/registry", 42)
	require.NoError(t, err)
	require.NoError(t, writer.Add(record.Record{Key: "L3JlZ2lzdHJ5L2E=", Value: "YQ=="}))
	_, err = writer.Commit()
	require.NoError(t, err)
	complete, err := os.ReadFile(path)
	require.NoError(t, err)

	tests := map[string][]byte{
		"missing footer": complete[:len(complete)/2],
		"corrupt record": []byte(`{"type":"kubebrain.logical.v1","prefix":"/registry","revision":42}
{"key":"L3JlZ2lzdHJ5L2E=","value":"corrupt","mod_revision":0,"create_revision":0,"version":0,"lease":0}
{"type":"footer","records":1,"sha256":"bad"}
`),
		"trailing data": append(append([]byte(nil), complete...), []byte("{}\n")...),
	}
	for name, contents := range tests {
		t.Run(name, func(t *testing.T) {
			corruptPath := filepath.Join(t.TempDir(), "corrupt.jsonl")
			require.NoError(t, os.WriteFile(corruptPath, contents, 0o600))
			_, err := OpenVerified(corruptPath)
			require.Error(t, err)
		})
	}
}

func TestAbortDoesNotReplaceExistingBackup(t *testing.T) {
	path := filepath.Join(t.TempDir(), "backup.jsonl")
	require.NoError(t, os.WriteFile(path, []byte("existing"), 0o600))
	writer, err := NewAtomicWriter(path, "/registry", 1)
	require.NoError(t, err)
	require.NoError(t, writer.Add(record.Record{Key: "L3JlZ2lzdHJ5L2E="}))
	writer.Abort()

	contents, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Equal(t, "existing", string(contents))
}

func TestOpenVerifiedRejectsManifestMismatchAndDuplicateKeys(t *testing.T) {
	tests := map[string][]record.Record{
		"outside prefix": {
			{Key: "L291dHNpZGUva2V5", Value: "YQ=="},
		},
		"duplicate key": {
			{Key: "L3JlZ2lzdHJ5L2tleQ==", Value: "YQ=="},
			{Key: "L3JlZ2lzdHJ5L2tleQ==", Value: "Yg=="},
		},
	}
	for name, records := range tests {
		t.Run(name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "backup.jsonl")
			writer, err := NewAtomicWriter(path, "/registry", 42)
			require.NoError(t, err)
			for _, rec := range records {
				require.NoError(t, writer.Add(rec))
			}
			_, err = writer.Commit()
			require.NoError(t, err)

			_, err = OpenVerified(path)
			require.Error(t, err)
		})
	}
}
