package etcdsnapshot

import (
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/kubewharf/kubebrain/hack/backup/internal/backupfile"
	"github.com/kubewharf/kubebrain/hack/backup/internal/record"
	production "github.com/kubewharf/kubebrain/pkg/etcdsnapshot"
)

// Convert writes a hash-protected etcd backend snapshot containing the
// compacted current state represented by a verified logical.v2 artifact.
// Authentication is intentionally disabled: logical artifacts contain no
// password hashes or auth revision and therefore cannot safely reproduce an
// auth-enabled instance.
type Options struct {
	AcknowledgeAuthDisabled bool
}

func Convert(input, output string, options Options) (backupfile.Status, error) {
	if !options.AcknowledgeAuthDisabled {
		return backupfile.Status{}, errors.New("conversion requires explicit acknowledgement that output auth is disabled")
	}
	verified, err := backupfile.OpenVerified(input)
	if err != nil {
		return backupfile.Status{}, err
	}
	defer verified.Close()
	status := verified.Status()
	if status.Format != backupfile.Format {
		return backupfile.Status{}, fmt.Errorf("etcd snapshot conversion requires %s, got %s", backupfile.Format, status.Format)
	}
	if status.Prefix != "/" {
		return backupfile.Status{}, fmt.Errorf("etcd snapshot conversion requires a full-keyspace prefix /, got %q", status.Prefix)
	}

	records := make([]decodedRecord, 0, status.Records)
	if err := verified.Records(func(rec record.Record) error {
		decoded, err := decodeRecord(rec, status.Revision)
		if err != nil {
			return fmt.Errorf("record %d: %w", len(records)+1, err)
		}
		records = append(records, decoded)
		return nil
	}); err != nil {
		return backupfile.Status{}, err
	}
	leases := make([]record.Lease, 0, status.Leases)
	if err := verified.Leases(func(lease record.Lease) error {
		if lease.GrantedTTL <= 0 {
			return fmt.Errorf("lease %d lacks granted_ttl required by etcd snapshots", lease.ID)
		}
		leases = append(leases, lease)
		return nil
	}); err != nil {
		return backupfile.Status{}, err
	}

	if _, err := os.Stat(output); err == nil {
		return backupfile.Status{}, fmt.Errorf("output already exists: %s: %w", output, os.ErrExist)
	} else if !os.IsNotExist(err) {
		return backupfile.Status{}, err
	}
	if err := os.MkdirAll(filepath.Dir(output), 0o750); err != nil {
		return backupfile.Status{}, err
	}
	tmp, err := os.CreateTemp(filepath.Dir(output), ".kubebrain-etcd-snapshot-*")
	if err != nil {
		return backupfile.Status{}, err
	}
	tmpPath := tmp.Name()
	if err := tmp.Close(); err != nil {
		os.Remove(tmpPath)
		return backupfile.Status{}, err
	}
	defer os.Remove(tmpPath)
	if err := os.Chmod(tmpPath, 0o600); err != nil {
		return backupfile.Status{}, err
	}
	if err := writeBackend(tmpPath, status.Revision, records, leases); err != nil {
		return backupfile.Status{}, err
	}
	if err := appendIntegrityHash(tmpPath); err != nil {
		return backupfile.Status{}, err
	}
	if err := os.Link(tmpPath, output); err != nil {
		if errors.Is(err, os.ErrExist) {
			return backupfile.Status{}, fmt.Errorf("output already exists: %s: %w", output, os.ErrExist)
		}
		return backupfile.Status{}, err
	}
	dir, err := os.Open(filepath.Dir(output))
	if err != nil {
		return backupfile.Status{}, err
	}
	if err := dir.Sync(); err != nil {
		dir.Close()
		return backupfile.Status{}, err
	}
	if err := dir.Close(); err != nil {
		return backupfile.Status{}, err
	}
	return status, nil
}

type decodedRecord struct {
	record.Record
	key   []byte
	value []byte
}

func decodeRecord(rec record.Record, snapshotRevision int64) (decodedRecord, error) {
	key, err := base64.StdEncoding.DecodeString(rec.Key)
	if err != nil {
		return decodedRecord{}, fmt.Errorf("decode key: %w", err)
	}
	value, err := base64.StdEncoding.DecodeString(rec.Value)
	if err != nil {
		return decodedRecord{}, fmt.Errorf("decode value: %w", err)
	}
	if len(key) == 0 {
		return decodedRecord{}, fmt.Errorf("empty keys are invalid")
	}
	if rec.CreateRevision <= 0 || rec.ModRevision < rec.CreateRevision || rec.ModRevision > snapshotRevision || rec.Version <= 0 {
		return decodedRecord{}, fmt.Errorf("invalid MVCC metadata create=%d mod=%d version=%d snapshot=%d",
			rec.CreateRevision, rec.ModRevision, rec.Version, snapshotRevision)
	}
	if rec.Version > rec.ModRevision-rec.CreateRevision+1 {
		return decodedRecord{}, fmt.Errorf("version %d cannot fit between create revision %d and mod revision %d",
			rec.Version, rec.CreateRevision, rec.ModRevision)
	}
	return decodedRecord{Record: rec, key: key, value: value}, nil
}

func writeBackend(path string, revision int64, records []decodedRecord, leases []record.Lease) error {
	state := production.State{Revision: revision}
	for _, rec := range records {
		state.Records = append(state.Records, production.Record{
			Key: rec.key, Value: rec.value, CreateRevision: rec.CreateRevision,
			ModRevision: rec.ModRevision, Version: rec.Version, Lease: rec.Lease,
		})
	}
	for _, lease := range leases {
		state.Leases = append(state.Leases, production.Lease{
			ID: lease.ID, GrantedTTL: lease.GrantedTTL, RemainingTTL: lease.TTL,
		})
	}
	// Logical artifacts deliberately exclude KubeBrain's internal auth records.
	// Make the limitation explicit in the generated backend instead of emitting
	// an auth-enabled snapshot with missing credentials.
	return production.WriteBackend(path, state)
}

func appendIntegrityHash(path string) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	out, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0)
	if err != nil {
		return err
	}
	_, writeErr := out.Write(h.Sum(nil))
	syncErr := out.Sync()
	closeErr := out.Close()
	if writeErr != nil {
		return writeErr
	}
	if syncErr != nil {
		return syncErr
	}
	return closeErr
}
