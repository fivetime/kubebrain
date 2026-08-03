package etcdsnapshot

import (
	"crypto/sha256"
	"encoding/base64"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	bolt "go.etcd.io/bbolt"
	"go.etcd.io/etcd/api/v3/mvccpb"
	etcdutlsnapshot "go.etcd.io/etcd/etcdutl/v3/snapshot"
	"go.etcd.io/etcd/server/v3/lease/leasepb"
	"go.etcd.io/etcd/server/v3/storage/mvcc"
	"go.etcd.io/etcd/server/v3/storage/schema"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"

	"github.com/kubewharf/kubebrain/hack/backup/internal/backupfile"
	"github.com/kubewharf/kubebrain/hack/backup/internal/record"
)

func TestConvertWritesHashProtectedEtcdBackend(t *testing.T) {
	const snapshotRevision int64 = 468126003565721322
	input := filepath.Join(t.TempDir(), "logical.jsonl")
	writer, err := backupfile.NewAtomicWriter(input, "/", snapshotRevision)
	require.NoError(t, err)
	require.NoError(t, writer.AddLease(record.Lease{ID: 123, TTL: 30, GrantedTTL: 60}))
	require.NoError(t, writer.Add(backupRecord("/registry/a", "alpha", 17, 23, 3, 123)))
	require.NoError(t, writer.Add(backupRecord("/registry/b", "beta", 41, 41, 1, 0)))
	_, err = writer.Commit()
	require.NoError(t, err)

	output := filepath.Join(t.TempDir(), "snapshot.db")
	status, err := Convert(input, output, Options{AcknowledgeAuthDisabled: true})
	require.NoError(t, err)
	require.Equal(t, snapshotRevision, status.Revision)
	require.Equal(t, 2, status.Records)

	contents, err := os.ReadFile(output)
	require.NoError(t, err)
	require.Greater(t, len(contents), sha256.Size)
	wantHash := sha256.Sum256(contents[:len(contents)-sha256.Size])
	require.Equal(t, wantHash[:], contents[len(contents)-sha256.Size:])

	manager := etcdutlsnapshot.NewV3(zap.NewNop())
	officialStatus, err := manager.Status(output)
	require.NoError(t, err)
	require.Equal(t, snapshotRevision, officialStatus.Revision)
	require.Equal(t, 2, officialStatus.TotalKey)
	require.Equal(t, "3.7.0", officialStatus.Version)
	restoreDir := filepath.Join(t.TempDir(), "restored")
	require.NoError(t, manager.Restore(etcdutlsnapshot.RestoreConfig{
		SnapshotPath: output, Name: "default", OutputDataDir: restoreDir,
		PeerURLs: []string{"http://127.0.0.1:2380"}, InitialCluster: "default=http://127.0.0.1:2380",
		InitialClusterToken: "kubebrain-test", InitialMmapSize: 64 * 1024 * 1024,
	}))
	require.FileExists(t, filepath.Join(restoreDir, "member", "snap", "db"))

	db, err := bolt.Open(output, 0o400, &bolt.Options{ReadOnly: true})
	require.NoError(t, err)
	defer db.Close()
	require.NoError(t, db.View(func(tx *bolt.Tx) error {
		for _, bucket := range schema.AllBuckets {
			require.NotNil(t, tx.Bucket(bucket.Name()), string(bucket.Name()))
		}
		require.Equal(t, "3.7.0", string(tx.Bucket(schema.Meta.Name()).Get(schema.MetaStorageVersionName)))
		require.Equal(t, []byte{0}, tx.Bucket(schema.Auth.Name()).Get(schema.AuthEnabledKeyName))

		got := make(map[string]*mvccpb.KeyValue)
		var maxRevision int64
		require.NoError(t, tx.Bucket(schema.Key.Name()).ForEach(func(key, value []byte) error {
			revision := mvcc.BytesToRev(key).Main
			if revision > maxRevision {
				maxRevision = revision
			}
			if mvcc.IsTombstone(key) {
				return nil
			}
			var kv mvccpb.KeyValue
			if err := proto.Unmarshal(value, &kv); err != nil {
				return err
			}
			got[string(kv.Key)] = &kv
			return nil
		}))
		require.Equal(t, snapshotRevision, maxRevision)
		require.True(t, proto.Equal(&mvccpb.KeyValue{Key: []byte("/registry/a"), Value: []byte("alpha"), CreateRevision: 17, ModRevision: 23, Version: 3, Lease: 123}, got["/registry/a"]))
		require.True(t, proto.Equal(&mvccpb.KeyValue{Key: []byte("/registry/b"), Value: []byte("beta"), CreateRevision: 41, ModRevision: 41, Version: 1}, got["/registry/b"]))

		var leases []*leasepb.Lease
		require.NoError(t, tx.Bucket(schema.Lease.Name()).ForEach(func(_, value []byte) error {
			var lease leasepb.Lease
			if err := proto.Unmarshal(value, &lease); err != nil {
				return err
			}
			leases = append(leases, &lease)
			return nil
		}))
		require.Len(t, leases, 1)
		require.True(t, proto.Equal(&leasepb.Lease{ID: 123, TTL: 60, RemainingTTL: 30}, leases[0]))
		return nil
	}))
}

func TestConvertPublishesWithoutOverwrite(t *testing.T) {
	input := filepath.Join(t.TempDir(), "logical.jsonl")
	writer, err := backupfile.NewAtomicWriter(input, "/", 42)
	require.NoError(t, err)
	require.NoError(t, writer.Add(backupRecord("/a", "a", 40, 42, 2, 0)))
	_, err = writer.Commit()
	require.NoError(t, err)

	output := filepath.Join(t.TempDir(), "snapshot.db")
	start := make(chan struct{})
	errs := make(chan error, 2)
	var workers sync.WaitGroup
	for range 2 {
		workers.Add(1)
		go func() {
			defer workers.Done()
			<-start
			_, convertErr := Convert(input, output, Options{AcknowledgeAuthDisabled: true})
			errs <- convertErr
		}()
	}
	close(start)
	workers.Wait()
	close(errs)
	var succeeded, rejected int
	for convertErr := range errs {
		if convertErr == nil {
			succeeded++
		} else {
			require.ErrorIs(t, convertErr, os.ErrExist)
			rejected++
		}
	}
	require.Equal(t, 1, succeeded)
	require.Equal(t, 1, rejected)
}

func TestConvertRejectsIncompleteOrImpossibleArtifacts(t *testing.T) {
	tests := []struct {
		name   string
		prefix string
		record record.Record
		lease  *record.Lease
		want   string
	}{
		{name: "partial prefix", prefix: "/registry", record: backupRecord("/registry/a", "a", 1, 1, 1, 0), want: "full-keyspace prefix /"},
		{name: "missing granted ttl", prefix: "/", record: backupRecord("/a", "a", 1, 1, 1, 7), lease: &record.Lease{ID: 7, TTL: 30}, want: "lacks granted_ttl"},
		{name: "future mod revision", prefix: "/", record: backupRecord("/a", "a", 1, 43, 1, 0), want: "invalid MVCC metadata"},
		{name: "impossible version", prefix: "/", record: backupRecord("/a", "a", 40, 42, 4, 0), want: "cannot fit"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			input := filepath.Join(t.TempDir(), "logical.jsonl")
			writer, err := backupfile.NewAtomicWriter(input, test.prefix, 42)
			require.NoError(t, err)
			if test.lease != nil {
				require.NoError(t, writer.AddLease(*test.lease))
			}
			require.NoError(t, writer.Add(test.record))
			_, err = writer.Commit()
			require.NoError(t, err)
			_, err = Convert(input, filepath.Join(t.TempDir(), "snapshot.db"), Options{AcknowledgeAuthDisabled: true})
			require.ErrorContains(t, err, test.want)
		})
	}
}

func TestConvertRequiresExplicitAuthDowngradeAcknowledgement(t *testing.T) {
	_, err := Convert("unused", "unused", Options{})
	require.EqualError(t, err, "conversion requires explicit acknowledgement that output auth is disabled")
}

func backupRecord(key, value string, create, mod, version, lease int64) record.Record {
	return record.Record{
		Key: base64.StdEncoding.EncodeToString([]byte(key)), Value: base64.StdEncoding.EncodeToString([]byte(value)),
		CreateRevision: create, ModRevision: mod, Version: version, Lease: lease,
	}
}
