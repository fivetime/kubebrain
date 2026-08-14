package legacyremediation

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	bolt "go.etcd.io/bbolt"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	clientv3 "go.etcd.io/etcd/client/v3"
)

func TestConfigFromEnvPreservesExactIdentityInputs(t *testing.T) {
	t.Setenv("ACTION", "compact")
	t.Setenv("ENDPOINT", "https://kubebrain:2379")
	t.Setenv("EXPECTED_CLUSTER_ID", "18446744073709551615")
	t.Setenv("EXPECTED_REVISION", "9223372036854775807")
	t.Setenv("ALLOW_IRREVERSIBLE_LEGACY_HISTORY_COMPACTION", "true")
	t.Setenv("ETCDCTL_USER", "root:secret:with:colons")
	c, err := ConfigFromEnv()
	require.NoError(t, err)
	require.Equal(t, "18446744073709551615", c.ExpectedClusterID)
	require.Equal(t, "9223372036854775807", c.ExpectedRevision)
	require.True(t, c.AllowIrreversible)
	require.Equal(t, "root", c.User)
	require.Equal(t, "secret:with:colons", c.Password)
}

type fakeEtcdClient struct {
	status        *clientv3.StatusResponse
	snapshotBytes []byte
	compacted     bool
	compactRev    int64
}

func (f *fakeEtcdClient) Status(context.Context, string) (*clientv3.StatusResponse, error) {
	return f.status, nil
}
func (f *fakeEtcdClient) Snapshot(context.Context) (io.ReadCloser, error) {
	if !f.compacted {
		return nil, errors.New(LegacyDiagnostic + `: key "/old"; minimum physical compact revision 3`)
	}
	return io.NopCloser(bytes.NewReader(f.snapshotBytes)), nil
}
func (f *fakeEtcdClient) Compact(_ context.Context, revision int64, _ ...clientv3.CompactOption) (*clientv3.CompactResponse, error) {
	f.compacted = true
	f.compactRev = revision
	return &clientv3.CompactResponse{Header: &etcdserverpb.ResponseHeader{Revision: revision}}, nil
}

func TestRunCompactsOnlyAfterExactIdentityConfirmationAndPublishes(t *testing.T) {
	artifact := validSnapshotArtifact(t)
	client := &fakeEtcdClient{status: &clientv3.StatusResponse{Header: &etcdserverpb.ResponseHeader{ClusterId: 18446744073709551615, Revision: 41}}, snapshotBytes: artifact}
	output := filepath.Join(t.TempDir(), "snapshot.db")
	config := Config{Action: "compact", Endpoint: "https://kb:2379", ConfirmEndpoint: "https://kb:2379",
		ExpectedClusterID: "18446744073709551615", ExpectedRevision: "41", AllowIrreversible: true, Output: output}
	var log bytes.Buffer
	result, code, err := runWithClient(context.Background(), config, &log, client)
	require.NoError(t, err)
	require.Zero(t, code)
	require.Equal(t, int64(3), client.compactRev)
	require.Equal(t, "3", result.CompactRevision)
	require.Equal(t, "remediated", result.SnapshotStatus)
	require.Equal(t, artifact, requireFileBytes(t, output))
	require.Contains(t, log.String(), "snapshot_status=remediated")
}

func TestRunRefusesIdentityDriftBeforeCompaction(t *testing.T) {
	client := &fakeEtcdClient{status: &clientv3.StatusResponse{Header: &etcdserverpb.ResponseHeader{ClusterId: 7301, Revision: 42}}}
	config := Config{Action: "compact", Endpoint: "https://kb:2379", ConfirmEndpoint: "https://kb:2379",
		ExpectedClusterID: "7301", ExpectedRevision: "41", AllowIrreversible: true, Output: filepath.Join(t.TempDir(), "snapshot.db")}
	_, code, err := runWithClient(context.Background(), config, io.Discard, client)
	require.ErrorContains(t, err, "confirmation fields")
	require.Equal(t, 2, code)
	require.False(t, client.compacted)
}

func TestConfigFromEnvRejectsMultipleOrUnsafeEndpoints(t *testing.T) {
	for _, endpoint := range []string{"", "a,b", "a b", "a\nother", `a\other`} {
		t.Run(endpoint, func(t *testing.T) {
			t.Setenv("ENDPOINT", endpoint)
			_, err := ConfigFromEnv()
			require.ErrorContains(t, err, "ENDPOINT")
		})
	}
}

func TestVerifySnapshotChecksDigestAndBboltConsistency(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "snapshot.db")
	db, err := bolt.Open(path, 0o600, nil)
	require.NoError(t, err)
	require.NoError(t, db.Update(func(tx *bolt.Tx) error {
		bucket, createErr := tx.CreateBucket([]byte("key"))
		if createErr != nil {
			return createErr
		}
		return bucket.Put([]byte("k"), []byte("v"))
	}))
	require.NoError(t, db.Close())
	contents, err := os.ReadFile(path)
	require.NoError(t, err)
	digest := sha256.Sum256(contents)
	require.NoError(t, os.WriteFile(path, append(contents, digest[:]...), 0o600))
	require.NoError(t, verifySnapshot(path))

	contents[len(contents)/2] ^= 0xff
	require.NoError(t, os.WriteFile(path, append(contents, digest[:]...), 0o600))
	require.ErrorContains(t, verifySnapshot(path), "SHA-256 mismatch")
}

func validSnapshotArtifact(t *testing.T) []byte {
	t.Helper()
	path := filepath.Join(t.TempDir(), "valid.db")
	db, err := bolt.Open(path, 0o600, nil)
	require.NoError(t, err)
	require.NoError(t, db.Update(func(tx *bolt.Tx) error { _, createErr := tx.CreateBucketIfNotExists([]byte("key")); return createErr }))
	require.NoError(t, db.Close())
	contents := requireFileBytes(t, path)
	digest := sha256.Sum256(contents)
	return append(contents, digest[:]...)
}

func requireFileBytes(t *testing.T, path string) []byte {
	t.Helper()
	contents, err := os.ReadFile(path)
	require.NoError(t, err)
	return contents
}
