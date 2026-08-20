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
	"time"

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
	statuses      []*clientv3.StatusResponse
	statusCalls   int
	snapshotBytes []byte
	snapshotClose error
	snapshotCalls int
	snapshotReply *clientv3.SnapshotResponse
	snapshotErr   error
	snapshotRaw   bool
	compacted     bool
	compactRev    int64
	compactReply  *clientv3.CompactResponse
}

func validRemediationStatus(clusterID uint64, revision int64) *clientv3.StatusResponse {
	return &clientv3.StatusResponse{
		Header:  &etcdserverpb.ResponseHeader{ClusterId: clusterID, MemberId: 17, Revision: revision},
		Version: "3.7.0", StorageVersion: "3.7.0",
		Leader: 17,
	}
}

type closeErrorReader struct {
	io.Reader
	err error
}

func (r closeErrorReader) Close() error { return r.err }

func (f *fakeEtcdClient) Status(context.Context, string) (*clientv3.StatusResponse, error) {
	if len(f.statuses) != 0 {
		index := f.statusCalls
		if index >= len(f.statuses) {
			index = len(f.statuses) - 1
		}
		f.statusCalls++
		return f.statuses[index], nil
	}
	f.statusCalls++
	return f.status, nil
}
func (f *fakeEtcdClient) SnapshotWithVersion(context.Context) (*clientv3.SnapshotResponse, error) {
	f.snapshotCalls++
	if f.snapshotRaw {
		return f.snapshotReply, f.snapshotErr
	}
	if !f.compacted {
		return nil, errors.New(LegacyDiagnostic + `: key "/old"; minimum physical compact revision 3`)
	}
	return &clientv3.SnapshotResponse{Version: "3.7.0", Snapshot: closeErrorReader{Reader: bytes.NewReader(f.snapshotBytes), err: f.snapshotClose}}, nil
}

type failingWriter struct{ err error }

func (w failingWriter) Write([]byte) (int, error) { return 0, w.err }

func TestRunStopsBeforeSnapshotWhenStatusOutputFails(t *testing.T) {
	writeErr := errors.New("status pipe closed")
	client := &fakeEtcdClient{status: validRemediationStatus(7301, 42)}

	_, code, err := runWithClient(context.Background(), Config{Action: "diagnose", Endpoint: "https://kb:2379"}, failingWriter{err: writeErr}, client)

	require.ErrorIs(t, err, writeErr)
	require.Equal(t, 1, code)
	require.Zero(t, client.snapshotCalls)
	require.False(t, client.compacted)
}
func (f *fakeEtcdClient) Compact(_ context.Context, revision int64, _ ...clientv3.CompactOption) (*clientv3.CompactResponse, error) {
	f.compacted = true
	f.compactRev = revision
	if f.compactReply != nil {
		return f.compactReply, nil
	}
	return &clientv3.CompactResponse{Header: &etcdserverpb.ResponseHeader{ClusterId: f.status.Header.ClusterId, MemberId: f.status.Header.MemberId, Revision: f.status.Header.Revision}}, nil
}

func TestDownloadRejectsSnapshotWhenResponseCloseFails(t *testing.T) {
	closeErr := errors.New("snapshot response close failed")
	client := &fakeEtcdClient{compacted: true, snapshotBytes: validSnapshotArtifact(t), snapshotClose: closeErr}
	path := filepath.Join(t.TempDir(), "snapshot.db")

	err := downloadAndValidate(context.Background(), client, path, "3.7.0")

	require.ErrorIs(t, err, closeErr)
	_, statErr := os.Stat(path)
	require.ErrorIs(t, statErr, os.ErrNotExist)
}

func TestDownloadRejectsMalformedSnapshotResponsesAndClosesReaders(t *testing.T) {
	path := filepath.Join(t.TempDir(), "snapshot.db")
	client := &fakeEtcdClient{snapshotRaw: true}
	err := downloadAndValidate(context.Background(), client, path, "3.7.0")
	require.ErrorContains(t, err, "empty response")

	client.snapshotReply = &clientv3.SnapshotResponse{Version: "3.7.0"}
	err = downloadAndValidate(context.Background(), client, path, "3.7.0")
	require.ErrorContains(t, err, "empty reader")

	closeErr := errors.New("close mismatched snapshot")
	client.snapshotReply = &clientv3.SnapshotResponse{Version: "3.6.0", Snapshot: closeErrorReader{Reader: bytes.NewReader(nil), err: closeErr}}
	err = downloadAndValidate(context.Background(), client, path, "3.7.0")
	require.ErrorContains(t, err, "does not match")
	require.ErrorIs(t, err, closeErr)

	transportErr := errors.New("snapshot transport failed")
	closeErr = errors.New("close mixed snapshot")
	client.snapshotReply = &clientv3.SnapshotResponse{Version: "3.7.0", Snapshot: closeErrorReader{Reader: bytes.NewReader(nil), err: closeErr}}
	client.snapshotErr = transportErr
	err = downloadAndValidate(context.Background(), client, path, "3.7.0")
	require.ErrorIs(t, err, transportErr)
	require.ErrorIs(t, err, closeErr)
	_, statErr := os.Stat(path)
	require.ErrorIs(t, statErr, os.ErrNotExist)
}

func TestRunCompactsOnlyAfterExactIdentityConfirmationAndPublishes(t *testing.T) {
	artifact := validSnapshotArtifact(t)
	client := &fakeEtcdClient{status: validRemediationStatus(18446744073709551615, 41), snapshotBytes: artifact}
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
	client := &fakeEtcdClient{status: validRemediationStatus(7301, 42)}
	config := Config{Action: "compact", Endpoint: "https://kb:2379", ConfirmEndpoint: "https://kb:2379",
		ExpectedClusterID: "7301", ExpectedRevision: "41", AllowIrreversible: true, Output: filepath.Join(t.TempDir(), "snapshot.db")}
	_, code, err := runWithClient(context.Background(), config, io.Discard, client)
	require.ErrorContains(t, err, "confirmation fields")
	require.Equal(t, 2, code)
	require.False(t, client.compacted)
}

func TestRunRefusesEndpointDriftAroundCompactionAndSnapshot(t *testing.T) {
	initial := validRemediationStatus(7301, 41)
	drifted := validRemediationStatus(7302, 42)
	config := Config{Action: "compact", Endpoint: "https://kb:2379", ConfirmEndpoint: "https://kb:2379",
		ExpectedClusterID: "7301", ExpectedRevision: "41", AllowIrreversible: true, Output: filepath.Join(t.TempDir(), "snapshot.db")}

	client := &fakeEtcdClient{status: initial, statuses: []*clientv3.StatusResponse{initial, drifted}}
	_, code, err := runWithClient(context.Background(), config, io.Discard, client)
	require.ErrorContains(t, err, "identity changed")
	require.Equal(t, 1, code)
	require.False(t, client.compacted)

	client = &fakeEtcdClient{status: initial, snapshotBytes: validSnapshotArtifact(t), compactReply: &clientv3.CompactResponse{
		Header: &etcdserverpb.ResponseHeader{ClusterId: 7301, MemberId: 17, Revision: 40},
	}}
	_, code, err = runWithClient(context.Background(), config, io.Discard, client)
	require.ErrorContains(t, err, "invalid acknowledgement")
	require.Equal(t, 1, code)

	output := filepath.Join(t.TempDir(), "snapshot.db")
	config.Output = output
	client = &fakeEtcdClient{status: initial, statuses: []*clientv3.StatusResponse{initial, initial, drifted}, snapshotBytes: validSnapshotArtifact(t)}
	_, code, err = runWithClient(context.Background(), config, io.Discard, client)
	require.ErrorContains(t, err, "post-compaction endpoint identity")
	require.Equal(t, 1, code)
	_, statErr := os.Stat(output)
	require.ErrorIs(t, statErr, os.ErrNotExist)
}

func TestValidateStatusResponse(t *testing.T) {
	require.NoError(t, validateStatusResponse(validRemediationStatus(7, 11)))
	tests := []*clientv3.StatusResponse{
		nil,
		{},
		{Header: &etcdserverpb.ResponseHeader{ClusterId: 7, MemberId: 17, Revision: 0}, Version: "3.7.0", Leader: 17},
		{Header: &etcdserverpb.ResponseHeader{ClusterId: 0, MemberId: 17, Revision: 11}, Version: "3.7.0", Leader: 17},
		{Header: &etcdserverpb.ResponseHeader{ClusterId: 7, MemberId: 0, Revision: 11}, Version: "3.7.0", Leader: 17},
		{Header: &etcdserverpb.ResponseHeader{ClusterId: 7, MemberId: 17, Revision: 11}, Leader: 17},
		{Header: &etcdserverpb.ResponseHeader{ClusterId: 7, MemberId: 17, Revision: 11}, Version: "not-semver", Leader: 17},
		{Header: &etcdserverpb.ResponseHeader{ClusterId: 7, MemberId: 17, Revision: 11}, Version: "3.7.0", Leader: 17},
		{Header: &etcdserverpb.ResponseHeader{ClusterId: 7, MemberId: 17, Revision: 11}, Version: "3.7.0", StorageVersion: "not-semver", Leader: 17},
		{Header: &etcdserverpb.ResponseHeader{ClusterId: 7, MemberId: 17, Revision: 11}, Version: "3.7.0", StorageVersion: "3.7.0", Leader: 17, DbSize: -1},
		{Header: &etcdserverpb.ResponseHeader{ClusterId: 7, MemberId: 17, Revision: 11}, Version: "3.7.0", StorageVersion: "3.7.0", Leader: 17, DbSizeInUse: -1},
		{Header: &etcdserverpb.ResponseHeader{ClusterId: 7, MemberId: 17, Revision: 11}, Version: "3.7.0", StorageVersion: "3.7.0"},
		{Header: &etcdserverpb.ResponseHeader{ClusterId: 7, MemberId: 17, Revision: 11}, Version: "3.7.0", StorageVersion: "3.7.0", Leader: 17, Errors: []string{"unhealthy"}},
	}
	for i, status := range tests {
		require.Error(t, validateStatusResponse(status), i)
	}
}

func TestValidateCompactResponse(t *testing.T) {
	require.NoError(t, validateCompactResponse(&clientv3.CompactResponse{Header: &etcdserverpb.ResponseHeader{ClusterId: 7, MemberId: 17, Revision: 12}}, 7, 11))
	for i, response := range []*clientv3.CompactResponse{
		nil,
		{},
		{Header: &etcdserverpb.ResponseHeader{ClusterId: 8, MemberId: 17, Revision: 11}},
		{Header: &etcdserverpb.ResponseHeader{ClusterId: 7, MemberId: 0, Revision: 11}},
		{Header: &etcdserverpb.ResponseHeader{ClusterId: 7, MemberId: 17, Revision: 10}},
	} {
		require.Error(t, validateCompactResponse(response, 7, 11), i)
	}
}

func TestContinuationStatusRejectsIdentityRevisionAndVersionDrift(t *testing.T) {
	initial := validRemediationStatus(7, 11)
	for name, next := range map[string]*clientv3.StatusResponse{
		"cluster":         validRemediationStatus(8, 12),
		"revision":        validRemediationStatus(7, 10),
		"server version":  validRemediationStatus(7, 12),
		"storage version": validRemediationStatus(7, 12),
	} {
		if name == "server version" {
			next.Version = "3.7.1"
		}
		if name == "storage version" {
			next.StorageVersion = "3.6.0"
		}
		t.Run(name, func(t *testing.T) {
			_, err := continuationStatus(context.Background(), &fakeEtcdClient{status: next}, "https://kb:2379", initial, 11)
			require.Error(t, err)
		})
	}
	next := validRemediationStatus(7, 12)
	next.Header.MemberId = 19
	_, err := continuationStatus(context.Background(), &fakeEtcdClient{status: next}, "https://kb:2379", initial, 11)
	require.NoError(t, err)
}

func TestReferenceSnapshotStatusContinuity(t *testing.T) {
	endpoint := os.Getenv("REFERENCE_LEGACY_REMEDIATION_ENDPOINT")
	if endpoint == "" {
		t.Skip("set REFERENCE_LEGACY_REMEDIATION_ENDPOINT to a disposable reference etcd")
	}
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	result, code, err := Run(ctx, Config{Action: "diagnose", Endpoint: endpoint, Timeout: 30 * time.Second}, io.Discard)
	require.NoError(t, err)
	require.Zero(t, code)
	require.Equal(t, "healthy", result.SnapshotStatus)
	require.NotEmpty(t, result.ClusterID)
	require.NotEmpty(t, result.Revision)
}

func TestRunRejectsMalformedStatusBeforeSnapshot(t *testing.T) {
	client := &fakeEtcdClient{}
	_, code, err := runWithClient(context.Background(), Config{Action: "diagnose", Endpoint: "https://kb:2379"}, io.Discard, client)
	require.ErrorContains(t, err, "invalid status response header")
	require.Equal(t, 1, code)
	require.Zero(t, client.snapshotCalls)
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
