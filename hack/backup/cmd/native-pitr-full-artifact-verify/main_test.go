package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/kubewharf/kubebrain/hack/backup/internal/nativepitr"
	"github.com/kubewharf/kubebrain/hack/backup/internal/pitrinventory"
	"github.com/kubewharf/kubebrain/pkg/backend/coder"
	backuppb "github.com/pingcap/kvproto/pkg/brpb"
	"github.com/stretchr/testify/require"
)

const testDigest = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"

func TestRunVerifiesExactMirror(t *testing.T) {
	dir := t.TempDir()
	ks, err := coder.NewKeyspace("tenant-a")
	require.NoError(t, err)
	task := nativepitr.TaskCreateReceipt{Format: nativepitr.TaskCreateFormat, ClusterID: 11, Keyspace: "tenant-a", TaskName: "task-a", StartTS: 100, CommittedAtTS: 110, EndTS: 1000, StartKeyHex: hex.EncodeToString(ks.ObjectKeyspaceStart()), EndKeyHex: hex.EncodeToString(ks.ObjectKeyspaceEnd()), LogStoragePrefix: "s3://bucket/log-a", LogStorageSHA256: testDigest, PreflightSHA256: testDigest, OwnerKey: nativepitr.TaskOwnerKey, BootstrapSafePointID: "kubebrain-native-pitr-bootstrap-operation-a", BootstrapSafePointTTL: 7200, AtomicMetadataCreated: true}
	content := []byte("sst-data")
	digest := sha256.Sum256(content)
	file := &backuppb.File{Name: "1/write.sst", Cf: "write", Size_: uint64(len(content)), Sha256: digest[:]}
	metaBytes, err := (&backuppb.BackupMeta{ClusterId: 11, StartVersion: 0, EndVersion: 120, IsTxnKv: true, Files: []*backuppb.File{file}}).Marshal()
	require.NoError(t, err)
	full, err := nativepitr.BuildFullSnapshot(task, testDigest, "s3://bucket/full-a", metaBytes)
	require.NoError(t, err)
	fullBytes, err := json.Marshal(full)
	require.NoError(t, err)
	fullPath := filepath.Join(dir, "full.json")
	attestation, err := nativepitr.BuildFullBackupAttestation("Release Version: v7.5.1\nGit Commit Hash: 7d16cc79e81bbf573124df3fd9351c26963f3e70\n", testDigest, testDigest, full.StoragePrefix, full.BackupTS, []string{"backup", "txn", "--storage=" + full.StoragePrefix, "--backupts=120", "--crypter.method=plaintext"}, 2_000_000_000)
	require.NoError(t, err)
	attestationBytes, err := json.Marshal(attestation)
	require.NoError(t, err)
	attestationPath := filepath.Join(dir, "attestation.json")
	root := filepath.Join(dir, "mirror")
	require.NoError(t, os.MkdirAll(filepath.Join(root, "1"), 0o700))
	require.NoError(t, os.WriteFile(fullPath, fullBytes, 0o600))
	require.NoError(t, os.WriteFile(attestationPath, attestationBytes, 0o600))
	require.NoError(t, os.WriteFile(filepath.Join(root, "backupmeta"), metaBytes, 0o600))
	require.NoError(t, os.WriteFile(filepath.Join(root, "1/write.sst"), content, 0o600))
	metaDigest := sha256.Sum256(metaBytes)
	inventory := pitrinventory.Receipt{Format: pitrinventory.Format, ObjectStoreID: "store-a", Bucket: "bucket", Prefix: "full-a", Entries: []pitrinventory.Entry{{Name: "1/write.sst", ObjectKey: "full-a/1/write.sst", VersionID: "v-data", Bytes: int64(len(content)), SHA256: hex.EncodeToString(digest[:]), RetentionMode: "COMPLIANCE", RetainUntilUnix: 2_100_000_000}, {Name: "backupmeta", ObjectKey: "full-a/backupmeta", VersionID: "v-meta", Bytes: int64(len(metaBytes)), SHA256: hex.EncodeToString(metaDigest[:]), RetentionMode: "COMPLIANCE", RetainUntilUnix: 2_100_000_000}}, ObjectCount: 2, TotalBytes: uint64(len(content) + len(metaBytes)), Pages: 1, PaginationExhausted: true, ExactVersionsVerified: true, MinRetainUntilUnix: 2_050_000_000, CheckedAtUnix: 2_000_000_000}
	inventoryBytes, err := json.Marshal(inventory)
	require.NoError(t, err)
	inventoryBytes = append(inventoryBytes, '\n')
	inventoryPath := filepath.Join(dir, "inventory.json")
	require.NoError(t, os.WriteFile(inventoryPath, inventoryBytes, 0o600))
	var out bytes.Buffer
	require.NoError(t, run(fullPath, attestationPath, inventoryPath, root, "", "", &out))
	var receipt nativepitr.ArtifactReceipt
	require.NoError(t, json.Unmarshal(out.Bytes(), &receipt))
	require.Equal(t, 2, receipt.ObjectCount)

	encryptedAttestation, err := nativepitr.BuildFullBackupAttestationWithEncryption("Release Version: v7.5.1\nGit Commit Hash: 7d16cc79e81bbf573124df3fd9351c26963f3e70\n", testDigest, testDigest, full.StoragePrefix, full.BackupTS, nativepitr.EncryptionIdentity{Method: nativepitr.CipherMethodAES256CTR, KeyID: "kms/test/versions/1"}, []string{"backup", "txn", "--storage=" + full.StoragePrefix, "--backupts=120", "--crypter.method=aes256-ctr", "--crypter.key-id=kms/test/versions/1"}, 2_000_000_000)
	require.NoError(t, err)
	encryptedBytes, err := json.Marshal(encryptedAttestation)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(attestationPath, encryptedBytes, 0o600))
	require.ErrorContains(t, run(fullPath, attestationPath, inventoryPath, root, "kms/test/versions/2", filepath.Join(dir, "missing-key"), &bytes.Buffer{}), "attestation-bound")
}

func TestRunRequiresRemoteInventory(t *testing.T) {
	require.ErrorContains(t, run("", "", "", "", "", "", &bytes.Buffer{}), "required")
}

func TestReadReceiptRejectsOversizedInput(t *testing.T) {
	path := filepath.Join(t.TempDir(), "receipt.json")
	require.NoError(t, os.WriteFile(path, make([]byte, maxReceiptBytes+1), 0o600))
	_, err := readReceipt(path)
	require.ErrorContains(t, err, "receipt")
	require.ErrorContains(t, err, "exceeds")
}
