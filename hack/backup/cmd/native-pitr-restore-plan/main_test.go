package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/kubewharf/kubebrain/hack/backup/internal/backupfile"
	"github.com/kubewharf/kubebrain/hack/backup/internal/nativepitr"
	"github.com/kubewharf/kubebrain/pkg/backend/coder"
	"github.com/stretchr/testify/require"
)

const testDigest = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"

func TestRunWritesPlanFromExactReceipts(t *testing.T) {
	dir := t.TempDir()
	ks, err := coder.NewKeyspace("tenant-a")
	require.NoError(t, err)
	task := nativepitr.TaskCreateReceipt{Format: nativepitr.TaskCreateFormat, ClusterID: 11, Keyspace: "tenant-a", TaskName: "task-a", StartTS: 100, CommittedAtTS: 110, EndTS: 1000, StartKeyHex: hex.EncodeToString(ks.ObjectKeyspaceStart()), EndKeyHex: hex.EncodeToString(ks.ObjectKeyspaceEnd()), LogStoragePrefix: "s3://bucket/immutable/log-a", LogStorageSHA256: testDigest, PreflightSHA256: testDigest, OwnerKey: nativepitr.TaskOwnerKey, BootstrapSafePointID: "kubebrain-native-pitr-bootstrap-operation-a", BootstrapSafePointTTL: 7200, AtomicMetadataCreated: true}
	taskBytes, err := json.Marshal(task)
	require.NoError(t, err)
	taskDigest := sha256.Sum256(taskBytes)
	full := nativepitr.FullSnapshotReceipt{Format: nativepitr.FullSnapshotFormat, ClusterID: 11, Keyspace: "tenant-a", TaskName: "task-a", TaskStartTS: 100, TaskCommittedAtTS: 110, TaskEndTS: 1000, BackupStartTS: 0, BackupTS: 120, StartKeyHex: task.StartKeyHex, EndKeyHex: task.EndKeyHex, StoragePrefix: "s3://bucket/immutable/full-a", BackupMetaSHA256: testDigest, BackupMetaBytes: 100, TaskCreateSHA256: hex.EncodeToString(taskDigest[:]), HasFileIndex: true, Transactional: true, Scope: "whole-cluster", BackupMetaInventory: true}
	fullBytes, err := json.Marshal(full)
	require.NoError(t, err)
	fullDigest := sha256.Sum256(fullBytes)
	objects := []nativepitr.ArtifactObject{{Name: "backupmeta", Bytes: 100, SHA256: testDigest, Kind: "backupmeta"}, {Name: "data/write.sst", Bytes: 10, SHA256: testDigest, Kind: "data"}}
	objectBytes, err := json.Marshal(objects)
	require.NoError(t, err)
	manifestDigest := sha256.Sum256(objectBytes)
	attestation, err := nativepitr.BuildFullBackupAttestation("Release Version: v7.5.1\nGit Commit Hash: 7d16cc79e81bbf573124df3fd9351c26963f3e70\n", testDigest, testDigest, full.StoragePrefix, full.BackupTS, []string{"backup", "txn", "--storage=" + full.StoragePrefix, "--backupts=120", "--crypter.method=plaintext"}, 2_000_000_000)
	require.NoError(t, err)
	attestationSHA, err := nativepitr.FullBackupAttestationSHA256(attestation)
	require.NoError(t, err)
	artifacts := nativepitr.ArtifactReceipt{Format: nativepitr.ArtifactReceiptFormat, ClusterID: 11, Keyspace: "tenant-a", TaskName: "task-a", BackupTS: 120, StoragePrefix: full.StoragePrefix, FullReceiptSHA256: hex.EncodeToString(fullDigest[:]), BackupMetaSHA256: testDigest, RemoteInventorySHA256: testDigest, ObjectStoreID: "store-a", Bucket: "bucket", ObjectPrefix: "immutable/full-a", MinRetainUntilUnix: 2_050_000_000, InventoryCheckedAtUnix: 2_000_000_000, Objects: objects, ObjectCount: 2, TotalBytes: 110, ManifestSHA256: hex.EncodeToString(manifestDigest[:]), ExactMirror: true, RemoteVersionsVerified: true, Encryption: "plaintext", BackupAttestationSHA256: attestationSHA, BRBinarySHA256: attestation.BRBinarySHA256, BackupAttestation: attestation, AllObjectsVerified: true}
	ready := nativepitr.TaskReadyReceipt{Format: nativepitr.TaskReadyFormat, ClusterID: 11, Keyspace: "tenant-a", TaskName: "task-a", StartTS: 100, CommittedAtTS: 110, EndTS: 1000, GlobalCheckpointTS: 200, AdvancerOwner: "owner-1", PreflightSHA256: testDigest, BootstrapSafePointID: task.BootstrapSafePointID, BootstrapReleased: true, MetadataSnapshotValid: true}
	readyBytes, err := json.Marshal(ready)
	require.NoError(t, err)
	readyDigest := sha256.Sum256(readyBytes)
	logObjects := []nativepitr.LogArtifactObject{{Name: "v1/backupmeta/1.meta", Bytes: 10, SHA256: testDigest, Kind: "metadata"}, {Name: "v1/log/1.log", Bytes: 20, SHA256: testDigest, Kind: "data"}}
	logObjectBytes, err := json.Marshal(logObjects)
	require.NoError(t, err)
	logManifestDigest := sha256.Sum256(logObjectBytes)
	logs := nativepitr.LogArtifactReceipt{Format: nativepitr.LogArtifactReceiptFormat, ClusterID: 11, Keyspace: "tenant-a", TaskName: "task-a", TaskCreateSHA256: hex.EncodeToString(taskDigest[:]), TaskReadySHA256: hex.EncodeToString(readyDigest[:]), StartTS: 100, GlobalCheckpointTS: 200, StoragePrefix: task.LogStoragePrefix, StorageSHA256: task.LogStorageSHA256, RemoteInventorySHA256: testDigest, ObjectStoreID: "store-a", Bucket: "bucket", ObjectPrefix: "immutable/log-a", MinRetainUntilUnix: 2_050_000_000, InventoryCheckedAtUnix: 2_000_000_000, Objects: logObjects, ObjectCount: 2, MetadataCount: 1, DataObjectCount: 1, VerifiedSegmentCount: 1, TotalBytes: 30, ManifestSHA256: hex.EncodeToString(logManifestDigest[:]), MetadataMaxResolvedTS: 200, ExactMirror: true, RemoteVersionsVerified: true, AllSegmentsVerified: true}
	coordStart := []byte("/kubebrain-internal/ks-tenant-a/")
	coordEnd := []byte("/kubebrain-internal/ks-tenant-a0")
	sourceExclusive := nativepitr.SourceRangeExclusiveReceipt{Format: nativepitr.SourceRangeExclusiveFormat, ClusterID: 11, PDAddrs: []string{"source-pd:2379"}, Keyspace: "tenant-a", StartKeyHex: task.StartKeyHex, EndKeyHex: task.EndKeyHex, CoordinationStartKeyHex: hex.EncodeToString(coordStart), CoordinationEndKeyHex: hex.EncodeToString(coordEnd), CoordinationRangeExcluded: true, SnapshotTS: 120, FullSnapshotReceiptSHA256: hex.EncodeToString(fullDigest[:]), CheckedAtUnix: 2_000_000_000, ReadOnly: true}
	target := nativepitr.TargetSnapshotEmptyReceipt{Format: nativepitr.TargetSnapshotEmptyFormat, ClusterID: 22, PDAddrs: []string{"pd:2379"}, Stores: []nativepitr.TargetStore{{ID: 1, Address: "tikv:20160"}}, SnapshotTS: 130, ScanScope: nativepitr.WholeTransactionalKeyspace, CheckedAtUnix: 2_000_000_000, ReadOnly: true}
	paths := []string{filepath.Join(dir, "task.json"), filepath.Join(dir, "full.json"), filepath.Join(dir, "artifacts.json"), filepath.Join(dir, "ready.json"), filepath.Join(dir, "logs.json"), filepath.Join(dir, "source-exclusive.json"), filepath.Join(dir, "target.json"), filepath.Join(dir, "source.logical.v2"), filepath.Join(dir, "source-capture.json")}
	for i, value := range []any{task, full, artifacts, ready, logs, sourceExclusive, target} {
		b, err := json.Marshal(value)
		require.NoError(t, err)
		require.NoError(t, os.WriteFile(paths[i], b, 0o600))
	}
	w, err := backupfile.NewAtomicWriter(paths[7], "/", 119)
	require.NoError(t, err)
	witnessStatus, err := w.Commit()
	require.NoError(t, err)
	witnessBytes, err := os.ReadFile(paths[7])
	require.NoError(t, err)
	witnessDigest := sha256.Sum256(witnessBytes)
	capture := nativepitr.SourceCaptureReceipt{Format: nativepitr.SourceCaptureReceiptFormat, SourceCaptureFenceSHA256: testDigest, TaskCreateSHA256: hex.EncodeToString(taskDigest[:]), FullSnapshotSHA256: hex.EncodeToString(fullDigest[:]), WitnessFileSHA256: hex.EncodeToString(witnessDigest[:]), WitnessContentSHA256: witnessStatus.SHA256, OperationID: "capture-a", SourceClusterID: task.ClusterID, Keyspace: task.Keyspace, WitnessRevision: witnessStatus.Revision, FullBackupTS: full.BackupTS, CaptureTS: 180, FenceSnapshotTS: 130, ContinuousSourceExclusion: true, AllFenceKeysReopened: true, FinalizedAtUnix: 2_000_000_001}
	captureBytes, err := json.Marshal(capture)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(paths[8], captureBytes, 0o600))
	var out bytes.Buffer
	require.NoError(t, run(paths[0], paths[1], paths[2], paths[3], paths[4], paths[5], paths[6], paths[7], paths[8], nativepitr.ReceiptPlanInputs{}, &out))
	var plan nativepitr.Plan
	require.NoError(t, json.Unmarshal(out.Bytes(), &plan))
	require.NoError(t, plan.Validate())
	require.Equal(t, byte('\n'), out.Bytes()[out.Len()-1])
}

func TestRunRejectsMissingReceipts(t *testing.T) {
	require.ErrorContains(t, run("", "", "", "", "", "", "", "", "", nativepitr.ReceiptPlanInputs{}, &bytes.Buffer{}), "required")
}
