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
	artifacts := nativepitr.ArtifactReceipt{Format: nativepitr.ArtifactReceiptFormat, ClusterID: 11, Keyspace: "tenant-a", TaskName: "task-a", BackupTS: 120, StoragePrefix: full.StoragePrefix, FullReceiptSHA256: hex.EncodeToString(fullDigest[:]), BackupMetaSHA256: testDigest, Objects: objects, ObjectCount: 2, TotalBytes: 110, ManifestSHA256: hex.EncodeToString(manifestDigest[:]), ExactMirror: true, Encryption: "plaintext", AllObjectsVerified: true}
	ready := nativepitr.TaskReadyReceipt{Format: nativepitr.TaskReadyFormat, ClusterID: 11, Keyspace: "tenant-a", TaskName: "task-a", StartTS: 100, CommittedAtTS: 110, EndTS: 1000, GlobalCheckpointTS: 200, AdvancerOwner: "owner-1", PreflightSHA256: testDigest, BootstrapSafePointID: task.BootstrapSafePointID, BootstrapReleased: true, MetadataSnapshotValid: true}
	readyBytes, err := json.Marshal(ready)
	require.NoError(t, err)
	readyDigest := sha256.Sum256(readyBytes)
	logObjects := []nativepitr.LogArtifactObject{{Name: "v1/backupmeta/1.meta", Bytes: 10, SHA256: testDigest, Kind: "metadata"}, {Name: "v1/log/1.log", Bytes: 20, SHA256: testDigest, Kind: "data"}}
	logObjectBytes, err := json.Marshal(logObjects)
	require.NoError(t, err)
	logManifestDigest := sha256.Sum256(logObjectBytes)
	logs := nativepitr.LogArtifactReceipt{Format: nativepitr.LogArtifactReceiptFormat, ClusterID: 11, Keyspace: "tenant-a", TaskName: "task-a", TaskCreateSHA256: hex.EncodeToString(taskDigest[:]), TaskReadySHA256: hex.EncodeToString(readyDigest[:]), StartTS: 100, GlobalCheckpointTS: 200, StoragePrefix: task.LogStoragePrefix, StorageSHA256: task.LogStorageSHA256, Objects: logObjects, ObjectCount: 2, MetadataCount: 1, DataObjectCount: 1, VerifiedSegmentCount: 1, TotalBytes: 30, ManifestSHA256: hex.EncodeToString(logManifestDigest[:]), MetadataMaxResolvedTS: 200, ExactMirror: true, AllSegmentsVerified: true}
	paths := []string{filepath.Join(dir, "task.json"), filepath.Join(dir, "full.json"), filepath.Join(dir, "artifacts.json"), filepath.Join(dir, "ready.json"), filepath.Join(dir, "logs.json")}
	for i, value := range []any{task, full, artifacts, ready, logs} {
		b, err := json.Marshal(value)
		require.NoError(t, err)
		require.NoError(t, os.WriteFile(paths[i], b, 0o600))
	}
	var out bytes.Buffer
	require.NoError(t, run(paths[0], paths[1], paths[2], paths[3], paths[4], nativepitr.ReceiptPlanInputs{TargetClusterID: 22, EmptyWitnessSHA256: testDigest, RestoreTS: 180}, &out))
	var plan nativepitr.Plan
	require.NoError(t, json.Unmarshal(out.Bytes(), &plan))
	require.NoError(t, plan.Validate())
	require.Equal(t, byte('\n'), out.Bytes()[out.Len()-1])
}

func TestRunRejectsMissingReceipts(t *testing.T) {
	require.ErrorContains(t, run("", "", "", "", "", nativepitr.ReceiptPlanInputs{}, &bytes.Buffer{}), "required")
}
