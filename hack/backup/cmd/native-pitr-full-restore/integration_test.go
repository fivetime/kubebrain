package main

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/kubewharf/kubebrain/hack/backup/internal/nativepitr"
	"github.com/kubewharf/kubebrain/hack/backup/internal/pitrinventory"
	"github.com/kubewharf/kubebrain/pkg/backend/coder"
	storagetikv "github.com/kubewharf/kubebrain/pkg/storage/tikv"
	"github.com/stretchr/testify/require"
	pd "github.com/tikv/pd/client"
)

// TestNativeFullRestoreRealBR is an opt-in destructive integration test for
// two disposable, dedicated PD/TiKV clusters. The caller owns their lifecycle.
func TestNativeFullRestoreRealBR(t *testing.T) {
	sourcePD := os.Getenv("KUBEBRAIN_NATIVE_PITR_SOURCE_PD")
	targetPD := os.Getenv("KUBEBRAIN_NATIVE_PITR_TARGET_PD")
	br := os.Getenv("KUBEBRAIN_NATIVE_PITR_BR")
	if sourcePD == "" || targetPD == "" || br == "" {
		t.Skip("set KUBEBRAIN_NATIVE_PITR_SOURCE_PD, KUBEBRAIN_NATIVE_PITR_TARGET_PD, and KUBEBRAIN_NATIVE_PITR_BR")
	}
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Minute)
	defer cancel()
	sourceAddrs, err := parseAddrs(sourcePD)
	require.NoError(t, err)
	targetAddrs, err := parseAddrs(targetPD)
	require.NoError(t, err)

	sourceKV, err := storagetikv.NewKvStorage(sourceAddrs, 1, storagetikv.Security{})
	require.NoError(t, err)
	defer sourceKV.Close()
	ks, err := coder.NewKeyspace("restore-integration")
	require.NoError(t, err)
	key := ks.NewCoder().EncodeRevisionKey([]byte("/native-full"))
	value := []byte("kubebrain-native-full-restore-v1")
	batch := sourceKV.BeginBatchWrite()
	batch.Put(key, value, 0)
	require.NoError(t, batch.Commit(ctx))
	backupTS, err := sourceKV.GetTimestampOracle(ctx)
	require.NoError(t, err)

	root := t.TempDir()
	backup := exec.CommandContext(ctx, br, "backup", "txn", "--pd", strings.Join(sourceAddrs, ","), "--storage", "local://"+root, "--backupts", fmt.Sprint(backupTS), "--checksum=false", "--log-file", "/dev/stderr")
	backup.Stdout, backup.Stderr = os.Stderr, os.Stderr
	require.NoError(t, backup.Run())

	pdc, err := pd.NewClientWithContext(ctx, sourceAddrs, pd.SecurityOption{})
	require.NoError(t, err)
	clusterID := pdc.GetClusterID(ctx)
	pdc.Close()
	require.NotZero(t, clusterID)
	const d = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
	endTS := backupTS + (uint64((10*time.Minute)/time.Millisecond) << 18)
	task := nativepitr.TaskCreateReceipt{Format: nativepitr.TaskCreateFormat, ClusterID: clusterID, Keyspace: ks.Name(), TaskName: "restore-integration", StartTS: backupTS - 2, CommittedAtTS: backupTS - 1, EndTS: endTS, StartKeyHex: hex.EncodeToString(ks.ObjectKeyspaceStart()), EndKeyHex: hex.EncodeToString(ks.ObjectKeyspaceEnd()), LogStoragePrefix: "s3://integration/log/task", LogStorageSHA256: d, PreflightSHA256: d, OwnerKey: nativepitr.TaskOwnerKey, BootstrapSafePointID: "kubebrain-native-pitr-bootstrap-restore-integration", BootstrapSafePointTTL: 7200, AtomicMetadataCreated: true}
	taskBytes := canonicalFile(t, filepath.Join(t.TempDir(), "task.json"), task)
	full, err := nativepitr.BuildFullSnapshot(task, digest(taskBytes), "s3://integration/full/snapshot", mustRead(t, filepath.Join(root, "backupmeta")))
	require.NoError(t, err)
	fullPath := filepath.Join(t.TempDir(), "full.json")
	fullBytes := canonicalFile(t, fullPath, full)

	inventory := inventoryForRoot(t, root, "integration", "full/snapshot")
	inventoryPath := filepath.Join(t.TempDir(), "inventory.json")
	inventoryBytes := canonicalFile(t, inventoryPath, inventory)
	artifact, err := nativepitr.VerifyFullArtifacts(full, digest(fullBytes), inventory, digest(inventoryBytes), root)
	require.NoError(t, err)
	artifactPath := filepath.Join(t.TempDir(), "artifact.json")
	artifactBytes := canonicalFile(t, artifactPath, artifact)

	sourceEvidence, err := nativepitr.InspectLiveSourceRangeExclusive(ctx, full, digest(fullBytes), sourceAddrs, "", "", "", time.Now().Unix())
	require.NoError(t, err)
	sourcePath := filepath.Join(t.TempDir(), "source.json")
	sourceBytes := canonicalFile(t, sourcePath, sourceEvidence)
	targetEvidence, err := nativepitr.InspectLiveTargetSnapshotEmpty(ctx, targetAddrs, "", "", "", time.Now().Unix())
	require.NoError(t, err)
	targetPath := filepath.Join(t.TempDir(), "target.json")
	targetBytes := canonicalFile(t, targetPath, targetEvidence)

	ready := nativepitr.TaskReadyReceipt{Format: nativepitr.TaskReadyFormat, ClusterID: clusterID, Keyspace: task.Keyspace, TaskName: task.TaskName, StartTS: task.StartTS, CommittedAtTS: task.CommittedAtTS, EndTS: task.EndTS, GlobalCheckpointTS: full.BackupTS + 1, AdvancerOwner: "integration-owner", PreflightSHA256: d, BootstrapSafePointID: task.BootstrapSafePointID, BootstrapReleased: true, MetadataSnapshotValid: true}
	readyBytes, err := json.Marshal(ready)
	require.NoError(t, err)
	emptyObjects := []nativepitr.LogArtifactObject{}
	emptyManifest, err := json.Marshal(emptyObjects)
	require.NoError(t, err)
	logs := nativepitr.LogArtifactReceipt{Format: nativepitr.LogArtifactReceiptFormat, ClusterID: clusterID, Keyspace: task.Keyspace, TaskName: task.TaskName, TaskCreateSHA256: digest(taskBytes), TaskReadySHA256: digest(readyBytes), StartTS: task.StartTS, GlobalCheckpointTS: ready.GlobalCheckpointTS, StoragePrefix: task.LogStoragePrefix, StorageSHA256: task.LogStorageSHA256, RemoteInventorySHA256: d, ObjectStoreID: "integration", Bucket: "integration", ObjectPrefix: "log/task", MinRetainUntilUnix: 2_100_000_000, InventoryCheckedAtUnix: 2_000_000_000, Objects: emptyObjects, ManifestSHA256: digest(emptyManifest), ExactMirror: true, RemoteVersionsVerified: true, AllSegmentsVerified: true}
	plan, err := nativepitr.BuildFromReceipts(task, full, artifact, ready, logs, sourceEvidence, targetEvidence, nativepitr.ReceiptPlanInputs{TaskCreateSHA256: digest(taskBytes), FullSnapshotSHA256: digest(fullBytes), ArtifactReceiptSHA256: digest(artifactBytes), TaskReadySHA256: digest(readyBytes), LogArtifactSHA256: d, SourceExclusiveSHA256: digest(sourceBytes), TargetReceiptSHA256: digest(targetBytes), RestoreTS: full.BackupTS})
	require.NoError(t, err)
	planPath := filepath.Join(t.TempDir(), "plan.json")
	planBytes := canonicalFile(t, planPath, plan)

	var receiptOut strings.Builder
	err = execute(ctx, options{plan: planPath, full: fullPath, artifacts: artifactPath, inventory: inventoryPath, artifactRoot: root, sourceExclusive: sourcePath, target: targetPath, pdAddrs: strings.Join(targetAddrs, ","), brBinary: br, approve: digest(planBytes), timeout: 3 * time.Minute}, osRunner{}, nativepitr.InspectLiveTargetSnapshotEmpty, &receiptOut, os.Stderr, time.Now)
	require.NoError(t, err)
	_, err = nativepitr.DecodeFullRestoreExecution(strings.NewReader(receiptOut.String()))
	require.NoError(t, err)
	targetKV, err := storagetikv.NewKvStorage(targetAddrs, 1, storagetikv.Security{})
	require.NoError(t, err)
	defer targetKV.Close()
	got, err := targetKV.Get(ctx, key)
	require.NoError(t, err)
	require.Equal(t, value, got)
}

func canonicalFile(t *testing.T, path string, v any) []byte {
	t.Helper()
	b, err := json.Marshal(v)
	require.NoError(t, err)
	b = append(b, '\n')
	require.NoError(t, os.WriteFile(path, b, 0o600))
	return b
}
func mustRead(t *testing.T, path string) []byte {
	t.Helper()
	b, err := os.ReadFile(path)
	require.NoError(t, err)
	return b
}
func inventoryForRoot(t *testing.T, root, bucket, prefix string) pitrinventory.Receipt {
	t.Helper()
	var entries []pitrinventory.Entry
	err := filepath.WalkDir(root, func(path string, e fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if path == root || e.IsDir() {
			return nil
		}
		b, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		name := filepath.ToSlash(rel)
		entries = append(entries, pitrinventory.Entry{Name: name, ObjectKey: prefix + "/" + name, VersionID: "integration-version-" + fmt.Sprint(len(entries)+1), Bytes: int64(len(b)), SHA256: digest(b), RetentionMode: "COMPLIANCE", RetainUntilUnix: 2_100_000_000})
		return nil
	})
	require.NoError(t, err)
	sort.Slice(entries, func(i, j int) bool { return entries[i].Name < entries[j].Name })
	var total uint64
	for _, e := range entries {
		total += uint64(e.Bytes)
	}
	return pitrinventory.Receipt{Format: pitrinventory.Format, ObjectStoreID: "integration", Bucket: bucket, Prefix: prefix, Entries: entries, ObjectCount: len(entries), TotalBytes: total, Pages: 1, PaginationExhausted: true, ExactVersionsVerified: true, MinRetainUntilUnix: 2_100_000_000, CheckedAtUnix: 2_000_000_000}
}
