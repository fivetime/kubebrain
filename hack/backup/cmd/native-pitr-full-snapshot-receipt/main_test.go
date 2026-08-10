package main

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/kubewharf/kubebrain/hack/backup/internal/nativepitr"
	"github.com/kubewharf/kubebrain/pkg/backend/coder"
	backuppb "github.com/pingcap/kvproto/pkg/brpb"
	"github.com/stretchr/testify/require"
)

func TestRunBuildsReceiptFromBackupMeta(t *testing.T) {
	dir := t.TempDir()
	ks, err := coder.NewKeyspace("tenant-a")
	require.NoError(t, err)
	task := nativepitr.TaskCreateReceipt{Format: nativepitr.TaskCreateFormat, ClusterID: 11, Keyspace: "tenant-a", TaskName: "task-a", StartTS: 100, CommittedAtTS: 110, EndTS: 1000, StartKeyHex: hex.EncodeToString(ks.ObjectKeyspaceStart()), EndKeyHex: hex.EncodeToString(ks.ObjectKeyspaceEnd()), LogStoragePrefix: "s3://bucket/immutable/log-a", LogStorageSHA256: testDigest, PreflightSHA256: testDigest, OwnerKey: nativepitr.TaskOwnerKey, BootstrapSafePointID: "kubebrain-native-pitr-bootstrap-operation-a", BootstrapSafePointTTL: 7200, AtomicMetadataCreated: true}
	taskBytes, err := json.Marshal(task)
	require.NoError(t, err)
	metaBytes, err := (&backuppb.BackupMeta{ClusterId: 11, StartVersion: 0, EndVersion: 120, IsTxnKv: true, Files: []*backuppb.File{{Name: "1.sst"}}}).Marshal()
	require.NoError(t, err)
	taskPath, metaPath := filepath.Join(dir, "task.json"), filepath.Join(dir, "backupmeta")
	require.NoError(t, os.WriteFile(taskPath, taskBytes, 0o600))
	require.NoError(t, os.WriteFile(metaPath, metaBytes, 0o600))
	var out bytes.Buffer
	require.NoError(t, run(taskPath, metaPath, "s3://bucket/immutable/full-a", &out))
	var receipt nativepitr.FullSnapshotReceipt
	require.NoError(t, json.Unmarshal(out.Bytes(), &receipt))
	require.Equal(t, uint64(120), receipt.BackupTS)
	require.Equal(t, 1, receipt.LegacyFileCount)
}

const testDigest = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
