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
	backuppb "github.com/pingcap/kvproto/pkg/brpb"
	"github.com/stretchr/testify/require"
)

const testDigest = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"

func TestRunVerifiesLogMirror(t *testing.T) {
	dir := t.TempDir()
	ks, err := coder.NewKeyspace("tenant-a")
	require.NoError(t, err)
	task := nativepitr.TaskCreateReceipt{Format: nativepitr.TaskCreateFormat, ClusterID: 11, Keyspace: "tenant-a", TaskName: "task-a", StartTS: 100, CommittedAtTS: 110, EndTS: 1000, StartKeyHex: hex.EncodeToString(ks.ObjectKeyspaceStart()), EndKeyHex: hex.EncodeToString(ks.ObjectKeyspaceEnd()), LogStoragePrefix: "s3://bucket/log-a", LogStorageSHA256: testDigest, PreflightSHA256: testDigest, OwnerKey: nativepitr.TaskOwnerKey, BootstrapSafePointID: "kubebrain-native-pitr-bootstrap-operation-a", BootstrapSafePointTTL: 7200, AtomicMetadataCreated: true}
	ready := nativepitr.TaskReadyReceipt{Format: nativepitr.TaskReadyFormat, ClusterID: 11, Keyspace: "tenant-a", TaskName: "task-a", StartTS: 100, CommittedAtTS: 110, EndTS: 1000, GlobalCheckpointTS: 200, AdvancerOwner: "owner", PreflightSHA256: testDigest, BootstrapSafePointID: task.BootstrapSafePointID, BootstrapReleased: true, MetadataSnapshotValid: true}
	data := []byte("log-events")
	digest := sha256.Sum256(data)
	file := &backuppb.DataFileInfo{Path: "v1/log/data.log", Sha256: digest[:], NumberOfEntries: 1, MinTs: 100, MaxTs: 200, ResolvedTs: 200, Cf: "write", Length: uint64(len(data))}
	metaBytes, err := (&backuppb.Metadata{Files: []*backuppb.DataFileInfo{file}, StoreId: 1, MinTs: 100, MaxTs: 200, ResolvedTs: 200, MetaVersion: backuppb.MetaVersion_V1}).Marshal()
	require.NoError(t, err)
	root := filepath.Join(dir, "mirror")
	require.NoError(t, os.MkdirAll(filepath.Join(root, "v1/backupmeta"), 0o700))
	require.NoError(t, os.MkdirAll(filepath.Join(root, "v1/log"), 0o700))
	require.NoError(t, os.WriteFile(filepath.Join(root, "v1/backupmeta/1.meta"), metaBytes, 0o600))
	require.NoError(t, os.WriteFile(filepath.Join(root, file.Path), data, 0o600))
	paths := []string{filepath.Join(dir, "task.json"), filepath.Join(dir, "ready.json")}
	for i, receipt := range []any{task, ready} {
		b, err := json.Marshal(receipt)
		require.NoError(t, err)
		require.NoError(t, os.WriteFile(paths[i], b, 0o600))
	}
	var out bytes.Buffer
	require.NoError(t, run(paths[0], paths[1], root, &out))
	got, err := nativepitr.DecodeLogArtifactReceipt(bytes.NewReader(out.Bytes()))
	require.NoError(t, err)
	require.Equal(t, 1, got.VerifiedSegmentCount)
}

func TestRunRequiresInputs(t *testing.T) {
	require.ErrorContains(t, run("", "", "", &bytes.Buffer{}), "required")
}
