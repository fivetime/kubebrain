package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/kubewharf/kubebrain/hack/backup/internal/backupfile"
	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/mvccpb"
	"google.golang.org/protobuf/proto"
)

func TestCompareKVsRequiresPhysicalMetadataIdentity(t *testing.T) {
	expected := map[string]expectedKV{
		"key": {key: []byte("key"), value: []byte("value"), createRevision: 10, modRevision: 12, version: 2, lease: 99},
	}
	valid := &mvccpb.KeyValue{Key: []byte("key"), Value: []byte("value"), CreateRevision: 10, ModRevision: 12, Version: 2, Lease: 99}
	require.NoError(t, compareKVs(expected, []*mvccpb.KeyValue{valid}))

	for _, tc := range []struct {
		name   string
		mutate func(*mvccpb.KeyValue)
	}{
		{name: "value", mutate: func(kv *mvccpb.KeyValue) { kv.Value = []byte("other") }},
		{name: "create revision", mutate: func(kv *mvccpb.KeyValue) { kv.CreateRevision++ }},
		{name: "mod revision", mutate: func(kv *mvccpb.KeyValue) { kv.ModRevision++ }},
		{name: "version", mutate: func(kv *mvccpb.KeyValue) { kv.Version++ }},
		{name: "lease identity", mutate: func(kv *mvccpb.KeyValue) { kv.Lease++ }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			changed := proto.Clone(valid).(*mvccpb.KeyValue)
			tc.mutate(changed)
			require.ErrorContains(t, compareKVs(expected, []*mvccpb.KeyValue{changed}), "metadata/value mismatch")
		})
	}
	require.ErrorContains(t, compareKVs(expected, nil), "record count")
}

func TestValidateReceiptChain(t *testing.T) {
	witness := []byte("immutable-witness")
	status := backupfile.Status{Format: backupfile.Format, Prefix: "/registry", Revision: 100, Records: 3, Leases: 1, SHA256: "artifact-sha"}
	snapshot := snapshotReceipt{Format: "kubebrain.cold-physical-snapshot.v2", OperationID: "operation-a"}
	snapshot.Witness.Format = status.Format
	snapshot.Witness.Prefix = status.Prefix
	snapshot.Witness.Revision = status.Revision
	snapshot.Witness.Records = status.Records
	snapshot.Witness.Leases = status.Leases
	snapshot.Witness.SHA256 = status.SHA256
	snapshot.Witness.FileSHA256 = digest(witness)
	snapshotData, err := json.Marshal(snapshot)
	require.NoError(t, err)
	restore := restoreReceipt{Format: "kubebrain.cold-physical-restore.v1", OperationID: snapshot.OperationID, SourceReceiptSHA: digest(snapshotData)}
	restore.Target.ClusterID = "12345"
	restore.RestoreManifest.Format = "kubernetes-list.canonical-json.v1"
	restore.RestoreManifest.SHA256 = digest([]byte("manifest"))
	restore.RestoreManifest.ItemCount = 4
	restore.RestoreManifest.VolumeSnapshotContents = 1
	restore.RestoreManifest.VolumeSnapshots = 1
	restore.RestoreManifest.PersistentVolumeClaims = 1
	restore.RestoreManifest.TidbClusters = 1
	restore.VolumeSnapshotContents = []struct{}{{}}
	restore.PVCs = []struct{}{{}}
	restoreData, err := json.Marshal(restore)
	require.NoError(t, err)

	_, _, err = validateReceiptChain(status, witness, snapshotData, restoreData)
	require.NoError(t, err)
	_, _, err = validateReceiptChain(status, []byte("tampered"), snapshotData, restoreData)
	require.ErrorContains(t, err, "witness binding mismatch")

	brokenRestore := restore
	brokenRestore.SourceReceiptSHA = digest([]byte("other"))
	brokenData, err := json.Marshal(brokenRestore)
	require.NoError(t, err)
	_, _, err = validateReceiptChain(status, witness, snapshotData, brokenData)
	require.ErrorContains(t, err, "does not bind")

	brokenRestore = restore
	brokenRestore.RestoreManifest.SHA256 = ""
	brokenData, err = json.Marshal(brokenRestore)
	require.NoError(t, err)
	_, _, err = validateReceiptChain(status, witness, snapshotData, brokenData)
	require.ErrorContains(t, err, "canonical restore manifest")
}

func TestWriteAtomicSemanticReceipt(t *testing.T) {
	directory := t.TempDir()
	path := filepath.Join(directory, "receipt.json")
	require.NoError(t, writeAtomic(path, semanticReceipt{Format: "test"}))
	info, err := os.Stat(path)
	require.NoError(t, err)
	require.Equal(t, os.FileMode(0o600), info.Mode().Perm())
	require.Error(t, writeAtomic(filepath.Join(directory, "missing", "receipt.json"), semanticReceipt{}))
}

func TestEqualStrings(t *testing.T) {
	require.True(t, equalStrings([]string{"a", "b"}, []string{"a", "b"}))
	require.False(t, equalStrings([]string{"a"}, []string{"b"}))
	require.False(t, equalStrings([]string{"a"}, []string{"a", "b"}))
}
