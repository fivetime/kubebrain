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

func TestValidateRestoreManifestBinding(t *testing.T) {
	snapshot := snapshotReceipt{OperationID: "restore-test"}
	snapshot.Inventory.VolumeSnapshotClass.Driver = "csi.example.test"
	snapshot.Inventory.Storage.Namespace = "tidb-cluster"
	snapshot.Inventory.Storage.TidbCluster = "kb"
	snapshot.Inventory.Storage.UID = "uid-tidb"
	snapshot.Inventory.Storage.ClusterID = "12345"
	snapshot.Snapshots = append(snapshot.Snapshots, struct {
		SourcePVC      string `json:"source_pvc"`
		Component      string `json:"component"`
		SnapshotHandle string `json:"snapshot_handle"`
	}{SourcePVC: "pd-kb-pd-0", Component: "pd", SnapshotHandle: "handle-pd-0"})
	objectName := restoreManifestObjectName(snapshot.OperationID, "pd-kb-pd-0")
	restore := restoreReceipt{}
	restore.RestoreManifest.Format = "kubernetes-list.canonical-json.v1"
	restore.RestoreManifest.ItemCount = 4
	restore.RestoreManifest.VolumeSnapshotContents = 1
	restore.RestoreManifest.VolumeSnapshots = 1
	restore.RestoreManifest.PersistentVolumeClaims = 1
	restore.RestoreManifest.TidbClusters = 1
	manifest := map[string]any{
		"apiVersion": "v1",
		"kind":       "List",
		"items": []any{
			map[string]any{
				"kind": "VolumeSnapshotContent",
				"metadata": map[string]any{
					"name": objectName,
				},
				"spec": map[string]any{
					"deletionPolicy":          "Retain",
					"driver":                  "csi.example.test",
					"volumeSnapshotClassName": "target-snapshots",
					"source":                  map[string]any{"snapshotHandle": "handle-pd-0"},
					"volumeSnapshotRef": map[string]any{
						"name": objectName, "namespace": "tidb-cluster",
					},
				},
			},
			map[string]any{
				"kind": "VolumeSnapshot",
				"metadata": map[string]any{
					"name": objectName, "namespace": "tidb-cluster",
				},
				"spec": map[string]any{
					"source": map[string]any{"volumeSnapshotContentName": objectName},
				},
			},
			map[string]any{
				"kind": "PersistentVolumeClaim",
				"metadata": map[string]any{
					"name": "pd-kb-pd-0", "namespace": "tidb-cluster",
					"labels": map[string]any{"app.kubernetes.io/component": "pd"},
				},
				"spec": map[string]any{
					"dataSource": map[string]any{"kind": "VolumeSnapshot", "name": objectName},
				},
			},
			map[string]any{
				"kind": "TidbCluster",
				"metadata": map[string]any{
					"name": "kb", "namespace": "tidb-cluster",
					"annotations": map[string]any{
						"kubebrain.io/source-cluster-id":      "12345",
						"kubebrain.io/source-tidbcluster-uid": "uid-tidb",
					},
				},
				"spec": map[string]any{"paused": true},
			},
		},
	}
	canonical, err := json.Marshal(manifest)
	require.NoError(t, err)
	restore.RestoreManifest.SHA256 = digest(canonical)
	pretty, err := json.MarshalIndent(manifest, "", "  ")
	require.NoError(t, err)
	require.NoError(t, validateRestoreManifestBinding(pretty, restore, snapshot))

	manifest["items"].([]any)[3] = map[string]any{"kind": "ConfigMap"}
	tampered, err := json.Marshal(manifest)
	require.NoError(t, err)
	require.ErrorContains(t, validateRestoreManifestBinding(tampered, restore, snapshot), "does not match")

	manifest = map[string]any{}
	require.NoError(t, json.Unmarshal(canonical, &manifest))
	vsc := manifest["items"].([]any)[0].(map[string]any)
	vsc["spec"].(map[string]any)["source"].(map[string]any)["snapshotHandle"] = "other-handle"
	tampered, err = json.Marshal(manifest)
	require.NoError(t, err)
	restore.RestoreManifest.SHA256 = digest(tampered)
	require.ErrorContains(t, validateRestoreManifestBinding(tampered, restore, snapshot), "VolumeSnapshotContent")

	manifest = map[string]any{}
	require.NoError(t, json.Unmarshal(canonical, &manifest))
	items := manifest["items"].([]any)
	manifest["items"] = append(items, items[0])
	tampered, err = json.Marshal(manifest)
	require.NoError(t, err)
	restore.RestoreManifest.SHA256 = digest(tampered)
	restore.RestoreManifest.ItemCount = 5
	restore.RestoreManifest.VolumeSnapshotContents = 2
	require.ErrorContains(t, validateRestoreManifestBinding(tampered, restore, snapshot), "cover every")
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
