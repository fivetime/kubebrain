package main

import (
	"encoding/base64"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/kubewharf/kubebrain/hack/backup/internal/backupfile"
	"github.com/kubewharf/kubebrain/hack/backup/internal/record"
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

func TestFileDigest(t *testing.T) {
	path := filepath.Join(t.TempDir(), "witness.jsonl")
	data := []byte("immutable-witness\n")
	require.NoError(t, os.WriteFile(path, data, 0o600))
	got, err := fileDigest(path)
	require.NoError(t, err)
	require.Equal(t, digest(data), got)
}

func TestOpenStableWitnessRejectsDriftDuringValidation(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "witness.jsonl")
	drift := filepath.Join(dir, "witness-drift.jsonl")
	writeVerifiedWitness(t, path, "/registry", 100)
	writeVerifiedWitness(t, drift, "/registry", 101)
	afterWitnessDigestForTest = func() {
		require.NoError(t, os.WriteFile(path, mustRead(t, drift), 0o600))
	}
	t.Cleanup(func() {
		afterWitnessDigestForTest = func() {}
	})

	verified, _, err := openStableWitness(path)
	require.Nil(t, verified)
	require.ErrorContains(t, err, "witness file changed after validation")
}

func TestValidateReceiptChain(t *testing.T) {
	witness := []byte("immutable-witness")
	witnessFileSHA := digest(witness)
	status := backupfile.Status{Format: backupfile.Format, Prefix: "/registry", Revision: 100, CreatedAtUnix: 1760000000, Records: 3, Leases: 1, SHA256: "artifact-sha"}
	snapshot := snapshotReceipt{Format: "kubebrain.cold-physical-snapshot.v2", OperationID: "operation-a"}
	snapshot.CreatedAt = "2026-07-21T00:00:00Z"
	snapshot.Inventory.Format = "kubebrain.cold-physical-snapshot-preflight.v2"
	snapshot.Inventory.VolumeSnapshotClass.Name = "source-snapshots"
	snapshot.Inventory.VolumeSnapshotClass.Driver = "csi.example.test"
	snapshot.Inventory.VolumeSnapshotClass.DeletionPolicy = "Retain"
	snapshot.Inventory.KubeBrain.Namespace = "kubebrain-system"
	snapshot.Inventory.KubeBrain.StatefulSet = "kubebrain"
	snapshot.Inventory.KubeBrain.UID = "uid-kubebrain"
	snapshot.Inventory.Storage.Namespace = "tidb-cluster"
	snapshot.Inventory.Storage.TidbCluster = "kb"
	snapshot.Inventory.Storage.UID = "uid-source-tidb"
	snapshot.Inventory.Storage.ClusterID = "12345"
	snapshot.Inventory.RecoveryBlueprint.TidbCluster = recoveryTidbCluster{
		APIVersion: "pingcap.com/v1alpha1", Kind: "TidbCluster", Metadata: recoveryMetadata{Name: "kb", Namespace: "tidb-cluster"},
		Spec: json.RawMessage(`{"pd":{"replicas":1},"tikv":{"replicas":0}}`),
	}
	snapshot.Inventory.PDPVCs = []sourcePVC{{
		Name: "pd-kb-pd-0", UID: "uid-source-pvc", PV: "source-pv", Phase: "Bound",
		Labels:     map[string]string{"app.kubernetes.io/instance": "kb", "app.kubernetes.io/component": "pd"},
		VolumeMode: "Filesystem", AccessModes: []string{"ReadWriteOnce"}, StorageClass: "source-storage", RequestedStorage: "1Gi",
	}}
	snapshot.Snapshots = append(snapshot.Snapshots, sourceSnapshot{
		Name: "source-snapshot", UID: "uid-source-snapshot", Content: "source-content", ContentUID: "uid-source-content",
		SourcePVC: "pd-kb-pd-0", Component: "pd", SnapshotHandle: "handle-pd-0", RestoreSize: "1Gi",
	})
	snapshot.Witness.Format = status.Format
	snapshot.Witness.Prefix = status.Prefix
	snapshot.Witness.Revision = status.Revision
	snapshot.Witness.CreatedAtUnix = status.CreatedAtUnix
	snapshot.Witness.Records = status.Records
	snapshot.Witness.Leases = status.Leases
	snapshot.Witness.SHA256 = status.SHA256
	snapshot.Witness.FileSHA256 = witnessFileSHA
	snapshotData, err := json.Marshal(snapshot)
	require.NoError(t, err)
	restore := restoreReceipt{Format: "kubebrain.cold-physical-restore.v1", OperationID: snapshot.OperationID, SourceReceiptSHA: digest(snapshotData)}
	restore.CompletedAt = "2026-07-21T00:05:00Z"
	restore.Target.KubeSystemUID = "uid-kube-system"
	restore.Target.NamespaceUID = "uid-namespace"
	restore.Target.Namespace = snapshot.Inventory.Storage.Namespace
	restore.Target.TidbCluster = snapshot.Inventory.Storage.TidbCluster
	restore.Target.TidbClusterUID = "uid-restored-tidb"
	restore.Target.ClusterID = "12345"
	restore.RestoreManifest.Format = "kubernetes-list.canonical-json.v1"
	restore.RestoreManifest.SHA256 = digest([]byte("manifest"))
	restore.RestoreManifest.ItemCount = 4
	restore.RestoreManifest.VolumeSnapshotContents = 1
	restore.RestoreManifest.VolumeSnapshots = 1
	restore.RestoreManifest.PersistentVolumeClaims = 1
	restore.RestoreManifest.TidbClusters = 1
	restore.VolumeSnapshotContents = []restoredVolumeSnapshotContent{{
		Name: restoreManifestObjectName(snapshot.OperationID, "pd-kb-pd-0"), UID: "uid-content",
		Driver: "csi.example.test", SnapshotClass: "target-snapshots", DeletionPolicy: "Retain", SnapshotHandle: "handle-pd-0",
		SnapshotRef: restoredSnapshotRef{APIVersion: "snapshot.storage.k8s.io/v1", Kind: "VolumeSnapshot",
			Namespace: "tidb-cluster", Name: restoreManifestObjectName(snapshot.OperationID, "pd-kb-pd-0")},
	}}
	restore.VolumeSnapshots = []restoredVolumeSnapshot{{
		Name: restoreManifestObjectName(snapshot.OperationID, "pd-kb-pd-0"), UID: "uid-snapshot",
		SnapshotClass: "target-snapshots", SourceContent: restoreManifestObjectName(snapshot.OperationID, "pd-kb-pd-0"),
		BoundContent: restoreManifestObjectName(snapshot.OperationID, "pd-kb-pd-0"), Ready: true,
	}}
	restore.PVCs = []restoredPVC{{
		Name: "pd-kb-pd-0", UID: "uid-pvc", PV: "pv-pd-kb-pd-0", Phase: "Bound",
		StorageClass: "target-storage", VolumeMode: "Filesystem", AccessModes: []string{"ReadWriteOnce"}, RequestedStorage: "1Gi",
		DataSource: restoredDataSource{APIGroup: "snapshot.storage.k8s.io", Kind: "VolumeSnapshot", Name: restoreManifestObjectName(snapshot.OperationID, "pd-kb-pd-0")},
	}}
	restore.PVs = []restoredPV{{
		Name: "pv-pd-kb-pd-0", UID: "uid-pv", Phase: "Bound", StorageClass: "target-storage", VolumeMode: "Filesystem",
		Capacity: "1Gi", CSIDriver: "csi.example.test", VolumeHandle: "volume-pd-0",
		ClaimRef: restoredClaimRef{APIVersion: "v1", Kind: "PersistentVolumeClaim", Namespace: "tidb-cluster", Name: "pd-kb-pd-0", UID: "uid-pvc"},
	}}
	restoreData, err := json.Marshal(restore)
	require.NoError(t, err)

	_, _, err = validateReceiptChain(status, witnessFileSHA, snapshotData, restoreData)
	require.NoError(t, err)

	equalTimeSnapshot := cloneSnapshotReceipt(t, snapshot)
	equalTimeSnapshot.CreatedAt = time.Unix(status.CreatedAtUnix, 0).UTC().Format(time.RFC3339)
	equalTimeSnapshotData, err := json.Marshal(equalTimeSnapshot)
	require.NoError(t, err)
	equalTimeRestore := cloneRestoreReceipt(restore)
	equalTimeRestore.SourceReceiptSHA = digest(equalTimeSnapshotData)
	equalTimeRestore.CompletedAt = equalTimeSnapshot.CreatedAt
	equalTimeRestoreData, err := json.Marshal(equalTimeRestore)
	require.NoError(t, err)
	_, _, err = validateReceiptChain(status, witnessFileSHA, equalTimeSnapshotData, equalTimeRestoreData)
	require.NoError(t, err, "second-resolution receipt timestamps may be equal")

	var executorReceipt map[string]any
	require.NoError(t, json.Unmarshal(restoreData, &executorReceipt))
	executorReceipt["volume_snapshots"] = []any{map[string]any{
		"name": restoreManifestObjectName(snapshot.OperationID, "pd-kb-pd-0"), "uid": "uid-snapshot",
		"snapshot_class": "target-snapshots", "source_content": restoreManifestObjectName(snapshot.OperationID, "pd-kb-pd-0"),
		"bound_content": restoreManifestObjectName(snapshot.OperationID, "pd-kb-pd-0"), "ready": true,
	}}
	executorReceipt["pvs"] = []any{map[string]any{
		"name": "pv-pd-kb-pd-0", "uid": "uid-pv", "phase": "Bound", "storage_class": "target-storage",
		"volume_mode": "Filesystem", "capacity": "1Gi", "csi_driver": "csi.example.test", "volume_handle": "volume-pd-0",
		"claim_ref": map[string]any{"api_version": "v1", "kind": "PersistentVolumeClaim", "namespace": "tidb-cluster", "name": "pd-kb-pd-0", "uid": "uid-pvc"},
	}}
	executorReceipt["pvcs"] = []any{map[string]any{
		"name": "pd-kb-pd-0", "uid": "uid-pvc", "pv": "pv-pd-kb-pd-0", "phase": "Bound",
		"storage_class": "target-storage", "volume_mode": "Filesystem", "access_modes": []any{"ReadWriteOnce"},
		"requested_storage": "1Gi", "data_source": map[string]any{"api_group": "snapshot.storage.k8s.io", "kind": "VolumeSnapshot", "name": restoreManifestObjectName(snapshot.OperationID, "pd-kb-pd-0")},
	}}
	executorData, err := json.Marshal(executorReceipt)
	require.NoError(t, err)
	_, _, err = validateReceiptChain(status, witnessFileSHA, snapshotData, executorData)
	require.NoError(t, err, "the semantic verifier must accept the executor's complete restore receipt schema")

	snapshotUnknown := append(append([]byte(nil), snapshotData[:len(snapshotData)-1]...), []byte(`,"unexpected":true}`)...)
	_, _, err = validateReceiptChain(status, witnessFileSHA, snapshotUnknown, restoreData)
	require.ErrorContains(t, err, "unknown field")

	restoreTrailing := append(append([]byte(nil), restoreData...), []byte(`{"trailing":true}`)...)
	_, _, err = validateReceiptChain(status, witnessFileSHA, snapshotData, restoreTrailing)
	require.ErrorContains(t, err, "trailing JSON")

	_, _, err = validateReceiptChain(status, digest([]byte("tampered")), snapshotData, restoreData)
	require.ErrorContains(t, err, "witness binding mismatch")

	brokenSnapshot := snapshot
	brokenSnapshot.CreatedAt = "not-a-time"
	brokenSnapshotData, err := json.Marshal(brokenSnapshot)
	require.NoError(t, err)
	_, _, err = validateReceiptChain(status, witnessFileSHA, brokenSnapshotData, restoreData)
	require.ErrorContains(t, err, "created_at")

	brokenSnapshot = cloneSnapshotReceipt(t, snapshot)
	brokenSnapshot.CreatedAt = time.Unix(status.CreatedAtUnix-1, 0).UTC().Format(time.RFC3339)
	brokenSnapshotData, err = json.Marshal(brokenSnapshot)
	require.NoError(t, err)
	brokenRestore := cloneRestoreReceipt(restore)
	brokenRestore.SourceReceiptSHA = digest(brokenSnapshotData)
	brokenData, err := json.Marshal(brokenRestore)
	require.NoError(t, err)
	_, _, err = validateReceiptChain(status, witnessFileSHA, brokenSnapshotData, brokenData)
	require.ErrorContains(t, err, "snapshot receipt predates semantic witness")

	brokenSnapshot = snapshot
	brokenSnapshot.Inventory.Format = "other"
	brokenSnapshotData, err = json.Marshal(brokenSnapshot)
	require.NoError(t, err)
	_, _, err = validateReceiptChain(status, witnessFileSHA, brokenSnapshotData, restoreData)
	require.ErrorContains(t, err, "inventory format")

	brokenSnapshot = cloneSnapshotReceipt(t, snapshot)
	brokenSnapshot.Inventory.PDPVCs[0].RequestedStorage = "512Mi"
	brokenSnapshotData, err = json.Marshal(brokenSnapshot)
	require.NoError(t, err)
	brokenRestore = cloneRestoreReceipt(restore)
	brokenRestore.SourceReceiptSHA = digest(brokenSnapshotData)
	brokenData, err = json.Marshal(brokenRestore)
	require.NoError(t, err)
	_, _, err = validateReceiptChain(status, witnessFileSHA, brokenSnapshotData, brokenData)
	require.ErrorContains(t, err, "restore size")

	brokenSnapshot = cloneSnapshotReceipt(t, snapshot)
	brokenSnapshot.Snapshots[0].UID = ""
	brokenSnapshotData, err = json.Marshal(brokenSnapshot)
	require.NoError(t, err)
	brokenRestore = cloneRestoreReceipt(restore)
	brokenRestore.SourceReceiptSHA = digest(brokenSnapshotData)
	brokenData, err = json.Marshal(brokenRestore)
	require.NoError(t, err)
	_, _, err = validateReceiptChain(status, witnessFileSHA, brokenSnapshotData, brokenData)
	require.ErrorContains(t, err, "snapshot inventory")

	brokenSnapshot = cloneSnapshotReceipt(t, snapshot)
	brokenSnapshot.Witness.CreatedAtUnix++
	brokenSnapshotData, err = json.Marshal(brokenSnapshot)
	require.NoError(t, err)
	brokenRestore = cloneRestoreReceipt(restore)
	brokenRestore.SourceReceiptSHA = digest(brokenSnapshotData)
	brokenData, err = json.Marshal(brokenRestore)
	require.NoError(t, err)
	_, _, err = validateReceiptChain(status, witnessFileSHA, brokenSnapshotData, brokenData)
	require.ErrorContains(t, err, "witness binding mismatch")

	brokenRestore = restore
	brokenRestore.SourceReceiptSHA = digest([]byte("other"))
	brokenData, err = json.Marshal(brokenRestore)
	require.NoError(t, err)
	_, _, err = validateReceiptChain(status, witnessFileSHA, snapshotData, brokenData)
	require.ErrorContains(t, err, "does not bind")

	brokenRestore = restore
	brokenRestore.RestoreManifest.SHA256 = ""
	brokenData, err = json.Marshal(brokenRestore)
	require.NoError(t, err)
	_, _, err = validateReceiptChain(status, witnessFileSHA, snapshotData, brokenData)
	require.ErrorContains(t, err, "canonical restore manifest")

	brokenRestore = restore
	brokenRestore.RestoreManifest.SHA256 = strings.ToUpper(restore.RestoreManifest.SHA256)
	brokenData, err = json.Marshal(brokenRestore)
	require.NoError(t, err)
	_, _, err = validateReceiptChain(status, witnessFileSHA, snapshotData, brokenData)
	require.ErrorContains(t, err, "canonical restore manifest")

	brokenRestore = cloneRestoreReceipt(restore)
	brokenRestore.VolumeSnapshotContents[0].SnapshotHandle = "other-handle"
	brokenData, err = json.Marshal(brokenRestore)
	require.NoError(t, err)
	_, _, err = validateReceiptChain(status, witnessFileSHA, snapshotData, brokenData)
	require.ErrorContains(t, err, "content inventory")

	brokenRestore = cloneRestoreReceipt(restore)
	brokenRestore.PVCs[0].Phase = "Pending"
	brokenData, err = json.Marshal(brokenRestore)
	require.NoError(t, err)
	_, _, err = validateReceiptChain(status, witnessFileSHA, snapshotData, brokenData)
	require.ErrorContains(t, err, "PVC inventory")

	brokenRestore = cloneRestoreReceipt(restore)
	brokenRestore.PVs[0].Capacity = "1023Mi"
	brokenData, err = json.Marshal(brokenRestore)
	require.NoError(t, err)
	_, _, err = validateReceiptChain(status, witnessFileSHA, snapshotData, brokenData)
	require.ErrorContains(t, err, "capacity does not satisfy")

	brokenRestore = cloneRestoreReceipt(restore)
	brokenRestore.VolumeSnapshots[0].Ready = false
	brokenData, err = json.Marshal(brokenRestore)
	require.NoError(t, err)
	_, _, err = validateReceiptChain(status, witnessFileSHA, snapshotData, brokenData)
	require.ErrorContains(t, err, "VolumeSnapshot inventory")

	brokenRestore = cloneRestoreReceipt(restore)
	brokenRestore.PVs[0].ClaimRef.UID = "substituted-claim"
	brokenData, err = json.Marshal(brokenRestore)
	require.NoError(t, err)
	_, _, err = validateReceiptChain(status, witnessFileSHA, snapshotData, brokenData)
	require.ErrorContains(t, err, "PV inventory")

	brokenRestore = restore
	brokenRestore.Target.ClusterID = "54321"
	brokenData, err = json.Marshal(brokenRestore)
	require.NoError(t, err)
	_, _, err = validateReceiptChain(status, witnessFileSHA, snapshotData, brokenData)
	require.ErrorContains(t, err, "target")

	brokenRestore = restore
	brokenRestore.CompletedAt = "not-a-time"
	brokenData, err = json.Marshal(brokenRestore)
	require.NoError(t, err)
	_, _, err = validateReceiptChain(status, witnessFileSHA, snapshotData, brokenData)
	require.ErrorContains(t, err, "completed_at")

	brokenRestore = cloneRestoreReceipt(restore)
	brokenRestore.CompletedAt = "2026-07-20T23:59:59Z"
	brokenData, err = json.Marshal(brokenRestore)
	require.NoError(t, err)
	_, _, err = validateReceiptChain(status, witnessFileSHA, snapshotData, brokenData)
	require.ErrorContains(t, err, "restore receipt predates snapshot receipt")

	brokenRestore = restore
	brokenRestore.Target.TidbClusterUID = snapshot.Inventory.Storage.UID
	brokenData, err = json.Marshal(brokenRestore)
	require.NoError(t, err)
	_, _, err = validateReceiptChain(status, witnessFileSHA, snapshotData, brokenData)
	require.ErrorContains(t, err, "target")
}

func TestValidateRestoreManifestBinding(t *testing.T) {
	snapshot := snapshotReceipt{OperationID: "restore-test"}
	snapshot.Inventory.VolumeSnapshotClass.Driver = "csi.example.test"
	snapshot.Inventory.Storage.Namespace = "tidb-cluster"
	snapshot.Inventory.Storage.TidbCluster = "kb"
	snapshot.Inventory.Storage.UID = "uid-tidb"
	snapshot.Inventory.Storage.ClusterID = "12345"
	snapshot.Inventory.PDPVCs = []sourcePVC{{
		Name: "pd-kb-pd-0", VolumeMode: "Filesystem", AccessModes: []string{"ReadWriteOnce"}, RequestedStorage: "1Gi",
	}}
	snapshot.Snapshots = append(snapshot.Snapshots, sourceSnapshot{
		SourcePVC: "pd-kb-pd-0", Component: "pd", SnapshotHandle: "handle-pd-0", RestoreSize: "1Gi",
	})
	objectName := restoreManifestObjectName(snapshot.OperationID, "pd-kb-pd-0")
	restore := restoreReceipt{}
	restore.RestoreManifest.Format = "kubernetes-list.canonical-json.v1"
	restore.RestoreManifest.ItemCount = 4
	restore.RestoreManifest.VolumeSnapshotContents = 1
	restore.RestoreManifest.VolumeSnapshots = 1
	restore.RestoreManifest.PersistentVolumeClaims = 1
	restore.RestoreManifest.TidbClusters = 1
	restore.VolumeSnapshotContents = []restoredVolumeSnapshotContent{{
		Name: objectName, UID: "uid-content", Driver: "csi.example.test", SnapshotClass: "target-snapshots",
		DeletionPolicy: "Retain", SnapshotHandle: "handle-pd-0",
		SnapshotRef: restoredSnapshotRef{APIVersion: "snapshot.storage.k8s.io/v1", Kind: "VolumeSnapshot", Namespace: "tidb-cluster", Name: objectName},
	}}
	restore.VolumeSnapshots = []restoredVolumeSnapshot{{
		Name: objectName, UID: "uid-snapshot", SnapshotClass: "target-snapshots", SourceContent: objectName, BoundContent: objectName, Ready: true,
	}}
	restore.PVCs = []restoredPVC{{
		Name: "pd-kb-pd-0", UID: "uid-pvc", PV: "pv-pd-kb-pd-0", Phase: "Bound", StorageClass: "target-storage",
		VolumeMode: "Filesystem", AccessModes: []string{"ReadWriteOnce"}, RequestedStorage: "1Gi",
		DataSource: restoredDataSource{APIGroup: "snapshot.storage.k8s.io", Kind: "VolumeSnapshot", Name: objectName},
	}}
	manifest := map[string]any{
		"apiVersion": "v1",
		"kind":       "List",
		"items": []any{
			map[string]any{
				"apiVersion": "snapshot.storage.k8s.io/v1",
				"kind":       "VolumeSnapshotContent",
				"metadata": map[string]any{
					"name": objectName,
					"labels": map[string]any{
						"kubebrain.io/operation-id": "restore-test",
					},
				},
				"spec": map[string]any{
					"deletionPolicy":          "Retain",
					"driver":                  "csi.example.test",
					"volumeSnapshotClassName": "target-snapshots",
					"source":                  map[string]any{"snapshotHandle": "handle-pd-0"},
					"volumeSnapshotRef": map[string]any{
						"apiVersion": "snapshot.storage.k8s.io/v1", "kind": "VolumeSnapshot",
						"name": objectName, "namespace": "tidb-cluster",
					},
				},
			},
			map[string]any{
				"apiVersion": "snapshot.storage.k8s.io/v1",
				"kind":       "VolumeSnapshot",
				"metadata": map[string]any{
					"name": objectName, "namespace": "tidb-cluster",
					"labels": map[string]any{
						"kubebrain.io/operation-id": "restore-test",
					},
				},
				"spec": map[string]any{
					"volumeSnapshotClassName": "target-snapshots",
					"source":                  map[string]any{"volumeSnapshotContentName": objectName},
				},
			},
			map[string]any{
				"apiVersion": "v1",
				"kind":       "PersistentVolumeClaim",
				"metadata": map[string]any{
					"name": "pd-kb-pd-0", "namespace": "tidb-cluster",
					"labels": map[string]any{
						"app.kubernetes.io/component": "pd",
						"kubebrain.io/operation-id":   "restore-test",
					},
				},
				"spec": map[string]any{
					"accessModes":      []any{"ReadWriteOnce"},
					"volumeMode":       "Filesystem",
					"storageClassName": "target-storage",
					"resources":        map[string]any{"requests": map[string]any{"storage": "1Gi"}},
					"dataSource": map[string]any{
						"apiGroup": "snapshot.storage.k8s.io", "kind": "VolumeSnapshot", "name": objectName,
					},
				},
			},
			map[string]any{
				"apiVersion": "pingcap.com/v1alpha1",
				"kind":       "TidbCluster",
				"metadata": map[string]any{
					"name": "kb", "namespace": "tidb-cluster",
					"annotations": map[string]any{
						"kubebrain.io/cold-restore-operation": "restore-test",
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

	tamperedReceipt := cloneRestoreReceipt(restore)
	tamperedReceipt.PVCs[0].RequestedStorage = "512Mi"
	require.ErrorContains(t, validateRestoreManifestBinding(pretty, tamperedReceipt, snapshot), "PVC inventory")
	tamperedReceipt = cloneRestoreReceipt(restore)
	tamperedReceipt.VolumeSnapshotContents[0].SnapshotClass = "substituted-class"
	require.ErrorContains(t, validateRestoreManifestBinding(pretty, tamperedReceipt, snapshot), "VolumeSnapshotContent inventory")
	tamperedSnapshot := snapshot
	tamperedSnapshot.Inventory.PDPVCs = append([]sourcePVC(nil), snapshot.Inventory.PDPVCs...)
	tamperedSnapshot.Inventory.PDPVCs[0].RequestedStorage = "2Gi"
	require.ErrorContains(t, validateRestoreManifestBinding(pretty, restore, tamperedSnapshot), "PVC")

	for _, tc := range []struct {
		name    string
		mutate  func(map[string]any)
		message string
	}{
		{
			name: "missing VSC operation label",
			mutate: func(value map[string]any) {
				vsc := value["items"].([]any)[0].(map[string]any)
				vsc["metadata"].(map[string]any)["labels"] = map[string]any{}
			},
			message: "VolumeSnapshotContent",
		},
		{
			name: "wrong VSC ref kind",
			mutate: func(value map[string]any) {
				vsc := value["items"].([]any)[0].(map[string]any)
				vsc["spec"].(map[string]any)["volumeSnapshotRef"].(map[string]any)["kind"] = "ConfigMap"
			},
			message: "VolumeSnapshotContent",
		},
		{
			name: "missing PVC dataSource apiGroup",
			mutate: func(value map[string]any) {
				pvc := value["items"].([]any)[2].(map[string]any)
				delete(pvc["spec"].(map[string]any)["dataSource"].(map[string]any), "apiGroup")
			},
			message: "PVC",
		},
		{
			name: "missing restore operation annotation",
			mutate: func(value map[string]any) {
				tidb := value["items"].([]any)[3].(map[string]any)
				delete(tidb["metadata"].(map[string]any)["annotations"].(map[string]any), "kubebrain.io/cold-restore-operation")
			},
			message: "TidbCluster",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var changed map[string]any
			require.NoError(t, json.Unmarshal(canonical, &changed))
			tc.mutate(changed)
			tampered, err := json.Marshal(changed)
			require.NoError(t, err)
			restore.RestoreManifest.SHA256 = digest(tampered)
			require.ErrorContains(t, validateRestoreManifestBinding(tampered, restore, snapshot), tc.message)
		})
	}

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
	require.NoError(t, writeAtomic(path, semanticReceipt{
		Format:                 "test",
		RestoreCompletedAt:     "2026-07-21T00:05:00Z",
		TargetKubeSystemUID:    "uid-kube-system",
		TargetNamespaceUID:     "uid-namespace",
		SourceTidbClusterUID:   "uid-source-tidb",
		RestoredTidbClusterUID: "uid-restored-tidb",
	}))
	info, err := os.Stat(path)
	require.NoError(t, err)
	require.Equal(t, os.FileMode(0o600), info.Mode().Perm())
	original, err := os.ReadFile(path)
	require.NoError(t, err)
	var receipt map[string]any
	require.NoError(t, json.Unmarshal(original, &receipt))
	require.Equal(t, "2026-07-21T00:05:00Z", receipt["restore_completed_at"])
	require.Equal(t, "uid-kube-system", receipt["target_kube_system_uid"])
	require.Equal(t, "uid-namespace", receipt["target_namespace_uid"])
	require.Equal(t, "uid-source-tidb", receipt["source_tidbcluster_uid"])
	require.Equal(t, "uid-restored-tidb", receipt["restored_tidbcluster_uid"])
	require.Error(t, writeAtomic(filepath.Join(directory, "missing", "receipt.json"), semanticReceipt{}))
	require.ErrorContains(t, writeAtomic(path, semanticReceipt{Format: "other"}), "already exists")
	data, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Equal(t, original, data)
}

func TestValidateSemanticReceiptRequiresCompleteIdentity(t *testing.T) {
	require.NoError(t, validateSemanticReceipt(validSemanticReceipt()))
	equalSecond := validSemanticReceipt()
	equalSecond.VerifiedAtUnix = 1_784_592_300
	require.NoError(t, validateSemanticReceipt(equalSecond), "second-resolution restore and verification times may be equal")

	for _, tc := range []struct {
		name   string
		mutate func(*semanticReceipt)
	}{
		{name: "bad format", mutate: func(r *semanticReceipt) { r.Format = "other" }},
		{name: "bad restore digest", mutate: func(r *semanticReceipt) { r.RestoreReceiptSHA256 = strings.ToUpper(r.RestoreReceiptSHA256) }},
		{name: "bad completed time", mutate: func(r *semanticReceipt) { r.RestoreCompletedAt = "not-a-time" }},
		{name: "source target UID reuse", mutate: func(r *semanticReceipt) { r.RestoredTidbClusterUID = r.SourceTidbClusterUID }},
		{name: "missing namespace UID", mutate: func(r *semanticReceipt) { r.TargetNamespaceUID = "" }},
		{name: "bad witness revision", mutate: func(r *semanticReceipt) { r.WitnessRevision = 0 }},
		{name: "missing historical proof", mutate: func(r *semanticReceipt) { r.HistoricalExact = false }},
		{name: "missing watch proof", mutate: func(r *semanticReceipt) { r.WatchProbeSucceeded = false }},
		{name: "delete before put", mutate: func(r *semanticReceipt) { r.ProbeDeleteRevision = r.ProbePutRevision }},
		{name: "missing verified time", mutate: func(r *semanticReceipt) { r.VerifiedAtUnix = 0 }},
		{name: "verified before restore", mutate: func(r *semanticReceipt) { r.VerifiedAtUnix = 1_784_592_299 }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			receipt := validSemanticReceipt()
			tc.mutate(&receipt)
			require.ErrorContains(t, validateSemanticReceipt(receipt), "semantic receipt")
		})
	}
}

func validSemanticReceipt() semanticReceipt {
	return semanticReceipt{
		Format:                 "kubebrain.cold-physical-semantic-verify.v1",
		OperationID:            "restore-test",
		RestoreReceiptSHA256:   strings.Repeat("a", 64),
		SnapshotReceiptSHA256:  strings.Repeat("b", 64),
		RestoreCompletedAt:     "2026-07-21T00:05:00Z",
		WitnessFormat:          backupfile.Format,
		WitnessSHA256:          strings.Repeat("c", 64),
		WitnessRevision:        100,
		WitnessRecords:         1,
		WitnessLeases:          0,
		RestoreManifestSHA256:  strings.Repeat("d", 64),
		TargetKubeSystemUID:    "uid-kube-system",
		TargetNamespaceUID:     "uid-namespace",
		SourceTidbClusterUID:   "uid-source-tidb",
		RestoredTidbClusterUID: "uid-restored-tidb",
		RestoredClusterID:      "12345",
		HistoricalExact:        true,
		CurrentExact:           true,
		LeaseIdentityExact:     true,
		WatchProbeSucceeded:    true,
		ProbePutRevision:       101,
		ProbeDeleteRevision:    102,
		VerifiedAtUnix:         1_784_592_301,
	}
}

func TestReadBoundedJSONFileRejectsOversizedInput(t *testing.T) {
	path := filepath.Join(t.TempDir(), "restore.json")
	require.NoError(t, os.WriteFile(path, make([]byte, maxColdRestoreJSONBytes+1), 0o600))
	_, err := readBoundedJSONFile(path, "restore receipt")
	require.ErrorContains(t, err, "restore receipt exceeds")
}

func TestReadStableBoundedJSONFileRejectsDriftDuringCapture(t *testing.T) {
	path := filepath.Join(t.TempDir(), "restore.json")
	require.NoError(t, os.WriteFile(path, []byte(`{"format":"initial"}`), 0o600))
	afterStableJSONReadForTest = func(description string) {
		if description == "restore receipt" {
			require.NoError(t, os.WriteFile(path, []byte(`{"format":"drifted"}`), 0o600))
		}
	}
	t.Cleanup(func() {
		afterStableJSONReadForTest = func(string) {}
	})

	_, _, err := readStableBoundedJSONFile(path, "restore receipt")
	require.ErrorContains(t, err, "restore receipt changed during capture")
}

func TestVerifyBoundedJSONFileDigestRejectsDriftAfterValidation(t *testing.T) {
	path := filepath.Join(t.TempDir(), "manifest.json")
	data := []byte(`{"kind":"List","items":[]}`)
	require.NoError(t, os.WriteFile(path, data, 0o600))
	_, sha, err := readStableBoundedJSONFile(path, "restore manifest")
	require.NoError(t, err)

	require.NoError(t, os.WriteFile(path, []byte(`{"kind":"List","items":[{}]}`), 0o600))
	require.ErrorContains(t, verifyBoundedJSONFileDigest(path, sha, "restore manifest"), "restore manifest changed after validation")
}

func TestEqualStrings(t *testing.T) {
	require.True(t, equalStrings([]string{"a", "b"}, []string{"a", "b"}))
	require.False(t, equalStrings([]string{"a"}, []string{"b"}))
	require.False(t, equalStrings([]string{"a"}, []string{"a", "b"}))
}

func TestValidateProbePrefix(t *testing.T) {
	require.NoError(t, validateProbePrefix("/__kubebrain/cold-restore-verify/instance-a"))
	require.NoError(t, validateProbePrefix("/tmp/cold-restore-verify"))

	for _, tc := range []struct {
		name    string
		prefix  string
		message string
	}{
		{name: "empty", prefix: "", message: "absolute key prefix"},
		{name: "relative", prefix: "relative", message: "absolute key prefix"},
		{name: "root", prefix: "/", message: "must not target"},
		{name: "registry", prefix: "/registry", message: "must not target"},
		{name: "registry child", prefix: "/registry/pods", message: "must not target"},
		{name: "control", prefix: "/__kubebrain/cold\nrestore", message: "control characters"},
		{name: "del", prefix: "/__kubebrain/cold\x7frestore", message: "control characters"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.ErrorContains(t, validateProbePrefix(tc.prefix), tc.message)
		})
	}
}

func cloneRestoreReceipt(value restoreReceipt) restoreReceipt {
	value.VolumeSnapshotContents = append([]restoredVolumeSnapshotContent(nil), value.VolumeSnapshotContents...)
	value.VolumeSnapshots = append([]restoredVolumeSnapshot(nil), value.VolumeSnapshots...)
	value.PVs = append([]restoredPV(nil), value.PVs...)
	value.PVCs = append([]restoredPVC(nil), value.PVCs...)
	for i := range value.PVCs {
		value.PVCs[i].AccessModes = append([]string(nil), value.PVCs[i].AccessModes...)
	}
	return value
}

func cloneSnapshotReceipt(t *testing.T, value snapshotReceipt) snapshotReceipt {
	t.Helper()
	data, err := json.Marshal(value)
	require.NoError(t, err)
	var cloned snapshotReceipt
	require.NoError(t, json.Unmarshal(data, &cloned))
	return cloned
}

func writeVerifiedWitness(t *testing.T, path, prefix string, revision int64) {
	t.Helper()
	writer, err := backupfile.NewAtomicWriter(path, prefix, revision)
	require.NoError(t, err)
	require.NoError(t, writer.Add(record.Record{
		Key: base64Value(prefix + "/key"), Value: base64Value("value"),
		CreateRevision: revision, ModRevision: revision, Version: 1,
	}))
	_, err = writer.Commit()
	require.NoError(t, err)
}

func base64Value(value string) string {
	return base64.StdEncoding.EncodeToString([]byte(value))
}

func mustRead(t *testing.T, path string) []byte {
	t.Helper()
	data, err := os.ReadFile(path)
	require.NoError(t, err)
	return data
}
