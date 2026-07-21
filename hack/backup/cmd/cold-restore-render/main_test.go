package main

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRenderColdRestoreManifest(t *testing.T) {
	manifest, err := render(validReceipt(), "target-snapshots", "target-storage")
	require.NoError(t, err)
	require.Equal(t, "List", manifest["kind"])
	items := manifest["items"].([]any)
	require.Len(t, items, 19)

	contents := 0
	snapshots := 0
	claims := 0
	for _, raw := range items {
		item := raw.(map[string]any)
		switch item["kind"] {
		case "VolumeSnapshotContent":
			contents++
			spec := item["spec"].(map[string]any)
			require.Equal(t, "Retain", spec["deletionPolicy"])
			require.Equal(t, "csi.example.test", spec["driver"])
		case "VolumeSnapshot":
			snapshots++
		case "PersistentVolumeClaim":
			claims++
			spec := item["spec"].(map[string]any)
			require.Equal(t, "target-storage", spec["storageClassName"])
			require.Equal(t, "VolumeSnapshot", spec["dataSource"].(map[string]any)["kind"])
		}
	}
	require.Equal(t, 6, contents)
	require.Equal(t, 6, snapshots)
	require.Equal(t, 6, claims)
	tidbCluster := items[len(items)-1].(map[string]any)
	require.Equal(t, "TidbCluster", tidbCluster["kind"])
	require.Equal(t, true, tidbCluster["spec"].(map[string]any)["paused"])
	require.Equal(t, "tidb-cluster", tidbCluster["metadata"].(map[string]any)["namespace"])
}

func TestWriteAtomic(t *testing.T) {
	directory := t.TempDir()
	path := filepath.Join(directory, "restore.json")
	require.NoError(t, writeAtomic(path, []byte("manifest\n")))
	value, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Equal(t, "manifest\n", string(value))
	info, err := os.Stat(path)
	require.NoError(t, err)
	require.Equal(t, os.FileMode(0o600), info.Mode().Perm())
	matches, err := filepath.Glob(filepath.Join(directory, ".cold-restore-*.tmp"))
	require.NoError(t, err)
	require.Empty(t, matches)
}

func TestRenderColdRestoreManifestRejectsIncompleteReceipts(t *testing.T) {
	for _, tc := range []struct {
		name    string
		mutate  func(*receipt)
		message string
	}{
		{name: "v1 receipt", mutate: func(r *receipt) { r.Format = "kubebrain.cold-physical-snapshot.v1" }, message: "unsupported receipt"},
		{name: "renamed target blueprint", mutate: func(r *receipt) {
			r.Inventory.RecoveryBlueprint.TidbCluster["metadata"].(map[string]any)["name"] = "other"
		}, message: "identity does not match"},
		{name: "replica mismatch", mutate: func(r *receipt) {
			r.Inventory.RecoveryBlueprint.TidbCluster["spec"].(map[string]any)["pd"].(map[string]any)["replicas"] = float64(2)
		}, message: "PVC count does not match"},
		{name: "duplicate PVC mapping", mutate: func(r *receipt) {
			r.Snapshots[1].SourcePVC = r.Snapshots[0].SourcePVC
			r.Snapshots[1].Component = r.Snapshots[0].Component
		}, message: "duplicate snapshot"},
		{name: "duplicate handle", mutate: func(r *receipt) { r.Snapshots[1].SnapshotHandle = r.Snapshots[0].SnapshotHandle }, message: "handles must be unique"},
		{name: "undersized claim", mutate: func(r *receipt) { r.Inventory.PDPVCs[0].RequestedStorage = "512Mi" }, message: "smaller than snapshot"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			value := validReceipt()
			tc.mutate(&value)
			_, err := render(value, "target-snapshots", "target-storage")
			require.ErrorContains(t, err, tc.message)
		})
	}
}

func validReceipt() receipt {
	value := receipt{Format: "kubebrain.cold-physical-snapshot.v2", OperationID: "restore-test"}
	value.SemanticWitness.Format = "kubebrain.logical.v2"
	value.SemanticWitness.Prefix = "/registry"
	value.SemanticWitness.Revision = 100
	value.SemanticWitness.Records = 1
	value.SemanticWitness.SHA256 = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	value.SemanticWitness.FileSHA256 = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	value.Inventory.VolumeSnapshotClass.Driver = "csi.example.test"
	value.Inventory.Storage.Namespace = "tidb-cluster"
	value.Inventory.Storage.TidbCluster = "kb"
	value.Inventory.Storage.UID = "uid-tidb"
	value.Inventory.Storage.ClusterID = "12345"
	value.Inventory.RecoveryBlueprint.TidbCluster = map[string]any{
		"apiVersion": "pingcap.com/v1alpha1",
		"kind":       "TidbCluster",
		"metadata":   map[string]any{"name": "kb", "namespace": "tidb-cluster"},
		"spec": map[string]any{
			"version": "v8.5.3",
			"pd":      map[string]any{"replicas": float64(3)},
			"tikv":    map[string]any{"replicas": float64(3)},
		},
	}
	for _, component := range []string{"pd", "tikv"} {
		for ordinal := 0; ordinal < 3; ordinal++ {
			name := component + "-kb-" + component + "-" + string(rune('0'+ordinal))
			volume := pvc{
				Name: name, UID: "uid-" + name, VolumeMode: "Filesystem", AccessModes: []string{"ReadWriteOnce"}, RequestedStorage: "1Gi",
				Labels: map[string]string{"app.kubernetes.io/instance": "kb", "app.kubernetes.io/component": component, "app.kubernetes.io/managed-by": "tidb-operator"},
			}
			if component == "pd" {
				value.Inventory.PDPVCs = append(value.Inventory.PDPVCs, volume)
			} else {
				value.Inventory.TiKVPVCs = append(value.Inventory.TiKVPVCs, volume)
			}
			value.Snapshots = append(value.Snapshots, snapshot{
				SourcePVC: name, Component: component, SnapshotHandle: "handle-" + name, RestoreSize: "1Gi",
			})
		}
	}
	return value
}
