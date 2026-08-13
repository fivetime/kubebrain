package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/kubewharf/kubebrain/hack/backup/internal/nativepitr"
	"github.com/stretchr/testify/require"
)

func TestRunPublishesCanonicalReceiptWithoutOverwrite(t *testing.T) {
	dir := t.TempDir()
	input, output := filepath.Join(dir, "input.json"), filepath.Join(dir, "receipt.json")
	value := nativepitr.TargetProvisioningReceipt{Format: nativepitr.TargetProvisioningReceiptFormat, Namespace: "tidb-cluster", TidbCluster: "kb", TidbClusterUID: "tc-1", ClusterID: 1, PDReplicas: 1, TiKVReplicas: 1, Ready: true, ObservedAtUnix: 1, ReadOnlyInspection: true, Volumes: []nativepitr.TargetVolumeIdentity{{Component: "pd", PVCName: "pd-0", PVCUID: "pvc-1", PVName: "pv-1", PVUID: "pvuid-1", CSIDriver: "csi.test", VolumeHandle: "disk-1"}, {Component: "tikv", PVCName: "tikv-0", PVCUID: "pvc-2", PVName: "pv-2", PVUID: "pvuid-2", CSIDriver: "csi.test", VolumeHandle: "disk-2"}}}
	b, err := json.MarshalIndent(value, "", "  ")
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(input, b, 0o600))
	require.NoError(t, run(input, output))
	got, err := os.ReadFile(output)
	require.NoError(t, err)
	canonical, err := json.Marshal(value)
	require.NoError(t, err)
	require.Equal(t, append(canonical, '\n'), got)
	require.Equal(t, os.FileMode(0o600), mustStat(t, output).Mode().Perm())
	require.Error(t, run(input, output))
}

func mustStat(t *testing.T, path string) os.FileInfo {
	t.Helper()
	info, err := os.Stat(path)
	require.NoError(t, err)
	return info
}
