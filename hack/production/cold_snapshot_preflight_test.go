package production_test

import (
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestColdSnapshotPreflight(t *testing.T) {
	for _, tc := range []struct {
		name        string
		resources   string
		policy      string
		tidbID      string
		kubebrainID string
		pvcJSON     string
		wantOK      bool
		wantOutput  string
	}{
		{name: "valid inventory", wantOK: true},
		{name: "missing snapshot API", resources: "none", wantOutput: "VolumeSnapshot API is unavailable"},
		{name: "snapshot class deletes content", policy: "Delete", wantOutput: "deletionPolicy=Retain"},
		{name: "wrong storage identity", tidbID: "wrong\t7662961163671170154", wantOutput: "TidbCluster identity mismatch"},
		{name: "wrong kubebrain identity", kubebrainID: "wrong", wantOutput: "StatefulSet UID mismatch"},
		{name: "unbound TiKV PVC", pvcJSON: coldSnapshotPVCJSON("Pending"), wantOutput: "TiKV PVC inventory must be Bound"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			fakeKubectl := filepath.Join(dir, "kubectl")
			require.NoError(t, os.WriteFile(fakeKubectl, []byte(`#!/usr/bin/env bash
set -euo pipefail
if [[ "$*" == *"api-resources"* ]]; then
  if [[ "$FAKE_RESOURCES" == "none" ]]; then exit 0; fi
  printf '%s\n' volumesnapshots.snapshot.storage.k8s.io volumesnapshotclasses.snapshot.storage.k8s.io
elif [[ "$*" == *"get volumesnapshotclass"* ]]; then
  printf 'csi.example.test\t%s' "$FAKE_POLICY"
elif [[ "$*" == *"get tidbcluster"* ]]; then
  IFS=$'\t' read -r uid cluster_id <<<"$FAKE_TIDB_ID"
  printf '{"apiVersion":"pingcap.com/v1alpha1","kind":"TidbCluster","metadata":{"name":"kb","namespace":"tidb-cluster","uid":"%s"},"spec":{"version":"v8.5.3","pd":{"replicas":3},"tikv":{"replicas":3}},"status":{"clusterID":"%s"}}' "$uid" "$cluster_id"
elif [[ "$*" == *"get statefulset"* ]]; then
  printf '%s' "$FAKE_KUBEBRAIN_ID"
elif [[ "$*" == *"get pvc"* ]]; then
  printf '%s' "$FAKE_PVC_JSON"
else
  exit 1
fi
`), 0o755))
			policy := tc.policy
			if policy == "" {
				policy = "Retain"
			}
			tidbID := tc.tidbID
			if tidbID == "" {
				tidbID = "uid-tidb\t7662961163671170154"
			}
			kubebrainID := tc.kubebrainID
			if kubebrainID == "" {
				kubebrainID = "uid-kubebrain"
			}
			pvcJSON := tc.pvcJSON
			if pvcJSON == "" {
				pvcJSON = coldSnapshotPVCJSON("Bound")
			}
			command := exec.Command("bash", "../backup/cold-snapshot-preflight.sh")
			command.Env = append(os.Environ(),
				"KUBECTL="+fakeKubectl,
				"ALLOW_COLD_PHYSICAL_SNAPSHOT=true",
				"VOLUME_SNAPSHOT_CLASS=retained",
				"EXPECTED_KUBEBRAIN_STATEFULSET_UID=uid-kubebrain",
				"EXPECTED_TIDB_CLUSTER_UID=uid-tidb",
				"EXPECTED_TIKV_CLUSTER_ID=7662961163671170154",
				"FAKE_RESOURCES="+tc.resources,
				"FAKE_POLICY="+policy,
				"FAKE_TIDB_ID="+tidbID,
				"FAKE_KUBEBRAIN_ID="+kubebrainID,
				"FAKE_PVC_JSON="+pvcJSON,
			)
			output, err := command.CombinedOutput()
			if tc.wantOK {
				require.NoError(t, err, string(output))
				var manifest map[string]any
				require.NoError(t, json.Unmarshal(output, &manifest))
				require.Equal(t, "kubebrain.cold-physical-snapshot-preflight.v2", manifest["format"])
			} else {
				require.Error(t, err, string(output))
				require.Contains(t, string(output), tc.wantOutput)
			}
		})
	}
}

func TestColdSnapshotPreflightRequiresExplicitApproval(t *testing.T) {
	command := exec.Command("bash", "../backup/cold-snapshot-preflight.sh")
	command.Env = append(os.Environ(), "ALLOW_COLD_PHYSICAL_SNAPSHOT=false")
	output, err := command.CombinedOutput()
	require.Error(t, err)
	require.Contains(t, string(output), "ALLOW_COLD_PHYSICAL_SNAPSHOT=true")
}

func coldSnapshotPVCJSON(tikvPhase string) string {
	items := make([]map[string]any, 0, 6)
	for _, component := range []string{"pd", "tikv"} {
		for ordinal := range 3 {
			phase := "Bound"
			if component == "tikv" {
				phase = tikvPhase
			}
			name := component + "-kb-" + component + "-" + string(rune('0'+ordinal))
			items = append(items, map[string]any{
				"metadata": map[string]any{"name": name, "uid": "uid-" + name, "labels": map[string]any{
					"app.kubernetes.io/component": component, "app.kubernetes.io/instance": "kb", "app.kubernetes.io/managed-by": "tidb-operator",
				}},
				"spec": map[string]any{
					"volumeName": "pv-" + name, "storageClassName": "fast", "volumeMode": "Filesystem",
					"accessModes": []string{"ReadWriteOnce"}, "resources": map[string]any{"requests": map[string]any{"storage": "1Gi"}},
				},
				"status": map[string]any{"phase": phase},
			})
		}
	}
	value, err := json.Marshal(map[string]any{"items": items})
	if err != nil {
		panic(err)
	}
	return string(value)
}
