package production_test

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
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
		provisioner string
		pvDriver    string
		wantOK      bool
		wantOutput  string
	}{
		{name: "valid inventory", wantOK: true},
		{name: "missing snapshot API", resources: "none", wantOutput: "VolumeSnapshot API is unavailable"},
		{name: "snapshot class deletes content", policy: "Delete", wantOutput: "deletionPolicy=Retain"},
		{name: "storage class uses another CSI driver", provisioner: "other.csi.test", wantOutput: "StorageClass fast provisioner"},
		{name: "source PV uses another CSI driver", pvDriver: "other.csi.test", wantOutput: "source PV identity does not match PVC"},
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
elif [[ "$*" == *"get pv "* ]]; then
  name="$(sed -n 's/.*get pv \([^ ]*\).*/\1/p' <<<"$*")"
  pvc="${name#pv-}"
  printf '{"metadata":{"name":"%s","uid":"uid-%s"},"spec":{"storageClassName":"fast","volumeMode":"Filesystem","claimRef":{"apiVersion":"v1","kind":"PersistentVolumeClaim","namespace":"tidb-cluster","name":"%s","uid":"uid-%s"},"csi":{"driver":"%s","volumeHandle":"handle-%s"}},"status":{"phase":"Bound"}}' "$name" "$name" "$pvc" "$pvc" "$FAKE_PV_DRIVER" "$name"
elif [[ "$*" == *"get storageclass"* ]]; then
  printf '%s' "$FAKE_PROVISIONER"
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
			provisioner := tc.provisioner
			if provisioner == "" {
				provisioner = "csi.example.test"
			}
			pvDriver := tc.pvDriver
			if pvDriver == "" {
				pvDriver = "csi.example.test"
			}
			env := []string{
				"KUBECTL=" + fakeKubectl,
				"KUBE_CONTEXT=preproduction",
				"ALLOW_COLD_PHYSICAL_SNAPSHOT=true",
				"VOLUME_SNAPSHOT_CLASS=retained",
				"EXPECTED_KUBEBRAIN_STATEFULSET_UID=uid-kubebrain",
				"EXPECTED_TIDB_CLUSTER_UID=uid-tidb",
				"EXPECTED_TIKV_CLUSTER_ID=7662961163671170154",
				"FAKE_RESOURCES=" + tc.resources,
				"FAKE_POLICY=" + policy,
				"FAKE_TIDB_ID=" + tidbID,
				"FAKE_KUBEBRAIN_ID=" + kubebrainID,
				"FAKE_PVC_JSON=" + pvcJSON,
				"FAKE_PROVISIONER=" + provisioner,
				"FAKE_PV_DRIVER=" + pvDriver,
			}
			output, err := runColdSnapshotPreflight(t, env)
			if tc.wantOK {
				require.NoError(t, err, string(output))
				var manifest map[string]any
				require.NoError(t, json.Unmarshal(output, &manifest))
				require.Equal(t, "kubebrain.cold-physical-snapshot-preflight.v2", manifest["format"])
				firstPD := manifest["pd_pvcs"].([]any)[0].(map[string]any)
				require.Equal(t, "uid-pv-pd-kb-pd-0", firstPD["pv_uid"])
				require.Equal(t, "csi.example.test", firstPD["csi_driver"])
				require.Equal(t, "handle-pv-pd-kb-pd-0", firstPD["volume_handle"])
			} else {
				require.Error(t, err, string(output))
				require.Contains(t, string(output), tc.wantOutput)
			}
		})
	}
}

func TestProductionReadinessColdSnapshotExamplesRequireExplicitContext(t *testing.T) {
	data, err := os.ReadFile(filepath.Join("..", "..", "docs", "production_readiness_cn.md"))
	require.NoError(t, err)
	doc := string(data)

	for _, command := range []string{
		"hack/backup/cold-snapshot-preflight.sh",
		"hack/backup/cold-snapshot-execute.sh",
	} {
		end := strings.Index(doc, command)
		require.NotEqual(t, -1, end, "cold snapshot command is missing")
		start := strings.LastIndex(doc[:end], "```shell")
		require.NotEqual(t, -1, start, "cold snapshot shell example is missing")
		require.Contains(t, doc[start:end], "KUBE_CONTEXT=")
		require.Contains(t, doc[start:end], "ALLOW_COLD_PHYSICAL_SNAPSHOT=true")
	}
	require.Contains(t, doc, "当前 kubectl context 永不作为默认值接受")
	require.Contains(t, doc, "不能只依赖 preflight 阶段的批准")
	require.Contains(t, doc, "固定 PV UID、CSI driver 与不可变 volumeHandle")
	require.Contains(t, doc, "创建第一个 retained VolumeSnapshot 前再次复核这些物理身份")
	require.Contains(t, doc, "实际观察值作为 `source_volume_handle` 写入 snapshot receipt 条目")
	require.Contains(t, doc, "最终 CSI snapshotHandle 均须一一唯一")
	require.Contains(t, doc, "receipt 原子发布前，executor 会再次读取全部 retained")
	require.Contains(t, doc, "capture completion")
	require.Contains(t, doc, "拒绝晚于当前控制面时钟")
	require.Contains(t, doc, "`witness.created_at_unix <= snapshot.created_at`")
	require.Contains(t, doc, "`operation_id-source_pvc`")
}

func TestColdSnapshotPreflightRequiresExplicitApproval(t *testing.T) {
	env := []string{"ALLOW_COLD_PHYSICAL_SNAPSHOT=false"}
	output, err := runColdSnapshotPreflight(t, env)
	require.Error(t, err)
	require.Contains(t, string(output), "ALLOW_COLD_PHYSICAL_SNAPSHOT=true")
}

func TestColdSnapshotPreflightRequiresExplicitContext(t *testing.T) {
	env := []string{
		"ALLOW_COLD_PHYSICAL_SNAPSHOT=true",
		"VOLUME_SNAPSHOT_CLASS=retained",
		"EXPECTED_KUBEBRAIN_STATEFULSET_UID=uid-kubebrain",
		"EXPECTED_TIDB_CLUSTER_UID=uid-tidb",
		"EXPECTED_TIKV_CLUSTER_ID=7662961163671170154",
		"KUBECTL=/does/not/exist",
	}
	output, err := runColdSnapshotPreflight(t, env)
	require.Error(t, err)
	require.Contains(t, string(output), "KUBE_CONTEXT is required; the current context is never accepted implicitly")
}

func runColdSnapshotPreflight(t *testing.T, env []string) ([]byte, error) {
	t.Helper()
	return runProductionScriptCommand(t, "../backup/cold-snapshot-preflight.sh", env)
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
