package nativepitr

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func provisioningReceipt(clusterID uint64, suffix string) TargetProvisioningReceipt {
	return TargetProvisioningReceipt{Format: TargetProvisioningReceiptFormat, Namespace: "tidb-cluster", TidbCluster: "kb", TidbClusterUID: "tc-" + suffix, ClusterID: clusterID, PDReplicas: 1, TiKVReplicas: 1, Ready: true, ObservedAtUnix: 10, ReadOnlyInspection: true, Volumes: []TargetVolumeIdentity{
		{Component: "pd", PVCName: "pd-kb-pd-0", PVCUID: "pvc-pd-" + suffix, PVName: "pv-pd-" + suffix, PVUID: "pvuid-pd-" + suffix, CSIDriver: "csi.test", VolumeHandle: "disk-pd-" + suffix},
		{Component: "tikv", PVCName: "tikv-kb-tikv-0", PVCUID: "pvc-tikv-" + suffix, PVName: "pv-tikv-" + suffix, PVUID: "pvuid-tikv-" + suffix, CSIDriver: "csi.test", VolumeHandle: "disk-tikv-" + suffix},
	}}
}

func TestTargetProvisioningReceiptStrictAndReplacementDisjoint(t *testing.T) {
	oldTarget, newTarget := validTarget(), validTarget()
	newTarget.ClusterID++
	oldTarget.CheckedAtUnix, newTarget.CheckedAtUnix = 11, 11
	oldReceipt, newReceipt := provisioningReceipt(oldTarget.ClusterID, "old"), provisioningReceipt(newTarget.ClusterID, "new")
	require.NoError(t, VerifyReplacementProvisioning(oldReceipt, newReceipt, oldTarget, newTarget))
	b, err := json.Marshal(newReceipt)
	require.NoError(t, err)
	_, err = DecodeTargetProvisioningReceipt(strings.NewReader(string(b)))
	require.NoError(t, err)
	_, err = DecodeTargetProvisioningReceipt(strings.NewReader(string(b) + `{}`))
	require.ErrorContains(t, err, "trailing")
}

func TestReplacementProvisioningRejectsOldStorageReuse(t *testing.T) {
	oldTarget, newTarget := validTarget(), validTarget()
	newTarget.ClusterID++
	oldTarget.CheckedAtUnix, newTarget.CheckedAtUnix = 11, 11
	oldReceipt, base := provisioningReceipt(oldTarget.ClusterID, "old"), provisioningReceipt(newTarget.ClusterID, "new")
	for _, mutate := range []func(*TargetProvisioningReceipt){
		func(r *TargetProvisioningReceipt) { r.TidbClusterUID = oldReceipt.TidbClusterUID },
		func(r *TargetProvisioningReceipt) { r.Volumes[0].PVCUID = oldReceipt.Volumes[0].PVCUID },
		func(r *TargetProvisioningReceipt) { r.Volumes[0].PVUID = oldReceipt.Volumes[0].PVUID },
		func(r *TargetProvisioningReceipt) { r.Volumes[0].VolumeHandle = oldReceipt.Volumes[0].VolumeHandle },
	} {
		newReceipt := base
		newReceipt.Volumes = append([]TargetVolumeIdentity(nil), base.Volumes...)
		mutate(&newReceipt)
		require.Error(t, VerifyReplacementProvisioning(oldReceipt, newReceipt, oldTarget, newTarget))
	}
}
