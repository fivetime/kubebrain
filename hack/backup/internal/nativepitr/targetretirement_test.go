package nativepitr

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func retirementReceipt(provisioning TargetProvisioningReceipt, target TargetSnapshotEmptyReceipt) TargetRetirementReceipt {
	volumes := make([]RetiredTargetVolume, len(provisioning.Volumes))
	for i, volume := range provisioning.Volumes {
		volumes[i] = RetiredTargetVolume{TargetVolumeIdentity: volume, PVPhase: "Released", PVCUIDAbsent: true}
	}
	return TargetRetirementReceipt{Format: TargetRetirementReceiptFormat, OldTargetProvisioningSHA256: digest, OldRestoreAdmissionSHA256: digest, Namespace: provisioning.Namespace, TidbCluster: provisioning.TidbCluster, OldTidbClusterUID: provisioning.TidbClusterUID, OldClusterID: provisioning.ClusterID, Volumes: volumes, OldPDAddrs: append([]string(nil), target.PDAddrs...), EndpointProbeCount: 3, EndpointProbeIntervalSeconds: 2, OldTidbClusterUIDAbsent: true, AllOldPVCUIDsAbsent: true, AllOldVolumesDetached: true, AllOldPDEndpointsUnreachable: true, AdmissionFenceHeld: true, FirstProbedAtUnix: 10, CompletedAtUnix: 14, ReadOnlyInspection: true}
}

func TestTargetRetirementReceiptBindsReleasedOldStorage(t *testing.T) {
	target := validTarget()
	provisioning := provisioningReceipt(target.ClusterID, "old")
	admission, _, err := BuildRestoreAdmissionReceipt(validReceiptPlan(t), digest, "old-restore", 1, false)
	require.NoError(t, err)
	receipt := retirementReceipt(provisioning, target)
	require.NoError(t, VerifyTargetRetirementBinding(receipt, provisioning, digest, target, admission, digest))
	b, err := json.Marshal(receipt)
	require.NoError(t, err)
	_, err = DecodeTargetRetirementReceipt(strings.NewReader(string(b)))
	require.NoError(t, err)
	_, err = DecodeTargetRetirementReceipt(strings.NewReader(string(b) + `{}`))
	require.ErrorContains(t, err, "trailing")
}

func TestTargetRetirementReceiptRejectsWritableOldTarget(t *testing.T) {
	target := validTarget()
	provisioning := provisioningReceipt(target.ClusterID, "old")
	base := retirementReceipt(provisioning, target)
	for _, mutate := range []func(*TargetRetirementReceipt){
		func(r *TargetRetirementReceipt) { r.OldTidbClusterUIDAbsent = false },
		func(r *TargetRetirementReceipt) { r.Volumes[0].PVPhase = "Bound" },
		func(r *TargetRetirementReceipt) { r.Volumes[0].PVUIDAbsent = true },
		func(r *TargetRetirementReceipt) { r.Volumes[0].VolumeAttachmentCount = 1 },
		func(r *TargetRetirementReceipt) { r.AllOldPDEndpointsUnreachable = false },
		func(r *TargetRetirementReceipt) { r.AdmissionFenceHeld = false },
		func(r *TargetRetirementReceipt) { r.CompletedAtUnix-- },
	} {
		receipt := base
		receipt.Volumes = append([]RetiredTargetVolume(nil), base.Volumes...)
		mutate(&receipt)
		require.Error(t, receipt.Validate())
	}
}

func TestTargetRetirementReceiptAcceptsDeletedOldPVIdentity(t *testing.T) {
	target := validTarget()
	provisioning := provisioningReceipt(target.ClusterID, "old")
	receipt := retirementReceipt(provisioning, target)
	for i := range receipt.Volumes {
		receipt.Volumes[i].PVPhase = "Absent"
		receipt.Volumes[i].PVUIDAbsent = true
	}
	require.NoError(t, receipt.Validate())
}
