package nativepitr

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
)

const TargetRetirementReceiptFormat = "kubebrain.native-pitr-target-retirement.v1"

type RetiredTargetVolume struct {
	TargetVolumeIdentity
	PVPhase               string `json:"pv_phase"`
	PVCUIDAbsent          bool   `json:"pvc_uid_absent"`
	PVUIDAbsent           bool   `json:"pv_uid_absent"`
	VolumeAttachmentCount int    `json:"volume_attachment_count"`
}

type TargetRetirementReceipt struct {
	Format                       string                `json:"format"`
	OldTargetProvisioningSHA256  string                `json:"old_target_provisioning_receipt_sha256"`
	OldRestoreAdmissionSHA256    string                `json:"old_restore_admission_receipt_sha256"`
	Namespace                    string                `json:"namespace"`
	TidbCluster                  string                `json:"tidb_cluster"`
	OldTidbClusterUID            string                `json:"old_tidb_cluster_uid"`
	OldClusterID                 uint64                `json:"old_cluster_id"`
	Volumes                      []RetiredTargetVolume `json:"volumes"`
	OldPDAddrs                   []string              `json:"old_pd_addrs"`
	EndpointProbeCount           int                   `json:"endpoint_probe_count"`
	EndpointProbeIntervalSeconds int                   `json:"endpoint_probe_interval_seconds"`
	OldTidbClusterUIDAbsent      bool                  `json:"old_tidb_cluster_uid_absent"`
	AllOldPVCUIDsAbsent          bool                  `json:"all_old_pvc_uids_absent"`
	AllOldVolumesDetached        bool                  `json:"all_old_volumes_detached"`
	AllOldPDEndpointsUnreachable bool                  `json:"all_old_pd_endpoints_unreachable"`
	AdmissionFenceHeld           bool                  `json:"admission_fence_held"`
	FirstProbedAtUnix            int64                 `json:"first_probed_at_unix"`
	CompletedAtUnix              int64                 `json:"completed_at_unix"`
	ReadOnlyInspection           bool                  `json:"read_only_inspection"`
}

func (r TargetRetirementReceipt) Validate() error {
	if r.Format != TargetRetirementReceiptFormat || !sha256RE.MatchString(r.OldTargetProvisioningSHA256) || !sha256RE.MatchString(r.OldRestoreAdmissionSHA256) || !dnsLabel.MatchString(r.Namespace) || !dnsLabel.MatchString(r.TidbCluster) || !kubernetesUIDRE.MatchString(r.OldTidbClusterUID) || r.OldClusterID == 0 || len(r.Volumes) == 0 || len(r.OldPDAddrs) == 0 || r.EndpointProbeCount < 3 || r.EndpointProbeIntervalSeconds < 1 || !r.OldTidbClusterUIDAbsent || !r.AllOldPVCUIDsAbsent || !r.AllOldVolumesDetached || !r.AllOldPDEndpointsUnreachable || !r.AdmissionFenceHeld || r.FirstProbedAtUnix <= 0 || r.CompletedAtUnix < r.FirstProbedAtUnix+int64((r.EndpointProbeCount-1)*r.EndpointProbeIntervalSeconds) || !r.ReadOnlyInspection {
		return errors.New("invalid native PITR target retirement receipt")
	}
	for i, addr := range r.OldPDAddrs {
		if !validTargetEndpoint(addr) || (i > 0 && r.OldPDAddrs[i-1] >= addr) {
			return errors.New("target retirement PD endpoints are invalid or unsorted")
		}
	}
	previous := ""
	for _, volume := range r.Volumes {
		if volume.Component != "pd" && volume.Component != "tikv" {
			return errors.New("target retirement volume component is invalid")
		}
		order := volume.Component + "\x00" + volume.PVCName
		if previous >= order {
			return errors.New("target retirement volumes are not uniquely sorted")
		}
		previous = order
		pvRetired := (volume.PVPhase == "Released" || volume.PVPhase == "Failed") && !volume.PVUIDAbsent
		pvDeleted := volume.PVPhase == "Absent" && volume.PVUIDAbsent
		if !validTargetVolumeIdentity(volume.TargetVolumeIdentity) || !volume.PVCUIDAbsent || volume.VolumeAttachmentCount != 0 || (!pvRetired && !pvDeleted) {
			return errors.New("target retirement volume is not released and detached")
		}
	}
	return nil
}

func VerifyTargetRetirementBinding(retirement TargetRetirementReceipt, oldProvisioning TargetProvisioningReceipt, oldProvisioningSHA string, oldTarget TargetSnapshotEmptyReceipt, oldAdmission RestoreAdmissionReceipt, oldAdmissionSHA string) error {
	if err := retirement.Validate(); err != nil {
		return err
	}
	if err := oldProvisioning.Validate(); err != nil {
		return err
	}
	if err := oldTarget.Validate(); err != nil {
		return err
	}
	if err := oldAdmission.Validate(); err != nil {
		return err
	}
	if retirement.OldTargetProvisioningSHA256 != oldProvisioningSHA || retirement.OldRestoreAdmissionSHA256 != oldAdmissionSHA || oldAdmission.TargetClusterID != oldTarget.ClusterID || retirement.Namespace != oldProvisioning.Namespace || retirement.TidbCluster != oldProvisioning.TidbCluster || retirement.OldTidbClusterUID != oldProvisioning.TidbClusterUID || retirement.OldClusterID != oldProvisioning.ClusterID || retirement.OldClusterID != oldTarget.ClusterID || !equalStrings(retirement.OldPDAddrs, oldTarget.PDAddrs) || len(retirement.Volumes) != len(oldProvisioning.Volumes) {
		return errors.New("target retirement receipt does not bind the exact old target")
	}
	for i := range retirement.Volumes {
		if retirement.Volumes[i].TargetVolumeIdentity != oldProvisioning.Volumes[i] {
			return fmt.Errorf("target retirement volume %d does not match old provisioning", i)
		}
	}
	return nil
}

func DecodeTargetRetirementReceipt(reader io.Reader) (TargetRetirementReceipt, error) {
	var receipt TargetRetirementReceipt
	dec := json.NewDecoder(reader)
	dec.DisallowUnknownFields()
	if err := dec.Decode(&receipt); err != nil {
		return receipt, fmt.Errorf("decode target retirement receipt: %w", err)
	}
	if err := requireEOF(dec); err != nil {
		return receipt, errors.New("target retirement receipt contains trailing JSON")
	}
	return receipt, receipt.Validate()
}
