package nativepitr

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"regexp"
	"strings"
	"unicode"
)

const TargetProvisioningReceiptFormat = "kubebrain.native-pitr-target-provisioning.v1"

var kubernetesUIDRE = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._:-]{0,252}$`)
var kubernetesSubdomainRE = regexp.MustCompile(`^[a-z0-9]([-a-z0-9]*[a-z0-9])?(\.[a-z0-9]([-a-z0-9]*[a-z0-9])?)*$`)

type TargetVolumeIdentity struct {
	Component    string `json:"component"`
	PVCName      string `json:"pvc_name"`
	PVCUID       string `json:"pvc_uid"`
	PVName       string `json:"pv_name"`
	PVUID        string `json:"pv_uid"`
	CSIDriver    string `json:"csi_driver"`
	VolumeHandle string `json:"volume_handle"`
}

type TargetProvisioningReceipt struct {
	Format             string                 `json:"format"`
	Namespace          string                 `json:"namespace"`
	TidbCluster        string                 `json:"tidb_cluster"`
	TidbClusterUID     string                 `json:"tidb_cluster_uid"`
	ClusterID          uint64                 `json:"cluster_id"`
	PDReplicas         int                    `json:"pd_replicas"`
	TiKVReplicas       int                    `json:"tikv_replicas"`
	Volumes            []TargetVolumeIdentity `json:"volumes"`
	Ready              bool                   `json:"ready"`
	ObservedAtUnix     int64                  `json:"observed_at_unix"`
	ReadOnlyInspection bool                   `json:"read_only_inspection"`
}

func (r TargetProvisioningReceipt) Validate() error {
	if r.Format != TargetProvisioningReceiptFormat || !dnsLabel.MatchString(r.Namespace) || !dnsLabel.MatchString(r.TidbCluster) || !kubernetesUIDRE.MatchString(r.TidbClusterUID) || r.ClusterID == 0 || r.PDReplicas <= 0 || r.TiKVReplicas <= 0 || len(r.Volumes) != r.PDReplicas+r.TiKVReplicas || !r.Ready || r.ObservedAtUnix <= 0 || !r.ReadOnlyInspection {
		return errors.New("invalid native PITR target provisioning receipt")
	}
	pd, tikv := 0, 0
	pvcUIDs, pvUIDs, storageIDs := map[string]bool{}, map[string]bool{}, map[string]bool{}
	previous := ""
	for _, volume := range r.Volumes {
		if volume.Component == "pd" {
			pd++
		} else if volume.Component == "tikv" {
			tikv++
		} else {
			return errors.New("target provisioning volume has invalid component")
		}
		if !validTargetVolumeIdentity(volume) {
			return errors.New("target provisioning volume identity is incomplete")
		}
		order := volume.Component + "\x00" + volume.PVCName
		if previous >= order {
			return errors.New("target provisioning volumes are not uniquely sorted")
		}
		previous = order
		storageID := volume.CSIDriver + "\x00" + volume.VolumeHandle
		if pvcUIDs[volume.PVCUID] || pvUIDs[volume.PVUID] || storageIDs[storageID] {
			return errors.New("target provisioning receipt reuses a volume identity")
		}
		pvcUIDs[volume.PVCUID], pvUIDs[volume.PVUID], storageIDs[storageID] = true, true, true
	}
	if pd != r.PDReplicas || tikv != r.TiKVReplicas {
		return errors.New("target provisioning replica and volume counts differ")
	}
	return nil
}

func validTargetVolumeIdentity(volume TargetVolumeIdentity) bool {
	return (volume.Component == "pd" || volume.Component == "tikv") && len(volume.PVCName) <= 253 && len(volume.PVName) <= 253 && kubernetesSubdomainRE.MatchString(volume.PVCName) && kubernetesSubdomainRE.MatchString(volume.PVName) && kubernetesUIDRE.MatchString(volume.PVCUID) && kubernetesUIDRE.MatchString(volume.PVUID) && validProvisioningIdentity(volume.CSIDriver) && validProvisioningIdentity(volume.VolumeHandle)
}

func validProvisioningIdentity(value string) bool {
	return len(value) > 0 && len(value) <= 1024 && strings.IndexFunc(value, func(r rune) bool { return unicode.IsSpace(r) || unicode.IsControl(r) }) < 0
}

func VerifyReplacementProvisioning(oldReceipt, newReceipt TargetProvisioningReceipt, oldTarget, newTarget TargetSnapshotEmptyReceipt) error {
	if err := oldReceipt.Validate(); err != nil {
		return fmt.Errorf("old target provisioning: %w", err)
	}
	if err := newReceipt.Validate(); err != nil {
		return fmt.Errorf("new target provisioning: %w", err)
	}
	if oldReceipt.Namespace != newReceipt.Namespace || oldReceipt.TidbCluster != newReceipt.TidbCluster || oldReceipt.ClusterID != oldTarget.ClusterID || newReceipt.ClusterID != newTarget.ClusterID || oldReceipt.ClusterID == newReceipt.ClusterID || oldReceipt.TidbClusterUID == newReceipt.TidbClusterUID {
		return errors.New("target replacement provisioning does not bind distinct exact clusters")
	}
	if oldReceipt.ObservedAtUnix > oldTarget.CheckedAtUnix || newReceipt.ObservedAtUnix > newTarget.CheckedAtUnix || oldReceipt.PDReplicas != len(oldTarget.PDAddrs) || newReceipt.PDReplicas != len(newTarget.PDAddrs) || oldReceipt.TiKVReplicas != len(oldTarget.Stores) || newReceipt.TiKVReplicas != len(newTarget.Stores) {
		return errors.New("target provisioning topology or observation time does not match target-empty evidence")
	}
	oldPVC, oldPV, oldStorage := map[string]bool{}, map[string]bool{}, map[string]bool{}
	for _, volume := range oldReceipt.Volumes {
		oldPVC[volume.PVCUID], oldPV[volume.PVUID], oldStorage[volume.CSIDriver+"\x00"+volume.VolumeHandle] = true, true, true
	}
	for _, volume := range newReceipt.Volumes {
		switch {
		case oldPVC[volume.PVCUID]:
			return fmt.Errorf("replacement target reuses old PVC UID %s", volume.PVCUID)
		case oldPV[volume.PVUID]:
			return fmt.Errorf("replacement target reuses old PV UID %s", volume.PVUID)
		case oldStorage[volume.CSIDriver+"\x00"+volume.VolumeHandle]:
			return fmt.Errorf("replacement target reuses old CSI volume %s/%s", volume.CSIDriver, volume.VolumeHandle)
		}
	}
	return nil
}

func DecodeTargetProvisioningReceipt(reader io.Reader) (TargetProvisioningReceipt, error) {
	var receipt TargetProvisioningReceipt
	dec := json.NewDecoder(reader)
	dec.DisallowUnknownFields()
	if err := dec.Decode(&receipt); err != nil {
		return receipt, fmt.Errorf("decode target provisioning receipt: %w", err)
	}
	if err := requireEOF(dec); err != nil {
		return receipt, errors.New("target provisioning receipt contains trailing JSON")
	}
	return receipt, receipt.Validate()
}
