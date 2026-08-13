package nativepitr

import "errors"

const TargetRetirementAuthorizationFormat = "kubebrain.native-pitr-target-retirement-authorization.v1"

type TargetRetirementAuthorization struct {
	Format                       string                 `json:"format"`
	FailedOperationAuditSHA256   string                 `json:"failed_operation_audit_sha256"`
	FailedOperationParametersSHA string                 `json:"failed_operation_parameters_sha256"`
	OldPlanSHA256                string                 `json:"old_plan_sha256"`
	OldTargetSHA256              string                 `json:"old_target_snapshot_empty_sha256"`
	OldProvisioningSHA256        string                 `json:"old_target_provisioning_sha256"`
	OldAdmissionSHA256           string                 `json:"old_restore_admission_sha256"`
	Namespace                    string                 `json:"namespace"`
	TidbCluster                  string                 `json:"tidb_cluster"`
	TidbClusterUID               string                 `json:"tidb_cluster_uid"`
	ClusterID                    uint64                 `json:"cluster_id"`
	Volumes                      []TargetVolumeIdentity `json:"volumes"`
	AuthorizedAtUnix             int64                  `json:"authorized_at_unix"`
}

func (a TargetRetirementAuthorization) Validate() error {
	if a.Format != TargetRetirementAuthorizationFormat || !sha256RE.MatchString(a.FailedOperationAuditSHA256) || !sha256RE.MatchString(a.FailedOperationParametersSHA) || !sha256RE.MatchString(a.OldPlanSHA256) || !sha256RE.MatchString(a.OldTargetSHA256) || !sha256RE.MatchString(a.OldProvisioningSHA256) || !sha256RE.MatchString(a.OldAdmissionSHA256) || !dnsLabel.MatchString(a.Namespace) || !dnsLabel.MatchString(a.TidbCluster) || !kubernetesUIDRE.MatchString(a.TidbClusterUID) || a.ClusterID == 0 || len(a.Volumes) == 0 || a.AuthorizedAtUnix <= 0 {
		return errors.New("invalid native PITR target retirement authorization")
	}
	previous := ""
	for _, volume := range a.Volumes {
		order := volume.Component + "\x00" + volume.PVCName
		if !validTargetVolumeIdentity(volume) || previous >= order {
			return errors.New("invalid or unsorted retirement authorization volumes")
		}
		previous = order
	}
	return nil
}

func BuildTargetRetirementAuthorization(plan Plan, target TargetSnapshotEmptyReceipt, provisioning TargetProvisioningReceipt, admission RestoreAdmissionReceipt, a TargetRetirementAuthorization) (TargetRetirementAuthorization, error) {
	if err := plan.Validate(); err != nil {
		return TargetRetirementAuthorization{}, err
	}
	if err := target.Validate(); err != nil {
		return TargetRetirementAuthorization{}, err
	}
	if err := provisioning.Validate(); err != nil {
		return TargetRetirementAuthorization{}, err
	}
	if err := admission.Validate(); err != nil {
		return TargetRetirementAuthorization{}, err
	}
	if plan.Target.ClusterID != target.ClusterID || target.ClusterID != provisioning.ClusterID || admission.TargetClusterID != target.ClusterID || admission.PlanSHA256 != a.OldPlanSHA256 || admission.Keyspace != plan.Source.Keyspace || provisioning.ObservedAtUnix > target.CheckedAtUnix {
		return TargetRetirementAuthorization{}, errors.New("retirement authorization evidence does not bind one failed restore target")
	}
	a.Format = TargetRetirementAuthorizationFormat
	a.Namespace = provisioning.Namespace
	a.TidbCluster = provisioning.TidbCluster
	a.TidbClusterUID = provisioning.TidbClusterUID
	a.ClusterID = provisioning.ClusterID
	a.Volumes = append([]TargetVolumeIdentity(nil), provisioning.Volumes...)
	return a, a.Validate()
}
