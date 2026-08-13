package nativepitr

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestTargetRetirementAuthorizationBindsFailedTargetIdentity(t *testing.T) {
	plan := validReceiptPlan(t)
	target := validTarget()
	provisioning := provisioningReceipt(target.ClusterID, "old")
	admission, _, err := BuildRestoreAdmissionReceipt(plan, digest, "failed-restore", 1, false)
	require.NoError(t, err)
	a, err := BuildTargetRetirementAuthorization(plan, target, provisioning, admission, TargetRetirementAuthorization{FailedOperationAuditSHA256: digest, FailedOperationParametersSHA: strings.Repeat("1", 64), OldPlanSHA256: digest, OldTargetSHA256: strings.Repeat("2", 64), OldProvisioningSHA256: strings.Repeat("3", 64), OldAdmissionSHA256: strings.Repeat("4", 64), AuthorizedAtUnix: 10})
	require.NoError(t, err)
	require.Equal(t, provisioning.TidbClusterUID, a.TidbClusterUID)
	require.Equal(t, provisioning.Volumes, a.Volumes)
	admission.TargetClusterID++
	_, err = BuildTargetRetirementAuthorization(plan, target, provisioning, admission, a)
	require.Error(t, err)
}
