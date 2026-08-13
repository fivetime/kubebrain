package nativepitr

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func replacementManifest(t *testing.T) ReplacementTidbClusterManifest {
	t.Helper()
	raw := `{"apiVersion":"pingcap.com/v1alpha1","kind":"TidbCluster","metadata":{"name":"kb","namespace":"tidb-cluster","labels":{"app.kubernetes.io/name":"tidb-cluster","app.kubernetes.io/instance":"kb"},"annotations":{"dbaas.kubebrain.io/native-pitr-provision-authorization":"provision-1"}},"spec":{"version":"v8.5.3","pvReclaimPolicy":"Retain","pd":{"replicas":1,"requests":{"storage":"10Gi"}},"tikv":{"replicas":1,"requests":{"storage":"20Gi"}}}}`
	m, err := DecodeReplacementTidbClusterManifest(strings.NewReader(raw))
	require.NoError(t, err)
	return m
}
func TestTargetProvisionAuthorizationAndCreationBindDistinctUID(t *testing.T) {
	old := provisioningReceipt(42, "old")
	target := validTarget()
	target.ClusterID = 42
	retirement := retirementReceipt(old, target)
	manifest := replacementManifest(t)
	manifestSHA := strings.Repeat("5", 64)
	a, err := BuildTargetProvisionAuthorization(retirement, old, digest, digest, manifestSHA, "operation-1", manifest, 10)
	require.NoError(t, err)
	require.Equal(t, "provision-1", a.ManifestAuthorizationID)
	authSHA, _, err := DigestCanonicalJSON(a)
	require.NoError(t, err)
	creation := TargetProvisionCreationReceipt{Format: TargetProvisionCreationFormat, AuthorizationSHA256: authSHA, ManifestSHA256: manifestSHA, Namespace: a.Namespace, TidbCluster: a.TidbCluster, TidbClusterUID: "tc-new", ResourceVersion: "12", AdmittedIdentitySHA256: strings.Repeat("6", 64), CreatedAtUnix: 11, ServerDryRunPassed: true}
	_, err = BuildTargetProvisionCreationReceipt(a, authSHA, creation)
	require.NoError(t, err)
	creation.TidbClusterUID = old.TidbClusterUID
	_, err = BuildTargetProvisionCreationReceipt(a, authSHA, creation)
	require.Error(t, err)
}

func TestCurrentTargetProvisioningObjectBindsAdmittedIdentity(t *testing.T) {
	objectJSON := `{"apiVersion":"pingcap.com/v1alpha1","kind":"TidbCluster","metadata":{"name":"kb","namespace":"tidb-cluster","uid":"tc-new","resourceVersion":"12","labels":{"app.kubernetes.io/name":"tidb-cluster","app.kubernetes.io/instance":"kb"},"annotations":{"dbaas.kubebrain.io/native-pitr-provision-authorization":"provision-1"}},"spec":{"tikv":{"replicas":1},"pd":{"replicas":1},"defaulted":true},"status":{"ready":true}}`
	object, err := DecodeAdmittedTidbCluster(strings.NewReader(objectJSON))
	require.NoError(t, err)
	identitySHA, err := AdmittedTidbClusterIdentitySHA256(object)
	require.NoError(t, err)

	a := TargetProvisionAuthorization{Format: TargetProvisionAuthorizationFormat, RetirementReceiptSHA256: digest, OldProvisioningSHA256: digest, ManifestSHA256: digest, Namespace: "tidb-cluster", TidbCluster: "kb", OldTidbClusterUID: "tc-old", OldClusterID: 42, PDReplicas: 1, TiKVReplicas: 1, AuthorizationID: "operation-1", ManifestAuthorizationID: "provision-1", AuthorizedAtUnix: 10}
	creation := TargetProvisionCreationReceipt{Format: TargetProvisionCreationFormat, AuthorizationSHA256: digest, ManifestSHA256: digest, Namespace: "tidb-cluster", TidbCluster: "kb", TidbClusterUID: "tc-new", ResourceVersion: "12", AdmittedIdentitySHA256: identitySHA, CreatedAtUnix: 11, ServerDryRunPassed: true}
	require.NoError(t, VerifyCurrentTargetProvisioningObject(a, creation, object, digest))

	object.Spec.(map[string]any)["defaulted"] = false
	require.ErrorContains(t, VerifyCurrentTargetProvisioningObject(a, creation, object, digest), "does not match")
}

func TestAuthorizedReplacementProvisioningRejectsRetiredStorageReuse(t *testing.T) {
	old := provisioningReceipt(42, "old")
	newReceipt := provisioningReceipt(43, "new")
	newReceipt.ObservedAtUnix = 12
	a := TargetProvisionAuthorization{Format: TargetProvisionAuthorizationFormat, RetirementReceiptSHA256: digest, OldProvisioningSHA256: digest, ManifestSHA256: digest, Namespace: old.Namespace, TidbCluster: old.TidbCluster, OldTidbClusterUID: old.TidbClusterUID, OldClusterID: old.ClusterID, PDReplicas: 1, TiKVReplicas: 1, AuthorizationID: "operation-1", ManifestAuthorizationID: "provision-1", AuthorizedAtUnix: 10}
	creation := TargetProvisionCreationReceipt{Format: TargetProvisionCreationFormat, AuthorizationSHA256: digest, ManifestSHA256: digest, Namespace: old.Namespace, TidbCluster: old.TidbCluster, TidbClusterUID: newReceipt.TidbClusterUID, ResourceVersion: "12", AdmittedIdentitySHA256: digest, CreatedAtUnix: 11, ServerDryRunPassed: true}
	require.NoError(t, VerifyAuthorizedReplacementProvisioning(a, creation, old, newReceipt, digest))
	newReceipt.Volumes[0].VolumeHandle = old.Volumes[0].VolumeHandle
	require.ErrorContains(t, VerifyAuthorizedReplacementProvisioning(a, creation, old, newReceipt, digest), "reuses retired storage")
}
func TestReplacementManifestRejectsRestoreAndExtraMetadata(t *testing.T) {
	for _, raw := range []string{`{"apiVersion":"pingcap.com/v1alpha1","kind":"TidbCluster","metadata":{"name":"kb","namespace":"tidb-cluster","labels":{"app.kubernetes.io/name":"tidb-cluster","app.kubernetes.io/instance":"kb"},"annotations":{"dbaas.kubebrain.io/native-pitr-provision-authorization":"p"},"uid":"bad"},"spec":{}}`, `{"apiVersion":"pingcap.com/v1alpha1","kind":"TidbCluster","metadata":{"name":"kb","namespace":"tidb-cluster","labels":{"app.kubernetes.io/name":"tidb-cluster","app.kubernetes.io/instance":"kb"},"annotations":{"dbaas.kubebrain.io/native-pitr-provision-authorization":"p"}},"spec":{"pvReclaimPolicy":"Retain","initializer":{},"pd":{"replicas":1,"requests":{"storage":"1Gi"}},"tikv":{"replicas":1,"requests":{"storage":"1Gi"}}}}`} {
		_, err := DecodeReplacementTidbClusterManifest(strings.NewReader(raw))
		require.Error(t, err)
	}
}
