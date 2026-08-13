package nativepitr

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestTargetQualificationBindsProvisioningAndWholeTransactionalScan(t *testing.T) {
	provisioning := provisioningReceipt(42, "new")
	provisioning.ObservedAtUnix = 10
	target := validTarget()
	target.ClusterID = provisioning.ClusterID
	target.PDAddrs = []string{"kb-pd-0.kb-pd-peer.tidb-cluster.svc:2379"}
	target.Stores = target.Stores[:1]
	target.CheckedAtUnix = 11
	writers := TargetWriterExclusionEvidence{Namespace: "kubebrain-system", StatefulSet: "kubebrain", StatefulSetUID: "kb-uid", ResourceVersion: "12", ObservedAtUnix: 11, ReadOnlyInspection: true}
	r, err := BuildTargetQualificationReceipt(provisioning, target, writers, digest, digest, digest, target.PDAddrs, 12)
	require.NoError(t, err)
	require.NoError(t, VerifyTargetQualificationBinding(r, provisioning, target, writers, digest, digest, digest, target.PDAddrs))
	r.WriterExclusionSHA256 = "ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"
	require.ErrorContains(t, VerifyTargetQualificationBinding(r, provisioning, target, writers, digest, digest, digest, target.PDAddrs), "exact source evidence")
}

func TestTargetQualificationRejectsWrongEndpointsAndVisibleData(t *testing.T) {
	provisioning := provisioningReceipt(42, "new")
	target := validTarget()
	target.ClusterID = provisioning.ClusterID
	target.PDAddrs = []string{"pd-0:2379"}
	target.Stores = target.Stores[:1]
	target.CheckedAtUnix = 11
	writers := TargetWriterExclusionEvidence{Namespace: "kubebrain-system", StatefulSet: "kubebrain", StatefulSetUID: "kb-uid", ResourceVersion: "12", ObservedAtUnix: 11, ReadOnlyInspection: true}
	_, err := BuildTargetQualificationReceipt(provisioning, target, writers, digest, digest, digest, []string{"other:2379"}, 12)
	require.Error(t, err)
	target.VisibleCommittedKeyCount = 1
	_, err = BuildTargetQualificationReceipt(provisioning, target, writers, digest, digest, digest, target.PDAddrs, 12)
	require.Error(t, err)
	target.VisibleCommittedKeyCount = 0
	writers.ObservedPodCount = 1
	_, err = BuildTargetQualificationReceipt(provisioning, target, writers, digest, digest, digest, target.PDAddrs, 12)
	require.ErrorContains(t, err, "writer exclusion")
}

func TestTargetWriterExclusionContinuityRejectsScaleOrRevisionChange(t *testing.T) {
	recorded := TargetWriterExclusionEvidence{Namespace: "kubebrain-system", StatefulSet: "kubebrain", StatefulSetUID: "kb-uid", ResourceVersion: "12", ObservedAtUnix: 10, ReadOnlyInspection: true}
	current := recorded
	current.ObservedAtUnix = 11
	require.NoError(t, VerifyTargetWriterExclusionContinuity(recorded, current))
	current.ResourceVersion = "13"
	require.ErrorContains(t, VerifyTargetWriterExclusionContinuity(recorded, current), "changed")
	current = recorded
	current.DesiredReplicas = 1
	require.ErrorContains(t, VerifyTargetWriterExclusionContinuity(recorded, current), "writer exclusion")
}
