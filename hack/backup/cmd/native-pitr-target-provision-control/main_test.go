package main

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/kubewharf/kubebrain/hack/backup/internal/nativepitr"
	"github.com/stretchr/testify/require"
)

func TestDurableDryRunRecoversExactCreatedObject(t *testing.T) {
	dir := t.TempDir()
	a := nativepitr.TargetProvisionAuthorization{
		Format: nativepitr.TargetProvisionAuthorizationFormat, RetirementReceiptSHA256: strings.Repeat("1", 64),
		OldProvisioningSHA256: strings.Repeat("2", 64), ManifestSHA256: strings.Repeat("3", 64),
		Namespace: "tidb-cluster", TidbCluster: "kb", OldTidbClusterUID: "old-uid", OldClusterID: 42,
		PDReplicas: 1, TiKVReplicas: 1, AuthorizationID: "operation-1", ManifestAuthorizationID: "manifest-nonce", AuthorizedAtUnix: 10,
	}
	_, authData, err := nativepitr.DigestCanonicalJSON(a)
	require.NoError(t, err)
	authPath := filepath.Join(dir, "authorization.json")
	require.NoError(t, os.WriteFile(authPath, authData, 0o600))
	dryRunObject := filepath.Join(dir, "dry-run-object.json")
	createdObject := filepath.Join(dir, "created-object.json")
	base := `{"apiVersion":"pingcap.com/v1alpha1","kind":"TidbCluster","metadata":{"name":"kb","namespace":"tidb-cluster","labels":{"app.kubernetes.io/name":"tidb-cluster","app.kubernetes.io/instance":"kb"},"annotations":{"dbaas.kubebrain.io/native-pitr-provision-authorization":"manifest-nonce"}},"spec":{"pvReclaimPolicy":"Retain","pd":{"replicas":1},"tikv":{"replicas":1}}}`
	created := `{"apiVersion":"pingcap.com/v1alpha1","kind":"TidbCluster","metadata":{"name":"kb","namespace":"tidb-cluster","uid":"new-uid","resourceVersion":"12","labels":{"app.kubernetes.io/name":"tidb-cluster","app.kubernetes.io/instance":"kb"},"annotations":{"dbaas.kubebrain.io/native-pitr-provision-authorization":"manifest-nonce"}},"spec":{"pvReclaimPolicy":"Retain","pd":{"replicas":1},"tikv":{"replicas":1}}}`
	require.NoError(t, os.WriteFile(dryRunObject, []byte(base), 0o600))
	require.NoError(t, os.WriteFile(createdObject, []byte(created), 0o600))
	dryRunReceipt := filepath.Join(dir, "dry-run.json")
	require.NoError(t, run(options{mode: "record-dry-run", authorization: authPath, dryRunObject: dryRunObject, output: dryRunReceipt}, 11))
	creationReceipt := filepath.Join(dir, "creation.json")
	require.NoError(t, run(options{mode: "record-creation", authorization: authPath, dryRunReceipt: dryRunReceipt, createdObject: createdObject, output: creationReceipt}, 12))
	data, err := os.ReadFile(creationReceipt)
	require.NoError(t, err)
	receipt, err := nativepitr.DecodeTargetProvisionCreationReceipt(bytes.NewReader(data))
	require.NoError(t, err)
	require.Equal(t, "new-uid", receipt.TidbClusterUID)
}

func TestDurableDryRunRejectsDriftedRecoveryObject(t *testing.T) {
	dir := t.TempDir()
	a := nativepitr.TargetProvisionAuthorization{Format: nativepitr.TargetProvisionAuthorizationFormat, RetirementReceiptSHA256: strings.Repeat("1", 64), OldProvisioningSHA256: strings.Repeat("2", 64), ManifestSHA256: strings.Repeat("3", 64), Namespace: "tidb-cluster", TidbCluster: "kb", OldTidbClusterUID: "old-uid", OldClusterID: 42, PDReplicas: 1, TiKVReplicas: 1, AuthorizationID: "operation-1", ManifestAuthorizationID: "manifest-nonce", AuthorizedAtUnix: 10}
	_, authData, err := nativepitr.DigestCanonicalJSON(a)
	require.NoError(t, err)
	authPath := filepath.Join(dir, "auth")
	require.NoError(t, os.WriteFile(authPath, authData, 0o600))
	dryObject := filepath.Join(dir, "dry")
	createdObject := filepath.Join(dir, "created")
	require.NoError(t, os.WriteFile(dryObject, []byte(`{"apiVersion":"pingcap.com/v1alpha1","kind":"TidbCluster","metadata":{"name":"kb","namespace":"tidb-cluster","labels":{},"annotations":{"dbaas.kubebrain.io/native-pitr-provision-authorization":"manifest-nonce"}},"spec":{"pd":{"replicas":1},"tikv":{"replicas":1}}}`), 0o600))
	require.NoError(t, os.WriteFile(createdObject, []byte(`{"apiVersion":"pingcap.com/v1alpha1","kind":"TidbCluster","metadata":{"name":"kb","namespace":"tidb-cluster","uid":"new-uid","resourceVersion":"12","labels":{},"annotations":{"dbaas.kubebrain.io/native-pitr-provision-authorization":"manifest-nonce"}},"spec":{"pd":{"replicas":1},"tikv":{"replicas":3}}}`), 0o600))
	dryReceipt := filepath.Join(dir, "dry-receipt")
	require.NoError(t, run(options{mode: "record-dry-run", authorization: authPath, dryRunObject: dryObject, output: dryReceipt}, 11))
	creation := filepath.Join(dir, "creation")
	require.ErrorContains(t, run(options{mode: "record-creation", authorization: authPath, dryRunReceipt: dryReceipt, createdObject: createdObject, output: creation}, 12), "differs")
	require.NoFileExists(t, creation)
}

func TestQualifyTargetBindsExactWriterEvidence(t *testing.T) {
	dir := t.TempDir()
	write := func(name string, value any) string {
		t.Helper()
		_, data, err := nativepitr.DigestCanonicalJSON(value)
		require.NoError(t, err)
		path := filepath.Join(dir, name)
		require.NoError(t, os.WriteFile(path, data, 0o600))
		return path
	}
	provisioning := nativepitr.TargetProvisioningReceipt{
		Format: nativepitr.TargetProvisioningReceiptFormat, Namespace: "tidb-cluster", TidbCluster: "kb", TidbClusterUID: "new-uid",
		ClusterID: 42, PDReplicas: 1, TiKVReplicas: 1, Ready: true, ObservedAtUnix: 10, ReadOnlyInspection: true,
		Volumes: []nativepitr.TargetVolumeIdentity{
			{Component: "pd", PVCName: "pd-kb-pd-0", PVCUID: "pvc-pd", PVName: "pv-pd", PVUID: "pvuid-pd", CSIDriver: "csi.example", VolumeHandle: "pd-volume"},
			{Component: "tikv", PVCName: "tikv-kb-tikv-0", PVCUID: "pvc-tikv", PVName: "pv-tikv", PVUID: "pvuid-tikv", CSIDriver: "csi.example", VolumeHandle: "tikv-volume"},
		},
	}
	pdAddr := "kb-pd-0.kb-pd-peer.tidb-cluster.svc:2379"
	target := nativepitr.TargetSnapshotEmptyReceipt{Format: nativepitr.TargetSnapshotEmptyFormat, ClusterID: 42, PDAddrs: []string{pdAddr}, Stores: []nativepitr.TargetStore{{ID: 1, Address: "kb-tikv-0:20160"}}, SnapshotTS: 100, ScanScope: nativepitr.WholeTransactionalKeyspace, CheckedAtUnix: 11, ReadOnly: true}
	writers := nativepitr.TargetWriterExclusionEvidence{Namespace: "kubebrain-system", StatefulSet: "kubebrain", StatefulSetUID: "writer-uid", ResourceVersion: "12", ObservedAtUnix: 11, ReadOnlyInspection: true}
	provisioningPath := write("provisioning.json", provisioning)
	targetPath := write("target.json", target)
	writersPath := write("writers.json", writers)
	qualificationPath := filepath.Join(dir, "qualification.json")
	opts := options{mode: "qualify-target", newProvisioning: provisioningPath, targetEmpty: targetPath, writerExclusion: writersPath, expectedPDAddrs: pdAddr, output: qualificationPath}
	require.NoError(t, run(opts, 12))
	opts.mode, opts.createdObject, opts.output = "verify-qualification", qualificationPath, ""
	require.NoError(t, run(opts, 13))
	writers.ResourceVersion = "13"
	opts.writerExclusion = write("writers-drifted.json", writers)
	require.ErrorContains(t, run(opts, 13), "exact source evidence")
}

func TestReadRejectsOversizedEvidence(t *testing.T) {
	path := filepath.Join(t.TempDir(), "evidence.json")
	require.NoError(t, os.WriteFile(path, make([]byte, (8<<20)+1), 0o600))
	_, err := read(path)
	require.ErrorContains(t, err, "exceeds 8 MiB")
}

func TestWriteExclusiveDoesNotOverwriteOrLeaveTemporaryFiles(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "receipt.json")
	require.NoError(t, writeExclusive(path, []byte("first\n")))
	require.Error(t, writeExclusive(path, []byte("second\n")))
	data, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Equal(t, "first\n", string(data))
	temps, err := filepath.Glob(filepath.Join(dir, ".*.tmp-*"))
	require.NoError(t, err)
	require.Empty(t, temps)
}
