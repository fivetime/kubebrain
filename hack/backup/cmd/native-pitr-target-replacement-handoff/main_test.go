package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/kubewharf/kubebrain/hack/backup/internal/nativepitr"
	"github.com/stretchr/testify/require"
)

func TestApprovedPlanDigestRejectsUnknownAndPartialParameters(t *testing.T) {
	digest := strings.Repeat("a", 64)
	valid := `{"admission":"a","approve_plan_sha256":"` + digest + `","artifact_root":"r","full_artifacts":"fa","full_snapshot":"fs","pd_addrs":["pd:2379"],"plan":"p","remote_inventory":"ri","source_range_exclusive":"s","target_snapshot_empty":"t"}`
	got, err := approvedPlanDigest([]byte(valid))
	require.NoError(t, err)
	require.Equal(t, digest, got)

	_, err = approvedPlanDigest([]byte(strings.TrimSuffix(valid, "}") + `,"unknown":true}`))
	require.ErrorContains(t, err, "schema")
	_, err = approvedPlanDigest([]byte(`{"approve_plan_sha256":"` + digest + `"}`))
	require.ErrorContains(t, err, "schema")
	_, err = approvedPlanDigest([]byte(valid + `{}`))
	require.ErrorContains(t, err, "trailing")
}

func TestWriteExclusiveDoesNotOverwrite(t *testing.T) {
	path := filepath.Join(t.TempDir(), "handoff.json")
	value := nativeTargetReplacementForWriteTest()
	require.NoError(t, writeExclusive(path, value))
	first, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Equal(t, os.FileMode(0o600), mustStat(t, path).Mode().Perm())
	require.Error(t, writeExclusive(path, value))
	second, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Equal(t, first, second)
}

func nativeTargetReplacementForWriteTest() nativepitr.TargetReplacementHandoff {
	digest := strings.Repeat("a", 64)
	return nativepitr.TargetReplacementHandoff{Format: nativepitr.TargetReplacementHandoffFormat, FailedOperationAuditSHA256: digest, FailedOperationName: "native-pitr-restore-" + strings.Repeat("a", 20), FailedOperationParametersSHA: digest, OldPlanSHA256: digest, NewPlanSHA256: digest, OldTargetReceiptSHA256: digest, NewTargetReceiptSHA256: digest, OldTargetProvisioningSHA256: digest, NewTargetProvisioningSHA256: digest, OldTargetRetirementSHA256: digest, NewRestoreAdmissionSHA256: digest, SourceExclusiveSHA256: digest, FullArtifactSHA256: digest, OldTargetClusterID: 1, NewTargetClusterID: 2, ReplacementTargetEmpty: true, AdmissionFenceReacquired: true, CreatedAtUnix: 1}
}

func mustStat(t *testing.T, path string) os.FileInfo {
	t.Helper()
	info, err := os.Stat(path)
	require.NoError(t, err)
	return info
}

func TestApprovedPlanDigestRequiresPairedEncryptionFields(t *testing.T) {
	digest := strings.Repeat("a", 64)
	partial := `{"admission":"a","approve_plan_sha256":"` + digest + `","artifact_root":"r","cipher_method":"aes256-ctr","full_artifacts":"fa","full_snapshot":"fs","pd_addrs":["pd:2379"],"plan":"p","remote_inventory":"ri","source_range_exclusive":"s","target_snapshot_empty":"t"}`
	_, err := approvedPlanDigest([]byte(partial))
	require.ErrorContains(t, err, "schema")
}

func TestApprovedPlanDigestAcceptsOnlyCompletePriorReplacementGroup(t *testing.T) {
	digest := strings.Repeat("a", 64)
	base := `{"admission":"a","approve_plan_sha256":"` + digest + `","artifact_root":"r","full_artifacts":"fa","full_snapshot":"fs","pd_addrs":["pd:2379"],"plan":"p","remote_inventory":"ri","source_range_exclusive":"s","target_snapshot_empty":"t"`
	complete := base + `,"target_provisioning":"tp","target_provisioning_sha256":"` + digest + `","target_replacement_handoff":"rh","target_replacement_handoff_sha256":"` + digest + `"}`
	got, err := approvedPlanDigest([]byte(complete))
	require.NoError(t, err)
	require.Equal(t, digest, got)
	_, err = approvedPlanDigest([]byte(base + `,"target_replacement_handoff":"rh"}`))
	require.ErrorContains(t, err, "replacement schema")
	full := strings.TrimSuffix(complete, "}") + `,"old_restore_admission":"oa","old_restore_admission_sha256":"` + digest + `","old_target_provisioning":"op","old_target_provisioning_sha256":"` + digest + `","old_target_retirement":"or","old_target_retirement_sha256":"` + digest + `","old_target_snapshot_empty":"ot","old_target_snapshot_empty_sha256":"` + digest + `"}`
	_, err = approvedPlanDigest([]byte(full))
	require.NoError(t, err)
	_, err = approvedPlanDigest([]byte(strings.TrimSuffix(complete, "}") + `,"old_target_retirement":"or"}`))
	require.ErrorContains(t, err, "retirement schema")
}
