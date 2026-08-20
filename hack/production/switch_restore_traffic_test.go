package production_test

import (
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

const restoreArtifactSHA256 = "54d00d867758cef816bc4685f58e327b949712b07ebd17c3485f3ffc9e9f5133"

func TestRestoreTrafficCutoverLifecycleAndRollback(t *testing.T) {
	f := newTrafficFixture(t)
	f.run(t, "prepare", true, "")
	f.run(t, "cutover", true, "")
	f.run(t, "verify", true, "")
	f.run(t, "complete", true, "")
	f.run(t, "complete", true, "")

	data, err := os.ReadFile(filepath.Join(f.state, "restore-1.receipt.json"))
	require.NoError(t, err)
	var receipt map[string]any
	require.NoError(t, json.Unmarshal(data, &receipt))
	require.Equal(t, "kubebrain.restore-cutover.receipt.v1", receipt["format"])
	require.Equal(t, "uid-service", receipt["service_uid"])
	require.NotEmpty(t, receipt["cutover_state_sha256"])
	require.Equal(t, true, receipt["endpoint_uids_matched"])

	r := newTrafficFixture(t)
	r.run(t, "prepare", true, "")
	r.run(t, "cutover", true, "")
	r.run(t, "rollback", true, "")
	r.run(t, "rollback", true, "")
	r.run(t, "complete", false, "", "rolled back operation cannot complete")
}

func TestRestoreTrafficCutoverRejectsVerifiedMarkerBeforeCutover(t *testing.T) {
	f := newTrafficFixture(t)
	f.run(t, "prepare", true, "")
	f.run(t, "cutover", true, "")
	f.run(t, "verify", true, "")
	require.NoError(t, os.WriteFile(
		filepath.Join(f.state, "restore-1.verified"),
		[]byte("VERIFIED\tkubebrain.restore-cutover.marker.v1\t1\n"),
		0o600,
	))

	f.run(t, "complete", false, "", "restore cutover chronology is invalid")
	require.NoFileExists(t, filepath.Join(f.state, "restore-1.receipt.json"))
}

func TestRestoreTrafficCutoverRejectsMarkerTimestampAboveInt64(t *testing.T) {
	f := newTrafficFixture(t)
	f.run(t, "prepare", true, "")
	f.run(t, "cutover", true, "")
	require.NoError(t, os.WriteFile(
		filepath.Join(f.state, "restore-1.cutover"),
		[]byte("CUTOVER\tkubebrain.restore-cutover.marker.v1\ttarget\t9223372036854775808\n"),
		0o600,
	))

	f.run(t, "verify", false, "", "restore cutover chronology is invalid")
	require.NoFileExists(t, filepath.Join(f.state, "restore-1.verified"))
}

func TestRestoreTrafficCutoverRejectsReceiptTimestampAboveInt64(t *testing.T) {
	f := newTrafficFixture(t)
	f.run(t, "prepare", true, "")
	f.run(t, "cutover", true, "")
	f.run(t, "verify", true, "")
	f.run(t, "complete", true, "")
	receiptPath := filepath.Join(f.state, "restore-1.receipt.json")
	var receipt map[string]any
	require.NoError(t, json.Unmarshal(mustRead(t, receiptPath), &receipt))
	receipt["completed_at_unix"] = json.Number("9223372036854775808")
	data, err := json.Marshal(receipt)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(receiptPath, append(data, '\n'), 0o600))

	f.run(t, "complete", false, "", "existing restore cutover receipt does not match")
}

func TestRestoreTrafficCutoverRejectsOversizedExistingCutoverReceipt(t *testing.T) {
	f := newTrafficFixture(t)
	f.run(t, "prepare", true, "")
	f.run(t, "cutover", true, "")
	f.run(t, "verify", true, "")
	f.run(t, "complete", true, "")
	receiptPath := filepath.Join(f.state, "restore-1.receipt.json")
	receipt := append(mustRead(t, receiptPath), []byte(strings.Repeat(" ", (4<<20)+1))...)
	require.NoError(t, os.WriteFile(receiptPath, receipt, 0o600))

	f.run(t, "complete", false, "", "existing restore cutover receipt does not match")
}

func TestRestoreTrafficCutoverRejectsCutoverBeforeRestoreVerification(t *testing.T) {
	for _, version := range []int{1, 2, 3} {
		t.Run(strconv.Itoa(version), func(t *testing.T) {
			f := newTrafficFixture(t)
			if version == 2 {
				promoteTrafficRestoreReceiptToV2(t, f, 73)
			} else if version == 3 {
				promoteTrafficRestoreReceiptToV3(t, f, 73, 7)
			}
			path := filepath.Join(f.dir, "restore.json")
			var receipt map[string]any
			require.NoError(t, json.Unmarshal(mustRead(t, path), &receipt))
			receipt["verified_at_unix"] = float64(4102444800)
			data, err := json.Marshal(receipt)
			require.NoError(t, err)
			require.NoError(t, os.WriteFile(path, append(data, '\n'), 0o600))

			f.run(t, "prepare", true, "")
			f.run(t, "cutover", false, "", "restore verification occurred after cutover")
			require.NoFileExists(t, filepath.Join(f.dir, "selector-target"))
			require.NoFileExists(t, filepath.Join(f.state, "restore-1.cutover"))
		})
	}
}

func TestRestoreTrafficCutoverTreatsConcurrentReceiptPublishAsIdempotent(t *testing.T) {
	f := newTrafficFixture(t)
	f.run(t, "prepare", true, "")
	f.run(t, "cutover", true, "")
	f.run(t, "verify", true, "")
	f.run(t, "complete", true, "PUBLISH_CUTOVER_RECEIPT_DURING_JQ=true")

	data, err := os.ReadFile(filepath.Join(f.state, "restore-1.receipt.json"))
	require.NoError(t, err)
	var receipt map[string]any
	require.NoError(t, json.Unmarshal(data, &receipt))
	require.Equal(t, "kubebrain.restore-cutover.receipt.v1", receipt["format"])
	require.Equal(t, "uid-service", receipt["service_uid"])
	verifiedFields := strings.Split(strings.TrimSpace(string(mustRead(t,
		filepath.Join(f.state, "restore-1.verified")))), "\t")
	require.Len(t, verifiedFields, 3)
	verifiedAt, err := strconv.ParseFloat(verifiedFields[2], 64)
	require.NoError(t, err)
	require.Equal(t, verifiedAt, receipt["completed_at_unix"])
}

func TestRestoreTrafficCutoverRejectsStateDriftDuringReceiptPublish(t *testing.T) {
	f := newTrafficFixture(t)
	f.run(t, "prepare", true, "")
	f.run(t, "cutover", true, "")
	f.run(t, "verify", true, "")
	f.run(t, "complete", false, "TAMPER_CUTOVER_STATE_DURING_RECEIPT_JQ=true", "state changed")

	require.NoFileExists(t, filepath.Join(f.state, "restore-1.receipt.json"))
}

func TestRestoreTrafficCutoverRejectsInputDriftDuringCapture(t *testing.T) {
	t.Run("restore receipt", func(t *testing.T) {
		f := newTrafficFixture(t)
		f.run(t, "prepare", false, "TAMPER_RESTORE_RECEIPT_DURING_SHA256=true",
			"restore verification receipt changed while being captured")
		require.NoFileExists(t, filepath.Join(f.state, "restore-1.state"))
	})

	t.Run("backup", func(t *testing.T) {
		f := newTrafficFixture(t)
		f.run(t, "prepare", true, "")
		f.run(t, "cutover", true, "")
		f.run(t, "verify", false, "TAMPER_BACKUP_INPUT_DURING_SHA256=true",
			"backup input changed while being captured")
		require.NoFileExists(t, filepath.Join(f.state, "restore-1.verified"))
	})
}

func TestRestoreTrafficCutoverPassesFrozenBackupToVerify(t *testing.T) {
	f := newTrafficFixture(t)
	f.run(t, "prepare", true, "")
	f.run(t, "cutover", true, "")
	f.run(t, "verify", true, "ASSERT_FROZEN_BACKUP_INPUT=true")
}

func TestRestoreTrafficCutoverRejectsUnsafePublicEndpoint(t *testing.T) {
	for _, tc := range []struct {
		name     string
		endpoint string
	}{
		{name: "control character", endpoint: "https://service:2379\nother"},
		{name: "DEL", endpoint: "https://service:2379\x7fother"},
		{name: "quote", endpoint: `https://service:2379"other`},
		{name: "backslash", endpoint: `https://service:2379\other`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newTrafficFixture(t)
			f.run(t, "prepare", true, "")
			f.run(t, "cutover", true, "")
			f.run(t, "verify", false, "PUBLIC_ENDPOINT="+tc.endpoint,
				"PUBLIC_ENDPOINT contains unsupported characters")
			require.NoFileExists(t, filepath.Join(f.state, "restore-1.verified"))
		})
	}
}

func TestRestoreTrafficCutoverRejectsExistingReceiptWithUnknownFields(t *testing.T) {
	f := newTrafficFixture(t)
	f.run(t, "prepare", true, "")
	f.run(t, "cutover", true, "")
	f.run(t, "verify", true, "")

	receiptPath := filepath.Join(f.state, "restore-1.receipt.json")
	receipt := `{"artifact_sha256":"` + restoreArtifactSHA256 + `","completed_at_unix":1,"cutover_state_sha256":"` + fileDigest(t, filepath.Join(f.state, "restore-1.state")) + `","endpoint_uids_matched":true,"format":"kubebrain.restore-cutover.receipt.v1","instance":"instance-a","operation_id":"restore-1","pod_uids_unchanged":true,"public_data_verified":true,"replicas":2,"service_name":"kubebrain","service_namespace":"instance-a","service_uid":"uid-service","snapshot_revision":42,"source_instance":"source","target_instance":"target","unexpected":true}` + "\n"
	require.NoError(t, os.WriteFile(receiptPath, []byte(receipt), 0o600))

	f.run(t, "complete", false, "", "existing restore cutover receipt does not match")
	data, err := os.ReadFile(receiptPath)
	require.NoError(t, err)
	require.Equal(t, receipt, string(data))
}

func TestRestoreTrafficCutoverRejectsRestoreReceiptWithUnknownFields(t *testing.T) {
	f := newTrafficFixture(t)
	path := filepath.Join(f.dir, "restore.json")
	receipt := strings.TrimSpace(string(mustRead(t, path)))
	receipt = strings.TrimSuffix(receipt, "}") + `,"unexpected":true}` + "\n"
	require.NoError(t, os.WriteFile(path, []byte(receipt), 0o600))

	f.run(t, "prepare", false, "", "restore verification receipt is invalid")
	require.NoFileExists(t, filepath.Join(f.state, "restore-1.state"))
}

func TestRestoreTrafficCutoverRejectsOversizedRestoreReceipt(t *testing.T) {
	f := newTrafficFixture(t)
	path := filepath.Join(f.dir, "restore.json")
	receipt := append(mustRead(t, path), []byte(strings.Repeat(" ", (4<<20)+1))...)
	require.NoError(t, os.WriteFile(path, receipt, 0o600))

	f.run(t, "prepare", false, "", "restore verification receipt exceeds 4194304 bytes")
	require.NoFileExists(t, filepath.Join(f.state, "restore-1.state"))
}

func TestRestoreTrafficCutoverRejectsRestoreVerificationTimeAboveInt64(t *testing.T) {
	f := newTrafficFixture(t)
	path := filepath.Join(f.dir, "restore.json")
	receipt := strings.Replace(string(mustRead(t, path)), `"verified_at_unix":100`,
		`"verified_at_unix":9223372036854775808`, 1)
	require.NoError(t, os.WriteFile(path, []byte(receipt), 0o600))

	f.run(t, "prepare", false, "", "restore verification receipt is invalid")
	require.NoFileExists(t, filepath.Join(f.state, "restore-1.state"))
}

func TestRestoreTrafficCutoverRejectsBackupDifferentFromRestoreReceipt(t *testing.T) {
	f := newTrafficFixture(t)
	path := filepath.Join(f.dir, "restore.json")
	receipt := strings.ReplaceAll(string(mustRead(t, path)), restoreArtifactSHA256, strings.Repeat("9", 64))
	require.NoError(t, os.WriteFile(path, []byte(receipt), 0o600))

	f.run(t, "prepare", false, "", "backup input does not match restore verification receipt artifact digest")
	require.NoFileExists(t, filepath.Join(f.state, "restore-1.state"))
}

func TestRestoreTrafficCutoverAcceptsRevisionBoundV2Receipt(t *testing.T) {
	f := newTrafficFixture(t)
	promoteTrafficRestoreReceiptToV2(t, f, 84)

	f.run(t, "prepare", true, "")
}

func TestRestoreTrafficCutoverV2EvidenceChain(t *testing.T) {
	f := newTrafficFixture(t)
	path := promoteTrafficRestoreReceiptToV2(t, f, 73)
	restoreReceiptSHA := fileDigest(t, path)

	f.run(t, "prepare", true, "")
	f.run(t, "cutover", true, "")
	f.run(t, "verify", true, "")
	f.run(t, "complete", true, "")
	f.run(t, "complete", true, "")

	stateHeader := strings.Split(strings.SplitN(string(mustRead(t, filepath.Join(f.state, "restore-1.state"))), "\n", 2)[0], "\t")
	require.Len(t, stateHeader, 16)
	require.Equal(t, "kubebrain.restore-cutover.state.v2", stateHeader[1])
	require.Equal(t, "kubebrain.restore-verification.v2", stateHeader[13])
	require.Equal(t, "73", stateHeader[14])
	require.Equal(t, restoreReceiptSHA, stateHeader[15])

	var cutoverReceipt map[string]any
	require.NoError(t, json.Unmarshal(mustRead(t, filepath.Join(f.state, "restore-1.receipt.json")), &cutoverReceipt))
	require.Equal(t, "kubebrain.restore-cutover.receipt.v2", cutoverReceipt["format"])
	require.Equal(t, "kubebrain.restore-verification.v2", cutoverReceipt["restore_receipt_format"])
	require.Equal(t, restoreReceiptSHA, cutoverReceipt["restore_receipt_sha256"])
	require.Equal(t, float64(73), cutoverReceipt["initial_verified_target_revision"])
	require.Equal(t, float64(84), cutoverReceipt["public_verified_target_revision"])
}

func TestRestoreTrafficCutoverV3BindsTargetCluster(t *testing.T) {
	f := newTrafficFixture(t)
	promoteTrafficRestoreReceiptToV3(t, f, 73, 7)
	f.run(t, "prepare", true, "")
	f.run(t, "cutover", true, "")
	f.run(t, "verify", true, "")
	f.run(t, "complete", true, "")

	stateHeader := strings.Split(strings.SplitN(string(mustRead(t, filepath.Join(f.state, "restore-1.state"))), "\n", 2)[0], "\t")
	require.Len(t, stateHeader, 17)
	require.Equal(t, "kubebrain.restore-cutover.state.v3", stateHeader[1])
	require.Equal(t, "kubebrain.restore-verification.v3", stateHeader[13])
	require.Equal(t, "7", stateHeader[16])

	var cutoverReceipt map[string]any
	require.NoError(t, json.Unmarshal(mustRead(t, filepath.Join(f.state, "restore-1.receipt.json")), &cutoverReceipt))
	require.Equal(t, "kubebrain.restore-cutover.receipt.v3", cutoverReceipt["format"])
	require.Equal(t, float64(7), cutoverReceipt["verified_target_cluster_id"])
}

func TestRestoreTrafficCutoverV3RejectsTargetClusterDrift(t *testing.T) {
	f := newTrafficFixture(t)
	promoteTrafficRestoreReceiptToV3(t, f, 73, 7)
	f.run(t, "prepare", true, "")
	f.run(t, "cutover", true, "")
	f.run(t, "verify", false, "VERIFY_TARGET_CLUSTER_ID=8", "changed target cluster ID")
	require.NoFileExists(t, filepath.Join(f.state, "restore-1.verified"))
}

func TestRestoreTrafficCutoverV3RejectsStateClusterDrift(t *testing.T) {
	f := newTrafficFixture(t)
	promoteTrafficRestoreReceiptToV3(t, f, 73, 7)
	f.run(t, "prepare", true, "")
	statePath := filepath.Join(f.state, "restore-1.state")
	state := strings.Replace(string(mustRead(t, statePath)), "\t7\n", "\t8\n", 1)
	require.NoError(t, os.WriteFile(statePath, []byte(state), 0o600))
	f.run(t, "cutover", false, "", "restore verification receipt does not match prepared cutover state")
	require.NoFileExists(t, filepath.Join(f.dir, "selector-target"))
	require.NoFileExists(t, filepath.Join(f.state, "restore-1.cutover"))
}

func TestRestoreTrafficCutoverV3RejectsExistingReceiptClusterDrift(t *testing.T) {
	f := newTrafficFixture(t)
	promoteTrafficRestoreReceiptToV3(t, f, 73, 7)
	f.run(t, "prepare", true, "")
	f.run(t, "cutover", true, "")
	f.run(t, "verify", true, "")
	f.run(t, "complete", true, "")
	receiptPath := filepath.Join(f.state, "restore-1.receipt.json")
	receipt := strings.Replace(string(mustRead(t, receiptPath)), `"verified_target_cluster_id":7`, `"verified_target_cluster_id":8`, 1)
	require.NoError(t, os.WriteFile(receiptPath, []byte(receipt), 0o600))
	f.run(t, "complete", false, "", "existing restore cutover receipt does not match")
}

func TestRestoreTrafficCutoverV3AcceptsMaxUint64ClusterID(t *testing.T) {
	f := newTrafficFixture(t)
	promoteTrafficRestoreReceiptToV3(t, f, 73, ^uint64(0))
	f.run(t, "prepare", true, "")
	f.run(t, "cutover", true, "")
	f.run(t, "verify", true, "VERIFY_TARGET_CLUSTER_ID=18446744073709551615")
	f.run(t, "complete", true, "VERIFY_TARGET_CLUSTER_ID=18446744073709551615")
}

func TestRestoreTrafficCutoverDoesNotPublishRegressedPublicRevision(t *testing.T) {
	f := newTrafficFixture(t)
	promoteTrafficRestoreReceiptToV2(t, f, 73)
	f.run(t, "prepare", true, "")
	f.run(t, "cutover", true, "")
	f.run(t, "verify", true, "")
	f.run(t, "complete", false, "VERIFY_TARGET_REVISION=72", "public verification revision predates")
	require.NoFileExists(t, filepath.Join(f.state, "restore-1.receipt.json"))
}

func TestRestoreTrafficCutoverAcceptsMaxInt64Revision(t *testing.T) {
	f := newTrafficFixture(t)
	promoteTrafficRestoreReceiptToV2(t, f, 9223372036854775807)
	f.run(t, "prepare", true, "")
	f.run(t, "cutover", true, "")
	f.run(t, "verify", true, "VERIFY_TARGET_REVISION=9223372036854775807")
	f.run(t, "complete", true, "VERIFY_TARGET_REVISION=9223372036854775807")
}

func TestRestoreTrafficCutoverRejectsTamperedV2EvidenceChain(t *testing.T) {
	t.Run("state target revision", func(t *testing.T) {
		f := newTrafficFixture(t)
		promoteTrafficRestoreReceiptToV2(t, f, 73)
		f.run(t, "prepare", true, "")
		statePath := filepath.Join(f.state, "restore-1.state")
		state := strings.Replace(string(mustRead(t, statePath)), "\t73\t", "\t0\t", 1)
		require.NoError(t, os.WriteFile(statePath, []byte(state), 0o600))
		f.run(t, "cutover", false, "", "invalid schema")
	})

	t.Run("state target revision above int64", func(t *testing.T) {
		f := newTrafficFixture(t)
		promoteTrafficRestoreReceiptToV2(t, f, 73)
		f.run(t, "prepare", true, "")
		statePath := filepath.Join(f.state, "restore-1.state")
		state := strings.Replace(string(mustRead(t, statePath)), "\t73\t", "\t9223372036854775808\t", 1)
		require.NoError(t, os.WriteFile(statePath, []byte(state), 0o600))
		f.run(t, "cutover", false, "", "invalid schema")
	})

	t.Run("final public revision", func(t *testing.T) {
		f := newTrafficFixture(t)
		promoteTrafficRestoreReceiptToV2(t, f, 73)
		f.run(t, "prepare", true, "")
		f.run(t, "cutover", true, "")
		f.run(t, "verify", true, "")
		f.run(t, "complete", true, "")
		receiptPath := filepath.Join(f.state, "restore-1.receipt.json")
		receipt := strings.Replace(string(mustRead(t, receiptPath)), `"public_verified_target_revision":84`, `"public_verified_target_revision":0`, 1)
		require.NoError(t, os.WriteFile(receiptPath, []byte(receipt), 0o600))
		f.run(t, "complete", false, "", "existing restore cutover receipt does not match")
	})

	t.Run("final public revision predates initial", func(t *testing.T) {
		f := newTrafficFixture(t)
		promoteTrafficRestoreReceiptToV2(t, f, 73)
		f.run(t, "prepare", true, "")
		f.run(t, "cutover", true, "")
		f.run(t, "verify", true, "")
		f.run(t, "complete", true, "")
		receiptPath := filepath.Join(f.state, "restore-1.receipt.json")
		receipt := strings.Replace(string(mustRead(t, receiptPath)), `"public_verified_target_revision":84`, `"public_verified_target_revision":72`, 1)
		require.NoError(t, os.WriteFile(receiptPath, []byte(receipt), 0o600))
		f.run(t, "complete", false, "", "existing restore cutover receipt does not match")
	})
}

func TestRestoreTrafficCutoverRejectsMalformedRevisionBinding(t *testing.T) {
	for _, tc := range []struct {
		name   string
		format string
		value  any
	}{
		{name: "v2 missing revision", format: "kubebrain.restore-verification.v2"},
		{name: "v2 zero revision", format: "kubebrain.restore-verification.v2", value: float64(0)},
		{name: "v2 revision above int64", format: "kubebrain.restore-verification.v2", value: json.Number("9223372036854775808")},
		{name: "v1 carrying v2 field", format: "kubebrain.restore-verification.v1", value: float64(84)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newTrafficFixture(t)
			path := filepath.Join(f.dir, "restore.json")
			var receipt map[string]any
			require.NoError(t, json.Unmarshal(mustRead(t, path), &receipt))
			receipt["format"] = tc.format
			if tc.value == nil {
				delete(receipt, "verified_target_revision")
			} else {
				receipt["verified_target_revision"] = tc.value
			}
			data, err := json.Marshal(receipt)
			require.NoError(t, err)
			require.NoError(t, os.WriteFile(path, append(data, '\n'), 0o600))

			f.run(t, "prepare", false, "", "restore verification receipt is invalid")
			require.NoFileExists(t, filepath.Join(f.state, "restore-1.state"))
		})
	}
}

func TestRestoreTrafficCutoverRejectsMalformedClusterBinding(t *testing.T) {
	for _, tc := range []struct {
		name      string
		format    string
		clusterID any
	}{
		{name: "v3 missing cluster", format: "kubebrain.restore-verification.v3"},
		{name: "v3 zero cluster", format: "kubebrain.restore-verification.v3", clusterID: float64(0)},
		{name: "v3 cluster above uint64", format: "kubebrain.restore-verification.v3", clusterID: json.Number("18446744073709551616")},
		{name: "v2 carrying cluster", format: "kubebrain.restore-verification.v2", clusterID: float64(7)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newTrafficFixture(t)
			path := filepath.Join(f.dir, "restore.json")
			var receipt map[string]any
			require.NoError(t, json.Unmarshal(mustRead(t, path), &receipt))
			receipt["format"] = tc.format
			receipt["verified_target_revision"] = float64(84)
			if tc.clusterID == nil {
				delete(receipt, "verified_target_cluster_id")
			} else {
				receipt["verified_target_cluster_id"] = tc.clusterID
			}
			data, err := json.Marshal(receipt)
			require.NoError(t, err)
			require.NoError(t, os.WriteFile(path, append(data, '\n'), 0o600))
			f.run(t, "prepare", false, "", "restore verification receipt is invalid")
			require.NoFileExists(t, filepath.Join(f.state, "restore-1.state"))
		})
	}
}

func TestRestoreTrafficCutoverRejectsRestoreReceiptWithInvalidDigest(t *testing.T) {
	for _, tc := range []struct {
		name   string
		digest string
	}{
		{name: "short", digest: "abc123"},
		{name: "uppercase", digest: strings.ToUpper(restoreArtifactSHA256)},
		{name: "non-hex", digest: strings.Repeat("g", 64)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newTrafficFixture(t)
			path := filepath.Join(f.dir, "restore.json")
			receipt := strings.ReplaceAll(
				strings.TrimSpace(string(mustRead(t, path))),
				restoreArtifactSHA256,
				tc.digest,
			) + "\n"
			require.NoError(t, os.WriteFile(path, []byte(receipt), 0o600))

			f.run(t, "prepare", false, "", "restore verification receipt is invalid")
			require.NoFileExists(t, filepath.Join(f.state, "restore-1.state"))
		})
	}
}

func TestRestoreTrafficCutoverRejectsRestoreReceiptVerifiedBeforeArtifactCreation(t *testing.T) {
	f := newTrafficFixture(t)
	path := filepath.Join(f.dir, "restore.json")
	receipt := strings.Replace(
		strings.TrimSpace(string(mustRead(t, path))),
		`"verified_at_unix":100`,
		`"artifact_created_at_unix":101,"verified_at_unix":100`,
		1,
	) + "\n"
	require.NoError(t, os.WriteFile(path, []byte(receipt), 0o600))

	f.run(t, "prepare", false, "", "restore verification receipt is invalid")
	require.NoFileExists(t, filepath.Join(f.state, "restore-1.state"))
}

func TestRestoreTrafficCutoverAllowsRestoreReceiptVerifiedAtArtifactCreationSecond(t *testing.T) {
	f := newTrafficFixture(t)
	path := filepath.Join(f.dir, "restore.json")
	receipt := strings.Replace(
		strings.TrimSpace(string(mustRead(t, path))),
		`"verified_at_unix":100`,
		`"artifact_created_at_unix":100,"verified_at_unix":100`,
		1,
	) + "\n"
	require.NoError(t, os.WriteFile(path, []byte(receipt), 0o600))

	f.run(t, "prepare", true, "")
}

func TestRestoreTrafficCutoverRejectsRestoreReceiptWithInvalidPrefixes(t *testing.T) {
	for _, tc := range []struct {
		name string
		edit func(string) string
	}{
		{
			name: "relative source",
			edit: func(receipt string) string {
				return strings.Replace(receipt, `"source_prefix":"/registry"`, `"source_prefix":"registry"`, 1)
			},
		},
		{
			name: "source with control character",
			edit: func(receipt string) string {
				return replaceRestoreReceiptFieldForTest(t, receipt, "source_prefix", "/registry\tshadow")
			},
		},
		{
			name: "source with del",
			edit: func(receipt string) string {
				return replaceRestoreReceiptFieldForTest(t, receipt, "source_prefix", "/registry\x7fshadow")
			},
		},
		{
			name: "relative target",
			edit: func(receipt string) string {
				return strings.Replace(receipt, `"target_prefix":"/restored"`, `"target_prefix":"restored"`, 1)
			},
		},
		{
			name: "target with control character",
			edit: func(receipt string) string {
				return replaceRestoreReceiptFieldForTest(t, receipt, "target_prefix", "/restored\tshadow")
			},
		},
		{
			name: "target with del",
			edit: func(receipt string) string {
				return replaceRestoreReceiptFieldForTest(t, receipt, "target_prefix", "/restored\x7fshadow")
			},
		},
		{
			name: "same prefixes",
			edit: func(receipt string) string {
				return strings.Replace(receipt, `"target_prefix":"/restored"`, `"target_prefix":"/registry"`, 1)
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newTrafficFixture(t)
			path := filepath.Join(f.dir, "restore.json")
			receipt := tc.edit(strings.TrimSpace(string(mustRead(t, path)))) + "\n"
			require.NoError(t, os.WriteFile(path, []byte(receipt), 0o600))

			f.run(t, "prepare", false, "", "restore verification receipt is invalid")
			require.NoFileExists(t, filepath.Join(f.state, "restore-1.state"))
		})
	}
}

func replaceRestoreReceiptFieldForTest(t *testing.T, receipt, field, value string) string {
	t.Helper()
	var document map[string]any
	require.NoError(t, json.Unmarshal([]byte(receipt), &document))
	document[field] = value
	encoded, err := json.Marshal(document)
	require.NoError(t, err)
	return string(encoded)
}

func TestRestoreTrafficCutoverRejectsNonCanonicalState(t *testing.T) {
	f := newTrafficFixture(t)
	f.run(t, "prepare", true, "")
	statePath := filepath.Join(f.state, "restore-1.state")
	require.NoError(t, os.WriteFile(statePath, append(mustRead(t, statePath), []byte("UNKNOWN\trow\n")...), 0o600))

	f.run(t, "cutover", false, "", "state has invalid schema")
	require.NoFileExists(t, filepath.Join(f.state, "restore-1.cutover"))
}

func TestRestoreTrafficCutoverRejectsNonCanonicalMarkers(t *testing.T) {
	t.Run("cutover marker with extra field", func(t *testing.T) {
		f := newTrafficFixture(t)
		f.run(t, "prepare", true, "")
		f.run(t, "cutover", true, "")

		markerPath := filepath.Join(f.state, "restore-1.cutover")
		require.NoError(t, os.WriteFile(markerPath, []byte("CUTOVER\tkubebrain.restore-cutover.marker.v1\ttarget\t1\textra\n"), 0o600))

		f.run(t, "verify", false, "", "existing restore cutover marker does not match")
		require.NoFileExists(t, filepath.Join(f.state, "restore-1.verified"))
	})

	t.Run("verified marker with extra row", func(t *testing.T) {
		f := newTrafficFixture(t)
		f.run(t, "prepare", true, "")
		f.run(t, "cutover", true, "")
		f.run(t, "verify", true, "")

		markerPath := filepath.Join(f.state, "restore-1.verified")
		require.NoError(t, os.WriteFile(markerPath, append(mustRead(t, markerPath), []byte("UNKNOWN\trow\n")...), 0o600))

		f.run(t, "complete", false, "", "existing restore cutover marker does not match")
		require.NoFileExists(t, filepath.Join(f.state, "restore-1.receipt.json"))
	})
}

func TestRestoreTrafficCutoverFailsClosed(t *testing.T) {
	for _, tc := range []struct {
		name, action, drift, want string
		prior                     []string
	}{
		{"cutover before prepare", "cutover", "", "prepare evidence is missing", nil},
		{"wrong initial selector", "prepare", "selector-target", "must select SOURCE_INSTANCE", nil},
		{"target pod replaced", "cutover", "target-uid-drift", "target Pod UID/readiness/restart fence failed", []string{"prepare"}},
		{"service UID replaced", "cutover", "service-uid-drift", "Service identity or selector fence failed", []string{"prepare"}},
		{"CAS conflict", "cutover", "cas-conflict", "conflict", []string{"prepare"}},
		{"foreign endpoint", "cutover", "foreign-endpoint", "timed out waiting", []string{"prepare"}},
		{"public data mismatch", "verify", "verify-fail", "verification failed", []string{"prepare", "cutover"}},
		{"complete without verify", "complete", "", "cutover verification evidence is missing", []string{"prepare", "cutover"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newTrafficFixture(t)
			for _, action := range tc.prior {
				f.run(t, action, true, "")
			}
			if tc.drift != "" {
				require.NoError(t, os.WriteFile(filepath.Join(f.dir, tc.drift), []byte("1"), 0o600))
			}
			f.run(t, tc.action, false, "", tc.want)
			_, err := os.Stat(filepath.Join(f.state, "restore-1.receipt.json"))
			require.ErrorIs(t, err, os.ErrNotExist)
		})
	}
}

func TestRestoreTrafficRollbackRecoversPatchBeforeCutoverMarker(t *testing.T) {
	f := newTrafficFixture(t)
	f.run(t, "prepare", true, "")
	require.NoError(t, os.WriteFile(filepath.Join(f.dir, "selector-target"), []byte("1"), 0o600))
	f.run(t, "rollback", true, "")
	data, err := os.ReadFile(filepath.Join(f.stateDir(), "restore-1.rollback"))
	require.NoError(t, err)
	require.Contains(t, string(data), "ROLLBACK")
}

type trafficFixture struct {
	dir, state string
	env        []string
}

func promoteTrafficRestoreReceiptToV2(t *testing.T, f *trafficFixture, revision int64) string {
	t.Helper()
	path := filepath.Join(f.dir, "restore.json")
	var receipt map[string]any
	require.NoError(t, json.Unmarshal(mustRead(t, path), &receipt))
	receipt["format"] = "kubebrain.restore-verification.v2"
	receipt["verified_target_revision"] = revision
	data, err := json.Marshal(receipt)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(path, append(data, '\n'), 0o600))
	return path
}

func promoteTrafficRestoreReceiptToV3(t *testing.T, f *trafficFixture, revision int64, clusterID uint64) string {
	t.Helper()
	path := promoteTrafficRestoreReceiptToV2(t, f, revision)
	var receipt map[string]any
	require.NoError(t, json.Unmarshal(mustRead(t, path), &receipt))
	receipt["format"] = "kubebrain.restore-verification.v3"
	receipt["verified_target_cluster_id"] = clusterID
	data, err := json.Marshal(receipt)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(path, append(data, '\n'), 0o600))
	return path
}

func (f *trafficFixture) stateDir() string {
	return f.state
}

func newTrafficFixture(t *testing.T) *trafficFixture {
	t.Helper()
	dir := t.TempDir()
	state := filepath.Join(dir, "state")
	require.NoError(t, os.Mkdir(state, 0o700))
	backup := filepath.Join(dir, "backup.jsonl")
	require.NoError(t, os.WriteFile(backup, []byte("backup"), 0o600))
	restoreReceipt := filepath.Join(dir, "restore.json")
	require.NoError(t, os.WriteFile(restoreReceipt, []byte(`{
	  "format":"kubebrain.restore-verification.v1","artifact_format":"kubebrain.logical.v2",
	  "artifact_sha256":"`+restoreArtifactSHA256+`","snapshot_revision":42,"source_prefix":"/registry",
	  "target_prefix":"/restored","records":2,"artifact_leases":1,
	  "verified_target_leases":1,"verified_at_unix":100
	}`), 0o600))

	kubectl := filepath.Join(dir, "kubectl")
	writeTrafficExecutable(t, kubectl, `#!/usr/bin/env bash
set -euo pipefail
if [[ " $* " == *" patch service "* ]]; then
  [[ -f "$FAKE_DIR/cas-conflict" ]] && { echo conflict >&2; exit 1; }
  if [[ " $* " == *'"op":"replace"'*'"value":"target"'* ]]; then
    touch "$FAKE_DIR/selector-target"
  else
    rm -f "$FAKE_DIR/selector-target"
  fi
  exit 0
fi
if [[ " $* " == *" get service "* ]]; then
  selector=source
  [[ -f "$FAKE_DIR/selector-target" ]] && selector=target
  uid=uid-service
  [[ -f "$FAKE_DIR/service-uid-drift" ]] && uid=uid-new
  rv=10
  [[ "$selector" == target ]] && rv=11
  printf '{"metadata":{"uid":"%s","resourceVersion":"%s"},"spec":{"selector":{"app.kubernetes.io/name":"kubebrain","app.kubernetes.io/instance":"%s"}}}\n' "$uid" "$rv" "$selector"
  exit 0
fi
if [[ " $* " == *" get pods "* ]]; then
  instance=source
  [[ " $* " == *"instance=target"* ]] && instance=target
  uid1="uid-${instance}-0"; uid2="uid-${instance}-1"
  [[ "$instance" == target && -f "$FAKE_DIR/target-uid-drift" ]] && uid1=uid-target-new
  printf '{"items":[
    {"metadata":{"name":"kb-%s-0","uid":"%s"},"status":{"conditions":[{"type":"Ready","status":"True"}],"containerStatuses":[{"ready":true,"restartCount":0}]}},
    {"metadata":{"name":"kb-%s-1","uid":"%s"},"status":{"conditions":[{"type":"Ready","status":"True"}],"containerStatuses":[{"ready":true,"restartCount":0}]}}
  ]}\n' "$instance" "$uid1" "$instance" "$uid2"
  exit 0
fi
if [[ " $* " == *" get endpointslices "* ]]; then
  instance=source
  [[ -f "$FAKE_DIR/selector-target" ]] && instance=target
  uid1="uid-${instance}-0"; uid2="uid-${instance}-1"
  [[ -f "$FAKE_DIR/foreign-endpoint" ]] && uid2=uid-foreign
  printf '{"items":[{"metadata":{"ownerReferences":[{"kind":"Service","uid":"uid-service","controller":true}]},"endpoints":[
    {"conditions":{"ready":true,"serving":true,"terminating":false},"targetRef":{"kind":"Pod","uid":"%s"}},
    {"conditions":{"ready":true,"serving":true,"terminating":false},"targetRef":{"kind":"Pod","uid":"%s"}}
  ]}]}\n' "$uid1" "$uid2"
  exit 0
fi
echo "unexpected kubectl call: $*" >&2
exit 1
`)
	verify := filepath.Join(dir, "logical-verify")
	writeTrafficExecutable(t, verify, `#!/usr/bin/env bash
set -euo pipefail
[[ ! -f "$FAKE_DIR/verify-fail" ]] || { echo verification failed >&2; exit 1; }
if [[ "${ASSERT_FROZEN_BACKUP_INPUT:-false}" == true ]]; then
  [[ "$INPUT" != "$TRAFFIC_BACKUP_SOURCE" ]] || { echo backup input was not frozen >&2; exit 1; }
  cmp -s "$INPUT" "$TRAFFIC_BACKUP_SOURCE" || { echo frozen backup input content mismatch >&2; exit 1; }
fi
cat >"$RECEIPT_OUTPUT" <<EOF
{"format":"kubebrain.restore-verification.v3","artifact_format":"kubebrain.logical.v2","artifact_sha256":"`+restoreArtifactSHA256+`","snapshot_revision":42,"source_prefix":"/registry","target_prefix":"/restored","records":2,"artifact_leases":1,"verified_target_leases":1,"verified_target_revision":${VERIFY_TARGET_REVISION:-84},"verified_target_cluster_id":${VERIFY_TARGET_CLUSTER_ID:-7},"verified_at_unix":200}
EOF
chmod 600 "$RECEIPT_OUTPUT"
`)
	realSHA256Sum, err := exec.LookPath("sha256sum")
	require.NoError(t, err)
	sha256sum := filepath.Join(dir, "sha256sum")
	writeTrafficExecutable(t, sha256sum, `#!/usr/bin/env bash
set -euo pipefail
"$REAL_SHA256SUM" "$@"
if [[ "$#" -ge 1 && "$1" == "$TRAFFIC_RESTORE_RECEIPT_SOURCE" &&
  "${TAMPER_RESTORE_RECEIPT_DURING_SHA256:-false}" == true &&
  ! -f "$FAKE_DIR/restore-receipt-sha256-tampered" ]]; then
  touch "$FAKE_DIR/restore-receipt-sha256-tampered"
  printf ' ' >>"$TRAFFIC_RESTORE_RECEIPT_SOURCE"
fi
if [[ "$#" -ge 1 && "$1" == "$TRAFFIC_BACKUP_SOURCE" &&
  "${TAMPER_BACKUP_INPUT_DURING_SHA256:-false}" == true &&
  ! -f "$FAKE_DIR/backup-sha256-tampered" ]]; then
  touch "$FAKE_DIR/backup-sha256-tampered"
  printf 'changed\n' >>"$TRAFFIC_BACKUP_SOURCE"
fi
`)
	realJQ, err := exec.LookPath("jq")
	require.NoError(t, err)
	jq := filepath.Join(dir, "jq-wrapper")
	writeTrafficExecutable(t, jq, `#!/usr/bin/env bash
set -euo pipefail
"$REAL_JQ" "$@"
if [[ "${TAMPER_CUTOVER_STATE_DURING_RECEIPT_JQ:-false}" == true &&
  " $* " == *" -cnS "* && " $* " == *" kubebrain.restore-cutover.receipt.v1 "* ]]; then
  printf 'UNKNOWN\trow\n' >>"$STATE_DIR/$OPERATION_ID.state"
fi
if [[ "${PUBLISH_CUTOVER_RECEIPT_DURING_JQ:-false}" == true &&
  " $* " == *" -cnS "* && " $* " == *" kubebrain.restore-cutover.receipt.v1 "* &&
  ! -f "$STATE_DIR/$OPERATION_ID.receipt.json" ]]; then
  state_path="$STATE_DIR/$OPERATION_ID.state"
  artifact_sha="$(awk -F '\t' '$1 == "HEADER" {print $10; exit}' "$state_path")"
  snapshot_revision="$(awk -F '\t' '$1 == "HEADER" {print $11; exit}' "$state_path")"
  service_uid="$(awk -F '\t' '$1 == "SERVICE" {print $2; exit}' "$state_path")"
  state_sha="$(sha256sum "$state_path" | cut -d ' ' -f1)"
  completed_at="$(awk -F '\t' '$1 == "VERIFIED" {print $3; exit}' "$STATE_DIR/$OPERATION_ID.verified")"
  printf '{"artifact_sha256":"%s","completed_at_unix":%s,"cutover_state_sha256":"%s","endpoint_uids_matched":true,"format":"kubebrain.restore-cutover.receipt.v1","instance":"%s","operation_id":"%s","pod_uids_unchanged":true,"public_data_verified":true,"replicas":%s,"service_name":"%s","service_namespace":"%s","service_uid":"%s","snapshot_revision":%s,"source_instance":"%s","target_instance":"%s"}\n' \
    "$artifact_sha" "$completed_at" "$state_sha" "$INSTANCE" "$OPERATION_ID" "$EXPECTED_REPLICAS" "$SERVICE_NAME" "$SERVICE_NAMESPACE" "$service_uid" "$snapshot_revision" "$SOURCE_INSTANCE" "$TARGET_INSTANCE" >"$STATE_DIR/$OPERATION_ID.receipt.json"
  chmod 600 "$STATE_DIR/$OPERATION_ID.receipt.json"
fi
`)
	return &trafficFixture{dir: dir, state: state, env: []string{
		"OPERATION_ID=restore-1", "INSTANCE=instance-a", "STATE_DIR=" + state,
		"RESTORE_RECEIPT_INPUT=" + restoreReceipt, "BACKUP_INPUT=" + backup,
		"SERVICE_NAMESPACE=instance-a", "SERVICE_NAME=kubebrain",
		"SOURCE_INSTANCE=source", "TARGET_INSTANCE=target", "EXPECTED_REPLICAS=2",
		"PUBLIC_ENDPOINT=https://service:2379", "TIMEOUT_SECONDS=1", "POLL_INTERVAL_SECONDS=0",
		"KUBECTL=" + kubectl, "LOGICAL_VERIFY=" + verify, "FAKE_DIR=" + dir,
		"PATH=" + dir + string(os.PathListSeparator) + os.Getenv("PATH"),
		"REAL_SHA256SUM=" + realSHA256Sum,
		"TRAFFIC_RESTORE_RECEIPT_SOURCE=" + restoreReceipt,
		"TRAFFIC_BACKUP_SOURCE=" + backup,
		"JQ=" + jq, "REAL_JQ=" + realJQ,
	}}
}

func (f *trafficFixture) run(t *testing.T, action string, ok bool, extra string, outputs ...string) {
	t.Helper()
	env := append([]string{}, f.env...)
	env = append(env, "ACTION="+action)
	if extra != "" {
		env = append(env, extra)
	}
	out, err := runSwitchRestoreTraffic(t, env)
	if ok {
		require.NoError(t, err, string(out))
	} else {
		require.Error(t, err, string(out))
	}
	for _, wanted := range outputs {
		require.Contains(t, strings.ToLower(string(out)), strings.ToLower(wanted))
	}
}

func runSwitchRestoreTraffic(t *testing.T, env []string) ([]byte, error) {
	t.Helper()
	return runProductionScriptCommand(t, "switch-restore-traffic.sh", env)
}

func writeTrafficExecutable(t *testing.T, path, content string) {
	t.Helper()
	require.NoError(t, os.WriteFile(path, []byte(content), 0o700))
}
