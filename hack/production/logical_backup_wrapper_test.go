package production_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestProductionReadinessColdRestoreVerifyExampleIncludesRequiredInputs(t *testing.T) {
	data, err := os.ReadFile(filepath.Join("..", "..", "docs", "production_readiness_cn.md"))
	require.NoError(t, err)
	doc := string(data)

	start := strings.Index(doc, "ENDPOINT=https://restored-kubebrain:2379")
	require.NotEqual(t, -1, start, "cold restore verify example is missing")
	end := strings.Index(doc[start:], "hack/backup/cold-restore-verify.sh")
	require.NotEqual(t, -1, end, "cold restore verify command is missing")
	example := doc[start : start+end]

	for _, required := range []string{
		"WITNESS_FILE=",
		"SNAPSHOT_RECEIPT_FILE=",
		"RESTORE_RECEIPT_FILE=",
		"RESTORE_MANIFEST_FILE=",
		"SEMANTIC_RECEIPT_FILE=",
		"VERIFY_PREFIX=",
	} {
		require.Contains(t, example, required)
	}
	require.Contains(t, doc, "target kube-system/namespace UID")
	require.Contains(t, doc, "source/restored TidbCluster UID")
	require.Contains(t, doc, "写入前缺任一项都会 fail closed")
}

func TestLogicalBackupWrappersRejectUnsafeEndpoint(t *testing.T) {
	for _, tc := range []struct {
		name     string
		endpoint string
	}{
		{name: "control character", endpoint: "https://service:2379\nother"},
		{name: "DEL", endpoint: "https://service:2379\x7fother"},
		{name: "quote", endpoint: `https://service:2379"other`},
		{name: "backslash", endpoint: `https://service:2379\other`},
	} {
		for _, script := range []string{
			"../backup/logical-export.sh",
			"../backup/logical-verify.sh",
			"../backup/logical-restore.sh",
		} {
			t.Run(script+"/"+tc.name, func(t *testing.T) {
				dir := t.TempDir()
				output := filepath.Join(dir, "artifact.jsonl")
				receipt := filepath.Join(dir, "receipt.json")
				env := []string{
					"ENDPOINT=" + tc.endpoint,
					"OUTPUT=" + output,
					"INPUT=" + output,
					"RECEIPT_OUTPUT=" + receipt,
				}

				out, err := runProductionScriptCommand(t, script, env)
				require.Error(t, err, string(out))
				require.Contains(t, string(out), "ENDPOINT contains unsupported characters")
				require.NoFileExists(t, output)
				require.NoFileExists(t, receipt)
			})
		}
	}
}

func TestBackupSmokeWrappersRejectUnsafeEndpoint(t *testing.T) {
	for _, tc := range []struct {
		name     string
		endpoint string
	}{
		{name: "control character", endpoint: "https://service:2379\nother"},
		{name: "DEL", endpoint: "https://service:2379\x7fother"},
		{name: "quote", endpoint: `https://service:2379"other`},
		{name: "backslash", endpoint: `https://service:2379\other`},
	} {
		for _, script := range []string{
			"../backup/logical-drill.sh",
			"../backup/backup-integrity-smoke.sh",
			"../backup/restore-guard-smoke.sh",
			"../backup/restore-rollback-smoke.sh",
			"../backup/lease-restore-smoke.sh",
			"../backup/verify-content-smoke.sh",
		} {
			t.Run(script+"/"+tc.name, func(t *testing.T) {
				dir := t.TempDir()
				output := filepath.Join(dir, "artifact.jsonl")
				corruptOutput := filepath.Join(dir, "corrupt.jsonl")
				restoreLog := filepath.Join(dir, "restore.log")
				verifyLog := filepath.Join(dir, "verify.log")
				env := []string{
					"ENDPOINT=" + tc.endpoint,
					"PREFIX=/__kubebrain/smoke/source",
					"RESTORE_PREFIX=/__kubebrain/smoke/restore",
					"OUTPUT=" + output,
					"CORRUPT_OUTPUT=" + corruptOutput,
					"RESTORE_LOG=" + restoreLog,
					"VERIFY_LOG=" + verifyLog,
				}

				out, err := runProductionScriptCommand(t, script, env)
				require.Error(t, err, string(out))
				require.Contains(t, string(out), "ENDPOINT contains unsupported characters")
				require.NoFileExists(t, output)
				require.NoFileExists(t, corruptOutput)
				require.NoFileExists(t, restoreLog)
				require.NoFileExists(t, verifyLog)
			})
		}
	}
}

func TestColdRestoreVerifyWrapperRejectsUnsafeEndpoint(t *testing.T) {
	for _, tc := range []struct {
		name     string
		endpoint string
	}{
		{name: "control character", endpoint: "https://restored:2379\nother"},
		{name: "DEL", endpoint: "https://restored:2379\x7fother"},
		{name: "quote", endpoint: `https://restored:2379"other`},
		{name: "backslash", endpoint: `https://restored:2379\other`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			receipt := filepath.Join(dir, "semantic-receipt.json")
			env := []string{
				"ENDPOINT=" + tc.endpoint,
				"WITNESS_FILE=" + filepath.Join(dir, "witness.jsonl"),
				"SNAPSHOT_RECEIPT_FILE=" + filepath.Join(dir, "snapshot.json"),
				"RESTORE_RECEIPT_FILE=" + filepath.Join(dir, "restore.json"),
				"RESTORE_MANIFEST_FILE=" + filepath.Join(dir, "manifest.json"),
				"SEMANTIC_RECEIPT_FILE=" + receipt,
				"VERIFY_PREFIX=/__kubebrain/cold-restore-verify/test",
			}

			out, err := runProductionScriptCommand(t, "../backup/cold-restore-verify.sh", env)
			require.Error(t, err, string(out))
			require.Contains(t, string(out), "ENDPOINT contains unsupported characters")
			require.NoFileExists(t, receipt)
		})
	}
}

func TestLogicalObjectWrapperRejectsUnsafeS3Endpoint(t *testing.T) {
	for _, tc := range []struct {
		name     string
		endpoint string
	}{
		{name: "control character", endpoint: "https://s3.example\nother"},
		{name: "DEL", endpoint: "https://s3.example\x7fother"},
		{name: "quote", endpoint: `https://s3.example"other`},
		{name: "backslash", endpoint: `https://s3.example\other`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			receipt := filepath.Join(dir, "receipt.json")
			env := []string{
				"S3_ENDPOINT=" + tc.endpoint,
				"ACTION=usage",
				"OBJECT_STORE_ID=store-a",
				"S3_BUCKET=backups",
				"AWS_REGION=us-east-1",
				"AWS_ACCESS_KEY_ID=access",
				"AWS_SECRET_ACCESS_KEY=secret",
				"USAGE_PREFIX=instance-a/",
				"ALLOWED_FORMATS_JSON=[]",
				"RECEIPT_OUTPUT=" + receipt,
			}

			out, err := runProductionScriptCommand(t, "../backup/logical-object.sh", env)
			require.Error(t, err, string(out))
			require.Contains(t, string(out), "S3_ENDPOINT contains unsupported characters")
			require.NoFileExists(t, receipt)
		})
	}
}
