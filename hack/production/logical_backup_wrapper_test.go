package production_test

import (
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestLogicalBackupWrappersRejectUnsafeEndpoint(t *testing.T) {
	for _, tc := range []struct {
		name     string
		endpoint string
	}{
		{name: "control character", endpoint: "https://service:2379\nother"},
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

func TestColdRestoreVerifyWrapperRejectsUnsafeEndpoint(t *testing.T) {
	for _, tc := range []struct {
		name     string
		endpoint string
	}{
		{name: "control character", endpoint: "https://restored:2379\nother"},
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
