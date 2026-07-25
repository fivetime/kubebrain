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
