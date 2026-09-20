package imageprepull

import (
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestReleaseEvidenceFormatterRoundTrip(t *testing.T) {
	index, reviewed := releaseFixture(t)
	image, data := releaseBytes(t, index)
	path := filepath.Join(t.TempDir(), "index.json")
	require.NoError(t, os.WriteFile(path, data, 0600))
	wanted := ReleaseIdentity{Source: strings.Repeat("a", 40), RunID: "18446744073709551615", RunAttempt: "2", Image: image}
	raw, err := exec.CommandContext(t.Context(), "bash", "../../../../build/image-release-evidence.sh", image, path, wanted.Source, "fivetime/kubebrain", wanted.RunID, wanted.RunAttempt).Output()
	require.NoError(t, err)
	got, err := ParseReleaseEvidence(raw, data, wanted)
	require.NoError(t, err)
	require.Equal(t, wanted.RunID, got.RunID)
	require.Equal(t, reviewed, got.Platforms)
	for _, mode := range []string{"unknown", "duplicate", "case-alias", "source", "run", "attempt", "workflow", "repository", "image", "hash", "platform", "index-bytes", "version", "numeric-id", "null", "trailing", "oversize", "bad-expected"} {
		t.Run(mode, func(t *testing.T) {
			var object map[string]any
			require.NoError(t, json.Unmarshal(raw, &object))
			changedIndex := append([]byte(nil), data...)
			expect := wanted
			switch mode {
			case "unknown":
				object["approved"] = true
			case "source":
				object["source"] = strings.Repeat("b", 40)
			case "run":
				object["run_id"] = "1"
			case "attempt":
				object["run_attempt"] = "1"
			case "workflow":
				object["workflow"] = ".github/workflows/other.yml"
			case "repository":
				object["repository"] = "other/kubebrain"
			case "image":
				object["image"] = "ghcr.io/fivetime/kubebrain:dbaas"
			case "hash":
				object["index_sha256"] = strings.Repeat("c", 64)
			case "platform":
				object["platforms"].(map[string]any)["linux/amd64"] = reviewed["linux/arm64"]
			case "index-bytes":
				changedIndex = append(changedIndex, '\n')
			case "version":
				object["schema_version"] = 2
			case "numeric-id":
				object["run_id"] = 123
			case "bad-expected":
				expect.RunAttempt = "02"
			}
			input, err := json.Marshal(object)
			require.NoError(t, err)
			switch mode {
			case "duplicate":
				input = append([]byte(`{"schema_version":1,`), input[1:]...)
			case "case-alias":
				input = append([]byte(`{"SOURCE":"ignored",`), input[1:]...)
			case "null":
				input = []byte("null")
			case "trailing":
				input = append(input, []byte(" {}")...)
			case "oversize":
				input = []byte(strings.Repeat(" ", 32<<10+1))
			}
			result, err := ParseReleaseEvidence(input, changedIndex, expect)
			require.Error(t, err)
			require.Equal(t, ReleaseEvidence{}, result)
		})
	}
}
