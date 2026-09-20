package build_test

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"sigs.k8s.io/yaml"
)

func TestImagePlatformReleaseEvidence(t *testing.T) {
	for _, mode := range []string{"valid", "index-changed", "tag", "source", "repository", "run-id", "attempt", "missing-arch", "same-digest", "symlink"} {
		t.Run(mode, func(t *testing.T) {
			amd, arm := "sha256:"+strings.Repeat("a", 64), "sha256:"+strings.Repeat("b", 64)
			if mode == "same-digest" {
				arm = amd
			}
			data := []byte(fmt.Sprintf(`{"schemaVersion":2,"mediaType":"application/vnd.oci.image.index.v1+json","manifests":[{"mediaType":"application/vnd.oci.image.manifest.v1+json","digest":%q,"platform":{"os":"linux","architecture":"amd64"}},{"mediaType":"application/vnd.oci.image.manifest.v1+json","digest":%q,"platform":{"os":"linux","architecture":"arm64"}}]}`, amd, arm))
			if mode == "missing-arch" {
				data = []byte(strings.Replace(string(data), "arm64", "s390x", 1))
			}
			image := fmt.Sprintf("ghcr.io/fivetime/kubebrain@sha256:%x", sha256.Sum256(data))
			path := filepath.Join(t.TempDir(), "index.json")
			if mode == "index-changed" {
				data = append(data, '\n')
			}
			require.NoError(t, os.WriteFile(path, data, 0600))
			args := []string{"image-release-evidence.sh", image, path, strings.Repeat("c", 40), "fivetime/kubebrain", "18446744073709551615", "2"}
			switch mode {
			case "tag":
				args[1] = "ghcr.io/fivetime/kubebrain:dbaas"
			case "source":
				args[3] = "short"
			case "repository":
				args[4] = "other/kubebrain"
			case "run-id":
				args[5] = "01"
			case "attempt":
				args[6] = "0"
			case "symlink":
				link := path + ".link"
				require.NoError(t, os.Symlink(path, link))
				args[2] = link
			}
			out, err := exec.CommandContext(t.Context(), "bash", args...).Output()
			if mode != "valid" {
				require.Error(t, err)
				require.Empty(t, out)
				return
			}
			require.NoError(t, err)
			var receipt map[string]any
			require.NoError(t, json.Unmarshal(out, &receipt))
			require.Equal(t, image, receipt["image"])
			require.Equal(t, args[3], receipt["source"])
			require.Equal(t, args[5], receipt["run_id"], "IDs must remain exact decimal strings")
			require.Equal(t, args[6], receipt["run_attempt"])
			require.Equal(t, strings.Split(image, "sha256:")[1], receipt["index_sha256"])
			require.Equal(t, map[string]any{"linux/amd64": amd, "linux/arm64": arm}, receipt["platforms"])
		})
	}
}

func TestImagePlatformReleaseEvidenceWorkflowOrdering(t *testing.T) {
	data, err := os.ReadFile("../.github/workflows/image.yml")
	require.NoError(t, err)
	var workflow struct {
		Jobs map[string]struct {
			Steps []struct {
				Name, Run, Uses, If string
				With                map[string]any
				ContinueOnError     any `json:"continue-on-error"`
			}
		}
	}
	require.NoError(t, yaml.Unmarshal(data, &workflow))
	verify, promote, prepare, upload := -1, -1, -1, -1
	for i, step := range workflow.Jobs["build-and-push"].Steps {
		switch step.Name {
		case "Verify published test image":
			verify = i
		case "Promote verified image to dbaas":
			promote = i
		case "Prepare verified release evidence":
			prepare = i
			require.Empty(t, step.If)
			require.Nil(t, step.ContinueOnError)
			require.Contains(t, step.Run, "bash build/image-release-evidence.sh")
			require.Contains(t, step.Run, `"$GITHUB_REPOSITORY" "$GITHUB_RUN_ID" "$GITHUB_RUN_ATTEMPT"`)
			require.Contains(t, step.Run, `cp "$RUNNER_TEMP/dbaas-${GITHUB_RUN_ID}-${GITHUB_RUN_ATTEMPT}-manifest.json" "$release_dir/index.json"`)
		case "Upload verified release evidence":
			upload = i
			require.Empty(t, step.If)
			require.Nil(t, step.ContinueOnError)
			require.Equal(t, "actions/upload-artifact@ea165f8d65b6e75b540449e92b4886f43607fa02", step.Uses)
			require.Equal(t, "error", step.With["if-no-files-found"])
			require.Equal(t, "dbaas-release-${{ github.run_id }}-${{ github.run_attempt }}", step.With["name"])
		}
	}
	require.GreaterOrEqual(t, verify, 0)
	require.Greater(t, promote, verify)
	require.Greater(t, prepare, promote)
	require.Greater(t, upload, prepare)
}
