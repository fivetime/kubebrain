package build_test

import (
	"os"
	"regexp"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"sigs.k8s.io/yaml"
)

func TestImagePrepullBuildAndRuntimeInventory(t *testing.T) {
	data, err := os.ReadFile("../Dockerfile")
	require.NoError(t, err)
	dockerfile := string(data)
	require.Contains(t, dockerfile, "go build -trimpath -o /src/bin/kubebrain-image-prepull ./hack/production/cmd/image-prepull")
	runtime := dockerfile[strings.LastIndex(dockerfile, "\nFROM "):]
	require.Contains(t, runtime, "COPY --from=build /src/bin/kubebrain-image-prepull /usr/local/bin/kubebrain-image-prepull")
	require.Contains(t, runtime, "USER 65532:65532")
	// Every Go binary copied into the final runtime must be covered by CI's
	// architecture inventory. Intermediate BR images are not the data plane.
	copies := regexp.MustCompile(`(?m)^COPY --from=build /src/bin/(\S+) /usr/local/bin/(\S+)$`).FindAllStringSubmatch(runtime, -1)
	require.Len(t, copies, 67)
	seen := map[string]bool{}
	for _, copy := range copies {
		require.Equal(t, copy[1], copy[2])
		require.False(t, seen[copy[2]], "duplicate runtime binary: %s", copy[2])
		seen[copy[2]] = true
	}
}

func TestImagePrepullWorkflowGatesPublishAndChecksShippedHelper(t *testing.T) {
	data, err := os.ReadFile("../.github/workflows/image.yml")
	require.NoError(t, err)
	var workflow struct {
		Jobs map[string]struct {
			Steps []struct {
				Name, Run       string
				ContinueOnError bool `json:"continue-on-error"`
			}
		}
	}
	require.NoError(t, yaml.Unmarshal(data, &workflow))
	check, publish, verify, promote := -1, -1, -1, -1
	for i, step := range workflow.Jobs["build-and-push"].Steps {
		switch step.Name {
		case "Verify isolated image preparation contracts":
			check = i
			require.False(t, step.ContinueOnError)
			require.Contains(t, step.Run, "set -euo pipefail")
			require.Contains(t, step.Run, "go test -race ./hack/production/internal/imageprepull ./hack/production/cmd/image-prepull -count=1 -timeout=5m")
			require.Contains(t, step.Run, "go test ./build -run '^TestImagePrepull'")
		case "Build and push TiKV test image":
			publish = i
		case "Verify published test image":
			verify = i
			require.False(t, step.ContinueOnError)
			require.Contains(t, step.Run, `for arch in amd64 arm64; do`)
			require.Contains(t, step.Run, `--platform "linux/$arch" --network=none --read-only`)
			require.Contains(t, step.Run, `--cap-drop=ALL --security-opt=no-new-privileges`)
			require.Contains(t, step.Run, `type=bind,src=$manifest,dst=/release-index.json,readonly`)
			require.Contains(t, step.Run, `--entrypoint /usr/local/bin/kubebrain-image-prepull "$platform_reference"`)
			require.Contains(t, step.Run, `--mode=verify-release --index-file=/release-index.json --image="$reference"`)
			require.Contains(t, step.Run, `.runtimeDigests == {"linux/amd64":[$amd64,($image | split("@")[1])],`)
			require.Contains(t, step.Run, `test "$go_binaries" -eq 67`)
			require.NotContains(t, step.Run, "--mode=prepare")
		case "Promote verified image to dbaas":
			promote = i
		}
	}
	require.GreaterOrEqual(t, check, 0)
	require.Greater(t, publish, check)
	require.Greater(t, verify, publish)
	require.Greater(t, promote, verify)
}
