package build_test

import (
	"context"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"sigs.k8s.io/yaml"
)

func TestImagePlatformDigestSelection(t *testing.T) {
	_, err := exec.LookPath("jq")
	require.NoError(t, err, "image validation requires jq")
	amdDigest := "sha256:" + strings.Repeat("a", 64)
	armDigest := "sha256:" + strings.Repeat("b", 64)
	const manifestType = "application/vnd.oci.image.manifest.v1+json"
	descriptor := func(osName, arch string, digest any, mediaType string) map[string]any {
		return map[string]any{"platform": map[string]string{"os": osName, "architecture": arch}, "digest": digest, "mediaType": mediaType}
	}
	amd := descriptor("linux", "amd64", amdDigest, manifestType)
	arm := descriptor("linux", "arm64", armDigest, manifestType)
	attestation := descriptor("unknown", "unknown", "sha256:"+strings.Repeat("c", 64), manifestType)
	for _, tc := range []struct {
		name, arch, want string
		manifests        []any
	}{
		{"amd64 with attestations", "amd64", amdDigest, []any{arm, attestation, amd}},
		{"arm64 with attestations", "arm64", armDigest, []any{amd, arm, attestation}},
		{"docker v2 manifest", "amd64", amdDigest, []any{descriptor("linux", "amd64", amdDigest, "application/vnd.docker.distribution.manifest.v2+json")}},
		{"missing architecture", "arm64", "", []any{amd, attestation}},
		{"duplicate architecture", "arm64", "", []any{arm, arm}},
		{"wrong OS", "arm64", "", []any{descriptor("windows", "arm64", armDigest, manifestType)}},
		{"nested index", "arm64", "", []any{descriptor("linux", "arm64", armDigest, "application/vnd.oci.image.index.v1+json")}},
		{"null digest", "arm64", "", []any{descriptor("linux", "arm64", nil, manifestType)}},
		{"numeric digest", "arm64", "", []any{descriptor("linux", "arm64", 123, manifestType)}},
		{"short digest", "arm64", "", []any{descriptor("linux", "arm64", "sha256:abc", manifestType)}},
		{"trailing newline", "arm64", "", []any{descriptor("linux", "arm64", armDigest+"\n", manifestType)}},
		{"tag instead of digest", "arm64", "", []any{descriptor("linux", "arm64", "image:latest", manifestType)}},
		{"unsupported architecture", "s390x", "", []any{amd, arm}},
		{"empty index", "amd64", "", []any{}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			data, err := json.Marshal(map[string]any{"manifests": tc.manifests})
			require.NoError(t, err)
			runImagePlatformSelection(t, string(data), tc.arch, tc.want)
		})
	}
	for _, data := range []string{"", "{", "{}", "null", `{"manifests":[]} {"manifests":[]}`} {
		t.Run("invalid document "+data, func(t *testing.T) {
			runImagePlatformSelection(t, data, "amd64", "")
		})
	}
}

func runImagePlatformSelection(t *testing.T, data, arch, want string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "index.json")
	require.NoError(t, os.WriteFile(path, []byte(data), 0600))
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, "bash", "image-platform-digest.sh", path, arch)
	output, err := cmd.Output()
	if want == "" {
		require.Error(t, err, "invalid selection must fail closed")
		require.Empty(t, output, "invalid selection must not emit a reference")
	} else {
		require.NoError(t, err)
		require.Equal(t, want+"\n", string(output))
	}
}

func TestImagePlatformWorkflowUsesChildDigests(t *testing.T) {
	data, err := os.ReadFile("../.github/workflows/image.yml")
	require.NoError(t, err)
	var workflow struct {
		Jobs map[string]struct {
			Steps []struct{ Name, Run string }
		}
	}
	require.NoError(t, yaml.Unmarshal(data, &workflow))
	var verify, promote string
	selectionTest, publish := -1, -1
	for i, step := range workflow.Jobs["build-and-push"].Steps {
		switch step.Name {
		case "Verify published test image":
			verify = step.Run
		case "Promote verified image to dbaas":
			promote = step.Run
		case "Verify runtime manifest selection":
			selectionTest = i
			require.Contains(t, step.Run, "go test ./build -run '^TestImagePlatform'")
		case "Build and push TiKV test image":
			publish = i
		}
	}
	require.NotEmpty(t, verify)
	require.NotEmpty(t, promote)
	require.GreaterOrEqual(t, selectionTest, 0)
	require.Greater(t, publish, selectionTest)
	require.Contains(t, verify, `amd64_digest="$(bash build/image-platform-digest.sh "$manifest" amd64)"`)
	require.Contains(t, verify, `platform_digest="$(bash build/image-platform-digest.sh "$manifest" "$arch")"`)
	require.Contains(t, verify, `platform_reference="${{ steps.vars.outputs.image }}@$platform_digest"`)
	require.Contains(t, verify, `docker pull --platform "linux/$arch" "$platform_reference"`)
	require.Contains(t, verify, `--entrypoint /usr/local/bin/kube-brain "$platform_reference" version`)
	require.Contains(t, verify, `--entrypoint /usr/local/bin/kubectl "$platform_reference" version --client`)
	// Containers execute child manifests. The index is only queried or passed
	// as data to the offline identity checker, never used to select an image.
	for _, line := range strings.Split(verify, "\n") {
		if strings.Contains(line, `"$reference"`) {
			require.True(t, strings.Contains(line, `docker buildx imagetools inspect --raw "$reference"`) ||
				strings.Contains(line, `--mode=verify-release --index-file=/release-index.json --image="$reference"`) ||
				strings.Contains(line, `jq -e --arg image "$reference"`), "unexpected index use: %s", line)
		}
	}
	require.Contains(t, promote, `reference="${{ steps.vars.outputs.image }}@${{ steps.build.outputs.digest }}"`)
	require.Contains(t, promote, `--tag "${{ steps.vars.outputs.image }}:dbaas" "$reference"`)
}
