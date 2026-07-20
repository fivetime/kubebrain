package build_test

import (
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"sigs.k8s.io/yaml"
)

func TestWorkflowsAreValidYAML(t *testing.T) {
	for _, path := range []string{
		"../.github/workflows/ci.yml",
		"../.github/workflows/docker-image.yml",
		"../.github/workflows/integration.yml",
	} {
		t.Run(path, func(t *testing.T) {
			content, err := os.ReadFile(path)
			require.NoError(t, err)
			var workflow any
			require.NoError(t, yaml.Unmarshal(content, &workflow))
		})
	}
}

func TestCIDockerBuildSuppliesRequiredMetadata(t *testing.T) {
	workflow, err := os.ReadFile("../.github/workflows/ci.yml")
	require.NoError(t, err)
	content := string(workflow)

	for _, required := range []string{
		`GO_VERSION: "1.26.5"`,
		`echo "revision=$(git rev-parse HEAD)"`,
		"--build-arg TARGETARCH=amd64",
		`--build-arg KUBEBRAIN_VERSION="${{ steps.image.outputs.version }}"`,
		`--build-arg KUBEBRAIN_GIT_SHA="${{ steps.image.outputs.revision }}"`,
		`--build-arg KUBEBRAIN_BUILD_DATE="${{ steps.image.outputs.created }}"`,
		`= "${{ steps.image.outputs.revision }}"`,
		`= "65532:65532"`,
		`= "v1.36.2"`,
	} {
		require.Contains(t, content, required)
	}
	require.Equal(t, 2, strings.Count(content, "--build-arg STORAGE="))
	require.Equal(t, 2, strings.Count(content, "--build-arg KUBEBRAIN_GIT_SHA="))
}

func TestReleaseWorkflowPublishesVerifiedMultiPlatformImage(t *testing.T) {
	workflow, err := os.ReadFile("../.github/workflows/docker-image.yml")
	require.NoError(t, err)
	content := string(workflow)

	for _, required := range []string{
		"uses: docker/setup-qemu-action@v3",
		"uses: docker/setup-buildx-action@v3",
		"uses: docker/build-push-action@v6",
		"platforms: linux/amd64,linux/arm64",
		"KUBEBRAIN_VERSION=${{ steps.vars.outputs.version }}",
		"KUBEBRAIN_GIT_SHA=${{ steps.vars.outputs.revision }}",
		"KUBEBRAIN_BUILD_DATE=${{ steps.vars.outputs.created }}",
		"provenance: mode=max",
		"sbom: true",
		"docker buildx imagetools inspect --raw",
		`sort == ["amd64", "arm64"]`,
		`steps.vars.outputs.image }}@${{ steps.build.outputs.digest`,
	} {
		require.Contains(t, content, required)
	}
	require.Less(t,
		strings.Index(content, "uses: docker/setup-qemu-action@v3"),
		strings.Index(content, "uses: docker/build-push-action@v6"))
	require.Less(t,
		strings.Index(content, "uses: docker/build-push-action@v6"),
		strings.Index(content, "Verify published multi-platform index"))
}
