package build_test

import (
	"os"
	"path/filepath"
	"regexp"
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

func TestWorkflowActionsArePinnedAndCheckoutDropsCredentials(t *testing.T) {
	actionPattern := regexp.MustCompile(`(?m)^\s*uses:\s+([^#\s]+)`)
	pinnedActionPattern := regexp.MustCompile(`^[^@\s]+@[0-9a-f]{40}$`)

	for _, path := range []string{
		"../.github/workflows/ci.yml",
		"../.github/workflows/docker-image.yml",
		"../.github/workflows/integration.yml",
	} {
		t.Run(path, func(t *testing.T) {
			workflow, err := os.ReadFile(path)
			require.NoError(t, err)
			content := string(workflow)
			actions := actionPattern.FindAllStringSubmatch(content, -1)
			require.NotEmpty(t, actions)
			for _, action := range actions {
				require.Regexp(t, pinnedActionPattern, action[1])
			}
			require.Equal(t,
				strings.Count(content, "actions/checkout@"),
				strings.Count(content, "persist-credentials: false"))
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

func TestCIScansEveryGoModuleForReachableVulnerabilities(t *testing.T) {
	workflow, err := os.ReadFile("../.github/workflows/ci.yml")
	require.NoError(t, err)
	content := string(workflow)

	require.Contains(t, content, "go run golang.org/x/vuln/cmd/govulncheck@v1.6.0 ./...")
	moduleDirs := nestedGoModuleDirs(t)
	for _, moduleDir := range moduleDirs {
		require.Contains(t, content, "cd "+moduleDir+" &&", moduleDir)
	}
	require.Equal(t, len(moduleDirs)+1, strings.Count(content,
		"go run golang.org/x/vuln/cmd/govulncheck@v1.6.0 ./..."))
}

func TestCICompilesAndTestsEveryNestedGoModule(t *testing.T) {
	workflow, err := os.ReadFile("../.github/workflows/ci.yml")
	require.NoError(t, err)
	content := string(workflow)
	moduleDirs := nestedGoModuleDirs(t)

	for _, moduleDir := range moduleDirs {
		if moduleDir == "hack/etcd-client-compat" {
			require.Contains(t, content, "working-directory: hack/etcd-client-compat")
			continue
		}
		require.Contains(t, content, "            "+moduleDir+"\n", moduleDir)
	}
	for _, command := range []string{
		"go build -o \"$RUNNER_TEMP/nested-go-build/$module/\" ./...",
		"go vet ./... && go test ./...",
	} {
		require.Contains(t, content, command)
	}
}

func nestedGoModuleDirs(t *testing.T) []string {
	t.Helper()
	var moduleDirs []string
	require.NoError(t, filepath.WalkDir("..", func(path string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() && (entry.Name() == ".git" || entry.Name() == "vendor") {
			return filepath.SkipDir
		}
		if entry.Name() != "go.mod" || path == "../go.mod" {
			return nil
		}
		moduleDir, err := filepath.Rel("..", filepath.Dir(path))
		if err != nil {
			return err
		}
		moduleDirs = append(moduleDirs, filepath.ToSlash(moduleDir))
		return nil
	}))
	require.NotEmpty(t, moduleDirs)
	return moduleDirs
}

func TestReleaseWorkflowPublishesVerifiedMultiPlatformImage(t *testing.T) {
	workflow, err := os.ReadFile("../.github/workflows/docker-image.yml")
	require.NoError(t, err)
	content := string(workflow)

	for _, required := range []string{
		"uses: docker/setup-qemu-action@",
		"uses: docker/setup-buildx-action@",
		"uses: docker/build-push-action@",
		"platforms: linux/amd64,linux/arm64",
		"cache-from: type=gha",
		"cache-to: type=gha,mode=max",
		"KUBEBRAIN_VERSION=${{ steps.vars.outputs.version }}",
		"KUBEBRAIN_GIT_SHA=${{ steps.vars.outputs.revision }}",
		"KUBEBRAIN_BUILD_DATE=${{ steps.vars.outputs.created }}",
		"provenance: mode=max",
		"sbom: true",
		"docker buildx imagetools inspect --raw",
		`sort == ["amd64", "arm64"]`,
		`steps.vars.outputs.image }}@${{ steps.build.outputs.digest`,
		"Promote verified image to latest",
		"docker buildx imagetools create",
		"Verify immutable release source",
		`ref: ${{ github.event.inputs.source_ref || github.sha }}`,
		`test "$revision" = "$REQUESTED_REVISION"`,
		`git merge-base --is-ancestor "$revision" origin/main`,
		"branches:\n      - main",
	} {
		require.Contains(t, content, required)
	}
	require.NotContains(t, content, "      - master")
	tagsStart := strings.Index(content, "          tags: |")
	require.NotEqual(t, -1, tagsStart)
	cacheStart := strings.Index(content[tagsStart:], "          cache-from:")
	require.NotEqual(t, -1, cacheStart)
	require.NotContains(t, content[tagsStart:tagsStart+cacheStart], ":latest")
	require.Less(t,
		strings.Index(content, "uses: docker/setup-qemu-action@"),
		strings.Index(content, "uses: docker/build-push-action@"))
	require.Less(t,
		strings.Index(content, "uses: docker/build-push-action@"),
		strings.Index(content, "Verify published multi-platform index"))
	require.Less(t,
		strings.Index(content, "Verify published multi-platform index"),
		strings.Index(content, "Promote verified image to latest"))
}

func TestIntegrationToolDownloadsAreVersionedAndVerified(t *testing.T) {
	workflow, err := os.ReadFile("../.github/workflows/integration.yml")
	require.NoError(t, err)
	content := string(workflow)

	for _, required := range []string{
		`GO_VERSION: "1.26.5"`,
		"kindest/node:v1.36.1@sha256:3489c7674813ba5d8b1a9977baea8a6e553784dab7b84759d1014dbd78f7ebd5",
		"kind/releases/download/v0.32.0/kind-linux-amd64",
		"50030de23cf40a18505f20426f6a8506bedf13c6e509244bd1fa9463721b0f54",
		"release/v1.36.1/bin/linux/amd64/kubectl",
		"629d3f410e09bf49b64ae7079f7f0bda1191efed311f7d37fdbab0ad5b0ec2b7",
		"helm-v3.18.4-linux-amd64.tar.gz",
		"f8180838c23d7c7d797b208861fecb591d9ce1690d8704ed1e4cb8e2add966c1",
		"curl --proto '=https' --tlsv1.2",
		"sha256sum -c -",
	} {
		require.Contains(t, content, required)
	}
	require.Equal(t, 3, strings.Count(content, "sha256sum -c -"))
	require.NotContains(t, content, "raw.githubusercontent.com/helm/helm/main")
}
