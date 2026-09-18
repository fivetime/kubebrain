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
		"../.github/workflows/image.yml",
		"../.github/workflows/docker-image.yml",
		"../.github/workflows/integration.yml",
		"../.github/workflows/backend-integration.yml",
		"../.github/workflows/probe-regression.yml",
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
		"../.github/workflows/image.yml",
		"../.github/workflows/docker-image.yml",
		"../.github/workflows/integration.yml",
		"../.github/workflows/backend-integration.yml",
		"../.github/workflows/probe-regression.yml",
	} {
		t.Run(path, func(t *testing.T) {
			workflow, err := os.ReadFile(path)
			require.NoError(t, err)
			content := string(workflow)
			actions := actionPattern.FindAllStringSubmatch(content, -1)
			require.NotEmpty(t, actions)
			for _, action := range actions {
				if action[1] == "./.github/workflows/backend-integration.yml" {
					_, err := os.Stat("../.github/workflows/backend-integration.yml")
					require.NoError(t, err)
					continue // Repository-local reusable workflow uses the same commit.
				}
				require.Regexp(t, pinnedActionPattern, action[1])
			}
			require.Equal(t,
				strings.Count(content, "actions/checkout@"),
				strings.Count(content, "persist-credentials: false"))
		})
	}
}

func TestProbeRegressionCIExecutesUncachedRaceSuite(t *testing.T) {
	data, err := os.ReadFile("../.github/workflows/probe-regression.yml")
	require.NoError(t, err)
	var workflow struct {
		On map[string]struct {
			Branches []string `json:"branches"`
			Paths    []string `json:"paths"`
		} `json:"on"`
		Permissions map[string]string `json:"permissions"`
		Jobs        map[string]struct {
			If      string `json:"if"`
			RunsOn  string `json:"runs-on"`
			Timeout int    `json:"timeout-minutes"`
			Steps   []struct {
				Run             string `json:"run"`
				ContinueOnError bool   `json:"continue-on-error"`
			} `json:"steps"`
		} `json:"jobs"`
	}
	require.NoError(t, yaml.Unmarshal(data, &workflow))
	require.Len(t, workflow.On, 2)
	require.Contains(t, workflow.On, "push")
	require.Contains(t, workflow.On, "workflow_dispatch")
	require.Equal(t, []string{"dbaas"}, workflow.On["push"].Branches)
	for _, path := range []string{"hack/production/cmd/rollout-availability-probe/**", "hack/production/cmd/pod-log-capture/**", "hack/production/internal/**", "hack/scale-lab/**", "hack/etcd-client-compat/**", "hack/dev/create-apiserver-test-pki.sh", "pkg/**", "go.mod", "go.sum", "build/workflow_test.go", ".github/workflows/probe-regression.yml"} {
		require.Contains(t, workflow.On["push"].Paths, path)
	}
	require.Equal(t, map[string]string{"contents": "read"}, workflow.Permissions)
	require.Len(t, workflow.Jobs, 1)
	job := workflow.Jobs["probe-tests"]
	require.Equal(t, "github.ref == 'refs/heads/dbaas'", job.If)
	require.Equal(t, "self-hosted", job.RunsOn)
	require.Equal(t, 30, job.Timeout)
	var commands string
	for _, step := range job.Steps {
		require.False(t, step.ContinueOnError)
		if step.Run != "" {
			require.True(t, strings.HasPrefix(step.Run, "set -euo pipefail\n"))
			commands += step.Run
		}
	}
	require.Contains(t, commands, "go test -race -count=1 ./build\n")
	require.Contains(t, commands, "cd hack/etcd-client-compat\n")
	require.Contains(t, commands, "command -v bash jq openssl\n")
	require.Contains(t, string(data), "repository: etcd-io/etcd\n")
	require.Contains(t, string(data), "ref: 5cd9f4ee13801e18825d661e5005ae599460bc3a\n")
	require.Contains(t, commands, "test \"$(git -C .ci-reference-etcd rev-parse HEAD)\" = 5cd9f4ee13801e18825d661e5005ae599460bc3a\n")
	require.Contains(t, commands, "for module in api cache client/pkg client/v3 server; do\n")
	require.Contains(t, commands, "go mod edit \"-replace=$module_path=$GITHUB_WORKSPACE/.ci-reference-etcd/$module\"\n")
	require.Contains(t, commands, "go mod tidy\n")
	require.Contains(t, commands, "go mod verify\n")
	require.Contains(t, commands, "go test -race -count=1 -timeout=3m -v . -run '^Test(ControlPlane|Ephemeral.*PKI)'\n")
	require.Contains(t, commands, "go vet ./hack/production/cmd/rollout-availability-probe\n")
	require.Contains(t, commands, "go vet ./pkg/server/service/etcdproxy\n")
	require.Contains(t, commands, "go vet ./pkg/server/service/leader\n")
	require.Contains(t, commands, "go vet ./pkg/backend/election\n")
	require.Contains(t, commands, "go test -race -count=1 -timeout=3m -v ./pkg/backend/election\n")
	require.Contains(t, commands, "go test -race -count=1 -timeout=3m -v ./pkg/server/service/leader\n")
	require.Contains(t, commands, "go vet ./pkg/server\n")
	require.Contains(t, commands, "go vet ./pkg/endpoint ./pkg/transportidentity\n")
	require.Contains(t, commands, "go test -race -count=1 -timeout=3m -v ./pkg/endpoint ./pkg/transportidentity\n")
	require.Contains(t, commands, "go test -race -count=1 -timeout=3m -v ./pkg/server\n")
	require.Contains(t, workflow.On["push"].Paths, "cmd/option/**")
	require.Contains(t, commands, "go vet ./cmd/option\n")
	require.Contains(t, workflow.On["push"].Paths, "hack/production/cmd/peer-retirement-scope/**")
	require.Contains(t, commands, "go vet ./hack/production/cmd/peer-retirement-scope\n")
	require.Contains(t, commands, "go test -race -count=1 -timeout=2m -v ./hack/production/cmd/peer-retirement-scope\n")
	require.Contains(t, workflow.On["push"].Paths, "hack/production/cmd/peer-retirement-test-pki/**")
	require.Contains(t, commands, "go vet ./hack/production/cmd/peer-retirement-test-pki\n")
	require.Contains(t, commands, "go test -race -count=1 -timeout=2m -v ./hack/production/cmd/peer-retirement-test-pki\n")
	require.Contains(t, commands, "go test -race -count=1 -timeout=3m -v ./cmd/option\n")
	require.Contains(t, commands, "go vet ./pkg/server/service/revision\n")
	require.Contains(t, commands, "go test -race -count=1 -timeout=3m -v ./pkg/server/service/revision\n")
	require.Contains(t, commands, "go vet ./hack/production/cmd/pod-log-capture\n")
	require.Contains(t, workflow.On["push"].Paths, "hack/production/cmd/lease-term-probe/**")
	require.Contains(t, commands, "go vet ./hack/production/cmd/lease-term-probe\n")
	require.Contains(t, commands, "go test -race -count=1 -timeout=2m -v ./hack/production/cmd/lease-term-probe\n")
	require.Contains(t, commands, "go test -race -count=1 -timeout=2m -v ./hack/production/cmd/pod-log-capture\n")
	require.Contains(t, commands, "go test -race -count=1 -timeout=5m -v ./pkg/server/service/etcdproxy\n")
	require.Contains(t, commands, "go vet ./pkg/server/etcd\n")
	require.Contains(t, commands, "go test -count=1 -timeout=10m -v ./pkg/server/etcd\n")
	require.Contains(t, commands, "go test -race -count=1 -timeout=8m -v ./pkg/server/etcd -run '(ReadBarrier|Watch|Prev[Kk][Vv]|PrevHint|RevKeyCache|CachePromotion|MetadataSingleflight|PeriodicProgress)'\n")
	require.Contains(t, commands, "go test -race -count=1 -timeout=3m -v ./pkg/server/etcd -run '^TestMaintenanceSnapshotCancellation'\n")
	require.Contains(t, commands, "go test -race -count=1 -timeout=8m -v ./pkg/server/etcd -run '(Auth|JWT|PutPathsRejectGuardChangedAtCommit)'\n")
	require.Contains(t, commands, "go test -race -count=1 -timeout=8m -v ./pkg/server/etcd -run '(Lease|Revoke|Expiry|Checkpoint|Attachment)'\n")
	require.Contains(t, commands, "go test -race -count=1 -timeout=20m -v ./hack/production/cmd/rollout-availability-probe\n")
	require.NotContains(t, commands, "|| true")
	require.NotContains(t, commands, "kubectl")
}

func TestCIDockerBuildSuppliesRequiredMetadata(t *testing.T) {
	workflow, err := os.ReadFile("../.github/workflows/ci.yml")
	require.NoError(t, err)
	content := string(workflow)

	for _, required := range []string{
		`GO_VERSION: "1.26.8"`,
		`echo "revision=$(git rev-parse HEAD)"`,
		"--build-arg TARGETARCH=amd64",
		`--build-arg KUBEBRAIN_VERSION="${{ steps.image.outputs.version }}"`,
		`--build-arg KUBEBRAIN_GIT_SHA="${{ steps.image.outputs.revision }}"`,
		`--build-arg KUBEBRAIN_BUILD_DATE="${{ steps.image.outputs.created }}"`,
		`= "${{ steps.image.outputs.revision }}"`,
		`= "65532:65532"`,
		`= "v1.36.4+kubebrain"`,
	} {
		require.Contains(t, content, required)
	}
	require.Equal(t, 2, strings.Count(content, "--build-arg STORAGE="))
	require.Equal(t, 2, strings.Count(content, "--build-arg KUBEBRAIN_GIT_SHA="))
}

func TestBackendCIExecutesIsolatedRealProtocolGate(t *testing.T) {
	data, err := os.ReadFile("../.github/workflows/backend-integration.yml")
	require.NoError(t, err)
	var workflow struct {
		Jobs map[string]struct {
			If     string `json:"if"`
			RunsOn string `json:"runs-on"`
			Steps  []struct {
				Run             string `json:"run"`
				ContinueOnError bool   `json:"continue-on-error"`
			} `json:"steps"`
		} `json:"jobs"`
	}
	require.NoError(t, yaml.Unmarshal(data, &workflow))
	job, ok := workflow.Jobs["real-protocol"]
	require.True(t, ok)
	require.Equal(t, "self-hosted", job.RunsOn)
	require.Equal(t, "github.event_name != 'pull_request' || github.event.pull_request.head.repo.full_name == github.repository", job.If)
	var commands string
	for _, step := range job.Steps {
		require.False(t, step.ContinueOnError)
		if step.Run != "" {
			require.True(t, strings.HasPrefix(step.Run, "set -euo pipefail\n"))
			commands += step.Run
		}
	}
	for _, command := range []string{
		"go test -race ./build\n",
		"go test -race -count=1 -timeout=5m ./pkg/backend\n",
		"go vet ./pkg/backend\n",
		"go vet ./pkg/storage/tikv\n",
		"bash hack/backend-integration/run-real-local.sh --allow-local-containers\n",
		"bash hack/backend-integration/run-real-local.sh --allow-local-containers --race\n",
		"bash hack/backend-integration/run-real-local-interruption-test.sh --allow-local-containers\n",
	} {
		require.Contains(t, commands, command)
	}
	// Pull exactly the runner's immutable images, through the same local socket.
	entry, err := os.ReadFile("../hack/backend-integration/run-real-local.sh")
	require.NoError(t, err)
	images := regexp.MustCompile(`(?m)^(?:pd|tikv)_image=(pingcap/[^\s]+@sha256:[a-f0-9]{64})$`).FindAllStringSubmatch(string(entry), -1)
	require.Len(t, images, 2)
	for _, image := range images {
		require.Contains(t, commands, "docker --host unix:///var/run/docker.sock pull "+image[1]+"\n")
	}
	require.NotContains(t, commands, "|| true")
}

func TestMainCIReusesRealProtocolGateAfterMockRetirement(t *testing.T) {
	mainCI, err := os.ReadFile("../.github/workflows/ci.yml")
	require.NoError(t, err)
	var main struct {
		Jobs map[string]struct {
			Uses string `json:"uses"`
		} `json:"jobs"`
	}
	require.NoError(t, yaml.Unmarshal(mainCI, &main))
	require.Equal(t, "./.github/workflows/backend-integration.yml", main.Jobs["backend-protocol"].Uses)
	backendCI, err := os.ReadFile("../.github/workflows/backend-integration.yml")
	require.NoError(t, err)
	var backend struct {
		On map[string]any `json:"on"`
	}
	require.NoError(t, yaml.Unmarshal(backendCI, &backend))
	require.Contains(t, backend.On, "workflow_call")
	// The direct workflow and its caller must not cancel each other's jobs.
	require.Contains(t, string(backendCI), "group: backend-protocol-${{ github.workflow }}-${{ github.ref }}")
	for _, content := range []string{string(mainCI), string(backendCI)} {
		require.NotContains(t, content, "run-onepc.sh")
		require.NotContains(t, content, "hack/backend-integration/go.sum")
	}
	_, err = os.Stat("../hack/backend-integration/go.mod")
	require.ErrorIs(t, err, os.ErrNotExist)
}

func TestCIScansEveryGoModuleForReachableVulnerabilities(t *testing.T) {
	workflow, err := os.ReadFile("../.github/workflows/ci.yml")
	require.NoError(t, err)
	content := string(workflow)

	require.Contains(t, content, "go run golang.org/x/vuln/cmd/govulncheck@v1.6.0 -test ./...")
	moduleDirs := nestedGoModuleDirs(t)
	for _, moduleDir := range moduleDirs {
		require.Contains(t, content, "cd "+moduleDir+" &&", moduleDir)
	}
	require.Equal(t, len(moduleDirs)+1, strings.Count(content,
		"go run golang.org/x/vuln/cmd/govulncheck@v1.6.0 "))
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

func TestDBaaSImageWorkflowUsesSelfHostedAndIsolatesTestTags(t *testing.T) {
	workflow, err := os.ReadFile("../.github/workflows/image.yml")
	require.NoError(t, err)
	content := string(workflow)
	for _, required := range []string{
		"branches: [dbaas]",
		"if: github.ref == 'refs/heads/dbaas'",
		"runs-on: self-hosted",
		"persist-credentials: false",
		"file: Dockerfile",
		"platforms: linux/amd64,linux/arm64",
		"STORAGE=tikv",
		"KUBEBRAIN_VERSION=${{ steps.vars.outputs.version }}",
		"KUBEBRAIN_GIT_SHA=${{ steps.vars.outputs.revision }}",
		"KUBEBRAIN_BUILD_DATE=${{ steps.vars.outputs.created }}",
		"dbaas-${{ steps.vars.outputs.revision }}",
		"provenance: mode=max",
		"sbom: true",
		`grep -F -- "Storage:" | grep -F -- "TiKV"`,
		"Verify published test image",
		"Promote verified image to dbaas",
		`--tag "${{ steps.vars.outputs.image }}:dbaas"`,
	} {
		require.Contains(t, content, required)
	}
	require.NotContains(t, content, "pull_request:")
	require.NotContains(t, content, ":latest")
	require.Less(t, strings.Index(content, "Verify published test image"),
		strings.Index(content, "Promote verified image to dbaas"))
}

func TestDBaaSImageWorkflowSeparatesRegistryCacheFromRelease(t *testing.T) {
	data, err := os.ReadFile("../.github/workflows/image.yml")
	require.NoError(t, err)
	var workflow struct {
		Jobs map[string]struct {
			Steps []struct {
				Name, Uses      string
				With            map[string]any
				ContinueOnError any `json:"continue-on-error"`
			}
		}
	}
	require.NoError(t, yaml.Unmarshal(data, &workflow))
	const cacheRef = "${{ steps.vars.outputs.image }}:buildcache-dbaas"
	login, build, verify, promote := -1, -1, -1, -1
	for i, step := range workflow.Jobs["build-and-push"].Steps {
		if strings.HasPrefix(step.Uses, "docker/login-action@") {
			login = i
		}
		if strings.HasPrefix(step.Uses, "docker/build-push-action@") {
			build = i
			require.Nil(t, step.ContinueOnError, "cache migration must not suppress build/publish failures")
			export, ok := step.With["cache-to"].(string)
			require.True(t, ok)
			fields := map[string]string{}
			for _, field := range strings.Split(export, ",") {
				pair := strings.SplitN(strings.TrimSpace(field), "=", 2)
				require.Len(t, pair, 2)
				require.NotContains(t, fields, pair[0], "cache options must not be duplicated")
				fields[pair[0]] = pair[1]
			}
			require.Equal(t, map[string]string{
				"type": "registry", "ref": cacheRef, "mode": "max", "oci-mediatypes": "true", "image-manifest": "true",
			}, fields)
			imports, ok := step.With["cache-from"].(string)
			require.True(t, ok)
			require.Equal(t, []string{"type=registry,ref=" + cacheRef, "type=gha,scope=kubebrain-dbaas"},
				strings.Split(strings.TrimSpace(imports), "\n"), "legacy GHA cache is read-only during migration")
			tags, ok := step.With["tags"].(string)
			require.True(t, ok)
			require.Equal(t, "${{ steps.vars.outputs.image }}:dbaas-${{ steps.vars.outputs.revision }}", strings.TrimSpace(tags))
			require.NotContains(t, tags, cacheRef, "build cache must never overwrite a release tag")
		}
		switch step.Name {
		case "Verify published test image":
			verify = i
			require.Nil(t, step.ContinueOnError)
		case "Promote verified image to dbaas":
			promote = i
			require.Nil(t, step.ContinueOnError)
		}
	}
	require.GreaterOrEqual(t, login, 0)
	require.Greater(t, build, login)
	require.Greater(t, verify, build)
	require.Greater(t, promote, verify)
}

func TestIntegrationToolDownloadsAreVersionedAndVerified(t *testing.T) {
	workflow, err := os.ReadFile("../.github/workflows/integration.yml")
	require.NoError(t, err)
	content := string(workflow)

	for _, required := range []string{
		`GO_VERSION: "1.26.8"`,
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
