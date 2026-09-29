package build_test

import (
	"bufio"
	"os"
	"regexp"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

var pinnedBaseImage = regexp.MustCompile(
	`(?m)^FROM (?:--platform=\$\{[A-Z]+\} )?[^\s@]+:[^\s@]+@sha256:[a-f0-9]{64}(?: AS [a-zA-Z0-9_-]+)?$`,
)

func TestDockerfilePinsEveryBaseImageByDigest(t *testing.T) {
	dockerfile, err := os.ReadFile("../Dockerfile")
	require.NoError(t, err)

	var fromLines []string
	for _, line := range strings.Split(string(dockerfile), "\n") {
		if strings.HasPrefix(line, "FROM ") {
			fromLines = append(fromLines, line)
		}
	}
	require.NotEmpty(t, fromLines)
	for _, line := range fromLines {
		require.Regexp(t, pinnedBaseImage, line,
			"base images must retain a readable version tag and an immutable sha256 digest")
	}
}

func TestDockerfilePinsEveryExplicitRuntimePackage(t *testing.T) {
	dockerfile, err := os.ReadFile("../Dockerfile")
	require.NoError(t, err)
	content := string(dockerfile)

	var installs [][]string
	for searchFrom := 0; ; {
		relativeStart := strings.Index(content[searchFrom:], "RUN apk add --no-cache")
		if relativeStart == -1 {
			break
		}
		start := searchFrom + relativeStart
		end := strings.Index(content[start:], "&& addgroup")
		require.NotEqual(t, -1, end)
		install := strings.ReplaceAll(content[start:start+end], "\\", "")
		installs = append(installs, strings.Fields(install)[4:])
		searchFrom = start + end
	}

	require.Equal(t, [][]string{
		{"bash=5.3.3-r1", "ca-certificates=20260909-r0", "coreutils=9.8-r1", "gcompat=1.1.0-r4", "jq=1.8.2-r0"},
		{"bash=5.3.3-r1", "ca-certificates=20260909-r0", "coreutils=9.8-r1", "gcompat=1.1.0-r4", "jq=1.8.2-r0"},
		{
			"bash=5.3.3-r1",
			"ca-certificates=20260909-r0",
			"coreutils=9.8-r1",
			"curl=8.22.0-r0",
			"etcd-ctl=3.6.10-r1",
			"jq=1.8.2-r0",
			"openssl=3.5.8-r0",
		},
	}, installs,
		"runtime package additions and upgrades must pin exact versions")
}

func TestDockerfileExecutesInfoExecutorRuntimeContract(t *testing.T) {
	dockerfile, err := os.ReadFile("../Dockerfile")
	require.NoError(t, err)
	content := string(dockerfile)
	require.Contains(t, content, "COPY hack/production/*.sh /opt/kubebrain/hack/production/")
	require.Contains(t, content, "RUN REQUIRE_INFO_EXECUTOR_BINARIES=true \\")
	require.Contains(t, content, "/opt/kubebrain/hack/production/validate-info-executor-runtime.sh")

	contract, err := os.ReadFile("../hack/production/validate-info-executor-runtime.sh")
	require.NoError(t, err)
	require.Contains(t, string(contract), "required_commands+=(kubectl kubebrain-operationctl kubebrain-operation-worker)")
}

func TestDockerignoreExcludesLocalBuildArtifacts(t *testing.T) {
	file, err := os.Open("../.dockerignore")
	require.NoError(t, err)
	defer file.Close()

	patterns := make(map[string]struct{})
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line != "" && !strings.HasPrefix(line, "#") {
			patterns[line] = struct{}{}
		}
	}
	require.NoError(t, scanner.Err())

	for _, pattern := range []string{
		".git",
		".dev",
		".claude",
		".idea",
		".vscode",
		"bin",
		"output",
		"coverage.out",
		"/kube*",
		"/loadgen",
		"/tikv-persistence-smoke",
	} {
		require.Contains(t, patterns, pattern)
	}
	for _, requiredSource := range []string{
		"build",
		"cmd",
		"hack",
		"pkg",
		"go.mod",
		"go.sum",
	} {
		require.NotContains(t, patterns, requiredSource)
		require.NotContains(t, patterns, "/"+requiredSource)
	}
}

func TestDockerfileCachesObjectstoreDependenciesBeforeSourceCopy(t *testing.T) {
	dockerfile, err := os.ReadFile("../Dockerfile")
	require.NoError(t, err)
	content := string(dockerfile)

	steps := []string{
		"COPY go.mod go.sum ./",
		"RUN go mod download",
		"COPY hack/backup/objectstore/go.mod hack/backup/objectstore/go.sum ./hack/backup/objectstore/",
		"RUN cd hack/backup/objectstore && go mod download",
		"COPY hack/kubectl/go.mod hack/kubectl/go.sum ./hack/kubectl/",
		"RUN cd hack/kubectl && go mod download && go mod verify",
		"COPY . .",
	}
	previous := -1
	for _, step := range steps {
		index := strings.Index(content, step)
		require.Greater(t, index, previous, "Dockerfile step %q must retain dependency-cache order", step)
		previous = index
	}
}

func TestDockerfileBuildsSecurityPatchedKubectlForSupportedArchitectures(t *testing.T) {
	dockerfile, err := os.ReadFile("../Dockerfile")
	require.NoError(t, err)
	content := string(dockerfile)

	for _, required := range []string{
		"ARG TARGETARCH\n",
		"ENV GOOS=linux GOARCH=${TARGETARCH}",
		"amd64|arm64)",
		"unsupported TARGETARCH=$TARGETARCH",
		"bash ./hack/kubectl/build.sh /src/bin/kubectl",
		"COPY --from=build /src/bin/kubectl /usr/local/bin/kubectl",
	} {
		require.Contains(t, content, required)
	}
	require.NotContains(t, content, "kubectl=1.34.2-r6")
	require.NotContains(t, content, "dl.k8s.io", "downloaded kubectl bypasses our reviewed toolchain and dependency graph")

	require.NotRegexp(t, `(?m)^ARG TARGETARCH=`, content, "a default overrides BuildKit's automatic target architecture")
	targetArch := strings.Index(content, "ARG TARGETARCH\n")
	goTarget := strings.Index(content, "ENV GOOS=linux GOARCH=${TARGETARCH}")
	sourceCopy := strings.Index(content, "COPY . .")
	compile := strings.Index(content, "bash ./build/build-tikv.sh")
	require.Less(t, targetArch, goTarget)
	require.Less(t, goTarget, sourceCopy)
	require.Less(t, goTarget, compile)
}

func TestDockerfileMetadataDoesNotInvalidateDependencyOrRuntimePackageLayers(t *testing.T) {
	dockerfile, err := os.ReadFile("../Dockerfile")
	require.NoError(t, err)
	content := string(dockerfile)

	buildStart := strings.Index(content, "FROM --platform=${BUILDPLATFORM} golang:1.26.8-bookworm@sha256:9fdc884aacc3bec89b20ffc69f4bb369c78210e3e4f600387b5128b12c199f81")
	require.NotEqual(t, -1, buildStart)
	sourceCopy := strings.Index(content[buildStart:], "COPY . .")
	require.NotEqual(t, -1, sourceCopy)
	sourceCopy += buildStart
	buildMetadata := strings.Index(content[sourceCopy:], "ARG KUBEBRAIN_VERSION")
	require.NotEqual(t, -1, buildMetadata)
	buildMetadata += sourceCopy
	require.NotContains(t, content[buildStart:sourceCopy], "ARG KUBEBRAIN_",
		"release metadata changes must not invalidate module download layers")

	runtimeStart := strings.Index(content, "FROM alpine:3.23@sha256:")
	require.NotEqual(t, -1, runtimeStart)
	runtimePackages := strings.Index(content[runtimeStart:], "RUN apk add --no-cache")
	runtimeScripts := strings.Index(content[runtimeStart:], "COPY hack/production/*.sh")
	require.NotEqual(t, -1, runtimePackages)
	require.NotEqual(t, -1, runtimeScripts)
	runtimePackages += runtimeStart
	runtimeScripts += runtimeStart
	runtimeMetadata := strings.Index(content[runtimeScripts:], "ARG KUBEBRAIN_VERSION")
	require.NotEqual(t, -1, runtimeMetadata)
	runtimeMetadata += runtimeScripts
	require.Less(t, runtimePackages, runtimeMetadata)
	require.Less(t, runtimeScripts, runtimeMetadata)
	require.Contains(t, content[buildMetadata:], "RUN test -n \"$KUBEBRAIN_VERSION\"")
	require.Contains(t, content[runtimeMetadata:], "LABEL org.opencontainers.image.title=\"KubeBrain\"")
}

func TestDockerfileUsesNativeBuildPlatformForCrossCompilation(t *testing.T) {
	dockerfile, err := os.ReadFile("../Dockerfile")
	require.NoError(t, err)
	content := string(dockerfile)

	require.NotRegexp(t, `(?m)^ARG BUILDPLATFORM=`, content,
		"the compiler must inherit the actual builder platform, including native arm64 runners")
	require.Contains(t, content,
		"FROM --platform=${BUILDPLATFORM} golang:1.26.8-bookworm@sha256:9fdc884aacc3bec89b20ffc69f4bb369c78210e3e4f600387b5128b12c199f81")
	require.Equal(t, 1, strings.Count(content, "--platform=${BUILDPLATFORM}"),
		"only the compiler stage should use the build platform; runtime must use the target platform")
}
