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

	start := strings.Index(content, "RUN apk add --no-cache")
	require.NotEqual(t, -1, start)
	end := strings.Index(content[start:], "&& addgroup")
	require.NotEqual(t, -1, end)
	install := strings.ReplaceAll(content[start:start+end], "\\", "")

	expected := []string{
		"bash=5.3.3-r1",
		"ca-certificates=20260611-r0",
		"coreutils=9.8-r1",
		"curl=8.20.0-r0",
		"etcd-ctl=3.6.10-r1",
		"jq=1.8.1-r0",
		"openssl=3.5.7-r0",
	}
	require.Equal(t, append([]string{"RUN", "apk", "add", "--no-cache"}, expected...), strings.Fields(install),
		"runtime package additions and upgrades must pin exact versions")
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
		"ARG KUBECTL_VERSION=v1.36.2",
		"curl --proto '=https' --tlsv1.2 -fsSLo /src/bin/kubectl",
		"echo \"${KUBECTL_SHA256}  /src/bin/kubectl\" | sha256sum -c -",
		"COPY . .",
	}
	previous := -1
	for _, step := range steps {
		index := strings.Index(content, step)
		require.Greater(t, index, previous, "Dockerfile step %q must retain dependency-cache order", step)
		previous = index
	}
}

func TestDockerfilePinsOfficialKubectlForSupportedArchitectures(t *testing.T) {
	dockerfile, err := os.ReadFile("../Dockerfile")
	require.NoError(t, err)
	content := string(dockerfile)

	for _, required := range []string{
		"ARG TARGETARCH=amd64",
		"ENV GOOS=linux GOARCH=${TARGETARCH}",
		"ARG KUBECTL_VERSION=v1.36.2",
		"amd64) KUBECTL_SHA256=1e9045ec32bea85da43de85f0065358529ea7c7a152eca78154fba5b58c27d82",
		"arm64) KUBECTL_SHA256=c957eb8c4bea27a3bb35b269edd9082e27f027f7b76b20b5bf4afebc726c6d3e",
		`"https://dl.k8s.io/release/${KUBECTL_VERSION}/bin/linux/${TARGETARCH}/kubectl"`,
		`echo "${KUBECTL_SHA256}  /src/bin/kubectl" | sha256sum -c -`,
		"COPY --from=build /src/bin/kubectl /usr/local/bin/kubectl",
	} {
		require.Contains(t, content, required)
	}
	require.NotContains(t, content, "kubectl=1.34.2-r6")

	targetArch := strings.Index(content, "ARG TARGETARCH=amd64")
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

	buildStart := strings.Index(content, "FROM --platform=${BUILDPLATFORM} golang:1.26.5-bookworm@sha256:1ecb7edf62a0408027bd5729dfd6b1b8766e578e8df93995b225dfd0944eb651")
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

	require.Contains(t, content, "ARG BUILDPLATFORM=linux/amd64")
	require.Contains(t, content,
		"FROM --platform=${BUILDPLATFORM} golang:1.26.5-bookworm@sha256:1ecb7edf62a0408027bd5729dfd6b1b8766e578e8df93995b225dfd0944eb651")
	require.Equal(t, 1, strings.Count(content, "--platform=${BUILDPLATFORM}"),
		"only the compiler stage should use the build platform; runtime must use the target platform")
}
