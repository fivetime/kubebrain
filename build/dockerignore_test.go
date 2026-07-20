package build_test

import (
	"bufio"
	"os"
	"regexp"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

var pinnedBaseImage = regexp.MustCompile(`(?m)^FROM [^\s@]+:[^\s@]+@sha256:[a-f0-9]{64}(?: AS [a-zA-Z0-9_-]+)?$`)

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
		"COPY . .",
	}
	previous := -1
	for _, step := range steps {
		index := strings.Index(content, step)
		require.Greater(t, index, previous, "Dockerfile step %q must retain dependency-cache order", step)
		previous = index
	}
}

func TestDockerfileMetadataDoesNotInvalidateDependencyOrRuntimePackageLayers(t *testing.T) {
	dockerfile, err := os.ReadFile("../Dockerfile")
	require.NoError(t, err)
	content := string(dockerfile)

	buildStart := strings.Index(content, "FROM golang:1.26-bookworm@sha256:")
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
