package build_test

import (
	"bufio"
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

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
