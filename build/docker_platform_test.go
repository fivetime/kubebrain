package build_test

import (
	"context"
	"debug/elf"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// The image workflow enables this real BuildKit check. Ordinary Go-only test
// environments retain the static Dockerfile guards without requiring Docker.
// No TARGETARCH build argument is supplied: that would hide default overrides.
func TestDockerfileAutomaticTargetArchitecture(t *testing.T) {
	if os.Getenv("KUBEBRAIN_TEST_DOCKER_PLATFORM") != "true" {
		t.Skip("image CI sets KUBEBRAIN_TEST_DOCKER_PLATFORM=true for the real BuildKit gate")
	}
	_, err := exec.LookPath("docker")
	require.NoError(t, err)
	content, err := os.ReadFile("../Dockerfile")
	require.NoError(t, err)
	var directives []string
	for _, line := range strings.Split(string(content), "\n") {
		if line == "COPY . ." {
			break
		}
		if strings.HasPrefix(line, "ARG BUILDPLATFORM") ||
			strings.HasPrefix(line, "FROM --platform=") ||
			strings.HasPrefix(line, "ARG TARGETARCH") ||
			strings.HasPrefix(line, "ENV GOOS=") {
			directives = append(directives, line)
		}
	}
	require.NotEmpty(t, directives)
	contextDir := t.TempDir()
	dockerfile := strings.Join(directives, "\n") + `
ENV CGO_ENABLED=0 GO111MODULE=off
COPY platform.go /platform.go
RUN go build -trimpath -o /target /platform.go
FROM scratch
COPY --from=build /target /target
`
	require.NoError(t, os.WriteFile(filepath.Join(contextDir, "Dockerfile"), []byte(dockerfile), 0600))
	require.NoError(t, os.WriteFile(filepath.Join(contextDir, "platform.go"), []byte("package main\nfunc main() {}\n"), 0600))
	for _, tc := range []struct {
		arch    string
		machine elf.Machine
	}{{"amd64", elf.EM_X86_64}, {"arm64", elf.EM_AARCH64}} {
		t.Run(tc.arch, func(t *testing.T) {
			outputDir := t.TempDir()
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Minute)
			defer cancel()
			cmd := exec.CommandContext(ctx, "docker", "buildx", "build", "--progress=plain",
				"--platform", "linux/"+tc.arch, "--output", "type=local,dest="+outputDir, contextDir)
			output, err := cmd.CombinedOutput()
			require.NoError(t, err, "%s", output)
			binary, err := elf.Open(filepath.Join(outputDir, "target"))
			require.NoError(t, err)
			defer binary.Close()
			require.Equal(t, tc.machine, binary.Machine,
				"--platform linux/%s must produce matching ELF bytes without a manual TARGETARCH override", tc.arch)
		})
	}
}
