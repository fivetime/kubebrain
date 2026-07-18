package build_test

import (
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestBuildBaseUsesExplicitMetadata(t *testing.T) {
	command := exec.Command("bash", "-c",
		`source ./build-base.sh TiKV && printf '%s\n' "$version" "$sha" "$date" "$ldflags"`)
	command.Dir = "."
	command.Env = append(os.Environ(),
		"KUBEBRAIN_VERSION=v1.2.3",
		"KUBEBRAIN_GIT_SHA=0123456789abcdef0123456789abcdef01234567",
		"KUBEBRAIN_BUILD_DATE=2026-07-18T10:00:00Z",
		"REQUIRE_BUILD_METADATA=true",
	)
	output, err := command.CombinedOutput()
	require.NoError(t, err, string(output))
	require.Contains(t, string(output), "v1.2.3")
	require.Contains(t, string(output), "0123456789abcdef0123456789abcdef01234567")
	require.Contains(t, string(output), "2026-07-18T10:00:00Z")
	require.Contains(t, string(output), "-X github.com/kubewharf/kubebrain/cmd/version.Version=v1.2.3")
}

func TestBuildBaseStrictModeRejectsAbbreviatedSHA(t *testing.T) {
	command := exec.Command("bash", "-c", `source ./build-base.sh TiKV`)
	command.Dir = "."
	command.Env = append(os.Environ(),
		"KUBEBRAIN_VERSION=v1.2.3",
		"KUBEBRAIN_GIT_SHA=01234567",
		"KUBEBRAIN_BUILD_DATE=2026-07-18T10:00:00Z",
		"REQUIRE_BUILD_METADATA=true",
	)
	output, err := command.CombinedOutput()
	require.Error(t, err)
	require.Contains(t, string(output), "full 40-character hexadecimal commit ID")
}

func TestBuildBaseStrictModeRejectsMissingMetadata(t *testing.T) {
	script, err := filepath.Abs("build-base.sh")
	require.NoError(t, err)
	command := exec.Command("bash", "-c", `source "$BUILD_SCRIPT" TiKV`)
	command.Dir = t.TempDir()
	command.Env = append(os.Environ(),
		"BUILD_SCRIPT="+script,
		"KUBEBRAIN_VERSION=",
		"KUBEBRAIN_GIT_SHA=",
		"KUBEBRAIN_BUILD_DATE=",
		"REQUIRE_BUILD_METADATA=true",
	)
	output, err := command.CombinedOutput()
	require.Error(t, err)
	require.Contains(t, string(output), "KUBEBRAIN_VERSION, KUBEBRAIN_GIT_SHA, and KUBEBRAIN_BUILD_DATE are required")
}
