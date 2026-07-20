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

func TestBuildBaseUsesCrossCompileTargetInVersionMetadata(t *testing.T) {
	command := exec.Command("bash", "-c",
		`source ./build-base.sh TiKV && printf '%s\n' "$go_os" "$go_arch" "$ldflags"`)
	command.Dir = "."
	command.Env = append(os.Environ(),
		"GOOS=linux",
		"GOARCH=arm64",
		"KUBEBRAIN_VERSION=v1.2.3",
		"KUBEBRAIN_GIT_SHA=0123456789abcdef0123456789abcdef01234567",
		"KUBEBRAIN_BUILD_DATE=2026-07-18T10:00:00Z",
		"REQUIRE_BUILD_METADATA=true",
	)
	output, err := command.CombinedOutput()
	require.NoError(t, err, string(output))
	require.Contains(t, string(output), "go_os     \tlinux")
	require.Contains(t, string(output), "go_arch   \tarm64")
	require.Contains(t, string(output),
		"-X github.com/kubewharf/kubebrain/cmd/version.GoOsArch=linux/arm64")
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

func TestBackendBuildWrappersStopAfterMetadataFailure(t *testing.T) {
	dir := t.TempDir()
	marker := filepath.Join(dir, "build-invoked")
	fakeGo := filepath.Join(dir, "go")
	require.NoError(t, os.WriteFile(fakeGo, []byte(`#!/usr/bin/env bash
set -euo pipefail
if [[ "$1" == env ]]; then
  case "$2" in
    GOVERSION) echo go1.26.0 ;;
    GOOS) echo linux ;;
    GOARCH) echo amd64 ;;
    *) exit 1 ;;
  esac
  exit 0
fi
touch "$BUILD_MARKER"
`), 0o700))

	for _, script := range []string{"build-tikv.sh", "build-badger.sh"} {
		t.Run(script, func(t *testing.T) {
			if err := os.Remove(marker); err != nil {
				require.ErrorIs(t, err, os.ErrNotExist)
			}
			command := exec.Command("bash", script)
			command.Dir = "."
			command.Env = append(os.Environ(),
				"PATH="+dir+":"+os.Getenv("PATH"),
				"BUILD_MARKER="+marker,
				"KUBEBRAIN_VERSION=v1.2.3",
				"KUBEBRAIN_GIT_SHA=not-a-full-sha",
				"KUBEBRAIN_BUILD_DATE=2026-07-18T10:00:00Z",
				"REQUIRE_BUILD_METADATA=true",
			)
			output, err := command.CombinedOutput()
			require.Error(t, err)
			require.Contains(t, string(output), "full 40-character hexadecimal commit ID")
			require.NoFileExists(t, marker)
		})
	}
}
