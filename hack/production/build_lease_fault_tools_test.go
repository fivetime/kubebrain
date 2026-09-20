package production_test

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestLeaseFaultToolBundleAssembly(t *testing.T) {
	for _, failed := range []bool{false, true} {
		name := "success"
		if failed {
			name = "compiler-failure"
		}
		t.Run(name, func(t *testing.T) {
			bundle, tools := t.TempDir(), t.TempDir()
			require.NoError(t, os.Chmod(bundle, 0700))
			fixture := filepath.Join(tools, "binary")
			require.NoError(t, os.WriteFile(fixture, []byte("fixture binary"), 0600))
			compiler := `#!/bin/bash
set -euo pipefail
[[ $CGO_ENABLED == 0 && $GOOS == linux && $GOARCH == arm64 ]]
[[ $# == 5 && $1 == build && $2 == -trimpath && $3 == -o ]]
cp -- "$FAULT_BUILD_FIXTURE" "$4"
[[ ${FAULT_BUILD_FAIL:-} != yes ]] || exit 7
`
			require.NoError(t, os.WriteFile(filepath.Join(tools, "go"), []byte(compiler), 0700))
			cmd := exec.Command("bash", "build-lease-fault-tools.sh", bundle, "arm64")
			cmd.Env = append(os.Environ(), "PATH="+tools+":"+os.Getenv("PATH"), "FAULT_BUILD_FIXTURE="+fixture)
			if failed {
				cmd.Env = append(cmd.Env, "FAULT_BUILD_FAIL=yes")
			}
			out, err := cmd.CombinedOutput()
			if failed {
				require.Error(t, err)
				require.FileExists(t, filepath.Join(bundle, "bin/lease-term-probe"), "retain partial build")
				require.NoFileExists(t, filepath.Join(bundle, "SHA256SUMS"))
				require.NotContains(t, string(out), "TOOLS_BUILT")
				return
			}
			require.NoError(t, err, string(out))
			manifest, err := os.ReadFile(filepath.Join(bundle, "SHA256SUMS"))
			require.NoError(t, err)
			require.Len(t, strings.Split(strings.TrimSpace(string(manifest)), "\n"), 27)
			verify := exec.Command("sha256sum", "-c", "SHA256SUMS")
			verify.Dir = bundle
			verified, err := verify.CombinedOutput()
			require.NoError(t, err, string(verified))
			for _, file := range []string{"protected-wait-worker.sh", "protected-metrics-worker.sh", "protected-stack-session.sh", "same-pod-process.jq", "expired-lease-wait-frames.jq"} {
				original, err := os.ReadFile(file)
				require.NoError(t, err)
				for _, subdir := range []string{"hack/production", "deploy/test-cluster"} {
					copied, err := os.ReadFile(filepath.Join(bundle, subdir, file))
					require.NoError(t, err)
					require.Equal(t, original, copied)
				}
			}
			// This compiler fixture checks assembly only; actual cross-compilation
			// is a separate validation and must not be inferred from fake binaries.
			require.Contains(t, string(out), "NOT_RUNTIME_OR_EXPERIMENT_ADMISSION")
		})
	}
}

func TestLeaseFaultToolBuildRejectsUnsafeDestinations(t *testing.T) {
	for _, mode := range []string{"relative", "root", "nonempty", "public", "symlink", "unsupported-arch"} {
		t.Run(mode, func(t *testing.T) {
			dir := t.TempDir()
			require.NoError(t, os.Chmod(dir, 0700))
			arch := "amd64"
			switch mode {
			case "relative":
				dir = "."
			case "root":
				dir = "/"
			case "nonempty":
				require.NoError(t, os.WriteFile(filepath.Join(dir, "keep"), []byte("keep"), 0600))
			case "public":
				require.NoError(t, os.Chmod(dir, 0755))
			case "symlink":
				link := filepath.Join(t.TempDir(), "link")
				require.NoError(t, os.Symlink(dir, link))
				dir = link
			case "unsupported-arch":
				arch = "wrong"
			}
			out, err := exec.Command("bash", "build-lease-fault-tools.sh", dir, arch).CombinedOutput()
			require.Error(t, err, string(out))
			require.NotContains(t, string(out), "TOOLS_BUILT")
			if mode == "nonempty" {
				data, err := os.ReadFile(filepath.Join(dir, "keep"))
				require.NoError(t, err)
				require.Equal(t, "keep", string(data))
			}
		})
	}
}
