package build_test

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestScaleLabRejectsInsecureGoToolchains(t *testing.T) {
	script := "../hack/scale-lab/check-go-version.sh"
	for _, tc := range []struct {
		name    string
		version string
		ok      bool
	}{
		{name: "vulnerable old baseline", version: "go1.26.5"},
		{name: "previous patch", version: "go1.26.7"},
		{name: "minimum", version: "go1.26.8", ok: true},
		{name: "newer patch", version: "go1.26.9", ok: true},
		{name: "newer minor", version: "go1.27.0", ok: true},
		{name: "unstable string", version: "go1.27rc1"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cmd := exec.Command("bash", script)
			cmd.Env = append(os.Environ(), "SCALE_LAB_GO_VERSION_OVERRIDE="+tc.version)
			output, err := cmd.CombinedOutput()
			if tc.ok {
				require.NoError(t, err, string(output))
				return
			}
			require.Error(t, err)
			require.Contains(t, string(output), "scale-lab requires")
		})
	}
}

func TestScaleLabBuildPhasesCheckGoVersionBeforeBuilding(t *testing.T) {
	contents, err := os.ReadFile("../hack/scale-lab/setup.sh")
	require.NoError(t, err)
	script := string(contents)
	require.Equal(t, 1, strings.Count(script, `"$HERE/check-go-version.sh"`))

	for _, phase := range []string{"phase_kubebrain()"} {
		start := strings.Index(script, phase)
		require.NotEqual(t, -1, start, phase)
		remainder := script[start:]
		check := strings.Index(remainder, `"$HERE/check-go-version.sh"`)
		build := strings.Index(remainder, "go build")
		require.NotEqual(t, -1, check, phase)
		require.NotEqual(t, -1, build, phase)
		require.Less(t, check, build, phase)
	}
	tools, err := os.ReadFile("../hack/scale-lab/build-tools.sh")
	require.NoError(t, err)
	check := strings.Index(string(tools), `"$HERE/check-go-version.sh"`)
	require.Greater(t, strings.Index(string(tools), "go build"), check)
	require.NotEqual(t, -1, check)
	require.Contains(t, script, `bash "$HERE/build-tools.sh"`)
}

func TestScaleLabToolsBuildsAdvertisedIndependentModules(t *testing.T) {
	contents, err := os.ReadFile("../hack/scale-lab/build-tools.sh")
	require.NoError(t, err)
	script := string(contents)
	require.Contains(t, script, "for module in loadgen bigstream")
	require.Contains(t, script, `cd "$HERE/$module" && go build`)
	require.Contains(t, script, "for probe in bulk foload elogprobe qlat slowwatch watchflood")
}

func TestScaleLabLocalToolsNeverLoadDeploymentEnvironment(t *testing.T) {
	dir := t.TempDir()
	calls := filepath.Join(dir, "calls")
	fakeGo := "#!/bin/sh\nprintf '%s\\n' \"$PWD $*\" >> \"$BUILD_CALLS\"\n"
	require.NoError(t, os.WriteFile(filepath.Join(dir, "go"), []byte(fakeGo), 0700))
	envFile := filepath.Join(dir, "lab.env")
	require.NoError(t, os.WriteFile(envFile, []byte("echo deployment-config-must-not-run >&2\nexit 99\n"), 0600))
	cmd := exec.Command("bash", "../hack/scale-lab/setup.sh", "tools")
	cmd.Env = append(os.Environ(), "PATH="+dir+":"+os.Getenv("PATH"), "BUILD_CALLS="+calls,
		"LAB_ENV="+envFile, "SCALE_LAB_BIN_DIR="+filepath.Join(dir, "bin"), "SCALE_LAB_GO_VERSION_OVERRIDE=go1.26.8")
	output, err := cmd.CombinedOutput()
	require.NoError(t, err, string(output))
	data, err := os.ReadFile(calls)
	require.NoError(t, err)
	require.Len(t, strings.Split(strings.TrimSpace(string(data)), "\n"), 8)
	for _, tool := range []string{"loadgen", "bigstream", "bulk", "foload", "elogprobe", "qlat", "slowwatch", "watchflood"} {
		require.Contains(t, string(data), filepath.Join(dir, "bin", tool))
	}
}
