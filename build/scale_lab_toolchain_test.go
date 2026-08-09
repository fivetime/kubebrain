package build_test

import (
	"os"
	"os/exec"
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
		{name: "previous patch", version: "go1.26.4"},
		{name: "minimum", version: "go1.26.5", ok: true},
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
	require.Equal(t, 2, strings.Count(script, `"$HERE/check-go-version.sh"`))

	for _, phase := range []string{"phase_kubebrain()", "phase_tools()"} {
		start := strings.Index(script, phase)
		require.NotEqual(t, -1, start, phase)
		remainder := script[start:]
		check := strings.Index(remainder, `"$HERE/check-go-version.sh"`)
		build := strings.Index(remainder, "go build")
		require.NotEqual(t, -1, check, phase)
		require.NotEqual(t, -1, build, phase)
		require.Less(t, check, build, phase)
	}
}
