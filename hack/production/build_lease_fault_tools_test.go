package production_test

import (
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

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
