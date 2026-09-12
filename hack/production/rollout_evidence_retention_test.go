package production_test

import (
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRolloutRuntimeEvidenceRetentionIsOptIn(t *testing.T) {
	for _, mode := range []string{"true", "false", "invalid", "failed", "rollback"} {
		t.Run(mode, func(t *testing.T) {
			// Retained files stay inside the test's owned temporary tree and are
			// removed by testing, never by a guessed production path.
			root := t.TempDir()
			t.Setenv("TMPDIR", root)
			settings := []string{"KEEP_RUNTIME_EVIDENCE=" + mode}
			if mode == "failed" {
				settings = []string{"KEEP_RUNTIME_EVIDENCE=true", "FAKE_PREPULL_FAIL_MODE=prepare"}
			}
			if mode == "rollback" {
				settings = []string{"KEEP_RUNTIME_EVIDENCE=true", "FAKE_ROLLOUT_FAIL=true"}
			}
			output, log, err := runPrepullRollout(t, settings...)
			if mode == "invalid" {
				require.Error(t, err)
				require.Contains(t, output, "KEEP_RUNTIME_EVIDENCE must be true or false")
				require.Empty(t, log, "invalid retention option must fail before cluster access")
				return
			}
			if mode == "failed" || mode == "rollback" {
				require.Error(t, err)
				require.NotContains(t, output, "KubeBrain rollout availability gate passed")
			} else {
				require.NoError(t, err, output)
			}
			require.Contains(t, log, "image-prepull --mode=recover-cleanup ", "local retention cannot skip cluster compensation")
			if mode == "rollback" {
				require.Contains(t, output, "restoring original image")
				require.Equal(t, 2, strings.Count(log, " patch statefulset/kubebrain "))
			}
			if mode == "false" {
				require.NotContains(t, output, "RUNTIME_EVIDENCE_")
				leftovers, globErr := filepath.Glob(filepath.Join(root, "tmp.*"))
				require.NoError(t, globErr)
				require.Empty(t, leftovers, "default mode must remove the runtime scratch directory")
				return
			}
			match := regexp.MustCompile(`(?m)^RUNTIME_EVIDENCE_DIRECTORY=(.+)$`).FindStringSubmatch(output)
			require.Len(t, match, 2, output)
			directory := match[1]
			require.Equal(t, root, filepath.Dir(directory))
			info, statErr := os.Stat(directory)
			require.NoError(t, statErr)
			require.True(t, info.IsDir())
			require.Equal(t, os.FileMode(0700), info.Mode().Perm())
			require.Contains(t, output, "RUNTIME_EVIDENCE_RETAINED directory="+directory)
		})
	}
}
