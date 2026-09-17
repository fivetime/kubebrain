package production_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestProductionTestShardsRejectPartialDiscovery(t *testing.T) {
	for _, mode := range []string{"--list", "--verify", "0"} {
		t.Run(mode, func(t *testing.T) {
			bin := t.TempDir()
			marker := filepath.Join(bin, "test-run-started")
			writeExecutable(t, filepath.Join(bin, "go"), `#!/usr/bin/env bash
if [[ "$*" == *"-list"* ]]; then
  echo TestPartialInventory
  echo 'discovery failed after partial output' >&2
  exit 17
fi
touch "$SHARD_RUN_MARKER"
`)
			output, err := runProductionCommand(t, "bash", []string{"test-shard.sh", mode, "1"}, []string{
				"PATH=" + bin + ":" + os.Getenv("PATH"),
				"SHARD_RUN_MARKER=" + marker,
			})
			require.Error(t, err, string(output))
			require.Contains(t, string(output), "discovery failed after partial output")
			require.NotContains(t, string(output), "verified 1 production tests")
			require.NotContains(t, string(output), "running production test shard")
			require.NoFileExists(t, marker)
		})
	}
}

func TestProductionTestShardsRejectFailedInventoryFilter(t *testing.T) {
	for _, stage := range []string{"awk", "sort"} {
		for _, mode := range []string{"--list", "--verify", "0"} {
			t.Run(stage+"/"+mode, func(t *testing.T) {
				bin := t.TempDir()
				writeExecutable(t, filepath.Join(bin, "go"), "#!/usr/bin/env bash\necho TestPartialInventory\n")
				writeExecutable(t, filepath.Join(bin, stage), "#!/usr/bin/env bash\ncat >/dev/null\necho TestPartialInventory\nexit 17\n")
				output, err := runProductionCommand(t, "bash", []string{"test-shard.sh", mode, "1"}, []string{"PATH=" + bin + ":" + os.Getenv("PATH")})
				require.Error(t, err, string(output))
				require.Contains(t, string(output), "production test discovery failed")
				require.NotContains(t, string(output), "running production test shard")
			})
		}
	}
}

func TestProductionTestShardsRejectEmptyInventory(t *testing.T) {
	for _, mode := range []string{"--list", "--verify", "0"} {
		t.Run(mode, func(t *testing.T) {
			bin := t.TempDir()
			writeExecutable(t, filepath.Join(bin, "go"), "#!/usr/bin/env bash\necho 'ok package with no discovered tests'\n")
			output, err := runProductionCommand(t, "bash", []string{"test-shard.sh", mode, "1"}, []string{"PATH=" + bin + ":" + os.Getenv("PATH")})
			require.Error(t, err, string(output))
			require.Contains(t, string(output), "no production tests discovered")
		})
	}
}

func TestProductionTestShardsAreExhaustiveAndNonEmpty(t *testing.T) {
	output, err := runProductionCommand(t, "bash", []string{"test-shard.sh", "--verify", "4"}, nil)
	require.NoError(t, err, string(output))
	require.Regexp(t, `verified [1-9][0-9]* production tests across 4 non-empty shards: [1-9][0-9]*( [1-9][0-9]*){3}`, string(output))
}

func TestCIExcludesMonolithAndRunsEveryProductionShard(t *testing.T) {
	data, err := os.ReadFile("../../.github/workflows/ci.yml")
	require.NoError(t, err)
	workflow := string(data)
	require.Contains(t, workflow, `go list ./... | grep -v '/hack/production$'`)
	require.Contains(t, workflow, `shard: [0, 1, 2, 3]`)
	require.Equal(t, 1, strings.Count(workflow, `hack/production/test-shard.sh --verify 4`))
	require.Equal(t, 1, strings.Count(workflow, `hack/production/test-shard.sh "${{ matrix.shard }}" 4`))
}
