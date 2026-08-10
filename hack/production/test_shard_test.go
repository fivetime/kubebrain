package production_test

import (
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

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
