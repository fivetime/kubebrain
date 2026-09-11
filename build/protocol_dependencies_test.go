package build_test

import (
	"context"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestRealProtocolTestsExcludeTiDBSQLMock(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
	defer cancel()
	command := exec.CommandContext(ctx, "go", "list", "-test", "-deps", "../pkg/storage/tikv")
	out, err := command.CombinedOutput()
	require.NoError(t, err, string(out))
	dependencies := strings.Split(string(out), "\n")
	require.Contains(t, dependencies, "github.com/kubewharf/kubebrain/pkg/storage/tikv.test",
		"the boundary must include the test binary, not just product sources")
	for _, dependency := range dependencies {
		require.False(t, dependency == "github.com/pingcap/tidb" ||
			strings.HasPrefix(dependency, "github.com/pingcap/tidb/"),
			"real protocol tests must not reintroduce TiDB SQL mocks: %s", dependency)
	}
}
