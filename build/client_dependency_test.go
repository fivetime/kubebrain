package build_test

import (
	"io/fs"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestTiKVClientUsesPinnedRemoteFork(t *testing.T) {
	module, err := os.ReadFile("../go.mod")
	require.NoError(t, err)
	require.Regexp(t, `(?m)^replace github\.com/tikv/client-go/v2 => github\.com/fivetime/tikv-client-go/v2 v2\.\d+\.\d+-(?:0\.)?\d{14}-[0-9a-f]{12}$`, string(module))
	_, err = os.Stat("../third_party/tikv-client-go")
	require.ErrorIs(t, err, fs.ErrNotExist, "the remote client must not have a second embedded source copy")

	for _, path := range []string{"../Dockerfile", "../.github/workflows/ci.yml", "../hack/dev/incluster-balancer-smoke.sh"} {
		content, err := os.ReadFile(path)
		require.NoError(t, err)
		require.NotContains(t, string(content), "third_party/tikv-client-go", path)
	}
}
