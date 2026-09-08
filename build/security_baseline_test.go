package build_test

import (
	"os"
	"regexp"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// These pins keep helper binaries on the same reviewed security baseline as
// the data plane. Reachability scans remain necessary as advisories evolve.
func TestProductModulesDeclareReviewedGoToolchain(t *testing.T) {
	for _, path := range []string{
		"../go.mod",
		"../hack/backup/objectstore/go.mod",
		"../hack/kubectl/go.mod",
		"../hack/etcd-client-compat/go.mod",
		"../hack/scale-lab/bigstream/go.mod",
		"../hack/scale-lab/loadgen/go.mod",
	} {
		t.Run(path, func(t *testing.T) {
			content, err := os.ReadFile(path)
			require.NoError(t, err)
			require.Regexp(t, `(?m)^toolchain go1\.26\.8$`, string(content))
		})
	}
}

func TestDBaaSImageScansShippedModulesBeforePublishing(t *testing.T) {
	content, err := os.ReadFile("../.github/workflows/image.yml")
	require.NoError(t, err)
	workflow := string(content)
	require.Contains(t, workflow, `go-version: '1.26.8'`)
	require.Contains(t, workflow, "go run golang.org/x/vuln/cmd/govulncheck@v1.6.0 ./...")
	require.Contains(t, workflow, "cd hack/backup/objectstore")
	require.Contains(t, workflow, "cd hack/kubectl")
	require.NotContains(t, workflow, "continue-on-error:")
	scan := strings.Index(workflow, "name: Scan shipped Go modules")
	build := strings.Index(workflow, "name: Build and push TiKV test image")
	require.GreaterOrEqual(t, scan, 0)
	require.Greater(t, build, scan)
	binaryScan := strings.Index(workflow, "name: Build and scan kubectl binaries")
	require.Greater(t, binaryScan, scan)
	require.Less(t, binaryScan, build)
	require.Contains(t, workflow, "for arch in amd64 arm64; do")
	require.Contains(t, workflow, `-mode=binary "$artifact_dir/kubectl-$arch"`)
	require.Contains(t, workflow, `cmp "$artifact_dir/kubectl-$arch" "$artifact_dir/shipped-$arch"`)
	require.Less(t, strings.Index(workflow, `cmp "$artifact_dir/kubectl-$arch"`), strings.Index(workflow, "name: Promote verified image to dbaas"))
}

func TestKubectlPreservesSupportedKubernetesMinorWindow(t *testing.T) {
	content, err := os.ReadFile("../hack/kubectl/go.mod")
	require.NoError(t, err)
	for _, module := range []string{"client-go", "component-base", "kubectl"} {
		require.Regexp(t, `(?m)^\s*k8s\.io/`+regexp.QuoteMeta(module)+` v0\.36\.4(?:\s|$)`, string(content))
	}
	content, err = os.ReadFile("../hack/kubectl/main.go")
	require.NoError(t, err)
	require.Contains(t, string(content), "cmd.NewDefaultKubectlCommand()")
	require.Contains(t, string(content), `"k8s.io/client-go/plugin/pkg/client/auth"`)
	require.Contains(t, string(content), "cmd.GetLogVerbosity(os.Args)")
}

func TestProductEtcdDependenciesIncludeSecurityFixes(t *testing.T) {
	for _, path := range []string{"../go.mod", "../hack/scale-lab/bigstream/go.mod"} {
		t.Run(path, func(t *testing.T) {
			content, err := os.ReadFile(path)
			require.NoError(t, err)
			for _, module := range []string{"api", "client/pkg", "client"} {
				require.Regexp(t, `(?m)^\s*(?:require\s+)?go\.etcd\.io/etcd/`+regexp.QuoteMeta(module)+`/v3 v3\.7\.1(?:\s|$)`, string(content))
			}
		})
	}
	content, err := os.ReadFile("../go.mod")
	require.NoError(t, err)
	require.Regexp(t, `(?m)^\s*go\.etcd\.io/etcd/server/v3 v3\.7\.1(?:\s|$)`, string(content))
	// The separate compatibility harness intentionally follows /root/etcd;
	// it must not be mistaken for the released embedded-server dependency.
}
