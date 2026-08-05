package compat

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"regexp"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDifferentialRunnerRejectsInvalidDestructiveApprovalBeforeDependencies(t *testing.T) {
	dir := t.TempDir()
	env := []string{
		"PATH=" + dir + ":/usr/bin:/bin",
		"ALLOW_DESTRUCTIVE_DIFFERENTIAL=maybe",
	}
	output, err := runDifferentialScript(t, env)
	require.Error(t, err)
	require.Contains(t, string(output), "ALLOW_DESTRUCTIVE_DIFFERENTIAL must be true or false, got maybe")
	require.NotContains(t, string(output), "missing required command")
}

func TestDifferentialRunnerRejectsMissingDestructiveApprovalBeforeDependencies(t *testing.T) {
	dir := t.TempDir()
	env := []string{
		"PATH=" + dir + ":/usr/bin:/bin",
		"KUBEBRAIN_ETCD_ENDPOINT=127.0.0.1:22379",
	}
	output, err := runDifferentialScript(t, env)
	require.Error(t, err)
	require.Contains(t, string(output), "refusing destructive differential suite")
	require.NotContains(t, string(output), "missing required command")
}

func TestDifferentialRunnerRejectsUnreachableAdvertisedClientURL(t *testing.T) {
	dir := t.TempDir()
	fakeEtcdctl := filepath.Join(dir, "etcdctl")
	require.NoError(t, os.WriteFile(fakeEtcdctl, []byte(`#!/usr/bin/env bash
set -euo pipefail
if [[ "$*" == *"member list -w json"* ]]; then
  printf '%s\n' '{"members":[{"name":"kubebrain-0","clientURLs":["http://internal.invalid:3379"]}]}'
  exit 0
fi
if [[ "$*" == *"--endpoints=http://internal.invalid:3379"* ]]; then
  exit 1
fi
if [[ "$*" == *"endpoint health"* ]]; then
  exit 0
fi
exit 1
`), 0o755))
	fakeCurl := filepath.Join(dir, "curl")
	require.NoError(t, os.WriteFile(fakeCurl, []byte("#!/usr/bin/env bash\nexit 1\n"), 0o755))

	env := []string{
		"PATH=" + dir + ":" + os.Getenv("PATH"),
		"KUBEBRAIN_ETCD_ENDPOINT=127.0.0.1:22379",
		"ALLOW_DESTRUCTIVE_DIFFERENTIAL=true",
		"REFERENCE_ETCD_BIN=/bin/true",
		"ETCDCTL_BIN=" + fakeEtcdctl,
	}
	output, err := runDifferentialScript(t, env)
	require.Error(t, err)
	require.Contains(t, string(output), "advertised client URL is unreachable")
	require.Contains(t, string(output), "http://internal.invalid:3379")
}

func TestDifferentialRunnerRejectsMissingAdvertisedClientURLs(t *testing.T) {
	dir := t.TempDir()
	fakeEtcdctl := filepath.Join(dir, "etcdctl")
	require.NoError(t, os.WriteFile(fakeEtcdctl, []byte(`#!/usr/bin/env bash
set -euo pipefail
if [[ "$*" == *"member list -w json"* ]]; then
  printf '%s\n' '{"members":[{"name":"kubebrain-0","clientURLs":[]}]}'
  exit 0
fi
if [[ "$*" == *"endpoint health"* ]]; then
  exit 0
fi
exit 1
`), 0o755))
	fakeCurl := filepath.Join(dir, "curl")
	require.NoError(t, os.WriteFile(fakeCurl, []byte("#!/usr/bin/env bash\nexit 1\n"), 0o755))

	env := []string{
		"PATH=" + dir + ":" + os.Getenv("PATH"),
		"KUBEBRAIN_ETCD_ENDPOINT=127.0.0.1:22379",
		"ALLOW_DESTRUCTIVE_DIFFERENTIAL=true",
		"REFERENCE_ETCD_BIN=/bin/true",
		"ETCDCTL_BIN=" + fakeEtcdctl,
	}
	output, err := runDifferentialScript(t, env)
	require.Error(t, err)
	require.Contains(t, string(output), "returned no advertised client URLs")
}

func TestDifferentialRunnerChecksClusterLocalAdvertisedURLInSelectedPod(t *testing.T) {
	dir := t.TempDir()
	fakeEtcdctl := filepath.Join(dir, "etcdctl")
	require.NoError(t, os.WriteFile(fakeEtcdctl, []byte(`#!/usr/bin/env bash
set -euo pipefail
if [[ "$*" == *"member list -w json"* ]]; then
  printf '%s\n' '{"members":[{"name":"kubebrain-0","clientURLs":["http://internal.invalid:3379"]}]}'
  exit 0
fi
if [[ "$*" == *"endpoint status -w json"* ]]; then
  printf '%s\n' '[{"Status":{"dbSizeQuota":1073741824}}]'
  exit 0
fi
if [[ "$*" == *"endpoint health"* ]]; then
  exit 0
fi
exit 1
`), 0o755))
	fakeKubectl := filepath.Join(dir, "kubectl")
	kubectlLog := filepath.Join(dir, "kubectl.log")
	require.NoError(t, os.WriteFile(fakeKubectl, []byte(`#!/usr/bin/env bash
set -euo pipefail
printf '%s\n' "$*" >>"$KUBECTL_LOG"
while [[ "$1" != "--" ]]; do shift; done
shift
exec "$@"
`), 0o755))
	fakeCurl := filepath.Join(dir, "curl")
	require.NoError(t, os.WriteFile(fakeCurl, []byte("#!/usr/bin/env bash\nexit 1\n"), 0o755))

	env := []string{
		"PATH=" + dir + ":" + os.Getenv("PATH"),
		"KUBEBRAIN_ETCD_ENDPOINT=127.0.0.1:22379",
		"ALLOW_DESTRUCTIVE_DIFFERENTIAL=true",
		"REFERENCE_ETCD_BIN=/bin/true",
		"ETCDCTL_BIN=" + fakeEtcdctl,
		"KUBECTL=" + fakeKubectl,
		"KUBECTL_LOG=" + kubectlLog,
		"ETCDCTL_EXEC_POD=kubebrain-0",
		"ETCDCTL_EXEC_NAMESPACE=kubebrain-dev",
		"ETCDCTL_EXEC_CONTAINER=kubebrain",
		"ETCDCTL_EXEC_BIN=" + fakeEtcdctl,
	}
	output, err := runDifferentialScript(t, env)
	require.Error(t, err)
	require.NotContains(t, string(output), "advertised client URL is unreachable")
	require.Contains(t, string(output), "reference etcd exited before becoming healthy")
	log, readErr := os.ReadFile(kubectlLog)
	require.NoError(t, readErr)
	require.Contains(t, string(log), "-n kubebrain-dev exec kubebrain-0 -c kubebrain -- "+fakeEtcdctl)
	require.Contains(t, string(log), "--endpoints=http://internal.invalid:3379 endpoint health")
}

func TestDifferentialRunnerTestsUseBoundedScriptHelper(t *testing.T) {
	text, err := os.ReadFile("run_differential_test.go")
	require.NoError(t, err)
	forbiddenCommand := `exec.Command("bash", "` + `run-differential.sh")`
	forbiddenCombinedOutput := "." + "CombinedOutput()"
	require.Contains(t, string(text), "runDifferentialScript(t, env)")
	require.NotContains(t, string(text), forbiddenCommand)
	require.NotContains(t, string(text), forbiddenCombinedOutput)
}

func TestDifferentialRunnerSelectsScenariosNotRunnerSelfTests(t *testing.T) {
	script, err := os.ReadFile("run-differential.sh")
	require.NoError(t, err)
	require.Contains(t, string(script), "-run 'Differential(Against|$)'",
		"the live runner must not inherit its own opt-in environment into runner unit tests")
	require.NotContains(t, string(script), "-run Differential -count=1")
}

func TestReferenceComparisonTestNamesAreSelectedByDifferentialRunner(t *testing.T) {
	files, err := filepath.Glob("*_test.go")
	require.NoError(t, err)
	selected := regexp.MustCompile(`Differential(Against|$)`)
	specialized := map[string]string{
		"TestWatchFragmentLimitBoundaryAcrossDirectReplicas": "requires KUBEBRAIN_DIRECT_ENDPOINTS",
	}
	for _, file := range files {
		parsed, parseErr := parser.ParseFile(token.NewFileSet(), file, nil, 0)
		require.NoError(t, parseErr)
		for _, declaration := range parsed.Decls {
			function, ok := declaration.(*ast.FuncDecl)
			if !ok || function.Body == nil || function.Name == nil || !regexp.MustCompile(`^Test`).MatchString(function.Name.Name) {
				continue
			}
			readsReferenceEndpoint := false
			ast.Inspect(function.Body, func(node ast.Node) bool {
				call, ok := node.(*ast.CallExpr)
				if !ok || len(call.Args) != 1 {
					return true
				}
				selector, ok := call.Fun.(*ast.SelectorExpr)
				if !ok || selector.Sel.Name != "Getenv" {
					return true
				}
				packageName, ok := selector.X.(*ast.Ident)
				argument, literal := call.Args[0].(*ast.BasicLit)
				if ok && packageName.Name == "os" && literal && argument.Kind == token.STRING && argument.Value == `"REFERENCE_ETCD_ENDPOINT"` {
					readsReferenceEndpoint = true
				}
				return true
			})
			if readsReferenceEndpoint {
				if reason, ok := specialized[function.Name.Name]; ok {
					require.NotEmpty(t, reason)
					continue
				}
				require.True(t, selected.MatchString(function.Name.Name), "%s in %s is skipped by run-differential.sh", function.Name.Name, file)
			}
		}
	}
}

func TestDifferentialRunnerEnablesHTTPGatewayScenarios(t *testing.T) {
	script, err := os.ReadFile("run-differential.sh")
	require.NoError(t, err)
	require.Contains(t, string(script), `REFERENCE_ETCD_GATEWAY_ENDPOINT="${REFERENCE_CLIENT_URL%/}"`)
	require.Contains(t, string(script), `KUBEBRAIN_GATEWAY_ENDPOINT="$KUBEBRAIN_GATEWAY_URL"`)
	require.Contains(t, string(script), `KUBEBRAIN_GATEWAY_URL="$(http_endpoint_url "$KUBEBRAIN_ENDPOINT")"`)
}

func TestDifferentialRunnerEnablesManualAlarmWithoutQuotaScenario(t *testing.T) {
	script, err := os.ReadFile("run-differential.sh")
	require.NoError(t, err)
	require.Contains(t, string(script), `KUBEBRAIN_NO_QUOTA_ENDPOINT="$KUBEBRAIN_ENDPOINT"`)
}

func TestDifferentialRunnerPassesOptionalMetricsEndpoints(t *testing.T) {
	script, err := os.ReadFile("run-differential.sh")
	require.NoError(t, err)
	require.Contains(t, string(script), `REFERENCE_ETCD_METRICS_ENDPOINT="${REFERENCE_CLIENT_URL%/}"`)
	require.Contains(t, string(script), `KUBEBRAIN_METRICS_ENDPOINT="$KUBEBRAIN_METRICS_ENDPOINT"`)
	require.Contains(t, string(script), `"$(http_endpoint_url "$KUBEBRAIN_METRICS_ENDPOINT")/metrics"`)
}

func TestDifferentialRunnerMatchesReferenceQuotaToTarget(t *testing.T) {
	script, err := os.ReadFile("run-differential.sh")
	require.NoError(t, err)
	require.Contains(t, string(script), `endpoint status -w json`)
	require.Contains(t, string(script), `.Status.dbSizeQuota // .Status.db_size_quota // 0`)
	require.Contains(t, string(script), `--quota-backend-bytes "$reference_quota"`)
}
