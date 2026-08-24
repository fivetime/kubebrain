package compat

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
)

func writeFakeReferenceEtcd(t *testing.T, dir string) string {
	t.Helper()
	binary := filepath.Join(dir, "etcd")
	require.NoError(t, os.WriteFile(binary, []byte(`#!/usr/bin/env bash
set -euo pipefail
if [[ "${1:-}" == "--version" ]]; then
  printf 'etcd Version: 3.8.0-alpha.0\nGit SHA: d947b2086\n'
  exit 0
fi
exit 0
`), 0o755))
	return binary
}

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
	fakeEtcd := writeFakeReferenceEtcd(t, dir)
	fakeEtcdctl := filepath.Join(dir, "etcdctl")
	require.NoError(t, os.WriteFile(fakeEtcdctl, []byte(`#!/usr/bin/env bash
set -euo pipefail
if [[ "${1:-}" == "--version" ]]; then
  printf 'Git SHA: d947b2086\n'
  exit 0
fi
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
if [[ "$*" == *"get /registry/etcd-client-compat/ --prefix --limit=1 -w json"* ]]; then
  printf '%s\n' '{"count":0}'
  exit 0
fi
if [[ "$*" == *"get  --from-key --limit=1 -w json"* ]]; then
  printf '%s\n' '{"count":0}'
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
		"REFERENCE_ETCD_BIN=" + fakeEtcd,
		"REFERENCE_ETCD_EXPECTED_GIT_SHA=d947b2086",
		"ETCDCTL_BIN=" + fakeEtcdctl,
	}
	output, err := runDifferentialScript(t, env)
	require.Error(t, err)
	require.Contains(t, string(output), "advertised client URL is unreachable")
	require.Contains(t, string(output), "http://internal.invalid:3379")
}

func TestDifferentialRunnerRejectsMissingAdvertisedClientURLs(t *testing.T) {
	dir := t.TempDir()
	fakeEtcd := writeFakeReferenceEtcd(t, dir)
	fakeEtcdctl := filepath.Join(dir, "etcdctl")
	require.NoError(t, os.WriteFile(fakeEtcdctl, []byte(`#!/usr/bin/env bash
set -euo pipefail
if [[ "${1:-}" == "--version" ]]; then
  printf 'Git SHA: d947b2086\n'
  exit 0
fi
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
		"REFERENCE_ETCD_BIN=" + fakeEtcd,
		"REFERENCE_ETCD_EXPECTED_GIT_SHA=d947b2086",
		"ETCDCTL_BIN=" + fakeEtcdctl,
	}
	output, err := runDifferentialScript(t, env)
	require.Error(t, err)
	require.Contains(t, string(output), "returned no advertised client URLs")
}

func TestDifferentialRunnerChecksClusterLocalAdvertisedURLInSelectedPod(t *testing.T) {
	dir := t.TempDir()
	fakeEtcd := writeFakeReferenceEtcd(t, dir)
	fakeEtcdctl := filepath.Join(dir, "etcdctl")
	require.NoError(t, os.WriteFile(fakeEtcdctl, []byte(`#!/usr/bin/env bash
set -euo pipefail
if [[ "${1:-}" == "--version" ]]; then
  printf 'Git SHA: d947b2086\n'
  exit 0
fi
if [[ "$*" == *"member list -w json"* ]]; then
  printf '%s\n' '{"members":[{"name":"kubebrain-0","clientURLs":["http://internal.invalid:3379"]}]}'
  exit 0
fi
if [[ "$*" == *"endpoint status -w json"* ]]; then
  printf '%s\n' '[{"Status":{"dbSizeQuota":1073741824}}]'
  exit 0
fi
if [[ "$*" == *"get /registry/etcd-client-compat/ --prefix --limit=1 -w json"* ]]; then
  printf '%s\n' '{"count":0}'
  exit 0
fi
if [[ "$*" == *"get  --from-key --limit=1 -w json"* ]]; then
  printf '%s\n' '{"count":0}'
  exit 0
fi
if [[ "$*" == *"auth status -w json"* ]]; then
  printf '%s\n' '{"enabled":false,"authRevision":17}'
  exit 0
fi
if [[ "$*" == *"user list -w json"* ]]; then
  printf '%s\n' '{"users":[]}'
  exit 0
fi
if [[ "$*" == *"role list -w json"* ]]; then
  printf '%s\n' '{"roles":[]}'
  exit 0
fi
if [[ "$*" == *"lease list -w json"* ]]; then
  printf '%s\n' '{"leases":[]}'
  exit 0
fi
if [[ "$*" == *"alarm list -w json"* ]]; then
  printf '%s\n' '{"alarms":[]}'
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
		"REFERENCE_ETCD_BIN=" + fakeEtcd,
		"REFERENCE_ETCD_EXPECTED_GIT_SHA=d947b2086",
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
	require.Contains(t, string(script), `TEST_RUN_PATTERN="${TEST_RUN_PATTERN:-Differential(Against|$)}"`)
	require.Contains(t, string(script), `-run "$TEST_RUN_PATTERN"`,
		"the live runner must default to differential scenarios while allowing an explicit focused reproduction")
	require.NotContains(t, string(script), "-run Differential -count=1")
}

func TestDifferentialRunnerRequiresCompatPrefixConservation(t *testing.T) {
	script, err := os.ReadFile("run-differential.sh")
	require.NoError(t, err)
	content := string(script)
	require.Contains(t, content, "assert_compat_prefix_empty preflight")
	require.Contains(t, content, "assert_compat_prefix_empty postflight")
	require.Contains(t, content, "get /registry/etcd-client-compat/ --prefix --limit=1 -w json")
	require.Contains(t, content, "differential test package failed with status")
	require.Contains(t, content, ") || test_status=$?")
}

func TestDifferentialRunnerRequiresEntireUserKeyspaceConservation(t *testing.T) {
	script, err := os.ReadFile("run-differential.sh")
	require.NoError(t, err)
	content := string(script)
	require.Contains(t, content, "assert_user_keyspace_empty preflight")
	require.Contains(t, content, "assert_user_keyspace_empty postflight")
	require.Contains(t, content, "get '' --from-key --limit=1 -w json")
}

func TestDifferentialRunnerRequiresDisposableControlState(t *testing.T) {
	script, err := os.ReadFile("run-differential.sh")
	require.NoError(t, err)
	content := string(script)
	require.Contains(t, content, "assert_disposable_control_state preflight")
	require.Contains(t, content, "assert_disposable_control_state postflight")
	for _, command := range []string{"auth status", "user list", "role list", "lease list", "alarm list"} {
		require.Contains(t, content, command+" -w json")
	}
	require.Contains(t, content, "(.enabled // false) == false")
}

func TestDifferentialRunnerRejectsDirtyControlStateBeforeReferenceStart(t *testing.T) {
	dir := t.TempDir()
	fakeEtcd := writeFakeReferenceEtcd(t, dir)
	fakeEtcdctl := filepath.Join(dir, "etcdctl")
	require.NoError(t, os.WriteFile(fakeEtcdctl, []byte(`#!/usr/bin/env bash
set -euo pipefail
if [[ "${1:-}" == "--version" ]]; then
  printf 'Git SHA: d947b2086\n'
  exit 0
fi
case "$*" in
  *"member list -w json"*) printf '%s\n' '{"members":[{"name":"kubebrain-0","clientURLs":["http://127.0.0.1:22379"]}]}' ;;
  *"endpoint status -w json"*) printf '%s\n' '[{"Status":{"dbSizeQuota":1073741824}}]' ;;
  *"get /registry/etcd-client-compat/ --prefix --limit=1 -w json"*|*"get  --from-key --limit=1 -w json"*) printf '%s\n' '{"count":0}' ;;
  *"auth status -w json"*) printf '%s\n' '{"enabled":false,"authRevision":17}' ;;
  *"user list -w json"*) printf '%s\n' '{"users":[]}' ;;
  *"role list -w json"*) printf '%s\n' '{"roles":[]}' ;;
  *"lease list -w json"*) printf '%s\n' '{"leases":[{"ID":1234}]}' ;;
  *"alarm list -w json"*) printf '%s\n' '{"alarms":[]}' ;;
  *"endpoint health"*) ;;
  *) exit 1 ;;
esac
`), 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "curl"), []byte("#!/usr/bin/env bash\nexit 1\n"), 0o755))

	output, err := runDifferentialScript(t, []string{
		"PATH=" + dir + ":" + os.Getenv("PATH"),
		"KUBEBRAIN_ETCD_ENDPOINT=127.0.0.1:22379",
		"ALLOW_DESTRUCTIVE_DIFFERENTIAL=true",
		"REFERENCE_ETCD_BIN=" + fakeEtcd,
		"REFERENCE_ETCD_EXPECTED_GIT_SHA=d947b2086",
		"ETCDCTL_BIN=" + fakeEtcdctl,
	})
	require.Error(t, err)
	require.Contains(t, string(output), "KubeBrain lease set must be empty during preflight")
	require.NotContains(t, string(output), "reference etcd exited before becoming healthy")
}

func TestDifferentialRunnerRejectsDirtyCompatPrefixBeforeReferenceStart(t *testing.T) {
	dir := t.TempDir()
	fakeEtcd := writeFakeReferenceEtcd(t, dir)
	fakeEtcdctl := filepath.Join(dir, "etcdctl")
	require.NoError(t, os.WriteFile(fakeEtcdctl, []byte(`#!/usr/bin/env bash
set -euo pipefail
if [[ "${1:-}" == "--version" ]]; then
  printf 'Git SHA: d947b2086\n'
  exit 0
fi
if [[ "$*" == *"endpoint health"* ]]; then
  exit 0
fi
if [[ "$*" == *"member list -w json"* ]]; then
  printf '%s\n' '{"members":[{"name":"kubebrain-0","clientURLs":["http://127.0.0.1:22379"]}]}'
  exit 0
fi
if [[ "$*" == *"endpoint status -w json"* ]]; then
  printf '%s\n' '[{"Status":{"dbSizeQuota":1073741824}}]'
  exit 0
fi
if [[ "$*" == *"get /registry/etcd-client-compat/ --prefix --limit=1 -w json"* ]]; then
  printf '%s\n' '{"count":1}'
  exit 0
fi
exit 1
`), 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "curl"), []byte("#!/usr/bin/env bash\nexit 1\n"), 0o755))

	output, err := runDifferentialScript(t, []string{
		"PATH=" + dir + ":" + os.Getenv("PATH"),
		"KUBEBRAIN_ETCD_ENDPOINT=127.0.0.1:22379",
		"ALLOW_DESTRUCTIVE_DIFFERENTIAL=true",
		"REFERENCE_ETCD_BIN=" + fakeEtcd,
		"REFERENCE_ETCD_EXPECTED_GIT_SHA=d947b2086",
		"ETCDCTL_BIN=" + fakeEtcdctl,
	})
	require.Error(t, err)
	require.Contains(t, string(output), "compat test prefix is not empty during preflight")
	require.NotContains(t, string(output), "reference etcd exited before becoming healthy")
}

func TestDifferentialRunnerSeparatesGRPCAuthorityFromGatewayURL(t *testing.T) {
	script, err := os.ReadFile("run-differential.sh")
	require.NoError(t, err)
	content := string(script)
	require.Contains(t, content, `KUBEBRAIN_GATEWAY_URL="$(http_endpoint_url "$KUBEBRAIN_ENDPOINT")"`)
	require.Contains(t, content, `KUBEBRAIN_GRPC_ENDPOINT="$(grpc_endpoint_authority "$KUBEBRAIN_ENDPOINT")"`)
	require.Contains(t, content, `http://*) endpoint="${endpoint#http://}"`)
	require.Contains(t, content, `https://*) endpoint="${endpoint#https://}"`)
	require.Contains(t, content, `KUBEBRAIN_ETCD_ENDPOINT="$KUBEBRAIN_GRPC_ENDPOINT"`)
	require.Contains(t, content, `KUBEBRAIN_NO_QUOTA_ENDPOINT="$KUBEBRAIN_GRPC_ENDPOINT"`)
	require.Contains(t, content, `KUBEBRAIN_GATEWAY_ENDPOINT="$KUBEBRAIN_GATEWAY_URL"`)
}

func TestDefaultReferenceEndpointTestsAreSelectedByDifferentialRunner(t *testing.T) {
	files, err := filepath.Glob("*_test.go")
	require.NoError(t, err)
	selected := regexp.MustCompile(`Differential(Against|$)`)
	defaultReferenceEndpoints := map[string]struct{}{
		"REFERENCE_ETCD_ENDPOINT":         {},
		"REFERENCE_ETCD_GATEWAY_ENDPOINT": {},
		"REFERENCE_ETCD_METRICS_ENDPOINT": {},
	}
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
			readReferenceEndpoints := make(map[string]struct{})
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
				if !ok || packageName.Name != "os" || !literal || argument.Kind != token.STRING {
					return true
				}
				environmentName, unquoteErr := strconv.Unquote(argument.Value)
				if unquoteErr != nil {
					return true
				}
				if _, ok := defaultReferenceEndpoints[environmentName]; ok {
					readReferenceEndpoints[environmentName] = struct{}{}
				}
				return true
			})
			if len(readReferenceEndpoints) > 0 {
				if reason, ok := specialized[function.Name.Name]; ok {
					require.NotEmpty(t, reason)
					continue
				}
				require.True(t, selected.MatchString(function.Name.Name), "%s in %s reads default reference endpoint(s) %v but is skipped by run-differential.sh", function.Name.Name, file, readReferenceEndpoints)
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
	require.Contains(t, string(script), `KUBEBRAIN_NO_QUOTA_ENDPOINT="$KUBEBRAIN_GRPC_ENDPOINT"`)
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
