package compat

import (
	"bufio"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDirectReplicaConsistencyRunnerFailsClosed(t *testing.T) {
	script, err := os.ReadFile("run-direct-replica-consistency.sh")
	require.NoError(t, err)
	manifest, err := os.ReadFile("direct-replica-scopes.txt")
	require.NoError(t, err)
	content := string(script) + string(manifest)
	require.Contains(t, string(script), "ALLOW_MUTATING_DIRECT_REPLICA_CONSISTENCY")
	require.Contains(t, string(script), "ALLOW_DESTRUCTIVE_DIRECT_REPLICA_CONSISTENCY")
	require.Contains(t, string(script), "one healthy three-member topology with an in-set leader")
	require.Contains(t, string(script), "assert_test_prefixes_empty postflight")
	require.Contains(t, string(script), "/dbaas-physical-traffic/")
	require.Contains(t, string(script), "changed the live lease set")
	require.Contains(t, string(script), "changed the live alarm set")
	require.Contains(t, string(script), "test package failed with status")
	require.Contains(t, content, "TestHashKVSnapshotIsConsistentAcrossKubeBrainReplicas")
	require.Contains(t, content, "TestMaintenanceHashKVSemantics")
	require.Contains(t, content, "TestMaintenanceHashKVHeaderStaysAtHashedSnapshotUnderWrites")
	require.Contains(t, content, "TestMaintenanceHashKVMatchesAcrossMembers")
	require.Contains(t, string(script), "KUBEBRAIN_MEMBERLIST_DIAL_ENDPOINTS")
	require.Contains(t, content, "TestHashKVCompactionConvergesAcrossKubeBrainReplicas")
	require.Contains(t, content, "TestMaintenanceHashKVStaysStableAcrossPhysicalCompaction")
	require.Contains(t, content, "TestPhysicalCompactionUnderTraffic")
	require.Contains(t, content, "TestLeaseSurvivesCompaction")
	require.Contains(t, content, "TestLeaseReadAndRevokeAcrossDirectReplicas")
	require.Contains(t, content, "TestMutationResponseHeadersAcrossDirectReplicas")
	require.Contains(t, content, "TestQuotaAlarmCrossEndpointDisarm")
	require.Contains(t, content, "TestWatchLocalControlResponsesAcrossDirectReplicas")
	require.Contains(t, content, "TestStatusAlarmCrossEndpointVisibility")
	require.Contains(t, content, "TestCorruptAlarmCrossEndpoint")
	require.Contains(t, content, "TestCombinedAlarmCrossEndpointStateTransition")
	require.Contains(t, content, "TestConcurrencyResponseHeadersAcrossDirectReplicas")
	require.Contains(t, content, "TestContendedConcurrencyResponseHeadersAcrossDirectReplicas")
	require.Contains(t, content, "TestControlResponseHeadersAcrossDirectReplicas")
	require.Contains(t, content, "TestUnknownAlarmMetricConvergesAcrossKubeBrainReplicas")
	require.Contains(t, string(script), "endpoint topology or endpoint-to-member mapping drifted")
	require.Contains(t, string(script), "direct KubeBrain endpoint response identity drifted")
	require.Contains(t, string(script), "direct KubeBrain replica metrics identity drifted")
	require.Contains(t, string(script), "--max-filesize 1048576")
	require.Contains(t, string(script), "for index in \"${!kubebrain_endpoints[@]}\"")
}

func TestDirectReplicaScopeManifestOwnsDirectEnvironmentTests(t *testing.T) {
	registered := readDirectReplicaScopeManifest(t)
	files, err := filepath.Glob("*_test.go")
	require.NoError(t, err)
	directEnvironment := map[string]struct{}{
		"KUBEBRAIN_DIRECT_ENDPOINTS": {}, "KUBEBRAIN_MULTI_ENDPOINTS": {},
		"KUBEBRAIN_MULTI_QUOTA_ENDPOINTS": {}, "KUBEBRAIN_ALARM_METRIC_ENDPOINTS": {},
		"KUBEBRAIN_ALARM_METRICS_ENDPOINTS": {}, "KUBEBRAIN_MEMBERLIST_ENDPOINTS_DIRECT": {},
		"KUBEBRAIN_MEMBERLIST_DIAL_ENDPOINTS": {},
	}
	found := make(map[string]struct{})
	for _, file := range files {
		parsed, parseErr := parser.ParseFile(token.NewFileSet(), file, nil, 0)
		require.NoError(t, parseErr)
		for _, declaration := range parsed.Decls {
			function, ok := declaration.(*ast.FuncDecl)
			if !ok || function.Body == nil || function.Name == nil || !strings.HasPrefix(function.Name.Name, "Test") {
				continue
			}
			readsDirectEnvironment := false
			ast.Inspect(function.Body, func(node ast.Node) bool {
				call, ok := node.(*ast.CallExpr)
				if !ok || len(call.Args) != 1 {
					return true
				}
				selector, ok := call.Fun.(*ast.SelectorExpr)
				if !ok {
					return true
				}
				pkg, pkgOK := selector.X.(*ast.Ident)
				literal, literalOK := call.Args[0].(*ast.BasicLit)
				if !pkgOK || pkg.Name != "os" || selector.Sel.Name != "Getenv" || !literalOK || literal.Kind != token.STRING {
					return true
				}
				name, unquoteErr := strconv.Unquote(literal.Value)
				if unquoteErr == nil {
					_, readsDirectEnvironment = directEnvironment[name]
				}
				return !readsDirectEnvironment
			})
			if readsDirectEnvironment {
				found[function.Name.Name] = struct{}{}
				_, owned := registered[function.Name.Name]
				require.True(t, owned, "%s in %s reads a direct-replica runner environment variable but has no scope in direct-replica-scopes.txt", function.Name.Name, file)
			}
		}
	}
	for testName := range registered {
		_, exists := found[testName]
		if !exists {
			// Some runner-owned tests intentionally use the common live endpoint.
			require.Contains(t, allCompatTestNames(t, files), testName, "manifest names a missing Go test")
		}
	}
}

func readDirectReplicaScopeManifest(t *testing.T) map[string]struct{} {
	t.Helper()
	file, err := os.Open("direct-replica-scopes.txt")
	require.NoError(t, err)
	defer file.Close()
	registered := make(map[string]struct{})
	seenRows := make(map[string]struct{})
	validScopes := map[string]struct{}{"all": {}, "all-metrics": {}, "hashkv-compaction": {}, "compaction": {}, "memberlist-hash": {}}
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		fields := strings.Fields(line)
		require.Len(t, fields, 2, "invalid manifest row %q", line)
		_, valid := validScopes[fields[0]]
		require.True(t, valid, "unknown direct-replica scope in %q", line)
		require.Regexp(t, `^Test[A-Za-z0-9_]+$`, fields[1])
		_, duplicate := seenRows[line]
		require.False(t, duplicate, "duplicate manifest row %q", line)
		seenRows[line] = struct{}{}
		registered[fields[1]] = struct{}{}
	}
	require.NoError(t, scanner.Err())
	require.NotEmpty(t, registered)
	return registered
}

func allCompatTestNames(t *testing.T, files []string) map[string]struct{} {
	t.Helper()
	names := make(map[string]struct{})
	for _, file := range files {
		parsed, err := parser.ParseFile(token.NewFileSet(), file, nil, 0)
		require.NoError(t, err)
		for _, declaration := range parsed.Decls {
			if function, ok := declaration.(*ast.FuncDecl); ok && function.Name != nil && strings.HasPrefix(function.Name.Name, "Test") {
				names[function.Name.Name] = struct{}{}
			}
		}
	}
	return names
}

func TestDirectReplicaConsistencyRunnerRejectsInvalidScopeBeforeDependencies(t *testing.T) {
	output, err := runCompatScriptCommand(t, "run-direct-replica-consistency.sh", []string{
		"PATH=" + t.TempDir() + ":/usr/bin:/bin",
		"TEST_SCOPE=unknown",
	})
	require.Error(t, err)
	require.Contains(t, string(output), "TEST_SCOPE must be all, hashkv-compaction, compaction, or memberlist-hash")
	require.NotContains(t, string(output), "missing required command")
}

func TestDirectReplicaConsistencyRunnerRejectsInvalidDestructiveApprovalBeforeDependencies(t *testing.T) {
	output, err := runCompatScriptCommand(t, "run-direct-replica-consistency.sh", []string{
		"PATH=" + t.TempDir() + ":/usr/bin:/bin",
		"ALLOW_DESTRUCTIVE_DIRECT_REPLICA_CONSISTENCY=maybe",
	})
	require.Error(t, err)
	require.Contains(t, string(output), "ALLOW_DESTRUCTIVE_DIRECT_REPLICA_CONSISTENCY must be true or false")
	require.NotContains(t, string(output), "missing required command")
}

func TestDirectReplicaConsistencyRunnerRejectsMetricsEndpointCountBeforeDependencies(t *testing.T) {
	output, err := runCompatScriptCommand(t, "run-direct-replica-consistency.sh", []string{
		"PATH=" + t.TempDir() + ":/usr/bin:/bin",
		"KUBEBRAIN_DIRECT_ENDPOINTS=127.0.0.1:1,127.0.0.1:2,127.0.0.1:3",
		"KUBEBRAIN_DIRECT_METRICS_ENDPOINTS=http://127.0.0.1:4,http://127.0.0.1:5",
		"ALLOW_MUTATING_DIRECT_REPLICA_CONSISTENCY=true",
	})
	require.Error(t, err)
	require.Contains(t, string(output), "KUBEBRAIN_DIRECT_METRICS_ENDPOINTS must contain exactly three non-empty")
	require.NotContains(t, string(output), "missing required command")
}

func TestDirectReplicaConsistencyRunnerRejectsInvalidApprovalBeforeDependencies(t *testing.T) {
	output, err := runCompatScriptCommand(t, "run-direct-replica-consistency.sh", []string{
		"PATH=" + t.TempDir() + ":/usr/bin:/bin",
		"ALLOW_MUTATING_DIRECT_REPLICA_CONSISTENCY=maybe",
	})
	require.Error(t, err)
	require.Contains(t, string(output), "ALLOW_MUTATING_DIRECT_REPLICA_CONSISTENCY must be true or false")
	require.NotContains(t, string(output), "missing required command")
}

func TestDirectReplicaConsistencyRunnerRequiresEndpointsBeforeDependencies(t *testing.T) {
	output, err := runCompatScriptCommand(t, "run-direct-replica-consistency.sh", []string{
		"PATH=" + t.TempDir() + ":/usr/bin:/bin",
		"ALLOW_MUTATING_DIRECT_REPLICA_CONSISTENCY=true",
	})
	require.Error(t, err)
	require.Contains(t, string(output), "set KUBEBRAIN_DIRECT_ENDPOINTS")
}

func TestDirectReplicaConsistencyRunnerRejectsEndpointCountBeforeDependencies(t *testing.T) {
	output, err := runCompatScriptCommand(t, "run-direct-replica-consistency.sh", []string{
		"PATH=" + t.TempDir() + ":/usr/bin:/bin",
		"KUBEBRAIN_DIRECT_ENDPOINTS=127.0.0.1:1,127.0.0.1:2",
		"ALLOW_MUTATING_DIRECT_REPLICA_CONSISTENCY=true",
	})
	require.Error(t, err)
	require.Contains(t, string(output), "must contain exactly three non-empty")
	require.NotContains(t, string(output), "missing required command")
}

func TestDirectReplicaConsistencyRunnerRequiresMutationApproval(t *testing.T) {
	output, err := runCompatScriptCommand(t, "run-direct-replica-consistency.sh", []string{
		"PATH=" + t.TempDir() + ":/usr/bin:/bin",
		"KUBEBRAIN_DIRECT_ENDPOINTS=127.0.0.1:1,127.0.0.1:2,127.0.0.1:3",
	})
	require.Error(t, err)
	require.Contains(t, string(output), "refusing mutating direct-replica consistency suite")
	require.NotContains(t, string(output), "missing required command")
}

func TestDirectReplicaConsistencyRunnerRequiresDestructiveApprovalForHashKVCompaction(t *testing.T) {
	for _, scope := range []string{"hashkv-compaction", "compaction"} {
		t.Run(scope, func(t *testing.T) {
			output, err := runCompatScriptCommand(t, "run-direct-replica-consistency.sh", []string{
				"PATH=" + t.TempDir() + ":/usr/bin:/bin",
				"KUBEBRAIN_DIRECT_ENDPOINTS=127.0.0.1:1,127.0.0.1:2,127.0.0.1:3",
				"ALLOW_MUTATING_DIRECT_REPLICA_CONSISTENCY=true",
				"TEST_SCOPE=" + scope,
			})
			require.Error(t, err)
			require.Contains(t, string(output), "compaction advances the target instance's global compact revision")
			require.NotContains(t, string(output), "missing required command")
		})
	}
}

func writeDirectReplicaRunnerFakes(t *testing.T, dir string) (string, string) {
	t.Helper()
	goRan := filepath.Join(dir, "go-ran")
	fakeGo := filepath.Join(dir, "go")
	require.NoError(t, os.WriteFile(fakeGo, []byte(`#!/usr/bin/env bash
set -euo pipefail
if [[ "${1:-}" == version ]]; then
  exit 0
fi
touch "$GO_RAN"
`), 0o755))
	fakeEtcdctl := filepath.Join(dir, "etcdctl")
	require.NoError(t, os.WriteFile(fakeEtcdctl, []byte(`#!/usr/bin/env bash
set -euo pipefail
if [[ "${1:-}" == "--version" ]]; then
  printf 'Git SHA: d947b2086\n'
  exit 0
fi
endpoint=""
for arg in "$@"; do
  case "$arg" in --endpoints=*) endpoint="${arg#--endpoints=}" ;; esac
done
case "$endpoint" in
  e0) member=101 ;;
  e1) member=102 ;;
  e2) member=103 ;;
  *) exit 91 ;;
esac
revision=10
if [[ -f "$GO_RAN" ]]; then
  revision=20
  if [[ "${DRIFT_POSTFLIGHT_MEMBER:-false}" == true && "$endpoint" == e1 ]]; then
    member=103
  fi
fi
header="{\"cluster_id\":900,\"member_id\":${member},\"revision\":${revision},\"raft_term\":4}"
case "$*" in
  *"endpoint status -w json"*) printf '[{"Status":{"header":%s,"leader":101}}]\n' "$header" ;;
  *" get "*" -w json"*) printf '{"header":%s,"count":0,"kvs":[],"more":false}\n' "$header" ;;
  *"lease list -w json"*) printf '{"cluster_id":900,"member_id":%s,"revision":%s,"raft_term":4,"leases":[]}\n' "$member" "$revision" ;;
  *"alarm list -w json"*) printf '{"header":%s,"alarms":[]}\n' "$header" ;;
  *) exit 92 ;;
esac
`), 0o755))
	fakeCurl := filepath.Join(dir, "curl")
	require.NoError(t, os.WriteFile(fakeCurl, []byte(`#!/usr/bin/env bash
set -euo pipefail
output=""
url=""
while [[ "$#" -gt 0 ]]; do
  case "$1" in
    --output) output="$2"; shift 2 ;;
    --) shift; url="$1"; shift ;;
    *) shift ;;
  esac
done
case "$url" in
  http://m0/metrics) member=65 ;;
  http://m1/metrics) member=66 ;;
  http://m2/metrics) member=67 ;;
  *) exit 93 ;;
esac
if [[ "${DRIFT_METRICS_MEMBER:-false}" == true && "$url" == http://m1/metrics ]]; then
  member=65
fi
printf 'etcd_server_id{cluster="default",server_id="%s"} 1\n' "$member" >"$output"
`), 0o755))
	return fakeEtcdctl, goRan
}

func directReplicaRunnerFakeEnv(dir, fakeEtcdctl, goRan string) []string {
	return []string{
		"PATH=" + dir + ":" + os.Getenv("PATH"),
		"KUBEBRAIN_DIRECT_ENDPOINTS=e0,e1,e2",
		"KUBEBRAIN_DIRECT_METRICS_ENDPOINTS=http://m0,http://m1,http://m2",
		"ALLOW_MUTATING_DIRECT_REPLICA_CONSISTENCY=true",
		"REFERENCE_ETCD_EXPECTED_GIT_SHA=d947b2086",
		"ETCDCTL_BIN=" + fakeEtcdctl,
		"GO_RAN=" + goRan,
	}
}

func TestDirectReplicaConsistencyRunnerRejectsPostflightEndpointMemberDrift(t *testing.T) {
	dir := t.TempDir()
	fakeEtcdctl, goRan := writeDirectReplicaRunnerFakes(t, dir)
	env := append(directReplicaRunnerFakeEnv(dir, fakeEtcdctl, goRan), "DRIFT_POSTFLIGHT_MEMBER=true")
	output, err := runCompatScriptCommand(t, "run-direct-replica-consistency.sh", env)
	require.Error(t, err)
	require.Contains(t, string(output), "endpoint topology or endpoint-to-member mapping drifted")
	_, statErr := os.Stat(goRan)
	require.NoError(t, statErr, "postflight drift must be observed after the selected Go tests run")
}

func TestDirectReplicaConsistencyRunnerRejectsMetricsMemberDriftBeforeTests(t *testing.T) {
	dir := t.TempDir()
	fakeEtcdctl, goRan := writeDirectReplicaRunnerFakes(t, dir)
	env := append(directReplicaRunnerFakeEnv(dir, fakeEtcdctl, goRan), "DRIFT_METRICS_MEMBER=true")
	output, err := runCompatScriptCommand(t, "run-direct-replica-consistency.sh", env)
	require.Error(t, err)
	require.Contains(t, string(output), "metrics identity drifted during preflight")
	_, statErr := os.Stat(goRan)
	require.ErrorIs(t, statErr, os.ErrNotExist, "metrics drift must fail before any selected Go test")
}
