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
	require.Contains(t, string(script), "direct KubeBrain replica metrics preflight failed")
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
