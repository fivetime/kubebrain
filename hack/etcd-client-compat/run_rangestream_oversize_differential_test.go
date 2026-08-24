package compat

import (
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRangeStreamOversizeDifferentialRunnerFailsClosed(t *testing.T) {
	script, err := os.ReadFile("run-rangestream-oversize-differential.sh")
	require.NoError(t, err)
	content := string(script)
	require.Contains(t, content, "ALLOW_DESTRUCTIVE_RANGESTREAM_OVERSIZE")
	require.Contains(t, content, "set KUBE_CONTEXT explicitly")
	require.Contains(t, content, "set NAMESPACE explicitly")
	require.Contains(t, content, "set KEYSPACE to a fresh disposable TiKV keyspace")
	require.Contains(t, content, "temporary oversize Pod already exists; refusing to delete it")
	require.Contains(t, content, "temporary oversize Service already exists; refusing to delete it")
	require.Contains(t, content, `if [[ "$pod_created" == true ]]`)
	require.Contains(t, content, `if [[ "$service_created" == true ]]`)
	require.Contains(t, content, "assert_clean_kubebrain_endpoint preflight")
	require.Equal(t, 2, strings.Count(content, "assert_clean_kubebrain_endpoint postflight"))
	require.Contains(t, content, ") || test_status=$?")
}

func TestRangeStreamOversizeDifferentialRunnerRejectsInvalidApprovalBeforeDependencies(t *testing.T) {
	output, err := runCompatScriptCommand(t, "run-rangestream-oversize-differential.sh", []string{
		"PATH=" + t.TempDir() + ":/usr/bin:/bin",
		"ALLOW_DESTRUCTIVE_RANGESTREAM_OVERSIZE=maybe",
	})
	require.Error(t, err)
	require.Contains(t, string(output), "ALLOW_DESTRUCTIVE_RANGESTREAM_OVERSIZE must be true or false")
	require.NotContains(t, string(output), "missing required command")
}

func TestRangeStreamOversizeDifferentialRunnerRequiresExplicitTargetBeforeDependencies(t *testing.T) {
	for _, testCase := range []struct {
		name string
		env  []string
		want string
	}{
		{name: "context", env: nil, want: "set KUBE_CONTEXT explicitly"},
		{name: "namespace", env: []string{"KUBE_CONTEXT=kind-test"}, want: "set NAMESPACE explicitly"},
		{name: "keyspace", env: []string{"KUBE_CONTEXT=kind-test", "NAMESPACE=test"}, want: "set KEYSPACE to a fresh disposable TiKV keyspace"},
		{name: "approval", env: []string{"KUBE_CONTEXT=kind-test", "NAMESPACE=test", "KEYSPACE=fresh"}, want: "refusing destructive RangeStream oversize differential"},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			env := append([]string{"PATH=" + t.TempDir() + ":/usr/bin:/bin"}, testCase.env...)
			output, err := runCompatScriptCommand(t, "run-rangestream-oversize-differential.sh", env)
			require.Error(t, err)
			require.Contains(t, string(output), testCase.want)
			require.NotContains(t, string(output), "missing required command")
		})
	}
}
