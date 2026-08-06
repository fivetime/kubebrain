package compat

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestRecentUpstreamAuditIsRecorded pins the DBaaS compatibility audit for the
// latest screened /root/etcd public-surface commits. The audit intentionally
// distinguishes portable etcd client/API behavior from Raft/WAL/bbolt/internal
// test-harness changes that KubeBrain must not mechanically copy into its
// TiKV/PD-backed dataplane.
func TestRecentUpstreamAuditIsRecorded(t *testing.T) {
	planPath := filepath.Join("..", "..", "docs", "dbaas_compatibility_plan_cn.md")
	contents, err := os.ReadFile(planPath)
	require.NoError(t, err)
	plan := string(contents)

	for _, needle := range []string{
		"A3765",
		"近期 upstream public-surface 覆盖审计",
		"2308ce157",
		"e84205af4",
		"b02869083",
		"4968db847",
		"0004f8e75",
		"9cdb1cf8",
		"71a9ffe87",
		"7624a8a2e",
		"8ce417fa0",
		"e40f9c68e",
		"695442b44",
		"fbba4f46e",
		"5d3241d01",
		"bd2427cd2",
		"c5d1b8a02",
		"b05c85872",
		"c51910e45",
		"A3770",
		"A3771",
		"A3772",
		"A3773",
		"A3774",
		"A3775",
		"43a4c4ecd",
		"a81b6d623",
		"f55d8a061",
		"f5912263c",
		"967feb0d6",
		"114f6ad80",
		"da0321d1",
		"A3776",
		"0d20d7da7",
		"236179af0",
		"4f081fb1a",
		"932dc99f1",
		"7c528e856",
		"a1cb0a244",
		"910fdba06",
		"fa8a5a248",
		"2214d9f13",
		"1fd87206f",
		"84862dbd6",
		"fd604517e",
		"A3777",
		"887c8486d",
		"bc5056f83",
		"b90bc8c3",
		"A3778",
		"a2987fdee",
		"9dffaa350",
		"a07ecd124",
		"204097b19",
		"59ce0ce31",
		"A3779",
		"0c68e485a",
		"5037a98f7",
		"871779c21",
		"b54c88406",
		"55988933b",
		"A3780",
		"43a6f8fa7",
		"e561d3ac9",
	} {
		require.Contains(t, plan, needle)
	}
}

func TestObservabilityDocIncludesUpstreamMetricParity(t *testing.T) {
	docPath := filepath.Join("..", "..", "docs", "observability_cn.md")
	contents, err := os.ReadFile(docPath)
	require.NoError(t, err)
	doc := string(contents)

	for _, needle := range []string{
		"etcd_server_request_duration_seconds",
		"etcd_debugging_server_watch_send_loop_watch_stream_duration_seconds",
		"etcd_debugging_server_watch_send_loop_watch_stream_duration_per_event_seconds",
		"etcd_debugging_server_watch_send_loop_control_stream_duration_seconds",
		"etcd_debugging_server_watch_send_loop_progress_duration_seconds",
	} {
		require.Contains(t, doc, needle)
	}
}
