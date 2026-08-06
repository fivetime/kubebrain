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
		"e40f9c68e",
		"695442b44",
		"fbba4f46e",
		"5d3241d01",
		"bd2427cd2",
		"c5d1b8a02",
		"b05c85872",
		"c51910e45",
		"A3770",
	} {
		require.Contains(t, plan, needle)
	}
}
