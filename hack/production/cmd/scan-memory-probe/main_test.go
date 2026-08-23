package main

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestValidateRequiresMultipleUniqueNamespacesAndBoundedBudgets(t *testing.T) {
	require.NoError(t, validate([]string{"tenant-a", "tenant-b"}, "probe=a", 50, 100, 1<<20, 256<<20, time.Minute))
	for _, tc := range []struct {
		name       string
		namespaces []string
		page, max  int64
	}{
		{name: "one namespace", namespaces: []string{"tenant-a"}, page: 50, max: 100},
		{name: "duplicate", namespaces: []string{"tenant-a", "tenant-a"}, page: 50, max: 100},
		{name: "invalid", namespaces: []string{"tenant-a", "tenant.b"}, page: 50, max: 100},
		{name: "not paginated", namespaces: []string{"tenant-a", "tenant-b"}, page: 100, max: 100},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.Error(t, validate(tc.namespaces, "probe=a", tc.page, tc.max, 1<<20, 256<<20, time.Minute))
		})
	}
	require.Error(t, validate([]string{"tenant-a", "tenant-b"}, "", 50, 100, 1<<20, 256<<20, time.Minute))
	require.Error(t, validate([]string{"tenant-a", "tenant-b"}, "bad selector", 50, 100, 1<<20, 256<<20, time.Minute))
}

func TestReadCgroupRequiresFiniteV2MemoryContract(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, "memory.current"), []byte("123\n"), 0o600))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "memory.peak"), []byte("456\n"), 0o600))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "memory.max"), []byte("268435456\n"), 0o600))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "memory.events"), []byte("low 0\nhigh 0\nmax 2\noom 3\noom_kill 1\noom_group_kill 0\n"), 0o600))

	snapshot, err := readCgroup(dir)
	require.NoError(t, err)
	require.Equal(t, cgroupSnapshot{Current: 123, Peak: 456, Limit: 268435456, OOM: 3, OOMKill: 1}, snapshot)

	require.NoError(t, os.WriteFile(filepath.Join(dir, "memory.max"), []byte("max\n"), 0o600))
	_, err = readCgroup(dir)
	require.ErrorContains(t, err, "unlimited")
}
