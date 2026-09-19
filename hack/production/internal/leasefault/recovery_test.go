package leasefault

import (
	"math"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"

	"github.com/stretchr/testify/require"
)

func recoveryPlan() ProtocolRecovery {
	return ProtocolRecovery{Owner: "fault-fresh.abcd1234", NamespaceUID: "namespace-uid", StatefulSetUID: "sts-uid",
		ClusterID: math.MaxUint64, AlarmMemberID: 9007199254740993, LeaseID: math.MinInt64, Key: "/acceptance/fresh-owned-key"}
}

func TestProtocolRecoveryCreateOnceAndReload(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, os.Chmod(dir, 0700))
	plan := recoveryPlan()
	require.NoError(t, ArmProtocolRecovery(dir, plan))
	got, err := LoadProtocolRecovery(dir, plan)
	require.NoError(t, err)
	require.Equal(t, plan, got)
	data, err := os.ReadFile(filepath.Join(dir, protocolRecoveryFile))
	require.NoError(t, err)
	require.Contains(t, string(data), `"cluster_id":"18446744073709551615"`)
	require.Error(t, ArmProtocolRecovery(dir, plan))
	after, err := os.ReadFile(filepath.Join(dir, protocolRecoveryFile))
	require.NoError(t, err)
	require.Equal(t, data, after)
	plan.LeaseID++
	got, err = LoadProtocolRecovery(dir, plan)
	require.Error(t, err)
	require.Zero(t, got)
}

func TestProtocolRecoveryRejectsUnsafeRecords(t *testing.T) {
	for _, mode := range []string{"directory-link", "public-directory", "file-link", "fifo", "public-file", "truncated", "duplicate", "unknown", "oversize", "missing"} {
		t.Run(mode, func(t *testing.T) {
			dir := t.TempDir()
			require.NoError(t, os.Chmod(dir, 0700))
			plan := recoveryPlan()
			require.NoError(t, ArmProtocolRecovery(dir, plan))
			path := filepath.Join(dir, protocolRecoveryFile)
			data, err := os.ReadFile(path)
			require.NoError(t, err)
			switch mode {
			case "directory-link":
				link := filepath.Join(t.TempDir(), "owner")
				require.NoError(t, os.Symlink(dir, link))
				dir = link
			case "public-directory":
				require.NoError(t, os.Chmod(dir, 0755))
			case "file-link":
				require.NoError(t, os.Rename(path, path+".original"))
				require.NoError(t, os.Symlink(path+".original", path))
			case "fifo":
				require.NoError(t, os.Remove(path))
				require.NoError(t, syscall.Mkfifo(path, 0600))
			case "public-file":
				require.NoError(t, os.Chmod(path, 0644))
			case "truncated":
				require.NoError(t, os.WriteFile(path, data[:len(data)/2], 0600))
			case "duplicate":
				require.NoError(t, os.WriteFile(path, []byte(strings.Replace(string(data), "{", `{"owner":"fault-fresh.abcd1234",`, 1)), 0600))
			case "unknown":
				require.NoError(t, os.WriteFile(path, []byte(strings.Replace(string(data), "{", `{"skip_recovery":true,`, 1)), 0600))
			case "oversize":
				require.NoError(t, os.WriteFile(path, []byte(strings.Repeat(" ", 4097)), 0600))
			case "missing":
				require.NoError(t, os.Remove(path))
			}
			got, err := LoadProtocolRecovery(dir, plan)
			require.Error(t, err)
			require.Zero(t, got)
		})
	}
}

func TestProtocolRecoveryInvalidPlanDoesNotArm(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, os.Chmod(dir, 0700))
	plan := recoveryPlan()
	plan.Key = "/business/key"
	require.Error(t, ArmProtocolRecovery(dir, plan))
	_, err := os.Stat(filepath.Join(dir, protocolRecoveryFile))
	require.True(t, os.IsNotExist(err))
}
