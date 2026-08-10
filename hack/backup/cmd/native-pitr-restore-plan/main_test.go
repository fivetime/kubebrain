package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/kubewharf/kubebrain/hack/backup/internal/nativepitr"
	"github.com/kubewharf/kubebrain/pkg/backend/coder"
	"github.com/stretchr/testify/require"
)

const testDigest = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"

func TestRunWritesCanonicalPlan(t *testing.T) {
	ks, err := coder.NewKeyspace("tenant-a")
	require.NoError(t, err)
	task := "tenant-a-pitr"
	info, ranges := "/tidb/br-stream/info/"+task, "/tidb/br-stream/ranges/"+task+"/"
	p := map[string]any{
		"format": nativepitr.PreflightFormat, "cluster_id": 11, "pd_addrs": []string{"pd:2379"},
		"keyspace": "tenant-a", "task_name": task, "start_key_hex": fmt.Sprintf("%x", ks.ObjectKeyspaceStart()), "end_key_hex": fmt.Sprintf("%x", ks.ObjectKeyspaceEnd()),
		"task_info_key": info, "task_ranges_prefix": ranges,
		"ownership_paths_checked": []string{info, ranges, "/tidb/br-stream/checkpoint/" + task + "/", "/tidb/br-stream/storage-checkpoint/" + task + "/", "/tidb/br-stream/pause/" + task, "/tidb/br-stream/last-error/" + task + "/"}, "task_name_available": true, "read_only": true,
		"stores": []map[string]any{{"id": 1, "address": "tikv:20160", "log_backup_service": "available"}},
	}
	b, err := json.Marshal(p)
	require.NoError(t, err)
	path := filepath.Join(t.TempDir(), "preflight.json")
	require.NoError(t, os.WriteFile(path, b, 0o600))
	in := nativepitr.Inputs{TaskStartTS: 100, FullBackupTS: 120, BackupMetaSHA256: testDigest, StoragePrefix: "s3://bucket/run", GlobalCheckpointTS: 200, AdvancerOwner: "owner-1", TargetClusterID: 22, EmptyWitnessSHA256: testDigest, RestoreTS: 180}
	var out bytes.Buffer
	require.NoError(t, run(path, in, &out))
	var plan nativepitr.Plan
	require.NoError(t, json.Unmarshal(out.Bytes(), &plan))
	require.NoError(t, plan.Validate())
	require.Equal(t, byte('\n'), out.Bytes()[out.Len()-1])
}

func TestRunRejectsMissingSource(t *testing.T) {
	require.ErrorContains(t, run("", nativepitr.Inputs{}, &bytes.Buffer{}), "required")
}
