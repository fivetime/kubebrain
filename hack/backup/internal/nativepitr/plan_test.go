package nativepitr

import (
	"encoding/hex"
	"encoding/json"
	"strings"
	"testing"

	"github.com/kubewharf/kubebrain/pkg/backend/coder"
	"github.com/stretchr/testify/require"
)

const digest = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"

func validPreflight() Preflight {
	ks, _ := coder.NewKeyspace("tenant-a")
	task := "tenant-a-pitr"
	info, ranges := "/tidb/br-stream/info/"+task, "/tidb/br-stream/ranges/"+task+"/"
	p := Preflight{Format: PreflightFormat, ClusterID: 11, PDAddrs: []string{"pd:2379"}, Keyspace: "tenant-a", TaskName: task, StartKeyHex: hex.EncodeToString(ks.ObjectKeyspaceStart()), EndKeyHex: hex.EncodeToString(ks.ObjectKeyspaceEnd()), TaskInfoKey: info, TaskRangesKey: ranges, OwnershipKeys: []string{info, ranges, "/tidb/br-stream/checkpoint/" + task + "/", "/tidb/br-stream/storage-checkpoint/" + task + "/", "/tidb/br-stream/pause/" + task, "/tidb/br-stream/last-error/" + task + "/"}, TaskAvailable: true, ReadOnly: true}
	p.Stores = append(p.Stores, struct {
		ID      uint64 `json:"id"`
		Address string `json:"address"`
		Service string `json:"log_backup_service"`
	}{ID: 1, Address: "tikv:20160", Service: "available"})
	return p
}

func validReceiptPlan(t *testing.T) Plan {
	t.Helper()
	task, _ := readyTask(t)
	full, err := BuildFullSnapshot(task, digest, "s3://bucket/immutable/full-1", fullMeta(t, task))
	require.NoError(t, err)
	plan, err := BuildFromReceipts(task, full, readyReceiptFor(task), ReceiptPlanInputs{TaskCreateSHA256: digest, TargetClusterID: 22, EmptyWitnessSHA256: digest, RestoreTS: 140})
	require.NoError(t, err)
	return plan
}

func TestPlanRejectsBrokenChain(t *testing.T) {
	tests := []struct {
		name string
		edit func(*Plan)
		want string
	}{
		{"log starts after snapshot", func(p *Plan) { p.Log.StartTS = 121 }, "start no later"},
		{"task committed after snapshot", func(p *Plan) { p.Log.TaskCommittedAtTS = 121 }, "metadata must commit"},
		{"restore before snapshot", func(p *Plan) { p.RestoreTS = 119 }, "precedes full"},
		{"checkpoint behind restore", func(p *Plan) { p.Log.GlobalCheckpointTS = 139 }, "exceeds durable"},
		{"same target", func(p *Plan) { p.Target.ClusterID = 11 }, "must differ"},
		{"bad backup digest", func(p *Plan) { p.Full.BackupMetaSHA256 = "ABC" }, "backupmeta"},
		{"bad witness", func(p *Plan) { p.Target.EmptyWitnessSHA256 = "" }, "empty-witness"},
		{"missing advancer", func(p *Plan) { p.Log.AdvancerOwner = "" }, "advancer owner"},
		{"unsafe storage", func(p *Plan) { p.Full.StoragePrefix = " s3://bucket" }, "storage prefix"},
		{"wrong tenant range", func(p *Plan) { p.Source.EndKeyHex = "ff" }, "does not match keyspace"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			plan := validReceiptPlan(t)
			tt.edit(&plan)
			require.ErrorContains(t, plan.Validate(), tt.want)
		})
	}
}

func TestBuildFromReceiptsEliminatesFreeFormSourceEvidence(t *testing.T) {
	task, _ := readyTask(t)
	full, err := BuildFullSnapshot(task, digest, "s3://bucket/immutable/full-1", fullMeta(t, task))
	require.NoError(t, err)
	ready := readyReceiptFor(task)
	plan, err := BuildFromReceipts(task, full, ready, ReceiptPlanInputs{TaskCreateSHA256: digest, TargetClusterID: 22, EmptyWitnessSHA256: digest, RestoreTS: 140})
	require.NoError(t, err)
	require.Equal(t, full.BackupTS, plan.Full.BackupTS)
	require.Equal(t, ready.AdvancerOwner, plan.Log.AdvancerOwner)

	full.TaskName = "other"
	_, err = BuildFromReceipts(task, full, ready, ReceiptPlanInputs{TaskCreateSHA256: digest, TargetClusterID: 22, EmptyWitnessSHA256: digest, RestoreTS: 140})
	require.ErrorContains(t, err, "different source tasks")

	full.TaskName = task.TaskName
	_, err = BuildFromReceipts(task, full, ready, ReceiptPlanInputs{TaskCreateSHA256: strings.Repeat("f", 64), TargetClusterID: 22, EmptyWitnessSHA256: digest, RestoreTS: 140})
	require.ErrorContains(t, err, "exact task-create")
}

func TestDecodePreflightIsStrict(t *testing.T) {
	p := validPreflight()
	b, err := json.Marshal(p)
	require.NoError(t, err)
	got, err := DecodePreflight(strings.NewReader(string(b)))
	require.NoError(t, err)
	require.Equal(t, p.ClusterID, got.ClusterID)

	unknown := strings.TrimSuffix(string(b), "}") + `,"unknown":true}`
	_, err = DecodePreflight(strings.NewReader(unknown))
	require.Error(t, err)
	_, err = DecodePreflight(strings.NewReader(string(b) + `{}`))
	require.ErrorContains(t, err, "trailing")

	p.TaskAvailable = false
	b, err = json.Marshal(p)
	require.NoError(t, err)
	_, err = DecodePreflight(strings.NewReader(string(b)))
	require.ErrorContains(t, err, "available task")

	p = validPreflight()
	p.EndKeyHex = "ff"
	b, err = json.Marshal(p)
	require.NoError(t, err)
	_, err = DecodePreflight(strings.NewReader(string(b)))
	require.ErrorContains(t, err, "does not match")

	p = validPreflight()
	p.OwnershipKeys = p.OwnershipKeys[:5]
	b, err = json.Marshal(p)
	require.NoError(t, err)
	_, err = DecodePreflight(strings.NewReader(string(b)))
	require.ErrorContains(t, err, "every task ownership")
}
