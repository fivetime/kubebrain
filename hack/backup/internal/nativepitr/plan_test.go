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

func validInputs() Inputs {
	return Inputs{TaskStartTS: 100, FullBackupTS: 120, BackupMetaSHA256: digest, StoragePrefix: "s3://bucket/immutable/run-1", GlobalCheckpointTS: 200, AdvancerOwner: "advancer-1", TargetClusterID: 22, EmptyWitnessSHA256: digest, RestoreTS: 180}
}

func TestBuildRestorePlan(t *testing.T) {
	plan, err := Build(validPreflight(), validInputs())
	require.NoError(t, err)
	require.Equal(t, PlanFormat, plan.Format)
	require.Equal(t, uint64(11), plan.Source.ClusterID)
	require.Equal(t, uint64(120), plan.Full.BackupTS)
	require.Equal(t, "br-txn", plan.Full.Mode)
	require.True(t, plan.ReadOnly)
	require.NoError(t, plan.Validate())
	plan.Source.EndKeyHex = "ff"
	require.ErrorContains(t, plan.Validate(), "does not match keyspace")
}

func TestBuildRejectsBrokenChain(t *testing.T) {
	tests := []struct {
		name string
		edit func(*Inputs)
		want string
	}{
		{"log starts after snapshot", func(i *Inputs) { i.TaskStartTS = 121 }, "start no later"},
		{"restore before snapshot", func(i *Inputs) { i.RestoreTS = 119 }, "precedes full"},
		{"checkpoint behind restore", func(i *Inputs) { i.GlobalCheckpointTS = 179 }, "exceeds durable"},
		{"same target", func(i *Inputs) { i.TargetClusterID = 11 }, "must differ"},
		{"bad backup digest", func(i *Inputs) { i.BackupMetaSHA256 = "ABC" }, "backupmeta"},
		{"bad witness", func(i *Inputs) { i.EmptyWitnessSHA256 = "" }, "empty-witness"},
		{"missing advancer", func(i *Inputs) { i.AdvancerOwner = "" }, "advancer owner"},
		{"unsafe storage", func(i *Inputs) { i.StoragePrefix = " s3://bucket" }, "storage prefix"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			in := validInputs()
			tt.edit(&in)
			_, err := Build(validPreflight(), in)
			require.ErrorContains(t, err, tt.want)
		})
	}
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
