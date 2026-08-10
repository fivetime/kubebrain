package nativepitr

import (
	"context"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	backuppb "github.com/pingcap/kvproto/pkg/brpb"
	"github.com/stretchr/testify/require"
)

type fakeTaskStatus struct {
	snapshot TaskStatusSnapshot
	err      error
}

func (f fakeTaskStatus) ReadTaskStatus(context.Context, TaskCreateReceipt) (TaskStatusSnapshot, error) {
	return f.snapshot, f.err
}

func readyTask(t *testing.T) (TaskCreateReceipt, TaskStatusSnapshot) {
	t.Helper()
	in := taskInput()
	task := TaskCreateReceipt{Format: TaskCreateFormat, ClusterID: in.Preflight.ClusterID, Keyspace: in.Preflight.Keyspace, TaskName: in.Preflight.TaskName, StartTS: in.StartTS, EndTS: in.EndTS, StartKeyHex: in.Preflight.StartKeyHex, EndKeyHex: in.Preflight.EndKeyHex, PreflightSHA256: in.PreflightSHA256, OwnerKey: TaskOwnerKey, BootstrapSafePointID: "kubebrain-native-pitr-bootstrap-operation-1", BootstrapSafePointTTL: bootstrapSafePointTTL, AtomicMetadataCreated: true}
	owner, err := json.Marshal(ownerRecord{Format: TaskCreateFormat, ClusterID: task.ClusterID, TaskName: task.TaskName, PreflightSHA256: task.PreflightSHA256, BootstrapSafePointID: task.BootstrapSafePointID})
	require.NoError(t, err)
	info, err := (&backuppb.StreamBackupTaskInfo{Name: task.TaskName, StartTs: task.StartTS, EndTs: task.EndTS}).Marshal()
	require.NoError(t, err)
	checkpoint := make([]byte, 8)
	binary.BigEndian.PutUint64(checkpoint, 150)
	start, _ := hex.DecodeString(task.StartKeyHex)
	end, _ := hex.DecodeString(task.EndKeyHex)
	return task, TaskStatusSnapshot{Owner: owner, Info: info, Ranges: map[string][]byte{"/tidb/br-stream/ranges/" + task.TaskName + "/" + string(start): end}, AdvancerOwner: []byte("owner"), GlobalCheckpoint: checkpoint}
}

func TestCheckTaskReadyReleasesGuardAfterValidation(t *testing.T) {
	task, snapshot := readyTask(t)
	sp := &fakeSafePoints{minimum: 90}
	receipt, err := CheckTaskReady(context.Background(), sp, fakeTaskStatus{snapshot: snapshot}, task.ClusterID, task)
	require.NoError(t, err)
	require.True(t, receipt.BootstrapReleased)
	require.Equal(t, uint64(150), receipt.GlobalCheckpointTS)
	require.Len(t, sp.calls, 2)
	require.Equal(t, bootstrapSafePointTTL, sp.calls[0].ttl)
	require.Zero(t, sp.calls[1].ttl)
}

func TestCheckTaskReadyFailsClosedWithoutReleasingGuard(t *testing.T) {
	tests := []struct {
		name string
		edit func(*TaskCreateReceipt, *TaskStatusSnapshot)
		want string
	}{
		{"wrong cluster", func(task *TaskCreateReceipt, _ *TaskStatusSnapshot) { task.ClusterID++ }, "connected PD cluster"},
		{"owner mismatch", func(_ *TaskCreateReceipt, s *TaskStatusSnapshot) { s.Owner = []byte(`{}`) }, "owner"},
		{"task mismatch", func(_ *TaskCreateReceipt, s *TaskStatusSnapshot) { s.Info = []byte("bad") }, "task"},
		{"extra range", func(_ *TaskCreateReceipt, s *TaskStatusSnapshot) { s.Ranges["extra"] = []byte("range") }, "exact tenant range"},
		{"no advancer", func(_ *TaskCreateReceipt, s *TaskStatusSnapshot) { s.AdvancerOwner = nil }, "no elected owner"},
		{"no checkpoint", func(_ *TaskCreateReceipt, s *TaskStatusSnapshot) { s.GlobalCheckpoint = nil }, "checkpoint"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			task, snapshot := readyTask(t)
			tt.edit(&task, &snapshot)
			sp := &fakeSafePoints{minimum: 90}
			_, err := CheckTaskReady(context.Background(), sp, fakeTaskStatus{snapshot: snapshot}, validPreflight().ClusterID, task)
			require.ErrorContains(t, err, tt.want)
			require.LessOrEqual(t, len(sp.calls), 1)
		})
	}
}

func TestCheckTaskReadyKeepsGuardOnReadError(t *testing.T) {
	task, _ := readyTask(t)
	sp := &fakeSafePoints{minimum: 90}
	_, err := CheckTaskReady(context.Background(), sp, fakeTaskStatus{err: errors.New("etcd unavailable")}, task.ClusterID, task)
	require.ErrorContains(t, err, "etcd unavailable")
	require.Len(t, sp.calls, 1)
}

func TestDecodeTaskCreateIsStrict(t *testing.T) {
	task, _ := readyTask(t)
	b, err := json.Marshal(task)
	require.NoError(t, err)
	decoded, err := DecodeTaskCreate(strings.NewReader(string(b)))
	require.NoError(t, err)
	require.Equal(t, task, decoded)

	_, err = DecodeTaskCreate(strings.NewReader(string(b) + `{}`))
	require.ErrorContains(t, err, "trailing")
	_, err = DecodeTaskCreate(strings.NewReader(strings.Replace(string(b), `"cluster_id"`, `"unknown"`, 1)))
	require.ErrorContains(t, err, "unknown")
}
