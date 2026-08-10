package nativepitr

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

type fakeTaskDelete struct {
	snapshot    TaskStatusSnapshot
	readErr     error
	deleted     bool
	deleteErr   error
	deleteCalls int
}

func (f *fakeTaskDelete) ReadTaskStatus(context.Context, TaskCreateReceipt) (TaskStatusSnapshot, error) {
	return f.snapshot, f.readErr
}

func (f *fakeTaskDelete) DeleteOwnedTask(_ context.Context, _ TaskCreateReceipt, got TaskStatusSnapshot) (bool, error) {
	f.deleteCalls++
	if len(got.Owner) == 0 {
		return false, errors.New("missing snapshot")
	}
	return f.deleted, f.deleteErr
}

func readyReceiptFor(task TaskCreateReceipt) TaskReadyReceipt {
	return TaskReadyReceipt{Format: TaskReadyFormat, ClusterID: task.ClusterID, Keyspace: task.Keyspace, TaskName: task.TaskName, StartTS: task.StartTS, EndTS: task.EndTS, GlobalCheckpointTS: 150, AdvancerOwner: "advancer-1", PreflightSHA256: task.PreflightSHA256, BootstrapSafePointID: task.BootstrapSafePointID, BootstrapReleased: true, MetadataSnapshotValid: true}
}

func TestDeleteTaskBindsReceiptsAndFinalCheckpoint(t *testing.T) {
	task, snapshot := readyTask(t)
	binary.BigEndian.PutUint64(snapshot.GlobalCheckpoint, 175)
	metadata := &fakeTaskDelete{snapshot: snapshot, deleted: true}
	receipt, err := DeleteTask(context.Background(), metadata, task.ClusterID, task, readyReceiptFor(task))
	require.NoError(t, err)
	require.Equal(t, uint64(175), receipt.FinalCheckpointTS)
	require.True(t, receipt.MetadataDeleted)
	require.True(t, receipt.CoordinatorManaged)
	require.Equal(t, 1, metadata.deleteCalls)
}

func TestDeleteTaskFailsClosed(t *testing.T) {
	tests := []struct {
		name string
		edit func(*TaskCreateReceipt, *TaskReadyReceipt, *TaskStatusSnapshot, *fakeTaskDelete)
		want string
	}{
		{"receipt mismatch", func(_ *TaskCreateReceipt, r *TaskReadyReceipt, _ *TaskStatusSnapshot, _ *fakeTaskDelete) {
			r.TaskName = "other"
		}, "do not match"},
		{"checkpoint regression", func(_ *TaskCreateReceipt, r *TaskReadyReceipt, s *TaskStatusSnapshot, _ *fakeTaskDelete) {
			r.GlobalCheckpointTS = 200
			binary.BigEndian.PutUint64(s.GlobalCheckpoint, 175)
		}, "regressed"},
		{"changed metadata", func(_ *TaskCreateReceipt, _ *TaskReadyReceipt, _ *TaskStatusSnapshot, d *fakeTaskDelete) {
			d.deleted = false
		}, "changed concurrently"},
		{"delete error", func(_ *TaskCreateReceipt, _ *TaskReadyReceipt, _ *TaskStatusSnapshot, d *fakeTaskDelete) {
			d.deleteErr = errors.New("etcd down")
		}, "etcd down"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			task, snapshot := readyTask(t)
			ready := readyReceiptFor(task)
			metadata := &fakeTaskDelete{snapshot: snapshot, deleted: true}
			tt.edit(&task, &ready, &snapshot, metadata)
			metadata.snapshot = snapshot
			_, err := DeleteTask(context.Background(), metadata, validPreflight().ClusterID, task, ready)
			require.ErrorContains(t, err, tt.want)
			if tt.name == "receipt mismatch" || tt.name == "checkpoint regression" {
				require.Zero(t, metadata.deleteCalls)
			}
		})
	}
}

func TestDecodeTaskReadyIsStrict(t *testing.T) {
	task, _ := readyTask(t)
	b, err := json.Marshal(readyReceiptFor(task))
	require.NoError(t, err)
	_, err = DecodeTaskReady(strings.NewReader(string(b)))
	require.NoError(t, err)
	_, err = DecodeTaskReady(strings.NewReader(string(b) + `{}`))
	require.ErrorContains(t, err, "trailing")
}
