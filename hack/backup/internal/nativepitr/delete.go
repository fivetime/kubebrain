package nativepitr

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"

	clientv3 "go.etcd.io/etcd/client/v3"
)

const TaskDeleteFormat = "kubebrain.native-pitr-task-delete.v1"

type TaskDeleteMetadata interface {
	TaskStatusMetadata
	DeleteOwnedTask(context.Context, TaskCreateReceipt, TaskStatusSnapshot) (bool, error)
}

func (e EtcdMetadata) DeleteOwnedTask(ctx context.Context, task TaskCreateReceipt, snapshot TaskStatusSnapshot) (bool, error) {
	infoKey := "/tidb/br-stream/info/" + task.TaskName
	rangesPrefix := "/tidb/br-stream/ranges/" + task.TaskName + "/"
	checkpointPrefix := "/tidb/br-stream/checkpoint/" + task.TaskName + "/"
	// TiDB v7.5.1 deletes StorageCheckpointOf(task) without a trailing slash,
	// which can also match a sibling task whose name shares the prefix. Keep the
	// exact task boundary even though this release supports only one task.
	storageCheckpointPrefix := "/tidb/br-stream/storage-checkpoint/" + task.TaskName + "/"
	lastErrorPrefix := "/tidb/br-stream/last-error/" + task.TaskName + "/"
	pauseKey := "/tidb/br-stream/pause/" + task.TaskName
	start, _ := hex.DecodeString(task.StartKeyHex)
	rangeKey := rangesPrefix + string(start)
	resp, err := e.KV.Txn(ctx).If(
		clientv3.Compare(clientv3.Value(TaskOwnerKey), "=", string(snapshot.Owner)),
		clientv3.Compare(clientv3.Value(infoKey), "=", string(snapshot.Info)),
		clientv3.Compare(clientv3.Value(rangeKey), "=", string(snapshot.Ranges[rangeKey])),
	).Then(
		clientv3.OpDelete(infoKey),
		clientv3.OpDelete(rangesPrefix, clientv3.WithPrefix()),
		clientv3.OpDelete(checkpointPrefix, clientv3.WithPrefix()),
		clientv3.OpDelete(storageCheckpointPrefix, clientv3.WithPrefix()),
		clientv3.OpDelete(pauseKey),
		clientv3.OpDelete(lastErrorPrefix, clientv3.WithPrefix()),
		clientv3.OpDelete(TaskOwnerKey),
	).Commit()
	if err != nil {
		return false, err
	}
	return resp.Succeeded, nil
}

type TaskDeleteReceipt struct {
	Format             string `json:"format"`
	ClusterID          uint64 `json:"cluster_id"`
	Keyspace           string `json:"keyspace"`
	TaskName           string `json:"task_name"`
	StartTS            uint64 `json:"start_ts"`
	EndTS              uint64 `json:"end_ts"`
	FinalCheckpointTS  uint64 `json:"final_checkpoint_ts"`
	PreflightSHA256    string `json:"preflight_sha256"`
	OwnerKey           string `json:"owner_key"`
	MetadataDeleted    bool   `json:"metadata_deleted"`
	CoordinatorManaged bool   `json:"coordinator_cleanup_delegated"`
}

func DecodeTaskReady(r io.Reader) (TaskReadyReceipt, error) {
	var ready TaskReadyReceipt
	decoder := json.NewDecoder(r)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&ready); err != nil {
		return ready, fmt.Errorf("decode task-ready receipt: %w", err)
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return ready, errors.New("task-ready receipt has trailing JSON")
	}
	if err := validateTaskReadyReceipt(ready); err != nil {
		return ready, err
	}
	return ready, nil
}

func DeleteTask(ctx context.Context, metadata TaskDeleteMetadata, clusterID uint64, task TaskCreateReceipt, ready TaskReadyReceipt) (TaskDeleteReceipt, error) {
	if err := validateTaskCreateReceipt(task); err != nil {
		return TaskDeleteReceipt{}, err
	}
	if err := validateTaskReadyReceipt(ready); err != nil {
		return TaskDeleteReceipt{}, err
	}
	if clusterID == 0 || clusterID != task.ClusterID || !readyMatchesTask(ready, task) {
		return TaskDeleteReceipt{}, errors.New("task receipts do not match each other and the connected PD cluster")
	}
	snapshot, err := metadata.ReadTaskStatus(ctx, task)
	if err != nil {
		return TaskDeleteReceipt{}, fmt.Errorf("read final task status snapshot: %w", err)
	}
	checkpoint, err := validateTaskStatus(task, snapshot)
	if err != nil {
		return TaskDeleteReceipt{}, err
	}
	if checkpoint < ready.GlobalCheckpointTS {
		return TaskDeleteReceipt{}, errors.New("global checkpoint regressed since task-ready receipt")
	}
	deleted, err := metadata.DeleteOwnedTask(ctx, task, snapshot)
	if err != nil {
		return TaskDeleteReceipt{}, fmt.Errorf("delete owned task metadata: %w", err)
	}
	if !deleted {
		return TaskDeleteReceipt{}, errors.New("task metadata changed concurrently; nothing was deleted")
	}
	return TaskDeleteReceipt{Format: TaskDeleteFormat, ClusterID: task.ClusterID, Keyspace: task.Keyspace, TaskName: task.TaskName, StartTS: task.StartTS, EndTS: task.EndTS, FinalCheckpointTS: checkpoint, PreflightSHA256: task.PreflightSHA256, OwnerKey: TaskOwnerKey, MetadataDeleted: true, CoordinatorManaged: true}, nil
}

func validateTaskReadyReceipt(ready TaskReadyReceipt) error {
	if ready.Format != TaskReadyFormat || ready.ClusterID == 0 || !ready.BootstrapReleased || !ready.MetadataSnapshotValid || ready.GlobalCheckpointTS < ready.StartTS || ready.GlobalCheckpointTS >= ready.EndTS {
		return errors.New("input is not a successful native PITR task-ready v2 receipt")
	}
	if !dnsLabel.MatchString(ready.TaskName) || ready.Keyspace == "" || !sha256RE.MatchString(ready.PreflightSHA256) || ready.BootstrapSafePointID == "" {
		return errors.New("task-ready receipt has invalid identity evidence")
	}
	if err := safeText("advancer owner", ready.AdvancerOwner); err != nil {
		return err
	}
	return nil
}

func readyMatchesTask(ready TaskReadyReceipt, task TaskCreateReceipt) bool {
	return ready.ClusterID == task.ClusterID && ready.Keyspace == task.Keyspace && ready.TaskName == task.TaskName && ready.StartTS == task.StartTS && ready.EndTS == task.EndTS && ready.PreflightSHA256 == task.PreflightSHA256 && ready.BootstrapSafePointID == task.BootstrapSafePointID
}
