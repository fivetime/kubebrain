package nativepitr

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/kubewharf/kubebrain/pkg/backend/coder"
	backuppb "github.com/pingcap/kvproto/pkg/brpb"
	clientv3 "go.etcd.io/etcd/client/v3"
)

const TaskReadyFormat = "kubebrain.native-pitr-task-ready.v4"

type TaskStatusSnapshot struct {
	Owner, Info, AdvancerOwner, GlobalCheckpoint []byte
	Ranges                                       map[string][]byte
}

type TaskStatusMetadata interface {
	ReadTaskStatus(context.Context, TaskCreateReceipt) (TaskStatusSnapshot, error)
}

func (e EtcdMetadata) ReadTaskStatus(ctx context.Context, task TaskCreateReceipt) (TaskStatusSnapshot, error) {
	checkpointKey := "/tidb/br-stream/checkpoint/" + task.TaskName + "/central_global"
	resp, err := e.KV.Txn(ctx).Then(
		clientv3.OpGet(TaskOwnerKey),
		clientv3.OpGet("/tidb/br-stream/info/"+task.TaskName),
		clientv3.OpGet("/tidb/br-stream/ranges/"+task.TaskName+"/", clientv3.WithPrefix()),
		clientv3.OpGet("/tidb/br-stream/owner", clientv3.WithPrefix()),
		clientv3.OpGet(checkpointKey),
	).Commit()
	if err != nil {
		return TaskStatusSnapshot{}, err
	}
	value := func(index int) []byte {
		kvs := resp.Responses[index].GetResponseRange().Kvs
		if len(kvs) != 1 {
			return nil
		}
		return append([]byte(nil), kvs[0].Value...)
	}
	ranges := make(map[string][]byte)
	for _, kv := range resp.Responses[2].GetResponseRange().Kvs {
		ranges[string(kv.Key)] = append([]byte(nil), kv.Value...)
	}
	advancerOwners := resp.Responses[3].GetResponseRange().Kvs
	var advancerOwner []byte
	if len(advancerOwners) == 1 {
		advancerOwner = append([]byte(nil), advancerOwners[0].Value...)
	}
	return TaskStatusSnapshot{Owner: value(0), Info: value(1), Ranges: ranges, AdvancerOwner: advancerOwner, GlobalCheckpoint: value(4)}, nil
}

type TaskReadyReceipt struct {
	Format                string `json:"format"`
	ClusterID             uint64 `json:"cluster_id"`
	Keyspace              string `json:"keyspace"`
	TaskName              string `json:"task_name"`
	StartTS               uint64 `json:"start_ts"`
	CommittedAtTS         uint64 `json:"task_committed_at_ts"`
	EndTS                 uint64 `json:"end_ts"`
	GlobalCheckpointTS    uint64 `json:"global_checkpoint_ts"`
	AdvancerOwner         string `json:"advancer_owner"`
	PreflightSHA256       string `json:"preflight_sha256"`
	BootstrapSafePointID  string `json:"bootstrap_safepoint_id"`
	BootstrapReleased     bool   `json:"bootstrap_safepoint_released"`
	MetadataSnapshotValid bool   `json:"metadata_snapshot_valid"`
}

func DecodeTaskCreate(r io.Reader) (TaskCreateReceipt, error) {
	var task TaskCreateReceipt
	decoder := json.NewDecoder(r)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&task); err != nil {
		return task, fmt.Errorf("decode task-create receipt: %w", err)
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return task, errors.New("task-create receipt has trailing JSON")
	}
	if err := validateTaskCreateReceipt(task); err != nil {
		return task, err
	}
	return task, nil
}

func CheckTaskReady(ctx context.Context, safePoints SafePointClient, metadata TaskStatusMetadata, clusterID uint64, task TaskCreateReceipt) (TaskReadyReceipt, error) {
	if err := validateTaskCreateReceipt(task); err != nil {
		return TaskReadyReceipt{}, err
	}
	if clusterID == 0 || clusterID != task.ClusterID {
		return TaskReadyReceipt{}, errors.New("task receipt does not belong to the connected PD cluster")
	}
	minimum, err := safePoints.UpdateServiceGCSafePoint(ctx, task.BootstrapSafePointID, bootstrapSafePointTTL, task.StartTS)
	if err != nil {
		return TaskReadyReceipt{}, fmt.Errorf("renew bootstrap GC safepoint: %w", err)
	}
	if minimum > task.StartTS {
		return TaskReadyReceipt{}, fmt.Errorf("GC safepoint %d has already passed task start %d", minimum, task.StartTS)
	}
	snapshot, err := metadata.ReadTaskStatus(ctx, task)
	if err != nil {
		return TaskReadyReceipt{}, fmt.Errorf("read task status snapshot: %w", err)
	}
	checkpoint, err := validateTaskStatus(task, snapshot)
	if err != nil {
		return TaskReadyReceipt{}, err
	}
	minimum, err = safePoints.UpdateServiceGCSafePoint(context.WithoutCancel(ctx), task.BootstrapSafePointID, 0, 0)
	if err != nil {
		return TaskReadyReceipt{}, fmt.Errorf("release bootstrap GC safepoint: %w", err)
	}
	if minimum > checkpoint {
		return TaskReadyReceipt{}, fmt.Errorf("GC safepoint %d passed global checkpoint %d while releasing bootstrap guard", minimum, checkpoint)
	}
	return TaskReadyReceipt{Format: TaskReadyFormat, ClusterID: task.ClusterID, Keyspace: task.Keyspace, TaskName: task.TaskName, StartTS: task.StartTS, CommittedAtTS: task.CommittedAtTS, EndTS: task.EndTS, GlobalCheckpointTS: checkpoint, AdvancerOwner: string(snapshot.AdvancerOwner), PreflightSHA256: task.PreflightSHA256, BootstrapSafePointID: task.BootstrapSafePointID, BootstrapReleased: true, MetadataSnapshotValid: true}, nil
}

func validateTaskCreateReceipt(task TaskCreateReceipt) error {
	if task.Format != TaskCreateFormat || !task.AtomicMetadataCreated || task.ClusterID == 0 {
		return errors.New("input is not a successful native PITR task-create v4 receipt")
	}
	if !dnsLabel.MatchString(task.TaskName) || task.Keyspace == "" || task.StartTS == 0 || task.CommittedAtTS < task.StartTS || task.EndTS <= task.CommittedAtTS {
		return errors.New("task-create receipt has invalid identity or timestamps")
	}
	ks, err := coder.NewKeyspace(task.Keyspace)
	if err != nil {
		return fmt.Errorf("task-create receipt keyspace: %w", err)
	}
	start, err := hex.DecodeString(task.StartKeyHex)
	if err != nil || !bytes.Equal(start, ks.ObjectKeyspaceStart()) {
		return errors.New("task-create receipt start key does not match its keyspace")
	}
	end, err := hex.DecodeString(task.EndKeyHex)
	if err != nil || !bytes.Equal(end, ks.ObjectKeyspaceEnd()) {
		return errors.New("task-create receipt end key does not match its keyspace")
	}
	bootstrapPrefix := "kubebrain-native-pitr-bootstrap-"
	operationID := strings.TrimPrefix(task.BootstrapSafePointID, bootstrapPrefix)
	if !sha256RE.MatchString(task.PreflightSHA256) || !sha256RE.MatchString(task.LogStorageSHA256) || task.OwnerKey != TaskOwnerKey || task.BootstrapSafePointTTL != bootstrapSafePointTTL || !strings.HasPrefix(task.BootstrapSafePointID, bootstrapPrefix) || !dnsLabel.MatchString(operationID) {
		return errors.New("task-create receipt has invalid ownership evidence")
	}
	if err := validateS3Prefix(task.LogStoragePrefix); err != nil {
		return fmt.Errorf("task-create log storage: %w", err)
	}
	return nil
}

func validateTaskStatus(task TaskCreateReceipt, snapshot TaskStatusSnapshot) (uint64, error) {
	var owner ownerRecord
	if err := json.Unmarshal(snapshot.Owner, &owner); err != nil || owner.Format != TaskCreateFormat || owner.ClusterID != task.ClusterID || owner.TaskName != task.TaskName || owner.PreflightSHA256 != task.PreflightSHA256 || owner.BootstrapSafePointID != task.BootstrapSafePointID || owner.LogStoragePrefix != task.LogStoragePrefix || owner.LogStorageSHA256 != task.LogStorageSHA256 {
		return 0, errors.New("stored native PITR owner does not match task receipt")
	}
	var info backuppb.StreamBackupTaskInfo
	if err := info.Unmarshal(snapshot.Info); err != nil || info.Name != task.TaskName || info.StartTs != task.StartTS || info.EndTs != task.EndTS {
		return 0, errors.New("stored backup-stream task does not match task receipt")
	}
	if validateTaskStorage(info.Storage) != nil {
		return 0, errors.New("stored backup-stream storage does not match task receipt")
	}
	storageDigest, storageErr := storageBackendSHA256(info.Storage)
	if storageErr != nil || canonicalS3Prefix(info.Storage) != task.LogStoragePrefix || storageDigest != task.LogStorageSHA256 {
		return 0, errors.New("stored backup-stream storage does not match task receipt")
	}
	start, _ := hex.DecodeString(task.StartKeyHex)
	end, _ := hex.DecodeString(task.EndKeyHex)
	exactRange := "/tidb/br-stream/ranges/" + task.TaskName + "/" + string(start)
	if len(snapshot.Ranges) != 1 || !bytes.Equal(snapshot.Ranges[exactRange], end) {
		return 0, errors.New("stored backup-stream range is not the exact tenant range")
	}
	if len(snapshot.AdvancerOwner) == 0 {
		return 0, errors.New("backup-stream checkpoint advancer has no elected owner")
	}
	if len(snapshot.GlobalCheckpoint) != 8 {
		return 0, errors.New("backup-stream global checkpoint is missing or malformed")
	}
	checkpoint := binary.BigEndian.Uint64(snapshot.GlobalCheckpoint)
	if checkpoint < task.StartTS || checkpoint >= task.EndTS {
		return 0, errors.New("backup-stream global checkpoint is outside the task interval")
	}
	return checkpoint, nil
}
