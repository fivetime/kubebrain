package nativepitr

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	backuppb "github.com/pingcap/kvproto/pkg/brpb"
	"github.com/tikv/client-go/v2/oracle"
	clientv3 "go.etcd.io/etcd/client/v3"
)

const (
	TaskOwnerKey          = "/kubebrain/native-pitr/owner"
	TaskCreateFormat      = "kubebrain.native-pitr-task-create.v4"
	bootstrapSafePointTTL = int64((2 * time.Hour) / time.Second)
)

type SafePointClient interface {
	UpdateServiceGCSafePoint(context.Context, string, int64, uint64) (uint64, error)
	GetTS(context.Context) (int64, int64, error)
}

type AtomicMetadata interface {
	CreateIfAbsent(context.Context, []string, []string, map[string][]byte) (bool, error)
}

type EtcdMetadata struct{ KV clientv3.KV }

func (e EtcdMetadata) CreateIfAbsent(ctx context.Context, absent, absentPrefixes []string, values map[string][]byte) (bool, error) {
	cmps := make([]clientv3.Cmp, 0, len(absent)+len(absentPrefixes))
	for _, key := range absent {
		cmps = append(cmps, clientv3.Compare(clientv3.Version(key), "=", 0))
	}
	for _, prefix := range absentPrefixes {
		cmps = append(cmps, clientv3.Compare(clientv3.Version(prefix), "=", 0).WithPrefix())
	}
	ops := make([]clientv3.Op, 0, len(values))
	// Fixed order makes request traces deterministic; etcd applies the txn atomically.
	for _, key := range absent {
		if value, ok := values[key]; ok {
			ops = append(ops, clientv3.OpPut(key, string(value)))
		}
	}
	resp, err := e.KV.Txn(ctx).If(cmps...).Then(ops...).Commit()
	if err != nil {
		return false, err
	}
	return resp.Succeeded, nil
}

type TaskCreateInput struct {
	Preflight       Preflight
	PreflightSHA256 string
	StartTS         uint64
	EndTS           uint64
	Storage         *backuppb.StorageBackend
	OperationID     string
}

type TaskCreateReceipt struct {
	Format                string `json:"format"`
	ClusterID             uint64 `json:"cluster_id"`
	Keyspace              string `json:"keyspace"`
	TaskName              string `json:"task_name"`
	StartTS               uint64 `json:"start_ts"`
	CommittedAtTS         uint64 `json:"task_committed_at_ts"`
	EndTS                 uint64 `json:"end_ts"`
	StartKeyHex           string `json:"start_key_hex"`
	EndKeyHex             string `json:"end_key_hex"`
	LogStoragePrefix      string `json:"log_storage_prefix"`
	LogStorageSHA256      string `json:"log_storage_backend_sha256"`
	PreflightSHA256       string `json:"preflight_sha256"`
	OwnerKey              string `json:"owner_key"`
	BootstrapSafePointID  string `json:"bootstrap_safepoint_id"`
	BootstrapSafePointTTL int64  `json:"bootstrap_safepoint_ttl_seconds"`
	AtomicMetadataCreated bool   `json:"atomic_metadata_created"`
}

type ownerRecord struct {
	Format               string `json:"format"`
	ClusterID            uint64 `json:"cluster_id"`
	TaskName             string `json:"task_name"`
	PreflightSHA256      string `json:"preflight_sha256"`
	BootstrapSafePointID string `json:"bootstrap_safepoint_id"`
	LogStoragePrefix     string `json:"log_storage_prefix"`
	LogStorageSHA256     string `json:"log_storage_backend_sha256"`
}

func CreateTask(ctx context.Context, safePoints SafePointClient, metadata AtomicMetadata, in TaskCreateInput) (TaskCreateReceipt, error) {
	if err := ValidatePreflight(in.Preflight); err != nil {
		return TaskCreateReceipt{}, err
	}
	if !sha256RE.MatchString(in.PreflightSHA256) {
		return TaskCreateReceipt{}, errors.New("invalid preflight SHA-256")
	}
	if in.StartTS == 0 || in.EndTS <= in.StartTS {
		return TaskCreateReceipt{}, errors.New("task timestamps must satisfy 0 < start_ts < end_ts")
	}
	if !dnsLabel.MatchString(in.OperationID) {
		return TaskCreateReceipt{}, errors.New("operation ID must be a lowercase DNS label")
	}
	if err := validateTaskStorage(in.Storage); err != nil {
		return TaskCreateReceipt{}, err
	}
	bootstrapID := "kubebrain-native-pitr-bootstrap-" + in.OperationID
	minimum, err := safePoints.UpdateServiceGCSafePoint(ctx, bootstrapID, bootstrapSafePointTTL, in.StartTS)
	if err != nil {
		return TaskCreateReceipt{}, fmt.Errorf("install bootstrap GC safepoint: %w", err)
	}
	if minimum > in.StartTS {
		_, _ = safePoints.UpdateServiceGCSafePoint(context.WithoutCancel(ctx), bootstrapID, 0, 0)
		return TaskCreateReceipt{}, fmt.Errorf("GC safepoint %d has already passed task start %d", minimum, in.StartTS)
	}

	info := &backuppb.StreamBackupTaskInfo{Storage: in.Storage, StartTs: in.StartTS, EndTs: in.EndTS, Name: in.Preflight.TaskName, TableFilter: []string{"kubebrain-physical-range"}, CompressionType: backuppb.CompressionType_ZSTD}
	infoBytes, err := info.Marshal()
	if err != nil {
		return TaskCreateReceipt{}, cleanupBootstrap(ctx, safePoints, bootstrapID, err)
	}
	start, _ := hex.DecodeString(in.Preflight.StartKeyHex)
	end, _ := hex.DecodeString(in.Preflight.EndKeyHex)
	logStoragePrefix := canonicalS3Prefix(in.Storage)
	logStorageSHA256, err := storageBackendSHA256(in.Storage)
	if err != nil {
		return TaskCreateReceipt{}, cleanupBootstrap(ctx, safePoints, bootstrapID, err)
	}
	ownerBytes, err := json.Marshal(ownerRecord{Format: TaskCreateFormat, ClusterID: in.Preflight.ClusterID, TaskName: in.Preflight.TaskName, PreflightSHA256: in.PreflightSHA256, BootstrapSafePointID: bootstrapID, LogStoragePrefix: logStoragePrefix, LogStorageSHA256: logStorageSHA256})
	if err != nil {
		return TaskCreateReceipt{}, cleanupBootstrap(ctx, safePoints, bootstrapID, err)
	}
	rangeKey := in.Preflight.TaskRangesKey + string(start)
	absent := []string{TaskOwnerKey, in.Preflight.TaskInfoKey, rangeKey}
	absentPrefixes := []string{
		"/tidb/br-stream/info/",
		in.Preflight.TaskRangesKey,
		"/tidb/br-stream/checkpoint/" + in.Preflight.TaskName + "/",
		"/tidb/br-stream/storage-checkpoint/" + in.Preflight.TaskName + "/",
		"/tidb/br-stream/last-error/" + in.Preflight.TaskName + "/",
	}
	created, err := metadata.CreateIfAbsent(ctx, absent, absentPrefixes, map[string][]byte{TaskOwnerKey: ownerBytes, in.Preflight.TaskInfoKey: infoBytes, rangeKey: end})
	if err != nil {
		return TaskCreateReceipt{}, cleanupBootstrap(ctx, safePoints, bootstrapID, fmt.Errorf("create task metadata: %w", err))
	}
	if !created {
		return TaskCreateReceipt{}, cleanupBootstrap(ctx, safePoints, bootstrapID, errors.New("native PITR owner, task, or range was concurrently created"))
	}
	physical, logical, err := safePoints.GetTS(ctx)
	if err != nil {
		return TaskCreateReceipt{}, fmt.Errorf("obtain post-commit task TSO (task metadata and bootstrap guard remain): %w", err)
	}
	committedAtTS := oracle.ComposeTS(physical, logical)
	if committedAtTS < in.StartTS || committedAtTS >= in.EndTS {
		return TaskCreateReceipt{}, errors.New("post-commit task TSO is outside the task interval; task metadata and bootstrap guard remain")
	}
	return TaskCreateReceipt{Format: TaskCreateFormat, ClusterID: in.Preflight.ClusterID, Keyspace: in.Preflight.Keyspace, TaskName: in.Preflight.TaskName, StartTS: in.StartTS, CommittedAtTS: committedAtTS, EndTS: in.EndTS, StartKeyHex: in.Preflight.StartKeyHex, EndKeyHex: in.Preflight.EndKeyHex, LogStoragePrefix: logStoragePrefix, LogStorageSHA256: logStorageSHA256, PreflightSHA256: in.PreflightSHA256, OwnerKey: TaskOwnerKey, BootstrapSafePointID: bootstrapID, BootstrapSafePointTTL: bootstrapSafePointTTL, AtomicMetadataCreated: true}, nil
}

func validateTaskStorage(storage *backuppb.StorageBackend) error {
	if storage == nil {
		return errors.New("backup storage is required")
	}
	s3 := storage.GetS3()
	if s3 == nil {
		return errors.New("native PITR production task currently requires S3-compatible storage")
	}
	if s3.Bucket == "" || s3.Prefix == "" {
		return errors.New("S3 bucket and prefix are required")
	}
	if err := safeText("S3 bucket", s3.Bucket); err != nil {
		return err
	}
	if err := safeText("S3 prefix", s3.Prefix); err != nil {
		return err
	}
	if s3.AccessKey != "" || s3.SecretAccessKey != "" || s3.SessionToken != "" {
		return errors.New("embedded S3 credentials are forbidden; use workload identity")
	}
	if err := validateS3Prefix(canonicalS3Prefix(storage)); err != nil {
		return fmt.Errorf("log storage: %w", err)
	}
	return nil
}

func canonicalS3Prefix(storage *backuppb.StorageBackend) string {
	s3 := storage.GetS3()
	return "s3://" + s3.Bucket + "/" + strings.TrimLeft(s3.Prefix, "/")
}

func storageBackendSHA256(storage *backuppb.StorageBackend) (string, error) {
	b, err := storage.Marshal()
	if err != nil {
		return "", fmt.Errorf("marshal log storage backend: %w", err)
	}
	digest := sha256.Sum256(b)
	return hex.EncodeToString(digest[:]), nil
}

func cleanupBootstrap(ctx context.Context, safePoints SafePointClient, id string, cause error) error {
	_, cleanupErr := safePoints.UpdateServiceGCSafePoint(context.WithoutCancel(ctx), id, 0, 0)
	if cleanupErr != nil {
		return errors.Join(cause, fmt.Errorf("remove bootstrap GC safepoint: %w", cleanupErr))
	}
	return cause
}
