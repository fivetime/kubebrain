package nativepitr

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"math"
	"testing"

	backuppb "github.com/pingcap/kvproto/pkg/brpb"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/oracle"
)

type fakeSafePoints struct {
	minimum   uint64
	err       error
	commitTS  uint64
	commitErr error
	physical  int64
	logical   int64
	rawTS     bool
	calls     []struct {
		id  string
		ttl int64
		ts  uint64
	}
}

func (f *fakeSafePoints) GetTS(context.Context) (int64, int64, error) {
	if f.commitErr != nil {
		return 0, 0, f.commitErr
	}
	if f.rawTS {
		return f.physical, f.logical, nil
	}
	ts := f.commitTS
	if ts == 0 {
		ts = 110
	}
	return oracle.ExtractPhysical(ts), oracle.ExtractLogical(ts), nil
}

func TestComposePDTS(t *testing.T) {
	physical := int64(1_700_000_000_000)
	for _, logical := range []int64{0, 1, pdTSLogicalLimit - 1} {
		ts, err := ComposePDTS(physical, logical)
		require.NoError(t, err)
		require.Equal(t, physical, oracle.ExtractPhysical(ts))
		require.Equal(t, logical, oracle.ExtractLogical(ts))
	}
	for _, components := range [][2]int64{
		{0, 0},
		{-1, 0},
		{1, -1},
		{1, pdTSLogicalLimit},
		{math.MaxInt64>>pdTSLogicalBits + 1, 0},
	} {
		_, err := ComposePDTS(components[0], components[1])
		require.Error(t, err, components)
	}
}

func (f *fakeSafePoints) UpdateServiceGCSafePoint(_ context.Context, id string, ttl int64, ts uint64) (uint64, error) {
	f.calls = append(f.calls, struct {
		id  string
		ttl int64
		ts  uint64
	}{id, ttl, ts})
	return f.minimum, f.err
}

type fakeMetadata struct {
	created        bool
	err            error
	absent         []string
	absentPrefixes []string
	values         map[string][]byte
}

func (f *fakeMetadata) CreateIfAbsent(_ context.Context, absent, absentPrefixes []string, values map[string][]byte) (bool, error) {
	f.absent, f.values = absent, values
	f.absentPrefixes = absentPrefixes
	return f.created, f.err
}

func s3Storage() *backuppb.StorageBackend {
	return &backuppb.StorageBackend{Backend: &backuppb.StorageBackend_S3{S3: &backuppb.S3{Bucket: "bucket", Prefix: "immutable/task-1", Region: "us-east-1"}}}
}

func taskInput() TaskCreateInput {
	return TaskCreateInput{Preflight: validPreflight(), PreflightSHA256: digest, StartTS: 100, EndTS: 1_000, Storage: s3Storage(), OperationID: "operation-1"}
}

func TestCreateTaskInstallsGuardThenAtomicallyCreatesMetadata(t *testing.T) {
	sp := &fakeSafePoints{minimum: 90}
	meta := &fakeMetadata{created: true}
	receipt, err := CreateTask(context.Background(), sp, meta, taskInput())
	require.NoError(t, err)
	require.Len(t, sp.calls, 1)
	require.Equal(t, uint64(100), sp.calls[0].ts)
	require.Equal(t, bootstrapSafePointTTL, sp.calls[0].ttl)
	require.Equal(t, TaskCreateFormat, receipt.Format)
	require.True(t, receipt.AtomicMetadataCreated)
	require.Equal(t, "s3://bucket/immutable/task-1", receipt.LogStoragePrefix)
	require.Regexp(t, sha256RE, receipt.LogStorageSHA256)
	require.Equal(t, []string{TaskOwnerKey, taskInput().Preflight.TaskInfoKey, taskInput().Preflight.TaskRangesKey + string(mustDecodeHex(t, taskInput().Preflight.StartKeyHex))}, meta.absent)
	require.Equal(t, []string{
		"/tidb/br-stream/info/",
		taskInput().Preflight.TaskRangesKey,
		"/tidb/br-stream/checkpoint/" + taskInput().Preflight.TaskName + "/",
		"/tidb/br-stream/storage-checkpoint/" + taskInput().Preflight.TaskName + "/",
		"/tidb/br-stream/last-error/" + taskInput().Preflight.TaskName + "/",
	}, meta.absentPrefixes)

	var info backuppb.StreamBackupTaskInfo
	require.NoError(t, info.Unmarshal(meta.values[taskInput().Preflight.TaskInfoKey]))
	require.Equal(t, uint64(100), info.StartTs)
	require.Equal(t, taskInput().Preflight.TaskName, info.Name)
	require.Equal(t, "bucket", info.Storage.GetS3().Bucket)
	require.Equal(t, mustDecodeHex(t, taskInput().Preflight.EndKeyHex), meta.values[meta.absent[2]])
	var owner ownerRecord
	require.NoError(t, json.Unmarshal(meta.values[TaskOwnerKey], &owner))
	require.Equal(t, receipt.LogStoragePrefix, owner.LogStoragePrefix)
	require.Equal(t, receipt.LogStorageSHA256, owner.LogStorageSHA256)
}

func TestCreateTaskFailsClosedAndRemovesOwnGuard(t *testing.T) {
	t.Run("GC already passed", func(t *testing.T) {
		sp := &fakeSafePoints{minimum: 101}
		_, err := CreateTask(context.Background(), sp, &fakeMetadata{created: true}, taskInput())
		require.ErrorContains(t, err, "already passed")
		require.Len(t, sp.calls, 2)
		require.Zero(t, sp.calls[1].ttl)
	})
	t.Run("metadata collision", func(t *testing.T) {
		sp := &fakeSafePoints{minimum: 90}
		_, err := CreateTask(context.Background(), sp, &fakeMetadata{created: false}, taskInput())
		require.ErrorContains(t, err, "concurrently created")
		require.Len(t, sp.calls, 2)
		require.Zero(t, sp.calls[1].ttl)
	})
	t.Run("metadata error", func(t *testing.T) {
		sp := &fakeSafePoints{minimum: 90}
		meta := &fakeMetadata{err: errors.New("etcd down")}
		_, err := CreateTask(context.Background(), sp, meta, taskInput())
		require.ErrorContains(t, err, "etcd down")
		require.Len(t, sp.calls, 2)
	})
	t.Run("post-commit TSO error keeps guard", func(t *testing.T) {
		sp := &fakeSafePoints{minimum: 90, commitErr: errors.New("TSO unavailable")}
		_, err := CreateTask(context.Background(), sp, &fakeMetadata{created: true}, taskInput())
		require.ErrorContains(t, err, "task metadata and bootstrap guard remain")
		require.Len(t, sp.calls, 1)
	})
	t.Run("post-commit TSO outside interval keeps guard", func(t *testing.T) {
		sp := &fakeSafePoints{minimum: 90, commitTS: 1_000}
		_, err := CreateTask(context.Background(), sp, &fakeMetadata{created: true}, taskInput())
		require.ErrorContains(t, err, "outside the task interval")
		require.Len(t, sp.calls, 1)
	})
	t.Run("malformed post-commit TSO keeps task and guard", func(t *testing.T) {
		sp := &fakeSafePoints{minimum: 90, rawTS: true, physical: 1, logical: pdTSLogicalLimit}
		_, err := CreateTask(context.Background(), sp, &fakeMetadata{created: true}, taskInput())
		require.ErrorContains(t, err, "invalid post-commit task TSO")
		require.Len(t, sp.calls, 1)
	})
}

func TestCreateTaskRejectsUnsafeInputBeforeMutation(t *testing.T) {
	tests := []struct {
		name string
		edit func(*TaskCreateInput)
		want string
	}{
		{"bad timestamps", func(i *TaskCreateInput) { i.EndTS = i.StartTS }, "timestamps"},
		{"missing storage", func(i *TaskCreateInput) { i.Storage = nil }, "storage"},
		{"embedded credentials", func(i *TaskCreateInput) { i.Storage.GetS3().AccessKey = "secret" }, "credentials"},
		{"unsafe storage URI", func(i *TaskCreateInput) { i.Storage.GetS3().Bucket = "key:secret@bucket" }, "storage"},
		{"bad preflight digest", func(i *TaskCreateInput) { i.PreflightSHA256 = "bad" }, "SHA-256"},
		{"bad operation ID", func(i *TaskCreateInput) { i.OperationID = "" }, "operation ID"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			in := taskInput()
			tt.edit(&in)
			sp := &fakeSafePoints{}
			_, err := CreateTask(context.Background(), sp, &fakeMetadata{created: true}, in)
			require.ErrorContains(t, err, tt.want)
			require.Empty(t, sp.calls)
		})
	}
}

func mustDecodeHex(t *testing.T, value string) []byte {
	t.Helper()
	b, err := hex.DecodeString(value)
	require.NoError(t, err)
	return b
}
