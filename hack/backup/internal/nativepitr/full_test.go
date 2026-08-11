package nativepitr

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"strings"
	"testing"

	backuppb "github.com/pingcap/kvproto/pkg/brpb"
	"github.com/stretchr/testify/require"
)

func fullMeta(t *testing.T, task TaskCreateReceipt) []byte {
	t.Helper()
	b, err := (&backuppb.BackupMeta{ClusterId: task.ClusterID, BrVersion: "v7.5.1", ClusterVersion: "7.5.1", Version: 1, StartVersion: 0, EndVersion: 120, IsTxnKv: true, FileIndex: &backuppb.MetaFile{DataFiles: []*backuppb.File{{Name: "1_2_3.sst"}}}}).Marshal()
	require.NoError(t, err)
	return b
}

func TestBuildFullSnapshotUsesBackupMetaAuthority(t *testing.T) {
	task, _ := readyTask(t)
	b := fullMeta(t, task)
	receipt, err := BuildFullSnapshot(task, digest, "s3://bucket/immutable/full-1", b)
	require.NoError(t, err)
	wantDigest := sha256.Sum256(b)
	require.Equal(t, uint64(120), receipt.BackupTS)
	require.Equal(t, hex.EncodeToString(wantDigest[:]), receipt.BackupMetaSHA256)
	require.Equal(t, int64(len(b)), receipt.BackupMetaBytes)
	require.True(t, receipt.Transactional)
	require.Equal(t, "whole-cluster", receipt.Scope)
	require.False(t, receipt.ObjectExistenceChecked)
	require.True(t, receipt.HasFileIndex)
}

func TestBuildFullSnapshotAcceptsBRTransactionalEmptyIndexes(t *testing.T) {
	task, _ := readyTask(t)
	var meta backuppb.BackupMeta
	require.NoError(t, meta.Unmarshal(fullMeta(t, task)))
	meta.SchemaIndex = &backuppb.MetaFile{}
	meta.RawRangeIndex = &backuppb.MetaFile{}
	meta.DdlIndexes = &backuppb.MetaFile{}
	meta.Ddls = []byte("[]")
	b, err := meta.Marshal()
	require.NoError(t, err)

	_, err = BuildFullSnapshot(task, digest, "s3://bucket/immutable/full-1", b)
	require.NoError(t, err)
}

func TestBuildFullSnapshotRejectsUnboundOrIncompleteMetadata(t *testing.T) {
	tests := []struct {
		name string
		edit func(*TaskCreateReceipt, *backuppb.BackupMeta, *string)
		want string
	}{
		{"cluster mismatch", func(_ *TaskCreateReceipt, m *backuppb.BackupMeta, _ *string) { m.ClusterId++ }, "cluster ID"},
		{"not txn", func(_ *TaskCreateReceipt, m *backuppb.BackupMeta, _ *string) { m.IsTxnKv = false }, "transactional"},
		{"raw mode", func(_ *TaskCreateReceipt, m *backuppb.BackupMeta, _ *string) { m.IsRawKv = true }, "transactional"},
		{"incremental", func(_ *TaskCreateReceipt, m *backuppb.BackupMeta, _ *string) { m.StartVersion = 1 }, "full point-in-time"},
		{"before task", func(task *TaskCreateReceipt, m *backuppb.BackupMeta, _ *string) {
			m.EndVersion = task.StartTS - 1
		}, "precedes task metadata commit"},
		{"no files", func(_ *TaskCreateReceipt, m *backuppb.BackupMeta, _ *string) { m.FileIndex = nil }, "inventory"},
		{"unexpected schema index", func(_ *TaskCreateReceipt, m *backuppb.BackupMeta, _ *string) {
			m.SchemaIndex = &backuppb.MetaFile{Schemas: []*backuppb.Schema{{Db: []byte("db")}}}
		}, "unexpected schema"},
		{"unexpected DDL", func(_ *TaskCreateReceipt, m *backuppb.BackupMeta, _ *string) {
			m.Ddls = []byte(`[{"query":"create table t"}]`)
		}, "unexpected schema"},
		{"unsafe storage", func(_ *TaskCreateReceipt, _ *backuppb.BackupMeta, s *string) { *s = "s3://key:secret@bucket/prefix" }, "storage prefix"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			task, _ := readyTask(t)
			var meta backuppb.BackupMeta
			require.NoError(t, meta.Unmarshal(fullMeta(t, task)))
			storage := "s3://bucket/immutable/full-1"
			tt.edit(&task, &meta, &storage)
			b, err := meta.Marshal()
			require.NoError(t, err)
			_, err = BuildFullSnapshot(task, digest, storage, b)
			require.ErrorContains(t, err, tt.want)
		})
	}
}

func TestDecodeFullSnapshotIsStrict(t *testing.T) {
	task, _ := readyTask(t)
	receipt, err := BuildFullSnapshot(task, digest, "s3://bucket/immutable/full-1", fullMeta(t, task))
	require.NoError(t, err)
	b, err := json.Marshal(receipt)
	require.NoError(t, err)
	_, err = DecodeFullSnapshot(strings.NewReader(string(b)))
	require.NoError(t, err)
	_, err = DecodeFullSnapshot(strings.NewReader(string(b) + `{}`))
	require.ErrorContains(t, err, "trailing")
}
