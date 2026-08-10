package nativepitr

import (
	"crypto/sha256"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/klauspost/compress/zstd"
	backuppb "github.com/pingcap/kvproto/pkg/brpb"
	"github.com/stretchr/testify/require"
)

func logArtifactFixture(t *testing.T, v2 bool) (TaskCreateReceipt, TaskReadyReceipt, string) {
	t.Helper()
	task, _ := readyTask(t)
	ready := readyReceiptFor(task)
	root := t.TempDir()
	raw := []byte("encoded-log-events")
	data := raw
	file := &backuppb.DataFileInfo{Path: "v1/log/1.log", NumberOfEntries: 2, MinTs: task.StartTS, MaxTs: ready.GlobalCheckpointTS, ResolvedTs: ready.GlobalCheckpointTS, Cf: "write", Length: uint64(len(raw))}
	h := sha256.Sum256(raw)
	file.Sha256 = h[:]
	meta := &backuppb.Metadata{StoreId: 1, MinTs: task.StartTS, MaxTs: ready.GlobalCheckpointTS, ResolvedTs: ready.GlobalCheckpointTS}
	if v2 {
		encoder, err := zstd.NewWriter(nil)
		require.NoError(t, err)
		data = encoder.EncodeAll(raw, nil)
		encoder.Close()
		file.Path = ""
		file.RangeLength = uint64(len(data))
		file.CompressionType = backuppb.CompressionType_ZSTD
		meta.MetaVersion = backuppb.MetaVersion_V2
		meta.FileGroups = []*backuppb.DataFileGroup{{Path: "v1/log/merged.log", DataFilesInfo: []*backuppb.DataFileInfo{file}, MinTs: file.MinTs, MaxTs: file.MaxTs, MinResolvedTs: file.ResolvedTs, Length: uint64(len(data))}}
	} else {
		meta.MetaVersion = backuppb.MetaVersion_V1
		meta.Files = []*backuppb.DataFileInfo{file}
	}
	metaBytes, err := meta.Marshal()
	require.NoError(t, err)
	metaPath := filepath.Join(root, "v1/backupmeta/0001.meta")
	require.NoError(t, os.MkdirAll(filepath.Dir(metaPath), 0o700))
	require.NoError(t, os.WriteFile(metaPath, metaBytes, 0o600))
	dataName := "v1/log/1.log"
	if v2 {
		dataName = "v1/log/merged.log"
	}
	require.NoError(t, os.MkdirAll(filepath.Join(root, "v1/log"), 0o700))
	require.NoError(t, os.WriteFile(filepath.Join(root, dataName), data, 0o600))
	return task, ready, root
}

func TestVerifyLogArtifactsV1AndCompressedV2(t *testing.T) {
	for _, v2 := range []bool{false, true} {
		t.Run(map[bool]string{false: "v1", true: "compressed-v2"}[v2], func(t *testing.T) {
			task, ready, root := logArtifactFixture(t, v2)
			receipt, err := VerifyLogArtifacts(task, digest, ready, digest, root)
			require.NoError(t, err)
			require.Equal(t, 2, receipt.ObjectCount)
			require.Equal(t, 1, receipt.VerifiedSegmentCount)
			require.NoError(t, receipt.Validate())
		})
	}
}

func TestVerifyLogArtifactsFailsClosed(t *testing.T) {
	tests := []struct {
		name string
		edit func(*testing.T, string)
		want string
	}{
		{"corrupt segment", func(t *testing.T, root string) {
			require.NoError(t, os.WriteFile(filepath.Join(root, "v1/log/1.log"), []byte("corrupt-log-events"), 0o600))
		}, "SHA-256"},
		{"missing data", func(t *testing.T, root string) { require.NoError(t, os.Remove(filepath.Join(root, "v1/log/1.log"))) }, "exact BR stream"},
		{"extra object", func(t *testing.T, root string) {
			require.NoError(t, os.WriteFile(filepath.Join(root, "extra"), []byte("x"), 0o600))
		}, "exact BR stream"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			task, ready, root := logArtifactFixture(t, false)
			tt.edit(t, root)
			_, err := VerifyLogArtifacts(task, digest, ready, digest, root)
			require.ErrorContains(t, err, tt.want)
		})
	}
}

func TestVerifyLogArtifactsRejectsDuplicateV1Object(t *testing.T) {
	task, ready, root := logArtifactFixture(t, false)
	metaPath := filepath.Join(root, "v1/backupmeta/0001.meta")
	b, err := os.ReadFile(metaPath)
	require.NoError(t, err)
	var meta backuppb.Metadata
	require.NoError(t, meta.Unmarshal(b))
	duplicate := *meta.Files[0]
	meta.Files = append(meta.Files, &duplicate)
	b, err = meta.Marshal()
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(metaPath, b, 0o600))
	_, err = VerifyLogArtifacts(task, digest, ready, digest, root)
	require.ErrorContains(t, err, "exactly one complete segment")
}

func TestDecodeLogArtifactReceiptIsStrictAndCanonical(t *testing.T) {
	task, ready, root := logArtifactFixture(t, false)
	receipt, err := VerifyLogArtifacts(task, digest, ready, digest, root)
	require.NoError(t, err)
	b, err := json.Marshal(receipt)
	require.NoError(t, err)
	_, err = DecodeLogArtifactReceipt(strings.NewReader(string(b)))
	require.NoError(t, err)
	_, err = DecodeLogArtifactReceipt(strings.NewReader(string(b) + `{}`))
	require.ErrorContains(t, err, "trailing")
	receipt.Objects[0], receipt.Objects[1] = receipt.Objects[1], receipt.Objects[0]
	b, err = json.Marshal(receipt)
	require.NoError(t, err)
	_, err = DecodeLogArtifactReceipt(strings.NewReader(string(b)))
	require.ErrorContains(t, err, "unsorted")
}
