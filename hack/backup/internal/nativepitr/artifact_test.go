package nativepitr

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	backuppb "github.com/pingcap/kvproto/pkg/brpb"
	"github.com/stretchr/testify/require"
)

func fileFor(name, cf string, content []byte) *backuppb.File {
	digest := sha256.Sum256(content)
	return &backuppb.File{Name: name, Cf: cf, Size_: uint64(len(content)), Sha256: digest[:]}
}

func artifactFixture(t *testing.T, indexed bool) (FullSnapshotReceipt, string, string) {
	t.Helper()
	task, _ := readyTask(t)
	root := t.TempDir()
	data := []byte("transactional-sst-content")
	dataFile := fileFor("1/region_write.sst", "write", data)
	require.NoError(t, os.MkdirAll(filepath.Join(root, "1"), 0o700))
	require.NoError(t, os.WriteFile(filepath.Join(root, filepath.FromSlash(dataFile.Name)), data, 0o600))
	meta := &backuppb.BackupMeta{ClusterId: task.ClusterID, StartVersion: 0, EndVersion: 120, IsTxnKv: true}
	if indexed {
		childBytes, err := (&backuppb.MetaFile{DataFiles: []*backuppb.File{dataFile}}).Marshal()
		require.NoError(t, err)
		node := fileFor("backupmeta.datafile.000000001", "", childBytes)
		require.NoError(t, os.WriteFile(filepath.Join(root, node.Name), childBytes, 0o600))
		meta.FileIndex = &backuppb.MetaFile{MetaFiles: []*backuppb.File{node}}
	} else {
		meta.Files = []*backuppb.File{dataFile}
	}
	metaBytes, err := meta.Marshal()
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(filepath.Join(root, "backupmeta"), metaBytes, 0o600))
	full, err := BuildFullSnapshot(task, digest, "s3://bucket/immutable/full-artifacts", metaBytes)
	require.NoError(t, err)
	return full, root, digest
}

func TestVerifyFullArtifactsLegacyAndRecursiveIndex(t *testing.T) {
	for _, indexed := range []bool{false, true} {
		t.Run(map[bool]string{false: "legacy", true: "recursive index"}[indexed], func(t *testing.T) {
			full, root, fullDigest := artifactFixture(t, indexed)
			inventory := inventoryForMirror(t, full.StoragePrefix, root)
			receipt, err := VerifyFullArtifacts(full, fullDigest, inventory, digest, root)
			require.NoError(t, err)
			require.True(t, receipt.AllObjectsVerified)
			wantCount := 2
			if indexed {
				wantCount = 3
			}
			require.Equal(t, wantCount, receipt.ObjectCount)
			require.NoError(t, receipt.Validate())
		})
	}
}

func TestVerifyFullArtifactsFailsClosed(t *testing.T) {
	tests := []struct {
		name string
		edit func(*testing.T, string)
		want string
	}{
		{"corrupt object", func(t *testing.T, root string) {
			require.NoError(t, os.WriteFile(filepath.Join(root, "1/region_write.sst"), []byte("corrupt"), 0o600))
		}, "size or SHA-256"},
		{"missing object", func(t *testing.T, root string) {
			require.NoError(t, os.Remove(filepath.Join(root, "1/region_write.sst")))
		}, "local full mirror"},
		{"extra object", func(t *testing.T, root string) {
			require.NoError(t, os.WriteFile(filepath.Join(root, "extra"), []byte("extra"), 0o600))
		}, "local full mirror"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			full, root, fullDigest := artifactFixture(t, false)
			inventory := inventoryForMirror(t, full.StoragePrefix, root)
			tt.edit(t, root)
			_, err := VerifyFullArtifacts(full, fullDigest, inventory, digest, root)
			require.ErrorContains(t, err, tt.want)
		})
	}
}

func TestVerifyFullArtifactsRejectsUnsafeAndEncryptedMetadata(t *testing.T) {
	task, _ := readyTask(t)
	for _, file := range []*backuppb.File{
		{Name: "../escape.sst", Size_: 1, Sha256: make([]byte, 32)},
		{Name: "data.sst", Size_: 1, Sha256: make([]byte, 32), CipherIv: []byte("iv")},
	} {
		metaBytes, err := (&backuppb.BackupMeta{ClusterId: task.ClusterID, StartVersion: 0, EndVersion: 120, IsTxnKv: true, Files: []*backuppb.File{file}}).Marshal()
		require.NoError(t, err)
		full, err := BuildFullSnapshot(task, digest, "s3://bucket/immutable/full-artifacts", metaBytes)
		require.NoError(t, err)
		root := t.TempDir()
		require.NoError(t, os.WriteFile(filepath.Join(root, "backupmeta"), metaBytes, 0o600))
		inventory := inventoryForMirror(t, full.StoragePrefix, root)
		_, err = VerifyFullArtifacts(full, digest, inventory, digest, root)
		require.Error(t, err)
	}
}

func TestVerifyFullArtifactsRejectsRemoteVersionMismatch(t *testing.T) {
	full, root, fullDigest := artifactFixture(t, false)
	inventory := inventoryForMirror(t, full.StoragePrefix, root)
	inventory.Entries[0].SHA256 = strings.Repeat("f", 64)
	_, err := VerifyFullArtifacts(full, fullDigest, inventory, digest, root)
	require.ErrorContains(t, err, "remote inventory object")
}

func TestDecodeArtifactReceiptIsStrict(t *testing.T) {
	full, root, fullDigest := artifactFixture(t, false)
	inventory := inventoryForMirror(t, full.StoragePrefix, root)
	receipt, err := VerifyFullArtifacts(full, fullDigest, inventory, digest, root)
	require.NoError(t, err)
	b, err := json.Marshal(receipt)
	require.NoError(t, err)
	_, err = DecodeArtifactReceipt(strings.NewReader(string(b)))
	require.NoError(t, err)
	_, err = DecodeArtifactReceipt(strings.NewReader(string(b) + `{}`))
	require.ErrorContains(t, err, "trailing")

	receipt.Objects[0].SHA256 = hex.EncodeToString(make([]byte, 32))
	b, err = json.Marshal(receipt)
	require.NoError(t, err)
	_, err = DecodeArtifactReceipt(strings.NewReader(string(b)))
	require.ErrorContains(t, err, "manifest digest")

	receipt, err = VerifyFullArtifacts(full, fullDigest, inventory, digest, root)
	require.NoError(t, err)
	receipt.Objects[0], receipt.Objects[1] = receipt.Objects[1], receipt.Objects[0]
	b, err = json.Marshal(receipt)
	require.NoError(t, err)
	_, err = DecodeArtifactReceipt(strings.NewReader(string(b)))
	require.ErrorContains(t, err, "canonically sorted")
}
