package nativepitr

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strings"

	backuppb "github.com/pingcap/kvproto/pkg/brpb"
)

const (
	ArtifactReceiptFormat = "kubebrain.native-pitr-full-artifacts.v1"
	maxMetaIndexBytes     = int64(64 << 20)
)

type ArtifactObject struct {
	Name   string `json:"name"`
	Bytes  uint64 `json:"bytes"`
	SHA256 string `json:"sha256"`
	Kind   string `json:"kind"`
}

type ArtifactReceipt struct {
	Format             string           `json:"format"`
	ClusterID          uint64           `json:"cluster_id"`
	Keyspace           string           `json:"keyspace"`
	TaskName           string           `json:"task_name"`
	BackupTS           uint64           `json:"backup_ts"`
	StoragePrefix      string           `json:"storage_prefix"`
	FullReceiptSHA256  string           `json:"full_snapshot_receipt_sha256"`
	BackupMetaSHA256   string           `json:"backupmeta_sha256"`
	Objects            []ArtifactObject `json:"objects"`
	ObjectCount        int              `json:"object_count"`
	TotalBytes         uint64           `json:"total_bytes"`
	ManifestSHA256     string           `json:"manifest_sha256"`
	ExactMirror        bool             `json:"exact_local_mirror"`
	Encryption         string           `json:"encryption"`
	AllObjectsVerified bool             `json:"all_objects_verified"`
}

func VerifyFullArtifacts(full FullSnapshotReceipt, fullReceiptSHA256, root string) (ArtifactReceipt, error) {
	if err := validateFullSnapshotReceipt(full); err != nil {
		return ArtifactReceipt{}, err
	}
	if !sha256RE.MatchString(fullReceiptSHA256) {
		return ArtifactReceipt{}, errors.New("invalid full-snapshot receipt SHA-256")
	}
	if root == "" {
		return ArtifactReceipt{}, errors.New("artifact root is required")
	}
	backupMetaPath := filepath.Join(root, "backupmeta")
	backupMetaBytes, err := readLimitedFile(backupMetaPath, full.BackupMetaBytes)
	if err != nil {
		return ArtifactReceipt{}, fmt.Errorf("read backupmeta: %w", err)
	}
	backupMetaDigest := sha256.Sum256(backupMetaBytes)
	if int64(len(backupMetaBytes)) != full.BackupMetaBytes || hex.EncodeToString(backupMetaDigest[:]) != full.BackupMetaSHA256 {
		return ArtifactReceipt{}, errors.New("local backupmeta does not match full-snapshot receipt")
	}
	var meta backuppb.BackupMeta
	if err := meta.Unmarshal(backupMetaBytes); err != nil {
		return ArtifactReceipt{}, fmt.Errorf("decode local backupmeta: %w", err)
	}
	if meta.ClusterId != full.ClusterID || meta.EndVersion != full.BackupTS || meta.StartVersion != 0 || !meta.IsTxnKv || meta.IsRawKv {
		return ArtifactReceipt{}, errors.New("local backupmeta identity does not match full-snapshot receipt")
	}
	expected := map[string]*backuppb.File{}
	for _, file := range meta.Files {
		if err := addExpectedFile(expected, file); err != nil {
			return ArtifactReceipt{}, err
		}
	}
	if err := walkMetaIndex(root, meta.FileIndex, expected); err != nil {
		return ArtifactReceipt{}, err
	}
	if len(expected) == 0 {
		return ArtifactReceipt{}, errors.New("backupmeta contains no verifiable data objects")
	}
	actual, err := scanMirror(root)
	if err != nil {
		return ArtifactReceipt{}, err
	}
	if len(actual) != len(expected)+1 || !actual["backupmeta"] {
		return ArtifactReceipt{}, errors.New("artifact root is not an exact backupmeta object mirror")
	}
	objects := make([]ArtifactObject, 0, len(expected)+1)
	objects = append(objects, ArtifactObject{Name: "backupmeta", Bytes: uint64(len(backupMetaBytes)), SHA256: full.BackupMetaSHA256, Kind: "backupmeta"})
	for name, file := range expected {
		if !actual[name] {
			return ArtifactReceipt{}, fmt.Errorf("required backup object %q is missing", name)
		}
		object, err := verifyObject(filepath.Join(root, filepath.FromSlash(name)), file, objectKind(file))
		if err != nil {
			return ArtifactReceipt{}, fmt.Errorf("verify backup object %q: %w", name, err)
		}
		objects = append(objects, object)
	}
	sort.Slice(objects, func(i, j int) bool { return objects[i].Name < objects[j].Name })
	manifest, err := json.Marshal(objects)
	if err != nil {
		return ArtifactReceipt{}, err
	}
	manifestDigest := sha256.Sum256(manifest)
	var total uint64
	for _, object := range objects {
		if math.MaxUint64-total < object.Bytes {
			return ArtifactReceipt{}, errors.New("backup artifact byte total overflows uint64")
		}
		total += object.Bytes
	}
	return ArtifactReceipt{Format: ArtifactReceiptFormat, ClusterID: full.ClusterID, Keyspace: full.Keyspace, TaskName: full.TaskName, BackupTS: full.BackupTS, StoragePrefix: full.StoragePrefix, FullReceiptSHA256: fullReceiptSHA256, BackupMetaSHA256: full.BackupMetaSHA256, Objects: objects, ObjectCount: len(objects), TotalBytes: total, ManifestSHA256: hex.EncodeToString(manifestDigest[:]), ExactMirror: true, Encryption: "plaintext", AllObjectsVerified: true}, nil
}

func DecodeArtifactReceipt(r io.Reader) (ArtifactReceipt, error) {
	var receipt ArtifactReceipt
	decoder := json.NewDecoder(r)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&receipt); err != nil {
		return receipt, fmt.Errorf("decode artifact receipt: %w", err)
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return receipt, errors.New("artifact receipt has trailing JSON")
	}
	if err := receipt.Validate(); err != nil {
		return receipt, err
	}
	return receipt, nil
}

func (r ArtifactReceipt) Validate() error {
	if r.Format != ArtifactReceiptFormat || r.ClusterID == 0 || !r.ExactMirror || !r.AllObjectsVerified || r.Encryption != "plaintext" {
		return errors.New("input is not a successful native PITR full-artifacts v1 receipt")
	}
	if !dnsLabel.MatchString(r.TaskName) || r.Keyspace == "" || r.BackupTS == 0 || r.ObjectCount != len(r.Objects) || r.ObjectCount < 2 || r.TotalBytes == 0 {
		return errors.New("artifact receipt has invalid identity or inventory totals")
	}
	if !sha256RE.MatchString(r.FullReceiptSHA256) || !sha256RE.MatchString(r.BackupMetaSHA256) || !sha256RE.MatchString(r.ManifestSHA256) {
		return errors.New("artifact receipt has invalid digest evidence")
	}
	if err := validateS3Prefix(r.StoragePrefix); err != nil {
		return err
	}
	copyObjects := append([]ArtifactObject(nil), r.Objects...)
	sort.Slice(copyObjects, func(i, j int) bool { return copyObjects[i].Name < copyObjects[j].Name })
	manifest, err := json.Marshal(copyObjects)
	if err != nil {
		return err
	}
	digest := sha256.Sum256(manifest)
	var total uint64
	seen := map[string]bool{}
	for i, object := range copyObjects {
		if object != r.Objects[i] {
			return errors.New("artifact receipt object inventory is not canonically sorted")
		}
		nameValid := object.Name == "backupmeta"
		if !nameValid {
			nameValid = validateObjectName(object.Name) == nil
		}
		if !nameValid || object.Bytes == 0 || !sha256RE.MatchString(object.SHA256) || (object.Kind != "backupmeta" && object.Kind != "data" && object.Kind != "meta-index") || (object.Name == "backupmeta") != (object.Kind == "backupmeta") || seen[object.Name] {
			return errors.New("artifact receipt contains an invalid object entry")
		}
		seen[object.Name] = true
		if math.MaxUint64-total < object.Bytes {
			return errors.New("artifact receipt byte total overflows uint64")
		}
		total += object.Bytes
	}
	if !seen["backupmeta"] || total != r.TotalBytes || hex.EncodeToString(digest[:]) != r.ManifestSHA256 {
		return errors.New("artifact receipt inventory totals or manifest digest do not match")
	}
	return nil
}

func walkMetaIndex(root string, index *backuppb.MetaFile, expected map[string]*backuppb.File) error {
	if index == nil {
		return nil
	}
	for _, file := range index.DataFiles {
		if err := addExpectedFile(expected, file); err != nil {
			return err
		}
	}
	for _, node := range index.MetaFiles {
		if len(node.CipherIv) != 0 {
			return errors.New("encrypted BR meta indexes are not supported without a bound crypter key")
		}
		if err := addExpectedFile(expected, node); err != nil {
			return err
		}
		path := filepath.Join(root, filepath.FromSlash(node.Name))
		content, err := readLimitedFile(path, maxMetaIndexBytes)
		if err != nil {
			return fmt.Errorf("read meta index %q: %w", node.Name, err)
		}
		if err := verifyFileBytes(content, node); err != nil {
			return fmt.Errorf("verify meta index %q: %w", node.Name, err)
		}
		var child backuppb.MetaFile
		if err := child.Unmarshal(content); err != nil {
			return fmt.Errorf("decode meta index %q: %w", node.Name, err)
		}
		if err := walkMetaIndex(root, &child, expected); err != nil {
			return err
		}
	}
	return nil
}

func addExpectedFile(expected map[string]*backuppb.File, file *backuppb.File) error {
	if file == nil {
		return errors.New("backupmeta contains a nil file entry")
	}
	if err := validateObjectName(file.Name); err != nil {
		return err
	}
	if len(file.Sha256) != sha256.Size || file.Size_ == 0 {
		return fmt.Errorf("backup object %q has incomplete size or SHA-256 metadata", file.Name)
	}
	if len(file.CipherIv) != 0 {
		return fmt.Errorf("encrypted backup object %q is not supported without a bound crypter key", file.Name)
	}
	if _, exists := expected[file.Name]; exists {
		return fmt.Errorf("backupmeta contains duplicate object %q", file.Name)
	}
	expected[file.Name] = file
	return nil
}

func validateObjectName(name string) error {
	if name == "" || name == "backupmeta" || strings.Contains(name, "\\") || strings.ContainsAny(name, "\x00\r\n") || filepath.IsAbs(name) {
		return fmt.Errorf("invalid backup object name %q", name)
	}
	clean := filepath.ToSlash(filepath.Clean(filepath.FromSlash(name)))
	if clean != name || clean == "." || strings.HasPrefix(clean, "../") {
		return fmt.Errorf("invalid backup object name %q", name)
	}
	return nil
}

func scanMirror(root string) (map[string]bool, error) {
	actual := map[string]bool{}
	err := filepath.WalkDir(root, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if path == root || entry.IsDir() {
			return nil
		}
		if entry.Type()&os.ModeSymlink != 0 || !entry.Type().IsRegular() {
			return fmt.Errorf("artifact mirror contains non-regular file %q", path)
		}
		relative, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		name := filepath.ToSlash(relative)
		if actual[name] {
			return fmt.Errorf("artifact mirror contains duplicate path %q", name)
		}
		actual[name] = true
		return nil
	})
	return actual, err
}

func verifyObject(path string, file *backuppb.File, kind string) (ArtifactObject, error) {
	f, err := os.Open(path)
	if err != nil {
		return ArtifactObject{}, err
	}
	defer f.Close()
	hash := sha256.New()
	written, err := io.Copy(hash, f)
	if err != nil {
		return ArtifactObject{}, err
	}
	if uint64(written) != file.Size_ || !bytes.Equal(hash.Sum(nil), file.Sha256) {
		return ArtifactObject{}, errors.New("size or SHA-256 does not match backupmeta")
	}
	return ArtifactObject{Name: file.Name, Bytes: uint64(written), SHA256: hex.EncodeToString(file.Sha256), Kind: kind}, nil
}

func verifyFileBytes(content []byte, file *backuppb.File) error {
	digest := sha256.Sum256(content)
	if uint64(len(content)) != file.Size_ || !bytes.Equal(digest[:], file.Sha256) {
		return errors.New("size or SHA-256 does not match backupmeta")
	}
	return nil
}

func objectKind(file *backuppb.File) string {
	if file.Cf == "default" || file.Cf == "write" {
		return "data"
	}
	return "meta-index"
}

func readLimitedFile(path string, limit int64) ([]byte, error) {
	if limit <= 0 {
		return nil, errors.New("invalid file size limit")
	}
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	b, err := io.ReadAll(io.LimitReader(f, limit+1))
	if err != nil {
		return nil, err
	}
	if int64(len(b)) > limit {
		return nil, fmt.Errorf("file exceeds %d bytes", limit)
	}
	return b, nil
}
