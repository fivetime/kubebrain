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

	"github.com/kubewharf/kubebrain/hack/backup/internal/pitrinventory"
	backuppb "github.com/pingcap/kvproto/pkg/brpb"
)

const (
	ArtifactReceiptFormat       = "kubebrain.native-pitr-full-artifacts.v5"
	legacyArtifactReceiptFormat = "kubebrain.native-pitr-full-artifacts.v4"
	maxMetaIndexBytes           = int64(64 << 20)
)

type ArtifactObject struct {
	Name   string `json:"name"`
	Bytes  uint64 `json:"bytes"`
	SHA256 string `json:"sha256"`
	Kind   string `json:"kind"`
}

type ArtifactReceipt struct {
	Format                  string                `json:"format"`
	ClusterID               uint64                `json:"cluster_id"`
	Keyspace                string                `json:"keyspace"`
	TaskName                string                `json:"task_name"`
	BackupTS                uint64                `json:"backup_ts"`
	StoragePrefix           string                `json:"storage_prefix"`
	FullReceiptSHA256       string                `json:"full_snapshot_receipt_sha256"`
	BackupMetaSHA256        string                `json:"backupmeta_sha256"`
	RemoteInventorySHA256   string                `json:"remote_inventory_sha256"`
	ObjectStoreID           string                `json:"object_store_id"`
	Bucket                  string                `json:"bucket"`
	ObjectPrefix            string                `json:"object_prefix"`
	MinRetainUntilUnix      int64                 `json:"min_retain_until_unix"`
	InventoryCheckedAtUnix  int64                 `json:"inventory_checked_at_unix"`
	Objects                 []ArtifactObject      `json:"objects"`
	ObjectCount             int                   `json:"object_count"`
	TotalBytes              uint64                `json:"total_bytes"`
	ManifestSHA256          string                `json:"manifest_sha256"`
	ExactMirror             bool                  `json:"exact_local_mirror"`
	RemoteVersionsVerified  bool                  `json:"remote_exact_versions_verified"`
	Encryption              string                `json:"encryption"`
	EncryptionKeyID         string                `json:"encryption_key_id,omitempty"`
	BackupAttestationSHA256 string                `json:"full_backup_attestation_sha256"`
	BRBinarySHA256          string                `json:"br_binary_sha256"`
	BackupAttestation       FullBackupAttestation `json:"full_backup_attestation"`
	AllObjectsVerified      bool                  `json:"all_objects_verified"`
}

func VerifyFullArtifacts(full FullSnapshotReceipt, fullReceiptSHA256 string, attestation FullBackupAttestation, attestationSHA string, inventory pitrinventory.Receipt, inventorySHA, root string) (ArtifactReceipt, error) {
	return VerifyFullArtifactsWithEncryption(full, fullReceiptSHA256, attestation, attestationSHA, inventory, inventorySHA, root, nil)
}

// VerifyFullArtifactsWithEncryption decrypts metadata only for inventory
// interpretation. Digests, sizes, and exact-mirror evidence remain over the
// encrypted bytes stored by BR.
func VerifyFullArtifactsWithEncryption(full FullSnapshotReceipt, fullReceiptSHA256 string, attestation FullBackupAttestation, attestationSHA string, inventory pitrinventory.Receipt, inventorySHA, root string, encryptionKey []byte) (ArtifactReceipt, error) {
	if err := validateFullSnapshotReceipt(full); err != nil {
		return ArtifactReceipt{}, err
	}
	if !sha256RE.MatchString(fullReceiptSHA256) || !sha256RE.MatchString(inventorySHA) {
		return ArtifactReceipt{}, errors.New("invalid full-snapshot receipt SHA-256")
	}
	if err := attestation.Validate(); err != nil {
		return ArtifactReceipt{}, err
	}
	canonicalAttestationSHA, err := FullBackupAttestationSHA256(attestation)
	if err != nil || attestationSHA != canonicalAttestationSHA || attestation.StoragePrefix != full.StoragePrefix || attestation.BackupTS != full.BackupTS {
		return ArtifactReceipt{}, errors.New("full-backup attestation does not bind the full snapshot")
	}
	if root == "" {
		return ArtifactReceipt{}, errors.New("artifact root is required")
	}
	if err := inventory.Validate(); err != nil {
		return ArtifactReceipt{}, err
	}
	bucket, prefix, err := splitS3Prefix(full.StoragePrefix)
	if err != nil || inventory.Bucket != bucket || inventory.Prefix != prefix {
		return ArtifactReceipt{}, errors.New("remote inventory does not match full snapshot storage")
	}
	actual, err := scanMirror(root)
	if err != nil {
		return ArtifactReceipt{}, err
	}
	if len(actual) != inventory.ObjectCount {
		return ArtifactReceipt{}, errors.New("local full mirror does not match remote exact-version inventory")
	}
	remote := make(map[string]pitrinventory.Entry, len(inventory.Entries))
	for _, entry := range inventory.Entries {
		if !actual[entry.Name] {
			return ArtifactReceipt{}, fmt.Errorf("remote inventory object %q is missing from local mirror", entry.Name)
		}
		if _, err := verifyInventoryObject(filepath.Join(root, filepath.FromSlash(entry.Name)), entry); err != nil {
			return ArtifactReceipt{}, fmt.Errorf("verify remote inventory object %q: %w", entry.Name, err)
		}
		remote[entry.Name] = entry
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
	encryption := EncryptionIdentity{Method: attestation.CipherMethod, KeyID: attestation.EncryptionKeyID}
	plaintextMeta, err := decryptBRContent(backupMetaBytes, encryption, encryptionKey, nil, encryption.Method != CipherMethodPlaintext)
	if err != nil {
		return ArtifactReceipt{}, fmt.Errorf("decrypt local backupmeta: %w", err)
	}
	var meta backuppb.BackupMeta
	if err := meta.Unmarshal(plaintextMeta); err != nil {
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
	if err := walkMetaIndex(root, meta.FileIndex, expected, encryption, encryptionKey); err != nil {
		return ArtifactReceipt{}, err
	}
	if len(expected) == 0 {
		return ArtifactReceipt{}, errors.New("backupmeta contains no verifiable data objects")
	}
	if len(actual) != len(expected)+1 || !actual["backupmeta"] || len(remote) != len(expected)+1 {
		return ArtifactReceipt{}, errors.New("artifact root is not an exact backupmeta object mirror")
	}
	if entry := remote["backupmeta"]; entry.Bytes != int64(len(backupMetaBytes)) || entry.SHA256 != full.BackupMetaSHA256 {
		return ArtifactReceipt{}, errors.New("backupmeta differs from remote exact-version inventory")
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
		entry, ok := remote[name]
		if !ok || entry.Bytes != int64(object.Bytes) || entry.SHA256 != object.SHA256 {
			return ArtifactReceipt{}, fmt.Errorf("backup object %q differs from remote exact-version inventory", name)
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
	format := ArtifactReceiptFormat
	if attestation.Format == legacyFullBackupAttestationFormat {
		format = legacyArtifactReceiptFormat
	}
	return ArtifactReceipt{Format: format, ClusterID: full.ClusterID, Keyspace: full.Keyspace, TaskName: full.TaskName, BackupTS: full.BackupTS, StoragePrefix: full.StoragePrefix, FullReceiptSHA256: fullReceiptSHA256, BackupMetaSHA256: full.BackupMetaSHA256, RemoteInventorySHA256: inventorySHA, ObjectStoreID: inventory.ObjectStoreID, Bucket: inventory.Bucket, ObjectPrefix: inventory.Prefix, MinRetainUntilUnix: inventory.MinRetainUntilUnix, InventoryCheckedAtUnix: inventory.CheckedAtUnix, Objects: objects, ObjectCount: len(objects), TotalBytes: total, ManifestSHA256: hex.EncodeToString(manifestDigest[:]), ExactMirror: true, RemoteVersionsVerified: true, Encryption: attestation.CipherMethod, EncryptionKeyID: attestation.EncryptionKeyID, BackupAttestationSHA256: attestationSHA, BRBinarySHA256: attestation.BRBinarySHA256, BackupAttestation: attestation, AllObjectsVerified: true}, nil
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
	if (r.Format != ArtifactReceiptFormat && r.Format != legacyArtifactReceiptFormat) || r.ClusterID == 0 || !r.ExactMirror || !r.RemoteVersionsVerified || !r.AllObjectsVerified {
		return errors.New("input is not a successful native PITR full-artifacts receipt")
	}
	encryption := EncryptionIdentity{Method: r.Encryption, KeyID: r.EncryptionKeyID}
	if err := encryption.Validate(); err != nil || (r.Format == legacyArtifactReceiptFormat && encryption.Method != CipherMethodPlaintext) {
		return errors.New("full artifact receipt has invalid encryption identity")
	}
	if !dnsLabel.MatchString(r.TaskName) || r.Keyspace == "" || r.BackupTS == 0 || r.ObjectCount != len(r.Objects) || r.ObjectCount < 2 || r.TotalBytes == 0 {
		return errors.New("artifact receipt has invalid identity or inventory totals")
	}
	if !sha256RE.MatchString(r.FullReceiptSHA256) || !sha256RE.MatchString(r.BackupMetaSHA256) || !sha256RE.MatchString(r.RemoteInventorySHA256) || !sha256RE.MatchString(r.ManifestSHA256) || !sha256RE.MatchString(r.BackupAttestationSHA256) || !sha256RE.MatchString(r.BRBinarySHA256) || safeText("object store ID", r.ObjectStoreID) != nil || r.MinRetainUntilUnix <= r.InventoryCheckedAtUnix || r.InventoryCheckedAtUnix <= 0 {
		return errors.New("artifact receipt has invalid digest evidence")
	}
	attestationSHA, attestationErr := FullBackupAttestationSHA256(r.BackupAttestation)
	if attestationErr != nil || attestationSHA != r.BackupAttestationSHA256 || r.BackupAttestation.BRBinarySHA256 != r.BRBinarySHA256 || r.BackupAttestation.StoragePrefix != r.StoragePrefix || r.BackupAttestation.BackupTS != r.BackupTS || r.BackupAttestation.CipherMethod != r.Encryption || r.BackupAttestation.EncryptionKeyID != r.EncryptionKeyID {
		return errors.New("artifact receipt has invalid full-backup attestation")
	}
	if err := validateS3Prefix(r.StoragePrefix); err != nil {
		return err
	}
	bucket, prefix, err := splitS3Prefix(r.StoragePrefix)
	if err != nil || r.Bucket != bucket || r.ObjectPrefix != prefix {
		return errors.New("full artifact receipt remote inventory scope does not match storage prefix")
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

func walkMetaIndex(root string, index *backuppb.MetaFile, expected map[string]*backuppb.File, encryption EncryptionIdentity, encryptionKey []byte) error {
	if index == nil {
		return nil
	}
	for _, file := range index.DataFiles {
		if err := addExpectedFile(expected, file); err != nil {
			return err
		}
	}
	for _, node := range index.MetaFiles {
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
		plaintext, err := decryptBRContent(content, encryption, encryptionKey, node.CipherIv, false)
		if err != nil {
			return fmt.Errorf("decrypt meta index %q: %w", node.Name, err)
		}
		var child backuppb.MetaFile
		if err := child.Unmarshal(plaintext); err != nil {
			return fmt.Errorf("decode meta index %q: %w", node.Name, err)
		}
		if err := walkMetaIndex(root, &child, expected, encryption, encryptionKey); err != nil {
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
	// TiKV v7.5.1 emits a random CipherIv even when BackupRequest.cipher_info
	// defaults to Plaintext. The IV therefore cannot attest encryption mode.
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
