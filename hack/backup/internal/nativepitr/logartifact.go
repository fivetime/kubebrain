package nativepitr

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/klauspost/compress/zstd"
	"github.com/kubewharf/kubebrain/hack/backup/internal/pitrinventory"
	backuppb "github.com/pingcap/kvproto/pkg/brpb"
)

const LogArtifactReceiptFormat = "kubebrain.native-pitr-log-artifacts.v2"

type LogArtifactObject struct {
	Name   string `json:"name"`
	Bytes  uint64 `json:"bytes"`
	SHA256 string `json:"sha256"`
	Kind   string `json:"kind"`
}

type LogArtifactReceipt struct {
	Format                 string              `json:"format"`
	ClusterID              uint64              `json:"cluster_id"`
	Keyspace               string              `json:"keyspace"`
	TaskName               string              `json:"task_name"`
	TaskCreateSHA256       string              `json:"task_create_receipt_sha256"`
	TaskReadySHA256        string              `json:"task_ready_receipt_sha256"`
	StartTS                uint64              `json:"start_ts"`
	GlobalCheckpointTS     uint64              `json:"global_checkpoint_ts"`
	StoragePrefix          string              `json:"storage_prefix"`
	StorageSHA256          string              `json:"storage_backend_sha256"`
	RemoteInventorySHA256  string              `json:"remote_inventory_sha256"`
	ObjectStoreID          string              `json:"object_store_id"`
	Bucket                 string              `json:"bucket"`
	ObjectPrefix           string              `json:"object_prefix"`
	MinRetainUntilUnix     int64               `json:"min_retain_until_unix"`
	InventoryCheckedAtUnix int64               `json:"inventory_checked_at_unix"`
	Objects                []LogArtifactObject `json:"objects"`
	ObjectCount            int                 `json:"object_count"`
	MetadataCount          int                 `json:"metadata_count"`
	DataObjectCount        int                 `json:"data_object_count"`
	ControlObjectCount     int                 `json:"control_object_count"`
	VerifiedSegmentCount   int                 `json:"verified_segment_count"`
	TotalBytes             uint64              `json:"total_bytes"`
	ManifestSHA256         string              `json:"manifest_sha256"`
	MetadataMaxResolvedTS  uint64              `json:"metadata_max_resolved_ts"`
	ExactMirror            bool                `json:"exact_local_mirror"`
	RemoteVersionsVerified bool                `json:"remote_exact_versions_verified"`
	AllSegmentsVerified    bool                `json:"all_segments_verified"`
}

type logDataObject struct {
	path     string
	length   uint64
	segments []*backuppb.DataFileInfo
}

func VerifyLogArtifacts(task TaskCreateReceipt, taskSHA string, ready TaskReadyReceipt, readySHA string, inventory pitrinventory.Receipt, inventorySHA, root string) (LogArtifactReceipt, error) {
	if err := validateTaskCreateReceipt(task); err != nil {
		return LogArtifactReceipt{}, err
	}
	if err := validateTaskReadyReceipt(ready); err != nil {
		return LogArtifactReceipt{}, err
	}
	if !readyMatchesTask(ready, task) {
		return LogArtifactReceipt{}, errors.New("task-ready receipt does not match task-create receipt")
	}
	if !sha256RE.MatchString(taskSHA) || !sha256RE.MatchString(readySHA) || !sha256RE.MatchString(inventorySHA) {
		return LogArtifactReceipt{}, errors.New("invalid native PITR receipt SHA-256")
	}
	if root == "" {
		return LogArtifactReceipt{}, errors.New("log artifact root is required")
	}
	if err := inventory.Validate(); err != nil {
		return LogArtifactReceipt{}, err
	}
	bucket, prefix, err := splitS3Prefix(task.LogStoragePrefix)
	if err != nil || inventory.Bucket != bucket || inventory.Prefix != prefix {
		return LogArtifactReceipt{}, errors.New("remote inventory does not match task log storage")
	}
	actual, err := scanMirror(root)
	if err != nil {
		return LogArtifactReceipt{}, err
	}
	if len(actual) != inventory.ObjectCount {
		return LogArtifactReceipt{}, errors.New("local log mirror does not match remote exact-version inventory")
	}
	inventoryEntries := make(map[string]pitrinventory.Entry, len(inventory.Entries))
	objects := make([]LogArtifactObject, 0, len(inventory.Entries))
	for _, entry := range inventory.Entries {
		if !actual[entry.Name] {
			return LogArtifactReceipt{}, fmt.Errorf("remote inventory object %q is missing from local mirror", entry.Name)
		}
		object, err := verifyInventoryObject(filepath.Join(root, filepath.FromSlash(entry.Name)), entry)
		if err != nil {
			return LogArtifactReceipt{}, fmt.Errorf("verify remote inventory object %q: %w", entry.Name, err)
		}
		inventoryEntries[entry.Name] = entry
		objects = append(objects, object)
	}
	metadataPaths := make([]string, 0)
	for name := range actual {
		if strings.HasPrefix(name, "v1/backupmeta/") && strings.HasSuffix(name, ".meta") {
			metadataPaths = append(metadataPaths, name)
		}
	}
	sort.Strings(metadataPaths)
	expected := make(map[string]*logDataObject)
	segmentCount := 0
	maxResolved := uint64(0)
	for _, name := range metadataPaths {
		if err := validateObjectName(name); err != nil {
			return LogArtifactReceipt{}, err
		}
		content, err := readBoundedPath(filepath.Join(root, filepath.FromSlash(name)), maxMetaIndexBytes)
		if err != nil {
			return LogArtifactReceipt{}, fmt.Errorf("read stream metadata %q: %w", name, err)
		}
		var meta backuppb.Metadata
		if err := meta.Unmarshal(content); err != nil {
			return LogArtifactReceipt{}, fmt.Errorf("decode stream metadata %q: %w", name, err)
		}
		if err := collectLogMetadata(&meta, expected, &segmentCount, &maxResolved); err != nil {
			return LogArtifactReceipt{}, fmt.Errorf("validate stream metadata %q: %w", name, err)
		}
		digest := sha256.Sum256(content)
		if entry := inventoryEntries[name]; entry.SHA256 != hex.EncodeToString(digest[:]) || entry.Bytes != int64(len(content)) {
			return LogArtifactReceipt{}, fmt.Errorf("stream metadata %q differs from remote inventory", name)
		}
		setLogObjectKind(objects, name, "metadata")
	}
	if (segmentCount == 0) != (len(expected) == 0) {
		return LogArtifactReceipt{}, errors.New("stream metadata contains no verifiable log segments")
	}
	dataNames := make([]string, 0, len(expected))
	for name := range expected {
		if !actual[name] {
			return LogArtifactReceipt{}, fmt.Errorf("required log data object %q is missing", name)
		}
		dataNames = append(dataNames, name)
	}
	sort.Strings(dataNames)
	for _, name := range dataNames {
		object, err := verifyLogDataObject(root, expected[name])
		if err != nil {
			return LogArtifactReceipt{}, fmt.Errorf("verify log data object %q: %w", name, err)
		}
		entry, ok := inventoryEntries[name]
		if !ok || entry.Bytes != int64(object.Bytes) || entry.SHA256 != object.SHA256 {
			return LogArtifactReceipt{}, fmt.Errorf("log data object %q differs from remote inventory", name)
		}
		setLogObjectKind(objects, name, "data")
	}
	sort.Slice(objects, func(i, j int) bool { return objects[i].Name < objects[j].Name })
	manifest, err := json.Marshal(objects)
	if err != nil {
		return LogArtifactReceipt{}, err
	}
	manifestDigest := sha256.Sum256(manifest)
	var total uint64
	for _, object := range objects {
		if math.MaxUint64-total < object.Bytes {
			return LogArtifactReceipt{}, errors.New("log artifact byte total overflows uint64")
		}
		total += object.Bytes
	}
	return LogArtifactReceipt{Format: LogArtifactReceiptFormat, ClusterID: task.ClusterID, Keyspace: task.Keyspace, TaskName: task.TaskName, TaskCreateSHA256: taskSHA, TaskReadySHA256: readySHA, StartTS: task.StartTS, GlobalCheckpointTS: ready.GlobalCheckpointTS, StoragePrefix: task.LogStoragePrefix, StorageSHA256: task.LogStorageSHA256, RemoteInventorySHA256: inventorySHA, ObjectStoreID: inventory.ObjectStoreID, Bucket: inventory.Bucket, ObjectPrefix: inventory.Prefix, MinRetainUntilUnix: inventory.MinRetainUntilUnix, InventoryCheckedAtUnix: inventory.CheckedAtUnix, Objects: objects, ObjectCount: len(objects), MetadataCount: len(metadataPaths), DataObjectCount: len(expected), ControlObjectCount: len(objects) - len(metadataPaths) - len(expected), VerifiedSegmentCount: segmentCount, TotalBytes: total, ManifestSHA256: hex.EncodeToString(manifestDigest[:]), MetadataMaxResolvedTS: maxResolved, ExactMirror: true, RemoteVersionsVerified: true, AllSegmentsVerified: true}, nil
}

func verifyInventoryObject(path string, entry pitrinventory.Entry) (LogArtifactObject, error) {
	f, err := os.Open(path)
	if err != nil {
		return LogArtifactObject{}, err
	}
	defer f.Close()
	h := sha256.New()
	n, err := io.Copy(h, io.LimitReader(f, entry.Bytes+1))
	if err != nil || n != entry.Bytes || hex.EncodeToString(h.Sum(nil)) != entry.SHA256 {
		return LogArtifactObject{}, errors.New("size or SHA-256 does not match remote exact version")
	}
	return LogArtifactObject{Name: entry.Name, Bytes: uint64(entry.Bytes), SHA256: entry.SHA256, Kind: "control"}, nil
}

func setLogObjectKind(objects []LogArtifactObject, name, kind string) {
	for i := range objects {
		if objects[i].Name == name {
			objects[i].Kind = kind
			return
		}
	}
}

func splitS3Prefix(value string) (string, string, error) {
	trimmed := strings.TrimPrefix(value, "s3://")
	parts := strings.SplitN(trimmed, "/", 2)
	if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
		return "", "", errors.New("invalid S3 prefix")
	}
	return parts[0], strings.TrimSuffix(parts[1], "/"), nil
}

func collectLogMetadata(meta *backuppb.Metadata, expected map[string]*logDataObject, segmentCount *int, maxResolved *uint64) error {
	if meta.MetaVersion != backuppb.MetaVersion_V1 && meta.MetaVersion != backuppb.MetaVersion_V2 {
		return errors.New("unsupported BR stream metadata version")
	}
	// MaxTs includes both commit timestamps and prewrite start timestamps. A
	// transaction may start after this file's resolved commit boundary, so BR
	// legitimately emits MaxTs > ResolvedTs.
	if meta.MinTs == 0 || meta.MaxTs < meta.MinTs || meta.ResolvedTs == 0 {
		return fmt.Errorf("invalid stream metadata timestamp bounds: min_ts=%d max_ts=%d resolved_ts=%d", meta.MinTs, meta.MaxTs, meta.ResolvedTs)
	}
	if meta.ResolvedTs > *maxResolved {
		*maxResolved = meta.ResolvedTs
	}
	if meta.MetaVersion == backuppb.MetaVersion_V1 {
		if len(meta.Files) == 0 || len(meta.FileGroups) != 0 {
			return errors.New("invalid v1 stream metadata inventory")
		}
		for _, file := range meta.Files {
			if err := addLogDataObject(expected, file.GetPath(), file.GetLength(), file); err != nil {
				return err
			}
		}
		*segmentCount += len(meta.Files)
		return nil
	}
	if len(meta.Files) != 0 || len(meta.FileGroups) == 0 {
		return errors.New("invalid v2 stream metadata inventory")
	}
	for _, group := range meta.FileGroups {
		if group == nil || group.Length == 0 || len(group.DataFilesInfo) == 0 {
			return errors.New("invalid v2 stream data-file group")
		}
		for _, file := range group.DataFilesInfo {
			if err := addLogDataObject(expected, group.Path, group.Length, file); err != nil {
				return err
			}
		}
		*segmentCount += len(group.DataFilesInfo)
	}
	return nil
}

func addLogDataObject(expected map[string]*logDataObject, name string, length uint64, file *backuppb.DataFileInfo) error {
	if file == nil {
		return errors.New("stream metadata contains a nil data-file entry")
	}
	if err := validateObjectName(name); err != nil {
		return err
	}
	if strings.HasPrefix(name, "v1/backupmeta/") && strings.HasSuffix(name, ".meta") {
		return errors.New("log data object collides with the BR metadata namespace")
	}
	if length > math.MaxInt64 || file.Length > math.MaxInt64 || file.RangeOffset > math.MaxInt64 || file.RangeLength > math.MaxInt64 {
		return fmt.Errorf("log object %q exceeds supported local verifier size", name)
	}
	if length == 0 || len(file.Sha256) != sha256.Size || file.Length == 0 || file.NumberOfEntries <= 0 || file.MinTs == 0 || file.MaxTs < file.MinTs || file.ResolvedTs == 0 || (file.Cf != "" && file.Cf != "default" && file.Cf != "write") {
		return fmt.Errorf("log segment in %q has incomplete digest, size, timestamp, entry, or CF metadata", name)
	}
	if file.CompressionType != backuppb.CompressionType_UNKNOWN && file.CompressionType != backuppb.CompressionType_ZSTD {
		return fmt.Errorf("log segment in %q uses unsupported compression", name)
	}
	if file.RangeOffset > length || file.RangeLength > length-file.RangeOffset {
		return fmt.Errorf("log segment range exceeds data object %q", name)
	}
	if file.RangeOffset != 0 || file.RangeLength != 0 {
		if file.RangeLength == 0 {
			return fmt.Errorf("log segment in %q has an empty merged range", name)
		}
	} else if file.CompressionType != backuppb.CompressionType_UNKNOWN || file.Length != length {
		return fmt.Errorf("unmerged log object %q has inconsistent length or compression", name)
	}
	object := expected[name]
	if object == nil {
		object = &logDataObject{path: name, length: length}
		expected[name] = object
	} else if object.length != length {
		return fmt.Errorf("log object %q has conflicting lengths", name)
	}
	object.segments = append(object.segments, file)
	return nil
}

func verifyLogDataObject(root string, object *logDataObject) (LogArtifactObject, error) {
	segments := append([]*backuppb.DataFileInfo(nil), object.segments...)
	sort.Slice(segments, func(i, j int) bool { return segments[i].RangeOffset < segments[j].RangeOffset })
	if len(segments) == 0 {
		return LogArtifactObject{}, errors.New("data object has no log segments")
	}
	merged := segments[0].RangeLength != 0
	if !merged {
		if len(segments) != 1 || segments[0].RangeOffset != 0 || segments[0].Length != object.length {
			return LogArtifactObject{}, errors.New("unmerged log object does not have exactly one complete segment")
		}
	} else {
		end := uint64(0)
		for _, segment := range segments {
			if segment.RangeLength == 0 || segment.RangeOffset != end {
				return LogArtifactObject{}, errors.New("merged log object segments overlap or leave an uncovered range")
			}
			end += segment.RangeLength
		}
		if end != object.length {
			return LogArtifactObject{}, errors.New("merged log object segments do not cover the physical object")
		}
	}
	path := filepath.Join(root, filepath.FromSlash(object.path))
	f, err := os.Open(path)
	if err != nil {
		return LogArtifactObject{}, err
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil || info.Size() < 0 || uint64(info.Size()) != object.length {
		return LogArtifactObject{}, errors.New("physical object length does not match stream metadata")
	}
	physicalHash := sha256.New()
	if _, err := io.Copy(physicalHash, f); err != nil {
		return LogArtifactObject{}, err
	}
	for _, segment := range segments {
		if _, err := f.Seek(0, io.SeekStart); err != nil {
			return LogArtifactObject{}, err
		}
		var input io.Reader = f
		if segment.RangeLength != 0 {
			input = io.NewSectionReader(f, int64(segment.RangeOffset), int64(segment.RangeLength))
		}
		if err := verifyLogSegment(input, segment); err != nil {
			return LogArtifactObject{}, err
		}
	}
	return LogArtifactObject{Name: object.path, Bytes: object.length, SHA256: hex.EncodeToString(physicalHash.Sum(nil)), Kind: "data"}, nil
}

func verifyLogSegment(input io.Reader, segment *backuppb.DataFileInfo) error {
	var decoder *zstd.Decoder
	if segment.CompressionType == backuppb.CompressionType_ZSTD {
		var err error
		decoder, err = zstd.NewReader(input, zstd.WithDecoderMaxMemory(1<<30))
		if err != nil {
			return err
		}
		defer decoder.Close()
		input = decoder
	}
	hash := sha256.New()
	n, err := io.Copy(hash, io.LimitReader(input, int64(segment.Length)+1))
	if err != nil {
		return err
	}
	if uint64(n) != segment.Length || !bytes.Equal(hash.Sum(nil), segment.Sha256) {
		return errors.New("decoded segment size or SHA-256 does not match stream metadata")
	}
	return nil
}

func DecodeLogArtifactReceipt(r io.Reader) (LogArtifactReceipt, error) {
	var receipt LogArtifactReceipt
	decoder := json.NewDecoder(r)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&receipt); err != nil {
		return receipt, fmt.Errorf("decode log artifact receipt: %w", err)
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return receipt, errors.New("log artifact receipt has trailing JSON")
	}
	if err := receipt.Validate(); err != nil {
		return receipt, err
	}
	return receipt, nil
}

func (r LogArtifactReceipt) Validate() error {
	if r.Format != LogArtifactReceiptFormat || r.ClusterID == 0 || !r.ExactMirror || !r.RemoteVersionsVerified || !r.AllSegmentsVerified {
		return errors.New("input is not a successful native PITR log-artifacts v2 receipt")
	}
	if !dnsLabel.MatchString(r.TaskName) || r.Keyspace == "" || r.StartTS == 0 || r.GlobalCheckpointTS < r.StartTS || r.ObjectCount != len(r.Objects) || r.MetadataCount < 0 || r.DataObjectCount < 0 || r.ControlObjectCount < 0 || r.VerifiedSegmentCount < 0 || r.ObjectCount != r.MetadataCount+r.DataObjectCount+r.ControlObjectCount || (r.MetadataCount == 0) != (r.DataObjectCount == 0) || (r.DataObjectCount == 0) != (r.VerifiedSegmentCount == 0) || (r.MetadataCount > 0 && r.MetadataMaxResolvedTS == 0) {
		return errors.New("log artifact receipt has invalid identity, timestamp, or inventory totals")
	}
	if !sha256RE.MatchString(r.TaskCreateSHA256) || !sha256RE.MatchString(r.TaskReadySHA256) || !sha256RE.MatchString(r.StorageSHA256) || !sha256RE.MatchString(r.RemoteInventorySHA256) || !sha256RE.MatchString(r.ManifestSHA256) || safeText("object store ID", r.ObjectStoreID) != nil || r.MinRetainUntilUnix <= r.InventoryCheckedAtUnix || r.InventoryCheckedAtUnix <= 0 {
		return errors.New("log artifact receipt has invalid digest evidence")
	}
	if err := validateS3Prefix(r.StoragePrefix); err != nil {
		return err
	}
	bucket, prefix, err := splitS3Prefix(r.StoragePrefix)
	if err != nil || r.Bucket != bucket || r.ObjectPrefix != prefix {
		return errors.New("log artifact receipt remote inventory scope does not match storage prefix")
	}
	copyObjects := append(make([]LogArtifactObject, 0, len(r.Objects)), r.Objects...)
	sort.Slice(copyObjects, func(i, j int) bool { return copyObjects[i].Name < copyObjects[j].Name })
	manifest, err := json.Marshal(copyObjects)
	if err != nil {
		return err
	}
	digest := sha256.Sum256(manifest)
	var total uint64
	metadataCount, dataCount := 0, 0
	for i, object := range copyObjects {
		if object != r.Objects[i] || validateObjectName(object.Name) != nil || object.Bytes == 0 || !sha256RE.MatchString(object.SHA256) || (object.Kind != "metadata" && object.Kind != "data" && object.Kind != "control") || (i > 0 && copyObjects[i-1].Name == object.Name) {
			return errors.New("log artifact receipt contains an invalid or unsorted object entry")
		}
		if object.Kind == "metadata" {
			metadataCount++
		} else if object.Kind == "data" {
			dataCount++
		}
		if math.MaxUint64-total < object.Bytes {
			return errors.New("log artifact receipt byte total overflows uint64")
		}
		total += object.Bytes
	}
	if metadataCount != r.MetadataCount || dataCount != r.DataObjectCount || len(r.Objects)-metadataCount-dataCount != r.ControlObjectCount || total != r.TotalBytes || hex.EncodeToString(digest[:]) != r.ManifestSHA256 {
		return errors.New("log artifact receipt inventory totals or manifest digest do not match")
	}
	return nil
}

func readBoundedPath(path string, limit int64) ([]byte, error) {
	return readLimitedFile(path, limit)
}
