package objectstore

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/kubewharf/kubebrain/hack/backup/internal/backupfile"
	"github.com/kubewharf/kubebrain/hack/production/operationaudit"
)

const InventoryManifestFormat = "kubebrain.object-inventory-manifest.v1"
const InventoryReceiptFormat = "kubebrain.object-inventory.receipt.v1"

type InventoryEntry struct {
	ArtifactFormat  string `json:"artifact_format"`
	ObjectKey       string `json:"object_key"`
	VersionID       string `json:"version_id"`
	ArtifactSHA256  string `json:"artifact_sha256"`
	ObjectBytes     int64  `json:"object_bytes"`
	RetentionMode   string `json:"retention_mode"`
	RetainUntilUnix int64  `json:"retain_until_unix"`
}

type InventoryManifest struct {
	Format        string           `json:"format"`
	ObjectStoreID string           `json:"object_store_id"`
	Bucket        string           `json:"bucket"`
	Prefix        string           `json:"prefix"`
	Entries       []InventoryEntry `json:"entries"`
}

type InventoryManifestStatus struct {
	Manifest InventoryManifest
	SHA256   string
}

type InventoryRequest struct {
	Input         string
	ObjectStoreID string
	ReceiptOutput string
	Now           time.Time
}

type InventoryReceipt struct {
	Format           string `json:"format"`
	ObjectStoreID    string `json:"object_store_id"`
	Bucket           string `json:"bucket"`
	Prefix           string `json:"prefix"`
	ManifestSHA256   string `json:"manifest_sha256"`
	ExpectedVersions int    `json:"expected_versions"`
	RemoteVersions   int    `json:"remote_versions"`
	DeleteMarkers    int    `json:"delete_markers"`
	AllMatched       bool   `json:"all_matched"`
	CheckedAtUnix    int64  `json:"checked_at_unix"`
}

func InspectInventoryManifest(path string) (InventoryManifestStatus, error) {
	data, err := readBoundedObjectStoreJSONFile(path, "inventory manifest")
	if err != nil {
		return InventoryManifestStatus{}, err
	}
	var manifest InventoryManifest
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&manifest); err != nil {
		return InventoryManifestStatus{}, fmt.Errorf("decode inventory manifest: %w", err)
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		return InventoryManifestStatus{}, errors.New("inventory manifest contains trailing JSON")
	}
	canonical, err := json.Marshal(manifest)
	if err != nil {
		return InventoryManifestStatus{}, err
	}
	canonical = append(canonical, '\n')
	if !bytes.Equal(data, canonical) {
		return InventoryManifestStatus{}, errors.New("inventory manifest is not canonical")
	}
	if err := manifest.Validate(); err != nil {
		return InventoryManifestStatus{}, err
	}
	sum := sha256.Sum256(data)
	return InventoryManifestStatus{Manifest: manifest, SHA256: hex.EncodeToString(sum[:])}, nil
}

func (m InventoryManifest) Validate() error {
	if m.Format != InventoryManifestFormat || m.ObjectStoreID == "" || m.Bucket == "" ||
		m.Prefix == "" || !validObjectScopeValue(m.ObjectStoreID) ||
		!validObjectScopeValue(m.Bucket) || !validRelativeObjectPrefix(m.Prefix) {
		return errors.New("inventory manifest is incomplete")
	}
	previous := ""
	for _, entry := range m.Entries {
		if (entry.ArtifactFormat != backupfile.Format && entry.ArtifactFormat != operationaudit.Format) ||
			entry.ObjectKey == "" || !strings.HasPrefix(entry.ObjectKey, m.Prefix) ||
			!validRelativeObjectKey(entry.ObjectKey) || entry.VersionID == "" ||
			!validObjectScopeValue(entry.VersionID) || !validHexSHA256(entry.ArtifactSHA256) ||
			entry.ObjectBytes <= 0 ||
			(entry.RetentionMode != "COMPLIANCE" && entry.RetentionMode != "GOVERNANCE") ||
			entry.RetainUntilUnix <= 0 {
			return errors.New("inventory manifest contains an invalid entry")
		}
		identity := entry.ObjectKey + "\x00" + entry.VersionID
		if previous != "" && identity <= previous {
			return errors.New("inventory entries must be unique and sorted by key/version")
		}
		previous = identity
	}
	return nil
}

func BuildInventoryManifest(
	receiptPaths []string,
	objectStoreID, bucket, prefix, output string,
) (InventoryManifestStatus, error) {
	if objectStoreID == "" || bucket == "" || prefix == "" || output == "" {
		return InventoryManifestStatus{}, errors.New("inventory manifest request is incomplete")
	}
	entries := make([]InventoryEntry, 0, len(receiptPaths))
	for _, path := range receiptPaths {
		if path == "" {
			return InventoryManifestStatus{}, errors.New("inventory receipt path is empty")
		}
		data, err := readBoundedObjectStoreJSONFile(path, "inventory receipt")
		if err != nil {
			return InventoryManifestStatus{}, err
		}
		var header struct {
			Format string `json:"format"`
		}
		if err := json.Unmarshal(data, &header); err != nil {
			return InventoryManifestStatus{}, err
		}
		switch header.Format {
		case ReceiptFormat:
			receipt, err := ReadReceipt(path)
			if err != nil {
				return InventoryManifestStatus{}, err
			}
			if receipt.ObjectStoreID != objectStoreID || receipt.Bucket != bucket {
				return InventoryManifestStatus{}, errors.New("backup receipt object store does not match manifest")
			}
			entries = append(entries, InventoryEntry{
				ArtifactFormat: receipt.ArtifactFormat, ObjectKey: receipt.ObjectKey,
				VersionID: receipt.VersionID, ArtifactSHA256: receipt.ArtifactSHA256,
				ObjectBytes: receipt.ObjectBytes, RetentionMode: receipt.RetentionMode,
				RetainUntilUnix: receipt.RetainUntilUnix,
			})
		case AuditReceiptFormat:
			receipt, err := ReadAuditReceipt(path)
			if err != nil {
				return InventoryManifestStatus{}, err
			}
			if receipt.ObjectStoreID != objectStoreID || receipt.Bucket != bucket {
				return InventoryManifestStatus{}, errors.New("audit receipt object store does not match manifest")
			}
			entries = append(entries, InventoryEntry{
				ArtifactFormat: operationaudit.Format, ObjectKey: receipt.ObjectKey,
				VersionID: receipt.VersionID, ArtifactSHA256: receipt.ArtifactSHA256,
				ObjectBytes: receipt.ObjectBytes, RetentionMode: receipt.RetentionMode,
				RetainUntilUnix: receipt.RetainUntilUnix,
			})
		default:
			return InventoryManifestStatus{}, fmt.Errorf("unsupported inventory receipt format %q", header.Format)
		}
	}
	sort.Slice(entries, func(i, j int) bool {
		if entries[i].ObjectKey != entries[j].ObjectKey {
			return entries[i].ObjectKey < entries[j].ObjectKey
		}
		return entries[i].VersionID < entries[j].VersionID
	})
	manifest := InventoryManifest{
		Format: InventoryManifestFormat, ObjectStoreID: objectStoreID,
		Bucket: bucket, Prefix: prefix, Entries: entries,
	}
	if err := manifest.Validate(); err != nil {
		return InventoryManifestStatus{}, err
	}
	if err := writeJSONAtomic(output, manifest, "inventory manifest", func(path string) (any, error) {
		status, err := InspectInventoryManifest(path)
		return status.Manifest, err
	}); err != nil {
		return InventoryManifestStatus{}, err
	}
	return InspectInventoryManifest(filepath.Clean(output))
}

func ReconcileInventory(
	ctx context.Context,
	client S3API,
	request InventoryRequest,
) (InventoryReceipt, error) {
	if request.Input == "" || request.ObjectStoreID == "" || request.ReceiptOutput == "" {
		return InventoryReceipt{}, errors.New("inventory request is incomplete")
	}
	status, err := InspectInventoryManifest(request.Input)
	if err != nil {
		return InventoryReceipt{}, err
	}
	manifest := status.Manifest
	if manifest.ObjectStoreID != request.ObjectStoreID {
		return InventoryReceipt{}, errors.New("OBJECT_STORE_ID does not match inventory manifest")
	}
	versions, deleteMarkers, err := listAllVersions(ctx, client, manifest.Bucket, manifest.Prefix)
	if err != nil {
		return InventoryReceipt{}, err
	}
	if len(deleteMarkers) != 0 {
		return InventoryReceipt{}, fmt.Errorf("inventory prefix contains %d delete markers", len(deleteMarkers))
	}
	expected := make(map[string]InventoryEntry, len(manifest.Entries))
	for _, entry := range manifest.Entries {
		expected[entry.ObjectKey+"\x00"+entry.VersionID] = entry
	}
	if len(versions) != len(expected) {
		return InventoryReceipt{}, fmt.Errorf(
			"inventory version count mismatch: expected %d, got %d", len(expected), len(versions),
		)
	}
	for identity, version := range versions {
		entry, ok := expected[identity]
		if !ok {
			return InventoryReceipt{}, fmt.Errorf(
				"unexpected object version %s version=%s", aws.ToString(version.Key),
				aws.ToString(version.VersionId),
			)
		}
		if aws.ToInt64(version.Size) != entry.ObjectBytes {
			return InventoryReceipt{}, fmt.Errorf("listed object size differs for %s", entry.ObjectKey)
		}
		if err := verifyInventoryEntry(ctx, client, manifest, entry); err != nil {
			return InventoryReceipt{}, err
		}
	}
	now := request.Now.UTC()
	if now.IsZero() {
		now = time.Now().UTC()
	}
	receipt := InventoryReceipt{
		Format: InventoryReceiptFormat, ObjectStoreID: manifest.ObjectStoreID,
		Bucket: manifest.Bucket, Prefix: manifest.Prefix, ManifestSHA256: status.SHA256,
		ExpectedVersions: len(expected), RemoteVersions: len(versions), DeleteMarkers: 0,
		AllMatched: true, CheckedAtUnix: now.Unix(),
	}
	if existing, err := ReadInventoryReceipt(request.ReceiptOutput); err == nil {
		if existing.Format == receipt.Format && existing.ObjectStoreID == receipt.ObjectStoreID &&
			existing.Bucket == receipt.Bucket && existing.Prefix == receipt.Prefix &&
			existing.ManifestSHA256 == receipt.ManifestSHA256 &&
			existing.ExpectedVersions == receipt.ExpectedVersions &&
			existing.RemoteVersions == receipt.RemoteVersions && existing.DeleteMarkers == 0 &&
			existing.AllMatched {
			return existing, nil
		}
		return InventoryReceipt{}, errors.New("existing inventory receipt does not match reconciliation")
	} else if !errors.Is(err, os.ErrNotExist) {
		return InventoryReceipt{}, err
	}
	if err := WriteInventoryReceiptAtomic(request.ReceiptOutput, receipt); err != nil {
		return InventoryReceipt{}, err
	}
	return receipt, nil
}

func listAllVersions(
	ctx context.Context,
	client S3API,
	bucket, prefix string,
) (map[string]types.ObjectVersion, []types.DeleteMarkerEntry, error) {
	versions := make(map[string]types.ObjectVersion)
	var deleteMarkers []types.DeleteMarkerEntry
	var keyMarker, versionMarker *string
	for {
		output, err := client.ListObjectVersions(ctx, &s3.ListObjectVersionsInput{
			Bucket: aws.String(bucket), Prefix: aws.String(prefix),
			KeyMarker: keyMarker, VersionIdMarker: versionMarker,
		})
		if err != nil {
			return nil, nil, fmt.Errorf("list object versions: %w", err)
		}
		for _, version := range output.Versions {
			key, versionID := aws.ToString(version.Key), aws.ToString(version.VersionId)
			if key == "" || versionID == "" || !strings.HasPrefix(key, prefix) {
				return nil, nil, errors.New("object version listing contains invalid identity")
			}
			identity := key + "\x00" + versionID
			if _, exists := versions[identity]; exists {
				return nil, nil, errors.New("object version listing contains a duplicate")
			}
			versions[identity] = version
		}
		deleteMarkers = append(deleteMarkers, output.DeleteMarkers...)
		if !aws.ToBool(output.IsTruncated) {
			return versions, deleteMarkers, nil
		}
		nextKey, nextVersion := aws.ToString(output.NextKeyMarker), aws.ToString(output.NextVersionIdMarker)
		if nextKey == "" || (keyMarker != nil && nextKey == aws.ToString(keyMarker) &&
			nextVersion == aws.ToString(versionMarker)) {
			return nil, nil, errors.New("truncated object version listing did not advance")
		}
		keyMarker, versionMarker = aws.String(nextKey), aws.String(nextVersion)
	}
}

func verifyInventoryEntry(
	ctx context.Context,
	client S3API,
	manifest InventoryManifest,
	entry InventoryEntry,
) error {
	head, err := client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(manifest.Bucket), Key: aws.String(entry.ObjectKey),
		VersionId: aws.String(entry.VersionID),
	})
	if err != nil {
		return fmt.Errorf("head inventory version %s: %w", entry.ObjectKey, err)
	}
	if aws.ToString(head.VersionId) != entry.VersionID ||
		aws.ToInt64(head.ContentLength) != entry.ObjectBytes ||
		head.Metadata["kubebrain-format"] != entry.ArtifactFormat ||
		head.Metadata["kubebrain-object-store-id"] != manifest.ObjectStoreID ||
		head.Metadata["kubebrain-artifact-sha256"] != entry.ArtifactSHA256 ||
		head.Metadata["kubebrain-object-bytes"] != fmt.Sprintf("%d", entry.ObjectBytes) ||
		head.Metadata["kubebrain-retain-until-unix"] != fmt.Sprintf("%d", entry.RetainUntilUnix) {
		return fmt.Errorf("inventory metadata differs for %s version=%s", entry.ObjectKey, entry.VersionID)
	}
	retention, err := client.GetObjectRetention(ctx, &s3.GetObjectRetentionInput{
		Bucket: aws.String(manifest.Bucket), Key: aws.String(entry.ObjectKey),
		VersionId: aws.String(entry.VersionID),
	})
	if err != nil {
		return fmt.Errorf("read inventory retention %s: %w", entry.ObjectKey, err)
	}
	if retention.Retention == nil || retention.Retention.RetainUntilDate == nil ||
		string(retention.Retention.Mode) != entry.RetentionMode ||
		retention.Retention.RetainUntilDate.Unix() != entry.RetainUntilUnix {
		return fmt.Errorf("inventory retention differs for %s version=%s", entry.ObjectKey, entry.VersionID)
	}
	return nil
}

func (r InventoryReceipt) Validate() error {
	if r.Format != InventoryReceiptFormat || r.ObjectStoreID == "" || r.Bucket == "" ||
		r.Prefix == "" || !validObjectScopeValue(r.ObjectStoreID) ||
		!validObjectScopeValue(r.Bucket) || !validRelativeObjectPrefix(r.Prefix) ||
		!validHexSHA256(r.ManifestSHA256) ||
		r.ExpectedVersions < 0 || r.RemoteVersions != r.ExpectedVersions ||
		r.DeleteMarkers != 0 || !r.AllMatched || r.CheckedAtUnix <= 0 {
		return errors.New("object inventory receipt is incomplete")
	}
	return nil
}

func ReadInventoryReceipt(path string) (InventoryReceipt, error) {
	var receipt InventoryReceipt
	data, err := readBoundedObjectStoreJSONFile(path, "inventory receipt")
	if err != nil {
		return receipt, err
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&receipt); err != nil {
		return receipt, fmt.Errorf("decode inventory receipt: %w", err)
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		return receipt, errors.New("inventory receipt contains trailing JSON")
	}
	canonical, err := json.Marshal(receipt)
	if err != nil {
		return receipt, err
	}
	canonical = append(canonical, '\n')
	if !bytes.Equal(data, canonical) {
		return receipt, errors.New("inventory receipt is not canonical")
	}
	if err := receipt.Validate(); err != nil {
		return receipt, err
	}
	return receipt, nil
}

func WriteInventoryReceiptAtomic(path string, receipt InventoryReceipt) error {
	if err := receipt.Validate(); err != nil {
		return err
	}
	return writeJSONAtomic(path, receipt, "inventory receipt", func(path string) (any, error) {
		return ReadInventoryReceipt(path)
	})
}
