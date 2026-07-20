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
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

const UsageReceiptFormat = "kubebrain.object-usage.receipt.v1"

type UsageRequest struct {
	ObjectStoreID  string
	Bucket         string
	Prefix         string
	AllowedFormats []string
	ReceiptOutput  string
	Now            time.Time
}

type UsageReceipt struct {
	Format           string   `json:"format"`
	ObjectStoreID    string   `json:"object_store_id"`
	Bucket           string   `json:"bucket"`
	Prefix           string   `json:"prefix"`
	AllowedFormats   []string `json:"allowed_formats"`
	RemoteVersions   int      `json:"remote_versions"`
	DeleteMarkers    int      `json:"delete_markers"`
	TotalObjectBytes int64    `json:"total_object_bytes"`
	VersionsSHA256   string   `json:"versions_sha256"`
	CheckedAtUnix    int64    `json:"checked_at_unix"`
}

type usageIdentity struct {
	ObjectKey       string `json:"object_key"`
	VersionID       string `json:"version_id"`
	ArtifactFormat  string `json:"artifact_format"`
	ArtifactSHA256  string `json:"artifact_sha256"`
	ObjectBytes     int64  `json:"object_bytes"`
	RetentionMode   string `json:"retention_mode"`
	RetainUntilUnix int64  `json:"retain_until_unix"`
}

func MeasureUsage(ctx context.Context, client S3API, request UsageRequest) (UsageReceipt, error) {
	request.Prefix = strings.TrimSpace(request.Prefix)
	if request.ObjectStoreID == "" || request.Bucket == "" || request.Prefix == "" ||
		request.ReceiptOutput == "" || len(request.AllowedFormats) == 0 ||
		len(request.AllowedFormats) > 16 {
		return UsageReceipt{}, errors.New("object usage request is incomplete")
	}
	allowed := make(map[string]struct{}, len(request.AllowedFormats))
	previous := ""
	for _, format := range request.AllowedFormats {
		if format == "" || (previous != "" && format <= previous) {
			return UsageReceipt{}, errors.New("allowed formats must be non-empty, unique, and sorted")
		}
		allowed[format] = struct{}{}
		previous = format
	}
	versions, deleteMarkers, err := listAllVersions(ctx, client, request.Bucket, request.Prefix)
	if err != nil {
		return UsageReceipt{}, err
	}
	if len(deleteMarkers) != 0 {
		return UsageReceipt{}, fmt.Errorf("usage prefix contains %d delete markers", len(deleteMarkers))
	}
	identities := make([]usageIdentity, 0, len(versions))
	for _, version := range versions {
		key, versionID := aws.ToString(version.Key), aws.ToString(version.VersionId)
		head, err := client.HeadObject(ctx, &s3.HeadObjectInput{
			Bucket: aws.String(request.Bucket), Key: aws.String(key), VersionId: aws.String(versionID),
		})
		if err != nil {
			return UsageReceipt{}, fmt.Errorf("head usage version %s: %w", key, err)
		}
		format := head.Metadata["kubebrain-format"]
		digest := head.Metadata["kubebrain-artifact-sha256"]
		rawBytes := head.Metadata["kubebrain-object-bytes"]
		rawRetainUntil := head.Metadata["kubebrain-retain-until-unix"]
		objectBytes, bytesErr := strconv.ParseInt(rawBytes, 10, 64)
		retainUntil, retainErr := strconv.ParseInt(rawRetainUntil, 10, 64)
		if _, ok := allowed[format]; !ok || !validHexSHA256(digest) ||
			bytesErr != nil || objectBytes <= 0 || retainErr != nil || retainUntil <= 0 ||
			aws.ToString(head.VersionId) != versionID ||
			aws.ToInt64(head.ContentLength) != objectBytes ||
			aws.ToInt64(version.Size) != objectBytes ||
			head.Metadata["kubebrain-object-store-id"] != request.ObjectStoreID {
			return UsageReceipt{}, fmt.Errorf("usage metadata differs for %s version=%s", key, versionID)
		}
		retention, err := client.GetObjectRetention(ctx, &s3.GetObjectRetentionInput{
			Bucket: aws.String(request.Bucket), Key: aws.String(key), VersionId: aws.String(versionID),
		})
		if err != nil {
			return UsageReceipt{}, fmt.Errorf("read usage retention %s: %w", key, err)
		}
		if retention.Retention == nil || retention.Retention.RetainUntilDate == nil ||
			(string(retention.Retention.Mode) != "COMPLIANCE" &&
				string(retention.Retention.Mode) != "GOVERNANCE") ||
			retention.Retention.RetainUntilDate.Unix() != retainUntil {
			return UsageReceipt{}, fmt.Errorf("usage retention differs for %s version=%s", key, versionID)
		}
		identities = append(identities, usageIdentity{
			ObjectKey: key, VersionID: versionID, ArtifactFormat: format,
			ArtifactSHA256: digest, ObjectBytes: objectBytes,
			RetentionMode: string(retention.Retention.Mode), RetainUntilUnix: retainUntil,
		})
	}
	sort.Slice(identities, func(i, j int) bool {
		if identities[i].ObjectKey == identities[j].ObjectKey {
			return identities[i].VersionID < identities[j].VersionID
		}
		return identities[i].ObjectKey < identities[j].ObjectKey
	})
	var total int64
	for _, identity := range identities {
		if identity.ObjectBytes > int64(^uint64(0)>>1)-total {
			return UsageReceipt{}, errors.New("object usage total bytes overflow int64")
		}
		total += identity.ObjectBytes
	}
	canonical, err := json.Marshal(identities)
	if err != nil {
		return UsageReceipt{}, err
	}
	sum := sha256.Sum256(append(canonical, '\n'))
	now := request.Now.UTC()
	if now.IsZero() {
		now = time.Now().UTC()
	}
	receipt := UsageReceipt{
		Format: UsageReceiptFormat, ObjectStoreID: request.ObjectStoreID,
		Bucket: request.Bucket, Prefix: request.Prefix,
		AllowedFormats: append([]string(nil), request.AllowedFormats...),
		RemoteVersions: len(identities), DeleteMarkers: 0, TotalObjectBytes: total,
		VersionsSHA256: hex.EncodeToString(sum[:]), CheckedAtUnix: now.Unix(),
	}
	if existing, err := ReadUsageReceipt(request.ReceiptOutput); err == nil {
		if existing.Format == receipt.Format && existing.ObjectStoreID == receipt.ObjectStoreID &&
			existing.Bucket == receipt.Bucket && existing.Prefix == receipt.Prefix &&
			equalStrings(existing.AllowedFormats, receipt.AllowedFormats) &&
			existing.RemoteVersions == receipt.RemoteVersions && existing.DeleteMarkers == 0 &&
			existing.TotalObjectBytes == receipt.TotalObjectBytes &&
			existing.VersionsSHA256 == receipt.VersionsSHA256 {
			return existing, nil
		}
		return UsageReceipt{}, errors.New("existing object usage receipt does not match remote usage")
	} else if !errors.Is(err, os.ErrNotExist) {
		return UsageReceipt{}, err
	}
	if err := WriteUsageReceiptAtomic(request.ReceiptOutput, receipt); err != nil {
		return UsageReceipt{}, err
	}
	return receipt, nil
}

func equalStrings(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	for i := range left {
		if left[i] != right[i] {
			return false
		}
	}
	return true
}

func (r UsageReceipt) Validate() error {
	if r.Format != UsageReceiptFormat || r.ObjectStoreID == "" || r.Bucket == "" ||
		r.Prefix == "" || len(r.AllowedFormats) == 0 || len(r.AllowedFormats) > 16 ||
		r.RemoteVersions < 0 || r.DeleteMarkers != 0 || r.TotalObjectBytes < 0 ||
		!validHexSHA256(r.VersionsSHA256) || r.CheckedAtUnix <= 0 {
		return errors.New("object usage receipt is incomplete")
	}
	previous := ""
	for _, format := range r.AllowedFormats {
		if format == "" || (previous != "" && format <= previous) {
			return errors.New("object usage receipt formats are invalid")
		}
		previous = format
	}
	return nil
}

func ReadUsageReceipt(path string) (UsageReceipt, error) {
	var receipt UsageReceipt
	data, err := os.ReadFile(path)
	if err != nil {
		return receipt, err
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&receipt); err != nil {
		return receipt, fmt.Errorf("decode object usage receipt: %w", err)
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		return receipt, errors.New("object usage receipt contains trailing JSON")
	}
	canonical, err := json.Marshal(receipt)
	if err != nil {
		return receipt, err
	}
	canonical = append(canonical, '\n')
	if !bytes.Equal(data, canonical) {
		return receipt, errors.New("object usage receipt is not canonical")
	}
	if err := receipt.Validate(); err != nil {
		return receipt, err
	}
	return receipt, nil
}

func WriteUsageReceiptAtomic(path string, receipt UsageReceipt) error {
	if err := receipt.Validate(); err != nil {
		return err
	}
	return writeJSONAtomic(path, receipt, func(path string) (any, error) {
		return ReadUsageReceipt(path)
	})
}
