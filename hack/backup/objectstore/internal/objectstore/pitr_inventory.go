package objectstore

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"reflect"
	"sort"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/kubewharf/kubebrain/hack/backup/internal/pitrinventory"
)

type PITRInventoryRequest struct {
	ObjectStoreID      string
	Bucket             string
	Prefix             string
	MinRetainUntilUnix int64
	ReceiptOutput      string
	Now                time.Time
}

func CapturePITRInventory(ctx context.Context, client S3API, request PITRInventoryRequest) (pitrinventory.Receipt, error) {
	if !validObjectScopeValue(request.ObjectStoreID) || !validObjectScopeValue(request.Bucket) || !validRelativeObjectPrefix(request.Prefix) || request.MinRetainUntilUnix <= 0 || request.ReceiptOutput == "" {
		return pitrinventory.Receipt{}, errors.New("native PITR inventory request is incomplete")
	}
	versions, markers, pages, err := listAllVersionsWithPages(ctx, client, request.Bucket, request.Prefix)
	if err != nil {
		return pitrinventory.Receipt{}, err
	}
	if len(markers) != 0 {
		return pitrinventory.Receipt{}, fmt.Errorf("native PITR prefix contains %d delete markers", len(markers))
	}
	now := request.Now.UTC()
	if now.IsZero() {
		now = time.Now().UTC()
	}
	entries := make([]pitrinventory.Entry, 0, len(versions))
	seenKeys := make(map[string]bool)
	base := strings.TrimSuffix(request.Prefix, "/") + "/"
	for _, version := range versions {
		key, versionID := aws.ToString(version.Key), aws.ToString(version.VersionId)
		if !strings.HasPrefix(key, base) || key == base || seenKeys[key] || !aws.ToBool(version.IsLatest) {
			return pitrinventory.Receipt{}, errors.New("native PITR prefix is not an immutable one-version-per-key snapshot")
		}
		seenKeys[key] = true
		entry, err := verifyPITRVersion(ctx, client, request, key, versionID, aws.ToInt64(version.Size), now)
		if err != nil {
			return pitrinventory.Receipt{}, err
		}
		entry.Name = strings.TrimPrefix(key, base)
		entries = append(entries, entry)
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].Name < entries[j].Name })
	var total uint64
	for _, entry := range entries {
		total += uint64(entry.Bytes)
	}
	receipt := pitrinventory.Receipt{Format: pitrinventory.Format, ObjectStoreID: request.ObjectStoreID, Bucket: request.Bucket, Prefix: strings.TrimSuffix(request.Prefix, "/"), Entries: entries, ObjectCount: len(entries), TotalBytes: total, Pages: pages, PaginationExhausted: true, ExactVersionsVerified: true, MinRetainUntilUnix: request.MinRetainUntilUnix, CheckedAtUnix: now.Unix()}
	if err := receipt.Validate(); err != nil {
		return pitrinventory.Receipt{}, err
	}
	if existing, _, err := pitrinventory.ReadCanonical(request.ReceiptOutput); err == nil {
		if existing.CheckedAtUnix <= receipt.CheckedAtUnix {
			existingComparable, receiptComparable := existing, receipt
			existingComparable.CheckedAtUnix, receiptComparable.CheckedAtUnix = 0, 0
			if reflect.DeepEqual(existingComparable, receiptComparable) {
				return existing, nil
			}
		}
		return pitrinventory.Receipt{}, errors.New("existing native PITR inventory receipt does not match reconciliation")
	} else if !errors.Is(err, os.ErrNotExist) {
		return pitrinventory.Receipt{}, err
	}
	if err := writeJSONAtomicLimit(request.ReceiptOutput, receipt, "native PITR object inventory", pitrinventory.MaxBytes, func(path string) (any, error) {
		got, _, err := pitrinventory.ReadCanonical(path)
		return got, err
	}); err != nil {
		return pitrinventory.Receipt{}, err
	}
	return receipt, nil
}

func verifyPITRVersion(ctx context.Context, client S3API, request PITRInventoryRequest, key, versionID string, listedBytes int64, now time.Time) (entryResult pitrinventory.Entry, retErr error) {
	if listedBytes <= 0 || listedBytes == math.MaxInt64 || !validObjectScopeValue(versionID) {
		return pitrinventory.Entry{}, errors.New("native PITR object listing has invalid size or version")
	}
	head, err := client.HeadObject(ctx, &s3.HeadObjectInput{Bucket: aws.String(request.Bucket), Key: aws.String(key), VersionId: aws.String(versionID)})
	if err != nil {
		return pitrinventory.Entry{}, err
	}
	if aws.ToString(head.VersionId) != versionID || aws.ToInt64(head.ContentLength) != listedBytes {
		return pitrinventory.Entry{}, fmt.Errorf("native PITR exact-version HEAD differs for %s", key)
	}
	retention, err := client.GetObjectRetention(ctx, &s3.GetObjectRetentionInput{Bucket: aws.String(request.Bucket), Key: aws.String(key), VersionId: aws.String(versionID)})
	if err != nil {
		return pitrinventory.Entry{}, err
	}
	if retention.Retention == nil || retention.Retention.RetainUntilDate == nil {
		return pitrinventory.Entry{}, fmt.Errorf("native PITR exact-version retention is missing for %s", key)
	}
	mode, retainUntil := string(retention.Retention.Mode), retention.Retention.RetainUntilDate.Unix()
	if (mode != "COMPLIANCE" && mode != "GOVERNANCE") || retainUntil < request.MinRetainUntilUnix || retainUntil <= now.Unix() {
		return pitrinventory.Entry{}, fmt.Errorf("native PITR exact-version retention is insufficient for %s", key)
	}
	object, err := client.GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String(request.Bucket), Key: aws.String(key), VersionId: aws.String(versionID)})
	if err != nil {
		return pitrinventory.Entry{}, err
	}
	defer func() { retErr = errors.Join(retErr, object.Body.Close()) }()
	if aws.ToString(object.VersionId) != versionID {
		return pitrinventory.Entry{}, fmt.Errorf("native PITR GET returned a different version for %s", key)
	}
	hash := sha256.New()
	n, err := io.Copy(hash, io.LimitReader(object.Body, listedBytes+1))
	if err != nil {
		return pitrinventory.Entry{}, err
	}
	if n != listedBytes {
		return pitrinventory.Entry{}, fmt.Errorf("native PITR exact-version body size differs for %s", key)
	}
	return pitrinventory.Entry{ObjectKey: key, VersionID: versionID, Bytes: listedBytes, SHA256: hex.EncodeToString(hash.Sum(nil)), RetentionMode: mode, RetainUntilUnix: retainUntil}, nil
}

func listAllVersionsWithPages(ctx context.Context, client S3API, bucket, prefix string) ([]types.ObjectVersion, []types.DeleteMarkerEntry, int, error) {
	var versions []types.ObjectVersion
	var markers []types.DeleteMarkerEntry
	var keyMarker, versionMarker *string
	pages := 0
	for {
		output, err := client.ListObjectVersions(ctx, &s3.ListObjectVersionsInput{Bucket: aws.String(bucket), Prefix: aws.String(prefix), KeyMarker: keyMarker, VersionIdMarker: versionMarker})
		if err != nil {
			return nil, nil, pages, fmt.Errorf("list native PITR object versions: %w", err)
		}
		pages++
		versions = append(versions, output.Versions...)
		markers = append(markers, output.DeleteMarkers...)
		if !aws.ToBool(output.IsTruncated) {
			return versions, markers, pages, nil
		}
		nextKey, nextVersion := aws.ToString(output.NextKeyMarker), aws.ToString(output.NextVersionIdMarker)
		if nextKey == "" || (keyMarker != nil && nextKey == aws.ToString(keyMarker) && nextVersion == aws.ToString(versionMarker)) {
			return nil, nil, pages, errors.New("truncated native PITR object version listing did not advance")
		}
		keyMarker, versionMarker = aws.String(nextKey), aws.String(nextVersion)
	}
}
