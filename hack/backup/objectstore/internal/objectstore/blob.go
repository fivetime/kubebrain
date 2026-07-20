package objectstore

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

const maxImmutableBlobBytes = 16 << 20

type BlobRequest struct {
	Input           string
	ArtifactFormat  string
	ArtifactID      string
	Instance        string
	ObjectStoreID   string
	Bucket          string
	ObjectKey       string
	RetentionMode   string
	RetainUntilUnix int64
	ReceiptOutput   string
	Now             time.Time
}

type BlobReadRequest struct {
	Output             string
	ArtifactFormat     string
	ArtifactID         string
	Instance           string
	ObjectStoreID      string
	Bucket             string
	ObjectKey          string
	MinRetainUntilUnix int64
}

func ArchiveBlob(ctx context.Context, client S3API, request BlobRequest) (BlobReceipt, error) {
	if request.Input == "" || request.ArtifactFormat == "" || request.ArtifactID == "" ||
		request.Instance == "" || request.ObjectStoreID == "" || request.Bucket == "" ||
		request.ObjectKey == "" || request.ReceiptOutput == "" || request.RetainUntilUnix <= 0 {
		return BlobReceipt{}, errors.New("immutable blob archive request is incomplete")
	}
	mode, err := objectLockMode(request.RetentionMode)
	if err != nil {
		return BlobReceipt{}, err
	}
	body, err := readBoundedBlob(request.Input)
	if err != nil {
		return BlobReceipt{}, err
	}
	sum := sha256.Sum256(body)
	digest := fmt.Sprintf("%x", sum[:])
	now := request.Now.UTC()
	if now.IsZero() {
		now = time.Now().UTC()
	}
	retainUntil := time.Unix(request.RetainUntilUnix, 0).UTC()
	if !retainUntil.After(now) {
		return BlobReceipt{}, errors.New("retain-until timestamp must be in the future")
	}
	metadata := blobMetadata(request, digest, int64(len(body)), retainUntil)

	if _, err := os.Stat(request.ReceiptOutput); err == nil {
		existing, readErr := ReadBlobReceipt(request.ReceiptOutput)
		if readErr != nil {
			return BlobReceipt{}, readErr
		}
		if !blobReceiptMatches(existing, request, digest, int64(len(body))) {
			return BlobReceipt{}, errors.New("existing immutable blob receipt does not match the archive request")
		}
		if err := verifyBlobRemote(ctx, client, request, existing.VersionID, body); err != nil {
			return BlobReceipt{}, err
		}
		if err := validateBlobRetention(ctx, client, request, existing.VersionID, retainUntil); err != nil {
			return BlobReceipt{}, err
		}
		return existing, nil
	} else if !errors.Is(err, os.ErrNotExist) {
		return BlobReceipt{}, err
	}

	output, putErr := client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(request.Bucket), Key: aws.String(request.ObjectKey),
		Body: bytes.NewReader(body), ContentLength: aws.Int64(int64(len(body))),
		ChecksumAlgorithm: types.ChecksumAlgorithmSha256,
		ChecksumSHA256:    aws.String(base64.StdEncoding.EncodeToString(sum[:])),
		IfNoneMatch:       aws.String("*"), Metadata: metadata, ObjectLockMode: mode,
		ObjectLockRetainUntilDate: aws.Time(retainUntil),
	})
	var versionID string
	verificationCtx := ctx
	if putErr == nil {
		versionID = aws.ToString(output.VersionId)
	} else {
		headCtx := ctx
		if !isPreconditionFailure(putErr) {
			reconcileCtx, cancel := objectWriteReconciliationContext(ctx)
			defer cancel()
			verificationCtx = reconcileCtx
			headCtx = reconcileCtx
		}
		head, headErr := client.HeadObject(headCtx, &s3.HeadObjectInput{
			Bucket: aws.String(request.Bucket), Key: aws.String(request.ObjectKey),
		})
		if headErr != nil {
			if isPreconditionFailure(putErr) {
				return BlobReceipt{}, fmt.Errorf(
					"conditional immutable blob upload conflicted and existing object cannot be read: %w", headErr,
				)
			}
			return BlobReceipt{}, errors.Join(
				fmt.Errorf("conditional immutable blob upload: %w", putErr),
				fmt.Errorf("inspect immutable blob after failed conditional upload: %w", headErr),
			)
		}
		if err := validateHead(head, metadata, int64(len(body))); err != nil {
			conflictErr := fmt.Errorf("refusing to replace conflicting immutable blob: %w", err)
			if isPreconditionFailure(putErr) {
				return BlobReceipt{}, conflictErr
			}
			return BlobReceipt{}, errors.Join(
				fmt.Errorf("conditional immutable blob upload: %w", putErr), conflictErr,
			)
		}
		versionID = aws.ToString(head.VersionId)
	}
	if versionID == "" {
		return BlobReceipt{}, errors.New("object store did not return a version ID; Object Lock/versioning is required")
	}
	archivedAtUnix, err := exactVersionModifiedAt(
		verificationCtx, client, request.Bucket, request.ObjectKey, versionID,
		metadata, int64(len(body)), retainUntil,
	)
	if err != nil {
		return BlobReceipt{}, err
	}
	if err := verifyBlobRemote(verificationCtx, client, request, versionID, body); err != nil {
		return BlobReceipt{}, err
	}
	if err := validateBlobRetention(verificationCtx, client, request, versionID, retainUntil); err != nil {
		return BlobReceipt{}, err
	}
	receipt := BlobReceipt{
		Format: BlobReceiptFormat, ArtifactFormat: request.ArtifactFormat,
		ArtifactID: request.ArtifactID, Instance: request.Instance,
		ObjectStoreID: request.ObjectStoreID, Bucket: request.Bucket, ObjectKey: request.ObjectKey,
		VersionID: versionID, ArtifactSHA256: digest, ObjectBytes: int64(len(body)),
		RetentionMode: request.RetentionMode, RetainUntilUnix: request.RetainUntilUnix,
		RemoteVerified: true, ArchivedAtUnix: archivedAtUnix,
	}
	if err := WriteBlobReceiptAtomic(request.ReceiptOutput, receipt); err != nil {
		return BlobReceipt{}, err
	}
	return receipt, nil
}

func ReadBlob(ctx context.Context, client S3API, request BlobReadRequest) (BlobReadReceipt, error) {
	if request.Output == "" || request.ArtifactFormat == "" || request.ArtifactID == "" ||
		request.Instance == "" || request.ObjectStoreID == "" || request.Bucket == "" ||
		request.ObjectKey == "" || request.MinRetainUntilUnix <= 0 {
		return BlobReadReceipt{}, errors.New("immutable blob read request is incomplete")
	}
	versions, deleteMarkers, err := listAllVersions(ctx, client, request.Bucket, request.ObjectKey)
	if err != nil {
		return BlobReadReceipt{}, err
	}
	var version types.ObjectVersion
	matches := 0
	for _, candidate := range versions {
		if aws.ToString(candidate.Key) == request.ObjectKey {
			version = candidate
			matches++
		}
	}
	for _, marker := range deleteMarkers {
		if aws.ToString(marker.Key) == request.ObjectKey {
			return BlobReadReceipt{}, errors.New("immutable blob has a delete marker")
		}
	}
	if matches != 1 {
		return BlobReadReceipt{}, fmt.Errorf("immutable blob must have exactly one version, got %d", matches)
	}
	versionID := aws.ToString(version.VersionId)
	head, err := client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(request.Bucket), Key: aws.String(request.ObjectKey),
		VersionId: aws.String(versionID),
	})
	if err != nil {
		return BlobReadReceipt{}, fmt.Errorf("read immutable blob metadata: %w", err)
	}
	size := aws.ToInt64(head.ContentLength)
	digest := head.Metadata["kubebrain-artifact-sha256"]
	retainUntilUnix, err := strconv.ParseInt(head.Metadata["kubebrain-retain-until-unix"], 10, 64)
	if err != nil {
		return BlobReadReceipt{}, errors.New("immutable blob retain-until metadata is invalid")
	}
	if aws.ToString(head.VersionId) != versionID || size <= 0 || size > maxImmutableBlobBytes ||
		aws.ToInt64(version.Size) != size || !validHexSHA256(digest) ||
		head.Metadata["kubebrain-format"] != request.ArtifactFormat ||
		head.Metadata["kubebrain-artifact-id"] != request.ArtifactID ||
		head.Metadata["kubebrain-instance"] != request.Instance ||
		head.Metadata["kubebrain-object-store-id"] != request.ObjectStoreID ||
		head.Metadata["kubebrain-object-bytes"] != strconv.FormatInt(size, 10) ||
		retainUntilUnix < request.MinRetainUntilUnix {
		return BlobReadReceipt{}, errors.New("immutable blob metadata does not match the read request")
	}
	retention, err := client.GetObjectRetention(ctx, &s3.GetObjectRetentionInput{
		Bucket: aws.String(request.Bucket), Key: aws.String(request.ObjectKey),
		VersionId: aws.String(versionID),
	})
	if err != nil {
		return BlobReadReceipt{}, fmt.Errorf("read immutable blob retention: %w", err)
	}
	if retention.Retention == nil || retention.Retention.RetainUntilDate == nil ||
		(retention.Retention.Mode != types.ObjectLockRetentionModeCompliance &&
			retention.Retention.Mode != types.ObjectLockRetentionModeGovernance) ||
		retention.Retention.RetainUntilDate.Unix() != retainUntilUnix {
		return BlobReadReceipt{}, errors.New("immutable blob retention does not match metadata")
	}
	output, err := client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(request.Bucket), Key: aws.String(request.ObjectKey),
		VersionId: aws.String(versionID),
	})
	if err != nil {
		return BlobReadReceipt{}, fmt.Errorf("download immutable blob version: %w", err)
	}
	defer output.Body.Close()
	if aws.ToString(output.VersionId) != versionID {
		return BlobReadReceipt{}, errors.New("downloaded immutable blob returned a different version ID")
	}
	body, err := io.ReadAll(io.LimitReader(output.Body, maxImmutableBlobBytes+1))
	if err != nil {
		return BlobReadReceipt{}, err
	}
	sum := sha256.Sum256(body)
	if int64(len(body)) != size || fmt.Sprintf("%x", sum[:]) != digest {
		return BlobReadReceipt{}, errors.New("downloaded immutable blob differs from protected metadata")
	}
	if err := writeBlobOutputAtomic(request.Output, body); err != nil {
		return BlobReadReceipt{}, err
	}
	receipt := BlobReadReceipt{
		Format: BlobReadReceiptFormat, ArtifactFormat: request.ArtifactFormat,
		ArtifactID: request.ArtifactID, Instance: request.Instance,
		ObjectStoreID: request.ObjectStoreID, Bucket: request.Bucket, ObjectKey: request.ObjectKey,
		VersionID: versionID, ArtifactSHA256: digest, ObjectBytes: size,
		RetentionMode: string(retention.Retention.Mode), RetainUntilUnix: retainUntilUnix,
		RemoteVerified: true,
	}
	if err := receipt.Validate(); err != nil {
		return BlobReadReceipt{}, err
	}
	return receipt, nil
}

func writeBlobOutputAtomic(path string, body []byte) error {
	if existing, err := os.ReadFile(path); err == nil {
		if bytes.Equal(existing, body) {
			return nil
		}
		return fmt.Errorf("refusing to overwrite immutable blob output %q", path)
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	dir := filepath.Dir(path)
	temp, err := os.CreateTemp(dir, "."+filepath.Base(path)+".tmp-*")
	if err != nil {
		return err
	}
	tempName := temp.Name()
	defer os.Remove(tempName)
	if err := temp.Chmod(0o600); err != nil {
		temp.Close()
		return err
	}
	if _, err := temp.Write(body); err != nil {
		temp.Close()
		return err
	}
	if err := temp.Sync(); err != nil {
		temp.Close()
		return err
	}
	if err := temp.Close(); err != nil {
		return err
	}
	if err := os.Link(tempName, path); err != nil {
		return err
	}
	directory, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer directory.Close()
	return directory.Sync()
}

func readBoundedBlob(path string) ([]byte, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	body, err := io.ReadAll(io.LimitReader(file, maxImmutableBlobBytes+1))
	if err != nil {
		return nil, err
	}
	if len(body) == 0 {
		return nil, errors.New("immutable blob is empty")
	}
	if len(body) > maxImmutableBlobBytes {
		return nil, fmt.Errorf("immutable blob exceeds %d bytes", maxImmutableBlobBytes)
	}
	return body, nil
}

func blobMetadata(
	request BlobRequest,
	digest string,
	size int64,
	retainUntil time.Time,
) map[string]string {
	return map[string]string{
		"kubebrain-format":            request.ArtifactFormat,
		"kubebrain-artifact-id":       request.ArtifactID,
		"kubebrain-instance":          request.Instance,
		"kubebrain-object-store-id":   request.ObjectStoreID,
		"kubebrain-artifact-sha256":   digest,
		"kubebrain-object-bytes":      strconv.FormatInt(size, 10),
		"kubebrain-retain-until-unix": strconv.FormatInt(retainUntil.Unix(), 10),
	}
}

func blobReceiptMatches(receipt BlobReceipt, request BlobRequest, digest string, size int64) bool {
	return receipt.ArtifactFormat == request.ArtifactFormat &&
		receipt.ArtifactID == request.ArtifactID && receipt.Instance == request.Instance &&
		receipt.ObjectStoreID == request.ObjectStoreID && receipt.Bucket == request.Bucket &&
		receipt.ObjectKey == request.ObjectKey && receipt.ArtifactSHA256 == digest &&
		receipt.ObjectBytes == size && receipt.RetentionMode == request.RetentionMode &&
		receipt.RetainUntilUnix == request.RetainUntilUnix
}

func verifyBlobRemote(
	ctx context.Context,
	client S3API,
	request BlobRequest,
	versionID string,
	expected []byte,
) error {
	output, err := client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(request.Bucket), Key: aws.String(request.ObjectKey),
		VersionId: aws.String(versionID),
	})
	if err != nil {
		return fmt.Errorf("download immutable blob: %w", err)
	}
	defer output.Body.Close()
	body, err := io.ReadAll(io.LimitReader(output.Body, int64(len(expected))+1))
	if err != nil {
		return err
	}
	if !bytes.Equal(body, expected) {
		return errors.New("remote immutable blob bytes differ")
	}
	return nil
}

func validateBlobRetention(
	ctx context.Context,
	client S3API,
	request BlobRequest,
	versionID string,
	retainUntil time.Time,
) error {
	retention, err := client.GetObjectRetention(ctx, &s3.GetObjectRetentionInput{
		Bucket: aws.String(request.Bucket), Key: aws.String(request.ObjectKey),
		VersionId: aws.String(versionID),
	})
	if err != nil {
		return fmt.Errorf("read remote immutable blob retention: %w", err)
	}
	if retention.Retention == nil || string(retention.Retention.Mode) != request.RetentionMode ||
		retention.Retention.RetainUntilDate == nil ||
		retention.Retention.RetainUntilDate.Unix() != retainUntil.Unix() {
		return errors.New("remote immutable blob retention does not match the requested policy")
	}
	return nil
}
