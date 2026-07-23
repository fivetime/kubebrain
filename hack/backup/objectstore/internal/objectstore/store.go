package objectstore

import (
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
	"github.com/aws/smithy-go"
	"github.com/kubewharf/kubebrain/hack/backup/internal/backupfile"
)

type S3API interface {
	PutObject(context.Context, *s3.PutObjectInput, ...func(*s3.Options)) (*s3.PutObjectOutput, error)
	HeadObject(context.Context, *s3.HeadObjectInput, ...func(*s3.Options)) (*s3.HeadObjectOutput, error)
	GetObject(context.Context, *s3.GetObjectInput, ...func(*s3.Options)) (*s3.GetObjectOutput, error)
	GetObjectRetention(context.Context, *s3.GetObjectRetentionInput, ...func(*s3.Options)) (*s3.GetObjectRetentionOutput, error)
	DeleteObject(context.Context, *s3.DeleteObjectInput, ...func(*s3.Options)) (*s3.DeleteObjectOutput, error)
	ListObjectVersions(context.Context, *s3.ListObjectVersionsInput, ...func(*s3.Options)) (*s3.ListObjectVersionsOutput, error)
}

type UploadRequest struct {
	Input           string
	Instance        string
	BackupID        string
	ObjectStoreID   string
	Bucket          string
	ObjectKey       string
	RetentionMode   string
	RetainUntilUnix int64
	ExpectedPrefix  string
	MinRecords      int
	MaxAgeSeconds   int64
	ReceiptOutput   string
	Now             time.Time
}

func Upload(ctx context.Context, client S3API, request UploadRequest) (Receipt, error) {
	if request.Input == "" || request.Instance == "" || request.BackupID == "" || request.ObjectStoreID == "" ||
		request.Bucket == "" || request.ObjectKey == "" || request.ReceiptOutput == "" ||
		request.RetainUntilUnix <= 0 || request.ExpectedPrefix == "" ||
		request.MinRecords < 0 || request.MaxAgeSeconds <= 0 {
		return Receipt{}, errors.New("upload request is incomplete")
	}
	mode, err := objectLockMode(request.RetentionMode)
	if err != nil {
		return Receipt{}, err
	}
	artifact, err := backupfile.OpenVerified(request.Input)
	if err != nil {
		return Receipt{}, fmt.Errorf("validate local artifact: %w", err)
	}
	defer artifact.Close()
	status := artifact.Status()
	frozenInput := artifact.Path()
	info, err := os.Stat(frozenInput)
	if err != nil {
		return Receipt{}, err
	}
	now := request.Now.UTC()
	if now.IsZero() {
		now = time.Now().UTC()
	}
	if err := validateProductionArtifact(status, request, now); err != nil {
		return Receipt{}, err
	}
	retainUntil := time.Unix(request.RetainUntilUnix, 0).UTC()
	if !retainUntil.After(now) {
		return Receipt{}, errors.New("retain-until timestamp must be in the future")
	}
	file, err := os.Open(frozenInput)
	if err != nil {
		return Receipt{}, err
	}
	defer file.Close()
	checksum, artifactFileSHA256, err := fileSHA256(frozenInput)
	if err != nil {
		return Receipt{}, err
	}
	metadata := artifactMetadata(request, status, artifactFileSHA256, info.Size(), retainUntil)
	if _, err := os.Stat(request.ReceiptOutput); err == nil {
		existing, readErr := ReadReceipt(request.ReceiptOutput)
		if readErr != nil {
			return Receipt{}, readErr
		}
		if err := validateReceiptRequest(existing, request, status, artifactFileSHA256, info.Size()); err != nil {
			return Receipt{}, err
		}
		if _, err := verifyRemote(
			ctx, client, request.Bucket, request.ObjectKey, existing.VersionID, status, artifactFileSHA256, info.Size(),
		); err != nil {
			return Receipt{}, err
		}
		if err := validateRemoteRetention(ctx, client, existing.VersionID, request, retainUntil); err != nil {
			return Receipt{}, err
		}
		return existing, nil
	} else if !errors.Is(err, os.ErrNotExist) {
		return Receipt{}, err
	}
	output, putErr := client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:                    aws.String(request.Bucket),
		Key:                       aws.String(request.ObjectKey),
		Body:                      file,
		ContentLength:             aws.Int64(info.Size()),
		ChecksumAlgorithm:         types.ChecksumAlgorithmSha256,
		ChecksumSHA256:            aws.String(checksum),
		IfNoneMatch:               aws.String("*"),
		Metadata:                  metadata,
		ObjectLockMode:            mode,
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
				return Receipt{}, fmt.Errorf(
					"conditional upload conflicted and existing object cannot be read: %w", headErr,
				)
			}
			return Receipt{}, errors.Join(
				fmt.Errorf("conditional object upload: %w", putErr),
				fmt.Errorf("inspect object after failed conditional upload: %w", headErr),
			)
		}
		if err := validateHead(head, metadata, info.Size()); err != nil {
			conflictErr := fmt.Errorf("refusing to replace conflicting object: %w", err)
			if isPreconditionFailure(putErr) {
				return Receipt{}, conflictErr
			}
			return Receipt{}, errors.Join(fmt.Errorf("conditional object upload: %w", putErr), conflictErr)
		}
		versionID = aws.ToString(head.VersionId)
	}
	if versionID == "" {
		return Receipt{}, errors.New("object store did not return a version ID; Object Lock/versioning is required")
	}
	uploadedAtUnix, err := exactVersionModifiedAt(
		verificationCtx, client, request.Bucket, request.ObjectKey, versionID,
		metadata, info.Size(), retainUntil,
	)
	if err != nil {
		return Receipt{}, err
	}

	verified, err := verifyRemote(
		verificationCtx, client, request.Bucket, request.ObjectKey, versionID, status, artifactFileSHA256, info.Size(),
	)
	if err != nil {
		return Receipt{}, err
	}
	if err := validateRemoteRetention(verificationCtx, client, versionID, request, retainUntil); err != nil {
		return Receipt{}, err
	}
	receipt := Receipt{
		Format: ReceiptFormat, Instance: request.Instance, BackupID: request.BackupID,
		ObjectStoreID: request.ObjectStoreID,
		Bucket:        request.Bucket, ObjectKey: request.ObjectKey, VersionID: versionID,
		ArtifactFileSHA256: artifactFileSHA256, ArtifactFormat: status.Format, ArtifactSHA256: status.SHA256,
		SnapshotRevision: status.Revision, CreatedAtUnix: status.CreatedAtUnix,
		Records: status.Records, Leases: status.Leases, ObjectBytes: info.Size(),
		RetentionMode: request.RetentionMode, RetainUntilUnix: retainUntil.Unix(),
		RemoteVerified: verified, UploadedAtUnix: uploadedAtUnix,
	}
	if err := WriteReceiptAtomic(request.ReceiptOutput, receipt); err != nil {
		return Receipt{}, err
	}
	return receipt, nil
}

func validateProductionArtifact(status backupfile.Status, request UploadRequest, now time.Time) error {
	if status.Format != backupfile.Format {
		return fmt.Errorf("production object backup requires %s, got %s", backupfile.Format, status.Format)
	}
	if status.Prefix != request.ExpectedPrefix {
		return fmt.Errorf("expected backup prefix %q, got %q", request.ExpectedPrefix, status.Prefix)
	}
	if status.Records < request.MinRecords {
		return fmt.Errorf("expected at least %d records, got %d", request.MinRecords, status.Records)
	}
	if status.CreatedAtUnix <= 0 {
		return errors.New("backup does not contain a protected creation timestamp")
	}
	age := now.Unix() - status.CreatedAtUnix
	if age < -300 {
		return fmt.Errorf("backup creation timestamp is %d seconds in the future", -age)
	}
	if age > request.MaxAgeSeconds {
		return fmt.Errorf("backup is %d seconds old, maximum is %d", age, request.MaxAgeSeconds)
	}
	return nil
}

func validateReceiptRequest(
	receipt Receipt,
	request UploadRequest,
	status backupfile.Status,
	artifactFileSHA256 string,
	size int64,
) error {
	if receipt.Instance != request.Instance || receipt.BackupID != request.BackupID ||
		receipt.ObjectStoreID != request.ObjectStoreID ||
		receipt.Bucket != request.Bucket || receipt.ObjectKey != request.ObjectKey ||
		(receipt.ArtifactFileSHA256 != "" && receipt.ArtifactFileSHA256 != artifactFileSHA256) ||
		receipt.ArtifactFormat != status.Format || receipt.ArtifactSHA256 != status.SHA256 ||
		receipt.SnapshotRevision != status.Revision || receipt.CreatedAtUnix != status.CreatedAtUnix ||
		receipt.Records != status.Records || receipt.Leases != status.Leases ||
		receipt.ObjectBytes != size || receipt.RetentionMode != request.RetentionMode ||
		receipt.RetainUntilUnix != request.RetainUntilUnix {
		return errors.New("existing receipt does not match the upload request")
	}
	return nil
}

func validateRemoteRetention(
	ctx context.Context,
	client S3API,
	versionID string,
	request UploadRequest,
	retainUntil time.Time,
) error {
	retention, err := client.GetObjectRetention(ctx, &s3.GetObjectRetentionInput{
		Bucket: aws.String(request.Bucket), Key: aws.String(request.ObjectKey), VersionId: aws.String(versionID),
	})
	if err != nil {
		return fmt.Errorf("read remote retention: %w", err)
	}
	if retention.Retention == nil || string(retention.Retention.Mode) != request.RetentionMode ||
		retention.Retention.RetainUntilDate == nil ||
		retention.Retention.RetainUntilDate.Unix() != retainUntil.Unix() {
		return errors.New("remote object retention does not match the requested policy")
	}
	return nil
}

type DeleteRequest struct {
	Receipt       Receipt
	Confirmation  string
	ObjectStoreID string
	ReceiptOutput string
	Now           time.Time
}

const deletionReconciliationTimeout = 5 * time.Second

func Delete(ctx context.Context, client S3API, request DeleteRequest) (DeletionReceipt, error) {
	receipt := request.Receipt
	if err := receipt.Validate(); err != nil {
		return DeletionReceipt{}, err
	}
	expected := "delete:" + receipt.Instance + ":" + receipt.BackupID
	if request.Confirmation != expected {
		return DeletionReceipt{}, fmt.Errorf("delete confirmation must exactly equal %s", expected)
	}
	if request.ObjectStoreID == "" || request.ObjectStoreID != receipt.ObjectStoreID {
		return DeletionReceipt{}, errors.New("OBJECT_STORE_ID does not match the upload receipt")
	}
	if request.ReceiptOutput == "" {
		return DeletionReceipt{}, errors.New("deletion receipt output is required")
	}
	now := request.Now
	if now.IsZero() {
		now = time.Now().UTC()
	}
	head, headErr := client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(receipt.Bucket), Key: aws.String(receipt.ObjectKey), VersionId: aws.String(receipt.VersionID),
	})
	if headErr != nil {
		if !isNotFound(headErr) {
			return DeletionReceipt{}, fmt.Errorf("read retained object version: %w", headErr)
		}
		if now.Unix() < receipt.RetainUntilUnix {
			return DeletionReceipt{}, errors.New("object version disappeared before retention expired")
		}
		return publishDeletionReceipt(request.ReceiptOutput, receipt)
	}
	if aws.ToString(head.VersionId) != receipt.VersionID ||
		aws.ToInt64(head.ContentLength) != receipt.ObjectBytes ||
		head.Metadata["kubebrain-artifact-sha256"] != receipt.ArtifactSHA256 {
		return DeletionReceipt{}, errors.New("remote object version no longer matches the upload receipt")
	}
	retention, err := client.GetObjectRetention(ctx, &s3.GetObjectRetentionInput{
		Bucket: aws.String(receipt.Bucket), Key: aws.String(receipt.ObjectKey), VersionId: aws.String(receipt.VersionID),
	})
	if err != nil {
		return DeletionReceipt{}, fmt.Errorf("read remote retention: %w", err)
	}
	if retention.Retention == nil || retention.Retention.RetainUntilDate == nil ||
		string(retention.Retention.Mode) != receipt.RetentionMode ||
		retention.Retention.RetainUntilDate.Unix() != receipt.RetainUntilUnix {
		return DeletionReceipt{}, errors.New("remote retention no longer matches the upload receipt")
	}
	if now.Unix() < receipt.RetainUntilUnix {
		return DeletionReceipt{}, fmt.Errorf("object retention has not expired; retain until %d", receipt.RetainUntilUnix)
	}
	_, err = client.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(receipt.Bucket), Key: aws.String(receipt.ObjectKey), VersionId: aws.String(receipt.VersionID),
	})
	if err != nil {
		deleteErr := fmt.Errorf("delete retained object version: %w", err)
		reconcileCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), deletionReconciliationTimeout)
		defer cancel()
		if verifyErr := verifyDeletedVersion(reconcileCtx, client, receipt); verifyErr != nil {
			return DeletionReceipt{}, errors.Join(deleteErr, verifyErr)
		}
		return publishDeletionReceipt(request.ReceiptOutput, receipt)
	}
	if err := verifyDeletedVersion(ctx, client, receipt); err != nil {
		return DeletionReceipt{}, err
	}
	return publishDeletionReceipt(request.ReceiptOutput, receipt)
}

func verifyDeletedVersion(ctx context.Context, client S3API, receipt Receipt) error {
	_, err := client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(receipt.Bucket), Key: aws.String(receipt.ObjectKey), VersionId: aws.String(receipt.VersionID),
	})
	if err == nil {
		return errors.New("deleted object version is still readable")
	}
	if !isNotFound(err) {
		return fmt.Errorf("verify deleted object version: %w", err)
	}
	return nil
}

func publishDeletionReceipt(path string, source Receipt) (DeletionReceipt, error) {
	receipt := DeletionReceipt{
		Format: DeletionReceiptFormat, Instance: source.Instance, BackupID: source.BackupID,
		ObjectStoreID: source.ObjectStoreID,
		Bucket:        source.Bucket, ObjectKey: source.ObjectKey, VersionID: source.VersionID,
		ArtifactSHA256: source.ArtifactSHA256, RetentionMode: source.RetentionMode,
		RetainUntilUnix: source.RetainUntilUnix, VersionAbsent: true,
		DeletedAtUnix: source.RetainUntilUnix,
	}
	if existing, err := ReadDeletionReceipt(path); err == nil {
		if existing.Instance == receipt.Instance && existing.BackupID == receipt.BackupID &&
			existing.ObjectStoreID == receipt.ObjectStoreID &&
			existing.Bucket == receipt.Bucket && existing.ObjectKey == receipt.ObjectKey &&
			existing.VersionID == receipt.VersionID && existing.ArtifactSHA256 == receipt.ArtifactSHA256 &&
			existing.RetentionMode == receipt.RetentionMode &&
			existing.RetainUntilUnix == receipt.RetainUntilUnix && existing.VersionAbsent {
			return existing, nil
		}
		return DeletionReceipt{}, errors.New("existing deletion receipt does not match the object version")
	} else if !errors.Is(err, os.ErrNotExist) {
		return DeletionReceipt{}, err
	}
	if err := WriteDeletionReceiptAtomic(path, receipt); err != nil {
		return DeletionReceipt{}, err
	}
	return receipt, nil
}

func verifyRemote(
	ctx context.Context,
	client S3API,
	bucket, key, versionID string,
	expected backupfile.Status,
	expectedFileSHA256 string,
	expectedSize int64,
) (bool, error) {
	output, err := client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(bucket), Key: aws.String(key), VersionId: aws.String(versionID),
	})
	if err != nil {
		return false, fmt.Errorf("download uploaded object: %w", err)
	}
	defer output.Body.Close()
	temp, err := os.CreateTemp("", "kubebrain-object-verify-*")
	if err != nil {
		return false, err
	}
	tempName := temp.Name()
	defer os.Remove(tempName)
	written, copyErr := io.Copy(temp, output.Body)
	if copyErr == nil {
		copyErr = temp.Sync()
	}
	closeErr := temp.Close()
	if copyErr != nil {
		return false, copyErr
	}
	if closeErr != nil {
		return false, closeErr
	}
	if written != expectedSize {
		return false, fmt.Errorf("remote object size mismatch: expected %d, got %d", expectedSize, written)
	}
	_, actualFileSHA256, err := fileSHA256(tempName)
	if err != nil {
		return false, err
	}
	if expectedFileSHA256 != "" && actualFileSHA256 != expectedFileSHA256 {
		return false, fmt.Errorf("remote object file SHA-256 mismatch: expected %s, got %s", expectedFileSHA256, actualFileSHA256)
	}
	actual, err := backupfile.Inspect(filepath.Clean(tempName))
	if err != nil {
		return false, fmt.Errorf("validate downloaded artifact: %w", err)
	}
	if actual != expected {
		return false, fmt.Errorf("downloaded artifact status mismatch: expected %+v, got %+v", expected, actual)
	}
	return true, nil
}

func artifactMetadata(
	request UploadRequest,
	status backupfile.Status,
	artifactFileSHA256 string,
	size int64,
	retainUntil time.Time,
) map[string]string {
	return map[string]string{
		"kubebrain-format":               status.Format,
		"kubebrain-instance":             request.Instance,
		"kubebrain-backup-id":            request.BackupID,
		"kubebrain-object-store-id":      request.ObjectStoreID,
		"kubebrain-artifact-file-sha256": artifactFileSHA256,
		"kubebrain-artifact-sha256":      status.SHA256,
		"kubebrain-snapshot-revision":    strconv.FormatInt(status.Revision, 10),
		"kubebrain-created-at-unix":      strconv.FormatInt(status.CreatedAtUnix, 10),
		"kubebrain-records":              strconv.Itoa(status.Records),
		"kubebrain-leases":               strconv.Itoa(status.Leases),
		"kubebrain-object-bytes":         strconv.FormatInt(size, 10),
		"kubebrain-retain-until-unix":    strconv.FormatInt(retainUntil.Unix(), 10),
	}
}

func validateHead(head *s3.HeadObjectOutput, expected map[string]string, size int64) error {
	if head == nil || aws.ToInt64(head.ContentLength) != size || aws.ToString(head.VersionId) == "" {
		return errors.New("existing object size or version ID differs")
	}
	for key, value := range expected {
		if head.Metadata[key] != value {
			return fmt.Errorf("existing object metadata %s differs", key)
		}
	}
	return nil
}

func exactVersionModifiedAt(
	ctx context.Context,
	client S3API,
	bucket, objectKey, versionID string,
	metadata map[string]string,
	size int64,
	retainUntil time.Time,
) (int64, error) {
	head, err := client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(bucket), Key: aws.String(objectKey), VersionId: aws.String(versionID),
	})
	if err != nil {
		return 0, fmt.Errorf("read exact object metadata: %w", err)
	}
	if err := validateHead(head, metadata, size); err != nil {
		return 0, fmt.Errorf("validate exact object metadata: %w", err)
	}
	if aws.ToString(head.VersionId) != versionID {
		return 0, errors.New("exact object returned a different version ID")
	}
	if head.LastModified == nil || head.LastModified.Unix() <= 0 ||
		!head.LastModified.Before(retainUntil) {
		return 0, errors.New("exact object has an invalid last-modified timestamp")
	}
	return head.LastModified.Unix(), nil
}

const objectWriteReconciliationTimeout = 30 * time.Minute

func objectWriteReconciliationContext(parent context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.WithoutCancel(parent), objectWriteReconciliationTimeout)
}

func fileSHA256(path string) (string, string, error) {
	file, err := os.Open(path)
	if err != nil {
		return "", "", err
	}
	defer file.Close()
	hash := sha256.New()
	if _, err := io.Copy(hash, file); err != nil {
		return "", "", err
	}
	sum := hash.Sum(nil)
	return base64.StdEncoding.EncodeToString(sum), fmt.Sprintf("%x", sum), nil
}

func objectLockMode(raw string) (types.ObjectLockMode, error) {
	switch raw {
	case "COMPLIANCE":
		return types.ObjectLockModeCompliance, nil
	case "GOVERNANCE":
		return types.ObjectLockModeGovernance, nil
	default:
		return "", errors.New("retention mode must be COMPLIANCE or GOVERNANCE")
	}
}

func isPreconditionFailure(err error) bool {
	var apiErr smithy.APIError
	return errors.As(err, &apiErr) &&
		(apiErr.ErrorCode() == "PreconditionFailed" || apiErr.ErrorCode() == "ConditionalRequestConflict")
}

func isNotFound(err error) bool {
	var apiErr smithy.APIError
	return errors.As(err, &apiErr) &&
		(apiErr.ErrorCode() == "NotFound" || apiErr.ErrorCode() == "NoSuchKey" || apiErr.ErrorCode() == "NoSuchVersion")
}
