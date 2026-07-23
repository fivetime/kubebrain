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
	"strconv"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/kubewharf/kubebrain/hack/production/operationaudit"
)

type AuditRequest struct {
	Input           string
	ObjectStoreID   string
	Bucket          string
	ObjectKey       string
	RetentionMode   string
	RetainUntilUnix int64
	ReceiptOutput   string
	Now             time.Time
}

func ArchiveAudit(ctx context.Context, client S3API, request AuditRequest) (AuditReceipt, error) {
	if request.Input == "" || request.ObjectStoreID == "" || request.Bucket == "" ||
		request.ObjectKey == "" || request.ReceiptOutput == "" || request.RetainUntilUnix <= 0 {
		return AuditReceipt{}, errors.New("operation audit request is incomplete")
	}
	mode, err := objectLockMode(request.RetentionMode)
	if err != nil {
		return AuditReceipt{}, err
	}
	status, body, err := operationaudit.InspectBytes(request.Input)
	if err != nil {
		return AuditReceipt{}, fmt.Errorf("validate operation audit artifact: %w", err)
	}
	now := request.Now.UTC()
	if now.IsZero() {
		now = time.Now().UTC()
	}
	retainUntil := time.Unix(request.RetainUntilUnix, 0).UTC()
	if !retainUntil.After(now) {
		return AuditReceipt{}, errors.New("retain-until timestamp must be in the future")
	}
	metadata := auditMetadata(request, status, retainUntil)
	if _, err := os.Stat(request.ReceiptOutput); err == nil {
		existing, readErr := ReadAuditReceipt(request.ReceiptOutput)
		if readErr != nil {
			return AuditReceipt{}, readErr
		}
		if !auditReceiptMatches(existing, request, status) {
			return AuditReceipt{}, errors.New("existing audit receipt does not match the archive request")
		}
		if err := verifyAuditRemote(ctx, client, request, existing.VersionID, status, body); err != nil {
			return AuditReceipt{}, err
		}
		if err := validateAuditRetention(ctx, client, request, existing.VersionID, retainUntil); err != nil {
			return AuditReceipt{}, err
		}
		return existing, nil
	} else if !errors.Is(err, os.ErrNotExist) {
		return AuditReceipt{}, err
	}
	frozenSum := sha256.Sum256(body)
	if fmt.Sprintf("%x", frozenSum[:]) != status.SHA256 || int64(len(body)) != status.Bytes {
		return AuditReceipt{}, errors.New("operation audit artifact changed while preparing upload")
	}
	output, putErr := client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(request.Bucket), Key: aws.String(request.ObjectKey),
		Body:          bytes.NewReader(body),
		ContentLength: aws.Int64(status.Bytes), ChecksumAlgorithm: types.ChecksumAlgorithmSha256,
		ChecksumSHA256: aws.String(base64.StdEncoding.EncodeToString(frozenSum[:])),
		IfNoneMatch:    aws.String("*"), Metadata: metadata, ObjectLockMode: mode,
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
				return AuditReceipt{}, fmt.Errorf(
					"conditional audit upload conflicted and existing object cannot be read: %w", headErr,
				)
			}
			return AuditReceipt{}, errors.Join(
				fmt.Errorf("conditional audit object upload: %w", putErr),
				fmt.Errorf("inspect audit object after failed conditional upload: %w", headErr),
			)
		}
		if err := validateHead(head, metadata, status.Bytes); err != nil {
			conflictErr := fmt.Errorf("refusing to replace conflicting audit object: %w", err)
			if isPreconditionFailure(putErr) {
				return AuditReceipt{}, conflictErr
			}
			return AuditReceipt{}, errors.Join(
				fmt.Errorf("conditional audit object upload: %w", putErr), conflictErr,
			)
		}
		versionID = aws.ToString(head.VersionId)
	}
	if versionID == "" {
		return AuditReceipt{}, errors.New("object store did not return a version ID; Object Lock/versioning is required")
	}
	archivedAtUnix, err := exactVersionModifiedAt(
		verificationCtx, client, request.Bucket, request.ObjectKey, versionID,
		metadata, status.Bytes, retainUntil,
	)
	if err != nil {
		return AuditReceipt{}, err
	}
	if err := verifyAuditRemote(verificationCtx, client, request, versionID, status, body); err != nil {
		return AuditReceipt{}, err
	}
	if err := validateAuditRetention(verificationCtx, client, request, versionID, retainUntil); err != nil {
		return AuditReceipt{}, err
	}
	artifact := status.Artifact
	receipt := AuditReceipt{
		Format: AuditReceiptFormat, OperationID: artifact.OperationID, OperationUID: artifact.UID,
		Instance: artifact.Instance, OperationType: artifact.Type, Phase: artifact.Phase,
		ExecutionReceiptSHA256: artifact.ReceiptSHA256, ObjectStoreID: request.ObjectStoreID,
		Bucket: request.Bucket, ObjectKey: request.ObjectKey, VersionID: versionID,
		ArtifactSHA256: status.SHA256, ObjectBytes: status.Bytes,
		RetentionMode: request.RetentionMode, RetainUntilUnix: retainUntil.Unix(),
		RemoteVerified: true, ArchivedAtUnix: archivedAtUnix,
	}
	if err := WriteAuditReceiptAtomic(request.ReceiptOutput, receipt); err != nil {
		return AuditReceipt{}, err
	}
	return receipt, nil
}

func verifyAuditRemote(
	ctx context.Context,
	client S3API,
	request AuditRequest,
	versionID string,
	expected operationaudit.Status,
	expectedBody []byte,
) error {
	output, err := client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(request.Bucket), Key: aws.String(request.ObjectKey),
		VersionId: aws.String(versionID),
	})
	if err != nil {
		return fmt.Errorf("download operation audit object: %w", err)
	}
	defer output.Body.Close()
	body, err := io.ReadAll(io.LimitReader(output.Body, expected.Bytes+1))
	if err != nil {
		return err
	}
	if int64(len(body)) != expected.Bytes {
		return errors.New("remote operation audit size differs")
	}
	sum := sha256.Sum256(body)
	if fmt.Sprintf("%x", sum[:]) != expected.SHA256 {
		return errors.New("remote operation audit digest differs")
	}
	if !bytes.Equal(body, expectedBody) {
		return errors.New("remote operation audit bytes differ")
	}
	return nil
}

func validateAuditRetention(
	ctx context.Context,
	client S3API,
	request AuditRequest,
	versionID string,
	retainUntil time.Time,
) error {
	retention, err := client.GetObjectRetention(ctx, &s3.GetObjectRetentionInput{
		Bucket: aws.String(request.Bucket), Key: aws.String(request.ObjectKey),
		VersionId: aws.String(versionID),
	})
	if err != nil {
		return fmt.Errorf("read remote audit retention: %w", err)
	}
	if retention.Retention == nil || string(retention.Retention.Mode) != request.RetentionMode ||
		retention.Retention.RetainUntilDate == nil ||
		retention.Retention.RetainUntilDate.Unix() != retainUntil.Unix() {
		return errors.New("remote audit retention does not match the requested policy")
	}
	return nil
}

func auditMetadata(
	request AuditRequest,
	status operationaudit.Status,
	retainUntil time.Time,
) map[string]string {
	artifact := status.Artifact
	return map[string]string{
		"kubebrain-format":                   operationaudit.Format,
		"kubebrain-operation-id":             artifact.OperationID,
		"kubebrain-operation-uid":            artifact.UID,
		"kubebrain-instance":                 artifact.Instance,
		"kubebrain-operation-type":           artifact.Type,
		"kubebrain-operation-phase":          artifact.Phase,
		"kubebrain-execution-receipt-sha256": artifact.ReceiptSHA256,
		"kubebrain-object-store-id":          request.ObjectStoreID,
		"kubebrain-artifact-sha256":          status.SHA256,
		"kubebrain-object-bytes":             strconv.FormatInt(status.Bytes, 10),
		"kubebrain-retain-until-unix":        strconv.FormatInt(retainUntil.Unix(), 10),
	}
}

func auditReceiptMatches(
	receipt AuditReceipt,
	request AuditRequest,
	status operationaudit.Status,
) bool {
	artifact := status.Artifact
	return receipt.OperationID == artifact.OperationID && receipt.OperationUID == artifact.UID &&
		receipt.Instance == artifact.Instance && receipt.OperationType == artifact.Type &&
		receipt.Phase == artifact.Phase && receipt.ExecutionReceiptSHA256 == artifact.ReceiptSHA256 &&
		receipt.ObjectStoreID == request.ObjectStoreID && receipt.Bucket == request.Bucket &&
		receipt.ObjectKey == request.ObjectKey && receipt.ArtifactSHA256 == status.SHA256 &&
		receipt.ObjectBytes == status.Bytes && receipt.RetentionMode == request.RetentionMode &&
		receipt.RetainUntilUnix == request.RetainUntilUnix
}
