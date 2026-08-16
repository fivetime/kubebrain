package objectstore

import (
	"bytes"
	"context"
	"encoding/base64"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
	"github.com/kubewharf/kubebrain/hack/backup/internal/backupfile"
	"github.com/kubewharf/kubebrain/hack/backup/internal/record"
	"github.com/stretchr/testify/require"
)

func TestCanceledPutObjectLeavesNoRemoteArtifact(t *testing.T) {
	directEndpoint := os.Getenv("KUBEBRAIN_OBJECTSTORE_CANCEL_S3_ENDPOINT")
	bucket := os.Getenv("KUBEBRAIN_OBJECTSTORE_CANCEL_S3_BUCKET")
	accessKey := os.Getenv("AWS_ACCESS_KEY_ID")
	secretKey := os.Getenv("AWS_SECRET_ACCESS_KEY")
	if directEndpoint == "" || bucket == "" || accessKey == "" || secretKey == "" {
		t.Skip("set the real object-store cancellation integration environment")
	}
	target, err := url.Parse(directEndpoint)
	require.NoError(t, err)
	started := make(chan struct{})
	proxy := httputil.NewSingleHostReverseProxy(target)
	transport := http.DefaultTransport.(*http.Transport).Clone()
	proxy.Transport = roundTripperFunc(func(request *http.Request) (*http.Response, error) {
		if request.Method == http.MethodPut && request.Body != nil {
			request.Body = &throttledReadCloser{ReadCloser: request.Body, started: started}
		}
		return transport.RoundTrip(request)
	})
	proxy.ErrorHandler = func(response http.ResponseWriter, _ *http.Request, proxyErr error) {
		if !errors.Is(proxyErr, context.Canceled) {
			http.Error(response, proxyErr.Error(), http.StatusBadGateway)
		}
	}
	server := httptest.NewServer(proxy)
	defer server.Close()

	client := realS3Client(t, server.URL, accessKey, secretKey)
	directClient := realS3Client(t, directEndpoint, accessKey, secretKey)
	artifact := writeLargeCancellationArtifact(t)
	receiptPath := filepath.Join(t.TempDir(), "receipt.json")
	objectKey := "cancel/inflight-single-put.jsonl"
	uploadCtx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() {
		_, uploadErr := Upload(uploadCtx, client, UploadRequest{
			Input: artifact, Instance: "cancel-integration", BackupID: "cancel-1",
			ObjectStoreID: "minio-integration", Bucket: bucket, ObjectKey: objectKey,
			RetentionMode: "COMPLIANCE", RetainUntilUnix: time.Now().Add(time.Hour).Unix(),
			ExpectedPrefix: "/registry", MinRecords: 1, MaxAgeSeconds: 300,
			ReceiptOutput: receiptPath,
		})
		done <- uploadErr
	}()
	select {
	case <-started:
	case <-time.After(10 * time.Second):
		t.Fatal("PutObject body did not start")
	}
	time.Sleep(100 * time.Millisecond)
	cancel()
	select {
	case err = <-done:
		require.Error(t, err)
		require.ErrorContains(t, err, "conditional object upload")
		require.ErrorContains(t, err, "inspect object after failed conditional upload")
	case <-time.After(10 * time.Second):
		t.Fatal("canceled PutObject did not return")
	}
	_, statErr := os.Stat(receiptPath)
	require.ErrorIs(t, statErr, os.ErrNotExist)

	_, err = directClient.HeadObject(t.Context(), &s3.HeadObjectInput{
		Bucket: aws.String(bucket), Key: aws.String(objectKey),
	})
	require.Error(t, err, "a canceled single PUT must not publish a partial object")
	var apiErr smithy.APIError
	require.ErrorAs(t, err, &apiErr)
	require.Contains(t, []string{"NotFound", "NoSuchKey"}, apiErr.ErrorCode())
	incomplete, err := directClient.ListMultipartUploads(t.Context(), &s3.ListMultipartUploadsInput{
		Bucket: aws.String(bucket), Prefix: aws.String("cancel/"),
	})
	require.NoError(t, err)
	require.Empty(t, incomplete.Uploads, "the single-PUT workflow must not leave multipart state")
}

func TestCommittedPutObjectResponseLossReconcilesRealS3(t *testing.T) {
	directEndpoint, bucket, accessKey, secretKey := realObjectStoreEnv(t)
	target, err := url.Parse(directEndpoint)
	require.NoError(t, err)
	uploadCtx, cancel := context.WithCancel(t.Context())
	defer cancel()
	proxy := httputil.NewSingleHostReverseProxy(target)
	var lost sync.Once
	proxy.ModifyResponse = func(response *http.Response) error {
		if response.Request.Method == http.MethodPut {
			lost.Do(cancel)
			return errors.New("injected loss of committed PutObject response")
		}
		return nil
	}
	proxy.ErrorHandler = func(response http.ResponseWriter, _ *http.Request, proxyErr error) {
		http.Error(response, proxyErr.Error(), http.StatusBadGateway)
	}
	server := httptest.NewServer(proxy)
	defer server.Close()

	client := realS3Client(t, server.URL, accessKey, secretKey)
	directClient := realS3Client(t, directEndpoint, accessKey, secretKey)
	artifact := writeLargeCancellationArtifact(t)
	receiptPath := filepath.Join(t.TempDir(), "receipt.json")
	objectKey := "reconcile/committed-response-loss.jsonl"
	receipt, err := Upload(uploadCtx, client, UploadRequest{
		Input: artifact, Instance: "reconcile-integration", BackupID: "reconcile-1",
		ObjectStoreID: "minio-integration", Bucket: bucket, ObjectKey: objectKey,
		RetentionMode: "COMPLIANCE", RetainUntilUnix: time.Now().Add(time.Hour).Unix(),
		ExpectedPrefix: "/registry", MinRecords: 1, MaxAgeSeconds: 300,
		ReceiptOutput: receiptPath,
	})
	require.NoError(t, err)
	require.ErrorIs(t, uploadCtx.Err(), context.Canceled)
	require.NotEmpty(t, receipt.VersionID)
	require.True(t, receipt.RemoteVerified)
	persisted, err := ReadReceipt(receiptPath)
	require.NoError(t, err)
	require.Equal(t, receipt, persisted)

	head, err := directClient.HeadObject(t.Context(), &s3.HeadObjectInput{
		Bucket: aws.String(bucket), Key: aws.String(objectKey), VersionId: aws.String(receipt.VersionID),
	})
	require.NoError(t, err)
	require.Equal(t, receipt.VersionID, aws.ToString(head.VersionId))
	require.Equal(t, receipt.ArtifactFileSHA256, head.Metadata["kubebrain-artifact-file-sha256"])
	versions, err := directClient.ListObjectVersions(t.Context(), &s3.ListObjectVersionsInput{
		Bucket: aws.String(bucket), Prefix: aws.String(objectKey),
	})
	require.NoError(t, err)
	require.Len(t, versions.Versions, 1, "response loss must not cause an overwrite retry")
	require.Empty(t, versions.DeleteMarkers)
}

func TestConditionalUploadRefusesConflictingRealS3Object(t *testing.T) {
	directEndpoint, bucket, accessKey, secretKey := realObjectStoreEnv(t)
	client := realS3Client(t, directEndpoint, accessKey, secretKey)
	objectKey := "conflict/existing-locked-object.jsonl"
	retainUntil := time.Now().Add(time.Hour).UTC()
	originalBody := []byte("different protected object")
	original, err := client.PutObject(t.Context(), &s3.PutObjectInput{
		Bucket: aws.String(bucket), Key: aws.String(objectKey), Body: bytes.NewReader(originalBody),
		ContentLength: aws.Int64(int64(len(originalBody))),
		Metadata: map[string]string{
			"kubebrain-backup-id": "different-backup",
		},
		ObjectLockMode:            types.ObjectLockModeCompliance,
		ObjectLockRetainUntilDate: aws.Time(retainUntil),
	})
	require.NoError(t, err)
	require.NotEmpty(t, aws.ToString(original.VersionId))

	receiptPath := filepath.Join(t.TempDir(), "receipt.json")
	_, err = Upload(t.Context(), client, UploadRequest{
		Input: writeLargeCancellationArtifact(t), Instance: "conflict-integration", BackupID: "conflict-1",
		ObjectStoreID: "minio-integration", Bucket: bucket, ObjectKey: objectKey,
		RetentionMode: "COMPLIANCE", RetainUntilUnix: time.Now().Add(2 * time.Hour).Unix(),
		ExpectedPrefix: "/registry", MinRecords: 1, MaxAgeSeconds: 300,
		ReceiptOutput: receiptPath,
	})
	require.ErrorContains(t, err, "refusing to replace conflicting object")
	_, statErr := os.Stat(receiptPath)
	require.ErrorIs(t, statErr, os.ErrNotExist)

	versions, err := client.ListObjectVersions(t.Context(), &s3.ListObjectVersionsInput{
		Bucket: aws.String(bucket), Prefix: aws.String(objectKey),
	})
	require.NoError(t, err)
	require.Len(t, versions.Versions, 1)
	require.Equal(t, aws.ToString(original.VersionId), aws.ToString(versions.Versions[0].VersionId))
	require.Empty(t, versions.DeleteMarkers)
	remote, err := client.GetObject(t.Context(), &s3.GetObjectInput{
		Bucket: aws.String(bucket), Key: aws.String(objectKey), VersionId: original.VersionId,
	})
	require.NoError(t, err)
	defer remote.Body.Close()
	remoteBody, err := io.ReadAll(remote.Body)
	require.NoError(t, err)
	require.Equal(t, originalBody, remoteBody)
}

func TestConditionalUploadRejectsMatchingMetadataCorruptRealS3Body(t *testing.T) {
	directEndpoint, bucket, accessKey, secretKey := realObjectStoreEnv(t)
	client := realS3Client(t, directEndpoint, accessKey, secretKey)
	artifactPath := writeLargeCancellationArtifact(t)
	artifact, err := backupfile.OpenVerified(artifactPath)
	require.NoError(t, err)
	defer artifact.Close()
	info, err := os.Stat(artifact.Path())
	require.NoError(t, err)
	_, artifactFileSHA256, err := fileSHA256(artifact.Path())
	require.NoError(t, err)
	now := time.Now().UTC()
	retainUntil := now.Add(time.Hour)
	objectKey := "conflict/matching-metadata-corrupt-body.jsonl"
	request := UploadRequest{
		Input: artifactPath, Instance: "corrupt-integration", BackupID: "corrupt-1",
		ObjectStoreID: "minio-integration", Bucket: bucket, ObjectKey: objectKey,
		RetentionMode: "COMPLIANCE", RetainUntilUnix: retainUntil.Unix(),
		ExpectedPrefix: "/registry", MinRecords: 1, MaxAgeSeconds: 300,
		ReceiptOutput: filepath.Join(t.TempDir(), "receipt.json"), Now: now,
	}
	metadata := artifactMetadata(request, artifact.Status(), artifactFileSHA256, info.Size(), retainUntil)
	corruptBody := bytes.Repeat([]byte("z"), int(info.Size()))
	original, err := client.PutObject(t.Context(), &s3.PutObjectInput{
		Bucket: aws.String(bucket), Key: aws.String(objectKey), Body: bytes.NewReader(corruptBody),
		ContentLength:             aws.Int64(info.Size()),
		Metadata:                  metadata,
		ObjectLockMode:            types.ObjectLockModeCompliance,
		ObjectLockRetainUntilDate: aws.Time(retainUntil),
	})
	require.NoError(t, err)
	require.NotEmpty(t, aws.ToString(original.VersionId))

	_, err = Upload(t.Context(), client, request)
	require.ErrorContains(t, err, "remote object file SHA-256 mismatch")
	_, statErr := os.Stat(request.ReceiptOutput)
	require.ErrorIs(t, statErr, os.ErrNotExist)
	versions, err := client.ListObjectVersions(t.Context(), &s3.ListObjectVersionsInput{
		Bucket: aws.String(bucket), Prefix: aws.String(objectKey),
	})
	require.NoError(t, err)
	require.Len(t, versions.Versions, 1)
	require.Equal(t, aws.ToString(original.VersionId), aws.ToString(versions.Versions[0].VersionId))
	require.Empty(t, versions.DeleteMarkers)
}

func TestConditionalUploadRejectsInsufficientRealS3Retention(t *testing.T) {
	directEndpoint, bucket, accessKey, secretKey := realObjectStoreEnv(t)
	client := realS3Client(t, directEndpoint, accessKey, secretKey)
	artifactPath := writeLargeCancellationArtifact(t)
	artifact, err := backupfile.OpenVerified(artifactPath)
	require.NoError(t, err)
	defer artifact.Close()
	body, err := os.ReadFile(artifact.Path())
	require.NoError(t, err)
	info, err := os.Stat(artifact.Path())
	require.NoError(t, err)
	checksum, artifactFileSHA256, err := fileSHA256(artifact.Path())
	require.NoError(t, err)
	now := time.Now().UTC()
	requestedRetention := now.Add(2 * time.Hour)
	actualRetention := now.Add(time.Hour)
	objectKey := "conflict/insufficient-retention.jsonl"
	request := UploadRequest{
		Input: artifactPath, Instance: "retention-integration", BackupID: "retention-1",
		ObjectStoreID: "minio-integration", Bucket: bucket, ObjectKey: objectKey,
		RetentionMode: "COMPLIANCE", RetainUntilUnix: requestedRetention.Unix(),
		ExpectedPrefix: "/registry", MinRecords: 1, MaxAgeSeconds: 300,
		ReceiptOutput: filepath.Join(t.TempDir(), "receipt.json"), Now: now,
	}
	metadata := artifactMetadata(request, artifact.Status(), artifactFileSHA256, info.Size(), requestedRetention)
	original, err := client.PutObject(t.Context(), &s3.PutObjectInput{
		Bucket: aws.String(bucket), Key: aws.String(objectKey), Body: bytes.NewReader(body),
		ContentLength:             aws.Int64(info.Size()),
		ChecksumAlgorithm:         types.ChecksumAlgorithmSha256,
		ChecksumSHA256:            aws.String(checksum),
		Metadata:                  metadata,
		ObjectLockMode:            types.ObjectLockModeCompliance,
		ObjectLockRetainUntilDate: aws.Time(actualRetention),
	})
	require.NoError(t, err)
	require.NotEmpty(t, aws.ToString(original.VersionId))

	_, err = Upload(t.Context(), client, request)
	require.ErrorContains(t, err, "remote object retention does not match the requested policy")
	_, statErr := os.Stat(request.ReceiptOutput)
	require.ErrorIs(t, statErr, os.ErrNotExist)
	versions, err := client.ListObjectVersions(t.Context(), &s3.ListObjectVersionsInput{
		Bucket: aws.String(bucket), Prefix: aws.String(objectKey),
	})
	require.NoError(t, err)
	require.Len(t, versions.Versions, 1)
	require.Equal(t, aws.ToString(original.VersionId), aws.ToString(versions.Versions[0].VersionId))
	require.Empty(t, versions.DeleteMarkers)
}

func TestRestartRecoversReceiptAfterRealS3Commit(t *testing.T) {
	directEndpoint, bucket, accessKey, secretKey := realObjectStoreEnv(t)
	client := realS3Client(t, directEndpoint, accessKey, secretKey)
	now := time.Now().UTC()
	objectKey := "reconcile/receipt-write-failure.jsonl"
	badReceiptPath := filepath.Join(t.TempDir(), "missing", "receipt.json")
	request := UploadRequest{
		Input: writeLargeCancellationArtifact(t), Instance: "receipt-integration", BackupID: "receipt-1",
		ObjectStoreID: "minio-integration", Bucket: bucket, ObjectKey: objectKey,
		RetentionMode: "COMPLIANCE", RetainUntilUnix: now.Add(2 * time.Hour).Unix(),
		ExpectedPrefix: "/registry", MinRecords: 1, MaxAgeSeconds: 300,
		ReceiptOutput: badReceiptPath, Now: now,
	}
	_, err := Upload(t.Context(), client, request)
	require.ErrorContains(t, err, "no such file or directory")
	versions, err := client.ListObjectVersions(t.Context(), &s3.ListObjectVersionsInput{
		Bucket: aws.String(bucket), Prefix: aws.String(objectKey),
	})
	require.NoError(t, err)
	require.Len(t, versions.Versions, 1)
	committedVersion := aws.ToString(versions.Versions[0].VersionId)
	require.NotEmpty(t, committedVersion)

	restarted := request
	restarted.ReceiptOutput = filepath.Join(t.TempDir(), "receipt.json")
	restarted.Now = now.Add(time.Second)
	receipt, err := Upload(t.Context(), client, restarted)
	require.NoError(t, err)
	require.Equal(t, committedVersion, receipt.VersionID)
	persisted, err := ReadReceipt(restarted.ReceiptOutput)
	require.NoError(t, err)
	require.Equal(t, receipt, persisted)
	versions, err = client.ListObjectVersions(t.Context(), &s3.ListObjectVersionsInput{
		Bucket: aws.String(bucket), Prefix: aws.String(objectKey),
	})
	require.NoError(t, err)
	require.Len(t, versions.Versions, 1, "restart recovery must reuse the committed version")
	require.Equal(t, committedVersion, aws.ToString(versions.Versions[0].VersionId))
	require.Empty(t, versions.DeleteMarkers)
}

func realObjectStoreEnv(t *testing.T) (string, string, string, string) {
	t.Helper()
	directEndpoint := os.Getenv("KUBEBRAIN_OBJECTSTORE_CANCEL_S3_ENDPOINT")
	bucket := os.Getenv("KUBEBRAIN_OBJECTSTORE_CANCEL_S3_BUCKET")
	accessKey := os.Getenv("AWS_ACCESS_KEY_ID")
	secretKey := os.Getenv("AWS_SECRET_ACCESS_KEY")
	if directEndpoint == "" || bucket == "" || accessKey == "" || secretKey == "" {
		t.Skip("set the real object-store cancellation integration environment")
	}
	return directEndpoint, bucket, accessKey, secretKey
}

type roundTripperFunc func(*http.Request) (*http.Response, error)

func (f roundTripperFunc) RoundTrip(request *http.Request) (*http.Response, error) { return f(request) }

type throttledReadCloser struct {
	io.ReadCloser
	started chan struct{}
	once    sync.Once
}

func (r *throttledReadCloser) Read(buffer []byte) (int, error) {
	r.once.Do(func() { close(r.started) })
	if len(buffer) > 32*1024 {
		buffer = buffer[:32*1024]
	}
	n, err := r.ReadCloser.Read(buffer)
	if n > 0 {
		time.Sleep(20 * time.Millisecond)
	}
	return n, err
}

func (r *throttledReadCloser) Close() error { return r.ReadCloser.Close() }

func realS3Client(t *testing.T, endpoint, accessKey, secretKey string) *s3.Client {
	t.Helper()
	cfg, err := config.LoadDefaultConfig(t.Context(), config.WithRegion("us-east-1"),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(accessKey, secretKey, "")))
	require.NoError(t, err)
	return s3.NewFromConfig(cfg, func(options *s3.Options) {
		options.BaseEndpoint = aws.String(endpoint)
		options.UsePathStyle = true
	})
}

func writeLargeCancellationArtifact(t *testing.T) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "large-backup.jsonl")
	writer, err := backupfile.NewAtomicWriter(path, "/registry", 12345)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, writer.Abort()) })
	require.NoError(t, writer.Add(record.Record{
		Key:         base64.StdEncoding.EncodeToString([]byte("/registry/cancel")),
		Value:       base64.StdEncoding.EncodeToString(bytes.Repeat([]byte("x"), 8*1024*1024)),
		ModRevision: 12345, CreateRevision: 12345, Version: 1,
	}))
	_, err = writer.Commit()
	require.NoError(t, err)
	return path
}
