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
	t.Cleanup(writer.Abort)
	require.NoError(t, writer.Add(record.Record{
		Key:         base64.StdEncoding.EncodeToString([]byte("/registry/cancel")),
		Value:       base64.StdEncoding.EncodeToString(bytes.Repeat([]byte("x"), 8*1024*1024)),
		ModRevision: 12345, CreateRevision: 12345, Version: 1,
	}))
	_, err = writer.Commit()
	require.NoError(t, err)
	return path
}
