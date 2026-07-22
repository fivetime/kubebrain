package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/kubewharf/kubebrain/hack/backup/objectstore/internal/objectstore"
)

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), durationEnv("TIMEOUT", 30*time.Minute))
	defer cancel()
	action := os.Getenv("ACTION")
	if action == "manifest" {
		receiptPaths := stringArrayEnv("RECEIPT_INPUTS_JSON")
		status, err := objectstore.BuildInventoryManifest(
			receiptPaths, os.Getenv("OBJECT_STORE_ID"), os.Getenv("S3_BUCKET"),
			os.Getenv("INVENTORY_PREFIX"), os.Getenv("INVENTORY_OUTPUT"),
		)
		if err != nil {
			log.Fatal(err)
		}
		_ = json.NewEncoder(os.Stdout).Encode(status)
		return
	}
	client, err := newClient(ctx)
	if err != nil {
		log.Fatal(err)
	}
	switch action {
	case "upload":
		retainUntilUnix := int64Env("RETAIN_UNTIL_UNIX")
		minRecords := nonNegativeIntEnv("MIN_RECORDS")
		maxAgeSeconds := int64Env("MAX_AGE_SECONDS")
		receipt, err := objectstore.Upload(ctx, client, objectstore.UploadRequest{
			Input: os.Getenv("INPUT"), Instance: os.Getenv("INSTANCE"), BackupID: os.Getenv("BACKUP_ID"),
			ObjectStoreID: os.Getenv("OBJECT_STORE_ID"),
			Bucket:        os.Getenv("S3_BUCKET"), ObjectKey: os.Getenv("S3_OBJECT_KEY"),
			RetentionMode: os.Getenv("RETENTION_MODE"), RetainUntilUnix: retainUntilUnix,
			ExpectedPrefix: os.Getenv("EXPECTED_PREFIX"), MinRecords: minRecords,
			MaxAgeSeconds: maxAgeSeconds,
			ReceiptOutput: os.Getenv("RECEIPT_OUTPUT"),
		})
		if err != nil {
			log.Fatal(err)
		}
		_ = json.NewEncoder(os.Stdout).Encode(receipt)
	case "delete":
		receipt, err := objectstore.ReadReceipt(os.Getenv("RECEIPT_INPUT"))
		if err != nil {
			log.Fatal(err)
		}
		deletion, err := objectstore.Delete(ctx, client, objectstore.DeleteRequest{
			Receipt: receipt, Confirmation: os.Getenv("DELETE_CONFIRM"),
			ObjectStoreID: os.Getenv("OBJECT_STORE_ID"),
			ReceiptOutput: os.Getenv("DELETE_RECEIPT_OUTPUT"),
		})
		if err != nil {
			log.Fatal(err)
		}
		_ = json.NewEncoder(os.Stdout).Encode(deletion)
	case "archive":
		receipt, err := objectstore.ArchiveAudit(ctx, client, objectstore.AuditRequest{
			Input: os.Getenv("INPUT"), ObjectStoreID: os.Getenv("OBJECT_STORE_ID"),
			Bucket: os.Getenv("S3_BUCKET"), ObjectKey: os.Getenv("S3_OBJECT_KEY"),
			RetentionMode:   os.Getenv("RETENTION_MODE"),
			RetainUntilUnix: int64Env("RETAIN_UNTIL_UNIX"),
			ReceiptOutput:   os.Getenv("RECEIPT_OUTPUT"),
		})
		if err != nil {
			log.Fatal(err)
		}
		_ = json.NewEncoder(os.Stdout).Encode(receipt)
	case "blob":
		receipt, err := objectstore.ArchiveBlob(ctx, client, objectstore.BlobRequest{
			Input: os.Getenv("INPUT"), ArtifactFormat: os.Getenv("ARTIFACT_FORMAT"),
			ArtifactID: os.Getenv("ARTIFACT_ID"), Instance: os.Getenv("INSTANCE"),
			ObjectStoreID: os.Getenv("OBJECT_STORE_ID"), Bucket: os.Getenv("S3_BUCKET"),
			ObjectKey: os.Getenv("S3_OBJECT_KEY"), RetentionMode: os.Getenv("RETENTION_MODE"),
			RetainUntilUnix: int64Env("RETAIN_UNTIL_UNIX"),
			ReceiptOutput:   os.Getenv("RECEIPT_OUTPUT"),
		})
		if err != nil {
			log.Fatal(err)
		}
		_ = json.NewEncoder(os.Stdout).Encode(receipt)
	case "blob-read":
		var artifactFormats []string
		if raw := os.Getenv("ARTIFACT_FORMATS_JSON"); raw != "" {
			artifactFormats = mustParseJSONStringArray(raw, "ARTIFACT_FORMATS_JSON")
		}
		receipt, err := objectstore.ReadBlob(ctx, client, objectstore.BlobReadRequest{
			Output: os.Getenv("OUTPUT"), ArtifactFormat: os.Getenv("ARTIFACT_FORMAT"),
			ArtifactFormats: artifactFormats,
			ArtifactID:      os.Getenv("ARTIFACT_ID"), Instance: os.Getenv("INSTANCE"),
			ObjectStoreID: os.Getenv("OBJECT_STORE_ID"), Bucket: os.Getenv("S3_BUCKET"),
			ObjectKey:          os.Getenv("S3_OBJECT_KEY"),
			MinRetainUntilUnix: int64Env("MIN_RETAIN_UNTIL_UNIX"),
		})
		if err != nil {
			log.Fatal(err)
		}
		_ = json.NewEncoder(os.Stdout).Encode(receipt)
	case "inventory":
		receipt, err := objectstore.ReconcileInventory(ctx, client, objectstore.InventoryRequest{
			Input: os.Getenv("INVENTORY_INPUT"), ObjectStoreID: os.Getenv("OBJECT_STORE_ID"),
			ReceiptOutput: os.Getenv("RECEIPT_OUTPUT"),
		})
		if err != nil {
			log.Fatal(err)
		}
		_ = json.NewEncoder(os.Stdout).Encode(receipt)
	case "usage":
		allowedFormats := stringArrayEnv("ALLOWED_FORMATS_JSON")
		receipt, err := objectstore.MeasureUsage(ctx, client, objectstore.UsageRequest{
			ObjectStoreID: os.Getenv("OBJECT_STORE_ID"), Bucket: os.Getenv("S3_BUCKET"),
			Prefix: os.Getenv("USAGE_PREFIX"), AllowedFormats: allowedFormats,
			ReceiptOutput: os.Getenv("RECEIPT_OUTPUT"),
		})
		if err != nil {
			log.Fatal(err)
		}
		_ = json.NewEncoder(os.Stdout).Encode(receipt)
	default:
		log.Fatal("ACTION must be upload, delete, archive, blob, blob-read, manifest, inventory, or usage")
	}
}

func stringArrayEnv(name string) []string {
	return mustParseJSONStringArray(os.Getenv(name), name)
}

func mustParseJSONStringArray(raw, name string) []string {
	values, err := parseJSONStringArray(raw, name)
	if err != nil {
		log.Fatal(err)
	}
	return values
}

func parseJSONStringArray(raw, name string) ([]string, error) {
	var values []string
	decoder := json.NewDecoder(strings.NewReader(raw))
	if err := decoder.Decode(&values); err != nil {
		return nil, fmt.Errorf("%s must be a JSON string array", name)
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		return nil, fmt.Errorf("%s must contain a single JSON string array", name)
	}
	if values == nil {
		return nil, fmt.Errorf("%s must be a JSON string array, not null", name)
	}
	for index, value := range values {
		if value == "" {
			return nil, fmt.Errorf("%s[%d] must be a non-empty string", name, index)
		}
	}
	return values, nil
}

func newClient(ctx context.Context) (*s3.Client, error) {
	endpoint := os.Getenv("S3_ENDPOINT")
	region := os.Getenv("AWS_REGION")
	accessKey := os.Getenv("AWS_ACCESS_KEY_ID")
	secretKey := os.Getenv("AWS_SECRET_ACCESS_KEY")
	if endpoint == "" || region == "" || accessKey == "" || secretKey == "" {
		return nil, errors.New("S3_ENDPOINT, AWS_REGION, AWS_ACCESS_KEY_ID, and AWS_SECRET_ACCESS_KEY are required")
	}
	if _, err := url.ParseRequestURI(endpoint); err != nil {
		return nil, fmt.Errorf("invalid S3_ENDPOINT: %w", err)
	}
	cfg, err := config.LoadDefaultConfig(
		ctx,
		config.WithRegion(region),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(accessKey, secretKey, os.Getenv("AWS_SESSION_TOKEN"))),
	)
	if err != nil {
		return nil, err
	}
	forcePathStyle, err := parseOptionalBoolEnv("S3_FORCE_PATH_STYLE")
	if err != nil {
		return nil, err
	}
	return s3.NewFromConfig(cfg, func(options *s3.Options) {
		options.BaseEndpoint = aws.String(endpoint)
		options.UsePathStyle = forcePathStyle
	}), nil
}

func parseOptionalBoolEnv(name string) (bool, error) {
	raw := os.Getenv(name)
	if raw == "" {
		return false, nil
	}
	value, err := strconv.ParseBool(raw)
	if err != nil {
		return false, fmt.Errorf("%s must be a boolean: %w", name, err)
	}
	return value, nil
}

func int64Env(name string) int64 {
	value, err := strconv.ParseInt(os.Getenv(name), 10, 64)
	if err != nil || value <= 0 {
		log.Fatalf("%s must be a positive integer", name)
	}
	return value
}

func nonNegativeIntEnv(name string) int {
	value, err := strconv.Atoi(os.Getenv(name))
	if err != nil || value < 0 {
		log.Fatalf("%s must be a non-negative integer", name)
	}
	return value
}

func durationEnv(name string, fallback time.Duration) time.Duration {
	raw := os.Getenv(name)
	if raw == "" {
		return fallback
	}
	value, err := time.ParseDuration(raw)
	if err != nil || value <= 0 {
		log.Fatalf("%s must be a positive Go duration", name)
	}
	return value
}

func init() {
	log.SetFlags(0)
}
