package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/kubewharf/kubebrain/hack/production/internal/meteringstorage"
)

func main() {
	var instance, executor, sourceStore, sourceBucket, sourcePrefix string
	var meteringStore, meteringBucket, snapshotPrefix, retentionMode, allowedFormatsJSON string
	var retentionDuration, finalizationDelay, timeout time.Duration
	var sourceEndpoint, sourceRegion, sourceAccessKey, sourceSecretKey, sourceSessionToken string
	var meteringEndpoint, meteringRegion, meteringAccessKey, meteringSecretKey, meteringSessionToken string
	var sourcePathStyle, meteringPathStyle bool
	flag.StringVar(&instance, "instance", "", "stable DBaaS instance identifier")
	flag.StringVar(&executor, "object-executor", "/usr/local/bin/kubebrain-logical-object", "Object Lock executor")
	flag.StringVar(&sourceStore, "source-object-store-id", "", "billed object store identifier")
	flag.StringVar(&sourceBucket, "source-bucket", "", "billed Object Lock bucket")
	flag.StringVar(&sourcePrefix, "source-prefix", "", "instance-exclusive billed object prefix")
	flag.StringVar(&allowedFormatsJSON, "allowed-formats-json", `["kubebrain.logical.v2"]`, "sorted JSON array of billed artifact formats")
	flag.StringVar(&meteringStore, "metering-object-store-id", "", "metering evidence object store identifier")
	flag.StringVar(&meteringBucket, "metering-bucket", "", "metering evidence Object Lock bucket")
	flag.StringVar(&snapshotPrefix, "snapshot-prefix", "metering-storage-samples", "immutable storage sample key prefix")
	flag.StringVar(&retentionMode, "retention-mode", "COMPLIANCE", "COMPLIANCE or GOVERNANCE")
	flag.DurationVar(&retentionDuration, "retention-duration", 7*365*24*time.Hour, "sample retention from slot end")
	flag.DurationVar(&finalizationDelay, "finalization-delay", 10*time.Minute, "delay after the complete UTC hour")
	flag.DurationVar(&timeout, "timeout", 20*time.Minute, "overall inventory and archive deadline")
	flag.Parse()
	sourceEndpoint, sourceRegion = os.Getenv("SOURCE_S3_ENDPOINT"), os.Getenv("SOURCE_AWS_REGION")
	sourceAccessKey, sourceSecretKey = os.Getenv("SOURCE_AWS_ACCESS_KEY_ID"), os.Getenv("SOURCE_AWS_SECRET_ACCESS_KEY")
	sourceSessionToken = os.Getenv("SOURCE_AWS_SESSION_TOKEN")
	sourcePathStyle, err := parseOptionalBoolEnv("SOURCE_S3_FORCE_PATH_STYLE")
	if err != nil {
		log.Fatal(err)
	}
	meteringEndpoint, meteringRegion = os.Getenv("METERING_S3_ENDPOINT"), os.Getenv("METERING_AWS_REGION")
	meteringAccessKey, meteringSecretKey = os.Getenv("METERING_AWS_ACCESS_KEY_ID"), os.Getenv("METERING_AWS_SECRET_ACCESS_KEY")
	meteringSessionToken = os.Getenv("METERING_AWS_SESSION_TOKEN")
	meteringPathStyle, err = parseOptionalBoolEnv("METERING_S3_FORCE_PATH_STYLE")
	if err != nil {
		log.Fatal(err)
	}

	allowedFormats, err := parseJSONStringArray(allowedFormatsJSON)
	if err != nil {
		log.Fatalf("allowed-formats-json must be a JSON string array: %v", err)
	}
	if instance == "" || sourceStore == "" || sourceBucket == "" || sourcePrefix == "" ||
		meteringStore == "" || meteringBucket == "" || timeout <= 0 ||
		sourceEndpoint == "" || sourceRegion == "" || sourceAccessKey == "" || sourceSecretKey == "" ||
		meteringEndpoint == "" || meteringRegion == "" || meteringAccessKey == "" || meteringSecretKey == "" {
		log.Fatal("instance, source and metering object store settings, and a positive timeout are required")
	}
	archiver := &meteringstorage.Archiver{
		Instance: instance, Executor: executor,
		SourceObjectStoreID: sourceStore, SourceBucket: sourceBucket, SourcePrefix: sourcePrefix,
		AllowedFormats: allowedFormats,
		SourceExecutorEnv: objectStoreEnvironment(sourceEndpoint, sourceRegion, sourceAccessKey,
			sourceSecretKey, sourceSessionToken, sourcePathStyle),
		MeteringObjectStoreID: meteringStore,
		MeteringBucket:        meteringBucket, SnapshotPrefix: snapshotPrefix,
		MeteringExecutorEnv: objectStoreEnvironment(meteringEndpoint, meteringRegion,
			meteringAccessKey, meteringSecretKey, meteringSessionToken, meteringPathStyle),
		RetentionMode: retentionMode, RetentionDuration: retentionDuration,
		SlotDuration: time.Hour, FinalizationDelay: finalizationDelay,
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	_, output, err := archiver.Process(ctx)
	if err != nil {
		log.Fatal(err)
	}
	if _, err := os.Stdout.Write(output); err != nil {
		log.Fatal(err)
	}
}

func parseJSONStringArray(raw string) ([]string, error) {
	var values []string
	decoder := json.NewDecoder(strings.NewReader(raw))
	if err := decoder.Decode(&values); err != nil {
		return nil, err
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		return nil, errors.New("contains trailing JSON")
	}
	if values == nil {
		return nil, errors.New("must not be null")
	}
	return values, nil
}

func objectStoreEnvironment(endpoint, region, accessKey, secretKey, token string, pathStyle bool) []string {
	return []string{
		"S3_ENDPOINT=" + endpoint, "AWS_REGION=" + region,
		"AWS_ACCESS_KEY_ID=" + accessKey, "AWS_SECRET_ACCESS_KEY=" + secretKey,
		"AWS_SESSION_TOKEN=" + token,
		"S3_FORCE_PATH_STYLE=" + strconv.FormatBool(pathStyle),
	}
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

func init() {
	log.SetFlags(0)
}
