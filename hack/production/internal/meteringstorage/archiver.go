package meteringstorage

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/kubewharf/kubebrain/hack/production/internal/processgroup"
)

type CommandRunner func(context.Context, string, []string) ([]byte, error)

type Archiver struct {
	Instance              string
	Executor              string
	SourceObjectStoreID   string
	SourceBucket          string
	SourcePrefix          string
	AllowedFormats        []string
	SourceExecutorEnv     []string
	MeteringObjectStoreID string
	MeteringBucket        string
	MeteringExecutorEnv   []string
	SnapshotPrefix        string
	RetentionMode         string
	RetentionDuration     time.Duration
	SlotDuration          time.Duration
	FinalizationDelay     time.Duration
	Now                   func() time.Time
	Run                   CommandRunner
}

func (a *Archiver) Validate() error {
	a.SourcePrefix = strings.Trim(a.SourcePrefix, "/")
	a.SnapshotPrefix = strings.Trim(a.SnapshotPrefix, "/")
	if !identifierPattern.MatchString(a.Instance) || a.Executor == "" ||
		a.SourceObjectStoreID == "" || a.MeteringObjectStoreID == "" ||
		a.SourceBucket == "" || a.SourcePrefix == "" || a.MeteringBucket == "" ||
		a.SnapshotPrefix == "" || len(a.AllowedFormats) == 0 ||
		(a.RetentionMode != "COMPLIANCE" && a.RetentionMode != "GOVERNANCE") ||
		a.RetentionDuration <= 0 || a.SlotDuration != time.Hour ||
		a.FinalizationDelay <= 0 || a.FinalizationDelay >= a.SlotDuration ||
		a.RetentionDuration <= a.SlotDuration+a.FinalizationDelay {
		return errors.New("object storage sample archiver configuration is incomplete")
	}
	for i, format := range a.AllowedFormats {
		if format == "" || (i > 0 && format <= a.AllowedFormats[i-1]) {
			return errors.New("object storage sample formats must be unique and sorted")
		}
	}
	if a.Now == nil {
		a.Now = time.Now
	}
	if a.Run == nil {
		a.Run = runCommand
	}
	return nil
}

func (a *Archiver) Process(ctx context.Context) (Snapshot, []byte, error) {
	if err := a.Validate(); err != nil {
		return Snapshot{}, nil, err
	}
	now := a.Now().UTC()
	slotEnd := now.Add(-a.FinalizationDelay).Truncate(a.SlotDuration)
	slotStart := slotEnd.Add(-a.SlotDuration)
	if slotStart.Unix() <= 0 {
		return Snapshot{}, nil, errors.New("object storage sample slot is before the Unix epoch")
	}
	retainUntil := slotEnd.Add(a.RetentionDuration).Unix()
	if retainUntil <= now.Unix() {
		return Snapshot{}, nil, errors.New("object storage sample retention is not in the future")
	}
	dir, err := os.MkdirTemp("", "kubebrain-object-storage-sample-*")
	if err != nil {
		return Snapshot{}, nil, err
	}
	defer os.RemoveAll(dir)
	usagePath := path.Join(dir, "usage.json")
	allowedJSON, err := json.Marshal(a.AllowedFormats)
	if err != nil {
		return Snapshot{}, nil, err
	}
	output, err := a.Run(ctx, a.Executor, append([]string{
		"ACTION=usage",
		"OBJECT_STORE_ID=" + a.SourceObjectStoreID,
		"S3_BUCKET=" + a.SourceBucket,
		"USAGE_PREFIX=" + a.SourcePrefix + "/",
		"ALLOWED_FORMATS_JSON=" + string(allowedJSON),
		"RECEIPT_OUTPUT=" + usagePath,
	}, a.SourceExecutorEnv...))
	if err != nil {
		return Snapshot{}, output, fmt.Errorf(
			"object storage usage executor failed: %w: %s", err, strings.TrimSpace(string(output)),
		)
	}
	usage, err := readUsageReceipt(usagePath)
	if err != nil {
		return Snapshot{}, output, err
	}
	var stdout UsageReceipt
	if err := json.Unmarshal(bytes.TrimSpace(output), &stdout); err != nil ||
		!usageReceiptsEqual(stdout, usage) {
		return Snapshot{}, output, errors.New("object storage usage stdout does not match receipt")
	}
	if usage.ObjectStoreID != a.SourceObjectStoreID || usage.Bucket != a.SourceBucket ||
		usage.Prefix != a.SourcePrefix+"/" ||
		!equalStrings(usage.AllowedFormats, a.AllowedFormats) ||
		usage.CheckedAtUnix < slotEnd.Unix() ||
		usage.CheckedAtUnix-slotEnd.Unix() > int64(a.FinalizationDelay/time.Second) {
		return Snapshot{}, output, errors.New("object storage usage receipt does not match sample slot")
	}
	snapshot, err := BuildSnapshot(a.Instance, slotStart.Unix(), slotEnd.Unix(), usage)
	if err != nil {
		return Snapshot{}, output, err
	}
	snapshotPath := path.Join(dir, "snapshot.json")
	status, err := WriteSnapshotAtomic(snapshotPath, snapshot)
	if err != nil {
		return Snapshot{}, output, err
	}
	receiptPath := path.Join(dir, "archive-receipt.json")
	artifactID := fmt.Sprintf("%s:%d:%d", a.Instance, slotStart.Unix(), slotEnd.Unix())
	objectKey := path.Join(
		a.SnapshotPrefix, a.Instance, slotStart.Format("2006/01/02/15"),
		fmt.Sprintf("%d-%d.json", slotStart.Unix(), slotEnd.Unix()),
	)
	output, err = a.Run(ctx, a.Executor, append([]string{
		"ACTION=blob",
		"INPUT=" + snapshotPath,
		"ARTIFACT_FORMAT=" + SnapshotFormat,
		"ARTIFACT_ID=" + artifactID,
		"INSTANCE=" + a.Instance,
		"OBJECT_STORE_ID=" + a.MeteringObjectStoreID,
		"S3_BUCKET=" + a.MeteringBucket,
		"S3_OBJECT_KEY=" + objectKey,
		"RETENTION_MODE=" + a.RetentionMode,
		"RETAIN_UNTIL_UNIX=" + strconv.FormatInt(retainUntil, 10),
		"RECEIPT_OUTPUT=" + receiptPath,
	}, a.MeteringExecutorEnv...))
	if err != nil {
		return Snapshot{}, output, fmt.Errorf(
			"object storage sample archive failed: %w: %s", err, strings.TrimSpace(string(output)),
		)
	}
	if err := validateBlobReceipt(
		output, artifactID, a.Instance, a.MeteringObjectStoreID, a.MeteringBucket,
		objectKey, retainUntil, status,
	); err != nil {
		return Snapshot{}, output, err
	}
	return snapshot, output, nil
}

func readUsageReceipt(path string) (UsageReceipt, error) {
	var receipt UsageReceipt
	data, err := os.ReadFile(path)
	if err != nil {
		return receipt, err
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&receipt); err != nil {
		return receipt, err
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		return receipt, errors.New("object usage receipt contains trailing JSON")
	}
	if receipt.Format != UsageReceiptFormat ||
		receipt.ObjectStoreID == "" || receipt.Bucket == "" || receipt.Prefix == "" ||
		len(receipt.AllowedFormats) == 0 || receipt.RemoteVersions < 0 ||
		receipt.DeleteMarkers != 0 || receipt.TotalObjectBytes < 0 ||
		!digestPattern.MatchString(receipt.VersionsSHA256) || receipt.CheckedAtUnix <= 0 {
		return receipt, errors.New("object usage receipt is incomplete")
	}
	canonical, err := json.Marshal(receipt)
	if err != nil {
		return receipt, err
	}
	canonical = append(canonical, '\n')
	if !bytes.Equal(data, canonical) {
		return receipt, errors.New("object usage receipt is not canonical")
	}
	return receipt, nil
}

func usageReceiptsEqual(left, right UsageReceipt) bool {
	return reflect.DeepEqual(left, right)
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

func validateBlobReceipt(
	data []byte,
	artifactID, instance, objectStoreID, bucket, objectKey string,
	retainUntil int64,
	status SnapshotStatus,
) error {
	var receipt struct {
		Format          string `json:"format"`
		ArtifactFormat  string `json:"artifact_format"`
		ArtifactID      string `json:"artifact_id"`
		Instance        string `json:"instance"`
		ObjectStoreID   string `json:"object_store_id"`
		Bucket          string `json:"bucket"`
		ObjectKey       string `json:"object_key"`
		VersionID       string `json:"version_id"`
		ArtifactSHA256  string `json:"artifact_sha256"`
		ObjectBytes     int64  `json:"object_bytes"`
		RetentionMode   string `json:"retention_mode"`
		RetainUntilUnix int64  `json:"retain_until_unix"`
		RemoteVerified  bool   `json:"remote_verified"`
		ArchivedAtUnix  int64  `json:"archived_at_unix"`
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&receipt); err != nil {
		return fmt.Errorf("decode object storage sample archive receipt: %w", err)
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		return errors.New("object storage sample archive receipt contains trailing JSON")
	}
	if receipt.Format != "kubebrain.object-immutable-blob.receipt.v1" ||
		receipt.ArtifactFormat != SnapshotFormat || receipt.ArtifactID != artifactID ||
		receipt.Instance != instance || receipt.ObjectStoreID != objectStoreID ||
		receipt.Bucket != bucket || receipt.ObjectKey != objectKey || receipt.VersionID == "" ||
		receipt.ArtifactSHA256 != status.SHA256 || receipt.ObjectBytes != status.Bytes ||
		(receipt.RetentionMode != "COMPLIANCE" && receipt.RetentionMode != "GOVERNANCE") ||
		receipt.RetainUntilUnix != retainUntil || !receipt.RemoteVerified ||
		receipt.ArchivedAtUnix <= 0 || receipt.ArchivedAtUnix >= retainUntil {
		return errors.New("object storage sample archive receipt does not match artifact")
	}
	return nil
}

type blobReceipt struct {
	Format          string `json:"format"`
	ArtifactFormat  string `json:"artifact_format"`
	ArtifactID      string `json:"artifact_id"`
	Instance        string `json:"instance"`
	ObjectStoreID   string `json:"object_store_id"`
	Bucket          string `json:"bucket"`
	ObjectKey       string `json:"object_key"`
	VersionID       string `json:"version_id"`
	ArtifactSHA256  string `json:"artifact_sha256"`
	ObjectBytes     int64  `json:"object_bytes"`
	RetentionMode   string `json:"retention_mode"`
	RetainUntilUnix int64  `json:"retain_until_unix"`
	RemoteVerified  bool   `json:"remote_verified"`
	ArchivedAtUnix  int64  `json:"archived_at_unix"`
}

func runCommand(ctx context.Context, executable string, environment []string) ([]byte, error) {
	command := exec.CommandContext(ctx, executable)
	command.Env = mergeExecutorEnvironment(os.Environ(), environment)
	processgroup.Configure(command)
	command.WaitDelay = 5 * time.Second
	output, err := command.CombinedOutput()
	if ctx.Err() != nil {
		return output, ctx.Err()
	}
	return output, err
}

func mergeExecutorEnvironment(base, overrides []string) []string {
	values := make(map[string]string, len(base)+len(overrides))
	order := make([]string, 0, len(base)+len(overrides))
	add := func(entry string) {
		key, _, ok := strings.Cut(entry, "=")
		if !ok || key == "" {
			return
		}
		if _, exists := values[key]; !exists {
			order = append(order, key)
		}
		values[key] = entry
	}
	for _, entry := range base {
		add(entry)
	}
	for _, entry := range overrides {
		add(entry)
	}
	result := make([]string, 0, len(order))
	for _, key := range order {
		result = append(result, values[key])
	}
	return result
}
