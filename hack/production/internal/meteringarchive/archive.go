package meteringarchive

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
	"strconv"
	"strings"
	"time"

	"github.com/kubewharf/kubebrain/hack/production/internal/processgroup"
)

type CommandRunner func(context.Context, string, []string) ([]byte, error)

type Archiver struct {
	Collector         *Collector
	Instance          string
	Executor          string
	ObjectStoreID     string
	Bucket            string
	Prefix            string
	RetentionMode     string
	RetentionDuration time.Duration
	SlotDuration      time.Duration
	FinalizationDelay time.Duration
	MaxStaleness      time.Duration
	Now               func() time.Time
	Run               CommandRunner
}

func (a *Archiver) Validate() error {
	a.Prefix = strings.Trim(a.Prefix, "/")
	if a.Collector == nil || !instancePattern.MatchString(a.Instance) || a.Executor == "" ||
		a.ObjectStoreID == "" || a.Bucket == "" || a.Prefix == "" ||
		(a.RetentionMode != "COMPLIANCE" && a.RetentionMode != "GOVERNANCE") ||
		a.RetentionDuration <= 0 || a.SlotDuration < time.Minute ||
		a.FinalizationDelay < 0 || a.MaxStaleness <= 0 {
		return errors.New("metering archiver configuration is incomplete")
	}
	if a.RetentionDuration <= a.FinalizationDelay+a.SlotDuration {
		return errors.New("metering retention must exceed the slot and finalization delay")
	}
	if a.Now == nil {
		a.Now = time.Now
	}
	if a.Run == nil {
		a.Run = runCommand
	}
	return nil
}

func (a *Archiver) Process(ctx context.Context) (Sample, []byte, error) {
	if err := a.Validate(); err != nil {
		return Sample{}, nil, err
	}
	now := a.Now().UTC()
	slotEnd := now.Add(-a.FinalizationDelay).Truncate(a.SlotDuration)
	slotStart := slotEnd.Add(-a.SlotDuration)
	if slotStart.Unix() <= 0 {
		return Sample{}, nil, errors.New("metering slot is before the Unix epoch")
	}
	sample, err := a.Collector.Collect(ctx, a.Instance, slotStart, slotEnd)
	if err != nil {
		return Sample{}, nil, err
	}
	dir, err := os.MkdirTemp("", "kubebrain-metering-*")
	if err != nil {
		return Sample{}, nil, err
	}
	defer os.RemoveAll(dir)
	artifactPath := path.Join(dir, "sample.json")
	receiptPath := path.Join(dir, "receipt.json")
	status, err := WriteAtomic(artifactPath, sample, a.MaxStaleness)
	if err != nil {
		return Sample{}, nil, err
	}
	artifactID := fmt.Sprintf("%s:%d:%d", a.Instance, sample.SlotStartUnix, sample.SlotEndUnix)
	slotTime := time.Unix(sample.SlotEndUnix, 0).UTC()
	objectKey := path.Join(
		a.Prefix, a.Instance, slotTime.Format("2006/01/02"),
		fmt.Sprintf("%d-%d.json", sample.SlotStartUnix, sample.SlotEndUnix),
	)
	retainUntil := slotTime.Add(a.RetentionDuration).Unix()
	if retainUntil <= now.Unix() {
		return Sample{}, nil, errors.New("metering retention deadline is not in the future")
	}
	environment := []string{
		"ACTION=blob",
		"INPUT=" + artifactPath,
		"ARTIFACT_FORMAT=" + sample.Format,
		"ARTIFACT_ID=" + artifactID,
		"INSTANCE=" + a.Instance,
		"OBJECT_STORE_ID=" + a.ObjectStoreID,
		"S3_BUCKET=" + a.Bucket,
		"S3_OBJECT_KEY=" + objectKey,
		"RETENTION_MODE=" + a.RetentionMode,
		"RETAIN_UNTIL_UNIX=" + strconv.FormatInt(retainUntil, 10),
		"RECEIPT_OUTPUT=" + receiptPath,
	}
	output, err := a.Run(ctx, a.Executor, environment)
	if err != nil {
		return Sample{}, output, fmt.Errorf(
			"metering Object Lock executor failed: %w: %s", err, strings.TrimSpace(string(output)),
		)
	}
	if err := validateExecutorReceipt(
		output, sample.Format, artifactID, a.Instance, a.ObjectStoreID, a.Bucket, objectKey,
		a.RetentionMode, retainUntil, status.SHA256, status.Bytes,
	); err != nil {
		return Sample{}, output, err
	}
	return sample, output, nil
}

func validateExecutorReceipt(
	data []byte,
	artifactFormat, artifactID, instance, objectStoreID, bucket, objectKey, retentionMode string,
	retainUntil int64,
	artifactSHA256 string,
	objectBytes int64,
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
		return fmt.Errorf("decode metering archive receipt: %w", err)
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		return errors.New("metering archive receipt contains trailing JSON")
	}
	if receipt.Format != "kubebrain.object-immutable-blob.receipt.v1" ||
		receipt.ArtifactFormat != artifactFormat || receipt.ArtifactID != artifactID ||
		receipt.Instance != instance || receipt.ObjectStoreID != objectStoreID ||
		receipt.Bucket != bucket || receipt.ObjectKey != objectKey || receipt.VersionID == "" ||
		receipt.ArtifactSHA256 != artifactSHA256 || receipt.ObjectBytes != objectBytes ||
		receipt.RetentionMode != retentionMode || receipt.RetainUntilUnix != retainUntil ||
		!receipt.RemoteVerified || receipt.ArchivedAtUnix <= 0 ||
		receipt.ArchivedAtUnix >= receipt.RetainUntilUnix {
		return errors.New("metering archive receipt does not match the sample")
	}
	return nil
}

func runCommand(ctx context.Context, executable string, environment []string) ([]byte, error) {
	command := exec.CommandContext(ctx, executable)
	command.Env = mergeEnvironment(os.Environ(), environment)
	processgroup.Configure(command)
	command.WaitDelay = 5 * time.Second
	return processgroup.CombinedOutput(command, processgroup.DefaultOutputLimitBytes)
}

func mergeEnvironment(base, overrides []string) []string {
	values := make(map[string]string, len(base)+len(overrides))
	order := make([]string, 0, len(base)+len(overrides))
	add := func(entry string) {
		key, _, found := strings.Cut(entry, "=")
		if !found || key == "" {
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
