package meteringbilling

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
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

	"github.com/kubewharf/kubebrain/hack/production/internal/meteringarchive"
	"github.com/kubewharf/kubebrain/hack/production/internal/meteringstorage"
	"github.com/kubewharf/kubebrain/hack/production/internal/processgroup"
)

type CommandRunner func(context.Context, string, []string) ([]byte, error)

type Biller struct {
	Instance            string
	PriceScope          string
	PriceVersion        string
	PriceCatalogFormat  string
	Executor            string
	ObjectStoreID       string
	Bucket              string
	RollupPrefix        string
	StorageRollupPrefix string
	PricePrefix         string
	ChargePrefix        string
	RetentionMode       string
	RetentionDuration   time.Duration
	PeriodDuration      time.Duration
	FinalizationDelay   time.Duration
	PeriodEnd           time.Time
	Now                 func() time.Time
	Run                 CommandRunner
}

func (b *Biller) Validate() error {
	b.RollupPrefix = strings.Trim(b.RollupPrefix, "/")
	b.StorageRollupPrefix = strings.Trim(b.StorageRollupPrefix, "/")
	b.PricePrefix = strings.Trim(b.PricePrefix, "/")
	b.ChargePrefix = strings.Trim(b.ChargePrefix, "/")
	if b.PriceCatalogFormat == "" {
		b.PriceCatalogFormat = CatalogFormat
	}
	if !versionPattern.MatchString(b.Instance) || !versionPattern.MatchString(b.PriceScope) ||
		!versionPattern.MatchString(b.PriceVersion) || b.Executor == "" || b.ObjectStoreID == "" ||
		b.Bucket == "" || b.RollupPrefix == "" || b.PricePrefix == "" || b.ChargePrefix == "" ||
		b.RollupPrefix == b.PricePrefix || b.RollupPrefix == b.ChargePrefix ||
		b.PricePrefix == b.ChargePrefix ||
		(b.PriceCatalogFormat != CatalogFormat && b.PriceCatalogFormat != CatalogFormatV2 &&
			b.PriceCatalogFormat != CatalogFormatV3) ||
		((b.PriceCatalogFormat == CatalogFormatV2 || b.PriceCatalogFormat == CatalogFormatV3) &&
			(b.StorageRollupPrefix == "" ||
				b.StorageRollupPrefix == b.RollupPrefix || b.StorageRollupPrefix == b.PricePrefix ||
				b.StorageRollupPrefix == b.ChargePrefix)) ||
		(b.RetentionMode != "COMPLIANCE" && b.RetentionMode != "GOVERNANCE") ||
		b.RetentionDuration <= 0 || b.PeriodDuration != 24*time.Hour ||
		b.FinalizationDelay < 0 || b.RetentionDuration <= b.FinalizationDelay+b.PeriodDuration {
		return errors.New("metering biller configuration is incomplete")
	}
	if b.Now == nil {
		b.Now = time.Now
	}
	if b.Run == nil {
		b.Run = runCommand
	}
	return nil
}

func (b *Biller) Process(ctx context.Context) (Charge, []byte, error) {
	if err := b.Validate(); err != nil {
		return Charge{}, nil, err
	}
	now := b.Now().UTC()
	eligibleEnd := now.Add(-b.FinalizationDelay).Truncate(b.PeriodDuration)
	periodEnd := eligibleEnd
	if !b.PeriodEnd.IsZero() {
		periodEnd = b.PeriodEnd.UTC()
		if periodEnd.Unix()%int64(b.PeriodDuration/time.Second) != 0 ||
			periodEnd.After(eligibleEnd) {
			return Charge{}, nil, errors.New("explicit charge period end is not eligible")
		}
	}
	periodStart := periodEnd.Add(-b.PeriodDuration)
	if periodStart.Unix() <= 0 {
		return Charge{}, nil, errors.New("charge period is before the Unix epoch")
	}
	retainUntil := periodStart.Add(time.Hour).Add(b.RetentionDuration).Unix()
	if retainUntil <= now.Unix() {
		return Charge{}, nil, errors.New("charge retention deadline is not in the future")
	}
	dir, err := os.MkdirTemp("", "kubebrain-metering-charge-*")
	if err != nil {
		return Charge{}, nil, err
	}
	defer os.RemoveAll(dir)

	rollupArtifactID := periodArtifactID(b.Instance, periodStart, periodEnd)
	rollupKey := path.Join(
		b.RollupPrefix, b.Instance, periodStart.Format("2006/01/02"),
		fmt.Sprintf("%d-%d.json", periodStart.Unix(), periodEnd.Unix()),
	)
	rollupPath := path.Join(dir, "rollup.json")
	rollupSource, output, err := b.readImmutable(
		ctx, map[string]string{
			CatalogFormat:   meteringarchive.RollupFormat,
			CatalogFormatV2: meteringarchive.RollupFormat,
			CatalogFormatV3: meteringarchive.RollupFormatV3,
		}[b.PriceCatalogFormat], rollupArtifactID, b.Instance,
		rollupKey, rollupPath, retainUntil,
	)
	if err != nil {
		return Charge{}, output, fmt.Errorf("read metering rollup: %w", err)
	}
	rollup, err := meteringarchive.ReadRollup(rollupPath)
	if err != nil {
		return Charge{}, output, err
	}
	if rollup.Instance != b.Instance || rollup.PeriodStartUnix != periodStart.Unix() ||
		rollup.PeriodEndUnix != periodEnd.Unix() {
		return Charge{}, output, errors.New("metering rollup does not match the charge period")
	}

	catalogKey := path.Join(b.PricePrefix, b.PriceScope, b.PriceVersion+".json")
	catalogPath := path.Join(dir, "catalog.json")
	catalogSource, output, err := b.readImmutable(
		ctx, b.PriceCatalogFormat, b.PriceVersion, b.PriceScope,
		catalogKey, catalogPath, retainUntil,
	)
	if err != nil {
		return Charge{}, output, fmt.Errorf("read metering price catalog: %w", err)
	}
	catalogStatus, err := ReadCatalog(catalogPath)
	if err != nil {
		return Charge{}, output, err
	}
	var charge Charge
	if catalogStatus.Catalog.Format == CatalogFormatV2 ||
		catalogStatus.Catalog.Format == CatalogFormatV3 {
		storageKey := path.Join(
			b.StorageRollupPrefix, b.Instance, periodStart.Format("2006/01/02"),
			fmt.Sprintf("%d-%d.json", periodStart.Unix(), periodEnd.Unix()),
		)
		storagePath := path.Join(dir, "storage-rollup.json")
		storageSource, storageOutput, readErr := b.readImmutable(
			ctx, meteringstorage.RollupFormat, rollupArtifactID, b.Instance,
			storageKey, storagePath, retainUntil,
		)
		if readErr != nil {
			return Charge{}, storageOutput, fmt.Errorf("read object storage rollup: %w", readErr)
		}
		storageStatus, readErr := meteringstorage.ReadRollup(storagePath)
		if readErr != nil {
			return Charge{}, storageOutput, readErr
		}
		if catalogStatus.Catalog.Format == CatalogFormatV3 {
			charge, err = BuildChargeV3(
				rollup, rollupSource, storageStatus.Rollup, storageSource,
				catalogStatus.Catalog, catalogSource,
			)
		} else {
			charge, err = BuildChargeV2(
				rollup, rollupSource, storageStatus.Rollup, storageSource,
				catalogStatus.Catalog, catalogSource,
			)
		}
	} else {
		charge, err = BuildCharge(rollup, rollupSource, catalogStatus.Catalog, catalogSource)
	}
	if err != nil {
		return Charge{}, output, err
	}
	chargePath := path.Join(dir, "charge.json")
	receiptPath := path.Join(dir, "receipt.json")
	status, err := WriteChargeAtomic(chargePath, charge)
	if err != nil {
		return Charge{}, nil, err
	}
	chargeArtifactID := periodArtifactID(b.Instance, periodStart, periodEnd)
	chargeKey := path.Join(
		b.ChargePrefix, b.Instance, periodStart.Format("2006/01/02"),
		fmt.Sprintf("%d-%d.json", periodStart.Unix(), periodEnd.Unix()),
	)
	output, err = b.Run(ctx, b.Executor, []string{
		"ACTION=blob",
		"INPUT=" + chargePath,
		"ARTIFACT_FORMAT=" + charge.Format,
		"ARTIFACT_ID=" + chargeArtifactID,
		"INSTANCE=" + b.Instance,
		"OBJECT_STORE_ID=" + b.ObjectStoreID,
		"S3_BUCKET=" + b.Bucket,
		"S3_OBJECT_KEY=" + chargeKey,
		"RETENTION_MODE=" + b.RetentionMode,
		"RETAIN_UNTIL_UNIX=" + strconv.FormatInt(retainUntil, 10),
		"RECEIPT_OUTPUT=" + receiptPath,
	})
	if err != nil {
		return Charge{}, output, fmt.Errorf(
			"metering charge Object Lock executor failed: %w: %s",
			err, strings.TrimSpace(string(output)),
		)
	}
	if _, err := parseBlobReceipt(
		output, charge.Format, chargeArtifactID, b.Instance, b.ObjectStoreID,
		b.Bucket, chargeKey, retainUntil, status.SHA256, status.Bytes,
	); err != nil {
		return Charge{}, output, err
	}
	return charge, output, nil
}

func (b *Biller) readImmutable(
	ctx context.Context,
	format, artifactID, objectInstance, objectKey, outputPath string,
	minRetainUntil int64,
) (Source, []byte, error) {
	output, err := b.Run(ctx, b.Executor, []string{
		"ACTION=blob-read",
		"OUTPUT=" + outputPath,
		"ARTIFACT_FORMAT=" + format,
		"ARTIFACT_ID=" + artifactID,
		"INSTANCE=" + objectInstance,
		"OBJECT_STORE_ID=" + b.ObjectStoreID,
		"S3_BUCKET=" + b.Bucket,
		"S3_OBJECT_KEY=" + objectKey,
		"MIN_RETAIN_UNTIL_UNIX=" + strconv.FormatInt(minRetainUntil, 10),
	})
	if err != nil {
		return Source{}, output, fmt.Errorf("%w: %s", err, strings.TrimSpace(string(output)))
	}
	receipt, err := parseBlobReceipt(
		output, format, artifactID, objectInstance, b.ObjectStoreID,
		b.Bucket, objectKey, minRetainUntil, "", 0,
	)
	if err != nil {
		return Source{}, output, err
	}
	data, err := os.ReadFile(outputPath)
	if err != nil {
		return Source{}, output, err
	}
	sum := sha256.Sum256(data)
	if int64(len(data)) != receipt.ObjectBytes ||
		hex.EncodeToString(sum[:]) != receipt.ArtifactSHA256 {
		return Source{}, output, errors.New("immutable read receipt does not match downloaded bytes")
	}
	return Source{
		ArtifactFormat: receipt.ArtifactFormat, ArtifactID: receipt.ArtifactID,
		ObjectKey: receipt.ObjectKey, VersionID: receipt.VersionID,
		ArtifactSHA256: receipt.ArtifactSHA256, ObjectBytes: receipt.ObjectBytes,
		RetainUntilUnix: receipt.RetainUntilUnix,
	}, output, nil
}

type objectReceipt struct {
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
	ArchivedAtUnix  int64  `json:"archived_at_unix,omitempty"`
}

func parseBlobReceipt(
	data []byte,
	artifactFormat, artifactID, instance, objectStoreID, bucket, objectKey string,
	minRetainUntil int64,
	expectedSHA string,
	expectedBytes int64,
) (objectReceipt, error) {
	var receipt objectReceipt
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&receipt); err != nil {
		return receipt, fmt.Errorf("decode immutable object receipt: %w", err)
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		return receipt, errors.New("immutable object receipt contains trailing JSON")
	}
	expectedReceiptFormat := "kubebrain.object-immutable-blob-read.receipt.v1"
	if expectedSHA != "" {
		expectedReceiptFormat = "kubebrain.object-immutable-blob.receipt.v1"
	}
	if receipt.Format != expectedReceiptFormat ||
		receipt.ArtifactFormat != artifactFormat || receipt.ArtifactID != artifactID ||
		receipt.Instance != instance || receipt.ObjectStoreID != objectStoreID ||
		receipt.Bucket != bucket || receipt.ObjectKey != objectKey || receipt.VersionID == "" ||
		!digestPattern.MatchString(receipt.ArtifactSHA256) || receipt.ObjectBytes <= 0 ||
		(receipt.RetentionMode != "COMPLIANCE" && receipt.RetentionMode != "GOVERNANCE") ||
		receipt.RetainUntilUnix < minRetainUntil || !receipt.RemoteVerified {
		return receipt, errors.New("immutable object receipt does not match request")
	}
	if expectedSHA != "" &&
		(receipt.ArtifactSHA256 != expectedSHA || receipt.ObjectBytes != expectedBytes ||
			receipt.RetainUntilUnix != minRetainUntil || receipt.ArchivedAtUnix <= 0 ||
			receipt.ArchivedAtUnix >= receipt.RetainUntilUnix) {
		return receipt, errors.New("immutable archive receipt does not match artifact")
	}
	return receipt, nil
}

func periodArtifactID(instance string, start, end time.Time) string {
	return fmt.Sprintf("%s:%d:%d", instance, start.Unix(), end.Unix())
}

func runCommand(ctx context.Context, executable string, environment []string) ([]byte, error) {
	command := exec.CommandContext(ctx, executable)
	command.Env = mergeEnvironment(os.Environ(), environment)
	processgroup.Configure(command)
	command.WaitDelay = 5 * time.Second
	return command.CombinedOutput()
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
