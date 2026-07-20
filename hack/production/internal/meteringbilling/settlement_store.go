package meteringbilling

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path"
	"strconv"
	"strings"
	"time"
)

type SettlementPublisher struct {
	Input             string
	Kind              string
	Executor          string
	ObjectStoreID     string
	Bucket            string
	AdjustmentPrefix  string
	PlanPrefix        string
	RetentionMode     string
	RetentionDuration time.Duration
	Now               func() time.Time
	Run               CommandRunner
}

func (p *SettlementPublisher) Publish(ctx context.Context) ([]byte, error) {
	p.AdjustmentPrefix = strings.Trim(p.AdjustmentPrefix, "/")
	p.PlanPrefix = strings.Trim(p.PlanPrefix, "/")
	if p.Input == "" || p.Executor == "" || p.ObjectStoreID == "" || p.Bucket == "" ||
		(p.RetentionMode != "COMPLIANCE" && p.RetentionMode != "GOVERNANCE") ||
		p.RetentionDuration <= 0 {
		return nil, errors.New("settlement publisher configuration is incomplete")
	}
	if p.Now == nil {
		p.Now = time.Now
	}
	if p.Run == nil {
		p.Run = runCommand
	}
	var format, artifactID, instance, objectKey, digest string
	var objectBytes, periodStart, approvedAt int64
	switch p.Kind {
	case "adjustment":
		if p.AdjustmentPrefix == "" {
			return nil, errors.New("adjustment prefix is empty")
		}
		status, err := ReadAdjustment(p.Input)
		if err != nil {
			return nil, err
		}
		format, artifactID, instance = AdjustmentFormat, status.Value.ID, status.Value.Instance
		objectKey = settlementObjectKey(p.AdjustmentPrefix, instance, artifactID)
		digest, objectBytes, periodStart = status.SHA256, status.Bytes, status.Value.PeriodStartUnix
		approvedAt = status.Value.Approval.ApprovedAtUnix
	case "plan":
		if p.PlanPrefix == "" {
			return nil, errors.New("invoice plan prefix is empty")
		}
		status, err := ReadInvoicePlan(p.Input)
		if err != nil {
			return nil, err
		}
		format, artifactID, instance = InvoicePlanFormat, status.Value.ID, status.Value.Instance
		objectKey = settlementObjectKey(p.PlanPrefix, instance, artifactID)
		digest, objectBytes, periodStart = status.SHA256, status.Bytes, status.Value.PeriodStartUnix
		approvedAt = status.Value.Approval.ApprovedAtUnix
	default:
		return nil, errors.New("settlement publisher kind must be adjustment or plan")
	}
	retainUntil := time.Unix(periodStart, 0).Add(time.Hour).Add(p.RetentionDuration).Unix()
	nowUnix := p.Now().UTC().Unix()
	if approvedAt > nowUnix {
		return nil, errors.New("settlement approval timestamp is in the future")
	}
	if retainUntil <= nowUnix {
		return nil, errors.New("settlement artifact retention is not in the future")
	}
	dir, err := os.MkdirTemp("", "kubebrain-settlement-publish-*")
	if err != nil {
		return nil, err
	}
	defer os.RemoveAll(dir)
	output, err := p.Run(ctx, p.Executor, []string{
		"ACTION=blob", "INPUT=" + p.Input, "ARTIFACT_FORMAT=" + format,
		"ARTIFACT_ID=" + artifactID, "INSTANCE=" + instance,
		"OBJECT_STORE_ID=" + p.ObjectStoreID, "S3_BUCKET=" + p.Bucket,
		"S3_OBJECT_KEY=" + objectKey, "RETENTION_MODE=" + p.RetentionMode,
		"RETAIN_UNTIL_UNIX=" + strconv.FormatInt(retainUntil, 10),
		"RECEIPT_OUTPUT=" + path.Join(dir, "receipt.json"),
	})
	if err != nil {
		return output, fmt.Errorf(
			"settlement Object Lock publisher failed: %w: %s",
			err, strings.TrimSpace(string(output)),
		)
	}
	if _, err := parseBlobReceipt(
		output, format, artifactID, instance, p.ObjectStoreID, p.Bucket, objectKey,
		retainUntil, digest, objectBytes,
	); err != nil {
		return output, err
	}
	return output, nil
}

type InvoiceFinalizer struct {
	Instance          string
	PlanID            string
	Executor          string
	ObjectStoreID     string
	Bucket            string
	ChargePrefix      string
	AdjustmentPrefix  string
	PlanPrefix        string
	InvoicePrefix     string
	RetentionMode     string
	RetentionDuration time.Duration
	Now               func() time.Time
	Run               CommandRunner
}

func (f *InvoiceFinalizer) Validate() error {
	f.ChargePrefix = strings.Trim(f.ChargePrefix, "/")
	f.AdjustmentPrefix = strings.Trim(f.AdjustmentPrefix, "/")
	f.PlanPrefix = strings.Trim(f.PlanPrefix, "/")
	f.InvoicePrefix = strings.Trim(f.InvoicePrefix, "/")
	if !versionPattern.MatchString(f.Instance) || !versionPattern.MatchString(f.PlanID) ||
		f.Executor == "" || f.ObjectStoreID == "" || f.Bucket == "" ||
		f.ChargePrefix == "" || f.AdjustmentPrefix == "" || f.PlanPrefix == "" ||
		f.InvoicePrefix == "" || hasDuplicateString(
		f.ChargePrefix, f.AdjustmentPrefix, f.PlanPrefix, f.InvoicePrefix,
	) || (f.RetentionMode != "COMPLIANCE" && f.RetentionMode != "GOVERNANCE") ||
		f.RetentionDuration <= 24*time.Hour {
		return errors.New("invoice finalizer configuration is incomplete")
	}
	if f.Now == nil {
		f.Now = time.Now
	}
	if f.Run == nil {
		f.Run = runCommand
	}
	return nil
}

func (f *InvoiceFinalizer) Process(ctx context.Context) (Invoice, []byte, error) {
	if err := f.Validate(); err != nil {
		return Invoice{}, nil, err
	}
	dir, err := os.MkdirTemp("", "kubebrain-invoice-finalize-*")
	if err != nil {
		return Invoice{}, nil, err
	}
	defer os.RemoveAll(dir)
	planPath := path.Join(dir, "plan.json")
	planKey := settlementObjectKey(f.PlanPrefix, f.Instance, f.PlanID)
	planSource, output, err := f.readImmutable(
		ctx, InvoicePlanFormat, f.PlanID, planKey, planPath, 1,
	)
	if err != nil {
		return Invoice{}, output, fmt.Errorf("read invoice plan: %w", err)
	}
	planStatus, err := ReadInvoicePlan(planPath)
	if err != nil {
		return Invoice{}, output, err
	}
	plan := planStatus.Value
	if plan.ID != f.PlanID || plan.Instance != f.Instance {
		return Invoice{}, output, errors.New("invoice plan identity does not match request")
	}
	if plan.Approval.ApprovedAtUnix > f.Now().UTC().Unix() {
		return Invoice{}, output, errors.New("invoice plan approval timestamp is in the future")
	}
	retainUntil := time.Unix(plan.PeriodStartUnix, 0).
		Add(time.Hour).Add(f.RetentionDuration).Unix()
	if retainUntil <= f.Now().UTC().Unix() || planSource.RetainUntilUnix < retainUntil {
		return Invoice{}, output, errors.New("invoice evidence retention is insufficient")
	}
	charges := make([]Charge, len(plan.Charges))
	chargeSources := make([]Source, len(plan.Charges))
	for index, planned := range plan.Charges {
		artifactID := periodArtifactIDUnix(
			plan.Instance, planned.PeriodStartUnix, planned.PeriodEndUnix,
		)
		start := time.Unix(planned.PeriodStartUnix, 0).UTC()
		key := path.Join(
			f.ChargePrefix, plan.Instance, start.Format("2006/01/02"),
			fmt.Sprintf("%d-%d.json", planned.PeriodStartUnix, planned.PeriodEndUnix),
		)
		outputPath := path.Join(dir, fmt.Sprintf("charge-%04d.json", index))
		source, readOutput, readErr := f.readImmutable(
			ctx, planned.ArtifactFormat, artifactID, key, outputPath, retainUntil,
		)
		if readErr != nil {
			return Invoice{}, readOutput, fmt.Errorf("read invoice charge %d: %w", index, readErr)
		}
		status, readErr := ReadCharge(outputPath)
		if readErr != nil {
			return Invoice{}, readOutput, readErr
		}
		charges[index], chargeSources[index] = status.Charge, source
	}
	adjustments := make([]Adjustment, len(plan.AdjustmentIDs))
	adjustmentSources := make([]Source, len(plan.AdjustmentIDs))
	for index, id := range plan.AdjustmentIDs {
		key := settlementObjectKey(f.AdjustmentPrefix, plan.Instance, id)
		outputPath := path.Join(dir, fmt.Sprintf("adjustment-%04d.json", index))
		source, readOutput, readErr := f.readImmutable(
			ctx, AdjustmentFormat, id, key, outputPath, retainUntil,
		)
		if readErr != nil {
			return Invoice{}, readOutput, fmt.Errorf(
				"read invoice adjustment %d: %w", index, readErr,
			)
		}
		status, readErr := ReadAdjustment(outputPath)
		if readErr != nil {
			return Invoice{}, readOutput, readErr
		}
		adjustments[index], adjustmentSources[index] = status.Value, source
	}
	invoice, err := BuildInvoice(
		plan, planSource, charges, chargeSources, adjustments, adjustmentSources,
		plan.Approval.ApprovedAtUnix,
	)
	if err != nil {
		return Invoice{}, output, err
	}
	invoicePath := path.Join(dir, "invoice.json")
	status, err := WriteInvoiceAtomic(invoicePath, invoice)
	if err != nil {
		return Invoice{}, nil, err
	}
	invoiceKey := settlementObjectKey(f.InvoicePrefix, f.Instance, f.PlanID)
	output, err = f.Run(ctx, f.Executor, []string{
		"ACTION=blob", "INPUT=" + invoicePath, "ARTIFACT_FORMAT=" + InvoiceFormat,
		"ARTIFACT_ID=" + f.PlanID, "INSTANCE=" + f.Instance,
		"OBJECT_STORE_ID=" + f.ObjectStoreID, "S3_BUCKET=" + f.Bucket,
		"S3_OBJECT_KEY=" + invoiceKey, "RETENTION_MODE=" + f.RetentionMode,
		"RETAIN_UNTIL_UNIX=" + strconv.FormatInt(retainUntil, 10),
		"RECEIPT_OUTPUT=" + path.Join(dir, "invoice.receipt.json"),
	})
	if err != nil {
		return Invoice{}, output, fmt.Errorf(
			"invoice Object Lock archive failed: %w: %s",
			err, strings.TrimSpace(string(output)),
		)
	}
	if _, err := parseBlobReceipt(
		output, InvoiceFormat, f.PlanID, f.Instance, f.ObjectStoreID, f.Bucket,
		invoiceKey, retainUntil, status.SHA256, status.Bytes,
	); err != nil {
		return Invoice{}, output, err
	}
	return invoice, output, nil
}

func (f *InvoiceFinalizer) readImmutable(
	ctx context.Context,
	format, artifactID, objectKey, outputPath string,
	minRetainUntil int64,
) (Source, []byte, error) {
	output, err := f.Run(ctx, f.Executor, []string{
		"ACTION=blob-read", "OUTPUT=" + outputPath, "ARTIFACT_FORMAT=" + format,
		"ARTIFACT_ID=" + artifactID, "INSTANCE=" + f.Instance,
		"OBJECT_STORE_ID=" + f.ObjectStoreID, "S3_BUCKET=" + f.Bucket,
		"S3_OBJECT_KEY=" + objectKey,
		"MIN_RETAIN_UNTIL_UNIX=" + strconv.FormatInt(minRetainUntil, 10),
	})
	if err != nil {
		return Source{}, output, fmt.Errorf("%w: %s", err, strings.TrimSpace(string(output)))
	}
	receipt, err := parseBlobReceipt(
		output, format, artifactID, f.Instance, f.ObjectStoreID, f.Bucket,
		objectKey, minRetainUntil, "", 0,
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
		return Source{}, output, errors.New("settlement receipt does not match downloaded bytes")
	}
	return Source{
		ArtifactFormat: receipt.ArtifactFormat, ArtifactID: receipt.ArtifactID,
		ObjectKey: receipt.ObjectKey, VersionID: receipt.VersionID,
		ArtifactSHA256: receipt.ArtifactSHA256, ObjectBytes: receipt.ObjectBytes,
		RetainUntilUnix: receipt.RetainUntilUnix,
	}, output, nil
}

func hasDuplicateString(values ...string) bool {
	seen := make(map[string]struct{}, len(values))
	for _, value := range values {
		if _, ok := seen[value]; ok {
			return true
		}
		seen[value] = struct{}{}
	}
	return false
}

func DecodeSettlementKind(path string) (string, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return "", err
	}
	var header struct {
		Format string `json:"format"`
	}
	if err := json.Unmarshal(data, &header); err != nil {
		return "", err
	}
	switch header.Format {
	case AdjustmentFormat:
		return "adjustment", nil
	case InvoicePlanFormat:
		return "plan", nil
	default:
		return "", errors.New("unsupported settlement artifact format")
	}
}
