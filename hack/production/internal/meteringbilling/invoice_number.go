package meteringbilling

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path"
	"strconv"
	"strings"
	"time"
)

const InvoiceNumberAssignmentFormat = "kubebrain.metering-invoice-number-assignment.v1"
const InvoiceNumberingPolicy = "series-sequence-12digit.v1"

type InvoiceNumberAssignmentOptions struct {
	ID             string
	Jurisdiction   string
	Series         string
	Sequence       int64
	AssignedAtUnix int64
}

type InvoiceNumberAssignment struct {
	Format                 string `json:"format"`
	ID                     string `json:"id"`
	Instance               string `json:"instance"`
	InvoiceID              string `json:"invoice_id"`
	PeriodStartUnix        int64  `json:"period_start_unix"`
	PeriodEndUnix          int64  `json:"period_end_unix"`
	Currency               string `json:"currency"`
	InvoiceSource          Source `json:"invoice_source"`
	InvoiceFinalizedAtUnix int64  `json:"invoice_finalized_at_unix"`
	InvoiceTotalMicros     int64  `json:"invoice_total_micros"`
	Jurisdiction           string `json:"jurisdiction"`
	Series                 string `json:"series"`
	Sequence               int64  `json:"sequence"`
	DisplayNumber          string `json:"display_number"`
	NumberingPolicy        string `json:"numbering_policy"`
	AssignedAtUnix         int64  `json:"assigned_at_unix"`
}

func BuildInvoiceNumberAssignment(
	invoice Invoice,
	invoiceSource Source,
	options InvoiceNumberAssignmentOptions,
) (InvoiceNumberAssignment, error) {
	if err := invoice.Validate(); err != nil {
		return InvoiceNumberAssignment{}, err
	}
	if err := validateSource(invoiceSource, InvoiceFormat, invoice.ID, invoice.PeriodEndUnix); err != nil {
		return InvoiceNumberAssignment{}, fmt.Errorf("invoice source: %w", err)
	}
	if !versionPattern.MatchString(options.ID) ||
		!versionPattern.MatchString(options.Jurisdiction) ||
		!versionPattern.MatchString(options.Series) ||
		options.Sequence <= 0 ||
		options.AssignedAtUnix < invoice.FinalizedAtUnix {
		return InvoiceNumberAssignment{}, errors.New("invoice number assignment options are incomplete")
	}
	assignment := InvoiceNumberAssignment{
		Format: InvoiceNumberAssignmentFormat, ID: options.ID, Instance: invoice.Instance,
		InvoiceID: invoice.ID, PeriodStartUnix: invoice.PeriodStartUnix,
		PeriodEndUnix: invoice.PeriodEndUnix, Currency: invoice.Currency,
		InvoiceSource: invoiceSource, InvoiceFinalizedAtUnix: invoice.FinalizedAtUnix,
		InvoiceTotalMicros: invoice.TotalMicros, Jurisdiction: options.Jurisdiction,
		Series: options.Series, Sequence: options.Sequence,
		DisplayNumber:   invoiceDisplayNumber(options.Series, options.Sequence),
		NumberingPolicy: InvoiceNumberingPolicy, AssignedAtUnix: options.AssignedAtUnix,
	}
	if err := assignment.Validate(); err != nil {
		return InvoiceNumberAssignment{}, err
	}
	return assignment, nil
}

func BuildInvoiceNumberAssignmentWithInvoiceStatus(
	invoiceStatus SettlementStatus[Invoice],
	invoiceSource Source,
	options InvoiceNumberAssignmentOptions,
) (InvoiceNumberAssignment, error) {
	if invoiceSource.ArtifactSHA256 != invoiceStatus.SHA256 ||
		invoiceSource.ObjectBytes != invoiceStatus.Bytes {
		return InvoiceNumberAssignment{}, errors.New("invoice number source does not match invoice bytes")
	}
	return BuildInvoiceNumberAssignment(invoiceStatus.Value, invoiceSource, options)
}

func (a InvoiceNumberAssignment) Validate() error {
	if a.Format != InvoiceNumberAssignmentFormat ||
		!versionPattern.MatchString(a.ID) ||
		!versionPattern.MatchString(a.Instance) ||
		!versionPattern.MatchString(a.InvoiceID) ||
		a.PeriodStartUnix <= 0 || a.PeriodEndUnix <= a.PeriodStartUnix ||
		a.PeriodStartUnix%86400 != 0 || a.PeriodEndUnix%86400 != 0 ||
		!currencyPattern.MatchString(a.Currency) ||
		validateSource(a.InvoiceSource, InvoiceFormat, a.InvoiceID, a.PeriodEndUnix) != nil ||
		a.InvoiceFinalizedAtUnix < a.PeriodEndUnix ||
		a.InvoiceTotalMicros < 0 ||
		!versionPattern.MatchString(a.Jurisdiction) ||
		!versionPattern.MatchString(a.Series) ||
		a.Sequence <= 0 ||
		a.DisplayNumber != invoiceDisplayNumber(a.Series, a.Sequence) ||
		a.NumberingPolicy != InvoiceNumberingPolicy ||
		a.AssignedAtUnix < a.InvoiceFinalizedAtUnix {
		return errors.New("invoice number assignment is incomplete")
	}
	return nil
}

func ReadInvoiceNumberAssignment(filePath string) (SettlementStatus[InvoiceNumberAssignment], error) {
	return readSettlement(filePath, "invoice number assignment", func(value InvoiceNumberAssignment) error {
		return value.Validate()
	})
}

func WriteInvoiceNumberAssignmentAtomic(filePath string, value InvoiceNumberAssignment) (SettlementStatus[InvoiceNumberAssignment], error) {
	return writeSettlement(filePath, "invoice number assignment", value, value.Validate, ReadInvoiceNumberAssignment)
}

type InvoiceNumberAssigner struct {
	Output            string
	ID                string
	Instance          string
	InvoiceID         string
	Jurisdiction      string
	Series            string
	Sequence          int64
	Executor          string
	ObjectStoreID     string
	Bucket            string
	InvoicePrefix     string
	NumberPrefix      string
	RetentionMode     string
	RetentionDuration time.Duration
	AssignedAtUnix    int64
	Publish           bool
	Now               func() time.Time
	Run               CommandRunner
}

func (a *InvoiceNumberAssigner) Validate() error {
	a.InvoicePrefix = strings.Trim(a.InvoicePrefix, "/")
	a.NumberPrefix = strings.Trim(a.NumberPrefix, "/")
	if a.Output == "" || !versionPattern.MatchString(a.ID) ||
		!versionPattern.MatchString(a.Instance) || !versionPattern.MatchString(a.InvoiceID) ||
		!versionPattern.MatchString(a.Jurisdiction) || !versionPattern.MatchString(a.Series) ||
		a.Sequence <= 0 || a.Executor == "" || a.ObjectStoreID == "" || a.Bucket == "" ||
		a.InvoicePrefix == "" || a.AssignedAtUnix <= 0 ||
		a.RetentionDuration <= 24*time.Hour ||
		(a.Publish && a.NumberPrefix == "") ||
		(a.RetentionMode != "COMPLIANCE" && a.RetentionMode != "GOVERNANCE") {
		return errors.New("invoice number assigner configuration is incomplete")
	}
	if a.Now == nil {
		a.Now = time.Now
	}
	if a.Run == nil {
		a.Run = runCommand
	}
	return nil
}

func (a *InvoiceNumberAssigner) Process(ctx context.Context) (SettlementStatus[InvoiceNumberAssignment], []byte, error) {
	if err := a.Validate(); err != nil {
		return SettlementStatus[InvoiceNumberAssignment]{}, nil, err
	}
	now := a.Now().UTC()
	if a.AssignedAtUnix > now.Unix() {
		return SettlementStatus[InvoiceNumberAssignment]{}, nil, errors.New("invoice number assignment timestamp is in the future")
	}
	dir, err := os.MkdirTemp("", "kubebrain-invoice-number-*")
	if err != nil {
		return SettlementStatus[InvoiceNumberAssignment]{}, nil, err
	}
	defer os.RemoveAll(dir)
	invoiceKey := settlementObjectKey(a.InvoicePrefix, a.Instance, a.InvoiceID)
	invoicePath := path.Join(dir, "invoice.json")
	invoiceSource, output, err := a.readImmutable(ctx, InvoiceFormat, a.InvoiceID, invoiceKey, invoicePath, 1)
	if err != nil {
		return SettlementStatus[InvoiceNumberAssignment]{}, output, fmt.Errorf("read invoice: %w", err)
	}
	invoiceStatus, err := ReadInvoice(invoicePath)
	if err != nil {
		return SettlementStatus[InvoiceNumberAssignment]{}, output, err
	}
	invoice := invoiceStatus.Value
	if invoice.ID != a.InvoiceID || invoice.Instance != a.Instance {
		return SettlementStatus[InvoiceNumberAssignment]{}, output, errors.New("invoice identity does not match request")
	}
	retainUntil := time.Unix(invoice.PeriodStartUnix, 0).
		Add(time.Hour).Add(a.RetentionDuration).Unix()
	if retainUntil <= now.Unix() || invoiceSource.RetainUntilUnix < retainUntil {
		return SettlementStatus[InvoiceNumberAssignment]{}, output, errors.New("invoice number evidence retention is insufficient")
	}
	assignment, err := BuildInvoiceNumberAssignmentWithInvoiceStatus(
		invoiceStatus, invoiceSource,
		InvoiceNumberAssignmentOptions{
			ID: a.ID, Jurisdiction: a.Jurisdiction, Series: a.Series,
			Sequence: a.Sequence, AssignedAtUnix: a.AssignedAtUnix,
		},
	)
	if err != nil {
		return SettlementStatus[InvoiceNumberAssignment]{}, output, err
	}
	status, err := WriteInvoiceNumberAssignmentAtomic(a.Output, assignment)
	if err != nil {
		return SettlementStatus[InvoiceNumberAssignment]{}, output, err
	}
	if !a.Publish {
		return status, output, nil
	}
	archiveInput := path.Join(dir, "invoice-number.archive.json")
	if frozen, err := WriteInvoiceNumberAssignmentAtomic(archiveInput, assignment); err != nil {
		return SettlementStatus[InvoiceNumberAssignment]{}, output, err
	} else if frozen.SHA256 != status.SHA256 || frozen.Bytes != status.Bytes {
		return SettlementStatus[InvoiceNumberAssignment]{}, output, errors.New("frozen invoice number assignment does not match output")
	}
	objectKey := settlementObjectKey(a.NumberPrefix, a.Instance, a.ID)
	archiveOutput, err := a.Run(ctx, a.Executor, []string{
		"ACTION=blob", "INPUT=" + archiveInput,
		"ARTIFACT_FORMAT=" + InvoiceNumberAssignmentFormat,
		"ARTIFACT_ID=" + a.ID, "INSTANCE=" + a.Instance,
		"OBJECT_STORE_ID=" + a.ObjectStoreID, "S3_BUCKET=" + a.Bucket,
		"S3_OBJECT_KEY=" + objectKey, "RETENTION_MODE=" + a.RetentionMode,
		"RETAIN_UNTIL_UNIX=" + strconv.FormatInt(retainUntil, 10),
		"RECEIPT_OUTPUT=" + path.Join(dir, "invoice-number.receipt.json"),
	})
	if err != nil {
		return SettlementStatus[InvoiceNumberAssignment]{}, archiveOutput, fmt.Errorf(
			"invoice number assignment Object Lock archive failed: %w: %s",
			err, strings.TrimSpace(string(archiveOutput)),
		)
	}
	if _, err := parseBlobReceipt(
		archiveOutput, InvoiceNumberAssignmentFormat, a.ID, a.Instance,
		a.ObjectStoreID, a.Bucket, objectKey, retainUntil, status.SHA256, status.Bytes,
	); err != nil {
		return SettlementStatus[InvoiceNumberAssignment]{}, archiveOutput, err
	}
	return status, archiveOutput, nil
}

func (a *InvoiceNumberAssigner) readImmutable(
	ctx context.Context,
	format, artifactID, objectKey, outputPath string,
	minRetainUntil int64,
) (Source, []byte, error) {
	output, err := a.Run(ctx, a.Executor, []string{
		"ACTION=blob-read", "OUTPUT=" + outputPath, "ARTIFACT_FORMAT=" + format,
		"ARTIFACT_ID=" + artifactID, "INSTANCE=" + a.Instance,
		"OBJECT_STORE_ID=" + a.ObjectStoreID, "S3_BUCKET=" + a.Bucket,
		"S3_OBJECT_KEY=" + objectKey,
		"MIN_RETAIN_UNTIL_UNIX=" + strconv.FormatInt(minRetainUntil, 10),
	})
	if err != nil {
		return Source{}, output, fmt.Errorf("%w: %s", err, strings.TrimSpace(string(output)))
	}
	receipt, err := parseBlobReceipt(
		output, format, artifactID, a.Instance, a.ObjectStoreID, a.Bucket,
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
		return Source{}, output, errors.New("invoice number receipt does not match downloaded bytes")
	}
	return Source{
		ArtifactFormat: receipt.ArtifactFormat, ArtifactID: receipt.ArtifactID,
		ObjectKey: receipt.ObjectKey, VersionID: receipt.VersionID,
		ArtifactSHA256: receipt.ArtifactSHA256, ObjectBytes: receipt.ObjectBytes,
		RetainUntilUnix: receipt.RetainUntilUnix,
	}, output, nil
}

func invoiceDisplayNumber(series string, sequence int64) string {
	return fmt.Sprintf("%s-%012d", series, sequence)
}
