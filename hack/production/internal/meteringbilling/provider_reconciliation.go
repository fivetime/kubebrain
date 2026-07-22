package meteringbilling

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path"
	"sort"
	"strconv"
	"strings"
	"time"
)

const ProviderStatementFormat = "kubebrain.metering-provider-statement.v1"
const ProviderReconciliationFormat = "kubebrain.metering-provider-reconciliation.v1"

var providerCategories = map[string]struct{}{
	"compute":        {},
	"memory":         {},
	"network":        {},
	"block_storage":  {},
	"object_storage": {},
	"control_plane":  {},
	"support":        {},
}

type ProviderStatementLine struct {
	Provider          string `json:"provider"`
	ProviderAccountID string `json:"provider_account_id"`
	ExternalInvoiceID string `json:"external_invoice_id"`
	ExternalLineID    string `json:"external_line_id"`
	Category          string `json:"category"`
	ObjectStoreID     string `json:"object_store_id,omitempty"`
	Bucket            string `json:"bucket,omitempty"`
	AmountMicros      int64  `json:"amount_micros"`
}

type ProviderStatement struct {
	Format          string                  `json:"format"`
	ID              string                  `json:"id"`
	Instance        string                  `json:"instance"`
	PeriodStartUnix int64                   `json:"period_start_unix"`
	PeriodEndUnix   int64                   `json:"period_end_unix"`
	Currency        string                  `json:"currency"`
	IssuedAtUnix    int64                   `json:"issued_at_unix"`
	Lines           []ProviderStatementLine `json:"lines"`
	TotalMicros     int64                   `json:"total_micros"`
}

type ProviderAllocation struct {
	Provider          string `json:"provider"`
	ProviderAccountID string `json:"provider_account_id"`
	Category          string `json:"category"`
	ObjectStoreID     string `json:"object_store_id,omitempty"`
	Bucket            string `json:"bucket,omitempty"`
	AmountMicros      int64  `json:"amount_micros"`
}

type ProviderReconciliation struct {
	Format                        string               `json:"format"`
	ID                            string               `json:"id"`
	Instance                      string               `json:"instance"`
	PeriodStartUnix               int64                `json:"period_start_unix"`
	PeriodEndUnix                 int64                `json:"period_end_unix"`
	Currency                      string               `json:"currency"`
	ProviderStatementID           string               `json:"provider_statement_id"`
	InvoiceID                     string               `json:"invoice_id"`
	ProviderStatementIssuedAtUnix int64                `json:"provider_statement_issued_at_unix"`
	InvoiceFinalizedAtUnix        int64                `json:"invoice_finalized_at_unix"`
	ProviderStatementSource       Source               `json:"provider_statement_source"`
	InvoiceSource                 Source               `json:"invoice_source"`
	ProviderAllocations           []ProviderAllocation `json:"provider_allocations"`
	ProviderStatementTotalMicros  int64                `json:"provider_statement_total_micros"`
	CustomerInvoiceTotalMicros    int64                `json:"customer_invoice_total_micros"`
	GrossMarginMicros             int64                `json:"gross_margin_micros"`
	ReconciledAtUnix              int64                `json:"reconciled_at_unix"`
}

func (l ProviderStatementLine) validate() error {
	if !versionPattern.MatchString(l.Provider) ||
		!versionPattern.MatchString(l.ProviderAccountID) ||
		!validProviderReference(l.ExternalInvoiceID) ||
		!validProviderReference(l.ExternalLineID) ||
		l.AmountMicros <= 0 {
		return errors.New("provider statement line is incomplete")
	}
	if _, ok := providerCategories[l.Category]; !ok {
		return errors.New("provider statement line category is unsupported")
	}
	if (l.ObjectStoreID == "") != (l.Bucket == "") {
		return errors.New("provider statement object storage identity is incomplete")
	}
	if l.ObjectStoreID != "" &&
		(!versionPattern.MatchString(l.ObjectStoreID) || !versionPattern.MatchString(l.Bucket)) {
		return errors.New("provider statement object storage identity is invalid")
	}
	if l.Category == "object_storage" && l.ObjectStoreID == "" {
		return errors.New("provider object storage cost is not bucket-scoped")
	}
	return nil
}

func (s ProviderStatement) Validate() error {
	if s.Format != ProviderStatementFormat ||
		!versionPattern.MatchString(s.ID) || !versionPattern.MatchString(s.Instance) ||
		s.PeriodStartUnix <= 0 || s.PeriodEndUnix <= s.PeriodStartUnix ||
		s.PeriodStartUnix%86400 != 0 || s.PeriodEndUnix%86400 != 0 ||
		!currencyPattern.MatchString(s.Currency) || s.IssuedAtUnix < s.PeriodEndUnix ||
		len(s.Lines) == 0 || s.TotalMicros <= 0 {
		return errors.New("provider statement is incomplete")
	}
	var total int64
	previous := ""
	for _, line := range s.Lines {
		if err := line.validate(); err != nil {
			return err
		}
		key := providerLineSortKey(line)
		if previous != "" && key <= previous {
			return errors.New("provider statement lines are not unique and sorted")
		}
		next, ok := addInt64(total, line.AmountMicros)
		if !ok {
			return errors.New("provider statement total overflows int64")
		}
		total = next
		previous = key
	}
	if total != s.TotalMicros {
		return errors.New("provider statement total does not match lines")
	}
	return nil
}

func (a ProviderAllocation) validate() error {
	if !versionPattern.MatchString(a.Provider) ||
		!versionPattern.MatchString(a.ProviderAccountID) ||
		a.AmountMicros <= 0 {
		return errors.New("provider allocation is incomplete")
	}
	if _, ok := providerCategories[a.Category]; !ok {
		return errors.New("provider allocation category is unsupported")
	}
	if (a.ObjectStoreID == "") != (a.Bucket == "") {
		return errors.New("provider allocation object storage identity is incomplete")
	}
	if a.ObjectStoreID != "" &&
		(!versionPattern.MatchString(a.ObjectStoreID) || !versionPattern.MatchString(a.Bucket)) {
		return errors.New("provider allocation object storage identity is invalid")
	}
	if a.Category == "object_storage" && a.ObjectStoreID == "" {
		return errors.New("provider object storage allocation is not bucket-scoped")
	}
	return nil
}

func BuildProviderReconciliation(
	id string,
	statement ProviderStatement,
	statementSource Source,
	invoice Invoice,
	invoiceSource Source,
	reconciledAtUnix int64,
) (ProviderReconciliation, error) {
	if err := statement.Validate(); err != nil {
		return ProviderReconciliation{}, err
	}
	if err := invoice.Validate(); err != nil {
		return ProviderReconciliation{}, err
	}
	if !versionPattern.MatchString(id) {
		return ProviderReconciliation{}, errors.New("provider reconciliation ID is invalid")
	}
	if statement.Instance != invoice.Instance ||
		statement.PeriodStartUnix != invoice.PeriodStartUnix ||
		statement.PeriodEndUnix != invoice.PeriodEndUnix ||
		statement.Currency != invoice.Currency {
		return ProviderReconciliation{}, errors.New("provider statement does not match invoice period")
	}
	if err := validateSource(
		statementSource, ProviderStatementFormat, statement.ID, statement.PeriodEndUnix,
	); err != nil {
		return ProviderReconciliation{}, fmt.Errorf("provider statement source: %w", err)
	}
	if err := validateSource(invoiceSource, InvoiceFormat, invoice.ID, invoice.PeriodEndUnix); err != nil {
		return ProviderReconciliation{}, fmt.Errorf("invoice source: %w", err)
	}
	if reconciledAtUnix < statement.IssuedAtUnix || reconciledAtUnix < invoice.FinalizedAtUnix {
		return ProviderReconciliation{}, errors.New("provider reconciliation timestamp is too early")
	}
	allocations, err := providerAllocations(statement.Lines)
	if err != nil {
		return ProviderReconciliation{}, err
	}
	margin, ok := addInt64(invoice.TotalMicros, -statement.TotalMicros)
	if !ok {
		return ProviderReconciliation{}, errors.New("provider reconciliation margin overflows int64")
	}
	result := ProviderReconciliation{
		Format: ProviderReconciliationFormat, ID: id, Instance: statement.Instance,
		PeriodStartUnix: statement.PeriodStartUnix, PeriodEndUnix: statement.PeriodEndUnix,
		Currency: statement.Currency, ProviderStatementID: statement.ID, InvoiceID: invoice.ID,
		ProviderStatementIssuedAtUnix: statement.IssuedAtUnix,
		InvoiceFinalizedAtUnix:        invoice.FinalizedAtUnix,
		ProviderStatementSource:       statementSource, InvoiceSource: invoiceSource,
		ProviderAllocations: allocations, ProviderStatementTotalMicros: statement.TotalMicros,
		CustomerInvoiceTotalMicros: invoice.TotalMicros, GrossMarginMicros: margin,
		ReconciledAtUnix: reconciledAtUnix,
	}
	if err := result.Validate(); err != nil {
		return ProviderReconciliation{}, err
	}
	return result, nil
}

func (r ProviderReconciliation) Validate() error {
	if r.Format != ProviderReconciliationFormat ||
		!versionPattern.MatchString(r.ID) || !versionPattern.MatchString(r.Instance) ||
		r.PeriodStartUnix <= 0 || r.PeriodEndUnix <= r.PeriodStartUnix ||
		r.PeriodStartUnix%86400 != 0 || r.PeriodEndUnix%86400 != 0 ||
		!currencyPattern.MatchString(r.Currency) ||
		!versionPattern.MatchString(r.ProviderStatementID) ||
		!versionPattern.MatchString(r.InvoiceID) ||
		r.ProviderStatementIssuedAtUnix < r.PeriodEndUnix ||
		r.InvoiceFinalizedAtUnix < r.PeriodEndUnix ||
		r.ReconciledAtUnix < r.ProviderStatementIssuedAtUnix ||
		r.ReconciledAtUnix < r.InvoiceFinalizedAtUnix ||
		r.ProviderStatementTotalMicros <= 0 || r.CustomerInvoiceTotalMicros < 0 ||
		len(r.ProviderAllocations) == 0 {
		return errors.New("provider reconciliation is incomplete")
	}
	if err := validateSource(
		r.ProviderStatementSource, ProviderStatementFormat, r.ProviderStatementID,
		r.PeriodEndUnix,
	); err != nil {
		return fmt.Errorf("provider statement source: %w", err)
	}
	if err := validateSource(r.InvoiceSource, InvoiceFormat, r.InvoiceID, r.PeriodEndUnix); err != nil {
		return fmt.Errorf("invoice source: %w", err)
	}
	var total int64
	previous := ""
	for _, allocation := range r.ProviderAllocations {
		if err := allocation.validate(); err != nil {
			return err
		}
		key := providerAllocationSortKey(allocation)
		if previous != "" && key <= previous {
			return errors.New("provider reconciliation allocations are not unique and sorted")
		}
		next, ok := addInt64(total, allocation.AmountMicros)
		if !ok {
			return errors.New("provider reconciliation total overflows int64")
		}
		total = next
		previous = key
	}
	margin, ok := addInt64(r.CustomerInvoiceTotalMicros, -r.ProviderStatementTotalMicros)
	if !ok || total != r.ProviderStatementTotalMicros || margin != r.GrossMarginMicros {
		return errors.New("provider reconciliation totals do not match sources")
	}
	return nil
}

func ReadProviderStatement(path string) (SettlementStatus[ProviderStatement], error) {
	return readSettlement(path, "metering provider statement", func(value ProviderStatement) error {
		return value.Validate()
	})
}

func WriteProviderStatementAtomic(path string, value ProviderStatement) (SettlementStatus[ProviderStatement], error) {
	return writeSettlement(path, "metering provider statement", value, value.Validate, ReadProviderStatement)
}

func ReadProviderReconciliation(path string) (SettlementStatus[ProviderReconciliation], error) {
	return readSettlement(path, "metering provider reconciliation", func(value ProviderReconciliation) error {
		return value.Validate()
	})
}

func WriteProviderReconciliationAtomic(path string, value ProviderReconciliation) (SettlementStatus[ProviderReconciliation], error) {
	return writeSettlement(path, "metering provider reconciliation", value, value.Validate, ReadProviderReconciliation)
}

type ProviderReconciler struct {
	Instance             string
	ProviderStatementID  string
	InvoiceID            string
	ReconciliationID     string
	Executor             string
	ObjectStoreID        string
	Bucket               string
	ProviderPrefix       string
	InvoicePrefix        string
	ReconciliationPrefix string
	RetentionMode        string
	RetentionDuration    time.Duration
	Now                  func() time.Time
	Run                  CommandRunner
}

func (r *ProviderReconciler) Validate() error {
	r.ProviderPrefix = strings.Trim(r.ProviderPrefix, "/")
	r.InvoicePrefix = strings.Trim(r.InvoicePrefix, "/")
	r.ReconciliationPrefix = strings.Trim(r.ReconciliationPrefix, "/")
	if !versionPattern.MatchString(r.Instance) ||
		!versionPattern.MatchString(r.ProviderStatementID) ||
		!versionPattern.MatchString(r.InvoiceID) ||
		!versionPattern.MatchString(r.ReconciliationID) ||
		r.Executor == "" || r.ObjectStoreID == "" || r.Bucket == "" ||
		r.ProviderPrefix == "" || r.InvoicePrefix == "" || r.ReconciliationPrefix == "" ||
		hasDuplicateString(r.ProviderPrefix, r.InvoicePrefix, r.ReconciliationPrefix) ||
		(r.RetentionMode != "COMPLIANCE" && r.RetentionMode != "GOVERNANCE") ||
		r.RetentionDuration <= 24*time.Hour {
		return errors.New("provider reconciler configuration is incomplete")
	}
	if r.Now == nil {
		r.Now = time.Now
	}
	if r.Run == nil {
		r.Run = runCommand
	}
	return nil
}

func (r *ProviderReconciler) Process(ctx context.Context) (ProviderReconciliation, []byte, error) {
	if err := r.Validate(); err != nil {
		return ProviderReconciliation{}, nil, err
	}
	dir, err := os.MkdirTemp("", "kubebrain-provider-reconcile-*")
	if err != nil {
		return ProviderReconciliation{}, nil, err
	}
	defer os.RemoveAll(dir)

	statementKey := settlementObjectKey(r.ProviderPrefix, r.Instance, r.ProviderStatementID)
	statementPath := path.Join(dir, "provider-statement.json")
	statementSource, output, err := r.readImmutable(
		ctx, ProviderStatementFormat, r.ProviderStatementID, statementKey, statementPath, 1,
	)
	if err != nil {
		return ProviderReconciliation{}, output, fmt.Errorf("read provider statement: %w", err)
	}
	statementStatus, err := ReadProviderStatement(statementPath)
	if err != nil {
		return ProviderReconciliation{}, output, err
	}
	statement := statementStatus.Value
	if statement.ID != r.ProviderStatementID || statement.Instance != r.Instance {
		return ProviderReconciliation{}, output, errors.New("provider statement identity does not match request")
	}

	invoiceKey := settlementObjectKey(r.InvoicePrefix, r.Instance, r.InvoiceID)
	invoicePath := path.Join(dir, "invoice.json")
	invoiceSource, output, err := r.readImmutable(
		ctx, InvoiceFormat, r.InvoiceID, invoiceKey, invoicePath, 1,
	)
	if err != nil {
		return ProviderReconciliation{}, output, fmt.Errorf("read invoice: %w", err)
	}
	invoiceStatus, err := ReadInvoice(invoicePath)
	if err != nil {
		return ProviderReconciliation{}, output, err
	}
	invoice := invoiceStatus.Value
	if invoice.ID != r.InvoiceID || invoice.Instance != r.Instance {
		return ProviderReconciliation{}, output, errors.New("invoice identity does not match request")
	}

	retainUntil := time.Unix(statement.PeriodStartUnix, 0).
		Add(time.Hour).Add(r.RetentionDuration).Unix()
	now := r.Now().UTC()
	if statementSource.RetainUntilUnix < retainUntil || invoiceSource.RetainUntilUnix < retainUntil ||
		retainUntil <= now.Unix() {
		return ProviderReconciliation{}, output, errors.New("provider reconciliation evidence retention is insufficient")
	}
	reconciliation, err := BuildProviderReconciliation(
		r.ReconciliationID, statement, statementSource, invoice, invoiceSource, now.Unix(),
	)
	if err != nil {
		return ProviderReconciliation{}, output, err
	}

	reconciliationPath := path.Join(dir, "reconciliation.json")
	status, err := WriteProviderReconciliationAtomic(reconciliationPath, reconciliation)
	if err != nil {
		return ProviderReconciliation{}, nil, err
	}
	reconciliationKey := settlementObjectKey(r.ReconciliationPrefix, r.Instance, r.ReconciliationID)
	output, err = r.Run(ctx, r.Executor, []string{
		"ACTION=blob", "INPUT=" + reconciliationPath,
		"ARTIFACT_FORMAT=" + ProviderReconciliationFormat,
		"ARTIFACT_ID=" + r.ReconciliationID, "INSTANCE=" + r.Instance,
		"OBJECT_STORE_ID=" + r.ObjectStoreID, "S3_BUCKET=" + r.Bucket,
		"S3_OBJECT_KEY=" + reconciliationKey, "RETENTION_MODE=" + r.RetentionMode,
		"RETAIN_UNTIL_UNIX=" + strconv.FormatInt(retainUntil, 10),
		"RECEIPT_OUTPUT=" + path.Join(dir, "reconciliation.receipt.json"),
	})
	if err != nil {
		return ProviderReconciliation{}, output, fmt.Errorf(
			"provider reconciliation Object Lock archive failed: %w: %s",
			err, strings.TrimSpace(string(output)),
		)
	}
	if _, err := parseBlobReceipt(
		output, ProviderReconciliationFormat, r.ReconciliationID, r.Instance,
		r.ObjectStoreID, r.Bucket, reconciliationKey, retainUntil, status.SHA256, status.Bytes,
	); err != nil {
		return ProviderReconciliation{}, output, err
	}
	return reconciliation, output, nil
}

func (r *ProviderReconciler) readImmutable(
	ctx context.Context,
	format, artifactID, objectKey, outputPath string,
	minRetainUntil int64,
) (Source, []byte, error) {
	output, err := r.Run(ctx, r.Executor, []string{
		"ACTION=blob-read", "OUTPUT=" + outputPath, "ARTIFACT_FORMAT=" + format,
		"ARTIFACT_ID=" + artifactID, "INSTANCE=" + r.Instance,
		"OBJECT_STORE_ID=" + r.ObjectStoreID, "S3_BUCKET=" + r.Bucket,
		"S3_OBJECT_KEY=" + objectKey,
		"MIN_RETAIN_UNTIL_UNIX=" + strconv.FormatInt(minRetainUntil, 10),
	})
	if err != nil {
		return Source{}, output, fmt.Errorf("%w: %s", err, strings.TrimSpace(string(output)))
	}
	receipt, err := parseBlobReceipt(
		output, format, artifactID, r.Instance, r.ObjectStoreID, r.Bucket,
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
		return Source{}, output, errors.New("provider reconciliation receipt does not match downloaded bytes")
	}
	return Source{
		ArtifactFormat: receipt.ArtifactFormat, ArtifactID: receipt.ArtifactID,
		ObjectKey: receipt.ObjectKey, VersionID: receipt.VersionID,
		ArtifactSHA256: receipt.ArtifactSHA256, ObjectBytes: receipt.ObjectBytes,
		RetainUntilUnix: receipt.RetainUntilUnix,
	}, output, nil
}

func providerAllocations(lines []ProviderStatementLine) ([]ProviderAllocation, error) {
	byKey := make(map[string]ProviderAllocation, len(lines))
	for _, line := range lines {
		allocation := ProviderAllocation{
			Provider: line.Provider, ProviderAccountID: line.ProviderAccountID,
			Category: line.Category, ObjectStoreID: line.ObjectStoreID, Bucket: line.Bucket,
		}
		key := providerAllocationSortKey(allocation)
		current := byKey[key]
		if current.Provider == "" {
			current = allocation
		}
		next, ok := addInt64(current.AmountMicros, line.AmountMicros)
		if !ok {
			return nil, errors.New("provider allocation total overflows int64")
		}
		current.AmountMicros = next
		byKey[key] = current
	}
	result := make([]ProviderAllocation, 0, len(byKey))
	for _, allocation := range byKey {
		result = append(result, allocation)
	}
	sort.Slice(result, func(i, j int) bool {
		return providerAllocationSortKey(result[i]) < providerAllocationSortKey(result[j])
	})
	return result, nil
}

func providerLineSortKey(line ProviderStatementLine) string {
	return strings.Join([]string{
		line.Provider, line.ProviderAccountID, line.ExternalInvoiceID, line.ExternalLineID,
		line.Category, line.ObjectStoreID, line.Bucket,
	}, "\x00")
}

func providerAllocationSortKey(allocation ProviderAllocation) string {
	return strings.Join([]string{
		allocation.Provider, allocation.ProviderAccountID, allocation.Category,
		allocation.ObjectStoreID, allocation.Bucket,
	}, "\x00")
}

func validProviderReference(value string) bool {
	if value == "" || len(value) > 256 || strings.TrimSpace(value) != value {
		return false
	}
	for _, r := range value {
		if r < 0x21 || r > 0x7e {
			return false
		}
	}
	return true
}
