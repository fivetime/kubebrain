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

const GeneralLedgerExportFormat = "kubebrain.metering-general-ledger-export.v1"

type GeneralLedgerLine struct {
	LineID       string `json:"line_id"`
	Account      string `json:"account"`
	Direction    string `json:"direction"`
	AmountMicros int64  `json:"amount_micros"`
	SourceFormat string `json:"source_format"`
	SourceID     string `json:"source_id"`
	SourceLineID string `json:"source_line_id,omitempty"`
}

type GeneralLedgerExport struct {
	Format                       string              `json:"format"`
	ID                           string              `json:"id"`
	Instance                     string              `json:"instance"`
	PeriodStartUnix              int64               `json:"period_start_unix"`
	PeriodEndUnix                int64               `json:"period_end_unix"`
	Currency                     string              `json:"currency"`
	InvoiceID                    string              `json:"invoice_id"`
	InvoiceSource                Source              `json:"invoice_source"`
	ProviderReconciliationID     string              `json:"provider_reconciliation_id,omitempty"`
	ProviderReconciliationSource *Source             `json:"provider_reconciliation_source,omitempty"`
	PaymentLedgerID              string              `json:"payment_ledger_id,omitempty"`
	PaymentLedgerSource          *Source             `json:"payment_ledger_source,omitempty"`
	Lines                        []GeneralLedgerLine `json:"lines"`
	DebitTotalMicros             int64               `json:"debit_total_micros"`
	CreditTotalMicros            int64               `json:"credit_total_micros"`
	ExportedAtUnix               int64               `json:"exported_at_unix"`
}

type GeneralLedgerExportOptions struct {
	ID             string
	ExportedAtUnix int64
}

func BuildGeneralLedgerExport(
	invoice Invoice,
	invoiceSource Source,
	reconciliation *ProviderReconciliation,
	reconciliationSource *Source,
	paymentLedger *PaymentLedger,
	paymentLedgerSource *Source,
	options GeneralLedgerExportOptions,
) (GeneralLedgerExport, error) {
	if err := invoice.Validate(); err != nil {
		return GeneralLedgerExport{}, err
	}
	if err := validateSource(invoiceSource, InvoiceFormat, invoice.ID, invoice.PeriodEndUnix); err != nil {
		return GeneralLedgerExport{}, fmt.Errorf("invoice source: %w", err)
	}
	if !versionPattern.MatchString(options.ID) || options.ExportedAtUnix < invoice.FinalizedAtUnix {
		return GeneralLedgerExport{}, errors.New("general ledger export identity is incomplete")
	}
	lines := []GeneralLedgerLine{
		generalLedgerLine("invoice-ar", "accounts_receivable", "debit", invoice.TotalMicros, invoiceSource, ""),
		generalLedgerLine("invoice-revenue", "kubebrain_revenue", "credit", invoice.TotalMicros, invoiceSource, ""),
	}
	var reconciliationID string
	if reconciliation != nil || reconciliationSource != nil {
		if reconciliation == nil || reconciliationSource == nil {
			return GeneralLedgerExport{}, errors.New("general ledger provider reconciliation input is incomplete")
		}
		if err := reconciliation.Validate(); err != nil {
			return GeneralLedgerExport{}, err
		}
		if err := validateSource(*reconciliationSource, ProviderReconciliationFormat, reconciliation.ID, reconciliation.PeriodEndUnix); err != nil {
			return GeneralLedgerExport{}, fmt.Errorf("provider reconciliation source: %w", err)
		}
		if reconciliation.Instance != invoice.Instance ||
			reconciliation.PeriodStartUnix != invoice.PeriodStartUnix ||
			reconciliation.PeriodEndUnix != invoice.PeriodEndUnix ||
			reconciliation.Currency != invoice.Currency ||
			reconciliation.InvoiceID != invoice.ID ||
			reconciliation.InvoiceSource != invoiceSource ||
			options.ExportedAtUnix < reconciliation.ReconciledAtUnix {
			return GeneralLedgerExport{}, errors.New("general ledger provider reconciliation does not match invoice")
		}
		reconciliationID = reconciliation.ID
		for _, allocation := range reconciliation.ProviderAllocations {
			sourceLineID := generalLedgerLineID("allocation", providerAllocationSortKey(allocation))
			lines = append(lines, generalLedgerLine(
				"provider-"+allocation.Category,
				"provider_cost_"+allocation.Category,
				"debit",
				allocation.AmountMicros,
				*reconciliationSource,
				sourceLineID,
			))
		}
		lines = append(lines, generalLedgerLine(
			"provider-payable",
			"provider_accounts_payable",
			"credit",
			reconciliation.ProviderStatementTotalMicros,
			*reconciliationSource,
			reconciliation.ProviderStatementID,
		))
	}
	var paymentLedgerID string
	if paymentLedger != nil || paymentLedgerSource != nil {
		if paymentLedger == nil || paymentLedgerSource == nil {
			return GeneralLedgerExport{}, errors.New("general ledger payment ledger input is incomplete")
		}
		if err := paymentLedger.Validate(); err != nil {
			return GeneralLedgerExport{}, err
		}
		if err := validateSource(*paymentLedgerSource, PaymentLedgerFormat, paymentLedger.ID, paymentLedger.PeriodEndUnix); err != nil {
			return GeneralLedgerExport{}, fmt.Errorf("payment ledger source: %w", err)
		}
		if paymentLedger.Instance != invoice.Instance ||
			paymentLedger.PeriodStartUnix != invoice.PeriodStartUnix ||
			paymentLedger.PeriodEndUnix != invoice.PeriodEndUnix ||
			paymentLedger.Currency != invoice.Currency ||
			paymentLedger.InvoiceID != invoice.ID ||
			paymentLedger.InvoiceSource != invoiceSource ||
			options.ExportedAtUnix < paymentLedger.GeneratedAtUnix {
			return GeneralLedgerExport{}, errors.New("general ledger payment ledger does not match invoice")
		}
		paymentLedgerID = paymentLedger.ID
		lines = appendPaymentLedgerLines(lines, paymentLedger, *paymentLedgerSource)
	}
	lines, debit, credit, err := normalizeGeneralLedgerLines(lines)
	if err != nil {
		return GeneralLedgerExport{}, err
	}
	export := GeneralLedgerExport{
		Format: GeneralLedgerExportFormat, ID: options.ID, Instance: invoice.Instance,
		PeriodStartUnix: invoice.PeriodStartUnix, PeriodEndUnix: invoice.PeriodEndUnix,
		Currency: invoice.Currency, InvoiceID: invoice.ID, InvoiceSource: invoiceSource,
		ProviderReconciliationID: reconciliationID, PaymentLedgerID: paymentLedgerID,
		Lines: lines, DebitTotalMicros: debit, CreditTotalMicros: credit,
		ExportedAtUnix: options.ExportedAtUnix,
	}
	if reconciliationSource != nil {
		source := *reconciliationSource
		export.ProviderReconciliationSource = &source
	}
	if paymentLedgerSource != nil {
		source := *paymentLedgerSource
		export.PaymentLedgerSource = &source
	}
	if err := export.Validate(); err != nil {
		return GeneralLedgerExport{}, err
	}
	return export, nil
}

func (e GeneralLedgerExport) Validate() error {
	if e.Format != GeneralLedgerExportFormat || !versionPattern.MatchString(e.ID) ||
		!versionPattern.MatchString(e.Instance) || !versionPattern.MatchString(e.InvoiceID) ||
		e.PeriodStartUnix <= 0 || e.PeriodEndUnix <= e.PeriodStartUnix ||
		e.PeriodStartUnix%86400 != 0 || e.PeriodEndUnix%86400 != 0 ||
		!currencyPattern.MatchString(e.Currency) ||
		validateSource(e.InvoiceSource, InvoiceFormat, e.InvoiceID, e.PeriodEndUnix) != nil ||
		len(e.Lines) == 0 || e.DebitTotalMicros <= 0 ||
		e.DebitTotalMicros != e.CreditTotalMicros ||
		e.ExportedAtUnix < e.PeriodEndUnix {
		return errors.New("general ledger export is incomplete")
	}
	if (e.ProviderReconciliationID == "") != (e.ProviderReconciliationSource == nil) ||
		(e.PaymentLedgerID == "") != (e.PaymentLedgerSource == nil) {
		return errors.New("general ledger export source binding is incomplete")
	}
	if e.ProviderReconciliationSource != nil {
		if err := validateSource(*e.ProviderReconciliationSource, ProviderReconciliationFormat, e.ProviderReconciliationID, e.PeriodEndUnix); err != nil {
			return fmt.Errorf("provider reconciliation source: %w", err)
		}
	}
	if e.PaymentLedgerSource != nil {
		if err := validateSource(*e.PaymentLedgerSource, PaymentLedgerFormat, e.PaymentLedgerID, e.PeriodEndUnix); err != nil {
			return fmt.Errorf("payment ledger source: %w", err)
		}
	}
	allowedSources := map[string]struct{}{
		generalLedgerSourceKey(e.InvoiceSource.ArtifactFormat, e.InvoiceSource.ArtifactID): {},
	}
	if e.ProviderReconciliationSource != nil {
		allowedSources[generalLedgerSourceKey(e.ProviderReconciliationSource.ArtifactFormat, e.ProviderReconciliationSource.ArtifactID)] = struct{}{}
	}
	if e.PaymentLedgerSource != nil {
		allowedSources[generalLedgerSourceKey(e.PaymentLedgerSource.ArtifactFormat, e.PaymentLedgerSource.ArtifactID)] = struct{}{}
	}
	sortedLines, debit, credit, err := normalizeGeneralLedgerLines(append([]GeneralLedgerLine(nil), e.Lines...))
	if err != nil {
		return err
	}
	for index := range e.Lines {
		if e.Lines[index].LineID != sortedLines[index].LineID {
			return errors.New("general ledger lines are not sorted")
		}
	}
	for _, line := range e.Lines {
		if _, ok := allowedSources[generalLedgerSourceKey(line.SourceFormat, line.SourceID)]; !ok {
			return errors.New("general ledger line source is not bound by the export")
		}
	}
	if debit != e.DebitTotalMicros || credit != e.CreditTotalMicros {
		return errors.New("general ledger export totals do not match lines")
	}
	return nil
}

func ReadGeneralLedgerExport(filePath string) (SettlementStatus[GeneralLedgerExport], error) {
	return readSettlement(filePath, "general ledger export", func(value GeneralLedgerExport) error {
		return value.Validate()
	})
}

func WriteGeneralLedgerExportAtomic(filePath string, value GeneralLedgerExport) (SettlementStatus[GeneralLedgerExport], error) {
	return writeSettlement(filePath, "general ledger export", value, value.Validate, ReadGeneralLedgerExport)
}

type GeneralLedgerExporter struct {
	Output                       string
	ID                           string
	Instance                     string
	InvoiceID                    string
	ProviderReconciliationID     string
	PaymentLedgerID              string
	Executor                     string
	ObjectStoreID                string
	Bucket                       string
	InvoicePrefix                string
	ProviderReconciliationPrefix string
	PaymentPrefix                string
	LedgerPrefix                 string
	RetentionMode                string
	RetentionDuration            time.Duration
	ExportedAtUnix               int64
	Publish                      bool
	Now                          func() time.Time
	Run                          CommandRunner
}

func (g *GeneralLedgerExporter) Validate() error {
	g.InvoicePrefix = strings.Trim(g.InvoicePrefix, "/")
	g.ProviderReconciliationPrefix = strings.Trim(g.ProviderReconciliationPrefix, "/")
	g.PaymentPrefix = strings.Trim(g.PaymentPrefix, "/")
	g.LedgerPrefix = strings.Trim(g.LedgerPrefix, "/")
	if g.Output == "" || !versionPattern.MatchString(g.ID) ||
		!versionPattern.MatchString(g.Instance) || !versionPattern.MatchString(g.InvoiceID) ||
		(g.ProviderReconciliationID != "" && !versionPattern.MatchString(g.ProviderReconciliationID)) ||
		(g.PaymentLedgerID != "" && !versionPattern.MatchString(g.PaymentLedgerID)) ||
		g.Executor == "" || g.ObjectStoreID == "" || g.Bucket == "" ||
		g.InvoicePrefix == "" || g.ExportedAtUnix <= 0 ||
		g.RetentionDuration <= 24*time.Hour ||
		(g.ProviderReconciliationID != "" && g.ProviderReconciliationPrefix == "") ||
		(g.PaymentLedgerID != "" && g.PaymentPrefix == "") ||
		(g.Publish && g.LedgerPrefix == "") ||
		(g.RetentionMode != "COMPLIANCE" && g.RetentionMode != "GOVERNANCE") {
		return errors.New("general ledger exporter configuration is incomplete")
	}
	if g.Now == nil {
		g.Now = time.Now
	}
	if g.Run == nil {
		g.Run = runCommand
	}
	return nil
}

func (g *GeneralLedgerExporter) Process(ctx context.Context) (SettlementStatus[GeneralLedgerExport], []byte, error) {
	if err := g.Validate(); err != nil {
		return SettlementStatus[GeneralLedgerExport]{}, nil, err
	}
	now := g.Now().UTC()
	if g.ExportedAtUnix > now.Unix() {
		return SettlementStatus[GeneralLedgerExport]{}, nil, errors.New("general ledger export timestamp is in the future")
	}
	dir, err := os.MkdirTemp("", "kubebrain-general-ledger-*")
	if err != nil {
		return SettlementStatus[GeneralLedgerExport]{}, nil, err
	}
	defer os.RemoveAll(dir)
	invoiceKey := settlementObjectKey(g.InvoicePrefix, g.Instance, g.InvoiceID)
	invoicePath := path.Join(dir, "invoice.json")
	invoiceSource, output, err := g.readImmutable(ctx, InvoiceFormat, g.InvoiceID, invoiceKey, invoicePath, 1)
	if err != nil {
		return SettlementStatus[GeneralLedgerExport]{}, output, fmt.Errorf("read invoice: %w", err)
	}
	invoiceStatus, err := ReadInvoice(invoicePath)
	if err != nil {
		return SettlementStatus[GeneralLedgerExport]{}, output, err
	}
	invoice := invoiceStatus.Value
	if invoice.ID != g.InvoiceID || invoice.Instance != g.Instance {
		return SettlementStatus[GeneralLedgerExport]{}, output, errors.New("invoice identity does not match request")
	}
	retainUntil := time.Unix(invoice.PeriodStartUnix, 0).
		Add(time.Hour).Add(g.RetentionDuration).Unix()
	if retainUntil <= now.Unix() || invoiceSource.RetainUntilUnix < retainUntil {
		return SettlementStatus[GeneralLedgerExport]{}, output, errors.New("general ledger invoice evidence retention is insufficient")
	}
	var reconciliation *ProviderReconciliation
	var reconciliationSource *Source
	if g.ProviderReconciliationID != "" {
		key := settlementObjectKey(g.ProviderReconciliationPrefix, g.Instance, g.ProviderReconciliationID)
		outputPath := path.Join(dir, "provider-reconciliation.json")
		source, readOutput, readErr := g.readImmutable(ctx, ProviderReconciliationFormat, g.ProviderReconciliationID, key, outputPath, retainUntil)
		if readErr != nil {
			return SettlementStatus[GeneralLedgerExport]{}, readOutput, fmt.Errorf("read provider reconciliation: %w", readErr)
		}
		status, readErr := ReadProviderReconciliation(outputPath)
		if readErr != nil {
			return SettlementStatus[GeneralLedgerExport]{}, readOutput, readErr
		}
		value := status.Value
		reconciliation = &value
		reconciliationSource = &source
		output = append(output, readOutput...)
	}
	var paymentLedger *PaymentLedger
	var paymentLedgerSource *Source
	if g.PaymentLedgerID != "" {
		key := settlementObjectKey(g.PaymentPrefix, g.Instance, g.PaymentLedgerID)
		outputPath := path.Join(dir, "payment-ledger.json")
		source, readOutput, readErr := g.readImmutable(ctx, PaymentLedgerFormat, g.PaymentLedgerID, key, outputPath, retainUntil)
		if readErr != nil {
			return SettlementStatus[GeneralLedgerExport]{}, readOutput, fmt.Errorf("read payment ledger: %w", readErr)
		}
		status, readErr := ReadPaymentLedger(outputPath)
		if readErr != nil {
			return SettlementStatus[GeneralLedgerExport]{}, readOutput, readErr
		}
		value := status.Value
		paymentLedger = &value
		paymentLedgerSource = &source
		output = append(output, readOutput...)
	}
	export, err := BuildGeneralLedgerExport(
		invoice, invoiceSource, reconciliation, reconciliationSource,
		paymentLedger, paymentLedgerSource,
		GeneralLedgerExportOptions{ID: g.ID, ExportedAtUnix: g.ExportedAtUnix},
	)
	if err != nil {
		return SettlementStatus[GeneralLedgerExport]{}, output, err
	}
	status, err := WriteGeneralLedgerExportAtomic(g.Output, export)
	if err != nil {
		return SettlementStatus[GeneralLedgerExport]{}, output, err
	}
	if !g.Publish {
		return status, output, nil
	}
	ledgerKey := settlementObjectKey(g.LedgerPrefix, g.Instance, g.ID)
	archiveOutput, err := g.Run(ctx, g.Executor, []string{
		"ACTION=blob", "INPUT=" + g.Output,
		"ARTIFACT_FORMAT=" + GeneralLedgerExportFormat,
		"ARTIFACT_ID=" + g.ID, "INSTANCE=" + g.Instance,
		"OBJECT_STORE_ID=" + g.ObjectStoreID, "S3_BUCKET=" + g.Bucket,
		"S3_OBJECT_KEY=" + ledgerKey, "RETENTION_MODE=" + g.RetentionMode,
		"RETAIN_UNTIL_UNIX=" + strconv.FormatInt(retainUntil, 10),
		"RECEIPT_OUTPUT=" + path.Join(dir, "general-ledger.receipt.json"),
	})
	if err != nil {
		return SettlementStatus[GeneralLedgerExport]{}, archiveOutput, fmt.Errorf(
			"general ledger export Object Lock archive failed: %w: %s",
			err, strings.TrimSpace(string(archiveOutput)),
		)
	}
	if _, err := parseBlobReceipt(
		archiveOutput, GeneralLedgerExportFormat, g.ID, g.Instance,
		g.ObjectStoreID, g.Bucket, ledgerKey, retainUntil, status.SHA256, status.Bytes,
	); err != nil {
		return SettlementStatus[GeneralLedgerExport]{}, archiveOutput, err
	}
	return status, archiveOutput, nil
}

func (g *GeneralLedgerExporter) readImmutable(
	ctx context.Context,
	format, artifactID, objectKey, outputPath string,
	minRetainUntil int64,
) (Source, []byte, error) {
	output, err := g.Run(ctx, g.Executor, []string{
		"ACTION=blob-read", "OUTPUT=" + outputPath, "ARTIFACT_FORMAT=" + format,
		"ARTIFACT_ID=" + artifactID, "INSTANCE=" + g.Instance,
		"OBJECT_STORE_ID=" + g.ObjectStoreID, "S3_BUCKET=" + g.Bucket,
		"S3_OBJECT_KEY=" + objectKey,
		"MIN_RETAIN_UNTIL_UNIX=" + strconv.FormatInt(minRetainUntil, 10),
	})
	if err != nil {
		return Source{}, output, fmt.Errorf("%w: %s", err, strings.TrimSpace(string(output)))
	}
	receipt, err := parseBlobReceipt(
		output, format, artifactID, g.Instance, g.ObjectStoreID, g.Bucket,
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
		return Source{}, output, errors.New("general ledger receipt does not match downloaded bytes")
	}
	return Source{
		ArtifactFormat: receipt.ArtifactFormat, ArtifactID: receipt.ArtifactID,
		ObjectKey: receipt.ObjectKey, VersionID: receipt.VersionID,
		ArtifactSHA256: receipt.ArtifactSHA256, ObjectBytes: receipt.ObjectBytes,
		RetainUntilUnix: receipt.RetainUntilUnix,
	}, output, nil
}

func appendPaymentLedgerLines(lines []GeneralLedgerLine, ledger *PaymentLedger, source Source) []GeneralLedgerLine {
	if ledger.PaymentsMicros > 0 {
		lines = append(lines,
			generalLedgerLine("payment-cash", "cash", "debit", ledger.PaymentsMicros, source, "payments"),
			generalLedgerLine("payment-ar", "accounts_receivable", "credit", ledger.PaymentsMicros, source, "payments"),
		)
	}
	if ledger.RefundsMicros > 0 {
		lines = append(lines,
			generalLedgerLine("refund-ar", "accounts_receivable", "debit", ledger.RefundsMicros, source, "refunds"),
			generalLedgerLine("refund-cash", "cash", "credit", ledger.RefundsMicros, source, "refunds"),
		)
	}
	if ledger.ChargebacksMicros > 0 {
		lines = append(lines,
			generalLedgerLine("chargeback-ar", "accounts_receivable", "debit", ledger.ChargebacksMicros, source, "chargebacks"),
			generalLedgerLine("chargeback-cash", "cash", "credit", ledger.ChargebacksMicros, source, "chargebacks"),
		)
	}
	return lines
}

func generalLedgerLine(prefix, account, direction string, amount int64, source Source, sourceLineID string) GeneralLedgerLine {
	return GeneralLedgerLine{
		LineID:       generalLedgerLineID(prefix, source.ArtifactFormat, source.ArtifactID, sourceLineID),
		Account:      account,
		Direction:    direction,
		AmountMicros: amount,
		SourceFormat: source.ArtifactFormat,
		SourceID:     source.ArtifactID,
		SourceLineID: sourceLineID,
	}
}

func normalizeGeneralLedgerLines(lines []GeneralLedgerLine) ([]GeneralLedgerLine, int64, int64, error) {
	sort.Slice(lines, func(i, j int) bool { return lines[i].LineID < lines[j].LineID })
	var debit, credit int64
	previous := ""
	for _, line := range lines {
		if !versionPattern.MatchString(line.Account) ||
			!validProviderReference(line.LineID) ||
			!validProviderReference(line.SourceFormat) ||
			(line.SourceLineID != "" && !validProviderReference(line.SourceLineID)) ||
			!versionPattern.MatchString(line.SourceID) ||
			line.AmountMicros <= 0 {
			return nil, 0, 0, errors.New("general ledger line is incomplete")
		}
		if previous != "" && line.LineID <= previous {
			return nil, 0, 0, errors.New("general ledger lines are not unique and sorted")
		}
		var ok bool
		switch line.Direction {
		case "debit":
			debit, ok = addInt64(debit, line.AmountMicros)
		case "credit":
			credit, ok = addInt64(credit, line.AmountMicros)
		default:
			return nil, 0, 0, errors.New("general ledger line direction is unsupported")
		}
		if !ok {
			return nil, 0, 0, errors.New("general ledger totals overflow int64")
		}
		previous = line.LineID
	}
	if debit != credit {
		return nil, 0, 0, errors.New("general ledger export is not balanced")
	}
	return lines, debit, credit, nil
}

func generalLedgerLineID(prefix string, parts ...string) string {
	sum := sha256.Sum256([]byte(strings.Join(parts, "\x00")))
	return prefix + "-" + hex.EncodeToString(sum[:6])
}

func generalLedgerSourceKey(format, id string) string {
	return format + "\x00" + id
}
