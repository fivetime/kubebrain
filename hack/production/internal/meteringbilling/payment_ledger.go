package meteringbilling

import (
	"bytes"
	"context"
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"slices"
	"sort"
	"strconv"
	"strings"
	"time"
)

const PaymentLedgerFormat = "kubebrain.metering-payment-ledger.v1"

var paymentLedgerCSVHeader = []string{
	"processor",
	"external_transaction_id",
	"kind",
	"amount_micros",
	"occurred_at_unix",
}

var paymentEntryKinds = map[string]struct{}{
	"payment":    {},
	"refund":     {},
	"chargeback": {},
}

type PaymentLedgerImportOptions struct {
	ID              string
	GeneratedAtUnix int64
}

type PaymentEntry struct {
	Processor             string `json:"processor"`
	ExternalTransactionID string `json:"external_transaction_id"`
	Kind                  string `json:"kind"`
	AmountMicros          int64  `json:"amount_micros"`
	OccurredAtUnix        int64  `json:"occurred_at_unix"`
}

type PaymentLedger struct {
	Format             string         `json:"format"`
	ID                 string         `json:"id"`
	InvoiceID          string         `json:"invoice_id"`
	Instance           string         `json:"instance"`
	PeriodStartUnix    int64          `json:"period_start_unix"`
	PeriodEndUnix      int64          `json:"period_end_unix"`
	Currency           string         `json:"currency"`
	InvoiceSource      Source         `json:"invoice_source"`
	InvoiceFinalizedAt int64          `json:"invoice_finalized_at_unix"`
	InvoiceTotalMicros int64          `json:"invoice_total_micros"`
	Entries            []PaymentEntry `json:"entries"`
	PaymentsMicros     int64          `json:"payments_micros"`
	RefundsMicros      int64          `json:"refunds_micros"`
	ChargebacksMicros  int64          `json:"chargebacks_micros"`
	NetPaidMicros      int64          `json:"net_paid_micros"`
	BalanceMicros      int64          `json:"balance_micros"`
	GeneratedAtUnix    int64          `json:"generated_at_unix"`
}

func BuildPaymentLedgerFromCSV(
	reader io.Reader,
	invoice Invoice,
	invoiceSource Source,
	options PaymentLedgerImportOptions,
) (PaymentLedger, error) {
	if reader == nil {
		return PaymentLedger{}, errors.New("payment ledger CSV input is empty")
	}
	if err := invoice.Validate(); err != nil {
		return PaymentLedger{}, err
	}
	if err := validateSource(invoiceSource, InvoiceFormat, invoice.ID, invoice.PeriodEndUnix); err != nil {
		return PaymentLedger{}, fmt.Errorf("invoice source: %w", err)
	}
	csvReader := csv.NewReader(reader)
	csvReader.FieldsPerRecord = -1
	records, err := csvReader.ReadAll()
	if err != nil {
		return PaymentLedger{}, fmt.Errorf("read payment ledger CSV: %w", err)
	}
	if len(records) < 2 {
		return PaymentLedger{}, errors.New("payment ledger CSV lacks transactions")
	}
	if !slices.Equal(records[0], paymentLedgerCSVHeader) {
		return PaymentLedger{}, errors.New("payment ledger CSV header is unsupported")
	}
	entries := make([]PaymentEntry, 0, len(records)-1)
	for index, record := range records[1:] {
		if len(record) != len(paymentLedgerCSVHeader) {
			return PaymentLedger{}, fmt.Errorf("payment ledger CSV line %d is incomplete", index+2)
		}
		amount, parseErr := strconv.ParseInt(record[3], 10, 64)
		if parseErr != nil {
			return PaymentLedger{}, fmt.Errorf("payment ledger CSV line %d amount is invalid", index+2)
		}
		occurredAt, parseErr := strconv.ParseInt(record[4], 10, 64)
		if parseErr != nil {
			return PaymentLedger{}, fmt.Errorf("payment ledger CSV line %d timestamp is invalid", index+2)
		}
		entry := PaymentEntry{
			Processor: record[0], ExternalTransactionID: record[1], Kind: record[2],
			AmountMicros: amount, OccurredAtUnix: occurredAt,
		}
		if err := entry.validate(options.GeneratedAtUnix); err != nil {
			return PaymentLedger{}, fmt.Errorf("payment ledger CSV line %d: %w", index+2, err)
		}
		entries = append(entries, entry)
	}
	sort.Slice(entries, func(i, j int) bool {
		return paymentEntrySortKey(entries[i]) < paymentEntrySortKey(entries[j])
	})
	payments, refunds, chargebacks, netPaid, balance, err := paymentLedgerTotals(
		invoice.TotalMicros, entries, options.GeneratedAtUnix,
	)
	if err != nil {
		return PaymentLedger{}, err
	}
	ledger := PaymentLedger{
		Format: PaymentLedgerFormat, ID: options.ID, InvoiceID: invoice.ID,
		Instance: invoice.Instance, PeriodStartUnix: invoice.PeriodStartUnix,
		PeriodEndUnix: invoice.PeriodEndUnix, Currency: invoice.Currency,
		InvoiceSource: invoiceSource, InvoiceFinalizedAt: invoice.FinalizedAtUnix,
		InvoiceTotalMicros: invoice.TotalMicros, Entries: entries,
		PaymentsMicros: payments, RefundsMicros: refunds,
		ChargebacksMicros: chargebacks, NetPaidMicros: netPaid, BalanceMicros: balance,
		GeneratedAtUnix: options.GeneratedAtUnix,
	}
	if err := ledger.Validate(); err != nil {
		return PaymentLedger{}, err
	}
	return ledger, nil
}

func (l PaymentLedger) Validate() error {
	if l.Format != PaymentLedgerFormat || !versionPattern.MatchString(l.ID) ||
		!versionPattern.MatchString(l.InvoiceID) ||
		!versionPattern.MatchString(l.Instance) || l.PeriodStartUnix <= 0 ||
		l.PeriodEndUnix <= l.PeriodStartUnix || l.PeriodStartUnix%86400 != 0 ||
		l.PeriodEndUnix%86400 != 0 || !currencyPattern.MatchString(l.Currency) ||
		validateSource(l.InvoiceSource, InvoiceFormat, l.InvoiceID, l.PeriodEndUnix) != nil ||
		l.InvoiceFinalizedAt < l.PeriodEndUnix || l.GeneratedAtUnix < l.InvoiceFinalizedAt ||
		l.InvoiceTotalMicros < 0 || len(l.Entries) == 0 {
		return errors.New("payment ledger is incomplete")
	}
	previous := ""
	for _, entry := range l.Entries {
		if err := entry.validate(l.GeneratedAtUnix); err != nil {
			return err
		}
		key := paymentEntrySortKey(entry)
		if previous != "" && key <= previous {
			return errors.New("payment ledger transactions are not unique and sorted")
		}
		previous = key
	}
	payments, refunds, chargebacks, netPaid, balance, err := paymentLedgerTotals(
		l.InvoiceTotalMicros, l.Entries, l.GeneratedAtUnix,
	)
	if err != nil {
		return err
	}
	if payments != l.PaymentsMicros || refunds != l.RefundsMicros ||
		chargebacks != l.ChargebacksMicros || netPaid != l.NetPaidMicros ||
		balance != l.BalanceMicros {
		return errors.New("payment ledger totals do not match transactions")
	}
	return nil
}

func (e PaymentEntry) validate(generatedAtUnix int64) error {
	if !versionPattern.MatchString(e.Processor) ||
		!validProviderReference(e.ExternalTransactionID) || e.AmountMicros <= 0 ||
		e.OccurredAtUnix <= 0 || e.OccurredAtUnix > generatedAtUnix {
		return errors.New("payment ledger transaction is incomplete")
	}
	if _, ok := paymentEntryKinds[e.Kind]; !ok {
		return errors.New("payment ledger transaction kind is unsupported")
	}
	return nil
}

func paymentLedgerTotals(
	invoiceTotal int64,
	entries []PaymentEntry,
	generatedAtUnix int64,
) (payments, refunds, chargebacks, netPaid, balance int64, err error) {
	for _, entry := range entries {
		if err := entry.validate(generatedAtUnix); err != nil {
			return 0, 0, 0, 0, 0, err
		}
		switch entry.Kind {
		case "payment":
			payments, err = addPaymentAmount(payments, entry.AmountMicros)
		case "refund":
			refunds, err = addPaymentAmount(refunds, entry.AmountMicros)
		case "chargeback":
			chargebacks, err = addPaymentAmount(chargebacks, entry.AmountMicros)
		}
		if err != nil {
			return 0, 0, 0, 0, 0, err
		}
	}
	var ok bool
	netPaid, ok = addInt64(payments, -refunds)
	if !ok {
		return 0, 0, 0, 0, 0, errors.New("payment ledger net paid overflows int64")
	}
	netPaid, ok = addInt64(netPaid, -chargebacks)
	if !ok || netPaid < 0 {
		return 0, 0, 0, 0, 0, errors.New("payment ledger net paid is invalid")
	}
	balance, ok = addInt64(invoiceTotal, -netPaid)
	if !ok || balance < 0 {
		return 0, 0, 0, 0, 0, errors.New("payment ledger balance is invalid")
	}
	return payments, refunds, chargebacks, netPaid, balance, nil
}

func addPaymentAmount(current, amount int64) (int64, error) {
	next, ok := addInt64(current, amount)
	if !ok {
		return 0, errors.New("payment ledger total overflows int64")
	}
	return next, nil
}

func ReadPaymentLedger(filePath string) (SettlementStatus[PaymentLedger], error) {
	return readSettlement(filePath, "payment ledger", func(value PaymentLedger) error {
		return value.Validate()
	})
}

func WritePaymentLedgerAtomic(filePath string, value PaymentLedger) (SettlementStatus[PaymentLedger], error) {
	return writeSettlement(filePath, "payment ledger", value, value.Validate, ReadPaymentLedger)
}

type PaymentLedgerPublisher struct {
	Input             string
	Executor          string
	ObjectStoreID     string
	Bucket            string
	PaymentPrefix     string
	RetentionMode     string
	RetentionDuration time.Duration
	Now               func() time.Time
	Run               CommandRunner
}

func (p *PaymentLedgerPublisher) Publish(ctx context.Context) (SettlementStatus[PaymentLedger], []byte, error) {
	p.PaymentPrefix = strings.Trim(p.PaymentPrefix, "/")
	if p.Input == "" || p.Executor == "" || p.ObjectStoreID == "" || p.Bucket == "" ||
		p.PaymentPrefix == "" ||
		(p.RetentionMode != "COMPLIANCE" && p.RetentionMode != "GOVERNANCE") ||
		p.RetentionDuration <= 24*time.Hour {
		return SettlementStatus[PaymentLedger]{}, nil, errors.New("payment ledger publisher configuration is incomplete")
	}
	if p.Now == nil {
		p.Now = time.Now
	}
	if p.Run == nil {
		p.Run = runCommand
	}
	status, err := ReadPaymentLedger(p.Input)
	if err != nil {
		return SettlementStatus[PaymentLedger]{}, nil, err
	}
	now := p.Now().UTC()
	if status.Value.GeneratedAtUnix > now.Unix() {
		return SettlementStatus[PaymentLedger]{}, nil, errors.New("payment ledger timestamp is in the future")
	}
	retainUntil := time.Unix(status.Value.PeriodStartUnix, 0).
		Add(time.Hour).Add(p.RetentionDuration).Unix()
	if status.Value.InvoiceSource.RetainUntilUnix < retainUntil || retainUntil <= now.Unix() {
		return SettlementStatus[PaymentLedger]{}, nil, errors.New("payment ledger evidence retention is insufficient")
	}
	dir, err := os.MkdirTemp("", "kubebrain-payment-ledger-publish-*")
	if err != nil {
		return SettlementStatus[PaymentLedger]{}, nil, err
	}
	defer os.RemoveAll(dir)
	objectKey := settlementObjectKey(p.PaymentPrefix, status.Value.Instance, status.Value.ID)
	output, err := p.Run(ctx, p.Executor, []string{
		"ACTION=blob", "INPUT=" + p.Input,
		"ARTIFACT_FORMAT=" + PaymentLedgerFormat,
		"ARTIFACT_ID=" + status.Value.ID, "INSTANCE=" + status.Value.Instance,
		"OBJECT_STORE_ID=" + p.ObjectStoreID, "S3_BUCKET=" + p.Bucket,
		"S3_OBJECT_KEY=" + objectKey, "RETENTION_MODE=" + p.RetentionMode,
		"RETAIN_UNTIL_UNIX=" + strconv.FormatInt(retainUntil, 10),
		"RECEIPT_OUTPUT=" + path.Join(dir, "payment-ledger.receipt.json"),
	})
	if err != nil {
		return SettlementStatus[PaymentLedger]{}, output, fmt.Errorf(
			"payment ledger Object Lock publisher failed: %w: %s",
			err, strings.TrimSpace(string(output)),
		)
	}
	if _, err := parseBlobReceipt(
		output, PaymentLedgerFormat, status.Value.ID, status.Value.Instance,
		p.ObjectStoreID, p.Bucket, objectKey, retainUntil, status.SHA256, status.Bytes,
	); err != nil {
		return SettlementStatus[PaymentLedger]{}, output, err
	}
	return status, output, nil
}

func EncodePaymentLedgerCSVHeader() []byte {
	var buffer bytes.Buffer
	writer := csv.NewWriter(&buffer)
	_ = writer.Write(paymentLedgerCSVHeader)
	writer.Flush()
	return buffer.Bytes()
}

func paymentEntrySortKey(entry PaymentEntry) string {
	return strings.Join([]string{entry.Processor, entry.ExternalTransactionID}, "\x00")
}
