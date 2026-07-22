package meteringbilling

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestBuildPaymentLedgerFromCSVCanonicalizesBalanceAndInvoiceSource(t *testing.T) {
	invoice, invoiceSource := paymentLedgerInvoice(t)
	generated := invoice.FinalizedAtUnix + 3600
	first := invoice.TotalMicros / 2
	refund := int64(7)
	second := invoice.TotalMicros - first + refund
	input := string(EncodePaymentLedgerCSVHeader()) +
		fmt.Sprintf("stripe,txn-002,payment,%d,%d\n", second, generated-20) +
		fmt.Sprintf("stripe,txn-001,payment,%d,%d\n", first, generated-30) +
		fmt.Sprintf("stripe,txn-003,refund,%d,%d\n", refund, generated-10)

	ledger, err := BuildPaymentLedgerFromCSV(
		strings.NewReader(input),
		invoice,
		invoiceSource,
		PaymentLedgerImportOptions{ID: "payments-july", GeneratedAtUnix: generated},
	)
	require.NoError(t, err)
	require.Equal(t, PaymentLedgerFormat, ledger.Format)
	require.Equal(t, invoice.ID, ledger.InvoiceID)
	require.Equal(t, invoiceSource, ledger.InvoiceSource)
	require.Equal(t, invoice.TotalMicros+refund, ledger.PaymentsMicros)
	require.Equal(t, refund, ledger.RefundsMicros)
	require.Equal(t, invoice.TotalMicros, ledger.NetPaidMicros)
	require.Zero(t, ledger.BalanceMicros)
	require.Equal(t, []string{"txn-001", "txn-002", "txn-003"}, []string{
		ledger.Entries[0].ExternalTransactionID,
		ledger.Entries[1].ExternalTransactionID,
		ledger.Entries[2].ExternalTransactionID,
	})

	output := filepath.Join(t.TempDir(), "payment-ledger.json")
	status, err := WritePaymentLedgerAtomic(output, ledger)
	require.NoError(t, err)
	read, err := ReadPaymentLedger(output)
	require.NoError(t, err)
	require.Equal(t, status, read)
}

func TestPaymentLedgerRejectsDuplicateFutureOverRefundAndSourceDrift(t *testing.T) {
	invoice, invoiceSource := paymentLedgerInvoice(t)
	generated := invoice.FinalizedAtUnix + 3600
	validLine := func(body string) string {
		return string(EncodePaymentLedgerCSVHeader()) + body
	}

	duplicate := validLine(
		fmt.Sprintf("stripe,txn-001,payment,10,%d\n", generated-2) +
			fmt.Sprintf("stripe,txn-001,refund,1,%d\n", generated-1),
	)
	_, err := BuildPaymentLedgerFromCSV(
		strings.NewReader(duplicate), invoice, invoiceSource,
		PaymentLedgerImportOptions{ID: "payments-july", GeneratedAtUnix: generated},
	)
	require.ErrorContains(t, err, "unique")

	overRefund := validLine(
		fmt.Sprintf("stripe,txn-001,payment,10,%d\n", generated-2) +
			fmt.Sprintf("stripe,txn-002,refund,11,%d\n", generated-1),
	)
	_, err = BuildPaymentLedgerFromCSV(
		strings.NewReader(overRefund), invoice, invoiceSource,
		PaymentLedgerImportOptions{ID: "payments-july", GeneratedAtUnix: generated},
	)
	require.ErrorContains(t, err, "net paid")

	future := validLine(fmt.Sprintf("stripe,txn-001,payment,10,%d\n", generated+1))
	_, err = BuildPaymentLedgerFromCSV(
		strings.NewReader(future), invoice, invoiceSource,
		PaymentLedgerImportOptions{ID: "payments-july", GeneratedAtUnix: generated},
	)
	require.ErrorContains(t, err, "transaction")

	wrongSource := invoiceSource
	wrongSource.ArtifactID = "other-invoice"
	_, err = BuildPaymentLedgerFromCSV(
		strings.NewReader(validLine(fmt.Sprintf("stripe,txn-001,payment,10,%d\n", generated-1))),
		invoice,
		wrongSource,
		PaymentLedgerImportOptions{ID: "payments-july", GeneratedAtUnix: generated},
	)
	require.ErrorContains(t, err, "invoice source")
}

func TestPaymentLedgerPublisherArchivesExactReceipt(t *testing.T) {
	invoice, invoiceSource := paymentLedgerInvoice(t)
	generated := invoice.FinalizedAtUnix + 3600
	retainUntil := time.Unix(invoice.PeriodStartUnix, 0).Add(time.Hour + 7*24*time.Hour).Unix()
	input := string(EncodePaymentLedgerCSVHeader()) +
		fmt.Sprintf("stripe,txn-001,payment,%d,%d\n", invoice.TotalMicros, generated-1)
	ledger, err := BuildPaymentLedgerFromCSV(
		strings.NewReader(input),
		invoice,
		invoiceSource,
		PaymentLedgerImportOptions{ID: "payments-july", GeneratedAtUnix: generated},
	)
	require.NoError(t, err)
	path := filepath.Join(t.TempDir(), "payment-ledger.json")
	status, err := WritePaymentLedgerAtomic(path, ledger)
	require.NoError(t, err)

	publisher := &PaymentLedgerPublisher{
		Input: path, Executor: "executor", ObjectStoreID: "store", Bucket: "billing",
		PaymentPrefix: "/payments/", RetentionMode: "COMPLIANCE",
		RetentionDuration: 7 * 24 * time.Hour,
		Now:               func() time.Time { return time.Unix(generated+3600, 0).UTC() },
	}
	publisher.Run = func(_ context.Context, executable string, environment []string) ([]byte, error) {
		require.Equal(t, "executor", executable)
		values := envMap(environment)
		require.Equal(t, "blob", values["ACTION"])
		require.Equal(t, PaymentLedgerFormat, values["ARTIFACT_FORMAT"])
		require.Equal(t, "payments/instance-a/payments-july.json", values["S3_OBJECT_KEY"])
		require.Equal(t, "payments-july", values["ARTIFACT_ID"])
		require.Equal(t, "instance-a", values["INSTANCE"])
		require.Equal(t, "store", values["OBJECT_STORE_ID"])
		require.Equal(t, "billing", values["S3_BUCKET"])
		require.Equal(t, "COMPLIANCE", values["RETENTION_MODE"])
		require.Equal(t, fmt.Sprintf("%d", retainUntil), values["RETAIN_UNTIL_UNIX"])
		data, err := os.ReadFile(values["INPUT"])
		require.NoError(t, err)
		sum := sha256.Sum256(data)
		receipt := objectReceipt{
			Format:         "kubebrain.object-immutable-blob.receipt.v1",
			ArtifactFormat: PaymentLedgerFormat, ArtifactID: ledger.ID,
			Instance: ledger.Instance, ObjectStoreID: "store", Bucket: "billing",
			ObjectKey: values["S3_OBJECT_KEY"], VersionID: "payment-ledger-version",
			ArtifactSHA256: hex.EncodeToString(sum[:]), ObjectBytes: int64(len(data)),
			RetentionMode: "COMPLIANCE", RetainUntilUnix: retainUntil,
			RemoteVerified: true, ArchivedAtUnix: generated + 3600,
		}
		return json.Marshal(receipt)
	}
	published, _, err := publisher.Publish(context.Background())
	require.NoError(t, err)
	require.Equal(t, status, published)
}

func paymentLedgerInvoice(t *testing.T) (Invoice, Source) {
	t.Helper()
	start := time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC)
	charge, chargeSource := settlementCharge(t, start)
	plan := validInvoicePlan(start, charge.Format, nil)
	invoice, err := BuildInvoice(
		plan, settlementSource(InvoicePlanFormat, plan.ID, "plans/invoice-july.json"),
		[]Charge{charge}, []Source{chargeSource}, nil, nil,
		plan.Approval.ApprovedAtUnix+1,
	)
	require.NoError(t, err)
	data := canonicalInvoiceBytes(t, invoice)
	return invoice, sourceForBytes(
		InvoiceFormat, invoice.ID, "invoices/instance-a/invoice-july.json", data,
		time.Date(2035, 1, 1, 0, 0, 0, 0, time.UTC).Unix(),
	)
}
