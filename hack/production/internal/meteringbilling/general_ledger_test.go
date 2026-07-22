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

func TestBuildGeneralLedgerExportBalancesInvoiceProviderAndPayments(t *testing.T) {
	invoice, invoiceSource, reconciliation, reconciliationSource, ledger, ledgerSource := generalLedgerInputs(t)
	export, err := BuildGeneralLedgerExport(
		invoice, invoiceSource, &reconciliation, &reconciliationSource, &ledger, &ledgerSource,
		GeneralLedgerExportOptions{
			ID:             "ledger-july",
			ExportedAtUnix: ledger.GeneratedAtUnix + 3600,
		},
	)
	require.NoError(t, err)
	require.Equal(t, GeneralLedgerExportFormat, export.Format)
	require.Equal(t, export.DebitTotalMicros, export.CreditTotalMicros)
	accounts := map[string]bool{}
	for _, line := range export.Lines {
		accounts[line.Account] = true
	}
	for _, account := range []string{
		"accounts_receivable", "kubebrain_revenue", "provider_accounts_payable",
		"provider_cost_compute", "provider_cost_object_storage", "cash",
	} {
		require.True(t, accounts[account], account)
	}
	output := filepath.Join(t.TempDir(), "ledger.json")
	status, err := WriteGeneralLedgerExportAtomic(output, export)
	require.NoError(t, err)
	read, err := ReadGeneralLedgerExport(output)
	require.NoError(t, err)
	require.Equal(t, status, read)
}

func TestGeneralLedgerExportRejectsMismatchedInputsAndTamperedTotals(t *testing.T) {
	invoice, invoiceSource, reconciliation, reconciliationSource, ledger, ledgerSource := generalLedgerInputs(t)
	badLedger := ledger
	badLedger.InvoiceSource.VersionID = "other-version"
	_, err := BuildGeneralLedgerExport(
		invoice, invoiceSource, &reconciliation, &reconciliationSource, &badLedger, &ledgerSource,
		GeneralLedgerExportOptions{ID: "ledger-july", ExportedAtUnix: ledger.GeneratedAtUnix + 3600},
	)
	require.ErrorContains(t, err, "payment ledger")

	export, err := BuildGeneralLedgerExport(
		invoice, invoiceSource, &reconciliation, &reconciliationSource, &ledger, &ledgerSource,
		GeneralLedgerExportOptions{ID: "ledger-july", ExportedAtUnix: ledger.GeneratedAtUnix + 3600},
	)
	require.NoError(t, err)
	export.Lines[0].AmountMicros++
	require.ErrorContains(t, export.Validate(), "balanced")
}

func TestGeneralLedgerExporterReadsExactArtifactsAndArchivesResult(t *testing.T) {
	invoice, invoiceSource, reconciliation, reconciliationSource, ledger, ledgerSource := generalLedgerInputs(t)
	exportedAt := ledger.GeneratedAtUnix + 3600
	retainUntil := time.Unix(invoice.PeriodStartUnix, 0).Add(time.Hour + 7*24*time.Hour).Unix()
	invoiceBytes := canonicalInvoiceBytes(t, invoice)
	reconciliationBytes := canonicalProviderReconciliationBytes(t, reconciliation)
	ledgerBytes := canonicalPaymentLedgerBytes(t, ledger)
	objects := map[string][]byte{
		invoiceSource.ObjectKey:        invoiceBytes,
		reconciliationSource.ObjectKey: reconciliationBytes,
		ledgerSource.ObjectKey:         ledgerBytes,
	}
	sources := map[string]Source{
		invoiceSource.ObjectKey:        invoiceSource,
		reconciliationSource.ObjectKey: reconciliationSource,
		ledgerSource.ObjectKey:         ledgerSource,
	}
	var archived []byte
	run := func(_ context.Context, executable string, environment []string) ([]byte, error) {
		require.Equal(t, "executor", executable)
		values := envMap(environment)
		switch values["ACTION"] {
		case "blob-read":
			key := values["S3_OBJECT_KEY"]
			require.NoError(t, os.WriteFile(values["OUTPUT"], objects[key], 0o600))
			return settlementReadReceipt(t, sources[key], values), nil
		case "blob":
			require.Equal(t, GeneralLedgerExportFormat, values["ARTIFACT_FORMAT"])
			require.Equal(t, "ledgers/instance-a/ledger-july.json", values["S3_OBJECT_KEY"])
			data, err := os.ReadFile(values["INPUT"])
			require.NoError(t, err)
			archived = append([]byte(nil), data...)
			sum := sha256.Sum256(data)
			receipt := objectReceipt{
				Format:         "kubebrain.object-immutable-blob.receipt.v1",
				ArtifactFormat: GeneralLedgerExportFormat, ArtifactID: "ledger-july",
				Instance: "instance-a", ObjectStoreID: "store", Bucket: "billing",
				ObjectKey: values["S3_OBJECT_KEY"], VersionID: "ledger-version",
				ArtifactSHA256: hex.EncodeToString(sum[:]), ObjectBytes: int64(len(data)),
				RetentionMode: "COMPLIANCE", RetainUntilUnix: retainUntil,
				RemoteVerified: true, ArchivedAtUnix: exportedAt + 3600,
			}
			return json.Marshal(receipt)
		default:
			t.Fatalf("unexpected action %q", values["ACTION"])
		}
		return nil, nil
	}
	outputPath := filepath.Join(t.TempDir(), "ledger.json")
	exporter := &GeneralLedgerExporter{
		Output: outputPath, ID: "ledger-july", Instance: "instance-a",
		InvoiceID: invoice.ID, ProviderReconciliationID: reconciliation.ID,
		PaymentLedgerID: ledger.ID, Executor: "executor", ObjectStoreID: "store",
		Bucket: "billing", InvoicePrefix: "invoices",
		ProviderReconciliationPrefix: "reconciliations", PaymentPrefix: "payments",
		LedgerPrefix: "ledgers", RetentionMode: "COMPLIANCE",
		RetentionDuration: 7 * 24 * time.Hour, ExportedAtUnix: exportedAt,
		Publish: true, Now: func() time.Time { return time.Unix(exportedAt+3600, 0).UTC() },
		Run: run,
	}
	status, _, err := exporter.Process(context.Background())
	require.NoError(t, err)
	require.NotEmpty(t, archived)
	read, err := ReadGeneralLedgerExport(outputPath)
	require.NoError(t, err)
	require.Equal(t, status, read)
}

func generalLedgerInputs(t *testing.T) (Invoice, Source, ProviderReconciliation, Source, PaymentLedger, Source) {
	t.Helper()
	invoice, invoiceSource := paymentLedgerInvoice(t)
	start := time.Unix(invoice.PeriodStartUnix, 0).UTC()
	statement := validProviderStatement(start)
	statementBytes := canonicalProviderStatementBytes(t, statement)
	statementSource := sourceForBytes(
		ProviderStatementFormat, statement.ID, "providers/instance-a/provider-july.json",
		statementBytes, time.Date(2035, 1, 1, 0, 0, 0, 0, time.UTC).Unix(),
	)
	reconciliation, err := BuildProviderReconciliation(
		"reconcile-july", statement, statementSource, invoice, invoiceSource,
		statement.IssuedAtUnix+1800,
	)
	require.NoError(t, err)
	reconciliationBytes := canonicalProviderReconciliationBytes(t, reconciliation)
	reconciliationSource := sourceForBytes(
		ProviderReconciliationFormat, reconciliation.ID,
		"reconciliations/instance-a/reconcile-july.json",
		reconciliationBytes, statementSource.RetainUntilUnix,
	)
	paymentInput := string(EncodePaymentLedgerCSVHeader()) +
		fmt.Sprintf("stripe,txn-001,payment,%d,%d\n", invoice.TotalMicros+7, invoice.FinalizedAtUnix+3600) +
		fmt.Sprintf("stripe,txn-002,refund,7,%d\n", invoice.FinalizedAtUnix+3700)
	ledger, err := BuildPaymentLedgerFromCSV(
		stringsReader(paymentInput), invoice, invoiceSource,
		PaymentLedgerImportOptions{ID: "payments-july", GeneratedAtUnix: statement.IssuedAtUnix + 3600},
	)
	require.NoError(t, err)
	ledgerBytes := canonicalPaymentLedgerBytes(t, ledger)
	ledgerSource := sourceForBytes(
		PaymentLedgerFormat, ledger.ID, "payments/instance-a/payments-july.json",
		ledgerBytes, statementSource.RetainUntilUnix,
	)
	return invoice, invoiceSource, reconciliation, reconciliationSource, ledger, ledgerSource
}

func canonicalProviderReconciliationBytes(t *testing.T, value ProviderReconciliation) []byte {
	t.Helper()
	output := filepath.Join(t.TempDir(), "provider-reconciliation.json")
	_, err := WriteProviderReconciliationAtomic(output, value)
	require.NoError(t, err)
	data, err := os.ReadFile(output)
	require.NoError(t, err)
	return data
}

func canonicalPaymentLedgerBytes(t *testing.T, value PaymentLedger) []byte {
	t.Helper()
	output := filepath.Join(t.TempDir(), "payment-ledger.json")
	_, err := WritePaymentLedgerAtomic(output, value)
	require.NoError(t, err)
	data, err := os.ReadFile(output)
	require.NoError(t, err)
	return data
}

func stringsReader(value string) *strings.Reader {
	return strings.NewReader(value)
}
