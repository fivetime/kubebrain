package meteringbilling

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestBuildProviderReconciliationAggregatesCrossAccountProviderCosts(t *testing.T) {
	start := time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC)
	charge, chargeSource := settlementCharge(t, start)
	plan := validInvoicePlan(start, charge.Format, nil)
	invoice, err := BuildInvoice(
		plan, settlementSource(InvoicePlanFormat, plan.ID, "plans/invoice-july.json"),
		[]Charge{charge}, []Source{chargeSource}, nil, nil,
		plan.Approval.ApprovedAtUnix+1,
	)
	require.NoError(t, err)
	statement := validProviderStatement(start)
	reconciliation, err := BuildProviderReconciliation(
		"reconcile-july", statement,
		settlementSource(ProviderStatementFormat, statement.ID, "providers/provider-july.json"),
		invoice, settlementSource(InvoiceFormat, invoice.ID, "invoices/invoice-july.json"),
		start.Add(72*time.Hour).Unix(),
	)
	require.NoError(t, err)
	require.Equal(t, ProviderReconciliationFormat, reconciliation.Format)
	require.Equal(t, statement.TotalMicros, reconciliation.ProviderStatementTotalMicros)
	require.Equal(t, invoice.TotalMicros, reconciliation.CustomerInvoiceTotalMicros)
	require.Equal(t, invoice.TotalMicros-statement.TotalMicros, reconciliation.GrossMarginMicros)
	require.Equal(t, []ProviderAllocation{
		{
			Provider: "aws", ProviderAccountID: "acct-a", Category: "compute",
			AmountMicros: 300,
		},
		{
			Provider: "aws", ProviderAccountID: "acct-a", Category: "object_storage",
			ObjectStoreID: "store-a", Bucket: "bucket-a", AmountMicros: 30,
		},
		{
			Provider: "aws", ProviderAccountID: "acct-b", Category: "object_storage",
			ObjectStoreID: "store-b", Bucket: "bucket-b", AmountMicros: 70,
		},
	}, reconciliation.ProviderAllocations)
	require.NoError(t, reconciliation.Validate())

	output := filepath.Join(t.TempDir(), "reconciliation.json")
	status, err := WriteProviderReconciliationAtomic(output, reconciliation)
	require.NoError(t, err)
	read, err := ReadProviderReconciliation(output)
	require.NoError(t, err)
	require.Equal(t, status, read)
}

func TestBuildProviderReconciliationWithStatusesRejectsSourceBytesDrift(t *testing.T) {
	start := time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC)
	charge, chargeSource := settlementCharge(t, start)
	plan := validInvoicePlan(start, charge.Format, nil)
	invoice, err := BuildInvoice(
		plan, settlementSource(InvoicePlanFormat, plan.ID, "plans/invoice-july.json"),
		[]Charge{charge}, []Source{chargeSource}, nil, nil,
		plan.Approval.ApprovedAtUnix+1,
	)
	require.NoError(t, err)
	statement := validProviderStatement(start)
	dir := t.TempDir()
	statementStatus, err := WriteProviderStatementAtomic(
		filepath.Join(dir, "provider-statement.json"), statement,
	)
	require.NoError(t, err)
	invoiceStatus, err := WriteInvoiceAtomic(filepath.Join(dir, "invoice.json"), invoice)
	require.NoError(t, err)
	retainUntil := start.Add(7 * 24 * time.Hour).Unix()
	statementSource := Source{
		ArtifactFormat: ProviderStatementFormat, ArtifactID: statement.ID,
		ObjectKey: "providers/instance-a/provider-july.json", VersionID: "provider-version",
		ArtifactSHA256: statementStatus.SHA256, ObjectBytes: statementStatus.Bytes,
		RetainUntilUnix: retainUntil,
	}
	invoiceSource := Source{
		ArtifactFormat: InvoiceFormat, ArtifactID: invoice.ID,
		ObjectKey: "invoices/instance-a/invoice-july.json", VersionID: "invoice-version",
		ArtifactSHA256: invoiceStatus.SHA256, ObjectBytes: invoiceStatus.Bytes,
		RetainUntilUnix: retainUntil,
	}
	reconciledAt := start.Add(72 * time.Hour).Unix()
	_, err = BuildProviderReconciliationWithStatuses(
		"reconcile-july", statementStatus, statementSource, invoiceStatus, invoiceSource, reconciledAt,
	)
	require.NoError(t, err)

	badStatementSource := statementSource
	badStatementSource.ArtifactSHA256 = strings.Repeat("b", 64)
	_, err = BuildProviderReconciliationWithStatuses(
		"reconcile-july", statementStatus, badStatementSource, invoiceStatus, invoiceSource, reconciledAt,
	)
	require.ErrorContains(t, err, "statement bytes")

	badInvoiceSource := invoiceSource
	badInvoiceSource.ArtifactSHA256 = strings.Repeat("b", 64)
	_, err = BuildProviderReconciliationWithStatuses(
		"reconcile-july", statementStatus, statementSource, invoiceStatus, badInvoiceSource, reconciledAt,
	)
	require.ErrorContains(t, err, "invoice bytes")
}

func TestProviderStatementRejectsUnsortedIncompleteAndTamperedCosts(t *testing.T) {
	start := time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC)
	statement := validProviderStatement(start)
	require.NoError(t, statement.Validate())

	unsorted := statement
	unsorted.Lines = append([]ProviderStatementLine(nil), statement.Lines...)
	unsorted.Lines[0], unsorted.Lines[1] = unsorted.Lines[1], unsorted.Lines[0]
	require.ErrorContains(t, unsorted.Validate(), "sorted")

	missingBucket := statement
	missingBucket.Lines = append([]ProviderStatementLine(nil), statement.Lines...)
	missingBucket.Lines[1].Bucket = ""
	require.ErrorContains(t, missingBucket.Validate(), "incomplete")

	tampered := statement
	tampered.TotalMicros++
	require.ErrorContains(t, tampered.Validate(), "total")
}

func TestProviderReconcilerReadsExactArtifactsAndArchivesResult(t *testing.T) {
	start := time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC)
	retainUntil := start.Add(time.Hour + 7*24*time.Hour).Unix()
	now := start.Add(72 * time.Hour)

	charge, chargeSource := settlementCharge(t, start)
	plan := validInvoicePlan(start, charge.Format, nil)
	invoice, err := BuildInvoice(
		plan, settlementSource(InvoicePlanFormat, plan.ID, "plan"),
		[]Charge{charge}, []Source{chargeSource}, nil, nil,
		plan.Approval.ApprovedAtUnix+1,
	)
	require.NoError(t, err)
	invoiceBytes := canonicalInvoiceBytes(t, invoice)
	invoiceKey := "invoices/instance-a/invoice-july.json"
	invoiceSource := sourceForBytes(InvoiceFormat, invoice.ID, invoiceKey, invoiceBytes, retainUntil)

	statement := validProviderStatement(start)
	statementBytes := canonicalProviderStatementBytes(t, statement)
	statementKey := "providers/instance-a/provider-july.json"
	statementSource := sourceForBytes(
		ProviderStatementFormat, statement.ID, statementKey, statementBytes, retainUntil,
	)

	objects := map[string][]byte{statementKey: statementBytes, invoiceKey: invoiceBytes}
	sources := map[string]Source{statementKey: statementSource, invoiceKey: invoiceSource}
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
			require.Equal(t, ProviderReconciliationFormat, values["ARTIFACT_FORMAT"])
			require.Equal(t, "reconcile-july", values["ARTIFACT_ID"])
			require.Equal(t, "reconciliations/instance-a/reconcile-july.json", values["S3_OBJECT_KEY"])
			data, err := os.ReadFile(values["INPUT"])
			require.NoError(t, err)
			archived = append([]byte(nil), data...)
			sum := sha256.Sum256(data)
			receipt := objectReceipt{
				Format:         "kubebrain.object-immutable-blob.receipt.v1",
				ArtifactFormat: ProviderReconciliationFormat, ArtifactID: "reconcile-july",
				Instance: "instance-a", ObjectStoreID: "store", Bucket: "billing",
				ObjectKey: values["S3_OBJECT_KEY"], VersionID: "reconciliation-version",
				ArtifactSHA256: hex.EncodeToString(sum[:]), ObjectBytes: int64(len(data)),
				RetentionMode: "COMPLIANCE", RetainUntilUnix: retainUntil,
				RemoteVerified: true, ArchivedAtUnix: now.Unix(),
			}
			return json.Marshal(receipt)
		default:
			t.Fatalf("unexpected action %q", values["ACTION"])
		}
		return nil, nil
	}
	reconciler := &ProviderReconciler{
		Instance: "instance-a", ProviderStatementID: statement.ID, InvoiceID: invoice.ID,
		ReconciliationID: "reconcile-july", Executor: "executor",
		ObjectStoreID: "store", Bucket: "billing", ProviderPrefix: "providers",
		InvoicePrefix: "invoices", ReconciliationPrefix: "reconciliations",
		RetentionMode: "COMPLIANCE", RetentionDuration: 7 * 24 * time.Hour,
		Now: func() time.Time { return now }, Run: run,
	}
	reconciliation, _, err := reconciler.Process(context.Background())
	require.NoError(t, err)
	require.Equal(t, statement.TotalMicros, reconciliation.ProviderStatementTotalMicros)
	require.NotEmpty(t, archived)
	readPath := filepath.Join(t.TempDir(), "archived.json")
	require.NoError(t, os.WriteFile(readPath, archived, 0o600))
	read, err := ReadProviderReconciliation(readPath)
	require.NoError(t, err)
	require.Equal(t, reconciliation, read.Value)
}

func TestProviderReconcilerFailsBeforeArchiveOnIdentityDrift(t *testing.T) {
	start := time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC)
	retainUntil := start.Add(2 * time.Hour).Unix()
	now := start.Add(72 * time.Hour)
	charge, chargeSource := settlementCharge(t, start)
	plan := validInvoicePlan(start, charge.Format, nil)
	invoice, err := BuildInvoice(
		plan, settlementSource(InvoicePlanFormat, plan.ID, "plan"),
		[]Charge{charge}, []Source{chargeSource}, nil, nil,
		plan.Approval.ApprovedAtUnix+1,
	)
	require.NoError(t, err)
	statement := validProviderStatement(start)
	statement.Instance = "instance-b"
	statementBytes := canonicalProviderStatementBytes(t, statement)
	statementKey := "providers/instance-a/provider-july.json"
	statementSource := sourceForBytes(
		ProviderStatementFormat, statement.ID, statementKey, statementBytes, retainUntil,
	)
	invoiceBytes := canonicalInvoiceBytes(t, invoice)
	invoiceKey := "invoices/instance-a/invoice-july.json"
	invoiceSource := sourceForBytes(InvoiceFormat, invoice.ID, invoiceKey, invoiceBytes, retainUntil)

	calls := 0
	reconciler := &ProviderReconciler{
		Instance: "instance-a", ProviderStatementID: statement.ID, InvoiceID: invoice.ID,
		ReconciliationID: "reconcile-july", Executor: "executor",
		ObjectStoreID: "store", Bucket: "billing", ProviderPrefix: "providers",
		InvoicePrefix: "invoices", ReconciliationPrefix: "reconciliations",
		RetentionMode: "COMPLIANCE", RetentionDuration: 7 * 24 * time.Hour,
		Now: func() time.Time { return now },
		Run: func(_ context.Context, _ string, environment []string) ([]byte, error) {
			values := envMap(environment)
			calls++
			require.Equal(t, "blob-read", values["ACTION"])
			if values["ARTIFACT_FORMAT"] == ProviderStatementFormat {
				require.NoError(t, os.WriteFile(values["OUTPUT"], statementBytes, 0o600))
				return settlementReadReceipt(t, statementSource, values), nil
			}
			require.NoError(t, os.WriteFile(values["OUTPUT"], invoiceBytes, 0o600))
			return settlementReadReceipt(t, invoiceSource, values), nil
		},
	}
	_, _, err = reconciler.Process(context.Background())
	require.ErrorContains(t, err, "identity")
	require.Equal(t, 1, calls)
}

func validProviderStatement(start time.Time) ProviderStatement {
	lines := []ProviderStatementLine{
		{
			Provider: "aws", ProviderAccountID: "acct-a", ExternalInvoiceID: "aws-inv-2026-07",
			ExternalLineID: "line-001", Category: "compute", AmountMicros: 300,
		},
		{
			Provider: "aws", ProviderAccountID: "acct-a", ExternalInvoiceID: "aws-inv-2026-07",
			ExternalLineID: "line-002", Category: "object_storage", ObjectStoreID: "store-a",
			Bucket: "bucket-a", AmountMicros: 30,
		},
		{
			Provider: "aws", ProviderAccountID: "acct-b", ExternalInvoiceID: "aws-inv-2026-07",
			ExternalLineID: "line-003", Category: "object_storage", ObjectStoreID: "store-b",
			Bucket: "bucket-b", AmountMicros: 70,
		},
	}
	return ProviderStatement{
		Format: ProviderStatementFormat, ID: "provider-july", Instance: "instance-a",
		PeriodStartUnix: start.Unix(), PeriodEndUnix: start.Add(24 * time.Hour).Unix(),
		Currency: "USD", IssuedAtUnix: start.Add(48 * time.Hour).Unix(),
		Lines: lines, TotalMicros: 400,
	}
}

func canonicalProviderStatementBytes(t *testing.T, statement ProviderStatement) []byte {
	t.Helper()
	path := filepath.Join(t.TempDir(), "provider-statement.json")
	_, err := WriteProviderStatementAtomic(path, statement)
	require.NoError(t, err)
	data, err := os.ReadFile(path)
	require.NoError(t, err)
	return data
}

func canonicalInvoiceBytes(t *testing.T, invoice Invoice) []byte {
	t.Helper()
	path := filepath.Join(t.TempDir(), "invoice.json")
	_, err := WriteInvoiceAtomic(path, invoice)
	require.NoError(t, err)
	data, err := os.ReadFile(path)
	require.NoError(t, err)
	return data
}
