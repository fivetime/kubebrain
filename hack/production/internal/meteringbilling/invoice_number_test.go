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

func TestBuildInvoiceNumberAssignmentBindsInvoiceAndDisplayNumber(t *testing.T) {
	invoice, invoiceSource := paymentLedgerInvoice(t)
	assignment, err := BuildInvoiceNumberAssignment(
		invoice,
		invoiceSource,
		InvoiceNumberAssignmentOptions{
			ID: "invoice-number-july", Jurisdiction: "us-ca", Series: "kb-us-2026",
			Sequence: 7, AssignedAtUnix: invoice.FinalizedAtUnix + 1,
		},
	)
	require.NoError(t, err)
	require.Equal(t, InvoiceNumberAssignmentFormat, assignment.Format)
	require.Equal(t, "kb-us-2026-000000000007", assignment.DisplayNumber)
	require.Equal(t, invoice.TotalMicros, assignment.InvoiceTotalMicros)
	require.Equal(t, invoiceSource, assignment.InvoiceSource)
	output := filepath.Join(t.TempDir(), "invoice-number.json")
	status, err := WriteInvoiceNumberAssignmentAtomic(output, assignment)
	require.NoError(t, err)
	read, err := ReadInvoiceNumberAssignment(output)
	require.NoError(t, err)
	require.Equal(t, status, read)
}

func TestInvoiceNumberAssignmentRejectsSourceAndDisplayTamper(t *testing.T) {
	invoice, invoiceSource := paymentLedgerInvoice(t)
	wrongSource := invoiceSource
	wrongSource.ArtifactID = "other-invoice"
	_, err := BuildInvoiceNumberAssignment(
		invoice,
		wrongSource,
		InvoiceNumberAssignmentOptions{
			ID: "invoice-number-july", Jurisdiction: "us-ca", Series: "kb-us-2026",
			Sequence: 7, AssignedAtUnix: invoice.FinalizedAtUnix + 1,
		},
	)
	require.ErrorContains(t, err, "invoice source")

	assignment, err := BuildInvoiceNumberAssignment(
		invoice,
		invoiceSource,
		InvoiceNumberAssignmentOptions{
			ID: "invoice-number-july", Jurisdiction: "us-ca", Series: "kb-us-2026",
			Sequence: 7, AssignedAtUnix: invoice.FinalizedAtUnix + 1,
		},
	)
	require.NoError(t, err)
	assignment.DisplayNumber = "kb-us-2026-000000000008"
	require.ErrorContains(t, assignment.Validate(), "incomplete")
}

func TestBuildInvoiceNumberAssignmentWithInvoiceStatusRejectsSourceBytesDrift(t *testing.T) {
	invoice, invoiceSource := paymentLedgerInvoice(t)
	output := filepath.Join(t.TempDir(), "invoice.json")
	invoiceStatus, err := WriteInvoiceAtomic(output, invoice)
	require.NoError(t, err)
	invoiceSource.ArtifactSHA256 = strings.Repeat("b", 64)
	_, err = BuildInvoiceNumberAssignmentWithInvoiceStatus(
		invoiceStatus,
		invoiceSource,
		InvoiceNumberAssignmentOptions{
			ID: "invoice-number-july", Jurisdiction: "us-ca", Series: "kb-us-2026",
			Sequence: 7, AssignedAtUnix: invoice.FinalizedAtUnix + 1,
		},
	)
	require.ErrorContains(t, err, "invoice bytes")
}

func TestInvoiceNumberAssignerReadsExactInvoiceAndArchivesResult(t *testing.T) {
	invoice, invoiceSource := paymentLedgerInvoice(t)
	assignedAt := invoice.FinalizedAtUnix + 3600
	retainUntil := time.Unix(invoice.PeriodStartUnix, 0).Add(time.Hour + 7*24*time.Hour).Unix()
	invoiceBytes := canonicalInvoiceBytes(t, invoice)
	outputPath := filepath.Join(t.TempDir(), "invoice-number.json")
	var archived []byte
	run := func(_ context.Context, executable string, environment []string) ([]byte, error) {
		require.Equal(t, "executor", executable)
		values := envMap(environment)
		switch values["ACTION"] {
		case "blob-read":
			require.Equal(t, InvoiceFormat, values["ARTIFACT_FORMAT"])
			require.Equal(t, invoiceSource.ObjectKey, values["S3_OBJECT_KEY"])
			require.NoError(t, os.WriteFile(values["OUTPUT"], invoiceBytes, 0o600))
			return settlementReadReceipt(t, invoiceSource, values), nil
		case "blob":
			require.Equal(t, InvoiceNumberAssignmentFormat, values["ARTIFACT_FORMAT"])
			require.Equal(t, "invoice-numbers/instance-a/invoice-number-july.json", values["S3_OBJECT_KEY"])
			require.NotEqual(t, outputPath, values["INPUT"])
			require.NoError(t, os.WriteFile(outputPath, []byte("{}\n"), 0o600))
			data, err := os.ReadFile(values["INPUT"])
			require.NoError(t, err)
			archived = append([]byte(nil), data...)
			sum := sha256.Sum256(data)
			receipt := objectReceipt{
				Format:         "kubebrain.object-immutable-blob.receipt.v1",
				ArtifactFormat: InvoiceNumberAssignmentFormat,
				ArtifactID:     "invoice-number-july",
				Instance:       "instance-a", ObjectStoreID: "store", Bucket: "billing",
				ObjectKey: values["S3_OBJECT_KEY"], VersionID: "invoice-number-version",
				ArtifactSHA256: hex.EncodeToString(sum[:]), ObjectBytes: int64(len(data)),
				RetentionMode: "COMPLIANCE", RetainUntilUnix: retainUntil,
				RemoteVerified: true, ArchivedAtUnix: assignedAt + 3600,
			}
			return json.Marshal(receipt)
		default:
			t.Fatalf("unexpected action %q", values["ACTION"])
		}
		return nil, nil
	}
	assigner := &InvoiceNumberAssigner{
		Output: outputPath, ID: "invoice-number-july", Instance: "instance-a",
		InvoiceID: invoice.ID, Jurisdiction: "us-ca", Series: "kb-us-2026", Sequence: 7,
		Executor: "executor", ObjectStoreID: "store", Bucket: "billing",
		InvoicePrefix: "invoices", NumberPrefix: "invoice-numbers",
		RetentionMode: "COMPLIANCE", RetentionDuration: 7 * 24 * time.Hour,
		AssignedAtUnix: assignedAt, Publish: true,
		Now: func() time.Time { return time.Unix(assignedAt+3600, 0).UTC() }, Run: run,
	}
	status, _, err := assigner.Process(context.Background())
	require.NoError(t, err)
	require.NotEmpty(t, archived)
	require.FileExists(t, outputPath)
	data, err := os.ReadFile(outputPath)
	require.NoError(t, err)
	require.Equal(t, []byte("{}\n"), data)
	archivedPath := filepath.Join(t.TempDir(), "archived-invoice-number.json")
	require.NoError(t, os.WriteFile(archivedPath, archived, 0o600))
	read, err := ReadInvoiceNumberAssignment(archivedPath)
	require.NoError(t, err)
	require.Equal(t, status, read)
}
