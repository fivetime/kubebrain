package meteringbilling

import (
	"math"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/kubewharf/kubebrain/hack/production/internal/meteringarchive"
	"github.com/kubewharf/kubebrain/hack/production/internal/meteringstorage"
	"github.com/stretchr/testify/require"
)

func TestBuildInvoiceBindsChargesAdjustmentsAndApproval(t *testing.T) {
	start := time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC)
	charge, chargeSource := settlementCharge(t, start)
	plan := validInvoicePlan(start, charge.Format, []string{"credit-001"})
	planSource := settlementSource(InvoicePlanFormat, plan.ID, "plans/invoice-july.json")
	adjustment := validAdjustment(start, chargeSource)
	adjustmentSource := settlementSource(
		AdjustmentFormat, adjustment.ID, "adjustments/credit-001.json",
	)
	invoice, err := BuildInvoice(
		plan, planSource, []Charge{charge}, []Source{chargeSource},
		[]Adjustment{adjustment}, []Source{adjustmentSource},
		plan.Approval.ApprovedAtUnix+1,
	)
	require.NoError(t, err)
	require.Equal(t, charge.TotalMicros, invoice.SubtotalMicros)
	require.Equal(t, int64(-7), invoice.AdjustmentTotalMicros)
	require.Equal(t, charge.TotalMicros-7, invoice.TotalMicros)
	require.NoError(t, invoice.Validate())

	path := filepath.Join(t.TempDir(), "invoice.json")
	first, err := WriteInvoiceAtomic(path, invoice)
	require.NoError(t, err)
	second, err := WriteInvoiceAtomic(path, invoice)
	require.NoError(t, err)
	require.Equal(t, first, second)
}

func TestBuildInvoiceWithStatusesRejectsSourceBytesDrift(t *testing.T) {
	start := time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC)
	charge, _ := settlementCharge(t, start)
	dir := t.TempDir()
	chargeStatus, err := WriteChargeAtomic(filepath.Join(dir, "charge.json"), charge)
	require.NoError(t, err)
	retainUntil := time.Date(2035, 1, 1, 0, 0, 0, 0, time.UTC).Unix()
	chargeSource := Source{
		ArtifactFormat: charge.Format,
		ArtifactID: periodArtifactIDUnix(
			charge.Instance, charge.PeriodStartUnix, charge.PeriodEndUnix,
		),
		ObjectKey: "charges/instance-a/2026/07/01/day.json", VersionID: "charge-version",
		ArtifactSHA256: chargeStatus.SHA256, ObjectBytes: chargeStatus.Bytes,
		RetainUntilUnix: retainUntil,
	}
	adjustment := validAdjustment(start, chargeSource)
	adjustmentStatus, err := WriteAdjustmentAtomic(
		filepath.Join(dir, "adjustment.json"), adjustment,
	)
	require.NoError(t, err)
	adjustmentSource := Source{
		ArtifactFormat: AdjustmentFormat, ArtifactID: adjustment.ID,
		ObjectKey: "adjustments/instance-a/credit-001.json", VersionID: "adjustment-version",
		ArtifactSHA256: adjustmentStatus.SHA256, ObjectBytes: adjustmentStatus.Bytes,
		RetainUntilUnix: retainUntil,
	}
	plan := validInvoicePlan(start, charge.Format, []string{adjustment.ID})
	planStatus, err := WriteInvoicePlanAtomic(filepath.Join(dir, "plan.json"), plan)
	require.NoError(t, err)
	planSource := Source{
		ArtifactFormat: InvoicePlanFormat, ArtifactID: plan.ID,
		ObjectKey: "plans/instance-a/invoice-july.json", VersionID: "plan-version",
		ArtifactSHA256: planStatus.SHA256, ObjectBytes: planStatus.Bytes,
		RetainUntilUnix: retainUntil,
	}
	finalizedAt := plan.Approval.ApprovedAtUnix + 1
	_, err = BuildInvoiceWithStatuses(
		planStatus, planSource, []ChargeStatus{chargeStatus}, []Source{chargeSource},
		[]SettlementStatus[Adjustment]{adjustmentStatus}, []Source{adjustmentSource}, finalizedAt,
	)
	require.NoError(t, err)

	badPlanSource := planSource
	badPlanSource.ArtifactSHA256 = strings.Repeat("b", 64)
	_, err = BuildInvoiceWithStatuses(
		planStatus, badPlanSource, []ChargeStatus{chargeStatus}, []Source{chargeSource},
		[]SettlementStatus[Adjustment]{adjustmentStatus}, []Source{adjustmentSource}, finalizedAt,
	)
	require.ErrorContains(t, err, "plan bytes")

	badChargeSource := chargeSource
	badChargeSource.ArtifactSHA256 = strings.Repeat("b", 64)
	_, err = BuildInvoiceWithStatuses(
		planStatus, planSource, []ChargeStatus{chargeStatus}, []Source{badChargeSource},
		[]SettlementStatus[Adjustment]{adjustmentStatus}, []Source{adjustmentSource}, finalizedAt,
	)
	require.ErrorContains(t, err, "charge bytes")

	badAdjustmentSource := adjustmentSource
	badAdjustmentSource.ArtifactSHA256 = strings.Repeat("b", 64)
	_, err = BuildInvoiceWithStatuses(
		planStatus, planSource, []ChargeStatus{chargeStatus}, []Source{chargeSource},
		[]SettlementStatus[Adjustment]{adjustmentStatus}, []Source{badAdjustmentSource}, finalizedAt,
	)
	require.ErrorContains(t, err, "adjustment bytes")
}

func TestSettlementRejectsMismatchDuplicateAndTamper(t *testing.T) {
	start := time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC)
	charge, chargeSource := settlementCharge(t, start)
	adjustment := validAdjustment(start, chargeSource)
	plan := validInvoicePlan(start, charge.Format, []string{adjustment.ID})
	planSource := settlementSource(InvoicePlanFormat, plan.ID, "plan")
	adjustmentSource := settlementSource(AdjustmentFormat, adjustment.ID, "adjustment")

	bad := adjustment
	bad.Approval.ApprovedBy = "forged"
	require.ErrorContains(t, bad.Validate(), "approval")
	bad = adjustment
	bad.ChargeSource.VersionID = "other"
	_, err := BuildInvoice(
		plan, planSource, []Charge{charge}, []Source{chargeSource},
		[]Adjustment{bad}, []Source{adjustmentSource}, plan.Approval.ApprovedAtUnix+1,
	)
	require.ErrorContains(t, err, "different charge")

	bad = adjustment
	bad.InvoiceID = "another-invoice"
	_, err = BuildInvoice(
		plan, planSource, []Charge{charge}, []Source{chargeSource},
		[]Adjustment{bad}, []Source{adjustmentSource}, plan.Approval.ApprovedAtUnix+1,
	)
	require.ErrorContains(t, err, "match plan")

	duplicatePlan := plan
	duplicatePlan.AdjustmentIDs = []string{"credit-001", "credit-001"}
	require.ErrorContains(t, duplicatePlan.Validate(), "unique")

	tooMuchCredit := adjustment
	tooMuchCredit.AmountMicros = -charge.TotalMicros - 1
	_, err = BuildInvoice(
		plan, planSource, []Charge{charge}, []Source{chargeSource},
		[]Adjustment{tooMuchCredit}, []Source{adjustmentSource},
		plan.Approval.ApprovedAtUnix+1,
	)
	require.ErrorContains(t, err, "total")

	overflow := adjustment
	overflow.AmountMicros = math.MaxInt64
	_, err = BuildInvoice(
		plan, planSource, []Charge{charge}, []Source{chargeSource},
		[]Adjustment{overflow}, []Source{adjustmentSource},
		plan.Approval.ApprovedAtUnix+1,
	)
	require.ErrorContains(t, err, "invalid")

	path := filepath.Join(t.TempDir(), "adjustment.json")
	_, err = WriteAdjustmentAtomic(path, adjustment)
	require.NoError(t, err)
	data, err := os.ReadFile(path)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(path, append([]byte(" "), data...), 0o600))
	_, err = ReadAdjustment(path)
	require.ErrorContains(t, err, "canonical")
}

func TestInvoicePlanRequiresContinuousDailyCharges(t *testing.T) {
	start := time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC)
	plan := validInvoicePlan(start, ChargeFormatV2, nil)
	plan.PeriodEndUnix = start.Add(48 * time.Hour).Unix()
	require.ErrorContains(t, plan.Validate(), "incomplete")
	plan.Charges = append(plan.Charges, PlannedCharge{
		PeriodStartUnix: start.Add(25 * time.Hour).Unix(),
		PeriodEndUnix:   start.Add(49 * time.Hour).Unix(), ArtifactFormat: ChargeFormatV2,
	})
	require.ErrorContains(t, plan.Validate(), "ordered")
}

func settlementCharge(t *testing.T, start time.Time) (Charge, Source) {
	t.Helper()
	rollup := validRollupForPeriod(start)
	storage := validStorageRollup(start)
	catalog := validCatalogV2ForPeriod()
	id := periodArtifactIDUnix("instance-a", start.Unix(), start.Add(24*time.Hour).Unix())
	charge, err := BuildChargeV2(
		rollup, settlementSource(meteringarchive.RollupFormat, id, "resource"),
		storage, settlementSource(meteringstorage.RollupFormat, id, "storage"),
		catalog, settlementSource(CatalogFormatV2, catalog.Version, "catalog"),
	)
	require.NoError(t, err)
	return charge, settlementSource(ChargeFormatV2, id, "charges/day.json")
}

func validAdjustment(start time.Time, chargeSource Source) Adjustment {
	return Adjustment{
		Format: AdjustmentFormat, ID: "credit-001", InvoiceID: "invoice-july",
		Instance:        "instance-a",
		PeriodStartUnix: start.Unix(), PeriodEndUnix: start.Add(24 * time.Hour).Unix(),
		Currency: "USD", AmountMicros: -7, ReasonCode: "service_credit",
		ChargeSource: chargeSource,
		Approval: Approval{
			ApprovedBy: BillingApprover, ApprovalID: "approval-credit-001",
			ApprovedAtUnix: start.Add(25 * time.Hour).Unix(),
		},
	}
}

func validInvoicePlan(start time.Time, chargeFormat string, adjustments []string) InvoicePlan {
	return InvoicePlan{
		Format: InvoicePlanFormat, ID: "invoice-july", Instance: "instance-a",
		PeriodStartUnix: start.Unix(), PeriodEndUnix: start.Add(24 * time.Hour).Unix(),
		Currency: "USD",
		Charges: []PlannedCharge{{
			PeriodStartUnix: start.Unix(), PeriodEndUnix: start.Add(24 * time.Hour).Unix(),
			ArtifactFormat: chargeFormat,
		}},
		AdjustmentIDs: adjustments,
		Approval: Approval{
			ApprovedBy: BillingApprover, ApprovalID: "approval-invoice-july",
			ApprovedAtUnix: start.Add(26 * time.Hour).Unix(),
		},
	}
}

func settlementSource(format, id, key string) Source {
	return Source{
		ArtifactFormat: format, ArtifactID: id, ObjectKey: key, VersionID: "version",
		ArtifactSHA256: strings.Repeat("a", 64), ObjectBytes: 100,
		RetainUntilUnix: time.Date(2035, 1, 1, 0, 0, 0, 0, time.UTC).Unix(),
	}
}
