package meteringbilling

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestSettlementPublisherValidatesBeforeObjectStore(t *testing.T) {
	calls := 0
	publisher := &SettlementPublisher{
		Input: filepath.Join(t.TempDir(), "bad.json"), Kind: "adjustment",
		Executor: "executor", ObjectStoreID: "store", Bucket: "billing",
		AdjustmentPrefix: "adjustments", RetentionMode: "COMPLIANCE",
		RetentionDuration: 7 * 24 * time.Hour,
		Run: func(context.Context, string, []string) ([]byte, error) {
			calls++
			return nil, nil
		},
	}
	require.NoError(t, os.WriteFile(publisher.Input, []byte("{}\n"), 0o600))
	_, err := publisher.Publish(context.Background())
	require.EqualError(t, err, "metering adjustment is incomplete")
	require.Zero(t, calls)

	start := time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC)
	_, source := settlementCharge(t, start)
	adjustment := validAdjustment(start, source)
	adjustment.Approval.ApprovedAtUnix = start.Add(72 * time.Hour).Unix()
	input := filepath.Join(t.TempDir(), "future.json")
	_, err = WriteAdjustmentAtomic(input, adjustment)
	require.NoError(t, err)
	publisher.Input = input
	publisher.Now = func() time.Time { return start.Add(48 * time.Hour) }
	_, err = publisher.Publish(context.Background())
	require.EqualError(t, err, "settlement approval timestamp is in the future")
	require.Zero(t, calls)
}

func TestSettlementPublisherUploadsFrozenCanonicalInput(t *testing.T) {
	start := time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC)
	charge, _ := settlementCharge(t, start)
	plan := validInvoicePlan(start, charge.Format, nil)
	input := filepath.Join(t.TempDir(), "plan.json")
	status, err := WriteInvoicePlanAtomic(input, plan)
	require.NoError(t, err)
	now := start.Add(48 * time.Hour)
	retainUntil := start.Add(time.Hour + 7*24*time.Hour).Unix()
	publisher := &SettlementPublisher{
		Input: input, Kind: "plan", Executor: "executor", ObjectStoreID: "store",
		Bucket: "billing", PlanPrefix: "plans", RetentionMode: "COMPLIANCE",
		RetentionDuration: 7 * 24 * time.Hour, Now: func() time.Time { return now },
		Run: func(_ context.Context, executable string, environment []string) ([]byte, error) {
			require.Equal(t, "executor", executable)
			values := envMap(environment)
			require.NotEqual(t, input, values["INPUT"])
			require.NoError(t, os.WriteFile(input, []byte("{}\n"), 0o600))
			data, err := os.ReadFile(values["INPUT"])
			require.NoError(t, err)
			sum := sha256.Sum256(data)
			receipt := objectReceipt{
				Format:         "kubebrain.object-immutable-blob.receipt.v1",
				ArtifactFormat: InvoicePlanFormat, ArtifactID: plan.ID, Instance: "instance-a",
				ObjectStoreID: "store", Bucket: "billing", ObjectKey: values["S3_OBJECT_KEY"],
				VersionID: "plan-version", ArtifactSHA256: hex.EncodeToString(sum[:]),
				ObjectBytes: int64(len(data)), RetentionMode: "COMPLIANCE",
				RetainUntilUnix: retainUntil, RemoteVerified: true, ArchivedAtUnix: now.Unix(),
			}
			return json.Marshal(receipt)
		},
	}
	output, err := publisher.Publish(context.Background())
	require.NoError(t, err, string(output))
	require.NotEmpty(t, output)
	require.Equal(t, int64(3), func() int64 {
		data, err := os.ReadFile(input)
		require.NoError(t, err)
		return int64(len(data))
	}())
	require.Equal(t, int64(len(canonicalPlanBytes(t, plan))), status.Bytes)
}

func TestInvoiceFinalizerReadsExactPlanSourcesAndArchivesDeterministically(t *testing.T) {
	start := time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC)
	retainUntil := start.Add(time.Hour + 7*24*time.Hour).Unix()
	now := start.Add(48 * time.Hour)
	charge, _ := settlementCharge(t, start)
	chargeKey := "charges/instance-a/2026/07/01/" +
		strconv.FormatInt(start.Unix(), 10) + "-" +
		strconv.FormatInt(start.Add(24*time.Hour).Unix(), 10) + ".json"
	chargeBytes := canonicalChargeBytes(t, charge)
	chargeSource := sourceForBytes(
		ChargeFormatV2, periodArtifactIDUnix("instance-a", start.Unix(),
			start.Add(24*time.Hour).Unix()), chargeKey, chargeBytes, retainUntil,
	)
	adjustment := validAdjustment(start, chargeSource)
	adjustmentBytes := canonicalAdjustmentBytes(t, adjustment)
	adjustmentKey := "adjustments/instance-a/credit-001.json"
	adjustmentSource := sourceForBytes(
		AdjustmentFormat, adjustment.ID, adjustmentKey, adjustmentBytes, retainUntil,
	)
	plan := validInvoicePlan(start, charge.Format, []string{adjustment.ID})
	planBytes := canonicalPlanBytes(t, plan)
	planKey := "plans/instance-a/invoice-july.json"
	planSource := sourceForBytes(InvoicePlanFormat, plan.ID, planKey, planBytes, retainUntil)
	objects := map[string][]byte{
		planKey: planBytes, chargeKey: chargeBytes, adjustmentKey: adjustmentBytes,
	}
	sources := map[string]Source{
		planKey: planSource, chargeKey: chargeSource, adjustmentKey: adjustmentSource,
	}
	var firstInvoice []byte
	readOrder := make([]string, 0, 6)
	run := func(_ context.Context, _ string, environment []string) ([]byte, error) {
		values := envMap(environment)
		switch values["ACTION"] {
		case "blob-read":
			key := values["S3_OBJECT_KEY"]
			readOrder = append(readOrder, key)
			require.NoError(t, os.WriteFile(values["OUTPUT"], objects[key], 0o600))
			return settlementReadReceipt(t, sources[key], values), nil
		case "blob":
			data, err := os.ReadFile(values["INPUT"])
			require.NoError(t, err)
			if firstInvoice == nil {
				firstInvoice = append([]byte(nil), data...)
			} else {
				require.Equal(t, firstInvoice, data)
			}
			sum := sha256.Sum256(data)
			receipt := objectReceipt{
				Format:         "kubebrain.object-immutable-blob.receipt.v1",
				ArtifactFormat: InvoiceFormat, ArtifactID: plan.ID, Instance: "instance-a",
				ObjectStoreID: "store", Bucket: "billing", ObjectKey: values["S3_OBJECT_KEY"],
				VersionID: "invoice-version", ArtifactSHA256: hex.EncodeToString(sum[:]),
				ObjectBytes: int64(len(data)), RetentionMode: "COMPLIANCE",
				RetainUntilUnix: retainUntil, RemoteVerified: true,
				ArchivedAtUnix: now.Unix(),
			}
			return json.Marshal(receipt)
		default:
			t.Fatalf("unexpected action %q", values["ACTION"])
		}
		return nil, nil
	}
	finalizer := &InvoiceFinalizer{
		Instance: "instance-a", PlanID: plan.ID, Executor: "executor",
		ObjectStoreID: "store", Bucket: "billing", ChargePrefix: "charges",
		AdjustmentPrefix: "adjustments", PlanPrefix: "plans", InvoicePrefix: "invoices",
		RetentionMode: "COMPLIANCE", RetentionDuration: 7 * 24 * time.Hour,
		Now: func() time.Time { return now }, Run: run,
	}
	first, _, err := finalizer.Process(context.Background())
	require.NoError(t, err)
	second, _, err := finalizer.Process(context.Background())
	require.NoError(t, err)
	require.Equal(t, first, second)
	require.Equal(t, []string{
		planKey, chargeKey, adjustmentKey, planKey, chargeKey, adjustmentKey,
	}, readOrder)
	require.Equal(t, charge.TotalMicros-7, first.TotalMicros)
	require.Equal(t, plan.Approval.ApprovedAtUnix, first.FinalizedAtUnix)
}

func TestInvoiceFinalizerFailsBeforeArchiveOnAdjustmentSourceMismatch(t *testing.T) {
	start := time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC)
	charge, chargeSource := settlementCharge(t, start)
	adjustment := validAdjustment(start, chargeSource)
	adjustment.ChargeSource.VersionID = "forged"
	plan := validInvoicePlan(start, charge.Format, []string{adjustment.ID})
	require.NoError(t, plan.Validate())
	require.NoError(t, adjustment.Validate())
	_, err := BuildInvoice(
		plan, settlementSource(InvoicePlanFormat, plan.ID, "plan"),
		[]Charge{charge}, []Source{chargeSource}, []Adjustment{adjustment},
		[]Source{settlementSource(AdjustmentFormat, adjustment.ID, "adjustment")},
		plan.Approval.ApprovedAtUnix,
	)
	require.EqualError(t, err, "metering invoice adjustment references a different charge")
}

func sourceForBytes(format, id, key string, data []byte, retainUntil int64) Source {
	sum := sha256.Sum256(data)
	return Source{
		ArtifactFormat: format, ArtifactID: id, ObjectKey: key, VersionID: format + "-version",
		ArtifactSHA256: hex.EncodeToString(sum[:]), ObjectBytes: int64(len(data)),
		RetainUntilUnix: retainUntil,
	}
}

func settlementReadReceipt(t *testing.T, source Source, values map[string]string) []byte {
	t.Helper()
	receipt := objectReceipt{
		Format:         "kubebrain.object-immutable-blob-read.receipt.v1",
		ArtifactFormat: source.ArtifactFormat, ArtifactID: source.ArtifactID,
		Instance: "instance-a", ObjectStoreID: "store", Bucket: "billing",
		ObjectKey: source.ObjectKey, VersionID: source.VersionID,
		ArtifactSHA256: source.ArtifactSHA256, ObjectBytes: source.ObjectBytes,
		RetentionMode: "COMPLIANCE", RetainUntilUnix: source.RetainUntilUnix,
		RemoteVerified: true,
	}
	require.Equal(t, values["ARTIFACT_FORMAT"], source.ArtifactFormat)
	data, err := json.Marshal(receipt)
	require.NoError(t, err)
	return data
}

func canonicalChargeBytes(t *testing.T, charge Charge) []byte {
	t.Helper()
	path := filepath.Join(t.TempDir(), "charge.json")
	_, err := WriteChargeAtomic(path, charge)
	require.NoError(t, err)
	data, err := os.ReadFile(path)
	require.NoError(t, err)
	return data
}

func canonicalAdjustmentBytes(t *testing.T, adjustment Adjustment) []byte {
	t.Helper()
	path := filepath.Join(t.TempDir(), "adjustment.json")
	_, err := WriteAdjustmentAtomic(path, adjustment)
	require.NoError(t, err)
	data, err := os.ReadFile(path)
	require.NoError(t, err)
	return data
}

func canonicalPlanBytes(t *testing.T, plan InvoicePlan) []byte {
	t.Helper()
	path := filepath.Join(t.TempDir(), "plan.json")
	_, err := WriteInvoicePlanAtomic(path, plan)
	require.NoError(t, err)
	data, err := os.ReadFile(path)
	require.NoError(t, err)
	return data
}
