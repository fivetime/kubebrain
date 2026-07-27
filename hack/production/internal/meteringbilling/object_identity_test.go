package meteringbilling

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestBillingObjectIdentityValidatorsRejectUnsafeScopeAndPrefixes(t *testing.T) {
	for _, value := range []string{"store-a", "bucket.a", "metering_prices"} {
		require.True(t, validObjectScopeValue(value), value)
	}
	for _, value := range []string{"", "store a", "store\nb", "store\u00a0b", string([]byte{0xff})} {
		require.False(t, validObjectScopeValue(value), value)
	}

	for _, value := range []string{"prices", "metering/prices", "metering/prices/"} {
		require.True(t, validRelativeObjectPrefix(value), value)
	}
	for _, value := range []string{"", ".", "..", "../prices", "/prices", "prices//v1", "prices/../other", "prices\nv1"} {
		require.False(t, validRelativeObjectPrefix(value), value)
	}
}

func TestBillingObjectExecutorsRejectUnsafeObjectIdentityBeforeRun(t *testing.T) {
	run := func(*bool) CommandRunner {
		return func(context.Context, string, []string) ([]byte, error) {
			t.Fatal("object executor must not run for unsafe identity")
			return nil, nil
		}
	}
	now := func() time.Time { return time.Date(2026, 7, 15, 0, 0, 0, 0, time.UTC) }

	t.Run("price publisher scope", func(t *testing.T) {
		called := false
		publisher := &Publisher{
			Input: "missing.json", PriceScope: "global", Executor: "executor",
			ObjectStoreID: "store\nbad", Bucket: "billing", PricePrefix: "prices",
			RetentionMode: "COMPLIANCE", RetentionDuration: 7 * 24 * time.Hour,
			Run: run(&called),
		}
		_, _, err := publisher.Publish(context.Background())
		require.EqualError(t, err, "metering price publisher configuration is incomplete")
		require.False(t, called)
	})
	t.Run("settlement publisher prefix", func(t *testing.T) {
		called := false
		publisher := &SettlementPublisher{
			Input: "missing.json", Kind: "adjustment", Executor: "executor",
			ObjectStoreID: "store", Bucket: "billing", AdjustmentPrefix: "adjustments/../other",
			RetentionMode: "COMPLIANCE", RetentionDuration: 7 * 24 * time.Hour,
			Run: run(&called),
		}
		_, err := publisher.Publish(context.Background())
		require.EqualError(t, err, "adjustment prefix is invalid")
		require.False(t, called)
	})
	t.Run("biller", func(t *testing.T) {
		biller := validBiller(now())
		biller.PricePrefix = "prices//bad"
		require.EqualError(t, biller.Validate(), "metering biller configuration is incomplete")
	})
	t.Run("invoice finalizer", func(t *testing.T) {
		finalizer := &InvoiceFinalizer{
			Instance: "instance-a", PlanID: "invoice-july", Executor: "executor",
			ObjectStoreID: "store", Bucket: "billing\u00a0bad",
			ChargePrefix: "charges", AdjustmentPrefix: "adjustments", PlanPrefix: "plans", InvoicePrefix: "invoices",
			RetentionMode: "COMPLIANCE", RetentionDuration: 7 * 24 * time.Hour,
		}
		require.EqualError(t, finalizer.Validate(), "invoice finalizer configuration is incomplete")
	})
	t.Run("provider statement publisher", func(t *testing.T) {
		called := false
		publisher := &ProviderStatementPublisher{
			Input: "missing.json", Executor: "executor", ObjectStoreID: "store", Bucket: "billing",
			ProviderPrefix: "/providers/../other/", RetentionMode: "COMPLIANCE",
			RetentionDuration: 7 * 24 * time.Hour, Run: run(&called),
		}
		_, _, err := publisher.Publish(context.Background())
		require.EqualError(t, err, "provider statement publisher configuration is incomplete")
		require.False(t, called)
	})
	t.Run("payment ledger processor", func(t *testing.T) {
		processor := &PaymentLedgerProcessor{
			InputCSV: "payments.csv", Output: "ledger.json", ID: "payment-july",
			GeneratedAtUnix: now().Unix(), Instance: "instance-a", InvoiceID: "invoice-july",
			Executor: "executor", ObjectStoreID: "store", Bucket: "billing",
			InvoicePrefix: ".", RetentionMode: "COMPLIANCE", RetentionDuration: 7 * 24 * time.Hour,
		}
		require.EqualError(t, processor.Validate(), "payment ledger processor configuration is incomplete")
	})
	t.Run("payment ledger publisher", func(t *testing.T) {
		called := false
		publisher := &PaymentLedgerPublisher{
			Input: "missing.json", Executor: "executor", ObjectStoreID: "store", Bucket: "billing",
			PaymentPrefix: "payments/../other", RetentionMode: "COMPLIANCE",
			RetentionDuration: 7 * 24 * time.Hour, Run: run(&called),
		}
		_, _, err := publisher.Publish(context.Background())
		require.EqualError(t, err, "payment ledger publisher configuration is incomplete")
		require.False(t, called)
	})
	t.Run("provider reconciler", func(t *testing.T) {
		reconciler := &ProviderReconciler{
			Instance: "instance-a", ProviderStatementID: "provider-july", InvoiceID: "invoice-july",
			ReconciliationID: "reconciliation-july", Executor: "executor", ObjectStoreID: "store", Bucket: "billing",
			ProviderPrefix: "providers", InvoicePrefix: "invoices", ReconciliationPrefix: "reconciliations//bad",
			RetentionMode: "COMPLIANCE", RetentionDuration: 7 * 24 * time.Hour,
		}
		require.EqualError(t, reconciler.Validate(), "provider reconciler configuration is incomplete")
	})
	t.Run("general ledger exporter optional prefix", func(t *testing.T) {
		exporter := &GeneralLedgerExporter{
			Output: "ledger.json", ID: "ledger-july", Instance: "instance-a", InvoiceID: "invoice-july",
			PaymentLedgerID: "payment-july", Executor: "executor", ObjectStoreID: "store", Bucket: "billing",
			InvoicePrefix: "invoices", PaymentPrefix: "payments//bad", ExportedAtUnix: now().Unix(),
			RetentionMode: "COMPLIANCE", RetentionDuration: 7 * 24 * time.Hour,
		}
		require.EqualError(t, exporter.Validate(), "general ledger exporter configuration is incomplete")
	})
	t.Run("invoice number assigner publish prefix", func(t *testing.T) {
		assigner := &InvoiceNumberAssigner{
			Output: "number.json", ID: "number-july", Instance: "instance-a", InvoiceID: "invoice-july",
			Jurisdiction: "us", Series: "us", Sequence: 1, Executor: "executor",
			ObjectStoreID: "store", Bucket: "billing", InvoicePrefix: "invoices", NumberPrefix: "numbers/../other",
			RetentionMode: "COMPLIANCE", RetentionDuration: 7 * 24 * time.Hour,
			AssignedAtUnix: now().Unix(), Publish: true,
		}
		require.EqualError(t, assigner.Validate(), "invoice number assigner configuration is incomplete")
	})
}
