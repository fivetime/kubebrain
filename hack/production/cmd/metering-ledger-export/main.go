package main

import (
	"context"
	"flag"
	"log"
	"os"
	"time"

	"github.com/kubewharf/kubebrain/hack/production/internal/meteringbilling"
)

func main() {
	var output, id, instance, invoiceID, reconciliationID, paymentLedgerID string
	var executor, objectStoreID, bucket, invoicePrefix, reconciliationPrefix, paymentPrefix string
	var ledgerPrefix, retentionMode string
	var exportedAtUnix int64
	var retentionDuration, timeout time.Duration
	var publish bool
	flag.StringVar(&output, "output", "", "canonical general ledger export JSON output")
	flag.StringVar(&id, "id", "", "immutable general ledger export ID")
	flag.StringVar(&instance, "instance", "", "stable DBaaS instance identifier")
	flag.StringVar(&invoiceID, "invoice-id", "", "immutable finalized invoice ID")
	flag.StringVar(&reconciliationID, "provider-reconciliation-id", "", "optional immutable provider reconciliation ID")
	flag.StringVar(&paymentLedgerID, "payment-ledger-id", "", "optional immutable payment ledger ID")
	flag.Int64Var(&exportedAtUnix, "exported-at-unix", 0, "general ledger export timestamp")
	flag.BoolVar(&publish, "publish", false, "archive the generated export through Object Lock")
	flag.StringVar(&executor, "object-executor", "/usr/local/bin/kubebrain-logical-object", "Object Lock executor")
	flag.StringVar(&objectStoreID, "object-store-id", "", "stable object store identifier")
	flag.StringVar(&bucket, "bucket", "", "Object Lock bucket")
	flag.StringVar(&invoicePrefix, "invoice-prefix", "metering-invoices", "immutable final invoice prefix")
	flag.StringVar(&reconciliationPrefix, "provider-reconciliation-prefix", "metering-provider-reconciliations", "immutable provider reconciliation prefix")
	flag.StringVar(&paymentPrefix, "payment-prefix", "metering-payment-ledgers", "immutable payment ledger prefix")
	flag.StringVar(&ledgerPrefix, "ledger-prefix", "metering-general-ledger-exports", "immutable general ledger export prefix")
	flag.StringVar(&retentionMode, "retention-mode", "COMPLIANCE", "COMPLIANCE or GOVERNANCE")
	flag.DurationVar(&retentionDuration, "retention-duration", 7*365*24*time.Hour, "general ledger export evidence retention")
	flag.DurationVar(&timeout, "timeout", 20*time.Minute, "overall exact read and optional archive deadline")
	flag.Parse()
	if output == "" || id == "" || instance == "" || invoiceID == "" ||
		exportedAtUnix <= 0 || objectStoreID == "" || bucket == "" || timeout <= 0 {
		log.Fatal("output, id, instance, invoice-id, exported-at-unix, object-store-id, bucket, and a positive timeout are required")
	}
	exporter := &meteringbilling.GeneralLedgerExporter{
		Output: output, ID: id, Instance: instance, InvoiceID: invoiceID,
		ProviderReconciliationID: reconciliationID, PaymentLedgerID: paymentLedgerID,
		Executor: executor, ObjectStoreID: objectStoreID, Bucket: bucket,
		InvoicePrefix: invoicePrefix, ProviderReconciliationPrefix: reconciliationPrefix,
		PaymentPrefix: paymentPrefix, LedgerPrefix: ledgerPrefix, RetentionMode: retentionMode,
		RetentionDuration: retentionDuration, ExportedAtUnix: exportedAtUnix, Publish: publish,
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	_, archiveOutput, err := exporter.Process(ctx)
	if err != nil {
		log.Fatal(err)
	}
	if publish {
		if _, err := os.Stdout.Write(archiveOutput); err != nil {
			log.Fatal(err)
		}
	}
}

func init() {
	log.SetFlags(0)
}
