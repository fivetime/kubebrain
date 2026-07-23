package main

import (
	"context"
	"flag"
	"log"
	"os"
	"time"

	"github.com/kubewharf/kubebrain/hack/production/internal/meteringbilling"
	"github.com/kubewharf/kubebrain/hack/production/internal/processgroup"
)

func main() {
	var instance, providerStatementID, invoiceID, reconciliationID, executor string
	var objectStoreID, bucket, providerPrefix, invoicePrefix, reconciliationPrefix, retentionMode string
	var retentionDuration, timeout time.Duration
	flag.StringVar(&instance, "instance", "", "stable DBaaS instance identifier")
	flag.StringVar(&providerStatementID, "provider-statement-id", "", "immutable provider statement ID")
	flag.StringVar(&invoiceID, "invoice-id", "", "immutable finalized invoice ID")
	flag.StringVar(&reconciliationID, "reconciliation-id", "", "immutable reconciliation artifact ID")
	flag.StringVar(&executor, "object-executor", "/usr/local/bin/kubebrain-logical-object", "Object Lock executor")
	flag.StringVar(&objectStoreID, "object-store-id", "", "stable object store identifier")
	flag.StringVar(&bucket, "bucket", "", "Object Lock bucket")
	flag.StringVar(&providerPrefix, "provider-prefix", "metering-provider-statements", "immutable provider statement prefix")
	flag.StringVar(&invoicePrefix, "invoice-prefix", "metering-invoices", "immutable final invoice prefix")
	flag.StringVar(&reconciliationPrefix, "reconciliation-prefix", "metering-provider-reconciliations", "immutable reconciliation prefix")
	flag.StringVar(&retentionMode, "retention-mode", "COMPLIANCE", "COMPLIANCE or GOVERNANCE")
	flag.DurationVar(&retentionDuration, "retention-duration", 7*365*24*time.Hour, "reconciliation evidence retention")
	flag.DurationVar(&timeout, "timeout", 20*time.Minute, "overall exact read and archive deadline")
	flag.Parse()
	if instance == "" || providerStatementID == "" || invoiceID == "" ||
		reconciliationID == "" || objectStoreID == "" || bucket == "" || timeout <= 0 {
		log.Fatal("instance, provider-statement-id, invoice-id, reconciliation-id, object-store-id, bucket, and a positive timeout are required")
	}
	if err := processgroup.ValidateExecutable(executor); err != nil {
		log.Fatal(err)
	}
	reconciler := &meteringbilling.ProviderReconciler{
		Instance: instance, ProviderStatementID: providerStatementID, InvoiceID: invoiceID,
		ReconciliationID: reconciliationID, Executor: executor,
		ObjectStoreID: objectStoreID, Bucket: bucket, ProviderPrefix: providerPrefix,
		InvoicePrefix: invoicePrefix, ReconciliationPrefix: reconciliationPrefix,
		RetentionMode: retentionMode, RetentionDuration: retentionDuration,
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	_, output, err := reconciler.Process(ctx)
	if err != nil {
		log.Fatal(err)
	}
	if _, err := os.Stdout.Write(output); err != nil {
		log.Fatal(err)
	}
}

func init() {
	log.SetFlags(0)
}
