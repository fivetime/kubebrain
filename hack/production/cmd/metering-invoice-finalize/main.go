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
	var instance, planID, executor, objectStoreID, bucket string
	var chargePrefix, adjustmentPrefix, planPrefix, invoicePrefix, retentionMode string
	var retentionDuration, timeout time.Duration
	flag.StringVar(&instance, "instance", "", "stable DBaaS instance identifier")
	flag.StringVar(&planID, "plan-id", "", "approved immutable invoice plan ID")
	flag.StringVar(&executor, "object-executor", "/usr/local/bin/kubebrain-logical-object", "Object Lock executor")
	flag.StringVar(&objectStoreID, "object-store-id", "", "stable object store identifier")
	flag.StringVar(&bucket, "bucket", "", "Object Lock bucket")
	flag.StringVar(&chargePrefix, "charge-prefix", "metering-charges", "immutable charge prefix")
	flag.StringVar(&adjustmentPrefix, "adjustment-prefix", "metering-adjustments", "immutable adjustment prefix")
	flag.StringVar(&planPrefix, "plan-prefix", "metering-invoice-plans", "immutable invoice plan prefix")
	flag.StringVar(&invoicePrefix, "invoice-prefix", "metering-invoices", "immutable final invoice prefix")
	flag.StringVar(&retentionMode, "retention-mode", "COMPLIANCE", "COMPLIANCE or GOVERNANCE")
	flag.DurationVar(&retentionDuration, "retention-duration", 7*365*24*time.Hour, "invoice evidence retention")
	flag.DurationVar(&timeout, "timeout", 30*time.Minute, "overall exact read and archive deadline")
	flag.Parse()
	if instance == "" || planID == "" || objectStoreID == "" || bucket == "" || timeout <= 0 {
		log.Fatal("instance, plan-id, object-store-id, bucket, and a positive timeout are required")
	}
	if err := processgroup.ValidateExecutable(executor); err != nil {
		log.Fatal(err)
	}
	finalizer := &meteringbilling.InvoiceFinalizer{
		Instance: instance, PlanID: planID, Executor: executor,
		ObjectStoreID: objectStoreID, Bucket: bucket, ChargePrefix: chargePrefix,
		AdjustmentPrefix: adjustmentPrefix, PlanPrefix: planPrefix,
		InvoicePrefix: invoicePrefix, RetentionMode: retentionMode,
		RetentionDuration: retentionDuration,
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	_, output, err := finalizer.Process(ctx)
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
