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
	var input, kind, executor, objectStoreID, bucket string
	var adjustmentPrefix, planPrefix, retentionMode string
	var retentionDuration, timeout time.Duration
	flag.StringVar(&input, "input", "", "canonical adjustment or invoice plan JSON")
	flag.StringVar(&kind, "kind", "", "adjustment or plan")
	flag.StringVar(&executor, "object-executor", "/usr/local/bin/kubebrain-logical-object", "Object Lock executor")
	flag.StringVar(&objectStoreID, "object-store-id", "", "stable object store identifier")
	flag.StringVar(&bucket, "bucket", "", "Object Lock bucket")
	flag.StringVar(&adjustmentPrefix, "adjustment-prefix", "metering-adjustments", "immutable adjustment prefix")
	flag.StringVar(&planPrefix, "plan-prefix", "metering-invoice-plans", "immutable invoice plan prefix")
	flag.StringVar(&retentionMode, "retention-mode", "COMPLIANCE", "COMPLIANCE or GOVERNANCE")
	flag.DurationVar(&retentionDuration, "retention-duration", 7*365*24*time.Hour, "settlement evidence retention")
	flag.DurationVar(&timeout, "timeout", 20*time.Minute, "overall validation and upload deadline")
	flag.Parse()
	if input == "" || kind == "" || objectStoreID == "" || bucket == "" || timeout <= 0 {
		log.Fatal("input, kind, object-store-id, bucket, and a positive timeout are required")
	}
	publisher := &meteringbilling.SettlementPublisher{
		Input: input, Kind: kind, Executor: executor, ObjectStoreID: objectStoreID,
		Bucket: bucket, AdjustmentPrefix: adjustmentPrefix, PlanPrefix: planPrefix,
		RetentionMode: retentionMode, RetentionDuration: retentionDuration,
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	output, err := publisher.Publish(ctx)
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
