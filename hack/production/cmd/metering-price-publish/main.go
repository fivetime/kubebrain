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
	var input, priceScope, executor, objectStoreID, bucket, pricePrefix, retentionMode string
	var retentionDuration, timeout time.Duration
	flag.StringVar(&input, "input", "", "canonical price catalog JSON")
	flag.StringVar(&priceScope, "price-scope", "global", "immutable price catalog scope")
	flag.StringVar(&executor, "object-executor", "/usr/local/bin/kubebrain-logical-object", "immutable Object Lock executor")
	flag.StringVar(&objectStoreID, "object-store-id", "", "stable object store account identifier")
	flag.StringVar(&bucket, "bucket", "", "Object Lock bucket")
	flag.StringVar(&pricePrefix, "price-prefix", "metering-prices", "immutable price catalog key prefix")
	flag.StringVar(&retentionMode, "retention-mode", "COMPLIANCE", "COMPLIANCE or GOVERNANCE")
	flag.DurationVar(&retentionDuration, "retention-duration", 7*365*24*time.Hour, "retention after catalog effective end")
	flag.DurationVar(&timeout, "timeout", 20*time.Minute, "overall validation and upload deadline")
	flag.Parse()
	if input == "" || objectStoreID == "" || bucket == "" || timeout <= 0 {
		log.Fatal("input, object-store-id, bucket, and a positive timeout are required")
	}
	publisher := &meteringbilling.Publisher{
		Input: input, PriceScope: priceScope, Executor: executor,
		ObjectStoreID: objectStoreID, Bucket: bucket, PricePrefix: pricePrefix,
		RetentionMode: retentionMode, RetentionDuration: retentionDuration,
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	_, output, err := publisher.Publish(ctx)
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
