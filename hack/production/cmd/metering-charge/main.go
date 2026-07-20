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
	var instance, priceScope, priceVersion, priceCatalogFormat, executor, objectStoreID, bucket string
	var rollupPrefix, storageRollupPrefix, pricePrefix, chargePrefix, retentionMode string
	var retentionDuration, periodDuration, finalizationDelay, timeout time.Duration
	var periodEndUnix int64
	flag.StringVar(&instance, "instance", "", "stable DBaaS instance identifier")
	flag.StringVar(&priceScope, "price-scope", "global", "immutable price catalog scope")
	flag.StringVar(&priceVersion, "price-version", "", "approved immutable price catalog version")
	flag.StringVar(&priceCatalogFormat, "price-catalog-format", meteringbilling.CatalogFormat, "immutable price catalog format")
	flag.StringVar(&executor, "object-executor", "/usr/local/bin/kubebrain-logical-object", "immutable Object Lock executor")
	flag.StringVar(&objectStoreID, "object-store-id", "", "stable object store account identifier")
	flag.StringVar(&bucket, "bucket", "", "Object Lock bucket")
	flag.StringVar(&rollupPrefix, "rollup-prefix", "metering-rollups", "immutable daily rollup key prefix")
	flag.StringVar(&storageRollupPrefix, "storage-rollup-prefix", "metering-storage-rollups", "immutable daily object storage rollup key prefix")
	flag.StringVar(&pricePrefix, "price-prefix", "metering-prices", "immutable price catalog key prefix")
	flag.StringVar(&chargePrefix, "charge-prefix", "metering-charges", "immutable charge key prefix")
	flag.StringVar(&retentionMode, "retention-mode", "COMPLIANCE", "COMPLIANCE or GOVERNANCE")
	flag.DurationVar(&retentionDuration, "retention-duration", 7*365*24*time.Hour, "charge evidence retention from first slot end")
	flag.DurationVar(&periodDuration, "period-duration", 24*time.Hour, "fixed charge period")
	flag.DurationVar(&finalizationDelay, "finalization-delay", time.Hour, "delay after a complete UTC period")
	flag.DurationVar(&timeout, "timeout", 20*time.Minute, "overall read and archive deadline")
	flag.Int64Var(&periodEndUnix, "period-end-unix", 0, "optional aligned historical period end for backfill")
	flag.Parse()

	if instance == "" || priceVersion == "" || objectStoreID == "" || bucket == "" || timeout <= 0 {
		log.Fatal("instance, price-version, object-store-id, bucket, and a positive timeout are required")
	}
	biller := &meteringbilling.Biller{
		Instance: instance, PriceScope: priceScope, PriceVersion: priceVersion,
		PriceCatalogFormat: priceCatalogFormat,
		Executor:           executor, ObjectStoreID: objectStoreID, Bucket: bucket,
		RollupPrefix: rollupPrefix, StorageRollupPrefix: storageRollupPrefix,
		PricePrefix: pricePrefix, ChargePrefix: chargePrefix,
		RetentionMode: retentionMode, RetentionDuration: retentionDuration,
		PeriodDuration: periodDuration, FinalizationDelay: finalizationDelay,
	}
	if periodEndUnix < 0 {
		log.Fatal("period-end-unix cannot be negative")
	}
	if periodEndUnix > 0 {
		biller.PeriodEnd = time.Unix(periodEndUnix, 0).UTC()
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	_, output, err := biller.Process(ctx)
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
