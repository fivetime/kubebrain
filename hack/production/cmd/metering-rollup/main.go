package main

import (
	"context"
	"flag"
	"log"
	"os"
	"time"

	"github.com/kubewharf/kubebrain/hack/production/internal/meteringarchive"
	"github.com/kubewharf/kubebrain/hack/production/internal/processgroup"
)

func main() {
	var instance, executor, objectStoreID, bucket, samplePrefix, rollupPrefix, retentionMode string
	var retentionDuration, periodDuration, finalizationDelay, slotDuration, maxStaleness, timeout time.Duration
	var periodEndUnix int64
	flag.StringVar(&instance, "instance", "", "stable DBaaS instance identifier")
	flag.StringVar(&executor, "object-executor", "/usr/local/bin/kubebrain-logical-object", "immutable Object Lock executor")
	flag.StringVar(&objectStoreID, "object-store-id", "", "stable object store account identifier")
	flag.StringVar(&bucket, "bucket", "", "Object Lock bucket")
	flag.StringVar(&samplePrefix, "sample-prefix", "metering-samples", "immutable hourly sample key prefix")
	flag.StringVar(&rollupPrefix, "rollup-prefix", "metering-rollups", "immutable daily rollup key prefix")
	flag.StringVar(&retentionMode, "retention-mode", "COMPLIANCE", "COMPLIANCE or GOVERNANCE")
	flag.DurationVar(&retentionDuration, "retention-duration", 7*365*24*time.Hour, "source sample retention from slot end")
	flag.DurationVar(&periodDuration, "period-duration", 24*time.Hour, "fixed rollup period")
	flag.DurationVar(&finalizationDelay, "finalization-delay", 30*time.Minute, "delay after a complete UTC period")
	flag.DurationVar(&slotDuration, "slot-duration", time.Hour, "source sample slot")
	flag.DurationVar(&maxStaleness, "max-staleness", 5*time.Minute, "source sample timestamp tolerance")
	flag.DurationVar(&timeout, "timeout", 20*time.Minute, "overall read and archive deadline")
	flag.Int64Var(&periodEndUnix, "period-end-unix", 0, "optional aligned historical period end for backfill")
	flag.Parse()

	if instance == "" || objectStoreID == "" || bucket == "" || timeout <= 0 {
		log.Fatal("instance, object-store-id, bucket, and a positive timeout are required")
	}
	if err := processgroup.ValidateExecutable(executor); err != nil {
		log.Fatal(err)
	}
	roller := &meteringarchive.Roller{
		Instance: instance, Executor: executor, ObjectStoreID: objectStoreID, Bucket: bucket,
		SamplePrefix: samplePrefix, RollupPrefix: rollupPrefix,
		RetentionMode: retentionMode, RetentionDuration: retentionDuration,
		PeriodDuration: periodDuration, FinalizationDelay: finalizationDelay,
		SlotDuration: slotDuration, MaxStaleness: maxStaleness,
	}
	if periodEndUnix < 0 {
		log.Fatal("period-end-unix cannot be negative")
	}
	if periodEndUnix > 0 {
		roller.PeriodEnd = time.Unix(periodEndUnix, 0).UTC()
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	_, output, err := roller.Process(ctx)
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
