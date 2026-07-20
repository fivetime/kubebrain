package main

import (
	"context"
	"flag"
	"log"
	"os"
	"time"

	"github.com/kubewharf/kubebrain/hack/production/internal/meteringstorage"
)

func main() {
	var instance, executor, objectStoreID, bucket, samplePrefix, rollupPrefix, retentionMode string
	var retentionDuration, finalizationDelay, sampleFinalizationDelay, timeout time.Duration
	var periodEndUnix int64
	flag.StringVar(&instance, "instance", "", "stable DBaaS instance identifier")
	flag.StringVar(&executor, "object-executor", "/usr/local/bin/kubebrain-logical-object", "Object Lock executor")
	flag.StringVar(&objectStoreID, "object-store-id", "", "metering evidence object store identifier")
	flag.StringVar(&bucket, "bucket", "", "metering evidence Object Lock bucket")
	flag.StringVar(&samplePrefix, "sample-prefix", "metering-storage-samples", "immutable hourly storage sample prefix")
	flag.StringVar(&rollupPrefix, "rollup-prefix", "metering-storage-rollups", "immutable daily storage rollup prefix")
	flag.StringVar(&retentionMode, "retention-mode", "COMPLIANCE", "COMPLIANCE or GOVERNANCE")
	flag.DurationVar(&retentionDuration, "retention-duration", 7*365*24*time.Hour, "evidence retention from slot end")
	flag.DurationVar(&finalizationDelay, "finalization-delay", 45*time.Minute, "delay after the complete UTC day")
	flag.DurationVar(&sampleFinalizationDelay, "sample-finalization-delay", 10*time.Minute, "maximum delay encoded in hourly samples")
	flag.DurationVar(&timeout, "timeout", 20*time.Minute, "overall read and archive deadline")
	flag.Int64Var(&periodEndUnix, "period-end-unix", 0, "optional aligned historical period end")
	flag.Parse()
	if instance == "" || objectStoreID == "" || bucket == "" || timeout <= 0 {
		log.Fatal("instance, object-store-id, bucket, and a positive timeout are required")
	}
	roller := &meteringstorage.Roller{
		Instance: instance, Executor: executor, ObjectStoreID: objectStoreID, Bucket: bucket,
		SnapshotPrefix: samplePrefix, RollupPrefix: rollupPrefix,
		RetentionMode: retentionMode, RetentionDuration: retentionDuration,
		FinalizationDelay: finalizationDelay, SampleFinalizationDelay: sampleFinalizationDelay,
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
