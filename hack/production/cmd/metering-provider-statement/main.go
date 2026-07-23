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
	var inputCSV, output, id, instance, currency string
	var executor, objectStoreID, bucket, providerPrefix, retentionMode string
	var periodStartUnix, periodEndUnix, issuedAtUnix int64
	var retentionDuration, timeout time.Duration
	var publish bool
	flag.StringVar(&inputCSV, "input-csv", "", "normalized provider statement CSV")
	flag.StringVar(&output, "output", "", "canonical provider statement JSON output")
	flag.StringVar(&id, "id", "", "immutable provider statement ID")
	flag.StringVar(&instance, "instance", "", "stable DBaaS instance identifier")
	flag.Int64Var(&periodStartUnix, "period-start-unix", 0, "UTC billing period start")
	flag.Int64Var(&periodEndUnix, "period-end-unix", 0, "UTC billing period end")
	flag.StringVar(&currency, "currency", "USD", "ISO 4217 currency code")
	flag.Int64Var(&issuedAtUnix, "issued-at-unix", 0, "provider statement issue timestamp")
	flag.BoolVar(&publish, "publish", false, "archive the generated statement through Object Lock")
	flag.StringVar(&executor, "object-executor", "/usr/local/bin/kubebrain-logical-object", "Object Lock executor")
	flag.StringVar(&objectStoreID, "object-store-id", "", "stable object store identifier")
	flag.StringVar(&bucket, "bucket", "", "Object Lock bucket")
	flag.StringVar(&providerPrefix, "provider-prefix", "metering-provider-statements", "immutable provider statement prefix")
	flag.StringVar(&retentionMode, "retention-mode", "COMPLIANCE", "COMPLIANCE or GOVERNANCE")
	flag.DurationVar(&retentionDuration, "retention-duration", 7*365*24*time.Hour, "provider statement evidence retention")
	flag.DurationVar(&timeout, "timeout", 20*time.Minute, "overall import and optional archive deadline")
	flag.Parse()
	if inputCSV == "" || output == "" || id == "" || instance == "" ||
		periodStartUnix <= 0 || periodEndUnix <= periodStartUnix || issuedAtUnix <= 0 ||
		timeout <= 0 {
		log.Fatal("input-csv, output, id, instance, valid period, issued-at-unix, and a positive timeout are required")
	}
	if publish {
		if objectStoreID == "" || bucket == "" {
			log.Fatal("object-store-id and bucket are required when publish is enabled")
		}
		if err := processgroup.ValidateExecutable(executor); err != nil {
			log.Fatal(err)
		}
	}
	input, err := os.Open(inputCSV)
	if err != nil {
		log.Fatal(err)
	}
	defer input.Close()
	statement, err := meteringbilling.BuildProviderStatementFromCSV(
		input,
		meteringbilling.ProviderStatementImportOptions{
			ID: id, Instance: instance, PeriodStartUnix: periodStartUnix,
			PeriodEndUnix: periodEndUnix, Currency: currency, IssuedAtUnix: issuedAtUnix,
		},
	)
	if err != nil {
		log.Fatal(err)
	}
	if _, err := meteringbilling.WriteProviderStatementAtomic(output, statement); err != nil {
		log.Fatal(err)
	}
	if !publish {
		return
	}
	publisher := &meteringbilling.ProviderStatementPublisher{
		Input: output, Executor: executor, ObjectStoreID: objectStoreID, Bucket: bucket,
		ProviderPrefix: providerPrefix, RetentionMode: retentionMode,
		RetentionDuration: retentionDuration,
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	_, archiveOutput, err := publisher.Publish(ctx)
	if err != nil {
		log.Fatal(err)
	}
	if _, err := os.Stdout.Write(archiveOutput); err != nil {
		log.Fatal(err)
	}
}

func init() {
	log.SetFlags(0)
}
