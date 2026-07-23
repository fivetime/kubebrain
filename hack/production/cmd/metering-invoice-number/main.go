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
	var output, id, instance, invoiceID, jurisdiction, series string
	var executor, objectStoreID, bucket, invoicePrefix, numberPrefix, retentionMode string
	var sequence, assignedAtUnix int64
	var retentionDuration, timeout time.Duration
	var publish bool
	flag.StringVar(&output, "output", "", "canonical invoice number assignment JSON output")
	flag.StringVar(&id, "id", "", "immutable invoice number assignment ID")
	flag.StringVar(&instance, "instance", "", "stable DBaaS instance identifier")
	flag.StringVar(&invoiceID, "invoice-id", "", "immutable finalized invoice ID")
	flag.StringVar(&jurisdiction, "jurisdiction", "", "external invoice numbering jurisdiction")
	flag.StringVar(&series, "series", "", "approved invoice numbering series")
	flag.Int64Var(&sequence, "sequence", 0, "approved positive invoice numbering sequence")
	flag.Int64Var(&assignedAtUnix, "assigned-at-unix", 0, "invoice number assignment timestamp")
	flag.BoolVar(&publish, "publish", false, "archive the generated assignment through Object Lock")
	flag.StringVar(&executor, "object-executor", "/usr/local/bin/kubebrain-logical-object", "Object Lock executor")
	flag.StringVar(&objectStoreID, "object-store-id", "", "stable object store identifier")
	flag.StringVar(&bucket, "bucket", "", "Object Lock bucket")
	flag.StringVar(&invoicePrefix, "invoice-prefix", "metering-invoices", "immutable final invoice prefix")
	flag.StringVar(&numberPrefix, "number-prefix", "metering-invoice-number-assignments", "immutable invoice number assignment prefix")
	flag.StringVar(&retentionMode, "retention-mode", "COMPLIANCE", "COMPLIANCE or GOVERNANCE")
	flag.DurationVar(&retentionDuration, "retention-duration", 7*365*24*time.Hour, "invoice number assignment evidence retention")
	flag.DurationVar(&timeout, "timeout", 20*time.Minute, "overall exact read and optional archive deadline")
	flag.Parse()
	if output == "" || id == "" || instance == "" || invoiceID == "" ||
		jurisdiction == "" || series == "" || sequence <= 0 ||
		assignedAtUnix <= 0 || objectStoreID == "" || bucket == "" || timeout <= 0 {
		log.Fatal("output, id, instance, invoice-id, jurisdiction, series, sequence, assigned-at-unix, object-store-id, bucket, and a positive timeout are required")
	}
	if err := processgroup.ValidateExecutable(executor); err != nil {
		log.Fatal(err)
	}
	assigner := &meteringbilling.InvoiceNumberAssigner{
		Output: output, ID: id, Instance: instance, InvoiceID: invoiceID,
		Jurisdiction: jurisdiction, Series: series, Sequence: sequence,
		Executor: executor, ObjectStoreID: objectStoreID, Bucket: bucket,
		InvoicePrefix: invoicePrefix, NumberPrefix: numberPrefix,
		RetentionMode: retentionMode, RetentionDuration: retentionDuration,
		AssignedAtUnix: assignedAtUnix, Publish: publish,
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	_, archiveOutput, err := assigner.Process(ctx)
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
