package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"io"
	"log"
	"os"
	"time"

	"github.com/kubewharf/kubebrain/hack/production/internal/meteringbilling"
)

func main() {
	var inputCSV, invoicePath, invoiceSourcePath, output, id, instance, invoiceID string
	var executor, objectStoreID, bucket, invoicePrefix, paymentPrefix, retentionMode string
	var generatedAtUnix int64
	var retentionDuration, timeout time.Duration
	var publish bool
	flag.StringVar(&inputCSV, "input-csv", "", "normalized payment ledger CSV")
	flag.StringVar(&invoicePath, "invoice", "", "canonical finalized invoice JSON")
	flag.StringVar(&invoiceSourcePath, "invoice-source", "", "JSON source binding for the finalized invoice")
	flag.StringVar(&output, "output", "", "canonical payment ledger JSON output")
	flag.StringVar(&id, "id", "", "immutable payment ledger ID")
	flag.Int64Var(&generatedAtUnix, "generated-at-unix", 0, "payment ledger generation timestamp")
	flag.StringVar(&instance, "instance", "", "stable DBaaS instance identifier for exact invoice read")
	flag.StringVar(&invoiceID, "invoice-id", "", "immutable finalized invoice ID for exact invoice read")
	flag.BoolVar(&publish, "publish", false, "archive the generated ledger through Object Lock")
	flag.StringVar(&executor, "object-executor", "/usr/local/bin/kubebrain-logical-object", "Object Lock executor")
	flag.StringVar(&objectStoreID, "object-store-id", "", "stable object store identifier")
	flag.StringVar(&bucket, "bucket", "", "Object Lock bucket")
	flag.StringVar(&invoicePrefix, "invoice-prefix", "metering-invoices", "immutable final invoice prefix")
	flag.StringVar(&paymentPrefix, "payment-prefix", "metering-payment-ledgers", "immutable payment ledger prefix")
	flag.StringVar(&retentionMode, "retention-mode", "COMPLIANCE", "COMPLIANCE or GOVERNANCE")
	flag.DurationVar(&retentionDuration, "retention-duration", 7*365*24*time.Hour, "payment ledger evidence retention")
	flag.DurationVar(&timeout, "timeout", 20*time.Minute, "overall import and optional archive deadline")
	flag.Parse()
	if inputCSV == "" || output == "" || id == "" || generatedAtUnix <= 0 || timeout <= 0 {
		log.Fatal("input-csv, output, id, generated-at-unix, and a positive timeout are required")
	}
	if (invoicePath == "") != (invoiceSourcePath == "") {
		log.Fatal("invoice and invoice-source must be provided together for offline mode")
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	if invoicePath == "" {
		if instance == "" || invoiceID == "" || objectStoreID == "" || bucket == "" {
			log.Fatal("instance, invoice-id, object-store-id, and bucket are required when invoice/invoice-source are omitted")
		}
		processor := &meteringbilling.PaymentLedgerProcessor{
			InputCSV: inputCSV, Output: output, ID: id, GeneratedAtUnix: generatedAtUnix,
			Instance: instance, InvoiceID: invoiceID, Executor: executor,
			ObjectStoreID: objectStoreID, Bucket: bucket, InvoicePrefix: invoicePrefix,
			PaymentPrefix: paymentPrefix, RetentionMode: retentionMode,
			RetentionDuration: retentionDuration, Publish: publish,
		}
		_, archiveOutput, err := processor.Process(ctx)
		if err != nil {
			log.Fatal(err)
		}
		if publish {
			if _, err := os.Stdout.Write(archiveOutput); err != nil {
				log.Fatal(err)
			}
		}
		return
	}
	input, err := os.Open(inputCSV)
	if err != nil {
		log.Fatal(err)
	}
	defer input.Close()
	invoiceStatus, err := meteringbilling.ReadInvoice(invoicePath)
	if err != nil {
		log.Fatal(err)
	}
	invoiceSource, err := readSource(invoiceSourcePath)
	if err != nil {
		log.Fatal(err)
	}
	ledger, err := meteringbilling.BuildPaymentLedgerFromCSV(
		input,
		invoiceStatus.Value,
		invoiceSource,
		meteringbilling.PaymentLedgerImportOptions{
			ID: id, GeneratedAtUnix: generatedAtUnix,
		},
	)
	if err != nil {
		log.Fatal(err)
	}
	if _, err := meteringbilling.WritePaymentLedgerAtomic(output, ledger); err != nil {
		log.Fatal(err)
	}
	if !publish {
		return
	}
	if objectStoreID == "" || bucket == "" {
		log.Fatal("object-store-id and bucket are required when publish is enabled")
	}
	publisher := &meteringbilling.PaymentLedgerPublisher{
		Input: output, Executor: executor, ObjectStoreID: objectStoreID, Bucket: bucket,
		PaymentPrefix: paymentPrefix, RetentionMode: retentionMode,
		RetentionDuration: retentionDuration,
	}
	_, archiveOutput, err := publisher.Publish(ctx)
	if err != nil {
		log.Fatal(err)
	}
	if _, err := os.Stdout.Write(archiveOutput); err != nil {
		log.Fatal(err)
	}
}

func readSource(path string) (meteringbilling.Source, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return meteringbilling.Source{}, err
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	var source meteringbilling.Source
	if err := decoder.Decode(&source); err != nil {
		return meteringbilling.Source{}, err
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		return meteringbilling.Source{}, errors.New("invoice source contains trailing JSON")
	}
	return source, nil
}

func init() {
	log.SetFlags(0)
}
