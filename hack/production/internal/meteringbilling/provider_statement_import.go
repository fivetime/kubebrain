package meteringbilling

import (
	"bytes"
	"context"
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"slices"
	"sort"
	"strconv"
	"strings"
	"time"
)

var providerStatementCSVHeader = []string{
	"provider",
	"provider_account_id",
	"external_invoice_id",
	"external_line_id",
	"category",
	"object_store_id",
	"bucket",
	"amount_micros",
}

type ProviderStatementImportOptions struct {
	ID              string
	Instance        string
	PeriodStartUnix int64
	PeriodEndUnix   int64
	Currency        string
	IssuedAtUnix    int64
}

func BuildProviderStatementFromCSV(
	reader io.Reader,
	options ProviderStatementImportOptions,
) (ProviderStatement, error) {
	if reader == nil {
		return ProviderStatement{}, errors.New("provider statement CSV input is empty")
	}
	csvReader := csv.NewReader(reader)
	csvReader.FieldsPerRecord = -1
	records, err := csvReader.ReadAll()
	if err != nil {
		return ProviderStatement{}, fmt.Errorf("read provider statement CSV: %w", err)
	}
	if len(records) < 2 {
		return ProviderStatement{}, errors.New("provider statement CSV lacks line items")
	}
	if !slices.Equal(records[0], providerStatementCSVHeader) {
		return ProviderStatement{}, errors.New("provider statement CSV header is unsupported")
	}
	lines := make([]ProviderStatementLine, 0, len(records)-1)
	var total int64
	for index, record := range records[1:] {
		if len(record) != len(providerStatementCSVHeader) {
			return ProviderStatement{}, fmt.Errorf("provider statement CSV line %d is incomplete", index+2)
		}
		amount, parseErr := strconv.ParseInt(record[7], 10, 64)
		if parseErr != nil {
			return ProviderStatement{}, fmt.Errorf("provider statement CSV line %d amount is invalid", index+2)
		}
		line := ProviderStatementLine{
			Provider: record[0], ProviderAccountID: record[1],
			ExternalInvoiceID: record[2], ExternalLineID: record[3],
			Category: record[4], ObjectStoreID: record[5], Bucket: record[6],
			AmountMicros: amount,
		}
		if err := line.validate(); err != nil {
			return ProviderStatement{}, fmt.Errorf("provider statement CSV line %d: %w", index+2, err)
		}
		next, ok := addInt64(total, line.AmountMicros)
		if !ok {
			return ProviderStatement{}, errors.New("provider statement CSV total overflows int64")
		}
		total = next
		lines = append(lines, line)
	}
	sort.Slice(lines, func(i, j int) bool {
		return providerLineSortKey(lines[i]) < providerLineSortKey(lines[j])
	})
	statement := ProviderStatement{
		Format: ProviderStatementFormat, ID: options.ID, Instance: options.Instance,
		PeriodStartUnix: options.PeriodStartUnix, PeriodEndUnix: options.PeriodEndUnix,
		Currency: options.Currency, IssuedAtUnix: options.IssuedAtUnix,
		Lines: lines, TotalMicros: total,
	}
	if err := statement.Validate(); err != nil {
		return ProviderStatement{}, err
	}
	return statement, nil
}

type ProviderStatementPublisher struct {
	Input             string
	Executor          string
	ObjectStoreID     string
	Bucket            string
	ProviderPrefix    string
	RetentionMode     string
	RetentionDuration time.Duration
	Now               func() time.Time
	Run               CommandRunner
}

func (p *ProviderStatementPublisher) Publish(ctx context.Context) (SettlementStatus[ProviderStatement], []byte, error) {
	p.ProviderPrefix = strings.Trim(p.ProviderPrefix, "/")
	if p.Input == "" || p.Executor == "" || p.ObjectStoreID == "" || p.Bucket == "" ||
		p.ProviderPrefix == "" ||
		(p.RetentionMode != "COMPLIANCE" && p.RetentionMode != "GOVERNANCE") ||
		p.RetentionDuration <= 24*time.Hour {
		return SettlementStatus[ProviderStatement]{}, nil, errors.New("provider statement publisher configuration is incomplete")
	}
	if p.Now == nil {
		p.Now = time.Now
	}
	if p.Run == nil {
		p.Run = runCommand
	}
	status, err := ReadProviderStatement(p.Input)
	if err != nil {
		return SettlementStatus[ProviderStatement]{}, nil, err
	}
	now := p.Now().UTC()
	if status.Value.IssuedAtUnix > now.Unix() {
		return SettlementStatus[ProviderStatement]{}, nil, errors.New("provider statement issue timestamp is in the future")
	}
	retainUntil := time.Unix(status.Value.PeriodStartUnix, 0).
		Add(time.Hour).Add(p.RetentionDuration).Unix()
	if retainUntil <= now.Unix() {
		return SettlementStatus[ProviderStatement]{}, nil, errors.New("provider statement retention is not in the future")
	}
	dir, err := os.MkdirTemp("", "kubebrain-provider-statement-publish-*")
	if err != nil {
		return SettlementStatus[ProviderStatement]{}, nil, err
	}
	defer os.RemoveAll(dir)
	objectKey := settlementObjectKey(p.ProviderPrefix, status.Value.Instance, status.Value.ID)
	output, err := p.Run(ctx, p.Executor, []string{
		"ACTION=blob", "INPUT=" + p.Input,
		"ARTIFACT_FORMAT=" + ProviderStatementFormat,
		"ARTIFACT_ID=" + status.Value.ID, "INSTANCE=" + status.Value.Instance,
		"OBJECT_STORE_ID=" + p.ObjectStoreID, "S3_BUCKET=" + p.Bucket,
		"S3_OBJECT_KEY=" + objectKey, "RETENTION_MODE=" + p.RetentionMode,
		"RETAIN_UNTIL_UNIX=" + strconv.FormatInt(retainUntil, 10),
		"RECEIPT_OUTPUT=" + path.Join(dir, "provider-statement.receipt.json"),
	})
	if err != nil {
		return SettlementStatus[ProviderStatement]{}, output, fmt.Errorf(
			"provider statement Object Lock publisher failed: %w: %s",
			err, strings.TrimSpace(string(output)),
		)
	}
	if _, err := parseBlobReceipt(
		output, ProviderStatementFormat, status.Value.ID, status.Value.Instance,
		p.ObjectStoreID, p.Bucket, objectKey, retainUntil, status.SHA256, status.Bytes,
	); err != nil {
		return SettlementStatus[ProviderStatement]{}, output, err
	}
	return status, output, nil
}

func EncodeProviderStatementCSVHeader() []byte {
	var buffer bytes.Buffer
	writer := csv.NewWriter(&buffer)
	_ = writer.Write(providerStatementCSVHeader)
	writer.Flush()
	return buffer.Bytes()
}
