package meteringbilling

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path"
	"strconv"
	"strings"
	"time"
)

type Publisher struct {
	Input             string
	PriceScope        string
	Executor          string
	ObjectStoreID     string
	Bucket            string
	PricePrefix       string
	RetentionMode     string
	RetentionDuration time.Duration
	Now               func() time.Time
	Run               CommandRunner
}

func (p *Publisher) Publish(ctx context.Context) (CatalogStatus, []byte, error) {
	p.PricePrefix = strings.Trim(p.PricePrefix, "/")
	if p.Input == "" || !versionPattern.MatchString(p.PriceScope) || p.Executor == "" ||
		p.ObjectStoreID == "" || p.Bucket == "" || p.PricePrefix == "" ||
		(p.RetentionMode != "COMPLIANCE" && p.RetentionMode != "GOVERNANCE") ||
		p.RetentionDuration <= 0 {
		return CatalogStatus{}, nil, errors.New("metering price publisher configuration is incomplete")
	}
	if p.Now == nil {
		p.Now = time.Now
	}
	if p.Run == nil {
		p.Run = runCommand
	}
	status, err := ReadCatalog(p.Input)
	if err != nil {
		return CatalogStatus{}, nil, err
	}
	retainUntil := time.Unix(status.Catalog.EffectiveEndUnix, 0).
		Add(p.RetentionDuration).UTC()
	if !retainUntil.After(p.Now().UTC()) {
		return CatalogStatus{}, nil, errors.New("metering price catalog retention is not in the future")
	}
	dir, err := os.MkdirTemp("", "kubebrain-price-publish-*")
	if err != nil {
		return CatalogStatus{}, nil, err
	}
	defer os.RemoveAll(dir)
	objectKey := path.Join(p.PricePrefix, p.PriceScope, status.Catalog.Version+".json")
	output, err := p.Run(ctx, p.Executor, []string{
		"ACTION=blob",
		"INPUT=" + p.Input,
		"ARTIFACT_FORMAT=" + status.Catalog.Format,
		"ARTIFACT_ID=" + status.Catalog.Version,
		"INSTANCE=" + p.PriceScope,
		"OBJECT_STORE_ID=" + p.ObjectStoreID,
		"S3_BUCKET=" + p.Bucket,
		"S3_OBJECT_KEY=" + objectKey,
		"RETENTION_MODE=" + p.RetentionMode,
		"RETAIN_UNTIL_UNIX=" + strconv.FormatInt(retainUntil.Unix(), 10),
		"RECEIPT_OUTPUT=" + path.Join(dir, "receipt.json"),
	})
	if err != nil {
		return CatalogStatus{}, output, fmt.Errorf(
			"price catalog Object Lock executor failed: %w: %s",
			err, strings.TrimSpace(string(output)),
		)
	}
	if _, err := parseBlobReceipt(
		output, status.Catalog.Format, status.Catalog.Version, p.PriceScope, p.ObjectStoreID,
		p.Bucket, objectKey, retainUntil.Unix(), status.SHA256, status.Bytes,
	); err != nil {
		return CatalogStatus{}, output, err
	}
	return status, output, nil
}
