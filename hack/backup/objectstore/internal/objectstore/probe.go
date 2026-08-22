package objectstore

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

const BucketProbeFormat = "kubebrain.object-store-bucket-probe.v1"

type BucketProbeAPI interface {
	GetBucketVersioning(context.Context, *s3.GetBucketVersioningInput, ...func(*s3.Options)) (*s3.GetBucketVersioningOutput, error)
	GetObjectLockConfiguration(context.Context, *s3.GetObjectLockConfigurationInput, ...func(*s3.Options)) (*s3.GetObjectLockConfigurationOutput, error)
}

type BucketProbe struct {
	Format            string `json:"format"`
	ObjectStoreID     string `json:"object_store_id"`
	Bucket            string `json:"bucket"`
	VersioningEnabled bool   `json:"versioning_enabled"`
	ObjectLockEnabled bool   `json:"object_lock_enabled"`
	CheckedAtUnix     int64  `json:"checked_at_unix"`
}

func ProbeBucket(ctx context.Context, client BucketProbeAPI, objectStoreID, bucket string, now time.Time) (BucketProbe, error) {
	if client == nil || !validObjectScopeValue(objectStoreID) || !validObjectScopeValue(bucket) {
		return BucketProbe{}, errors.New("object store bucket probe scope is invalid")
	}
	versioning, err := client.GetBucketVersioning(ctx, &s3.GetBucketVersioningInput{Bucket: aws.String(bucket)})
	if err != nil {
		return BucketProbe{}, fmt.Errorf("read bucket versioning: %w", err)
	}
	if versioning == nil || versioning.Status != types.BucketVersioningStatusEnabled {
		return BucketProbe{}, errors.New("object store bucket versioning is not Enabled")
	}
	lock, err := client.GetObjectLockConfiguration(ctx, &s3.GetObjectLockConfigurationInput{Bucket: aws.String(bucket)})
	if err != nil {
		return BucketProbe{}, fmt.Errorf("read bucket Object Lock configuration: %w", err)
	}
	if lock == nil || lock.ObjectLockConfiguration == nil ||
		lock.ObjectLockConfiguration.ObjectLockEnabled != types.ObjectLockEnabledEnabled {
		return BucketProbe{}, errors.New("object store bucket Object Lock is not Enabled")
	}
	if now.IsZero() {
		now = time.Now()
	}
	return BucketProbe{
		Format: BucketProbeFormat, ObjectStoreID: objectStoreID, Bucket: bucket,
		VersioningEnabled: true, ObjectLockEnabled: true, CheckedAtUnix: now.UTC().Unix(),
	}, nil
}
