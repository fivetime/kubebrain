package objectstore

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/stretchr/testify/require"
)

type bucketProbeClient struct {
	versioning *s3.GetBucketVersioningOutput
	lock       *s3.GetObjectLockConfigurationOutput
	versionErr error
	lockErr    error
	versionGet int
	lockGet    int
}

func (c *bucketProbeClient) GetBucketVersioning(context.Context, *s3.GetBucketVersioningInput, ...func(*s3.Options)) (*s3.GetBucketVersioningOutput, error) {
	c.versionGet++
	return c.versioning, c.versionErr
}

func (c *bucketProbeClient) GetObjectLockConfiguration(context.Context, *s3.GetObjectLockConfigurationInput, ...func(*s3.Options)) (*s3.GetObjectLockConfigurationOutput, error) {
	c.lockGet++
	return c.lock, c.lockErr
}

func enabledBucketProbeClient() *bucketProbeClient {
	return &bucketProbeClient{
		versioning: &s3.GetBucketVersioningOutput{Status: types.BucketVersioningStatusEnabled},
		lock: &s3.GetObjectLockConfigurationOutput{ObjectLockConfiguration: &types.ObjectLockConfiguration{
			ObjectLockEnabled: types.ObjectLockEnabledEnabled,
		}},
	}
}

func TestProbeBucketRequiresVersioningAndObjectLock(t *testing.T) {
	client := enabledBucketProbeClient()
	probe, err := ProbeBucket(context.Background(), client, "store-a", "audit-bucket", time.Unix(2_000_000_000, 0))
	require.NoError(t, err)
	require.Equal(t, BucketProbe{
		Format: BucketProbeFormat, ObjectStoreID: "store-a", Bucket: "audit-bucket",
		VersioningEnabled: true, ObjectLockEnabled: true, CheckedAtUnix: 2_000_000_000,
	}, probe)
	require.Equal(t, 1, client.versionGet)
	require.Equal(t, 1, client.lockGet)
}

func TestProbeBucketFailsClosed(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*bucketProbeClient)
		want   string
	}{
		{name: "versioning suspended", mutate: func(c *bucketProbeClient) { c.versioning.Status = types.BucketVersioningStatusSuspended }, want: "versioning is not Enabled"},
		{name: "lock missing", mutate: func(c *bucketProbeClient) { c.lock.ObjectLockConfiguration = nil }, want: "Object Lock is not Enabled"},
		{name: "version read fails", mutate: func(c *bucketProbeClient) { c.versionErr = errors.New("denied") }, want: "read bucket versioning"},
		{name: "lock read fails", mutate: func(c *bucketProbeClient) { c.lockErr = errors.New("denied") }, want: "read bucket Object Lock"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			client := enabledBucketProbeClient()
			test.mutate(client)
			_, err := ProbeBucket(context.Background(), client, "store-a", "audit-bucket", time.Time{})
			require.ErrorContains(t, err, test.want)
		})
	}
	client := enabledBucketProbeClient()
	_, err := ProbeBucket(context.Background(), client, "bad store", "audit-bucket", time.Time{})
	require.ErrorContains(t, err, "scope is invalid")
	require.Zero(t, client.versionGet)
	require.Zero(t, client.lockGet)
}
