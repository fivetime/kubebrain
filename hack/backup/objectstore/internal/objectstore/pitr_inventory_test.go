package objectstore

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/kubewharf/kubebrain/hack/backup/internal/pitrinventory"
	"github.com/stretchr/testify/require"
)

func TestCapturePITRInventoryExhaustsPagesAndVerifiesExactVersion(t *testing.T) {
	now := time.Unix(2_000_000_000, 0).UTC()
	client := &fakeS3{body: []byte("backup-object"), versionID: "version-1", retainUntil: now.Add(24 * time.Hour), mode: types.ObjectLockRetentionModeCompliance}
	version := types.ObjectVersion{Key: aws.String("instances/a/log/v1/log/1.log"), VersionId: aws.String("version-1"), Size: aws.Int64(int64(len(client.body))), IsLatest: aws.Bool(true)}
	client.listOutputs = []*s3.ListObjectVersionsOutput{{IsTruncated: aws.Bool(true), NextKeyMarker: aws.String("instances/a/log/v1/log/1.log"), NextVersionIdMarker: aws.String("version-1")}, {Versions: []types.ObjectVersion{version}}}
	output := filepath.Join(t.TempDir(), "inventory.json")
	receipt, err := CapturePITRInventory(context.Background(), client, PITRInventoryRequest{ObjectStoreID: "store-a", Bucket: "bucket-a", Prefix: "instances/a/log", MinRetainUntilUnix: now.Add(time.Hour).Unix(), ReceiptOutput: output, Now: now})
	require.NoError(t, err)
	require.Equal(t, 2, receipt.Pages)
	require.Equal(t, 1, receipt.ObjectCount)
	require.Equal(t, "v1/log/1.log", receipt.Entries[0].Name)
	read, _, err := pitrinventory.ReadCanonical(output)
	require.NoError(t, err)
	require.Equal(t, receipt, read)
	client.listCalls = 0
	retried, err := CapturePITRInventory(context.Background(), client, PITRInventoryRequest{ObjectStoreID: "store-a", Bucket: "bucket-a", Prefix: "instances/a/log", MinRetainUntilUnix: now.Add(time.Hour).Unix(), ReceiptOutput: output, Now: now.Add(time.Minute)})
	require.NoError(t, err)
	require.Equal(t, receipt, retried)
}

func TestCapturePITRInventoryRejectsMutableOrDeletedPrefix(t *testing.T) {
	now := time.Unix(2_000_000_000, 0).UTC()
	base := func() *fakeS3 {
		return &fakeS3{body: []byte("x"), versionID: "v1", retainUntil: now.Add(time.Hour), mode: types.ObjectLockRetentionModeCompliance}
	}
	t.Run("non-latest version", func(t *testing.T) {
		client := base()
		client.listOutputs = []*s3.ListObjectVersionsOutput{{Versions: []types.ObjectVersion{{Key: aws.String("p/x"), VersionId: aws.String("v1"), Size: aws.Int64(1), IsLatest: aws.Bool(false)}}}}
		_, err := CapturePITRInventory(context.Background(), client, PITRInventoryRequest{ObjectStoreID: "s", Bucket: "b", Prefix: "p", MinRetainUntilUnix: now.Add(time.Minute).Unix(), ReceiptOutput: filepath.Join(t.TempDir(), "r"), Now: now})
		require.ErrorContains(t, err, "immutable")
	})
	t.Run("delete marker", func(t *testing.T) {
		client := base()
		client.listOutputs = []*s3.ListObjectVersionsOutput{{DeleteMarkers: []types.DeleteMarkerEntry{{Key: aws.String("p/x"), VersionId: aws.String("v2")}}}}
		_, err := CapturePITRInventory(context.Background(), client, PITRInventoryRequest{ObjectStoreID: "s", Bucket: "b", Prefix: "p", MinRetainUntilUnix: now.Add(time.Minute).Unix(), ReceiptOutput: filepath.Join(t.TempDir(), "r"), Now: now})
		require.ErrorContains(t, err, "delete markers")
	})
}
