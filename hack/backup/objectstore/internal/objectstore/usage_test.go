package objectstore

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/kubewharf/kubebrain/hack/production/operationaudit"
	"github.com/stretchr/testify/require"
)

func TestMeasureUsageVerifiesAndTotalsEveryVersion(t *testing.T) {
	fixture := newInventoryFixture(t)
	request := UsageRequest{
		ObjectStoreID: "store-a", Bucket: "bucket-a", Prefix: "audits/",
		AllowedFormats: []string{operationaudit.Format},
		ReceiptOutput:  filepath.Join(t.TempDir(), "usage.json"),
		Now:            time.Unix(2_000_000_000, 0),
	}
	receipt, err := MeasureUsage(context.Background(), fixture.client, request)
	require.NoError(t, err)
	require.Equal(t, 2, receipt.RemoteVersions)
	require.Equal(t, int64(303), receipt.TotalObjectBytes)
	require.Len(t, receipt.VersionsSHA256, 64)
	require.Equal(t, int64(2_000_000_000), receipt.CheckedAtUnix)

	fixture.client.listCalls = 0
	request.Now = time.Unix(2_000_000_100, 0)
	repeated, err := MeasureUsage(context.Background(), fixture.client, request)
	require.NoError(t, err)
	require.Equal(t, receipt, repeated)
	persisted, err := ReadUsageReceipt(request.ReceiptOutput)
	require.NoError(t, err)
	require.Equal(t, receipt, persisted)
}

func TestMeasureUsageFailsClosedWithoutReceipt(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*inventoryFixture, *UsageRequest)
		want   string
	}{
		{
			name: "delete marker",
			mutate: func(f *inventoryFixture, _ *UsageRequest) {
				f.client.listOutputs[0].DeleteMarkers = []types.DeleteMarkerEntry{{
					Key: aws.String("audits/deleted"), VersionId: aws.String("marker"),
				}}
			},
			want: "delete markers",
		},
		{
			name: "unknown format",
			mutate: func(f *inventoryFixture, _ *UsageRequest) {
				f.client.heads[f.identity(0)].Metadata["kubebrain-format"] = "unknown"
			},
			want: "metadata differs",
		},
		{
			name: "size mismatch",
			mutate: func(f *inventoryFixture, _ *UsageRequest) {
				f.client.heads[f.identity(0)].ContentLength = aws.Int64(100)
			},
			want: "metadata differs",
		},
		{
			name: "retention drift",
			mutate: func(f *inventoryFixture, _ *UsageRequest) {
				f.client.retentions[f.identity(0)].Retention.RetainUntilDate =
					aws.Time(time.Unix(f.entries[0].RetainUntilUnix+1, 0))
			},
			want: "retention differs",
		},
		{
			name: "unsorted allowlist",
			mutate: func(_ *inventoryFixture, request *UsageRequest) {
				request.AllowedFormats = []string{"z", "a"}
			},
			want: "sorted",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fixture := newInventoryFixture(t)
			request := UsageRequest{
				ObjectStoreID: "store-a", Bucket: "bucket-a", Prefix: "audits/",
				AllowedFormats: []string{operationaudit.Format},
				ReceiptOutput:  filepath.Join(t.TempDir(), "usage.json"),
				Now:            time.Unix(2_000_000_000, 0),
			}
			test.mutate(fixture, &request)
			_, err := MeasureUsage(context.Background(), fixture.client, request)
			require.ErrorContains(t, err, test.want)
			_, statErr := os.Stat(request.ReceiptOutput)
			require.ErrorIs(t, statErr, os.ErrNotExist)
		})
	}
}

func TestUsageReceiptRejectsTamperAndNonCanonicalJSON(t *testing.T) {
	fixture := newInventoryFixture(t)
	path := filepath.Join(t.TempDir(), "usage.json")
	_, err := MeasureUsage(context.Background(), fixture.client, UsageRequest{
		ObjectStoreID: "store-a", Bucket: "bucket-a", Prefix: "audits/",
		AllowedFormats: []string{operationaudit.Format}, ReceiptOutput: path,
		Now: time.Unix(2_000_000_000, 0),
	})
	require.NoError(t, err)
	data, err := os.ReadFile(path)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(path, append([]byte(" "), data...), 0o600))
	_, err = ReadUsageReceipt(path)
	require.ErrorContains(t, err, "canonical")

	require.NoError(t, os.WriteFile(path, []byte(strings.Replace(
		string(data), `"total_object_bytes":303`, `"total_object_bytes":-1`, 1,
	)), 0o600))
	_, err = ReadUsageReceipt(path)
	require.Error(t, err)
}
