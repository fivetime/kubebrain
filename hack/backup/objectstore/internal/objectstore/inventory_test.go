package objectstore

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/kubewharf/kubebrain/hack/production/operationaudit"
	"github.com/stretchr/testify/require"
)

func TestInventoryReconcilesPaginatedExactVersionsAndRetries(t *testing.T) {
	fixture := newInventoryFixture(t)
	fixture.client.listOutputs = []*s3.ListObjectVersionsOutput{
		{
			Versions:    []types.ObjectVersion{fixture.version(0)},
			IsTruncated: aws.Bool(true), NextKeyMarker: aws.String(fixture.entries[0].ObjectKey),
			NextVersionIdMarker: aws.String(fixture.entries[0].VersionID),
		},
		{Versions: []types.ObjectVersion{fixture.version(1)}},
	}
	receipt, err := ReconcileInventory(context.Background(), fixture.client, fixture.request)
	require.NoError(t, err)
	require.Equal(t, 2, receipt.ExpectedVersions)
	require.Equal(t, 2, receipt.RemoteVersions)
	require.True(t, receipt.AllMatched)
	require.Equal(t, 2, fixture.client.listCalls)

	fixture.client.listCalls = 0
	retried, err := ReconcileInventory(context.Background(), fixture.client, fixture.request)
	require.NoError(t, err)
	require.Equal(t, receipt, retried)

	data, err := os.ReadFile(fixture.request.ReceiptOutput)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(fixture.request.ReceiptOutput, append(data, ' '), 0o600))
	_, err = ReadInventoryReceipt(fixture.request.ReceiptOutput)
	require.ErrorContains(t, err, "canonical")
}

func TestInventoryRejectsRemoteDriftWithoutReceipt(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mutate func(*inventoryFixture)
		want   string
	}{
		{
			name: "missing version",
			mutate: func(f *inventoryFixture) {
				f.client.listOutputs = []*s3.ListObjectVersionsOutput{
					{Versions: []types.ObjectVersion{f.version(0)}},
				}
			},
			want: "version count mismatch",
		},
		{
			name: "unexpected version",
			mutate: func(f *inventoryFixture) {
				f.client.listOutputs = []*s3.ListObjectVersionsOutput{{Versions: []types.ObjectVersion{
					f.version(0), f.version(1),
					{Key: aws.String("audits/unexpected"), VersionId: aws.String("v3"), Size: aws.Int64(1)},
				}}}
			},
			want: "version count mismatch",
		},
		{
			name: "delete marker",
			mutate: func(f *inventoryFixture) {
				f.client.listOutputs = []*s3.ListObjectVersionsOutput{{
					Versions: []types.ObjectVersion{f.version(0), f.version(1)},
					DeleteMarkers: []types.DeleteMarkerEntry{{
						Key: aws.String("audits/deleted"), VersionId: aws.String("marker"),
					}},
				}}
			},
			want: "delete markers",
		},
		{
			name: "metadata",
			mutate: func(f *inventoryFixture) {
				f.client.heads[f.identity(0)].Metadata["kubebrain-artifact-sha256"] = strings.Repeat("9", 64)
			},
			want: "metadata differs",
		},
		{
			name: "retention",
			mutate: func(f *inventoryFixture) {
				f.client.retentions[f.identity(0)].Retention.RetainUntilDate =
					aws.Time(time.Unix(f.entries[0].RetainUntilUnix+1, 0))
			},
			want: "retention differs",
		},
		{
			name: "pagination does not advance",
			mutate: func(f *inventoryFixture) {
				f.client.listOutputs = []*s3.ListObjectVersionsOutput{{
					Versions: []types.ObjectVersion{f.version(0)}, IsTruncated: aws.Bool(true),
				}}
			},
			want: "did not advance",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fixture := newInventoryFixture(t)
			tc.mutate(fixture)
			_, err := ReconcileInventory(context.Background(), fixture.client, fixture.request)
			require.ErrorContains(t, err, tc.want)
			_, statErr := os.Stat(fixture.request.ReceiptOutput)
			require.ErrorIs(t, statErr, os.ErrNotExist)
		})
	}
}

func TestInventoryManifestRequiresCanonicalSortedUniqueEntries(t *testing.T) {
	fixture := newInventoryFixture(t)
	manifest := fixture.manifest
	manifest.Entries[0], manifest.Entries[1] = manifest.Entries[1], manifest.Entries[0]
	writeInventoryJSON(t, fixture.request.Input, manifest)
	_, err := InspectInventoryManifest(fixture.request.Input)
	require.ErrorContains(t, err, "sorted")

	data, err := os.ReadFile(fixture.request.Input)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(fixture.request.Input, append([]byte(" "), data...), 0o600))
	_, err = InspectInventoryManifest(fixture.request.Input)
	require.Error(t, err)
}

func TestInventoryManifestRejectsUnsafeObjectIdentity(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mutate func(*InventoryManifest)
		want   string
	}{
		{
			name: "object store control byte",
			mutate: func(manifest *InventoryManifest) {
				manifest.ObjectStoreID = "store\t-a"
			},
			want: "incomplete",
		},
		{
			name: "bucket whitespace",
			mutate: func(manifest *InventoryManifest) {
				manifest.Bucket = "bucket a"
			},
			want: "incomplete",
		},
		{
			name: "prefix parent",
			mutate: func(manifest *InventoryManifest) {
				manifest.Prefix = "../audits/"
			},
			want: "incomplete",
		},
		{
			name: "entry key parent",
			mutate: func(manifest *InventoryManifest) {
				manifest.Entries[0].ObjectKey = "../audits/a.json"
			},
			want: "invalid entry",
		},
		{
			name: "entry key unclean",
			mutate: func(manifest *InventoryManifest) {
				manifest.Entries[0].ObjectKey = "audits//a.json"
			},
			want: "invalid entry",
		},
		{
			name: "entry version control byte",
			mutate: func(manifest *InventoryManifest) {
				manifest.Entries[0].VersionID = "v\t1"
			},
			want: "invalid entry",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			manifest := newInventoryFixture(t).manifest
			tc.mutate(&manifest)
			require.ErrorContains(t, manifest.Validate(), tc.want)
		})
	}
}

func TestInventoryReceiptRejectsUnsafeObjectIdentity(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mutate func(*InventoryReceipt)
	}{
		{
			name: "object store invalid utf8",
			mutate: func(receipt *InventoryReceipt) {
				receipt.ObjectStoreID = string([]byte{'s', 0xff})
			},
		},
		{
			name: "bucket whitespace",
			mutate: func(receipt *InventoryReceipt) {
				receipt.Bucket = "bucket a"
			},
		},
		{
			name: "prefix unclean",
			mutate: func(receipt *InventoryReceipt) {
				receipt.Prefix = "audits//"
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			receipt := InventoryReceipt{
				Format: InventoryReceiptFormat, ObjectStoreID: "store-a",
				Bucket: "bucket-a", Prefix: "audits/",
				ManifestSHA256: strings.Repeat("a", 64),
				AllMatched:     true, CheckedAtUnix: 1,
			}
			tc.mutate(&receipt)
			require.ErrorContains(t, receipt.Validate(), "incomplete")
		})
	}
}

func TestBuildInventoryManifestSortsReceiptsAndRejectsDuplicates(t *testing.T) {
	dir := t.TempDir()
	writeAuditReceipt := func(name, key, version, digest string) string {
		path := filepath.Join(dir, name)
		writeInventoryValue(t, path, AuditReceipt{
			Format: AuditReceiptFormat, OperationID: name, OperationUID: "uid-" + name,
			Instance: "instance-a", OperationType: "Backup", Phase: "Succeeded",
			ExecutionReceiptSHA256: strings.Repeat("c", 64), ObjectStoreID: "store-a",
			Bucket: "bucket-a", ObjectKey: key, VersionID: version,
			ArtifactSHA256: digest, ObjectBytes: 100, RetentionMode: "COMPLIANCE",
			RetainUntilUnix: 200, RemoteVerified: true, ArchivedAtUnix: 100,
		})
		return path
	}
	b := writeAuditReceipt("b.json", "audits/b.json", "v2", strings.Repeat("b", 64))
	a := writeAuditReceipt("a.json", "audits/a.json", "v1", strings.Repeat("a", 64))
	output := filepath.Join(dir, "manifest.json")
	status, err := BuildInventoryManifest(
		[]string{b, a}, "store-a", "bucket-a", "audits/", output,
	)
	require.NoError(t, err)
	require.Equal(t, "audits/a.json", status.Manifest.Entries[0].ObjectKey)
	require.Equal(t, "audits/b.json", status.Manifest.Entries[1].ObjectKey)
	require.NoError(t, func() error {
		_, err := BuildInventoryManifest(
			[]string{b, a}, "store-a", "bucket-a", "audits/", output,
		)
		return err
	}())

	_, err = BuildInventoryManifest(
		[]string{a, a}, "store-a", "bucket-a", "audits/",
		filepath.Join(dir, "duplicate.json"),
	)
	require.ErrorContains(t, err, "unique")
}

type inventoryFixture struct {
	manifest InventoryManifest
	entries  []InventoryEntry
	request  InventoryRequest
	client   *fakeS3
}

func newInventoryFixture(t *testing.T) *inventoryFixture {
	t.Helper()
	entries := []InventoryEntry{
		{
			ArtifactFormat: operationaudit.Format, ObjectKey: "audits/a.json", VersionID: "v1",
			ArtifactSHA256: strings.Repeat("a", 64), ObjectBytes: 101,
			RetentionMode: "COMPLIANCE", RetainUntilUnix: 2_000_000_100,
		},
		{
			ArtifactFormat: operationaudit.Format, ObjectKey: "audits/b.json", VersionID: "v2",
			ArtifactSHA256: strings.Repeat("b", 64), ObjectBytes: 202,
			RetentionMode: "GOVERNANCE", RetainUntilUnix: 2_000_000_200,
		},
	}
	manifest := InventoryManifest{
		Format: InventoryManifestFormat, ObjectStoreID: "store-a",
		Bucket: "bucket-a", Prefix: "audits/", Entries: entries,
	}
	dir := t.TempDir()
	input := filepath.Join(dir, "manifest.json")
	writeInventoryJSON(t, input, manifest)
	client := &fakeS3{
		heads:      make(map[string]*s3.HeadObjectOutput),
		retentions: make(map[string]*s3.GetObjectRetentionOutput),
	}
	fixture := &inventoryFixture{
		manifest: manifest, entries: entries, client: client,
		request: InventoryRequest{
			Input: input, ObjectStoreID: "store-a",
			ReceiptOutput: filepath.Join(dir, "receipt.json"),
			Now:           time.Unix(2_000_000_000, 0),
		},
	}
	for i, entry := range entries {
		client.heads[fixture.identity(i)] = &s3.HeadObjectOutput{
			ContentLength: aws.Int64(entry.ObjectBytes), VersionId: aws.String(entry.VersionID),
			Metadata: map[string]string{
				"kubebrain-format":            entry.ArtifactFormat,
				"kubebrain-object-store-id":   manifest.ObjectStoreID,
				"kubebrain-artifact-sha256":   entry.ArtifactSHA256,
				"kubebrain-object-bytes":      formatInt(entry.ObjectBytes),
				"kubebrain-retain-until-unix": formatInt(entry.RetainUntilUnix),
			},
		}
		client.retentions[fixture.identity(i)] = &s3.GetObjectRetentionOutput{
			Retention: &types.ObjectLockRetention{
				Mode:            types.ObjectLockRetentionMode(entry.RetentionMode),
				RetainUntilDate: aws.Time(time.Unix(entry.RetainUntilUnix, 0)),
			},
		}
	}
	client.listOutputs = []*s3.ListObjectVersionsOutput{{
		Versions: []types.ObjectVersion{fixture.version(0), fixture.version(1)},
	}}
	return fixture
}

func (f *inventoryFixture) identity(index int) string {
	return f.entries[index].ObjectKey + "\x00" + f.entries[index].VersionID
}

func (f *inventoryFixture) version(index int) types.ObjectVersion {
	entry := f.entries[index]
	return types.ObjectVersion{
		Key: aws.String(entry.ObjectKey), VersionId: aws.String(entry.VersionID),
		Size: aws.Int64(entry.ObjectBytes),
	}
}

func writeInventoryJSON(t *testing.T, path string, value InventoryManifest) {
	t.Helper()
	data, err := json.Marshal(value)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(path, append(data, '\n'), 0o600))
}

func writeInventoryValue(t *testing.T, path string, value any) {
	t.Helper()
	data, err := json.Marshal(value)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(path, append(data, '\n'), 0o600))
}

func formatInt(value int64) string {
	return fmt.Sprintf("%d", value)
}
