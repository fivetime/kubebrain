package meteringbilling

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestBuildProviderStatementFromCSVSortsAndCanonicalizesClassifiedLines(t *testing.T) {
	start := time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC)
	input := string(EncodeProviderStatementCSVHeader()) +
		"aws,acct-b,aws-inv-2026-07,line-003,object_storage,store-b,bucket-b,70\n" +
		"aws,acct-a,aws-inv-2026-07,line-001,compute,,,300\n" +
		"aws,acct-a,aws-inv-2026-07,line-002,object_storage,store-a,bucket-a,30\n"
	statement, err := BuildProviderStatementFromCSV(
		strings.NewReader(input),
		ProviderStatementImportOptions{
			ID: "provider-july", Instance: "instance-a",
			PeriodStartUnix: start.Unix(), PeriodEndUnix: start.Add(24 * time.Hour).Unix(),
			Currency: "USD", IssuedAtUnix: start.Add(48 * time.Hour).Unix(),
		},
	)
	require.NoError(t, err)
	require.Equal(t, int64(400), statement.TotalMicros)
	require.Equal(t, []string{"line-001", "line-002", "line-003"}, []string{
		statement.Lines[0].ExternalLineID,
		statement.Lines[1].ExternalLineID,
		statement.Lines[2].ExternalLineID,
	})

	output := filepath.Join(t.TempDir(), "provider-statement.json")
	status, err := WriteProviderStatementAtomic(output, statement)
	require.NoError(t, err)
	read, err := ReadProviderStatement(output)
	require.NoError(t, err)
	require.Equal(t, status, read)
}

func TestBuildProviderStatementFromCSVRejectsUnknownHeaderDuplicateAndUnclassifiedObjectStorage(t *testing.T) {
	start := time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC)
	options := ProviderStatementImportOptions{
		ID: "provider-july", Instance: "instance-a",
		PeriodStartUnix: start.Unix(), PeriodEndUnix: start.Add(24 * time.Hour).Unix(),
		Currency: "USD", IssuedAtUnix: start.Add(48 * time.Hour).Unix(),
	}
	_, err := BuildProviderStatementFromCSV(strings.NewReader("provider,amount_micros\naws,1\n"), options)
	require.ErrorContains(t, err, "header")

	duplicate := string(EncodeProviderStatementCSVHeader()) +
		"aws,acct-a,invoice,line-001,compute,,,10\n" +
		"aws,acct-a,invoice,line-001,compute,,,20\n"
	_, err = BuildProviderStatementFromCSV(strings.NewReader(duplicate), options)
	require.ErrorContains(t, err, "sorted")

	unclassified := string(EncodeProviderStatementCSVHeader()) +
		"aws,acct-a,invoice,line-001,object_storage,,,10\n"
	_, err = BuildProviderStatementFromCSV(strings.NewReader(unclassified), options)
	require.ErrorContains(t, err, "bucket-scoped")
}

func TestProviderStatementPublisherValidatesCanonicalStatementAndArchivesExactReceipt(t *testing.T) {
	start := time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC)
	retainUntil := start.Add(time.Hour + 7*24*time.Hour).Unix()
	now := start.Add(72 * time.Hour)
	statement := validProviderStatement(start)
	statementPath := filepath.Join(t.TempDir(), "provider-statement.json")
	status, err := WriteProviderStatementAtomic(statementPath, statement)
	require.NoError(t, err)
	publisher := &ProviderStatementPublisher{
		Input: statementPath, Executor: "executor", ObjectStoreID: "store",
		Bucket: "billing", ProviderPrefix: "/providers/", RetentionMode: "COMPLIANCE",
		RetentionDuration: 7 * 24 * time.Hour, Now: func() time.Time { return now },
	}
	publisher.Run = func(_ context.Context, executable string, environment []string) ([]byte, error) {
		require.Equal(t, "executor", executable)
		values := envMap(environment)
		require.Equal(t, "blob", values["ACTION"])
		require.Equal(t, ProviderStatementFormat, values["ARTIFACT_FORMAT"])
		require.Equal(t, "providers/instance-a/provider-july.json", values["S3_OBJECT_KEY"])
		require.Equal(t, "provider-july", values["ARTIFACT_ID"])
		require.Equal(t, "instance-a", values["INSTANCE"])
		require.Equal(t, "store", values["OBJECT_STORE_ID"])
		require.Equal(t, "billing", values["S3_BUCKET"])
		require.Equal(t, "COMPLIANCE", values["RETENTION_MODE"])
		require.Equal(t, strconvFormatInt(retainUntil), values["RETAIN_UNTIL_UNIX"])
		data, err := os.ReadFile(values["INPUT"])
		require.NoError(t, err)
		sum := sha256.Sum256(data)
		receipt := objectReceipt{
			Format:         "kubebrain.object-immutable-blob.receipt.v1",
			ArtifactFormat: ProviderStatementFormat, ArtifactID: statement.ID,
			Instance: statement.Instance, ObjectStoreID: "store", Bucket: "billing",
			ObjectKey: values["S3_OBJECT_KEY"], VersionID: "provider-statement-version",
			ArtifactSHA256: hex.EncodeToString(sum[:]), ObjectBytes: int64(len(data)),
			RetentionMode: "COMPLIANCE", RetainUntilUnix: retainUntil,
			RemoteVerified: true, ArchivedAtUnix: now.Unix(),
		}
		return json.Marshal(receipt)
	}
	published, _, err := publisher.Publish(context.Background())
	require.NoError(t, err)
	require.Equal(t, status, published)
}

func TestProviderStatementPublisherRejectsNonCanonicalOrFutureStatementBeforeExecutor(t *testing.T) {
	start := time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC)
	path := filepath.Join(t.TempDir(), "bad.json")
	require.NoError(t, os.WriteFile(path, []byte("{}\n"), 0o600))
	called := false
	publisher := &ProviderStatementPublisher{
		Input: path, Executor: "executor", ObjectStoreID: "store", Bucket: "billing",
		ProviderPrefix: "providers", RetentionMode: "COMPLIANCE",
		RetentionDuration: 7 * 24 * time.Hour,
		Run: func(context.Context, string, []string) ([]byte, error) {
			called = true
			return nil, nil
		},
	}
	_, _, err := publisher.Publish(context.Background())
	require.Error(t, err)
	require.False(t, called)

	future := validProviderStatement(start)
	future.IssuedAtUnix = start.Add(72 * time.Hour).Unix()
	futurePath := filepath.Join(t.TempDir(), "future.json")
	_, err = WriteProviderStatementAtomic(futurePath, future)
	require.NoError(t, err)
	publisher.Input = futurePath
	publisher.Now = func() time.Time { return start.Add(48 * time.Hour) }
	_, _, err = publisher.Publish(context.Background())
	require.ErrorContains(t, err, "future")
	require.False(t, called)
}

func strconvFormatInt(value int64) string {
	return strconv.FormatInt(value, 10)
}
