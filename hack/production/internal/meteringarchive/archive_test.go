package meteringarchive

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestArchiverUsesStableSlotIdentityAndVerifiedReceipt(t *testing.T) {
	server := completePrometheusServer(t, 1_700_006_390)
	defer server.Close()
	collector, err := NewCollector(server.URL, server.Client(), "", 5*time.Minute)
	require.NoError(t, err)
	archiver := &Archiver{
		Collector: collector, Instance: "instance-a", Executor: "/executor",
		ObjectStoreID: "store-a", Bucket: "metering", Prefix: "/samples/",
		RetentionMode: "COMPLIANCE", RetentionDuration: 365 * 24 * time.Hour,
		SlotDuration: time.Hour, FinalizationDelay: 10 * time.Minute,
		MaxStaleness: 5 * time.Minute,
		Now:          func() time.Time { return time.Unix(1_700_007_601, 0) },
	}
	var firstArtifact []byte
	archiver.Run = func(_ context.Context, executable string, environment []string) ([]byte, error) {
		require.Equal(t, "/executor", executable)
		values := envMap(environment)
		require.Equal(t, "blob", values["ACTION"])
		require.Equal(t, Format, values["ARTIFACT_FORMAT"])
		require.Equal(t, "instance-a:1700002800:1700006400", values["ARTIFACT_ID"])
		require.Equal(t, "samples/instance-a/2023/11/15/1700002800-1700006400.json", values["S3_OBJECT_KEY"])
		artifact, err := os.ReadFile(values["INPUT"])
		require.NoError(t, err)
		if firstArtifact == nil {
			firstArtifact = append([]byte(nil), artifact...)
		} else {
			require.Equal(t, firstArtifact, artifact)
		}
		sum := sha256.Sum256(artifact)
		retainUntil, err := strconv.ParseInt(values["RETAIN_UNTIL_UNIX"], 10, 64)
		require.NoError(t, err)
		return json.Marshal(map[string]any{
			"format":          "kubebrain.object-immutable-blob.receipt.v1",
			"artifact_format": Format, "artifact_id": values["ARTIFACT_ID"],
			"instance": "instance-a", "object_store_id": "store-a", "bucket": "metering",
			"object_key": values["S3_OBJECT_KEY"], "version_id": "version-1",
			"artifact_sha256": hex.EncodeToString(sum[:]), "object_bytes": len(artifact),
			"retention_mode": "COMPLIANCE", "retain_until_unix": retainUntil,
			"remote_verified": true, "archived_at_unix": int64(1_700_007_601),
		})
	}
	first, _, err := archiver.Process(context.Background())
	require.NoError(t, err)
	second, _, err := archiver.Process(context.Background())
	require.NoError(t, err)
	require.Equal(t, first, second)
	require.Equal(t, int64(1_700_002_800), first.SlotStartUnix)
	require.Equal(t, int64(1_700_006_400), first.SlotEndUnix)
}

func TestArchiverRejectsIncompletePrometheusAndUnverifiedReceipt(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, _ *http.Request) {
		writePrometheusVector(t, response, "instance-a", 1_700_006_390, "0")
	}))
	defer server.Close()
	collector, err := NewCollector(server.URL, server.Client(), "", 5*time.Minute)
	require.NoError(t, err)
	archiver := validArchiver(collector)
	called := false
	archiver.Run = func(context.Context, string, []string) ([]byte, error) {
		called = true
		return nil, nil
	}
	_, _, err = archiver.Process(context.Background())
	require.ErrorContains(t, err, "data is incomplete")
	require.False(t, called)

	server.Config.Handler = http.HandlerFunc(func(response http.ResponseWriter, _ *http.Request) {
		writePrometheusVector(t, response, "instance-a", 1_700_006_390, "1")
	})
	archiver.Run = func(context.Context, string, []string) ([]byte, error) {
		return []byte(`{"format":"wrong"}`), nil
	}
	_, _, err = archiver.Process(context.Background())
	require.ErrorContains(t, err, "receipt")
}

func TestArchiverValidationRejectsUnsafeRetentionAndPrefix(t *testing.T) {
	server := completePrometheusServer(t, 1_700_006_390)
	defer server.Close()
	collector, err := NewCollector(server.URL, server.Client(), "", time.Minute)
	require.NoError(t, err)
	archiver := validArchiver(collector)
	archiver.RetentionDuration = archiver.SlotDuration
	require.ErrorContains(t, archiver.Validate(), "metering retention must exceed the slot and finalization delay")
	archiver.RetentionDuration = 365 * 24 * time.Hour
	archiver.Prefix = "/"
	require.ErrorContains(t, archiver.Validate(), "metering archiver configuration is incomplete")
}

func TestArchiverValidationRejectsUnsafeObjectIdentity(t *testing.T) {
	server := completePrometheusServer(t, 1_700_006_390)
	defer server.Close()
	collector, err := NewCollector(server.URL, server.Client(), "", time.Minute)
	require.NoError(t, err)
	for _, tc := range []struct {
		name   string
		mutate func(*Archiver)
	}{
		{name: "object store control", mutate: func(a *Archiver) { a.ObjectStoreID = "store-a\nother" }},
		{name: "bucket whitespace", mutate: func(a *Archiver) { a.Bucket = "metering bucket" }},
		{name: "prefix parent", mutate: func(a *Archiver) { a.Prefix = "../samples" }},
		{name: "prefix unclean", mutate: func(a *Archiver) { a.Prefix = "samples//hourly" }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			archiver := validArchiver(collector)
			tc.mutate(archiver)
			require.ErrorContains(t, archiver.Validate(), "object identity")
		})
	}
}

func TestRunCommandReturnsContextErrorOnCancellation(t *testing.T) {
	executable := blockingExecutor(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()

	_, err := runCommand(ctx, executable, nil)
	require.ErrorIs(t, err, context.DeadlineExceeded)
}

func blockingExecutor(t *testing.T) string {
	t.Helper()
	executable := filepath.Join(t.TempDir(), "blocking-executor")
	require.NoError(t, os.WriteFile(executable, []byte("#!/bin/sh\nsleep 30\n"), 0o755))
	return executable
}

func validArchiver(collector *Collector) *Archiver {
	return &Archiver{
		Collector: collector, Instance: "instance-a", Executor: "/executor",
		ObjectStoreID: "store-a", Bucket: "metering", Prefix: "samples",
		RetentionMode: "COMPLIANCE", RetentionDuration: 365 * 24 * time.Hour,
		SlotDuration: time.Hour, FinalizationDelay: 10 * time.Minute,
		MaxStaleness: 5 * time.Minute,
		Now:          func() time.Time { return time.Unix(1_700_007_601, 0) },
	}
}

func completePrometheusServer(t *testing.T, timestamp int64) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		query := request.URL.Query().Get("query")
		require.True(t,
			strings.HasPrefix(query, completenessMetric+"{") ||
				func() bool {
					for _, metric := range Metrics {
						if strings.HasPrefix(query, metric+"{") {
							return true
						}
					}
					return false
				}(),
		)
		writePrometheusVector(t, response, "instance-a", timestamp, "1")
	}))
}

func envMap(environment []string) map[string]string {
	values := make(map[string]string, len(environment))
	for _, entry := range environment {
		key, value, found := strings.Cut(entry, "=")
		if found {
			values[key] = value
		}
	}
	return values
}
