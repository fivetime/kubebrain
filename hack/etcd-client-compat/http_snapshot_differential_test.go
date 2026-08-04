package compat

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type httpSnapshotOutcome struct {
	StatusOK              bool
	MultipleChunks        bool
	RemainingMonotonic    bool
	TerminalRemainingZero bool
	ConsistentVersion     bool
	DigestValid           bool
}

type httpSnapshotEnvelope struct {
	Result struct {
		RemainingBytes string `json:"remaining_bytes"`
		Blob           []byte `json:"blob"`
		Version        string `json:"version"`
	} `json:"result"`
}

func TestHTTPSnapshotStreamDifferentialAgainstReferenceEtcd(t *testing.T) {
	reference := os.Getenv("REFERENCE_ETCD_ENDPOINT")
	if reference == "" {
		t.Skip("set REFERENCE_ETCD_ENDPOINT to run differential compatibility tests")
	}

	want := httpSnapshotOutcome{
		StatusOK:              true,
		MultipleChunks:        true,
		RemainingMonotonic:    true,
		TerminalRemainingZero: true,
		ConsistentVersion:     true,
		DigestValid:           true,
	}
	referenceOutcome := runHTTPSnapshotScenario(t, reference)
	require.Equal(t, want, referenceOutcome)
	require.Equal(t, referenceOutcome, runHTTPSnapshotScenario(t, compatEndpoint(t)))
}

func runHTTPSnapshotScenario(t *testing.T, endpoint string) httpSnapshotOutcome {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	url := strings.TrimSuffix(endpoint, "/") + "/v3/maintenance/snapshot"
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewBufferString("{}"))
	require.NoError(t, err)
	request.Header.Set("Content-Type", "application/json")
	response, err := http.DefaultClient.Do(request)
	require.NoError(t, err)
	defer func() { require.NoError(t, response.Body.Close()) }()

	outcome := httpSnapshotOutcome{StatusOK: response.StatusCode == http.StatusOK}
	if response.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(response.Body, 4<<10))
		t.Fatalf("snapshot HTTP status %s: %s", response.Status, body)
	}

	decoder := json.NewDecoder(response.Body)
	var blobs [][]byte
	var previousRemaining uint64
	var version string
	monotonic := true
	consistentVersion := true
	for index := 0; ; index++ {
		var envelope httpSnapshotEnvelope
		err = decoder.Decode(&envelope)
		if err == io.EOF {
			break
		}
		require.NoError(t, err)
		var remaining uint64
		if envelope.Result.RemainingBytes != "" {
			remaining, err = strconv.ParseUint(envelope.Result.RemainingBytes, 10, 64)
			require.NoError(t, err)
		}
		if index > 0 && remaining > previousRemaining {
			monotonic = false
		}
		previousRemaining = remaining
		if index == 0 {
			version = envelope.Result.Version
		} else if envelope.Result.Version != version {
			consistentVersion = false
		}
		blobs = append(blobs, envelope.Result.Blob)
	}

	outcome.MultipleChunks = len(blobs) > 1
	outcome.RemainingMonotonic = monotonic
	outcome.TerminalRemainingZero = len(blobs) > 0 && previousRemaining == 0
	outcome.ConsistentVersion = version != "" && consistentVersion
	if len(blobs) > 1 && len(blobs[len(blobs)-1]) == sha256.Size {
		digest := sha256.New()
		for _, blob := range blobs[:len(blobs)-1] {
			_, _ = digest.Write(blob)
		}
		outcome.DigestValid = bytes.Equal(digest.Sum(nil), blobs[len(blobs)-1])
	}
	return outcome
}
