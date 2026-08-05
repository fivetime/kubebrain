package compat

import (
	"context"
	"crypto/sha256"
	"errors"
	"io"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/coreos/go-semver/semver"
	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type snapshotStreamOutcome struct {
	Version            string
	BackendBytes       int
	DataResponses      int
	FinalBytes         int
	DataFramesNonempty bool
	RemainingExact     bool
	DigestMatches      bool
}

func TestSnapshotStreamProtocolDifferentialAgainstReferenceEtcd(t *testing.T) {
	reference := os.Getenv("REFERENCE_ETCD_ENDPOINT")
	kubebrain := os.Getenv("KUBEBRAIN_ETCD_ENDPOINT")
	if reference == "" || kubebrain == "" {
		t.Skip("set REFERENCE_ETCD_ENDPOINT and KUBEBRAIN_ETCD_ENDPOINT")
	}
	referenceOutcome := captureSnapshotStream(t, reference)
	kubebrainOutcome := captureSnapshotStream(t, kubebrain)
	for name, outcome := range map[string]snapshotStreamOutcome{"reference": referenceOutcome, "kubebrain": kubebrainOutcome} {
		t.Run(name, func(t *testing.T) {
			_, err := semver.NewVersion(outcome.Version)
			require.NoError(t, err)
			require.Positive(t, outcome.BackendBytes)
			require.Positive(t, outcome.DataResponses)
			require.Equal(t, sha256.Size, outcome.FinalBytes)
			require.True(t, outcome.DataFramesNonempty)
			require.True(t, outcome.RemainingExact)
			require.True(t, outcome.DigestMatches)
		})
	}
	require.Equal(t, "3.7.0", kubebrainOutcome.Version)
}

func captureSnapshotStream(t *testing.T, endpoint string) snapshotStreamOutcome {
	t.Helper()
	target := strings.TrimPrefix(strings.TrimPrefix(endpoint, "http://"), "https://")
	conn, err := grpc.NewClient(target, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, conn.Close()) })
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	stream, err := etcdserverpb.NewMaintenanceClient(conn).Snapshot(ctx, &etcdserverpb.SnapshotRequest{})
	require.NoError(t, err)
	var responses []*etcdserverpb.SnapshotResponse
	for {
		response, recvErr := stream.Recv()
		if errors.Is(recvErr, io.EOF) {
			break
		}
		require.NoError(t, recvErr)
		responses = append(responses, response)
	}
	require.GreaterOrEqual(t, len(responses), 2)
	hash := sha256.New()
	backendBytes := 0
	version := ""
	dataResponses := responses[:len(responses)-1]
	for _, response := range dataResponses {
		require.LessOrEqual(t, len(response.Blob), 32*1024)
		if version == "" {
			version = response.Version
		}
		require.Equal(t, version, response.Version)
		backendBytes += len(response.Blob)
		_, _ = hash.Write(response.Blob)
	}
	final := responses[len(responses)-1]
	require.Zero(t, final.RemainingBytes)
	require.Equal(t, version, final.Version)
	dataFramesNonempty := true
	remainingExact := true
	cumulative := 0
	for _, response := range dataResponses {
		dataFramesNonempty = dataFramesNonempty && len(response.Blob) > 0
		cumulative += len(response.Blob)
		remainingExact = remainingExact && response.RemainingBytes == uint64(backendBytes-cumulative)
	}
	return snapshotStreamOutcome{
		Version: version, BackendBytes: backendBytes, DataResponses: len(dataResponses),
		FinalBytes: len(final.Blob), DataFramesNonempty: dataFramesNonempty,
		RemainingExact: remainingExact, DigestMatches: string(hash.Sum(nil)) == string(final.Blob),
	}
}
